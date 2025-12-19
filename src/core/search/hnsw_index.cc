// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/strings/match.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/hnsw_alg.h"
#include "core/search/mrmw_mutex.h"
#include "core/search/vector_utils.h"
#include "util/fibers/synchronization.h"

namespace dfly::search {

using namespace std;

namespace {

class HnswSpace : public hnswlib::SpaceInterface<float> {
  unsigned dim_;

  VectorSimilarity sim_;

  static float L2DistanceStatic(const void* pVect1, const void* pVect2, const void* param) {
    return L2Distance(static_cast<const float*>(pVect1), static_cast<const float*>(pVect2),

                      *static_cast<const unsigned*>(param));
  }

  static float IPDistanceStatic(const void* pVect1, const void* pVect2, const void* param) {
    return IPDistance(static_cast<const float*>(pVect1), static_cast<const float*>(pVect2),

                      *static_cast<const unsigned*>(param));
  }

 public:
  explicit HnswSpace(size_t dim, VectorSimilarity sim) : dim_(dim), sim_(sim) {
  }

  size_t get_data_size() {
    return dim_ * sizeof(float);
  }

  hnswlib::DISTFUNC<float> get_dist_func() {
    if (sim_ == VectorSimilarity::L2) {
      return L2DistanceStatic;

    } else {
      return IPDistanceStatic;
    }
  }

  void* get_dist_func_param() {
    return &dim_;
  }
};

}  // namespace

// TODO: to replace it and use HierarchicalNSW directly.
struct HnswlibAdapter {
  // Default setting of hnswlib/hnswalg
  constexpr static size_t kDefaultEfRuntime = 10;

  explicit HnswlibAdapter(const SchemaField::VectorParams& params)
      : space_{MakeSpace(params.dim, params.sim)},
        world_{GetSpacePtr(), params.capacity, params.hnsw_m, params.hnsw_ef_construction,
               100 /* seed*/} {
  }

  ~HnswlibAdapter() {
    LOG(INFO) << "add_execution_time: " << add_execution_time.load();
  }

  void Add(const void* data, GlobalDocId id, ShardId sid) {
    /// shard_set->AddL2(sid, [data, id, this]() {
    while (true) {
      try {
        MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
        std::shared_lock resize_lock(resize_mutex_);
        auto start = std::chrono::high_resolution_clock::now();
        world_.addPoint(data, id);
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::microseconds duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        add_execution_time.fetch_add(duration.count());

        return;
      } catch (const std::exception& e) {
        std::string error_msg = e.what();
        if (absl::StrContains(error_msg, "The number of elements exceeds the specified limit")) {
          ResizeIfFull();
          continue;
        }
        LOG(ERROR) << "HnswlibAdapter::Add exception: " << e.what();
      }
    }
    //});
  }

  void Remove(GlobalDocId id) {
    try {
      MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
      world_.markDelete(id);
    } catch (const std::exception& e) {
      LOG(WARNING) << "HnswlibAdapter::Remove exception: " << e.what();
    }
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    world_.setEf(ef.value_or(kDefaultEfRuntime));
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    return QueueToVec(world_.searchKnn(target, k));
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                       const vector<GlobalDocId>& allowed) {
    struct BinsearchFilter : hnswlib::BaseFilterFunctor {
      virtual bool operator()(hnswlib::labeltype id) {
        return binary_search(allowed->begin(), allowed->end(), id);
      }

      BinsearchFilter(const vector<GlobalDocId>* allowed) : allowed{allowed} {
      }
      const vector<GlobalDocId>* allowed;
    };

    world_.setEf(ef.value_or(kDefaultEfRuntime));
    BinsearchFilter filter{&allowed};
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    return QueueToVec(world_.searchKnn(target, k, &filter));
  }

 private:
  using SpaceUnion = std::variant<hnswlib::L2Space, hnswlib::InnerProductSpace>;

  static SpaceUnion MakeSpace(size_t dim, VectorSimilarity sim) {
    if (sim == VectorSimilarity::L2)
      return hnswlib::L2Space{dim};
    else
      return hnswlib::InnerProductSpace{dim};
  }

  hnswlib::SpaceInterface<float>* GetSpacePtr() {
    return visit([](auto& space) -> hnswlib::SpaceInterface<float>* { return &space; }, space_);
  }

  // Function requires that we hold mutex while resizing index. resizeIndex is not thread safe with
  // insertion (https://github.com/nmslib/hnswlib/issues/267)
  void ResizeIfFull() {
    {
      // First check with reader lock to avoid contention.
      std::shared_lock lock(resize_mutex_);
      if (world_.getCurrentElementCount() < world_.getMaxElements() ||
          (world_.allow_replace_deleted_ && world_.getDeletedCount() > 0)) {
        return;
      }
    }
    try {
      // Upgrade to writer lock.
      std::unique_lock lock(resize_mutex_);
      if (world_.getCurrentElementCount() == world_.getMaxElements() &&
          (!world_.allow_replace_deleted_ || world_.getDeletedCount() == 0)) {
        auto max_elements = world_.getMaxElements();
        world_.resizeIndex(max_elements + kHnswElementsPerChunk);
        VLOG(1) << "Resizing HNSW Index from " << max_elements << " to " << max_elements * 2;
      }
    } catch (const std::exception& e) {
      LOG(FATAL) << "HnswlibAdapter::ResizeIfFull exception: " << e.what();
    }
  }

  template <typename Q> static vector<pair<float, GlobalDocId>> QueueToVec(Q queue) {
    vector<pair<float, GlobalDocId>> out(queue.size());
    size_t idx = out.size();
    while (!queue.empty()) {
      out[--idx] = queue.top();
      queue.pop();
    }
    return out;
  }

  SpaceUnion space_;
  HierarchicalNSW<float> world_;
  util::fb2::SharedMutex resize_mutex_;
  mutable MRMWMutex mrmw_mutex_;
  std::atomic<uint64_t> add_execution_time{0};
};

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource*)
    : dim_{params.dim}, sim_{params.sim}, adapter_{make_unique<HnswlibAdapter>(params)} {
  DCHECK(params.use_hnsw);
  // TODO: Patch hnsw to use MR
}

HnswVectorIndex::~HnswVectorIndex() {
}

bool HnswVectorIndex::Add(GlobalDocId id, const DocumentAccessor& doc, std::string_view field,
                          ShardId sid) {
  auto vector_ptr = doc.GetVectorDirectPtr(field, dim_);

  if (!vector_ptr) {
    return false;
  }

  if (vector_ptr) {
    adapter_->Add(*vector_ptr, id, sid);
  }

  return true;
}

std::vector<std::pair<float, GlobalDocId>> HnswVectorIndex::Knn(float* target, size_t k,
                                                                std::optional<size_t> ef) const {
  return adapter_->Knn(target, k, ef);
}

std::vector<std::pair<float, GlobalDocId>> HnswVectorIndex::Knn(
    float* target, size_t k, std::optional<size_t> ef,
    const std::vector<GlobalDocId>& allowed) const {
  return adapter_->Knn(target, k, ef, allowed);
}

void HnswVectorIndex::Remove(GlobalDocId id, const DocumentAccessor& doc, string_view field) {
  adapter_->Remove(id);
}
}  // namespace dfly::search
