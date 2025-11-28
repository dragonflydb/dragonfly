// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/strings/match.h>
#include <absl/synchronization/mutex.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include "base/logging.h"
#include "core/search/hnsw_alg.h"
#include "core/search/vector_utils.h"

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
      : space_{params.dim, params.sim},
        world_{&space_, params.capacity, params.hnsw_m, params.hnsw_ef_construction,
               100 /* seed*/} {
  }

  void Add(const float* data, GlobalDocId id) {
    while (true) {
      try {
        absl::ReaderMutexLock lock(&resize_mutex_);
        world_.addPoint(data, id);
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
  }

  void Remove(GlobalDocId id) {
    try {
      world_.markDelete(id);
    } catch (const std::exception& e) {
      LOG(WARNING) << "HnswlibAdapter::Remove exception: " << e.what();
    }
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    world_.setEf(ef.value_or(kDefaultEfRuntime));
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
    return QueueToVec(world_.searchKnn(target, k, &filter));
  }

 private:
  // Function requires that we hold mutex while resizing index. resizeIndex is not thread safe with
  // insertion (https://github.com/nmslib/hnswlib/issues/267)
  void ResizeIfFull() {
    {
      // First check with reader lock to avoid contention.
      absl::ReaderMutexLock lock(&resize_mutex_);
      if (world_.getCurrentElementCount() < world_.getMaxElements() ||
          (world_.allow_replace_deleted_ && world_.getDeletedCount() > 0)) {
        return;
      }
    }
    try {
      // Upgrade to writer lock.
      absl::WriterMutexLock lock(&resize_mutex_);
      if (world_.getCurrentElementCount() == world_.getMaxElements() &&
          (!world_.allow_replace_deleted_ || world_.getDeletedCount() == 0)) {
        auto max_elements = world_.getMaxElements();
        world_.resizeIndex(max_elements * 2);
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

  HnswSpace space_;
  HierarchicalNSW<float> world_;
  absl::Mutex resize_mutex_;
};

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource*)
    : dim_{params.dim}, sim_{params.sim}, adapter_{make_unique<HnswlibAdapter>(params)} {
  DCHECK(params.use_hnsw);
  // TODO: Patch hnsw to use MR
}

HnswVectorIndex::~HnswVectorIndex() {
}

bool HnswVectorIndex::Add(GlobalDocId id, const DocumentAccessor& doc, std::string_view field) {
  auto vector = doc.GetVector(field);

  if (!vector) {
    return false;
  }

  auto& [ptr, size] = vector.value();

  if (ptr && size != dim_) {
    return false;
  }

  if (ptr) {
    adapter_->Add(ptr.get(), id);
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
