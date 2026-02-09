// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/strings/match.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include "base/logging.h"
#include "core/search/hnsw_alg.h"
#include "core/search/mrmw_mutex.h"
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

  static float CosineDistanceStatic(const void* pVect1, const void* pVect2, const void* param) {
    return CosineDistance(static_cast<const float*>(pVect1), static_cast<const float*>(pVect2),
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
    } else if (sim_ == VectorSimilarity::COSINE) {
      return CosineDistanceStatic;
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

  explicit HnswlibAdapter(const SchemaField::VectorParams& params, bool copy_vector)
      : space_{params.dim, params.sim}, world_{&space_,       params.capacity,
                                               params.hnsw_m, params.hnsw_ef_construction,
                                               100 /* seed*/, copy_vector} {
  }

  void Add(const void* data, GlobalDocId id) {
    while (true) {
      try {
        MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
        absl::ReaderMutexLock resize_lock(&resize_mutex_);
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
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    HnswErrorStatus status = world_.markDelete(id);
    if (status != HnswErrorStatus::SUCCESS) {
      VLOG(1) << "HnswlibAdapter::Remove failed with status: " << static_cast<int>(status)
              << " for global id: " << id;
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

  HnswIndexMetadata GetMetadata() const {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    HnswIndexMetadata metadata;
    metadata.max_elements = world_.max_elements_;
    metadata.cur_element_count = world_.cur_element_count.load();
    metadata.maxlevel = world_.maxlevel_;
    metadata.enterpoint_node = world_.enterpoint_node_;
    metadata.M = world_.M_;
    metadata.maxM = world_.maxM_;
    metadata.maxM0 = world_.maxM0_;
    metadata.ef_construction = world_.ef_construction_;
    metadata.mult = world_.mult_;
    return metadata;
  }

  size_t GetNodeCount() const {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    return world_.cur_element_count.load();
  }

  std::vector<HnswNodeData> GetNodesRange(size_t start, size_t end) const {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    size_t count = world_.cur_element_count.load();
    end = std::min(end, count);
    start = std::min(start, end);

    std::vector<HnswNodeData> result;
    result.reserve(end - start);

    for (size_t internal_id = start; internal_id < end; ++internal_id) {
      HnswNodeData node_data;
      node_data.internal_id = internal_id;
      node_data.global_id = world_.getExternalLabel(internal_id);
      node_data.level = world_.element_levels_[internal_id];

      node_data.levels_links.resize(node_data.level + 1);

      auto* ll0 = world_.get_linklist0(internal_id);
      unsigned short link_count0 = world_.getListCount(ll0);
      auto* links0 = reinterpret_cast<uint32_t*>(ll0 + 1);
      node_data.levels_links[0].assign(links0, links0 + link_count0);

      for (int lvl = 1; lvl <= node_data.level; ++lvl) {
        auto* ll = world_.get_linklist(internal_id, lvl);
        unsigned short link_count = world_.getListCount(ll);
        auto* links = reinterpret_cast<uint32_t*>(ll + 1);
        node_data.levels_links[lvl].assign(links, links + link_count);
      }

      result.push_back(std::move(node_data));
    }
    return result;
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
  mutable MRMWMutex mrmw_mutex_;
};

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, bool copy_vector,
                                 PMR_NS::memory_resource*)
    : copy_vector_(copy_vector),
      dim_{params.dim},
      sim_{params.sim},
      adapter_{make_unique<HnswlibAdapter>(params, copy_vector)} {
  DCHECK(params.use_hnsw);
  // TODO: Patch hnsw to use MR
}

HnswVectorIndex::~HnswVectorIndex() {
}

bool HnswVectorIndex::Add(GlobalDocId id, const DocumentAccessor& doc, std::string_view field) {
  auto vector_ptr = doc.GetVector(field, dim_);

  if (!vector_ptr) {
    return false;
  }

  if (std::holds_alternative<OwnedFtVector>(*vector_ptr)) {
    auto owned_vector = std::get<OwnedFtVector>(*vector_ptr).first.get();
    if (owned_vector) {
      adapter_->Add(owned_vector, id);
      return true;
    }
  } else {
    auto borrowed_vector = std::get<BorrowedFtVector>(*vector_ptr);
    if (borrowed_vector) {
      // We requires 4-byte aligned memory for vectors. Borrowing vectors is used with HSET objects
      // but if we need to copy vector data we should also make copy of vector that is going to be
      // added to avoid potential issues with unaligned memory access.
      if (copy_vector_) {
        std::vector<float> vector_copy(dim_);
        std::memcpy(vector_copy.data(), borrowed_vector, dim_ * sizeof(float));
        adapter_->Add(vector_copy.data(), id);
      } else {
        adapter_->Add(borrowed_vector, id);
      }
      return true;
    }
  }

  // For HnswVectorIndex if we didn't add vector to index we should return false. Compared to
  // other in-shard index implementations where returning false removes document here we only
  // control if key should or shouldn't be ommited from defragmentation process.
  return false;
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

HnswIndexMetadata HnswVectorIndex::GetMetadata() const {
  return adapter_->GetMetadata();
}

size_t HnswVectorIndex::GetNodeCount() const {
  return adapter_->GetNodeCount();
}

std::vector<HnswNodeData> HnswVectorIndex::GetNodesRange(size_t start, size_t end) const {
  return adapter_->GetNodesRange(start, end);
}

}  // namespace dfly::search
