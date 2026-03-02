// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/container/flat_hash_map.h>
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
      : space_{params.dim, params.sim},
        world_{&space_,       params.capacity, params.hnsw_m, params.hnsw_ef_construction,
               100 /* seed*/, copy_vector},
        copy_vector_{copy_vector},
        data_size_{params.dim * sizeof(float)} {
  }

  // Adds a point to the index. If the write lock cannot be acquired (e.g.
  // serialization holds a read lock), the operation is deferred and will be
  // replayed by a subsequent write or TryProcessDeferred() call.
  // When copy_vector_ is false the index stores a raw pointer to external data,
  // so we must add the point synchronously before the caller's pointer goes out
  // of scope — use a blocking write lock in that case.
  void Add(const void* data, GlobalDocId id) {
    if (copy_vector_) {
      {
        MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock, std::try_to_lock);
        if (lock.locked()) {
          ProcessDeferred();
          DoAdd(data, id);
          return;
        }
      }
      // Could not acquire write lock — defer the operation.
      AddDeferredOp(id, DeferredOp(true, data, data_size_, /*copy=*/true));
      TryProcessDeferred();
    } else {
      MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
      ProcessDeferred();
      DoAdd(data, id);
    }
  }

  // Removes a point from the index. If the write lock cannot be acquired, the
  // operation is deferred.
  void Remove(GlobalDocId id) {
    {
      MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock, std::try_to_lock);
      if (lock.locked()) {
        ProcessDeferred();
        DoRemove(id);
        return;
      }
    }
    AddDeferredOp(id, DeferredOp(false, nullptr, 0, false));
    TryProcessDeferred();
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    TryProcessDeferred();
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

    TryProcessDeferred();
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
    return metadata;
  }

  void SetMetadata(const HnswIndexMetadata& metadata) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    absl::WriterMutexLock resize_lock(&resize_mutex_);

    // SetMetadata is only called during deserialization before the index is used.
    // Assert the index is empty to ensure no concurrent operations are possible.
    DCHECK_EQ(world_.cur_element_count.load(), 0u)
        << "SetMetadata should only be called on an empty index during deserialization";

    // Runtime check for release builds to prevent silent corruption
    if (world_.cur_element_count.load() != 0) {
      LOG(ERROR) << "SetMetadata called on non-empty HNSW index with "
                 << world_.cur_element_count.load() << " elements, ignoring";
      return;
    }

    // Pre-allocate capacity based on expected element count, but don't set cur_element_count.
    // cur_element_count will be set by RestoreFromNodes when the actual nodes are restored.
    if (world_.max_elements_ < metadata.cur_element_count) {
      world_.resizeIndex(metadata.cur_element_count);
    }
    // Note: Don't set cur_element_count here - RestoreFromNodes will set it after restoring nodes.
  }

  size_t GetNodeCount() const {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    return world_.cur_element_count.load();
  }

  std::vector<HnswNodeData> GetNodesRange(size_t start, size_t end) const {
    DCHECK(mrmw_mutex_.IsReadLocked());
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
  // A single deferred Add or Remove operation.
  struct DeferredOp {
    bool is_add;
    bool owns_data;        // If true, data_ptr was allocated by us and must be freed.
    const void* data_ptr;  // Pointer to vector data (owned or borrowed).

    DeferredOp(bool is_add, const void* data, size_t data_size, bool copy)
        : is_add(is_add), owns_data(copy && data != nullptr) {
      if (owns_data) {
        void* buf = mi_malloc(data_size);
        memcpy(buf, data, data_size);
        data_ptr = buf;
      } else {
        data_ptr = data;
      }
    }

    ~DeferredOp() {
      if (owns_data)
        mi_free(const_cast<void*>(data_ptr));
    }

    DeferredOp(DeferredOp&& o) noexcept
        : is_add(o.is_add), owns_data(o.owns_data), data_ptr(o.data_ptr) {
      o.owns_data = false;
      o.data_ptr = nullptr;
    }

    DeferredOp& operator=(DeferredOp&& o) noexcept {
      auto lhs = std::tie(is_add, owns_data, data_ptr);
      auto rhs = std::tie(o.is_add, o.owns_data, o.data_ptr);
      std::swap(lhs, rhs);
      return *this;
    }

    DeferredOp(const DeferredOp&) = delete;
    DeferredOp& operator=(const DeferredOp&) = delete;
  };

  // Actually add the point. Must be called while holding mrmw write lock.
  void DoAdd(const void* data, GlobalDocId id) {
    while (true) {
      try {
        absl::ReaderMutexLock resize_lock(&resize_mutex_);
        world_.addPoint(data, id);
        return;
      } catch (const std::exception& e) {
        std::string error_msg = e.what();
        if (absl::StrContains(error_msg, "The number of elements exceeds the specified limit")) {
          ResizeIfFull();
          continue;
        }
        LOG(ERROR) << "HnswlibAdapter::DoAdd exception: " << e.what();
        return;
      }
    }
  }

  void DoRemove(GlobalDocId id) {
    HnswErrorStatus status = world_.markDelete(id);
    if (status != HnswErrorStatus::SUCCESS) {
      VLOG(1) << "HnswlibAdapter::Remove failed with status: " << static_cast<int>(status)
              << " for global id: " << id;
    }
  }

  // Add a deferred operation, replacing any previous one for the same document.
  void AddDeferredOp(GlobalDocId id, DeferredOp op) {
    std::lock_guard g(deferred_mu_);
    deferred_ops_.insert_or_assign(id, std::move(op));
  }

  // Take all deferred operations out of the queue.
  absl::flat_hash_map<GlobalDocId, DeferredOp> TakeDeferredOps() {
    std::lock_guard g(deferred_mu_);
    absl::flat_hash_map<GlobalDocId, DeferredOp> ops;
    ops.swap(deferred_ops_);
    return ops;
  }

  // Drain the deferred operations queue. Must be called while holding the mrmw
  // write lock.  Only copy_vector_=true adds and removes can be deferred, so
  // ordering within the queue does not matter.
  void ProcessDeferred() {
    auto ops = TakeDeferredOps();
    for (auto& [id, op] : ops) {
      if (op.is_add) {
        DoAdd(op.data_ptr, id);
      } else {
        DoRemove(id);
      }
    }
  }

  // Non-blocking attempt to drain the deferred queue.
  void TryProcessDeferred() {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock, std::try_to_lock);
    if (lock.locked()) {
      ProcessDeferred();
    }
  }

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

 public:
  // Restore HNSW graph structure from serialized nodes with metadata
  void RestoreFromNodes(const std::vector<HnswNodeData>& nodes, const HnswIndexMetadata& metadata) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    absl::WriterMutexLock resize_lock(&resize_mutex_);

    if (nodes.empty()) {
      return;
    }

    // RestoreFromNodes is only called during deserialization on a freshly created index.
    // Assert the index is empty to prevent memory leaks from double-allocation of linkLists_.
    DCHECK_EQ(world_.cur_element_count.load(), 0u)
        << "RestoreFromNodes should only be called on an empty index during deserialization";

    // Ensure we have enough capacity
    size_t required_capacity = metadata.cur_element_count;
    if (world_.max_elements_ < required_capacity) {
      world_.resizeIndex(required_capacity);
    }

    // Restore each node - directly set up memory and fields
    size_t restored_count = 0;

    for (const auto& node : nodes) {
      size_t internal_id = node.internal_id;

      // Validate internal_id is within bounds - invalid internal_id indicates corrupted data
      CHECK(internal_id < world_.max_elements_);

      // Register label in lookup table
      world_.label_lookup_[node.global_id] = internal_id;

      // Set the level
      world_.element_levels_[internal_id] = node.level;

      // Clear level 0 memory and set label.
      // Memory layout: each element occupies size_data_per_element_ bytes starting at
      // data_level0_memory_ + internal_id * size_data_per_element_.
      // offsetLevel0_ is always 0, so we clear exactly one element's worth of data.
      // This matches the pattern in hnswlib's addPoint().
      memset(world_.data_level0_memory_ + internal_id * world_.size_data_per_element_, 0,
             world_.size_data_per_element_);
      world_.setExternalLabel(internal_id, node.global_id);

      // In copy mode, zero the vector memory so distance computations don't use
      // uninitialized data for nodes that are marked deleted.
      if (world_.copy_vector_) {
        char* data_ptr = world_.data_vector_memory_ + internal_id * world_.data_size_;
        memset(data_ptr, 0, world_.data_size_);
      }

      // Allocate upper layer links if needed
      if (node.level > 0) {
        world_.linkLists_[internal_id] =
            (char*)mi_malloc(world_.size_links_per_element_ * node.level + 1);
        memset(world_.linkLists_[internal_id], 0, world_.size_links_per_element_ * node.level + 1);
      }

      // Restore links for layer 0
      if (!node.levels_links.empty()) {
        auto* ll0 = world_.get_linklist0(internal_id);
        world_.setListCount(ll0, node.levels_links[0].size());
        auto* links0 = reinterpret_cast<uint32_t*>(ll0 + 1);
        std::copy(node.levels_links[0].begin(), node.levels_links[0].end(), links0);
      }

      // Restore links for upper layers
      for (int lvl = 1; lvl <= node.level && lvl < static_cast<int>(node.levels_links.size());
           ++lvl) {
        auto* ll = world_.get_linklist(internal_id, lvl);
        world_.setListCount(ll, node.levels_links[lvl].size());
        auto* links = reinterpret_cast<uint32_t*>(ll + 1);
        std::copy(node.levels_links[lvl].begin(), node.levels_links[lvl].end(), links);
      }

      // Track restored count so markDeletedInternal can validate internal_id bounds.
      world_.cur_element_count.store(++restored_count);

      // Mark node as deleted until UpdateVectorData provides valid vector data.
      // This prevents crashes from dereferencing uninitialised data pointers
      // (especially in borrowed-vector mode).
      world_.markDeletedInternal(internal_id);
    }

    // Set the metadata for the graph
    world_.maxlevel_ = metadata.maxlevel;
    world_.enterpoint_node_ = metadata.enterpoint_node;

    VLOG(1) << "Restored HNSW index with " << restored_count
            << " nodes, maxlevel=" << metadata.maxlevel
            << ", enterpoint=" << metadata.enterpoint_node;
  }

  // Update vector data for an existing node (used after RestoreFromNodes)
  void UpdateVectorData(GlobalDocId id, const void* data) {
    TryProcessDeferred();
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);

    // Find the internal id for this label
    auto it = world_.label_lookup_.find(id);
    if (it == world_.label_lookup_.end()) {
      LOG(WARNING) << "UpdateVectorData: label " << id << " not found in index";
      return;
    }

    size_t internal_id = it->second;

    // Copy/store the vector data based on copy_vector_ mode
    if (world_.copy_vector_) {
      // Owned mode: copy data into world's vector memory
      char* data_ptr = world_.data_vector_memory_ + internal_id * world_.data_size_;
      memcpy(data_ptr, data, world_.data_size_);
    } else {
      // Borrowed mode: store pointer to external data
      char* ptr_location = world_.getDataPtrByInternalId(internal_id);
      memcpy(ptr_location, &data, sizeof(void*));
    }

    // Unmark deleted so the node participates in KNN searches now that it
    // has valid vector data. During RestoreFromNodes all nodes are marked
    // deleted by default to prevent dereferencing uninitialised data.
    if (world_.isMarkedDeleted(internal_id)) {
      world_.unmarkDeletedInternal(internal_id);
    }
  }

  std::unique_ptr<MRMWMutexLock> GetReadLock() const {
    return std::make_unique<MRMWMutexLock>(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
  }

 private:
  HnswSpace space_;
  HierarchicalNSW<float> world_;
  absl::Mutex resize_mutex_;
  mutable MRMWMutex mrmw_mutex_;

  bool copy_vector_;                    // Whether vectors are copied into hnswlib.
  size_t data_size_;                    // Byte size of a single vector.
  mutable base::SpinLock deferred_mu_;  // Protects deferred_ops_.
  absl::flat_hash_map<GlobalDocId, DeferredOp> deferred_ops_;  // GUARDED_BY(deferred_mu_)
};

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, bool copy_vector,
                                 PMR_NS::memory_resource*)
    : copy_vector_(copy_vector),
      dim_{params.dim},
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

  const void* data = nullptr;
  if (std::holds_alternative<OwnedFtVector>(*vector_ptr)) {
    data = std::get<OwnedFtVector>(*vector_ptr).first.get();
  } else {
    data = std::get<BorrowedFtVector>(*vector_ptr);
  }

  if (!data) {
    return false;
  }

  adapter_->Add(data, id);
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

HnswIndexMetadata HnswVectorIndex::GetMetadata() const {
  return adapter_->GetMetadata();
}

void HnswVectorIndex::SetMetadata(const HnswIndexMetadata& metadata) {
  adapter_->SetMetadata(metadata);
}

size_t HnswVectorIndex::GetNodeCount() const {
  return adapter_->GetNodeCount();
}

std::vector<HnswNodeData> HnswVectorIndex::GetNodesRange(size_t start, size_t end) const {
  return adapter_->GetNodesRange(start, end);
}

void HnswVectorIndex::RestoreFromNodes(const std::vector<HnswNodeData>& nodes,
                                       const HnswIndexMetadata& metadata) {
  adapter_->RestoreFromNodes(nodes, metadata);
}

bool HnswVectorIndex::UpdateVectorData(GlobalDocId id, const DocumentAccessor& doc,
                                       std::string_view field) {
  auto vector_ptr = doc.GetVector(field, dim_);
  if (!vector_ptr ||
      *vector_ptr == search::DocumentAccessor::VectorInfo(search::BorrowedFtVector(nullptr))) {
    // Document doesn't have the vector field - mark node as deleted to prevent
    // "ghost" nodes with invalid vector data from participating in searches
    LOG(WARNING) << "UpdateVectorData: document " << id
                 << " missing vector field, marking node as deleted in HNSW index";
    adapter_->Remove(id);
    return false;
  }

  const void* data = nullptr;
  if (std::holds_alternative<OwnedFtVector>(*vector_ptr)) {
    data = std::get<OwnedFtVector>(*vector_ptr).first.get();
  } else {
    data = std::get<BorrowedFtVector>(*vector_ptr);
  }

  adapter_->UpdateVectorData(id, data);
  return true;
}

std::unique_ptr<MRMWMutexLock> HnswVectorIndex::GetReadLock() const {
  return adapter_->GetReadLock();
}

}  // namespace dfly::search
