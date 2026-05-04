// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/strings/match.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include "base/logging.h"
#include "core/search/hnsw_alg.h"
#include "core/search/mrmw_mutex.h"
#include "core/search/vector_utils.h"

ABSL_DECLARE_FLAG(bool, hnsw_sq8_quantization);

namespace rng = std::ranges;

namespace dfly::search {

using namespace std;

namespace {

// Helper to select distance function based on similarity metric
template <typename FuncType>
FuncType DistFunc(VectorSimilarity sim, FuncType l2_func, FuncType cos_func, FuncType ip_func) {
  if (sim == VectorSimilarity::L2) {
    return l2_func;
  } else if (sim == VectorSimilarity::COSINE) {
    return cos_func;
  } else {
    return ip_func;
  }
}

class HnswSpace : public QuantizedSpaceInterface<float> {
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

  size_t get_data_size() override {
    return dim_ * sizeof(float);
  }

  hnswlib::DISTFUNC<float> get_dist_func() override {
    return DistFunc(sim_, L2DistanceStatic, CosineDistanceStatic, IPDistanceStatic);
  }

  hnswlib::DISTFUNC<float> get_node_dist_func() override {
    return DistFunc(sim_, L2DistanceStatic, CosineDistanceStatic, IPDistanceStatic);
  }

  void* get_dist_func_param() override {
    return &dim_;
  }

  QuantizedSpaceInterface<float>::STOREFUNC get_store_func() override {
    return [](void* dest, const void* src, size_t data_size) { memcpy(dest, src, data_size); };
  }

  void cleanup() override {
  }
};

// Per-thread state for SQ8 ADC distance functions.
struct TLSQ8ADCState {
  const float* vector_ = nullptr;  // Pointer to the current query vector.
  float norm_ = 0.0f;  // Precomputed norm of the vector (for L2) or its magnitude (for Cosine).
};

thread_local TLSQ8ADCState tl_sq8_adc_state;

class HnswSpaceSQ8ADC : public QuantizedSpaceInterface<float> {
 private:
  size_t dim_;
  size_t meta_offset_;
  size_t data_size_;
  VectorSimilarity sim_;

  static void InitSearchVectorNormCache(const float* u, size_t dim, VectorSimilarity sim) {
    if (u == tl_sq8_adc_state.vector_) {
      return;
    }
    tl_sq8_adc_state.vector_ = u;
    float dot = DotProductF32(u, u, dim);
    tl_sq8_adc_state.norm_ = (sim == VectorSimilarity::COSINE) ? std::sqrt(dot) : dot;
  }

  // ADC: L2 - float32 / int8 vector
  static float L2DistanceADC(const void* u_void, const void* v_void, const void* params_void) {
    const auto* p = reinterpret_cast<const HnswSpaceSQ8ADC*>(params_void);
    const auto* u = static_cast<const float*>(u_void);
    const auto* v = static_cast<const int8_t*>(v_void);
    const float* meta_v = reinterpret_cast<const float*>(v + p->meta_offset_);

    InitSearchVectorNormCache(u, p->dim_, VectorSimilarity::L2);
    float dot = DotProductF32I8(u, v, p->dim_);
    return tl_sq8_adc_state.norm_ - 2.0f * meta_v[0] * dot + meta_v[1] * meta_v[1];
  }

  // SDC: L2 - int8 / int8 vector
  static float L2DistanceSDC(const void* u_void, const void* v_void, const void* params_void) {
    const auto* p = reinterpret_cast<const HnswSpaceSQ8ADC*>(params_void);
    const auto* u = static_cast<const int8_t*>(u_void);
    const auto* v = static_cast<const int8_t*>(v_void);
    const float* meta_u = reinterpret_cast<const float*>(u + p->meta_offset_);
    const float* meta_v = reinterpret_cast<const float*>(v + p->meta_offset_);

    float dot = DotProductI8(u, v, p->dim_);
    return meta_u[1] * meta_u[1] - 2.0f * meta_u[0] * meta_v[0] * dot + meta_v[1] * meta_v[1];
  }

  // ADC: IP - float32 / int8 vector.
  static float IPDistanceADC(const void* u_void, const void* v_void, const void* params_void) {
    const auto* p = reinterpret_cast<const HnswSpaceSQ8ADC*>(params_void);
    const auto* u = static_cast<const float*>(u_void);
    const auto* v = static_cast<const int8_t*>(v_void);
    const float* meta_v = reinterpret_cast<const float*>(v + p->meta_offset_);

    float dot = DotProductF32I8(u, v, p->dim_);
    return 1.0f - meta_v[0] * dot;
  }

  // SDC: IP - int8 / int8 vector.
  static float IPDistanceSDC(const void* u_void, const void* v_void, const void* params_void) {
    const auto* p = reinterpret_cast<const HnswSpaceSQ8ADC*>(params_void);
    const auto* u = static_cast<const int8_t*>(u_void);
    const auto* v = static_cast<const int8_t*>(v_void);
    const float* meta_u = reinterpret_cast<const float*>(u + p->meta_offset_);
    const float* meta_v = reinterpret_cast<const float*>(v + p->meta_offset_);

    float dot = DotProductI8(u, v, p->dim_);
    return 1.0f - meta_u[0] * meta_v[0] * dot;
  }

  // ADC: COS - float32 / int8 vector.
  static float CosineDistanceADC(const void* u_void, const void* v_void, const void* params_void) {
    const auto* p = reinterpret_cast<const HnswSpaceSQ8ADC*>(params_void);
    const auto* u = static_cast<const float*>(u_void);
    const auto* v = static_cast<const int8_t*>(v_void);
    const float* meta_v = reinterpret_cast<const float*>(v + p->meta_offset_);

    InitSearchVectorNormCache(u, p->dim_, VectorSimilarity::COSINE);

    float dot = DotProductF32I8(u, v, p->dim_);
    float denom = tl_sq8_adc_state.norm_ * meta_v[1];
    return denom < 1e-9f ? 1.0f : 1.0f - (meta_v[0] * dot) / denom;
  }

  // SDC: COS - int8 / int8 vector.
  static float CosineDistanceSDC(const void* u_void, const void* v_void, const void* params_void) {
    const auto* p = reinterpret_cast<const HnswSpaceSQ8ADC*>(params_void);
    const auto* u = static_cast<const int8_t*>(u_void);
    const auto* v = static_cast<const int8_t*>(v_void);
    const float* meta_u = reinterpret_cast<const float*>(u + p->meta_offset_);
    const float* meta_v = reinterpret_cast<const float*>(v + p->meta_offset_);

    float dot = DotProductI8(u, v, p->dim_);
    float denom = meta_u[1] * meta_v[1];
    return denom < 1e-9f ? 1.0f : 1.0f - (meta_u[0] * meta_v[0] * dot) / denom;
  }

 public:
  explicit HnswSpaceSQ8ADC(size_t dim, VectorSimilarity sim) : sim_(sim) {
    dim_ = dim;
    meta_offset_ = dim_ + ((4 - dim_ % 4) % 4);  // Align vector to 4 bytes.
    data_size_ = meta_offset_ + 2 * sizeof(float);
  }

  hnswlib::DISTFUNC<float> get_dist_func() override {
    return DistFunc(sim_, L2DistanceADC, CosineDistanceADC, IPDistanceADC);
  }

  hnswlib::DISTFUNC<float> get_node_dist_func() override {
    return DistFunc(sim_, L2DistanceSDC, CosineDistanceSDC, IPDistanceSDC);
  }

  void* get_dist_func_param() override {
    return this;
  }

  size_t get_data_size() override {
    return data_size_;
  }

  size_t get_dim() const {
    return dim_;
  }

  QuantizedSpaceInterface<float>::STOREFUNC get_store_func() override {
    return [this](void* dest, const void* src, size_t /*data_size*/) {
      const float* vec = static_cast<const float*>(src);
      int8_t* qvec = static_cast<int8_t*>(dest);

      // meta_offset_ is aligned to 4 bytes, so this float* is always properly aligned.
      float* meta = reinterpret_cast<float*>(qvec + meta_offset_);

      float abs_max = 0.0f, norm_sq = 0.0f;
      for (size_t i = 0; i < dim_; i++) {
        float x = vec[i];
        abs_max = std::max(abs_max, std::abs(x));
        norm_sq += x * x;
      }

      float scale = (abs_max > 1e-9f) ? (127.0f / abs_max) : 0.0f;

      for (size_t i = 0; i < dim_; i++) {
        qvec[i] = static_cast<int8_t>(
            std::clamp(static_cast<int>(std::round(vec[i] * scale)), -127, 127));
      }

      // Zero padding bytes between quantized data and float metadata.
      if (meta_offset_ > dim_) {
        memset(qvec + dim_, 0, meta_offset_ - dim_);
      }

      // meta[0]: scale multiplier
      // meta[1]: L2 norm of the original vector
      meta[0] = (abs_max > 1e-9f) ? (abs_max / 127.0f) : 0.0f;
      meta[1] = std::sqrt(norm_sq);
    };
  }

  void cleanup() override {
    tl_sq8_adc_state.vector_ = nullptr;
    tl_sq8_adc_state.norm_ = 0.0f;
  }
};

std::unique_ptr<QuantizedSpaceInterface<float>> CreateHnswSpace(
    const SchemaField::VectorParams& params) {
  if (absl::GetFlag(FLAGS_hnsw_sq8_quantization)) {
    return std::make_unique<HnswSpaceSQ8ADC>(params.dim, params.sim);
  } else {
    return std::make_unique<HnswSpace>(params.dim, params.sim);
  }
}

}  // namespace

// TODO: to replace it and use HierarchicalNSW directly.
struct HnswlibAdapter {
  // Default setting of hnswlib/hnswalg
  constexpr static size_t kDefaultEfRuntime = 10;

  explicit HnswlibAdapter(const SchemaField::VectorParams& params, bool store_vector)
      : space_{CreateHnswSpace(params)},
        world_{space_.get(),  params.capacity, params.hnsw_m, params.hnsw_ef_construction,
               100 /* seed*/, store_vector},
        store_vector_{store_vector},
        data_size_{params.dim * sizeof(float)},
        stub_vector_(data_size_ / sizeof(float), 1.0f) {
  }

  void Add(const void* data, GlobalDocId id) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    DoAdd(data, id);
  }

  void Remove(GlobalDocId id) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    DoRemove(id);
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    world_.setEf(ef.value_or(kDefaultEfRuntime));
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    absl::Cleanup cleanup = [this] { space_->cleanup(); };
    return QueueToVec(world_.searchKnn(target, k));
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                       const vector<GlobalDocId>& allowed) {
    struct BinsearchFilter : hnswlib::BaseFilterFunctor {
      virtual bool operator()(hnswlib::labeltype id) {
        return binary_search(allowed->begin(), allowed->end(), id);
      }

      explicit BinsearchFilter(const vector<GlobalDocId>* allowed) : allowed{allowed} {
      }
      const vector<GlobalDocId>* allowed;
    };

    world_.setEf(ef.value_or(kDefaultEfRuntime));
    BinsearchFilter filter{&allowed};
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    absl::Cleanup cleanup = [this] { space_->cleanup(); };
    return QueueToVec(world_.searchKnn(target, k, &filter));
  }

  // Brute-force KNN search over a specific subset of documents.
  // Computes distances for all provided document IDs and returns the k nearest neighbors.
  vector<pair<float, GlobalDocId>> SubsetKnn(float* target, size_t k,
                                             const vector<GlobalDocId>& docs) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    absl::Cleanup cleanup = [this] { space_->cleanup(); };
    return QueueToVec(world_.subsetKnnSearch(target, k, docs));
  }

  // Returns all documents within the given radius, with their distances.
  // Uses dynamic-range exploration (searchRange) to correctly handle cases where
  // the entry point is farther than radius.
  vector<pair<float, GlobalDocId>> RangeSearch(float* target, float radius) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    absl::Cleanup cleanup = [this] { space_->cleanup(); };
    return world_.searchRange(target, radius);
  }

  HnswIndexMetadata GetMetadata() const {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    HnswIndexMetadata metadata;
    metadata.enterpoint_node = world_.enterpoint_node_;
    return metadata;
  }

  int GetMaxLevel() const {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    return world_.maxlevel_;
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
  // Actually add the point. Must be called while holding mrmw write lock.
  void DoAdd(const void* data, GlobalDocId id) {
    absl::Cleanup add_cleanup = [this] { space_->cleanup(); };
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
    auto it = store_vector_ ? world_.label_lookup_.end() : world_.label_lookup_.find(id);

    HnswErrorStatus status = world_.markDelete(id);
    if (status != HnswErrorStatus::SUCCESS) {
      VLOG(1) << "HnswlibAdapter::Remove failed with status: " << static_cast<int>(status)
              << " for global id: " << id;
      return;
    }

    // In borrowed mode the node stays in the graph after markDelete and
    // traversal still computes distances for it.  Replace the external
    // pointer with stub_vector_ so the caller can free the original data.
    // Uses 1.0f (not zero) because CosineDistance(v, 0) = 0 would bias
    // traversal toward deleted nodes.
    if (it != world_.label_lookup_.end()) {
      const char* safe_ptr = reinterpret_cast<const char*>(stub_vector_.data());
      char* ptr_location = world_.getDataPtrByInternalId(it->second);
      memcpy(ptr_location, reinterpret_cast<const void*>(&safe_ptr), sizeof(void*));
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
  // Restore HNSW graph structure from serialized nodes with metadata.
  // Returns false if the input is inconsistent (e.g. entry point not in node set) —
  // caller should fall back to rebuilding the index from the keyspace.
  bool RestoreFromNodes(const std::vector<HnswNodeData>& nodes, const HnswIndexMetadata& metadata) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    absl::WriterMutexLock resize_lock(&resize_mutex_);

    if (nodes.empty()) {
      return true;
    }

    // RestoreFromNodes is only called during deserialization on a freshly created index.
    // Assert the index is empty to prevent memory leaks from double-allocation of linkLists_.
    DCHECK_EQ(world_.cur_element_count.load(), 0u)
        << "RestoreFromNodes should only be called on an empty index during deserialization";

    // hnswlib pairs enterpoint_node_ with maxlevel_; node levels are immutable after
    // creation, so the entry point's level in the serialized set equals the live
    // maxlevel at metadata capture. max(node.level) would risk OOB reads when a
    // concurrent Add raised maxlevel between capture and node serialization.
    size_t max_internal_id = 0;
    int entrypoint_level = -1;
    for (const auto& node : nodes) {
      max_internal_id = std::max<size_t>(max_internal_id, node.internal_id);
      if (node.internal_id == metadata.enterpoint_node)
        entrypoint_level = node.level;
    }
    if (entrypoint_level < 0) {
      LOG(ERROR) << "HNSW restore: entry point internal_id=" << metadata.enterpoint_node
                 << " not present in serialized node set (" << nodes.size()
                 << " nodes); skipping restore — index will be rebuilt from the keyspace";
      return false;
    }
    if (world_.max_elements_ < max_internal_id + 1) {
      world_.resizeIndex(max_internal_id + 1);
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
        rng::copy(node.levels_links[0], links0);
      }

      // Restore links for upper layers
      for (int lvl = 1; lvl <= node.level && lvl < static_cast<int>(node.levels_links.size());
           ++lvl) {
        auto* ll = world_.get_linklist(internal_id, lvl);
        world_.setListCount(ll, node.levels_links[lvl].size());
        auto* links = reinterpret_cast<uint32_t*>(ll + 1);
        rng::copy(node.levels_links[lvl], links);
      }

      // Track restored count so markDeletedInternal can validate internal_id bounds.
      world_.cur_element_count.store(++restored_count);

      // Mark node as deleted until UpdateVectorData provides valid vector data.
      world_.markDeletedInternal(internal_id);

      // In borrowed mode, deleted nodes are still traversed by addPoint.
      // Point to stub_vector_ so distance computations don't dereference nullptr.
      if (!store_vector_) {
        const char* safe_ptr = reinterpret_cast<const char*>(stub_vector_.data());
        char* ptr_location = world_.getDataPtrByInternalId(internal_id);
        memcpy(ptr_location, reinterpret_cast<const void*>(&safe_ptr), sizeof(void*));
      }
    }

    // Set the metadata for the graph
    world_.maxlevel_ = entrypoint_level;
    world_.enterpoint_node_ = metadata.enterpoint_node;

    VLOG(1) << "Restored HNSW index with " << restored_count
            << " nodes, maxlevel=" << entrypoint_level
            << ", enterpoint=" << metadata.enterpoint_node;
    return true;
  }

  // Update vector data for an existing node (used after RestoreFromNodes).
  // Returns false if the node doesn't exist in the index.
  bool UpdateVectorData(GlobalDocId id, const void* data) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);

    // Find the internal id for this label
    auto it = world_.label_lookup_.find(id);
    if (it == world_.label_lookup_.end()) {
      VLOG(1) << "UpdateVectorData: label " << id << " not found in index";
      return false;
    }

    size_t internal_id = it->second;

    // Copy/store the vector data based on copy_vector_ mode
    if (world_.copy_vector_) {
      char* data_ptr = world_.data_vector_memory_ + internal_id * world_.data_size_;
      world_.store_func_(data_ptr, data, world_.data_size_);
    } else {
      // Borrowed mode: store pointer to external data
      char* ptr_location = world_.getDataPtrByInternalId(internal_id);
      memcpy(ptr_location, reinterpret_cast<const void*>(&data), sizeof(void*));
    }

    // Unmark deleted so the node participates in KNN searches now that it
    // has valid vector data. During RestoreFromNodes all nodes are marked
    // deleted by default to prevent dereferencing uninitialised data.
    if (world_.isMarkedDeleted(internal_id)) {
      world_.unmarkDeletedInternal(internal_id);
    }
    return true;
  }

  MRMWMutexLock GetReadLock() const {
    return MRMWMutexLock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
  }

  size_t GetMemoryUsage() const {
    return world_.memorySize();
  }

 private:
  std::unique_ptr<QuantizedSpaceInterface<float>> space_;
  HierarchicalNSW<float> world_;
  absl::Mutex resize_mutex_;
  mutable MRMWMutex mrmw_mutex_;

  bool store_vector_;               // Whether vectors are copied into hnswlib.
  size_t data_size_;                // Byte size of a single vector (float32 of the original input).
  std::vector<float> stub_vector_;  // Non-zero data for deleted nodes in borrowed mode.
};

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, bool store_vector,
                                 PMR_NS::memory_resource* /*mr*/)
    : store_vector_(store_vector),
      dim_{params.dim},
      adapter_{make_unique<HnswlibAdapter>(params, store_vector)} {
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

std::vector<std::pair<float, GlobalDocId>> HnswVectorIndex::SubsetKnn(
    float* target, size_t k, const std::vector<GlobalDocId>& docs) const {
  return adapter_->SubsetKnn(target, k, docs);
}

std::vector<std::pair<float, GlobalDocId>> HnswVectorIndex::RangeQuery(float* target,
                                                                       float radius) const {
  return adapter_->RangeSearch(target, radius);
}

void HnswVectorIndex::Remove(GlobalDocId id) {
  adapter_->Remove(id);
}

HnswIndexMetadata HnswVectorIndex::GetMetadata() const {
  return adapter_->GetMetadata();
}

int HnswVectorIndex::GetMaxLevel() const {
  return adapter_->GetMaxLevel();
}

size_t HnswVectorIndex::GetNodeCount() const {
  return adapter_->GetNodeCount();
}

std::vector<HnswNodeData> HnswVectorIndex::GetNodesRange(size_t start, size_t end) const {
  return adapter_->GetNodesRange(start, end);
}

bool HnswVectorIndex::RestoreFromNodes(const std::vector<HnswNodeData>& nodes,
                                       const HnswIndexMetadata& metadata) {
  return adapter_->RestoreFromNodes(nodes, metadata);
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

  return adapter_->UpdateVectorData(id, data);
}

MRMWMutexLock HnswVectorIndex::GetReadLock() const {
  return adapter_->GetReadLock();
}

size_t HnswVectorIndex::GetMemoryUsage() const {
  return adapter_->GetMemoryUsage();
}

}  // namespace dfly::search
