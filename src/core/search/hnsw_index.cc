// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/strings/match.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include <mutex>
#include <shared_mutex>

#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/hnsw_alg.h"
#include "core/search/mrmw_mutex.h"
#include "core/search/vector_utils.h"
#include "util/fibers/synchronization.h"

#define USEARCH_USE_SIMSIMD WITH_SIMSIMD
#define USEARCH_USE_FP16LIB 1
#define USEARCH_USE_OPENMP 0

#include "core/search/usearch/index.hpp"
#include "core/search/usearch/index_dense.hpp"

namespace dfly::search {

using namespace std;

struct HnswlibAdapter {
  // Default setting of hnswlib/hnswalg
  constexpr static size_t kDefaultEfRuntime = 10;
  constexpr static size_t kIndexCapacitySize = 2 << 14;

  explicit HnswlibAdapter(const SchemaField::VectorParams& params) : index_size_(0) {
    auto similarity_metric = [](VectorSimilarity sim) {
      switch (sim) {
        case dfly::search::VectorSimilarity::COSINE:
          return unum::usearch::metric_kind_t::cos_k;
        case dfly::search::VectorSimilarity::IP:
          return unum::usearch::metric_kind_t::ip_k;
        case dfly::search::VectorSimilarity::L2:
          return unum::usearch::metric_kind_t::l2sq_k;
        default:
          LOG(FATAL) << "Unknown metric kind";
      }
    };

    unum::usearch::metric_punned_t metric(params.dim, similarity_metric(params.sim),
                                          unum::usearch::scalar_kind_t::f32_k);

    unum::usearch::index_dense_config_t config =
        unum::usearch::index_dense_config_t{params.hnsw_m, params.hnsw_ef_construction};
    config.connectivity_base = params.hnsw_m * 2;
    // We dont need to do key lookups (id -> vector) in the index.
    config.enable_key_lookups = false;

    index_ = unum::usearch::index_dense_gt<GlobalDocId>::make(metric, config);

    // What about thread number here ?!
    index_.reserve(kIndexCapacitySize);
  }

  void Add(const float* data, GlobalDocId id) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    // Check if we need to resize the index
    // We keep the size of the index in a separate atomic to avoid
    // locking exclusively when checking
    bool needs_resize = false;
    {
      std::shared_lock lk(mutex_);
      if (index_size_.fetch_add(1) + 1 > index_.capacity()) {
        needs_resize = true;
      }
    }

    if (needs_resize) {
      std::unique_lock ln(mutex_);
      // Do we still need to resize?
      // Another thread might have resized it already
      auto size = index_size_.load();
      if (size > index_.capacity()) {
        index_.reserve(size * 2);
      }
    }

    std::shared_lock lk(mutex_);
    index_.add(id, data);
  }

  void Remove(GlobalDocId id) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
    std::shared_lock lk(mutex_);
    index_.remove(id);
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
    std::shared_lock lk(mutex_);
    std::vector<pair<float, GlobalDocId>> results;
    auto search_result = index_.search(target, k, unum::usearch::index_dense_t::any_thread(), false,
                                       ef.value_or(kDefaultEfRuntime));
    for (std::size_t i = 0; i != search_result.size(); ++i)
      results.emplace_back(search_result[i].distance, search_result[i].member.key);
    return results;
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                       const vector<GlobalDocId>& allowed) {
    // to implement
    return {};
  }

 private:
  util::fb2::SharedMutex mutex_;
  std::atomic<size_t> index_size_;
  unum::usearch::index_dense_t index_;
  mutable MRMWMutex mrmw_mutex_;
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
