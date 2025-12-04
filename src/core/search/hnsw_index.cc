// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/hnsw_index.h"

#include <absl/strings/match.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include <mutex>

#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/hnsw_alg.h"
#include "core/search/mrmw_mutex.h"
#include "core/search/vector_utils.h"

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
  constexpr static size_t kIndexInitCapacitySize = 16384;

  explicit HnswlibAdapter(const SchemaField::VectorParams& params) : size_(0) {
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

    index_ = unum::usearch::index_dense_gt<GlobalDocId>::make(metric, config);

    index_.reserve(kIndexInitCapacitySize);
  }

  void Add(const float* data, GlobalDocId id) {
    size_.fetch_add(1);
    {
      absl::WriterMutexLock lk(&resize_mutex_);
      if (size_ == index_.capacity()) {
        index_.try_reserve(index_.capacity() * 2);
        MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kWriteLock);
      }
    }
    auto add_result = index_.add(id, data);
  }

  void Remove(GlobalDocId id) {
    index_.remove(id);
  }

  vector<pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
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
    MRMWMutexLock lock(&mrmw_mutex_, MRMWMutex::LockMode::kReadLock);
  }

 private:
  unum::usearch::index_dense_t index_;
  absl::Mutex resize_mutex_;
  std::atomic<size_t> size_;
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
