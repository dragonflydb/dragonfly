// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/global_vector_index.h"

#include <absl/strings/str_cat.h>

#include <shared_mutex>

#include "base/logging.h"
#include "core/search/indices.h"
#include "core/search/vector_utils.h"

namespace dfly {

using namespace std;
using namespace search;

GlobalVectorIndex::GlobalVectorIndex(const SchemaField::VectorParams& params,
                                     PMR_NS::memory_resource* mr)
    : params_(params) {
  // Create the appropriate vector index based on parameters
  if (params.use_hnsw) {
    vector_index_ = std::make_unique<HnswVectorIndex>(params, mr);
  } else {
    vector_index_ = std::make_unique<FlatVectorIndex>(params, mr);
  }
}

GlobalVectorIndex::~GlobalVectorIndex() = default;

std::vector<std::pair<float, uint64_t>> GlobalVectorIndex::Knn(float* target, size_t k,
                                                               std::optional<size_t> ef) const {
  std::vector<std::pair<float, uint64_t>> result;

  if (auto* hnsw_index = dynamic_cast<HnswVectorIndex*>(vector_index_.get())) {
    return hnsw_index->Knn(target, k, ef);
  } else if (auto* flat_index = dynamic_cast<FlatVectorIndex*>(vector_index_.get())) {
    // For flat index, we need to compute distances to all vectors
    // std::vector<std::pair<float, search::DocId>> distances;
    // auto [dim, sim] = vector_index_->Info();

    // for (const auto& [global_id, internal_id] : global_to_internal_) {
    //   const float* vec = flat_index->Get(internal_id);
    //   float dist = VectorDistance(target, vec, dim, sim);
    //   distances.emplace_back(dist, internal_id);
    // }

    // // Sort and take top k
    // size_t limit = std::min(k, distances.size());
    // std::partial_sort(distances.begin(), distances.begin() + limit, distances.end());
    // distances.resize(limit);

    // result.reserve(distances.size());
    // for (const auto& [distance, internal_id] : distances) {
    //   auto it = internal_to_global_.find(internal_id);
    //   if (it != internal_to_global_.end()) {
    //     result.emplace_back(distance, it->second);
    //   }
    // }
  }

  return result;
}

std::vector<std::pair<float, uint64_t>> GlobalVectorIndex::Knn(
    float* target, size_t k, std::optional<size_t> ef,
    const std::vector<GlobalDocId>& allowed) const {
  // // Convert allowed GlobalDocIds to internal DocIds
  // std::vector<search::DocId> allowed_internal;
  // allowed_internal.reserve(allowed.size());

  // for (const auto& global_id : allowed) {
  //   auto it = global_to_internal_.find(global_id);
  //   if (it != global_to_internal_.end()) {
  //     allowed_internal.push_back(it->second);
  //   }
  // }

  // std::sort(allowed_internal.begin(), allowed_internal.end());

  // std::vector<std::pair<float, GlobalDocId>> result;

  // if (auto* hnsw_index = dynamic_cast<HnswVectorIndex<uint64_t>*>(vector_index_.get())) {
  //   auto internal_results = hnsw_index->Knn(target, k, ef, allowed_internal);
  //   result.reserve(internal_results.size());

  //   for (const auto& [distance, internal_id] : internal_results) {
  //     auto it = internal_to_global_.find(internal_id);
  //     if (it != internal_to_global_.end()) {
  //       result.emplace_back(distance, it->second);
  //     }
  //   }
  // } else {
  //   // For flat index with filtering
  //   std::vector<std::pair<float, search::DocId>> distances;
  //   auto [dim, sim] = vector_index_->Info();

  //   for (search::DocId internal_id : allowed_internal) {
  //     if (auto* flat_index = dynamic_cast<FlatVectorIndex<uint64_t>*>(vector_index_.get())) {
  //       const float* vec = flat_index->Get(internal_id);
  //       float dist = VectorDistance(target, vec, dim, sim);
  //       distances.emplace_back(dist, internal_id);
  //     }
  //   }

  //   // Sort and take top k
  //   size_t limit = std::min(k, distances.size());
  //   std::partial_sort(distances.begin(), distances.begin() + limit, distances.end());
  //   distances.resize(limit);

  //   result.reserve(distances.size());
  //   for (const auto& [distance, internal_id] : distances) {
  //     auto it = internal_to_global_.find(internal_id);
  //     if (it != internal_to_global_.end()) {
  //       result.emplace_back(distance, it->second);
  //     }
  //   }
  // }

  // return result;

  return {};
}

std::pair<size_t, search::VectorSimilarity> GlobalVectorIndex::Info() const {
  return vector_index_->Info();
}

bool GlobalVectorIndex::AddVector(GlobalDocId global_id, const float* vector) {
  // Create mock document accessor for the vector index
  class VectorDocumentAccessor : public DocumentAccessor {
   public:
    VectorDocumentAccessor(const float* vec, size_t dim) : vector_(vec), dim_(dim) {
    }

    std::optional<StringList> GetStrings(std::string_view field) const override {
      return std::nullopt;
    }

    std::optional<VectorInfo> GetVector(std::string_view field) const override {
      if (!vector_)
        return VectorInfo{};

      auto ptr = std::make_unique<float[]>(dim_);
      std::memcpy(ptr.get(), vector_, dim_ * sizeof(float));
      return VectorInfo{std::move(ptr), dim_};
    }

    std::optional<NumsList> GetNumbers(std::string_view field) const override {
      return std::nullopt;
    }

    std::optional<StringList> GetTags(std::string_view field) const override {
      return std::nullopt;
    }

   private:
    const float* vector_;
    size_t dim_;
  };

  VectorDocumentAccessor doc_accessor(vector, params_.dim);

  // Add to vector index
  if (!vector_index_->Add(global_id.id, doc_accessor, "vector_field")) {
    return false;
  }

  VLOG(2) << "Added vector to global index: global_id={" << global_id.Shard() << ","
          << global_id.LocalDocId() << "}, internal_id=" << global_id.LocalDocId();

  return true;
}

void GlobalVectorIndex::RemoveVector(GlobalDocId global_id, std::string_view key) {
  // Create mock document accessor for removal
  class VectorDocumentAccessor : public DocumentAccessor {
   public:
    std::optional<StringList> GetStrings(std::string_view field) const override {
      return std::nullopt;
    }

    std::optional<VectorInfo> GetVector(std::string_view field) const override {
      return VectorInfo{};
    }

    std::optional<NumsList> GetNumbers(std::string_view field) const override {
      return std::nullopt;
    }

    std::optional<StringList> GetTags(std::string_view field) const override {
      return std::nullopt;
    }
  };

  VectorDocumentAccessor doc_accessor;

  // Remove from vector index
  vector_index_->Remove(global_id.id, doc_accessor, "vector_field");

  VLOG(2) << "Removed vector from global index: global_id={" << global_id.Shard() << ","
          << global_id.LocalDocId() << "}, key=" << key;
}

// Global registry implementation
GlobalVectorIndexRegistry& GlobalVectorIndexRegistry::Instance() {
  static GlobalVectorIndexRegistry instance;
  return instance;
}

std::shared_ptr<GlobalVectorIndex> GlobalVectorIndexRegistry::GetOrCreateVectorIndex(
    std::string_view index_name, std::string_view field_name,
    const search::SchemaField::VectorParams& params) {
  std::string key = MakeKey(index_name, field_name);

  {
    std::shared_lock<std::shared_mutex> lock(registry_mutex_);
    auto it = indices_.find(key);
    if (it != indices_.end()) {
      return it->second;
    }
  }

  std::unique_lock<std::shared_mutex> lock(registry_mutex_);
  // Double-check after acquiring write lock
  auto it = indices_.find(key);
  if (it != indices_.end()) {
    return it->second;
  }

  // Create new global vector index
  auto global_index = std::make_shared<GlobalVectorIndex>(params);
  indices_[key] = global_index;

  LOG(INFO) << "Created global vector index: " << key << ", dim=" << params.dim
            << ", use_hnsw=" << params.use_hnsw;

  return global_index;
}

void GlobalVectorIndexRegistry::RemoveVectorIndex(std::string_view index_name,
                                                  std::string_view field_name) {
  std::string key = MakeKey(index_name, field_name);

  std::unique_lock<std::shared_mutex> lock(registry_mutex_);
  auto it = indices_.find(key);
  if (it != indices_.end()) {
    LOG(INFO) << "Removed global vector index: " << key;
    indices_.erase(it);
  }
}

std::shared_ptr<GlobalVectorIndex> GlobalVectorIndexRegistry::GetVectorIndex(
    std::string_view index_name, std::string_view field_name) const {
  std::string key = MakeKey(index_name, field_name);

  std::shared_lock<std::shared_mutex> lock(registry_mutex_);
  auto it = indices_.find(key);
  return it != indices_.end() ? it->second : nullptr;
}

std::string GlobalVectorIndexRegistry::MakeKey(std::string_view index_name,
                                               std::string_view field_name) const {
  return absl::StrCat(index_name, ":", field_name);
}

}  // namespace dfly
