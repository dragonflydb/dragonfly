// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "core/search/base.h"
#include "core/search/indices.h"
#include "core/search/search.h"
#include "server/common.h"
#include "server/tx_base.h"

namespace dfly {

// Global document ID that uniquely identifies a document across all shards
struct GlobalDocId {
  ShardId shard_id;
  search::DocId local_doc_id;

  bool operator<(const GlobalDocId& other) const {
    return std::tie(shard_id, local_doc_id) < std::tie(other.shard_id, other.local_doc_id);
  }

  bool operator==(const GlobalDocId& other) const {
    return shard_id == other.shard_id && local_doc_id == other.local_doc_id;
  }

  bool operator!=(const GlobalDocId& other) const {
    return !(*this == other);
  }

  // Hash function for use in absl containers
  template <typename H> friend H AbslHashValue(H h, const GlobalDocId& id) {
    return H::combine(std::move(h), id.shard_id, id.local_doc_id);
  }
};

// Thread-safe global vector index that can be accessed from multiple threads
// Uses reader-writer locks to allow concurrent reads while protecting writes
class GlobalVectorIndex {
 public:
  explicit GlobalVectorIndex(const search::SchemaField::VectorParams& params,
                             PMR_NS::memory_resource* mr = PMR_NS::get_default_resource());

  ~GlobalVectorIndex();

  // Thread-safe read operations (multiple readers can access simultaneously)
  std::vector<std::pair<float, GlobalDocId>> Knn(float* target, size_t k,
                                                 std::optional<size_t> ef = std::nullopt) const;

  std::vector<std::pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                                 const std::vector<GlobalDocId>& allowed) const;

  // Get vector info (dimensions, similarity metric)
  std::pair<size_t, search::VectorSimilarity> Info() const;

  // Thread-safe write operations (exclusive access)
  bool AddVector(GlobalDocId global_id, std::string_view key, const float* vector);
  void RemoveVector(GlobalDocId global_id, std::string_view key);

  // Get statistics
  size_t Size() const;
  std::vector<GlobalDocId> GetAllDocsWithVectors() const;

  // Get key for global doc id (for fetching document fields)
  std::optional<std::string> GetKey(GlobalDocId global_id) const;

 private:
  // Convert between GlobalDocId and internal DocId used by vector index
  search::DocId ToInternalDocId(GlobalDocId global_id) const;
  GlobalDocId FromInternalDocId(search::DocId internal_id) const;

  mutable std::shared_mutex rw_mutex_;
  std::unique_ptr<search::BaseVectorIndex> vector_index_;

  // Mapping between GlobalDocId and internal DocId
  absl::flat_hash_map<GlobalDocId, search::DocId> global_to_internal_;
  absl::flat_hash_map<search::DocId, GlobalDocId> internal_to_global_;

  // Mapping from GlobalDocId to document key (for fetching fields later)
  absl::flat_hash_map<GlobalDocId, std::string> global_to_key_;

  // Counter for generating internal DocIds
  search::DocId next_internal_id_{0};

  // Vector parameters
  search::SchemaField::VectorParams params_;
};

// Global registry for all vector indices
class GlobalVectorIndexRegistry {
 public:
  static GlobalVectorIndexRegistry& Instance();

  // Get or create global vector index for given index name and field
  std::shared_ptr<GlobalVectorIndex> GetOrCreateVectorIndex(
      std::string_view index_name, std::string_view field_name,
      const search::SchemaField::VectorParams& params);

  // Remove vector index
  void RemoveVectorIndex(std::string_view index_name, std::string_view field_name);

  // Get existing vector index
  std::shared_ptr<GlobalVectorIndex> GetVectorIndex(std::string_view index_name,
                                                    std::string_view field_name) const;

 private:
  GlobalVectorIndexRegistry() = default;

  mutable std::shared_mutex registry_mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<GlobalVectorIndex>> indices_;

  std::string MakeKey(std::string_view index_name, std::string_view field_name) const;
};

}  // namespace dfly
