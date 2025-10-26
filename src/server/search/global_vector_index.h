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
  uint64_t id;

  explicit GlobalDocId(uint64_t id) : id(id) {
  }

  GlobalDocId(ShardId shard_id, search::DocId local_doc_id) {
    id = ((uint64_t)shard_id << 32) | local_doc_id;
  }

  ShardId Shard() const {
    return (id >> 32);
  }

  search::DocId LocalDocId() const {
    return (id)&0xFFFF;
  }

  // bool operator<(const GlobalDocId& other) const {
  //   return std::tie(Shard(), LocalDocId()) < std::tie(other.Shard(), other.LocalDocId());
  // }

  bool operator==(const GlobalDocId& other) const {
    return ShardId() == other.Shard() && LocalDocId() == other.LocalDocId();
  }

  bool operator!=(const GlobalDocId& other) const {
    return !(*this == other);
  }

  // Hash function for use in absl containers
  template <typename H> friend H AbslHashValue(H h, const GlobalDocId& id) {
    return H::combine(std::move(h), id.Shard(), id.LocalDocId());
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
  std::vector<std::pair<float, uint64_t>> Knn(float* target, size_t k,
                                              std::optional<size_t> ef = std::nullopt) const;

  std::vector<std::pair<float, uint64_t>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                              const std::vector<GlobalDocId>& allowed) const;

  // Get vector info (dimensions, similarity metric)
  std::pair<size_t, search::VectorSimilarity> Info() const;

  // Thread-safe write operations (exclusive access)
  bool AddVector(GlobalDocId global_id, const float* vector);
  void RemoveVector(GlobalDocId global_id, std::string_view key);

 private:
  std::unique_ptr<search::BaseVectorIndex> vector_index_;

  // Mapping from GlobalDocId to document key (for fetching fields later)
  absl::flat_hash_map<GlobalDocId, std::string> global_to_key_;

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
