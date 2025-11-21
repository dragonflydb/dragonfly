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
#include "core/search/index_result.h"
#include "core/search/indices.h"
#include "core/search/search.h"
#include "server/common.h"
#include "server/search/doc_index.h"
#include "server/tx_base.h"

namespace dfly {

struct KnnScoreSortOption;

class GlobalVectorIndex {
 public:
  GlobalVectorIndex(const search::SchemaField::VectorParams& params, std::string_view index_name,
                    PMR_NS::memory_resource* mr = PMR_NS::get_default_resource());

  ~GlobalVectorIndex();

  bool Add(search::GlobalDocId id, const search::DocumentAccessor& doc, std::string_view field);
  void Remove(search::GlobalDocId id, const search::DocumentAccessor& doc, std::string_view field);

  std::vector<SearchResult> Search(
      const search::AstKnnNode* knn,
      const std::optional<search::KnnScoreSortOption>& knn_score_option,
      const std::vector<SearchResult>& filter_docs, const SearchParams& params,
      const CommandContext& cmd_cntx);

 private:
  std::vector<std::pair<float, search::GlobalDocId>> SearchKnnHnsw(
      search::HnswVectorIndex* index, const search::AstKnnNode* knn,
      const std::optional<std::vector<search::GlobalDocId>>& allowed_docs);

  std::vector<std::pair<float, search::GlobalDocId>> SearchKnnFlat(
      search::FlatVectorIndex* index, const search::AstKnnNode* knn,
      const std::optional<std::vector<search::FlatVectorIndex::FilterShardDocs>>& allowed_docs);

  std::unique_ptr<search::BaseVectorIndex<search::GlobalDocId>> vector_index_;
  search::SchemaField::VectorParams params_;
  std::string index_name_;
};

// Global registry for all vector indices
class GlobalVectorIndexRegistry {
 public:
  static GlobalVectorIndexRegistry& Instance();

  // Create global vector index for given index name and field
  bool CreateVectorIndex(std::string_view index_name, std::string_view field_name,
                         const search::SchemaField::VectorParams& params);

  // Remove vector index
  void RemoveVectorIndex(std::string_view index_name, std::string_view field_name);

  // Get existing vector index
  std::shared_ptr<GlobalVectorIndex> GetVectorIndex(std::string_view index_name,
                                                    std::string_view field_name) const;

  // Reset all vector indices
  void Reset();

 private:
  GlobalVectorIndexRegistry() = default;

  mutable std::shared_mutex registry_mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<GlobalVectorIndex>> indices_;
  std::string MakeKey(std::string_view index_name, std::string_view field_name) const;
};

}  // namespace dfly
