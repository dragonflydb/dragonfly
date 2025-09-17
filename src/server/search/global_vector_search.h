// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/ast_expr.h"
#include "core/search/search.h"
#include "server/search/doc_index.h"
#include "server/search/global_vector_index.h"

namespace dfly {

// Global vector search result with global document IDs
struct GlobalSearchResult {
  size_t total_hits = 0;
  std::vector<std::pair<float, GlobalDocId>> knn_results;
  std::vector<SerializedSearchDoc> docs;
  std::optional<facade::ErrorReply> error;

  GlobalSearchResult() = default;
};

// Global vector search algorithm that uses the global vector index
class GlobalVectorSearchAlgorithm {
 public:
  GlobalVectorSearchAlgorithm() = default;

  // Initialize with query - similar to SearchAlgorithm::Init
  bool Init(std::string_view query, const search::QueryParams* params);

  // Search using global vector index for vector queries, fallback to shard-based for others
  GlobalSearchResult Search(std::string_view index_name,
                            const std::vector<ShardDocIndex*>& shard_indices) const;

  // Check if this is a vector-only KNN query that can use global index
  bool IsVectorOnlyQuery() const;

  // Get KNN sort option if present
  std::optional<search::KnnScoreSortOption> GetKnnScoreSortOption() const;

  // Extract vector field name from KNN query
  std::optional<std::string> ExtractVectorFieldName() const;

  // Get KNN parameters for global search
  struct KnnParams {
    float* vector;
    size_t limit;
    std::optional<size_t> ef_runtime;
  };
  std::optional<KnnParams> GetKnnParams() const;

  void EnableProfiling();

 private:
  std::unique_ptr<search::AstNode> query_;
  bool profiling_enabled_ = false;

  // Check if query contains only vector search (KNN) without other filters
  bool IsKnnOnlyQuery(const search::AstNode& node) const;

  // Extract vector field name from KNN query
  std::optional<std::string> ExtractVectorField(const search::AstNode& node) const;
};

// Helper class to coordinate between global vector index and shard-based document fetching
class GlobalVectorSearchCoordinator {
 public:
  // Execute global vector search and fetch document fields from appropriate shards
  static GlobalSearchResult ExecuteGlobalVectorSearch(
      std::string_view index_name, std::string_view query_str, const SearchParams& params,
      const search::QueryParams& query_params, const std::vector<ShardDocIndex*>& shard_indices);

 private:
  // Group global doc IDs by shard for efficient fetching
  static absl::flat_hash_map<ShardId, std::vector<GlobalDocId>> GroupByShard(
      const std::vector<GlobalDocId>& global_ids);

  // Fetch document fields from specific shard
  static std::vector<SerializedSearchDoc> FetchDocumentFields(
      ShardId shard_id, const std::vector<GlobalDocId>& shard_global_ids,
      const std::vector<std::pair<search::DocId, float>>& knn_scores, const SearchParams& params,
      ShardDocIndex* shard_index, const OpArgs& op_args);
};

}  // namespace dfly
