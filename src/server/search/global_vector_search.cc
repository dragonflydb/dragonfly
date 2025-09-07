// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/global_vector_search.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/search/ast_expr.h"
#include "core/search/parser.hh"
#include "core/search/query_driver.h"
#include "core/search/vector_utils.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"
#include "server/search/doc_accessors.h"

namespace dfly {

using namespace std;
using namespace search;

bool GlobalVectorSearchAlgorithm::Init(std::string_view query, const QueryParams* params) {
  try {
    QueryDriver driver{};
    driver.ResetScanner();
    driver.SetParams(params);
    driver.SetInput(std::string{query});
    (void)Parser (&driver)();
    query_ = std::make_unique<AstNode>(driver.Take());
  } catch (const Parser::syntax_error& se) {
    LOG(INFO) << "Failed to parse query \"" << query << "\": " << se.what();
    return false;
  } catch (...) {
    LOG_EVERY_T(INFO, 10) << "Unexpected query parser error \"" << query << "\"";
    return false;
  }

  if (std::holds_alternative<std::monostate>(*query_)) {
    LOG_EVERY_T(INFO, 10) << "Empty result after parsing query \"" << query << "\"";
    return false;
  }

  return true;
}

GlobalSearchResult GlobalVectorSearchAlgorithm::Search(
    std::string_view index_name, const std::vector<ShardDocIndex*>& shard_indices) const {
  if (!IsVectorOnlyQuery()) {
    // Fallback to traditional shard-based search for non-vector-only queries
    // This would need to be implemented to handle mixed queries
    LOG(WARNING) << "Mixed queries not yet supported in global vector search PoC";
    return GlobalSearchResult{};
  }

  // Extract vector field name from KNN query
  auto vector_field = ExtractVectorField(*query_);
  if (!vector_field) {
    LOG(ERROR) << "Could not extract vector field from KNN query";
    return GlobalSearchResult{};
  }

  // Get global vector index
  auto global_index =
      GlobalVectorIndexRegistry::Instance().GetVectorIndex(index_name, *vector_field);

  if (!global_index) {
    VLOG(1) << "Global vector index not found: " << index_name << ":" << *vector_field
            << ", will use shard-based search";
    // Return empty result to trigger fallback
    GlobalSearchResult empty_result;
    empty_result.total_hits = 0;
    return empty_result;
  }

  // Extract KNN parameters from query
  if (auto* knn_node = std::get_if<AstKnnNode>(query_.get())) {
    auto knn_results =
        global_index->Knn(knn_node->vec.first.get(), knn_node->limit, knn_node->ef_runtime);

    GlobalSearchResult result;
    result.total_hits = knn_results.size();
    result.knn_results = std::move(knn_results);

    LOG(INFO) << "Global vector search found " << result.total_hits << " results for index "
              << index_name << " field " << *vector_field;

    return result;
  }

  return GlobalSearchResult{};
}

bool GlobalVectorSearchAlgorithm::IsVectorOnlyQuery() const {
  return IsKnnOnlyQuery(*query_);
}

std::optional<KnnScoreSortOption> GlobalVectorSearchAlgorithm::GetKnnScoreSortOption() const {
  if (auto* knn = std::get_if<AstKnnNode>(query_.get())) {
    return KnnScoreSortOption{std::string_view{knn->score_alias}, knn->limit};
  }
  return std::nullopt;
}

std::optional<std::string> GlobalVectorSearchAlgorithm::ExtractVectorFieldName() const {
  if (auto* knn = std::get_if<AstKnnNode>(query_.get())) {
    return knn->field;
  }
  return std::nullopt;
}

std::optional<GlobalVectorSearchAlgorithm::KnnParams> GlobalVectorSearchAlgorithm::GetKnnParams()
    const {
  if (auto* knn_node = std::get_if<search::AstKnnNode>(query_.get())) {
    std::optional<size_t> ef_runtime = std::nullopt;
    if (knn_node->ef_runtime.has_value()) {
      ef_runtime = static_cast<size_t>(knn_node->ef_runtime.value());
    }
    return KnnParams{
        .vector = knn_node->vec.first.get(), .limit = knn_node->limit, .ef_runtime = ef_runtime};
  }
  return std::nullopt;
}

void GlobalVectorSearchAlgorithm::EnableProfiling() {
  profiling_enabled_ = true;
}

bool GlobalVectorSearchAlgorithm::IsKnnOnlyQuery(const AstNode& node) const {
  // Check if this is a pure KNN query without other filters
  if (auto* knn = std::get_if<AstKnnNode>(&node)) {
    // Pure KNN query should not have any filter
    return !knn->filter || std::holds_alternative<AstStarNode>(*knn->filter);
  }
  return false;
}

std::optional<std::string> GlobalVectorSearchAlgorithm::ExtractVectorField(
    const AstNode& node) const {
  if (auto* knn = std::get_if<AstKnnNode>(&node)) {
    return knn->field;
  }
  return std::nullopt;
}

// GlobalVectorSearchCoordinator implementation
GlobalSearchResult GlobalVectorSearchCoordinator::ExecuteGlobalVectorSearch(
    std::string_view index_name, std::string_view query_str, const SearchParams& params,
    const QueryParams& query_params, const std::vector<ShardDocIndex*>& shard_indices) {
  GlobalVectorSearchAlgorithm algo;
  if (!algo.Init(query_str, &query_params)) {
    return GlobalSearchResult{};
  }

  if (!algo.IsVectorOnlyQuery()) {
    LOG(WARNING) << "Non-vector-only queries not supported in PoC";
    return GlobalSearchResult{};
  }

  // Execute global search
  auto global_result = algo.Search(index_name, shard_indices);
  if (global_result.knn_results.empty()) {
    return global_result;
  }

  // Extract global doc IDs from results and create knn_scores mapping
  std::vector<GlobalDocId> global_doc_ids;
  std::vector<std::pair<search::DocId, float>> knn_scores;
  for (const auto& [score, global_id] : global_result.knn_results) {
    global_doc_ids.push_back(global_id);
    knn_scores.emplace_back(global_id.local_doc_id, score);
  }

  // Group global doc IDs by shard
  auto grouped_ids = GroupByShard(global_doc_ids);

  // Fetch document fields from each shard
  std::vector<SerializedSearchDoc> all_docs;
  for (const auto& [shard_id, shard_global_ids] : grouped_ids) {
    if (shard_id < shard_indices.size() && shard_indices[shard_id]) {
      // Need to execute on the correct shard, not current thread's shard
      // This is a limitation of the PoC - we can't easily access other shards from coordinator
      // For now, use current shard as workaround
      DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};
      OpArgs op_args{EngineShard::tlocal(), nullptr, db_cntx};

      auto shard_docs = FetchDocumentFields(shard_id, shard_global_ids, knn_scores, params,
                                            shard_indices[shard_id], op_args);

      all_docs.insert(all_docs.end(), std::make_move_iterator(shard_docs.begin()),
                      std::make_move_iterator(shard_docs.end()));
    } else {
      LOG(WARNING) << "Shard " << shard_id << " not available";
    }
  }

  // Apply SORTBY if needed (after fetching all documents)
  if (params.sort_option && !all_docs.empty()) {
    const auto& sort_opt = *params.sort_option;
    LOG(INFO) << "Applying SORTBY " << sort_opt.field.Name() << " "
              << (sort_opt.order == SortOrder::DESC ? "DESC" : "ASC") << " to " << all_docs.size()
              << " global search results";

    auto comparator = [&sort_opt](const SerializedSearchDoc& a, const SerializedSearchDoc& b) {
      std::string field_name{sort_opt.field.OutputName()};

      auto a_it = a.values.find(field_name);
      auto b_it = b.values.find(field_name);

      // Handle missing values (put them at the end)
      if (a_it == a.values.end() && b_it == b.values.end())
        return false;
      if (a_it == a.values.end())
        return false;  // a goes to end
      if (b_it == b.values.end())
        return true;  // b goes to end, a comes first

      bool result = a_it->second < b_it->second;
      return sort_opt.order == SortOrder::DESC ? !result : result;
    };

    std::sort(all_docs.begin(), all_docs.end(), comparator);
  }

  // Apply LIMIT after sorting (if any)
  if (!all_docs.empty()) {
    size_t start_idx = std::min(params.limit_offset, all_docs.size());
    size_t end_idx = std::min(start_idx + params.limit_total, all_docs.size());

    if (start_idx > 0 || end_idx < all_docs.size()) {
      std::vector<SerializedSearchDoc> limited_docs;
      limited_docs.reserve(end_idx - start_idx);

      for (size_t i = start_idx; i < end_idx; ++i) {
        limited_docs.push_back(std::move(all_docs[i]));
      }

      all_docs = std::move(limited_docs);
      LOG(INFO) << "Applied LIMIT " << params.limit_offset << " " << params.limit_total
                << ", result size: " << all_docs.size();
    }
  }

  global_result.docs = std::move(all_docs);
  return global_result;
}

absl::flat_hash_map<ShardId, std::vector<GlobalDocId>> GlobalVectorSearchCoordinator::GroupByShard(
    const std::vector<GlobalDocId>& global_ids) {
  absl::flat_hash_map<ShardId, std::vector<GlobalDocId>> grouped;

  for (const auto& global_id : global_ids) {
    grouped[global_id.shard_id].push_back(global_id);
  }

  return grouped;
}

std::vector<SerializedSearchDoc> GlobalVectorSearchCoordinator::FetchDocumentFields(
    ShardId shard_id, const std::vector<GlobalDocId>& shard_global_ids,
    const std::vector<std::pair<search::DocId, float>>& knn_scores, const SearchParams& params,
    ShardDocIndex* shard_index, const OpArgs& op_args) {
  std::vector<SerializedSearchDoc> docs;
  docs.reserve(shard_global_ids.size());

  // Create map for quick score lookup
  absl::flat_hash_map<search::DocId, float> score_map;
  for (const auto& [doc_id, score] : knn_scores) {
    score_map[doc_id] = score;
  }

  for (const auto& global_id : shard_global_ids) {
    // Load entry from shard
    auto entry = shard_index->LoadEntry(global_id.local_doc_id, op_args);
    if (!entry) {
      continue;  // Document might have expired
    }

    auto& [key, accessor] = *entry;

    // Serialize document fields based on return parameters
    SearchDocData fields{};
    auto index_info = shard_index->GetInfo();
    if (params.ShouldReturnAllFields()) {
      fields = accessor->Serialize(index_info.base_index.schema);
    }

    auto return_fields = params.return_fields.value_or(std::vector<FieldReference>{});
    auto more_fields = accessor->Serialize(index_info.base_index.schema, return_fields);
    fields.insert(std::make_move_iterator(more_fields.begin()),
                  std::make_move_iterator(more_fields.end()));

    // Get KNN score for this document
    float knn_score = 0.0f;
    auto score_it = score_map.find(global_id.local_doc_id);
    if (score_it != score_map.end()) {
      knn_score = score_it->second;
    }

    search::SortableValue sort_score = std::monostate{};

    docs.push_back({std::string{key}, std::move(fields), knn_score, sort_score});
  }

  return docs;
}

}  // namespace dfly
