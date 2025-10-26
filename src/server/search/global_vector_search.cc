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

}  // namespace dfly
