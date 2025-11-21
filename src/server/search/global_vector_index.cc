// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/global_vector_index.h"

#include <absl/strings/str_cat.h>

#include <memory>
#include <optional>
#include <shared_mutex>
#include <utility>

#include "base/logging.h"
#include "core/search/ast_expr.h"
#include "core/search/base.h"
#include "core/search/index_result.h"
#include "core/search/indices.h"
#include "core/search/vector_utils.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"
#include "server/search/doc_index.h"
#include "server/transaction.h"
#include "server/tx_base.h"

namespace dfly {

GlobalVectorIndex::GlobalVectorIndex(const search::SchemaField::VectorParams& params,
                                     std::string_view index_name, PMR_NS::memory_resource* mr)
    : params_(params), index_name_(index_name) {
  if (params.use_hnsw) {
    vector_index_ = std::make_unique<search::HnswVectorIndex>(params, mr);
  } else {
    vector_index_ = std::make_unique<search::FlatVectorIndex>(params, shard_set->size(), mr);
  }
}

GlobalVectorIndex::~GlobalVectorIndex() = default;

bool GlobalVectorIndex::Add(search::GlobalDocId id, const search::DocumentAccessor& doc,
                            std::string_view field) {
  return vector_index_->Add(id, doc, field);
}

void GlobalVectorIndex::Remove(search::GlobalDocId id, const search::DocumentAccessor& doc,
                               std::string_view field) {
  vector_index_->Remove(id, doc, field);
}

std::vector<std::pair<float, search::GlobalDocId>> GlobalVectorIndex::SearchKnnHnsw(
    search::HnswVectorIndex* index, const search::AstKnnNode* knn,
    const std::optional<std::vector<search::GlobalDocId>>& allowed_docs) {
  if (allowed_docs)
    return index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime, *allowed_docs);
  else
    return index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime);
}

std::vector<std::pair<float, search::GlobalDocId>> GlobalVectorIndex::SearchKnnFlat(
    search::FlatVectorIndex* index, const search::AstKnnNode* knn,
    const std::optional<std::vector<search::FlatVectorIndex::FilterShardDocs>>& allowed_docs) {
  if (allowed_docs)
    return index->Knn(knn->vec.first.get(), knn->limit, *allowed_docs);
  else
    return index->Knn(knn->vec.first.get(), knn->limit);
}

std::vector<SearchResult> GlobalVectorIndex::Search(
    const search::AstKnnNode* knn_node,
    const std::optional<search::KnnScoreSortOption>& knn_score_option,
    const std::vector<SearchResult>& shard_filter_docs, const SearchParams& params,
    const CommandContext& cmd_cntx) {
  std::vector<SearchResult> results(1);

  std::optional<std::vector<search::GlobalDocId>> filter_docs_global_ids = std::nullopt;
  std::optional<std::vector<search::FlatVectorIndex::FilterShardDocs>> filter_docs_shard_ids =
      std::nullopt;
  std::map<search::GlobalDocId, const SerializedSearchDoc*> filter_docs_lookup;

  const ShardId shard_size = shard_filter_docs.size();

  // We have pre filter so all documents should already be fetched
  if (knn_node->Filter()) {
    std::vector<search::GlobalDocId> global_ids;
    std::vector<search::FlatVectorIndex::FilterShardDocs> shard_ids;
    shard_ids.resize(shard_size);
    for (size_t shard_id = 0; shard_id < shard_size; shard_id++) {
      for (auto& doc : shard_filter_docs[shard_id].docs) {
        auto global_doc_id = search::CreateGlobalDocId(shard_id, doc.id);
        global_ids.emplace_back(global_doc_id);
        shard_ids[shard_id].push_back(doc.id);
        filter_docs_lookup[global_doc_id] = &doc;
      }
    }
    filter_docs_global_ids = std::move(global_ids);
    filter_docs_shard_ids = std::move(shard_ids);
  }

  std::vector<std::pair<float, search::GlobalDocId>> knn_results;
  if (auto hnsw_index = dynamic_cast<search::HnswVectorIndex*>(vector_index_.get()); hnsw_index) {
    knn_results = SearchKnnHnsw(hnsw_index, knn_node, filter_docs_global_ids);
  } else if (auto flat_index = dynamic_cast<search::FlatVectorIndex*>(vector_index_.get());
             flat_index) {
    knn_results = SearchKnnFlat(flat_index, knn_node, filter_docs_shard_ids);
  }

  std::vector<SerializedSearchDoc> knn_result_docs;
  knn_result_docs.reserve(knn_results.size());

  // Group by shard with minimal allocations
  std::vector<std::vector<std::pair<float, search::DocId>>> shard_doc_ids(shard_size);

  for (const auto& [score, global_id] : knn_results) {
    if (knn_node->Filter()) {
      knn_result_docs.emplace_back(*filter_docs_lookup[global_id]);
      // Update knn score
      knn_result_docs.back().knn_score = score;
    } else {
      ShardId shard_id = search::GlobalDocIdShardId(global_id);
      search::DocId doc_id = search::GlobalDocIdLocalId(global_id);
      shard_doc_ids[shard_id].emplace_back(score, doc_id);
    }
  }

  if (knn_node->Filter()) {
    results[0].total_hits = knn_results.size();
    results[0].docs = std::move(knn_result_docs);
    return results;
  }

  // Use per-shard vectors to avoid race conditions, but keep them minimal
  std::vector<std::vector<SerializedSearchDoc>> shard_docs(shard_size);
  std::atomic<bool> index_not_found{false};

  bool should_fetch_sort_field = false;
  if (params.sort_option) {
    should_fetch_sort_field = !params.sort_option->IsSame(*knn_score_option);
  }

  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(index_name_);

    if (!index) {
      index_not_found.store(true);
      return OpStatus::OK;
    }

    auto& shard_requests = shard_doc_ids[es->shard_id()];
    if (shard_requests.empty())
      return OpStatus::OK;

    auto& docs_for_shard = shard_docs[es->shard_id()];
    docs_for_shard.reserve(shard_requests.size());

    // Cache schema reference to avoid repeated lookups
    const auto& schema = index->GetInfo().base_index.schema;

    // Optimize serialization based on query type
    if (params.ShouldReturnAllFields()) {
      // Full serialization for full queries
      for (const auto& [score, doc_id] : shard_requests) {
        if (auto entry = index->LoadEntry(doc_id, t->GetOpArgs(es))) {
          auto& [key, accessor] = *entry;
          auto fields = accessor->Serialize(schema);

          // If we use SORT we need to update`sort_score`
          search::SortableValue sort_score = std::monostate{};
          if (should_fetch_sort_field) {
            sort_score = fields[params.sort_option->field.Name()];
          }

          docs_for_shard.push_back(
              {doc_id, std::string{key}, std::move(fields), score, sort_score});
        }
      }
    } else {
      // Selective field serialization
      auto return_fields = params.return_fields.value_or(std::vector<FieldReference>{});

      bool should_return_sort_field = false;
      if (should_fetch_sort_field) {
        for (const auto& return_field : return_fields) {
          if (params.sort_option->field.Name() == return_field.Name()) {
            should_return_sort_field = true;
          }
        }
      }

      // Sort field is not returned so we need to inject it
      if (should_fetch_sort_field && !should_return_sort_field) {
        return_fields.push_back(params.sort_option->field);
      }

      for (const auto& [score, doc_id] : shard_requests) {
        if (auto entry = index->LoadEntry(doc_id, t->GetOpArgs(es))) {
          auto& [key, accessor] = *entry;
          auto fields = return_fields.empty() ? SearchDocData{}
                                              // NOCONTENT query - no fields needed
                                              : accessor->Serialize(schema, return_fields);

          search::SortableValue sort_score = std::monostate{};
          if (should_fetch_sort_field) {
            sort_score = fields[params.sort_option->field.Name()];
            // Erase sort field from returned fields
            if (!should_return_sort_field) {
              fields.erase(params.sort_option->field.Name());
            }
          }

          docs_for_shard.push_back(
              {doc_id, std::string{key}, std::move(fields), score, sort_score});
        }
      }
    }

    return OpStatus::OK;
  });

  // Crete single vector of aggregated documents from all shards
  for (auto& docs : shard_docs) {
    knn_result_docs.insert(knn_result_docs.end(), std::make_move_iterator(docs.begin()),
                           std::make_move_iterator(docs.end()));
  }

  results[0].total_hits = knn_result_docs.size();
  results[0].docs = std::move(knn_result_docs);
  return results;
}

// Global registry implementation
GlobalVectorIndexRegistry& GlobalVectorIndexRegistry::Instance() {
  static GlobalVectorIndexRegistry instance;
  return instance;
}

bool GlobalVectorIndexRegistry::CreateVectorIndex(std::string_view index_name,
                                                  std::string_view field_name,
                                                  const search::SchemaField::VectorParams& params) {
  std::string key = MakeKey(index_name, field_name);

  std::shared_lock<std::shared_mutex> lock(registry_mutex_);

  auto it = indices_.find(key);
  if (it != indices_.end())
    return false;

  indices_[key] = std::make_shared<GlobalVectorIndex>(params, index_name);
  ;
  return true;
}

void GlobalVectorIndexRegistry::RemoveVectorIndex(std::string_view index_name,
                                                  std::string_view field_name) {
  std::string key = MakeKey(index_name, field_name);

  std::unique_lock<std::shared_mutex> lock(registry_mutex_);

  auto it = indices_.find(key);
  if (it != indices_.end())
    indices_.erase(it);
}

std::shared_ptr<GlobalVectorIndex> GlobalVectorIndexRegistry::GetVectorIndex(
    std::string_view index_name, std::string_view field_name) const {
  std::string key = MakeKey(index_name, field_name);

  std::shared_lock<std::shared_mutex> lock(registry_mutex_);

  auto it = indices_.find(key);
  return it != indices_.end() ? it->second : nullptr;
}

void GlobalVectorIndexRegistry::Reset() {
  std::unique_lock<std::shared_mutex> lock(registry_mutex_);
  indices_.clear();
}

std::string GlobalVectorIndexRegistry::MakeKey(std::string_view index_name,
                                               std::string_view field_name) const {
  return absl::StrCat(index_name, ":", field_name);
}

}  // namespace dfly
