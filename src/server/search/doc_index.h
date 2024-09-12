// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/mi_memory_resource.h"
#include "core/search/search.h"
#include "server/common.h"
#include "server/search/aggregator.h"
#include "server/table.h"

namespace dfly {

using SearchDocData = absl::flat_hash_map<std::string /*field*/, search::SortableValue /*value*/>;

std::string_view SearchFieldTypeToString(search::SchemaField::FieldType);

struct SerializedSearchDoc {
  std::string key;
  SearchDocData values;
  search::ResultScore score;

  bool operator<(const SerializedSearchDoc& other) const;
  bool operator>=(const SerializedSearchDoc& other) const;
};

struct SearchResult {
  SearchResult() = default;

  SearchResult(size_t total_hits, std::vector<SerializedSearchDoc> docs,
               std::optional<search::AlgorithmProfile> profile)
      : total_hits{total_hits}, docs{std::move(docs)}, profile{std::move(profile)} {
  }

  SearchResult(facade::ErrorReply error) : error{std::move(error)} {
  }

  size_t total_hits;
  std::vector<SerializedSearchDoc> docs;
  std::optional<search::AlgorithmProfile> profile;

  std::optional<facade::ErrorReply> error;
};

using FieldsList = std::vector<std::pair<std::string /*identifier*/, std::string /*short name*/>>;

struct SelectedFields {
  /*
  1. If not set -> return all fields
  2. If set but empty -> no fields should be returned
  3. If set and not empty -> return only these fields
  */
  std::optional<FieldsList> fields;

  bool ShouldReturnAllFields() const {
    return !fields.has_value();
  }

  bool ShouldReturnNoFields() const {
    return fields && fields->empty();
  }

  FieldsList* operator->() {
    return &fields.value();
  }

  const FieldsList* operator->() const {
    return &fields.value();
  }

  const FieldsList& GetFields() const {
    return fields.value();
  }
};

struct SearchParams {
  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset = 0;
  size_t limit_total = 10;

  // Set but empty means no fields should be returned
  SelectedFields return_fields;
  std::optional<search::SortOption> sort_option;
  search::QueryParams query_params;

  bool IdsOnly() const {
    return return_fields.ShouldReturnNoFields();
  }

  bool ShouldReturnField(std::string_view field) const;
};

struct AggregateParams {
  std::string_view index, query;
  search::QueryParams params;

  SelectedFields load_fields;
  std::vector<aggregate::PipelineStep> steps;
};

// Stores basic info about a document index.
struct DocIndex {
  enum DataType { HASH, JSON };

  // Get numeric OBJ_ code
  uint8_t GetObjCode() const;

  // Return true if the following document (key, obj_code) is tracked by this index.
  bool Matches(std::string_view key, unsigned obj_code) const;

  search::Schema schema;
  std::string prefix{};
  DataType type{HASH};
};

struct DocIndexInfo {
  DocIndex base_index;
  size_t num_docs = 0;

  // Build original ft.create command that can be used to re-create this index
  std::string BuildRestoreCommand() const;
};

class ShardDocIndices;

// Stores internal search indices for documents of a document index on a specific shard.
class ShardDocIndex {
  friend class ShardDocIndices;
  using DocId = search::DocId;

  // DocKeyIndex manages mapping document keys to ids and vice versa through a simple interface.
  struct DocKeyIndex {
    DocId Add(std::string_view key);
    DocId Remove(std::string_view key);

    std::string_view Get(DocId id) const;
    size_t Size() const;

   private:
    absl::flat_hash_map<std::string, DocId> ids_;
    std::vector<std::string> keys_;
    std::vector<DocId> free_ids_;
    DocId last_id_ = 0;
  };

 public:
  // Index must be rebuilt at least once after intialization
  ShardDocIndex(std::shared_ptr<DocIndex> index);

  // Perform search on all indexed documents and return results.
  SearchResult Search(const OpArgs& op_args, const SearchParams& params,
                      search::SearchAlgorithm* search_algo) const;

  // Perform search and load requested values - note params might be interpreted differently.
  std::vector<SearchDocData> SearchForAggregator(const OpArgs& op_args,
                                                 const AggregateParams& params,
                                                 search::SearchAlgorithm* search_algo) const;

  // Return whether base index matches
  bool Matches(std::string_view key, unsigned obj_code) const;

  void AddDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);

  DocIndexInfo GetInfo() const;

  io::Result<StringVec, facade::ErrorReply> GetTagVals(std::string_view field) const;

 private:
  // Clears internal data. Traverses all matching documents and assigns ids.
  void Rebuild(const OpArgs& op_args, PMR_NS::memory_resource* mr);

 private:
  bool was_built_ = false;
  std::shared_ptr<const DocIndex> base_;
  search::FieldIndices indices_;
  DocKeyIndex key_index_;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  ShardDocIndices();

  // Get sharded document index by its name or nullptr if not found
  ShardDocIndex* GetIndex(std::string_view name);

  // Init index: create shard local state for given index with given name.
  // Build if instance is in active state.
  void InitIndex(const OpArgs& op_args, std::string_view name, std::shared_ptr<DocIndex> index);

  // Drop index, return true if it existed and was dropped
  bool DropIndex(std::string_view name);

  // Drop all indices
  void DropAllIndices();

  // Rebuild all indices
  void RebuildAllIndices(const OpArgs& op_args);

  std::vector<std::string> GetIndexNames() const;

  void AddDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);

  size_t GetUsedMemory() const;
  SearchStats GetStats() const;  // combines stats for all indices
 private:
  // Clean caches that might have data from this index
  void DropIndexCache(const dfly::ShardDocIndex& shard_doc_index);

 private:
  MiMemoryResource local_mr_;
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;
};

}  // namespace dfly
