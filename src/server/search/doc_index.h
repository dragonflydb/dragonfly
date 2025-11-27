// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/mi_memory_resource.h"
#include "core/search/base.h"
#include "core/search/search.h"
#include "core/search/synonyms.h"
#include "server/common.h"
#include "server/search/aggregator.h"
#include "server/search/index_join.h"
#include "server/table.h"

namespace dfly {

struct BaseAccessor;

using SearchDocData = absl::flat_hash_map<std::string /*field*/, search::SortableValue /*value*/>;
using Synonyms = search::Synonyms;

std::string_view SearchFieldTypeToString(search::SchemaField::FieldType);

struct SerializedSearchDoc {
  std::string key;
  SearchDocData values;
  float knn_score;
  search::SortableValue sort_score;
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

// Field reference with optional alias as parsed from RETURN [field AS alias], LOAD, etc...
struct FieldReference {
  explicit FieldReference(std::string_view name, std::string_view alias = "")
      : name_{name}, alias_{alias} {
  }

  std::string_view Identifier(const search::Schema& schema, bool is_json) const {
    return (is_json && IsJsonPath(name_)) ? name_ : schema.LookupAlias(name_);
  }

  std::string_view Name() const {
    return name_;
  }

  std::string_view OutputName() const {
    return alias_.empty() ? name_ : alias_;
  }

 private:
  static bool IsJsonPath(std::string_view name);

  std::string_view name_, alias_;
};

enum class SortOrder { ASC, DESC };

struct SearchParams {
  struct SortOption {
    FieldReference field;
    SortOrder order = SortOrder::ASC;

    bool IsSame(const search::KnnScoreSortOption& knn_sort) const {
      return knn_sort.score_field_alias == field.OutputName();
    }
  };

  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset = 0;
  size_t limit_total = 10;

  /*
  1. If not set -> return all fields
  2. If set but empty -> no fields should be returned
  3. If set and not empty -> return only these fields
  */
  std::optional<std::vector<FieldReference>> return_fields;

  /*
    Fields that should be also loaded from the document.

    Only one of load_fields and return_fields should be set.
  */
  std::optional<std::vector<FieldReference>> load_fields;

  std::optional<SortOption> sort_option;

  search::OptionalFilters optional_filters;

  search::QueryParams query_params;

  bool ShouldReturnAllFields() const {
    return !return_fields.has_value();
  }

  bool IdsOnly() const {
    return return_fields && return_fields->empty();
  }

  bool ShouldReturnField(std::string_view alias) const;
};

struct AggregateParams {
  struct JoinParams {
    // Fist field is the index name, second is the field name.
    using Field = std::pair<std::string, std::string>;

    struct Condition {
      Condition(std::string_view field_, std::string_view foreign_index_,
                std::string_view foreign_field_)
          : field{field_}, foreign_field{Field{foreign_index_, foreign_field_}} {
      }

      std::string field;
      Field foreign_field;
    };

    std::string index;
    std::string index_alias;
    std::vector<Condition> conditions;
    std::string query = "*";
  };

  /* Can have 2 scenarios:
      1. No joins - then this is ignored
      2. Has joins and SORTBY ... LIMIT option - then this is used to sort/limit right after join
      3. Has joins and LIMIT option - then this is used to limit right after join.
     Next aggregation steps after first LIMIT or first SORTBY will be applied on the final result,
     after loading the data for all joined documents. */
  struct JoinAggregateParams {
    static constexpr size_t kDefaultLimit = std::numeric_limits<size_t>::max();

    bool HasLimit() const {
      return limit_total != kDefaultLimit;
    }

    bool HasValue() const {
      return HasLimit() || sort.has_value();
    }

    size_t limit_offset = 0;
    size_t limit_total = kDefaultLimit;
    std::optional<aggregate::SortParams> sort;
  };

  std::string_view index, query;
  search::QueryParams params;

  std::vector<JoinParams> joins;
  JoinAggregateParams join_agg_params;

  std::optional<std::vector<FieldReference>> load_fields;
  std::vector<aggregate::AggregationStep> steps;
};

// Stores basic info about a document index.
struct DocIndex {
  enum DataType { HASH, JSON };

  // Get numeric OBJ_ code
  uint8_t GetObjCode() const;

  // Return true if the following document (key, obj_code) is tracked by this index.
  bool Matches(std::string_view key, unsigned obj_code) const;

  search::Schema schema;
  search::IndicesOptions options{};
  std::vector<std::string> prefixes{};
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

  // Used in FieldsValuesPerDocId to store values for each field per document
  using FieldsValues = absl::InlinedVector<search::SortableValue, 4>;

  // DocKeyIndex manages mapping document keys to ids and vice versa through a simple interface.
  struct DocKeyIndex {
    DocId Add(std::string_view key);
    std::optional<DocId> Remove(std::string_view key);

    std::string_view Get(DocId id) const;
    size_t Size() const;

    // Get const reference to the internal ids map
    const absl::flat_hash_map<std::string, DocId>& GetDocKeysMap() const {
      return ids_;
    }

   private:
    absl::flat_hash_map<std::string, DocId> ids_;
    std::vector<std::string> keys_;
    std::vector<DocId> free_ids_;
    DocId last_id_ = 0;
  };

 public:
  // Index must be rebuilt at least once after intialization
  ShardDocIndex(std::shared_ptr<const DocIndex> index);

  // Perform search on all indexed documents and return results.
  SearchResult Search(const OpArgs& op_args, const SearchParams& params,
                      search::SearchAlgorithm* search_algo) const;

  // Perform search and load requested values - note params might be interpreted differently.
  std::vector<SearchDocData> SearchForAggregator(const OpArgs& op_args,
                                                 const AggregateParams& params,
                                                 search::SearchAlgorithm* search_algo) const;

  // Methods needed for join operation
  join::Vector<join::OwnedEntry> PreagregateDataForJoin(
      const OpArgs& op_args, absl::Span<const std::string_view> join_fields,
      search::SearchAlgorithm* search_algo) const;

  using FieldsValuesPerDocId = absl::flat_hash_map<DocId, FieldsValues>;
  FieldsValuesPerDocId LoadKeysData(const OpArgs& op_args,
                                    const absl::flat_hash_set<search::DocId>& doc_ids,
                                    absl::Span<const std::string_view> fields_to_load) const;

  // Return whether base index matches
  bool Matches(std::string_view key, unsigned obj_code) const;

  void AddDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);

  DocIndexInfo GetInfo() const;

  io::Result<StringVec, facade::ErrorReply> GetTagVals(std::string_view field) const;

  // Get synonym manager for this shard
  const Synonyms& GetSynonyms() const {
    return synonyms_;
  }

  Synonyms& GetSynonyms() {
    return synonyms_;
  }

  // Rebuild indices only for documents containing terms from the updated synonym group
  void RebuildForGroup(const OpArgs& op_args, const std::string_view& group_id,
                       const std::vector<std::string_view>& terms);

  // Public access to key index for direct operations (e.g., when dropping index with DD)
  const DocKeyIndex& key_index() const {
    return key_index_;
  }

 private:
  // Clears internal data. Traverses all matching documents and assigns ids.
  void Rebuild(const OpArgs& op_args, PMR_NS::memory_resource* mr);

  using LoadedEntry = std::pair<std::string_view, std::unique_ptr<BaseAccessor>>;
  std::optional<LoadedEntry> LoadEntry(search::DocId id, const OpArgs& op_args) const;

  // Behaviour identical to SortIndex::Sort for non-sortable fields that need to be fetched first
  std::vector<search::SortableValue> KeepTopKSorted(std::vector<DocId>* ids, size_t limit,
                                                    const SearchParams::SortOption& sort,
                                                    const OpArgs& op_args) const;

  std::shared_ptr<const DocIndex> base_;
  std::optional<search::FieldIndices> indices_;
  DocKeyIndex key_index_;
  Synonyms synonyms_;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  ShardDocIndices();

  // Get sharded document index by its name or nullptr if not found
  ShardDocIndex* GetIndex(std::string_view name);

  // Init index: create shard local state for given index with given name.
  // Build if instance is in active state.
  void InitIndex(const OpArgs& op_args, std::string_view name,
                 std::shared_ptr<const DocIndex> index);

  // Drop index, return the dropped index if it existed or nullptr otherwise
  std::unique_ptr<ShardDocIndex> DropIndex(std::string_view name);

  // Drop all indices
  void DropAllIndices();

  // Rebuild all indices
  void RebuildAllIndices(const OpArgs& op_args);

  std::vector<std::string> GetIndexNames() const;

  /* Use AddDoc and RemoveDoc only if pv object type is json or hset */
  void AddDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);

  size_t GetUsedMemory() const;
  SearchStats GetStats() const;  // combines stats for all indices
 private:
  // Clean caches that might have data from this index
  void DropIndexCache(const dfly::ShardDocIndex& shard_doc_index);

  MiMemoryResource local_mr_;
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;
};

}  // namespace dfly
