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

/* SearchField represents a field that can store combinations of identifiers and aliases in various
   forms: [identifier and alias], [alias and new_alias], [new identifier and alias] (used for JSON
   data) This class provides methods to retrieve the actual identifier and alias for a field,
   handling different naming conventions and resolving names based on the schema. */
class SearchField {
 private:
  static bool IsJsonPath(std::string_view name) {
    if (name.size() < 2) {
      return false;
    }
    return name.front() == '$' && (name[1] == '.' || name[1] == '[');
  }

 public:
  SearchField() = default;

  SearchField(StringOrView name, bool is_short_name)
      : name_(std::move(name)), is_short_name_(is_short_name) {
  }

  SearchField(StringOrView name, bool is_short_name, StringOrView new_alias)
      : name_(std::move(name)), is_short_name_(is_short_name), new_alias_(std::move(new_alias)) {
  }

  std::string_view GetIdentifier(const search::Schema& schema, bool is_json_field) const {
    auto as_view = NameView();
    if (!is_short_name_ || (is_json_field && IsJsonPath(as_view))) {
      return as_view;
    }
    return schema.LookupAlias(as_view);
  }

  std::string_view GetShortName() const {
    if (HasNewAlias()) {
      return AliasView();
    }
    return NameView();
  }

  std::string_view GetShortName(const search::Schema& schema) const {
    if (HasNewAlias()) {
      return AliasView();
    }
    return is_short_name_ ? NameView() : schema.LookupIdentifier(NameView());
  }

  /* Returns a new SearchField instance with name and alias stored as views to the values in this
   * SearchField */
  SearchField View() const {
    if (HasNewAlias()) {
      return SearchField{StringOrView::FromView(NameView()), is_short_name_,
                         StringOrView::FromView(AliasView())};
    }
    return SearchField{StringOrView::FromView(NameView()), is_short_name_};
  }

 private:
  bool HasNewAlias() const {
    return !new_alias_.empty();
  }

  std::string_view NameView() const {
    return name_.view();
  }

  std::string_view AliasView() const {
    return new_alias_.view();
  }

 private:
  StringOrView name_;
  bool is_short_name_;
  StringOrView new_alias_;
};

using SearchFieldsList = std::vector<SearchField>;

struct SearchParams {
  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset = 0;
  size_t limit_total = 10;

  /*
  1. If not set -> return all fields
  2. If set but empty -> no fields should be returned
  3. If set and not empty -> return only these fields
  */
  std::optional<SearchFieldsList> return_fields;

  /*
    Fields that should be also loaded from the document.

    Only one of load_fields and return_fields should be set.
  */
  std::optional<SearchFieldsList> load_fields;

  std::optional<search::SortOption> sort_option;
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
  std::string_view index, query;
  search::QueryParams params;

  std::optional<SearchFieldsList> load_fields;
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
    std::optional<DocId> Remove(std::string_view key);

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
  ShardDocIndex(std::shared_ptr<const DocIndex> index);

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
  std::shared_ptr<const DocIndex> base_;
  std::optional<search::FieldIndices> indices_;
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
  void InitIndex(const OpArgs& op_args, std::string_view name,
                 std::shared_ptr<const DocIndex> index);

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
