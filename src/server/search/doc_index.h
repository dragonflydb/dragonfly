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

#include "core/search/search.h"
#include "server/common.h"
#include "server/table.h"

namespace dfly {

using SearchDocData = absl::flat_hash_map<std::string /*field*/, std::string /*value*/>;

std::optional<search::SchemaField::FieldType> ParseSearchFieldType(std::string_view name);
std::string_view SearchFieldTypeToString(search::SchemaField::FieldType);

struct SerializedSearchDoc {
  std::string key;
  SearchDocData values;
  float knn_distance;
};

struct SearchResult {
  SearchResult() = default;

  SearchResult(std::vector<SerializedSearchDoc> docs, size_t total_hits,
               std::optional<search::AlgorithmProfile> profile)
      : docs{std::move(docs)}, total_hits{total_hits}, profile{std::move(profile)} {
  }

  SearchResult(facade::ErrorReply error) : error{std::move(error)} {
  }

  std::vector<SerializedSearchDoc> docs;
  size_t total_hits;
  std::optional<search::AlgorithmProfile> profile;

  std::optional<facade::ErrorReply> error;
};

struct SearchParams {
  using FieldReturnList =
      std::vector<std::pair<std::string /*identifier*/, std::string /*short name*/>>;

  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset;
  size_t limit_total;

  // Set but empty means no fields should be returned
  std::optional<FieldReturnList> return_fields;
  search::QueryParams query_params;

  bool IdsOnly() const {
    return return_fields && return_fields->empty();
  }

  bool ShouldReturnField(std::string_view field) const;
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
  size_t num_docs;

  // Build original ft.create command that can be used to re-create this index
  std::string BuildRestoreCommand() const;
};

// Stores internal search indices for documents of a document index on a specific shard.
class ShardDocIndex {
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

  // Clears internal data. Traverses all matching documents and assigns ids.
  void Rebuild(const OpArgs& op_args);

  // Return whether base index matches
  bool Matches(std::string_view key, unsigned obj_code) const;

  void AddDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);

  DocIndexInfo GetInfo() const;

 private:
  std::shared_ptr<const DocIndex> base_;
  search::FieldIndices indices_;
  DocKeyIndex key_index_;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  // Get sharded document index by its name or nullptr if not found
  ShardDocIndex* GetIndex(std::string_view name);

  // Init index: create shard local state for given index with given name.
  // Build if instance is in active state.
  void InitIndex(const OpArgs& op_args, std::string_view name, std::shared_ptr<DocIndex> index);

  // Drop index, return true if it existed and was dropped
  bool DropIndex(std::string_view name);

  // Rebuild all indices
  void RebuildAllIndices(const OpArgs& op_args);

  std::vector<std::string> GetIndexNames() const;

  void AddDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);

 private:
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;
};

#ifdef __APPLE__
inline ShardDocIndex* ShardDocIndices::GetIndex(std::string_view name) {
  return nullptr;
}

inline void ShardDocIndices::InitIndex(const OpArgs& op_args, std::string_view name,
                                       std::shared_ptr<DocIndex> index) {
}

inline bool ShardDocIndices::DropIndex(std::string_view name) {
  return false;
}

inline void ShardDocIndices::RebuildAllIndices(const OpArgs& op_args) {
}

inline std::vector<std::string> ShardDocIndices::GetIndexNames() const {
  return {};
}

inline void ShardDocIndices::AddDoc(std::string_view key, const DbContext& db_cnt,
                                    const PrimeValue& pv) {
}

inline void ShardDocIndices::RemoveDoc(std::string_view key, const DbContext& db_cnt,
                                       const PrimeValue& pv) {
}

#endif  // __APPLE__
}  // namespace dfly
