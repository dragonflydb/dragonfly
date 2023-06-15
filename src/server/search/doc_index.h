// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/search/search.h"
#include "server/common.h"
#include "server/table.h"

namespace dfly {

using SearchDocData = absl::flat_hash_map<std::string /*field*/, std::string /*value*/>;

search::FtVector BytesToFtVector(std::string_view value);

struct SerializedSearchDoc {
  std::string key;
  SearchDocData values;
  float knn_distance;
};

struct SearchResult {
  std::vector<SerializedSearchDoc> docs;
  size_t total_hits;
};

struct SearchParams {
  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset;
  size_t limit_total;

  search::FtVector knn_vector;
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

// Stores internal search indices for documents of a document index on a specific shard.
class ShardDocIndex {
  using DocId = search::DocId;

  // DocKeyIndex manages mapping document keys to ids and vice versa through a simple interface.
  struct DocKeyIndex {
    DocId Add(std::string_view key);
    DocId Remove(std::string_view key);
    std::string_view Get(DocId id) const;

   private:
    absl::flat_hash_map<std::string, DocId> ids_;
    std::vector<std::string> keys_;
    std::vector<DocId> free_ids_;
    DocId last_id_ = 0;
  };

 public:
  ShardDocIndex(std::shared_ptr<DocIndex> index);

  // Perform search on all indexed documents and return results.
  SearchResult Search(const OpArgs& op_args, const SearchParams& params,
                      search::SearchAlgorithm* search_algo) const;

  // Initialize index. Traverses all matching documents and assigns ids.
  void Init(const OpArgs& op_args);

  // Return whether base index matches
  bool Matches(std::string_view key, unsigned obj_code) const;

  void AddDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);

 private:
  std::shared_ptr<const DocIndex> base_;
  search::FieldIndices indices_;
  DocKeyIndex key_index_;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  // Get sharded document index by its name
  ShardDocIndex* GetIndex(std::string_view name);
  // Init index: create shard local state for given index with given name
  void InitIndex(const OpArgs& op_args, std::string_view name, std::shared_ptr<DocIndex> index);

  void AddDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);

 private:
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;
};

}  // namespace dfly
