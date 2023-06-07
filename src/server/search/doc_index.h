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

namespace dfly {

using SearchDocData = absl::flat_hash_map<std::string /*field*/, std::string /*value*/>;
using SerializedSearchDoc = std::pair<std::string /*key*/, SearchDocData>;

struct SearchResult {
  std::vector<SerializedSearchDoc> docs;
  size_t total_hits;
};

struct SearchParams {
  size_t limit_offset, limit_total;
};

// Stores basic info about a document index.
struct DocIndex {
  enum DataType { HASH, JSON };

  // Get numeric OBJ_ code
  uint8_t GetObjCode() const;

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
    void Delete(std::string_view key);
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

 private:
  std::shared_ptr<const DocIndex> base_;
  search::FieldIndices indices_;
  DocKeyIndex key_index_;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  ShardDocIndex* Get(std::string_view name) const;
  void Init(const OpArgs& op_args, std::string_view name, std::shared_ptr<DocIndex> index);

 private:
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;
};

}  // namespace dfly
