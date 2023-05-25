// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/search/search.h"
#include "server/common.h"

namespace dfly {

using SearchDocData = absl::flat_hash_map<std::string, std::string>;
using SerializedSearchDoc = std::pair<std::string /*key*/, SearchDocData>;

struct SearchIndex {
  enum DataType { HASH, JSON };

  // Get numeric OBJ_ code
  uint8_t GetObjCode() const;

  std::string prefix{};
  DataType type{HASH};
};

class ShardSearchIndex {
  using DocId = uint32_t;

  // Manages mapping document ids to keys and vice versa through a simple interface.
  struct DocKeyIndex {
    DocId Add(std::string_view key);
    void Delete(std::string_view key);
    std::string Get(DocId id);

   private:
    absl::flat_hash_map<std::string, DocId> ids;
    std::vector<std::string> keys;
    std::vector<DocId> free_ids;
    DocId next_id = 1;
  };

 public:
  ShardSearchIndex(std::shared_ptr<SearchIndex> index);

  std::vector<SerializedSearchDoc> Search(const OpArgs& op_args,
                                          search::SearchAlgorithm* search_algo);

  static ShardSearchIndex* GetOnShard(std::string_view name);
  static void InitOnShard(const OpArgs& op_args, std::string_view name,
                          std::shared_ptr<SearchIndex> index);

 private:
  void Init(const OpArgs& op_args);

 private:
  std::shared_ptr<const SearchIndex> index_;
  DocKeyIndex key_index_;

  static thread_local absl::flat_hash_map<std::string, ShardSearchIndex> indices_;
};

}  // namespace dfly
