// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_index.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/search/search_accessor.h"

namespace dfly {

namespace {

template <typename F>
void TraverseAllMatching(const SearchIndex& index, const OpArgs& op_args, F&& f) {
  auto& db_slice = op_args.shard->db_slice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));
  auto [prime_table, _] = db_slice.GetTables(op_args.db_cntx.db_index);

  string scratch;
  auto cb = [&](PrimeTable::iterator it) {
    // Check entry is hash
    const PrimeValue& pv = it->second;
    if (pv.ObjType() != index.GetObjCode())
      return;

    string_view key = it->first.GetSlice(&scratch);
    if (key.rfind(index.prefix, 0) != 0)
      return;

    auto accessor = GetAccessor(op_args, pv);
    f(key, accessor.get());
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = prime_table->Traverse(cursor, cb);
  } while (cursor);
}

}  // namespace

using namespace std;

ShardSearchIndex::DocId ShardSearchIndex::DocKeyIndex::Add(string_view key) {
  DCHECK_EQ(ids.count(key), 0u);

  DocId id;
  if (!free_ids.empty()) {
    id = free_ids.back();
    free_ids.pop_back();
    keys[id] = key;
  } else {
    id = next_id++;
    DCHECK_EQ(keys.size(), id);
    keys.emplace_back(key);
  }

  ids[key] = id;
  return id;
}

void ShardSearchIndex::DocKeyIndex::Delete(string_view key) {
  DCHECK_GT(ids.count(key), 0u);

  DocId id = ids.find(key)->second;
  keys[id] = "";
  ids.erase(key);
  free_ids.push_back(id);
}

string ShardSearchIndex::DocKeyIndex::Get(DocId id) {
  DCHECK_LT(id, keys.size());
  DCHECK_GT(keys[id].size(), 0u);

  return keys[id];
}

uint8_t SearchIndex::GetObjCode() const {
  return type == JSON ? OBJ_JSON : OBJ_HASH;
}

ShardSearchIndex::ShardSearchIndex(shared_ptr<SearchIndex> index) : index_{index} {
}

void ShardSearchIndex::Init(const OpArgs& op_args) {
  TraverseAllMatching(*index_, op_args,
                      [this](string_view key, BaseAccessor* doc) { key_index_.Add(key); });
}

vector<SerializedSearchDoc> ShardSearchIndex::Search(const OpArgs& op_args,
                                                     search::SearchAlgorithm* search_algo) {
  vector<SerializedSearchDoc> out;
  TraverseAllMatching(*index_, op_args, [search_algo, &out](string_view key, BaseAccessor* doc) {
    if (search_algo->Check(doc))
      out.emplace_back(key, doc->Serialize());
  });
  return {};
}

ShardSearchIndex* ShardSearchIndex::GetOnShard(string_view name) {
  auto it = indices_.find(name);
  return it != indices_.end() ? &it->second : nullptr;
}

void ShardSearchIndex::InitOnShard(const OpArgs& op_args, std::string_view name,
                                   shared_ptr<SearchIndex> index_ptr) {
  auto [it, _] = indices_.emplace(name, index_ptr);
  it->second.Init(op_args);
}

thread_local absl::flat_hash_map<std::string, ShardSearchIndex> ShardSearchIndex::indices_{};

}  // namespace dfly
