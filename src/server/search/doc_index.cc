// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_index.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"

extern "C" {
#include "redis/object.h"
};

namespace dfly {

using namespace std;

namespace {

template <typename F>
void TraverseAllMatching(const DocIndex& index, const OpArgs& op_args, F&& f) {
  auto& db_slice = op_args.shard->db_slice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));
  auto [prime_table, _] = db_slice.GetTables(op_args.db_cntx.db_index);

  string scratch;
  auto cb = [&](PrimeTable::iterator it) {
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

ShardDocIndex::DocId ShardDocIndex::DocKeyIndex::Add(string_view key) {
  DCHECK_EQ(ids_.count(key), 0u);

  DocId id;
  if (!free_ids_.empty()) {
    id = free_ids_.back();
    free_ids_.pop_back();
    keys_[id] = key;
  } else {
    id = ++last_id_;
    DCHECK_EQ(keys_.size(), id);
    keys_.emplace_back(key);
  }

  ids_[key] = id;
  return id;
}

void ShardDocIndex::DocKeyIndex::Delete(string_view key) {
  DCHECK_GT(ids_.count(key), 0u);

  DocId id = ids_.find(key)->second;
  keys_[id] = "";
  ids_.erase(key);
  free_ids_.push_back(id);
}

string ShardDocIndex::DocKeyIndex::Get(DocId id) {
  DCHECK_LT(id, keys_.size());
  DCHECK_GT(keys_[id].size(), 0u);

  return keys_[id];
}

uint8_t DocIndex::GetObjCode() const {
  return type == JSON ? OBJ_JSON : OBJ_HASH;
}

ShardDocIndex::ShardDocIndex(shared_ptr<DocIndex> index) : base_{index} {
}

void ShardDocIndex::Init(const OpArgs& op_args) {
  TraverseAllMatching(*base_, op_args,
                      [this](string_view key, BaseAccessor* doc) { key_index_.Add(key); });
}

vector<SerializedSearchDoc> ShardDocIndex::Search(const OpArgs& op_args,
                                                  search::SearchAlgorithm* search_algo) {
  vector<SerializedSearchDoc> out;
  TraverseAllMatching(*base_, op_args, [search_algo, &out](string_view key, BaseAccessor* doc) {
    if (search_algo->Check(doc))
      out.emplace_back(key, doc->Serialize());
  });
  return out;
}

ShardDocIndex* ShardDocIndices::Get(string_view name) {
  auto it = indices_.find(name);
  return it != indices_.end() ? it->second.get() : nullptr;
}

void ShardDocIndices::Init(const OpArgs& op_args, std::string_view name,
                           shared_ptr<DocIndex> index_ptr) {
  auto shard_index = make_unique<ShardDocIndex>(index_ptr);
  auto [it, _] = indices_.emplace(name, move(shard_index));
  it->second->Init(op_args);
}

}  // namespace dfly
