// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_index.h"

#include <memory>

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

    auto accessor = GetAccessor(op_args.db_cntx, pv);
    f(key, accessor.get());
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = prime_table->Traverse(cursor, cb);
  } while (cursor);
}

}  // namespace

search::FtVector BytesToFtVector(string_view value) {
  DCHECK_EQ(value.size() % sizeof(float), 0u);
  search::FtVector out(value.size() / sizeof(float));

  // Create copy for aligned access
  unique_ptr<float[]> float_ptr = make_unique<float[]>(out.size());
  memcpy(float_ptr.get(), value.data(), value.size());

  for (size_t i = 0; i < out.size(); i++)
    out[i] = float_ptr[i];
  return out;
}

ShardDocIndex::DocId ShardDocIndex::DocKeyIndex::Add(string_view key) {
  DCHECK_EQ(ids_.count(key), 0u);

  DocId id;
  if (!free_ids_.empty()) {
    id = free_ids_.back();
    free_ids_.pop_back();
    keys_[id] = key;
  } else {
    id = last_id_++;
    DCHECK_EQ(keys_.size(), id);
    keys_.emplace_back(key);
  }

  ids_[key] = id;
  return id;
}

ShardDocIndex::DocId ShardDocIndex::DocKeyIndex::Remove(string_view key) {
  DCHECK_GT(ids_.count(key), 0u);

  DocId id = ids_.find(key)->second;
  keys_[id] = "";
  ids_.erase(key);
  free_ids_.push_back(id);

  return id;
}

string_view ShardDocIndex::DocKeyIndex::Get(DocId id) const {
  DCHECK_LT(id, keys_.size());
  DCHECK_GT(keys_[id].size(), 0u);

  return keys_[id];
}

uint8_t DocIndex::GetObjCode() const {
  return type == JSON ? OBJ_JSON : OBJ_HASH;
}

bool DocIndex::Matches(string_view key, unsigned obj_code) const {
  return obj_code == GetObjCode() && key.rfind(prefix, 0) == 0;
}

ShardDocIndex::ShardDocIndex(shared_ptr<DocIndex> index)
    : base_{index}, indices_{index->schema}, key_index_{} {
}

void ShardDocIndex::Init(const OpArgs& op_args) {
  auto cb = [this](string_view key, BaseAccessor* doc) { indices_.Add(key_index_.Add(key), doc); };
  TraverseAllMatching(*base_, op_args, cb);
}

void ShardDocIndex::AddDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  auto accessor = GetAccessor(db_cntx, pv);
  indices_.Add(key_index_.Add(key), accessor.get());
}

void ShardDocIndex::RemoveDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  auto accessor = GetAccessor(db_cntx, pv);
  DocId id = key_index_.Remove(key);
  indices_.Remove(id, accessor.get());
}

bool ShardDocIndex::Matches(string_view key, unsigned obj_code) const {
  return base_->Matches(key, obj_code);
}

SearchResult ShardDocIndex::Search(const OpArgs& op_args, const SearchParams& params,
                                   search::SearchAlgorithm* search_algo) const {
  auto& db_slice = op_args.shard->db_slice();
  auto search_results = search_algo->Search(&indices_);

  size_t serialize_count = min(search_results.ids.size(), params.limit_offset + params.limit_total);

  vector<SerializedSearchDoc> out(serialize_count);
  for (size_t i = 0; i < serialize_count; i++) {
    auto key = key_index_.Get(search_results.ids[i]);
    auto it = db_slice.Find(op_args.db_cntx, key, base_->GetObjCode());
    CHECK(it) << "Expected key: " << key << " to exist";

    VLOG(0) << "Fetched " << key;

    auto doc_data = GetAccessor(op_args.db_cntx, (*it)->second)->Serialize(base_->schema);
    float score = search_results.knn_distances.empty() ? 0 : search_results.knn_distances[i];

    out[i] = SerializedSearchDoc{string{key}, std::move(doc_data), score};
  }

  return SearchResult{std::move(out), search_results.ids.size()};
}

ShardDocIndex* ShardDocIndices::GetIndex(string_view name) {
  auto it = indices_.find(name);
  return it != indices_.end() ? it->second.get() : nullptr;
}

void ShardDocIndices::InitIndex(const OpArgs& op_args, std::string_view name,
                                shared_ptr<DocIndex> index_ptr) {
  auto shard_index = make_unique<ShardDocIndex>(index_ptr);
  auto [it, _] = indices_.emplace(name, move(shard_index));
  it->second->Init(op_args);

  op_args.shard->db_slice().SetDocDeletionCallback(
      [this](string_view key, const DbContext& cntx, const PrimeValue& pv) {
        RemoveDoc(key, cntx, pv);
      });
}

void ShardDocIndices::AddDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  for (auto& [_, index] : indices_) {
    if (index->Matches(key, pv.ObjType()))
      index->AddDoc(key, db_cntx, pv);
  }
}

void ShardDocIndices::RemoveDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  for (auto& [_, index] : indices_) {
    if (index->Matches(key, pv.ObjType()))
      index->RemoveDoc(key, db_cntx, pv);
  }
}

}  // namespace dfly
