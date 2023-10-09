// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_index.h"

#include <absl/strings/str_join.h>

#include <memory>

#include "base/logging.h"
#include "core/overloaded.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"
#include "server/server_state.h"

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

const absl::flat_hash_map<string_view, search::SchemaField::FieldType> kSchemaTypes = {
    {"TAG"sv, search::SchemaField::TAG},
    {"TEXT"sv, search::SchemaField::TEXT},
    {"NUMERIC"sv, search::SchemaField::NUMERIC},
    {"VECTOR"sv, search::SchemaField::VECTOR}};

}  // namespace

bool SerializedSearchDoc::operator<(const SerializedSearchDoc& other) const {
  return this->score < other.score;
}

bool SerializedSearchDoc::operator>=(const SerializedSearchDoc& other) const {
  return this->score >= other.score;
}

bool SearchParams::ShouldReturnField(std::string_view field) const {
  auto cb = [field](const auto& entry) { return entry.first == field; };
  return !return_fields || any_of(return_fields->begin(), return_fields->end(), cb);
}

optional<search::SchemaField::FieldType> ParseSearchFieldType(string_view name) {
  auto it = kSchemaTypes.find(name);
  return it != kSchemaTypes.end() ? make_optional(it->second) : nullopt;
}

string_view SearchFieldTypeToString(search::SchemaField::FieldType type) {
  for (auto [it_name, it_type] : kSchemaTypes)
    if (it_type == type)
      return it_name;
  ABSL_UNREACHABLE();
  return "";
}

string DocIndexInfo::BuildRestoreCommand() const {
  std::string out;

  // ON HASH/JSON
  absl::StrAppend(&out, "ON", " ", base_index.type == DocIndex::HASH ? "HASH" : "JSON");

  // optional PREFIX 1 *prefix*
  if (!base_index.prefix.empty())
    absl::StrAppend(&out, " PREFIX", " 1 ", base_index.prefix);

  absl::StrAppend(&out, " SCHEMA");
  for (const auto& [fident, finfo] : base_index.schema.fields) {
    absl::StrAppend(&out, " ", fident, " AS ", finfo.short_name, " ",
                    SearchFieldTypeToString(finfo.type));

    Overloaded info{
        [](monostate) {},
        [out = &out](const search::SchemaField::VectorParams& params) {
          auto sim = params.sim == search::VectorSimilarity::L2 ? "L2" : "COSINE";
          absl::StrAppend(out, " ", params.use_hnsw ? "HNSW" : "FLAT", " 6 ", "DIM ", params.dim,
                          " DISTANCE_METRIC ", sim, " INITIAL_CAP ", params.capacity);
        },
    };
    visit(info, finfo.special_params);
  }

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

size_t ShardDocIndex::DocKeyIndex::Size() const {
  return ids_.size();
}

uint8_t DocIndex::GetObjCode() const {
  return type == JSON ? OBJ_JSON : OBJ_HASH;
}

bool DocIndex::Matches(string_view key, unsigned obj_code) const {
  return obj_code == GetObjCode() && key.rfind(prefix, 0) == 0;
}

ShardDocIndex::ShardDocIndex(shared_ptr<DocIndex> index)
    : base_{std::move(index)}, indices_{{}, nullptr}, key_index_{} {
}

void ShardDocIndex::Rebuild(const OpArgs& op_args, PMR_NS::memory_resource* mr) {
  key_index_ = DocKeyIndex{};
  indices_ = search::FieldIndices{base_->schema, mr};

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
  auto search_results = search_algo->Search(&indices_, params.limit_offset + params.limit_total);

  if (!search_results.error.empty())
    return SearchResult{facade::ErrorReply{std::move(search_results.error)}};

  vector<SerializedSearchDoc> out;
  out.reserve(search_results.ids.size());

  size_t expired_count = 0;
  for (size_t i = 0; i < search_results.ids.size(); i++) {
    auto key = key_index_.Get(search_results.ids[i]);
    auto it = db_slice.Find(op_args.db_cntx, key, base_->GetObjCode());

    if (!it || !IsValid(*it)) {  // Item must have expired
      expired_count++;
      continue;
    }

    auto accessor = GetAccessor(op_args.db_cntx, (*it)->second);
    auto doc_data = params.return_fields ? accessor->Serialize(base_->schema, *params.return_fields)
                                         : accessor->Serialize(base_->schema);

    auto score =
        search_results.scores.empty() ? std::monostate{} : std::move(search_results.scores[i]);
    out.push_back(SerializedSearchDoc{string{key}, std::move(doc_data), std::move(score)});
  }

  return SearchResult{search_results.total - expired_count, std::move(out),
                      std::move(search_results.profile)};
}

DocIndexInfo ShardDocIndex::GetInfo() const {
  return {*base_, key_index_.Size()};
}

ShardDocIndices::ShardDocIndices() : local_mr_{ServerState::tlocal()->data_heap()} {
}

ShardDocIndex* ShardDocIndices::GetIndex(string_view name) {
  auto it = indices_.find(name);
  return it != indices_.end() ? it->second.get() : nullptr;
}

void ShardDocIndices::InitIndex(const OpArgs& op_args, std::string_view name,
                                shared_ptr<DocIndex> index_ptr) {
  auto shard_index = make_unique<ShardDocIndex>(index_ptr);
  auto [it, _] = indices_.emplace(name, move(shard_index));

  // Don't build while loading, shutting down, etc.
  // After loading, indices are rebuilt separately
  if (ServerState::tlocal()->gstate() == GlobalState::ACTIVE)
    it->second->Rebuild(op_args, &local_mr_);

  op_args.shard->db_slice().SetDocDeletionCallback(
      [this](string_view key, const DbContext& cntx, const PrimeValue& pv) {
        RemoveDoc(key, cntx, pv);
      });
}

bool ShardDocIndices::DropIndex(string_view name) {
  auto it = indices_.find(name);
  if (it == indices_.end())
    return false;

  // Clean caches that might have data from this index
  auto info = it->second->GetInfo();
  for (const auto& [fident, field] : info.base_index.schema.fields)
    JsonAccessor::RemoveFieldFromCache(fident);

  indices_.erase(it);
  return true;
}

void ShardDocIndices::RebuildAllIndices(const OpArgs& op_args) {
  for (auto& [_, ptr] : indices_)
    ptr->Rebuild(op_args, &local_mr_);
}

vector<string> ShardDocIndices::GetIndexNames() const {
  vector<string> names{};
  names.reserve(indices_.size());
  for (const auto& [name, ptr] : indices_)
    names.push_back(name);
  return names;
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

size_t ShardDocIndices::GetUsedMemory() const {
  return local_mr_.used();
}

SearchStats ShardDocIndices::GetStats() const {
  size_t total_entries = 0;
  for (const auto& [_, index] : indices_)
    total_entries += index->GetInfo().num_docs;

  return {GetUsedMemory(), indices_.size(), total_entries};
}

}  // namespace dfly
