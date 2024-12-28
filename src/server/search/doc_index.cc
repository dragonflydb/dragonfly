// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_index.h"

#include <absl/strings/str_join.h>

#include <memory>

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "core/overloaded.h"
#include "core/search/indices.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using facade::ErrorReply;
using nonstd::make_unexpected;

namespace {

template <typename F>
void TraverseAllMatching(const DocIndex& index, const OpArgs& op_args, F&& f) {
  auto& db_slice = op_args.GetDbSlice();
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
    f(key, *accessor);
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = prime_table->Traverse(cursor, cb);
  } while (cursor);
}

}  // namespace

bool SerializedSearchDoc::operator<(const SerializedSearchDoc& other) const {
  return this->score < other.score;
}

bool SerializedSearchDoc::operator>=(const SerializedSearchDoc& other) const {
  return this->score >= other.score;
}

bool SearchParams::ShouldReturnField(std::string_view alias) const {
  auto cb = [alias](const auto& entry) { return entry.GetShortName() == alias; };
  return !return_fields || any_of(return_fields->begin(), return_fields->end(), cb);
}

string_view SearchFieldTypeToString(search::SchemaField::FieldType type) {
  switch (type) {
    case search::SchemaField::TAG:
      return "TAG";
    case search::SchemaField::TEXT:
      return "TEXT";
    case search::SchemaField::NUMERIC:
      return "NUMERIC";
    case search::SchemaField::VECTOR:
      return "VECTOR";
  }
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

  // STOPWORDS
  absl::StrAppend(&out, " STOPWORDS ", base_index.options.stopwords.size());
  for (const auto& sw : base_index.options.stopwords)
    absl::StrAppend(&out, " ", sw);

  absl::StrAppend(&out, " SCHEMA");
  for (const auto& [fident, finfo] : base_index.schema.fields) {
    // Store field name, alias and type
    absl::StrAppend(&out, " ", fident, " AS ", finfo.short_name, " ",
                    SearchFieldTypeToString(finfo.type));

    // Store shared field flags
    if (finfo.flags & search::SchemaField::SORTABLE)
      absl::StrAppend(&out, " SORTABLE");

    if (finfo.flags & search::SchemaField::NOINDEX)
      absl::StrAppend(&out, " NOINDEX");

    // Store specific params
    Overloaded info{
        [](monostate) {},
        [out = &out](const search::SchemaField::VectorParams& params) {
          auto sim = params.sim == search::VectorSimilarity::L2 ? "L2" : "COSINE";
          absl::StrAppend(out, " ", params.use_hnsw ? "HNSW" : "FLAT", " 6 ", "DIM ", params.dim,
                          " DISTANCE_METRIC ", sim, " INITIAL_CAP ", params.capacity);
        },
        [out = &out](const search::SchemaField::TagParams& params) {
          absl::StrAppend(out, " ", "SEPARATOR", " ", string{params.separator});
          if (params.case_sensitive)
            absl::StrAppend(out, " ", "CASESENSITIVE");
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

std::optional<ShardDocIndex::DocId> ShardDocIndex::DocKeyIndex::Remove(string_view key) {
  auto it = ids_.extract(key);
  if (!it) {
    return std::nullopt;
  }

  const DocId id = it.mapped();
  keys_[id] = "";
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

ShardDocIndex::ShardDocIndex(shared_ptr<const DocIndex> index)
    : base_{std::move(index)}, key_index_{} {
}

void ShardDocIndex::Rebuild(const OpArgs& op_args, PMR_NS::memory_resource* mr) {
  key_index_ = DocKeyIndex{};
  indices_.emplace(base_->schema, base_->options, mr);

  auto cb = [this](string_view key, const BaseAccessor& doc) {
    DocId id = key_index_.Add(key);
    if (!indices_->Add(id, doc)) {
      key_index_.Remove(key);
    }
  };

  TraverseAllMatching(*base_, op_args, cb);

  VLOG(1) << "Indexed " << key_index_.Size() << " docs on " << base_->prefix;
}

void ShardDocIndex::AddDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  if (!indices_)
    return;

  auto accessor = GetAccessor(db_cntx, pv);
  DocId id = key_index_.Add(key);
  if (!indices_->Add(id, *accessor)) {
    key_index_.Remove(key);
  }
}

void ShardDocIndex::RemoveDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  if (!indices_)
    return;

  auto accessor = GetAccessor(db_cntx, pv);
  auto id = key_index_.Remove(key);
  if (id) {
    indices_->Remove(id.value(), *accessor);
  }
}

bool ShardDocIndex::Matches(string_view key, unsigned obj_code) const {
  return base_->Matches(key, obj_code);
}

SearchFieldsList ToSV(const search::Schema& schema, const std::optional<SearchFieldsList>& fields) {
  SearchFieldsList sv_fields;
  if (fields) {
    sv_fields.reserve(fields->size());
    for (const auto& field : fields.value()) {
      sv_fields.push_back(field.View());
    }
  }
  return sv_fields;
}

SearchResult ShardDocIndex::Search(const OpArgs& op_args, const SearchParams& params,
                                   search::SearchAlgorithm* search_algo) const {
  auto& db_slice = op_args.GetDbSlice();
  auto search_results = search_algo->Search(&*indices_, params.limit_offset + params.limit_total);

  if (!search_results.error.empty())
    return SearchResult{facade::ErrorReply{std::move(search_results.error)}};

  SearchFieldsList fields_to_load = ToSV(
      base_->schema, params.ShouldReturnAllFields() ? params.load_fields : params.return_fields);

  vector<SerializedSearchDoc> out;
  out.reserve(search_results.ids.size());

  size_t expired_count = 0;
  for (size_t i = 0; i < search_results.ids.size(); i++) {
    auto key = key_index_.Get(search_results.ids[i]);
    auto it = db_slice.FindReadOnly(op_args.db_cntx, key, base_->GetObjCode());

    if (!it || !IsValid(*it)) {  // Item must have expired
      expired_count++;
      continue;
    }

    auto accessor = GetAccessor(op_args.db_cntx, (*it)->second);

    SearchDocData doc_data;
    if (params.ShouldReturnAllFields()) {
      /*
      In this case we need to load the whole document or loaded fields.
      For JSON indexes it would be {"$", <the whole document as string>}
      */
      doc_data = accessor->SerializeDocument(base_->schema);

      SearchDocData loaded_fields = accessor->Serialize(base_->schema, fields_to_load);
      doc_data.insert(std::make_move_iterator(loaded_fields.begin()),
                      std::make_move_iterator(loaded_fields.end()));
    } else {
      /* Load only specific fields */
      doc_data = accessor->Serialize(base_->schema, fields_to_load);
    }

    auto score = search_results.scores.empty() ? monostate{} : std::move(search_results.scores[i]);
    out.push_back(SerializedSearchDoc{string{key}, std::move(doc_data), std::move(score)});
  }

  return SearchResult{search_results.total - expired_count, std::move(out),
                      std::move(search_results.profile)};
}

using SortIndiciesFieldsList =
    std::vector<std::pair<string_view /*identifier*/, string_view /*alias*/>>;

std::pair<SearchFieldsList, SortIndiciesFieldsList> PreprocessAggregateFields(
    const search::Schema& schema, const AggregateParams& params,
    const std::optional<SearchFieldsList>& load_fields) {
  auto is_sortable = [&schema](std::string_view fident) {
    auto it = schema.fields.find(fident);
    return it != schema.fields.end() && (it->second.flags & search::SchemaField::SORTABLE);
  };

  absl::flat_hash_map<std::string_view, SearchField> fields_by_identifier;
  absl::flat_hash_map<std::string_view, std::string_view> sort_indicies_aliases;
  fields_by_identifier.reserve(schema.field_names.size());
  sort_indicies_aliases.reserve(schema.field_names.size());

  for (const auto& [fname, fident] : schema.field_names) {
    if (!is_sortable(fident)) {
      fields_by_identifier[fident] = {StringOrView::FromView(fident), true,
                                      StringOrView::FromView(fname)};
    } else {
      sort_indicies_aliases[fident] = fname;
    }
  }

  if (load_fields) {
    for (const auto& field : load_fields.value()) {
      const auto& fident = field.GetIdentifier(schema, false);
      if (!is_sortable(fident)) {
        fields_by_identifier[fident] = field.View();
      } else {
        sort_indicies_aliases[fident] = field.GetShortName();
      }
    }
  }

  SearchFieldsList fields;
  fields.reserve(fields_by_identifier.size());
  for (auto& [_, field] : fields_by_identifier) {
    fields.emplace_back(std::move(field));
  }

  SortIndiciesFieldsList sort_fields;
  sort_fields.reserve(sort_indicies_aliases.size());
  for (auto& [fident, fname] : sort_indicies_aliases) {
    sort_fields.emplace_back(fident, fname);
  }

  return {std::move(fields), std::move(sort_fields)};
}

vector<SearchDocData> ShardDocIndex::SearchForAggregator(
    const OpArgs& op_args, const AggregateParams& params,
    search::SearchAlgorithm* search_algo) const {
  auto& db_slice = op_args.GetDbSlice();
  auto search_results = search_algo->Search(&*indices_);

  if (!search_results.error.empty())
    return {};

  auto [fields_to_load, sort_indicies] =
      PreprocessAggregateFields(base_->schema, params, params.load_fields);

  vector<absl::flat_hash_map<string, search::SortableValue>> out;
  for (DocId doc : search_results.ids) {
    auto key = key_index_.Get(doc);
    auto it = db_slice.FindReadOnly(op_args.db_cntx, key, base_->GetObjCode());

    if (!it || !IsValid(*it))  // Item must have expired
      continue;

    auto accessor = GetAccessor(op_args.db_cntx, (*it)->second);

    SearchDocData extracted_sort_indicies;
    extracted_sort_indicies.reserve(sort_indicies.size());
    for (const auto& [fident, fname] : sort_indicies) {
      extracted_sort_indicies[fname] = indices_->GetSortIndexValue(doc, fident);
    }

    SearchDocData loaded = accessor->Serialize(base_->schema, fields_to_load);

    out.emplace_back(make_move_iterator(extracted_sort_indicies.begin()),
                     make_move_iterator(extracted_sort_indicies.end()));
    out.back().insert(make_move_iterator(loaded.begin()), make_move_iterator(loaded.end()));
  }

  return out;
}

DocIndexInfo ShardDocIndex::GetInfo() const {
  return {*base_, key_index_.Size()};
}

io::Result<StringVec, ErrorReply> ShardDocIndex::GetTagVals(string_view field) const {
  search::BaseIndex* base_index = indices_->GetIndex(field);
  if (base_index == nullptr) {
    return make_unexpected(ErrorReply{"-No such field"});
  }

  search::TagIndex* tag_index = dynamic_cast<search::TagIndex*>(base_index);
  if (tag_index == nullptr) {
    return make_unexpected(ErrorReply{"-Not a tag field"});
  }

  return tag_index->GetTerms();
}

ShardDocIndices::ShardDocIndices() : local_mr_{ServerState::tlocal()->data_heap()} {
}

ShardDocIndex* ShardDocIndices::GetIndex(string_view name) {
  auto it = indices_.find(name);
  return it != indices_.end() ? it->second.get() : nullptr;
}

void ShardDocIndices::InitIndex(const OpArgs& op_args, std::string_view name,
                                shared_ptr<const DocIndex> index_ptr) {
  auto shard_index = make_unique<ShardDocIndex>(std::move(index_ptr));
  auto [it, _] = indices_.emplace(name, std::move(shard_index));

  // Don't build while loading, shutting down, etc.
  // After loading, indices are rebuilt separately
  if (ServerState::tlocal()->gstate() == GlobalState::ACTIVE)
    it->second->Rebuild(op_args, &local_mr_);

  op_args.GetDbSlice().SetDocDeletionCallback(
      [this](string_view key, const DbContext& cntx, const PrimeValue& pv) {
        RemoveDoc(key, cntx, pv);
      });
}

bool ShardDocIndices::DropIndex(string_view name) {
  auto it = indices_.find(name);
  if (it == indices_.end())
    return false;

  DropIndexCache(*it->second);
  indices_.erase(it);

  return true;
}

void ShardDocIndices::DropAllIndices() {
  for (auto it = indices_.begin(); it != indices_.end(); it++) {
    DropIndexCache(*it->second);
  }
  indices_.clear();
}

void ShardDocIndices::DropIndexCache(const dfly::ShardDocIndex& shard_doc_index) {
  auto info = shard_doc_index.GetInfo();
  for (const auto& [fident, field] : info.base_index.schema.fields)
    JsonAccessor::RemoveFieldFromCache(fident);
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
