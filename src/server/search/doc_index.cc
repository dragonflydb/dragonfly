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
#include "server/family_utils.h"
#include "server/search/doc_accessors.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using facade::ErrorReply;
using nonstd::make_unexpected;

namespace {

using namespace search;

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

bool IsSortableField(std::string_view field_identifier, const search::Schema& schema) {
  auto it = schema.fields.find(field_identifier);
  return it != schema.fields.end() && (it->second.flags & search::SchemaField::SORTABLE);
}

SearchFieldsList ToSV(const search::Schema& schema, const std::optional<SearchFieldsList>& fields,
                      const absl::flat_hash_set<std::string_view>& skip_fields) {
  SearchFieldsList sv_fields;
  if (fields) {
    sv_fields.reserve(fields->size());
    for (const auto& field : fields.value()) {
      if (skip_fields.contains(field.NameView())) {
        continue;  // Skip the field if it is in the skip_fields set
      }
      sv_fields.push_back(field.View());
    }
  }
  return sv_fields;
}

using SortIndiciesFieldsList =
    std::vector<std::pair<string_view /*identifier*/, string_view /*alias*/>>;

std::pair<SearchFieldsList, SortIndiciesFieldsList> PreprocessAggregateFields(
    const Schema& schema, const AggregateParams& params,
    const std::optional<SearchFieldsList>& load_fields) {
  absl::flat_hash_map<std::string_view, SearchField> fields_by_identifier;
  absl::flat_hash_map<std::string_view, std::string_view> sort_indicies_aliases;
  fields_by_identifier.reserve(schema.field_names.size());
  sort_indicies_aliases.reserve(schema.field_names.size());

  for (const auto& [fname, fident] : schema.field_names) {
    if (!IsSortableField(fident, schema)) {
      fields_by_identifier[fident] = {StringOrView::FromView(fident), true,
                                      StringOrView::FromView(fname)};
    } else {
      sort_indicies_aliases[fident] = fname;
    }
  }

  if (load_fields) {
    for (const auto& field : load_fields.value()) {
      const auto& fident = field.GetIdentifier(schema, false);
      if (!IsSortableField(fident, schema)) {
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

// This method can change the order of the elements and also resize the vectors in search_result
std::vector<SerializedSearchDoc> SortSearchResultForNonSortableField(
    std::string_view field_alias, SortOrder sort_order, size_t shard_limit,
    const ShardDocIndex::DocKeyIndex& key_index,
    absl::FunctionRef<std::optional<SortableValue>(std::size_t)> get_sortable_field_value,
    SearchAlrgorithmResult* search_result) {
  std::vector<SortableValue> sortable_values;
  std::vector<size_t> ids_to_sort;

  const size_t initial_size = search_result->ids.size();
  sortable_values.reserve(initial_size);
  ids_to_sort.reserve(initial_size);

  for (size_t i = 0; i < initial_size; i++) {
    auto field_value = get_sortable_field_value(i);
    if (field_value.has_value()) {
      sortable_values.emplace_back(std::move(field_value).value());
      ids_to_sort.emplace_back(i);
    } else {
      sortable_values.emplace_back();
    }
  }

  auto comparator = BuildAscDescComparator<size_t>(
      [&](const size_t& l, const size_t& r) { return sortable_values[l] < sortable_values[r]; },
      [&](const size_t& l, const size_t& r) { return sortable_values[l] > sortable_values[r]; },
      sort_order);

  size_t size = ids_to_sort.size();
  if (shard_limit < size) {
    std::partial_sort(ids_to_sort.begin(), ids_to_sort.begin() + shard_limit, ids_to_sort.end(),
                      std::move(comparator));
    size = shard_limit;
    ids_to_sort.resize(shard_limit);
  } else {
    std::sort(ids_to_sort.begin(), ids_to_sort.end(), std::move(comparator));
  }

  // Now we need to fill the search_result in sorted order
  // And also fill the docs_data with the values
  search_result->RearrangeAccordingToIndexes(ids_to_sort);
  std::vector<SerializedSearchDoc> docs_data(size);
  for (size_t i = 0; i < size; i++) {
    const auto doc_id = search_result->ids[i];
    auto& value = sortable_values[ids_to_sort[i]];

    docs_data[i].key = key_index.Get(doc_id);
    docs_data[i].values[field_alias] = std::move(value);
  }
  return docs_data;
}

// This method can change the order of the elements and also resize the vectors in search_result
std::vector<SerializedSearchDoc> SortSearchResultForSortableField(
    std::string_view field_alias, SortOrder order, size_t shard_limit,
    const BaseSortIndex& sort_index, const ShardDocIndex::DocKeyIndex& key_index,
    SearchAlrgorithmResult* search_result) {
  auto sortable_values = sort_index.Sort(shard_limit, order, search_result);

  const size_t size = search_result->ids.size();
  DCHECK(sortable_values.size() == size);

  std::vector<SerializedSearchDoc> docs_data(size);
  for (size_t i = 0; i < size; i++) {
    const DocId doc_id = search_result->ids[i];
    docs_data[i].key = key_index.Get(doc_id);
    docs_data[i].values[field_alias] = std::move(sortable_values[i]);
  }
  return docs_data;
}

}  // namespace

bool operator==(const SearchField& l, const std::string_view& r) {
  return l.NameView() == r;
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
    Overloaded info{[](monostate) {},
                    [out = &out](const search::SchemaField::VectorParams& params) {
                      auto sim = params.sim == search::VectorSimilarity::L2 ? "L2" : "COSINE";
                      absl::StrAppend(out, " ", params.use_hnsw ? "HNSW" : "FLAT", " 6 ", "DIM ",
                                      params.dim, " DISTANCE_METRIC ", sim, " INITIAL_CAP ",
                                      params.capacity);
                    },
                    [out = &out](const search::SchemaField::TagParams& params) {
                      absl::StrAppend(out, " ", "SEPARATOR", " ", string{params.separator});
                      if (params.case_sensitive)
                        absl::StrAppend(out, " ", "CASESENSITIVE");
                    },
                    [out = &out](const search::SchemaField::TextParams& params) {
                      if (params.with_suffixtrie)
                        absl::StrAppend(out, " ", "WITH_SUFFIXTRIE");
                    }};
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
  // Check that this id was not removed
  DCHECK(id < last_id_ && std::find(free_ids_.begin(), free_ids_.end(), id) == free_ids_.end());

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
  indices_.emplace(base_->schema, base_->options, mr, &synonyms_);

  auto cb = [this](string_view key, const BaseAccessor& doc) {
    DocId id = key_index_.Add(key);
    if (!indices_->Add(id, doc)) {
      key_index_.Remove(key);
    }
  };

  TraverseAllMatching(*base_, op_args, cb);

  VLOG(1) << "Indexed " << key_index_.Size() << " docs on " << base_->prefix;
}

void ShardDocIndex::RebuildForGroup(const OpArgs& op_args, const std::string_view& group_id,
                                    const std::vector<std::string_view>& terms) {
  if (!indices_)
    return;

  absl::flat_hash_set<DocId> docs_to_rebuild;
  std::vector<search::TextIndex*> text_indices = indices_->GetAllTextIndices();

  // Find all documents containing any term from the synonyms group
  for (auto* text_index : text_indices) {
    for (const auto& term : terms) {
      if (const auto* container = text_index->Matching(term)) {
        for (DocId doc_id : *container) {
          docs_to_rebuild.insert(doc_id);
        }
      }
    }
  }

  auto& db_slice = op_args.GetDbSlice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));

  auto update_indices = [&](bool remove) {
    for (DocId doc_id : docs_to_rebuild) {
      std::string_view key = key_index_.Get(doc_id);
      auto it = db_slice.FindReadOnly(op_args.db_cntx, key, base_->GetObjCode());

      if (!it || !IsValid(*it)) {
        continue;
      }

      auto accessor = GetAccessor(op_args.db_cntx, (*it)->second);
      if (remove) {
        indices_->Remove(doc_id, *accessor);
      } else {
        // Add in this case always succeeds, because we are adding the same document again
        [[maybe_unused]] bool res = indices_->Add(doc_id, *accessor);
        DCHECK(res);
      }
    }
  };

  update_indices(true);
  synonyms_.UpdateGroup(group_id, terms);
  update_indices(false);
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

SearchResult ShardDocIndex::Search(const OpArgs& op_args, const SearchParams& params,
                                   search::SearchAlgorithm* search_algo) const {
  auto& db_slice = op_args.GetDbSlice();
  auto search_results = search_algo->Search(&*indices_);

  if (!search_results.error.empty())
    return SearchResult{facade::ErrorReply{std::move(search_results.error)}};

  /* We need to skip the knn score alias from the fields to load,
     because it is not a field in the schema, but an alias for the KNN
     score that is added by the search algorithm.
     If the sort option is provided, we also need to skip the sort field
     from the fields to load, because it will be loaded during sorting. */
  SearchFieldsList fields_to_load = ToSV(
      base_->schema, params.ShouldReturnAllFields() ? params.load_fields : params.return_fields,
      {search_algo->GetKnnScoreAlias(),
       params.sort_option ? params.sort_option->field.NameView() : ""sv});

  const size_t shard_limit = params.limit_offset + params.limit_total;
  const size_t size = std::min(search_results.ids.size(), shard_limit);

  std::vector<SerializedSearchDoc> out;

  bool out_initialized = false;
  if (params.sort_option &&
      params.sort_option->field.NameView() != search_algo->GetKnnScoreAlias()) {
    auto& field = params.sort_option->field;

    auto sorting_result =
        SortSearchResult(op_args, field, shard_limit, params.sort_option->order, &search_results);
    if (!sorting_result) {
      return SearchResult{std::move(sorting_result.error())};
    }

    out = std::move(sorting_result).value();
    out_initialized = true;
  } else {
    // We need to resize the results
    search_results.ids.resize(size);
    search_results.scores.resize(size);
  }

  DCHECK(search_results.ids.size() == size &&
         (search_results.scores.empty() || search_results.scores.size() == size));

  size_t expired_count = 0;
  for (size_t i = 0; i < size; i++) {
    const auto& doc_key = out_initialized ? out[i].key : key_index_.Get(search_results.ids[i]);
    auto it = db_slice.FindReadOnly(op_args.db_cntx, doc_key, base_->GetObjCode());
    if (!it || !IsValid(*it)) {  // Item can be expired
      expired_count++;
      continue;
    }

    if (!out_initialized) {
      out.emplace_back();
      out[i].key = doc_key;
    }

    auto accessor = GetAccessor(op_args.db_cntx, (*it)->second);

    auto& values = out[i].values;
    values.reserve(fields_to_load.size() + 1);

    if (params.ShouldReturnAllFields()) {
      /* In this case we need to load the whole document or loaded fields.
         For JSON indexes it would be {"$", <the whole document as string>} */
      SearchDocData doc_data = accessor->SerializeDocument(base_->schema);
      values.insert(std::make_move_iterator(doc_data.begin()),
                    std::make_move_iterator(doc_data.end()));
    }

    SearchDocData loaded_fields = accessor->Serialize(base_->schema, fields_to_load);
    values.insert(std::make_move_iterator(loaded_fields.begin()),
                  std::make_move_iterator(loaded_fields.end()));

    if (!search_results.scores.empty()) {
      out[i].score = search_results.scores[i];
    }
  }

  return SearchResult{search_results.total - expired_count, std::move(out),
                      std::move(search_results.profile)};
}

io::Result<std::vector<SerializedSearchDoc>, facade::ErrorReply> ShardDocIndex::SortSearchResult(
    const OpArgs& op_args, const SearchField& field, size_t shard_limit,
    search::SortOrder sort_order, search::SearchAlrgorithmResult* search_result) const {
  auto fident = field.GetIdentifier(base_->schema, false);

  if (IsSortableField(fident, base_->schema)) {
    BaseSortIndex* sort_index = indices_ ? indices_->GetSortIndex(fident) : nullptr;
    if (!sort_index) {
      return make_unexpected(
          facade::ErrorReply{absl::StrCat("-Sortable field is not found: "sv, field.NameView())});
    }
    return SortSearchResultForSortableField(field.NameView(), sort_order, shard_limit, *sort_index,
                                            key_index_, search_result);
  }

  auto get_value = [&](size_t index) -> std::optional<SortableValue> {
    DCHECK(index < search_result->ids.size());
    const auto& doc_id = search_result->ids[index];
    const auto& key = key_index_.Get(doc_id);
    auto it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, base_->GetObjCode());
    if (it && IsValid(*it)) {
      auto accessor = GetAccessor(op_args.db_cntx, (*it)->second);
      return accessor->SerializeSingleField(base_->schema, field);
    }
    return std::nullopt;
  };
  return SortSearchResultForNonSortableField(field.NameView(), sort_order, shard_limit, key_index_,
                                             get_value, search_result);
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
  DCHECK(IsIndexedKeyType(pv));
  for (auto& [_, index] : indices_) {
    if (index->Matches(key, pv.ObjType()))
      index->AddDoc(key, db_cntx, pv);
  }
}

void ShardDocIndices::RemoveDoc(string_view key, const DbContext& db_cntx, const PrimeValue& pv) {
  DCHECK(IsIndexedKeyType(pv));
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
