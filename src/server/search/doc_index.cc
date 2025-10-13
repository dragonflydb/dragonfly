// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_index.h"

#include <absl/strings/str_join.h>

#include <memory>
#include <queue>

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

using SortIndiciesFieldsList =
    std::vector<std::pair<string_view /*identifier*/, string_view /*alias*/>>;

std::pair<std::vector<FieldReference>, SortIndiciesFieldsList> PreprocessAggregateFields(
    const search::Schema& schema, const AggregateParams& params,
    const std::optional<std::vector<FieldReference>>& load_fields) {
  absl::flat_hash_map<std::string_view, FieldReference> fields_by_identifier;
  absl::flat_hash_map<std::string_view, std::string_view> sort_indicies_aliases;
  fields_by_identifier.reserve(schema.field_names.size());
  sort_indicies_aliases.reserve(schema.field_names.size());

  for (const auto& [fname, fident] : schema.field_names) {
    if (!IsSortableField(fident, schema)) {
      fields_by_identifier.emplace(fident, FieldReference{fident, fname});
    } else {
      sort_indicies_aliases[fident] = fname;
    }
  }

  for (const auto& field : load_fields.value_or(vector<FieldReference>{})) {
    string_view fident = field.Identifier(schema, false);
    if (!IsSortableField(fident, schema)) {
      fields_by_identifier.insert_or_assign(fident, field);
    } else {
      sort_indicies_aliases[fident] = field.OutputName();
    }
  }

  vector<FieldReference> fields;
  fields.reserve(fields_by_identifier.size());
  for (auto& [_, field] : fields_by_identifier) {
    fields.emplace_back(field);
  }

  return {std::move(fields), {sort_indicies_aliases.begin(), sort_indicies_aliases.end()}};
}

/* Separate fields into basic and sortable. The second vector contains flags indicating
   whether the field at the same index in the first vector is sortable or not. */
std::pair<std::vector<FieldReference>, std::vector<bool>> GetBasicFields(
    absl::Span<const std::string_view> fields, const search::Schema& schema) {
  const size_t fields_count = fields.size();
  std::vector<bool> is_sortable_field(fields_count);
  std::vector<FieldReference> basic_fields;
  basic_fields.reserve(fields_count);
  for (size_t i = 0; i < fields_count; ++i) {
    bool is_sortable = IsSortableField(fields[i], schema);
    is_sortable_field[i] = is_sortable;
    if (!is_sortable) {
      basic_fields.emplace_back(fields[i]);
    }
  }
  return {std::move(basic_fields), std::move(is_sortable_field)};
}

}  // namespace

bool FieldReference::IsJsonPath(std::string_view name) {
  if (name.size() < 2) {
    return false;
  }
  return name.front() == '$' && (name[1] == '.' || name[1] == '[');
}

bool SearchParams::ShouldReturnField(std::string_view alias) const {
  auto cb = [alias](const auto& entry) { return entry.OutputName() == alias; };
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
    case search::SchemaField::GEO:
      return "GEO";
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

    // Store specific params
    Overloaded info{
        [](monostate) {},
        [out = &out](const search::SchemaField::VectorParams& params) {
          auto sim = params.sim == search::VectorSimilarity::L2   ? "L2"
                     : params.sim == search::VectorSimilarity::IP ? "IP"
                                                                  : "COSINE";
          absl::StrAppend(out, " ", params.use_hnsw ? "HNSW" : "FLAT", " 6 ", "DIM ", params.dim,
                          " DISTANCE_METRIC ", sim, " INITIAL_CAP ", params.capacity);
        },
        [out = &out](const search::SchemaField::TagParams& params) {
          absl::StrAppend(out, " ", "SEPARATOR", " ", string{params.separator});
          if (params.case_sensitive)
            absl::StrAppend(out, " ", "CASESENSITIVE");
        },
        [out = &out](const search::SchemaField::TextParams& params) {
          if (params.with_suffixtrie)
            absl::StrAppend(out, " ", "WITH_SUFFIXTRIE");
        },
        [out = &out](const search::SchemaField::NumericParams& params) {
          absl::StrAppend(out, " ", "BLOCKSIZE", " ", std::to_string(params.block_size));
        }};
    visit(info, finfo.special_params);

    // Store shared field flags
    if (finfo.flags & search::SchemaField::SORTABLE)
      absl::StrAppend(&out, " SORTABLE");

    if (finfo.flags & search::SchemaField::NOINDEX)
      absl::StrAppend(&out, " NOINDEX");
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

  indices_->FinalizeInitialization();

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

optional<ShardDocIndex::LoadedEntry> ShardDocIndex::LoadEntry(DocId id,
                                                              const OpArgs& op_args) const {
  auto& db_slice = op_args.GetDbSlice();
  string_view key = key_index_.Get(id);
  auto it = db_slice.FindReadOnly(op_args.db_cntx, key, base_->GetObjCode());
  if (!it || !IsValid(*it))
    return std::nullopt;

  return {{key, GetAccessor(op_args.db_cntx, (*it)->second)}};
}

vector<search::SortableValue> ShardDocIndex::KeepTopKSorted(vector<DocId>* ids, size_t limit,
                                                            const SearchParams::SortOption& sort,
                                                            const OpArgs& op_args) const {
  DCHECK_GT(limit, 0u) << "Limit=0 still has O(ids->size()) complexity";

  auto comp = [order = sort.order](const auto& lhs, const auto& rhs) {
    return order == SortOrder::ASC ? lhs < rhs : lhs > rhs;
  };
  // Priority queue keeps top-k values in reverse order (to compare against top - worst value)
  using QPair = std::pair<search::SortableValue, DocId>;
  std::priority_queue<QPair, std::vector<QPair>, decltype(comp)> q(comp);

  // Iterate over all documents, extract sortable field and update the queue
  for (DocId id : *ids) {
    auto entry = LoadEntry(id, op_args);
    if (!entry)
      continue;

    auto result = entry->second->Serialize(base_->schema, {sort.field});
    if (result.empty())
      continue;

    // Check if the extracted value is better than the worst (q.top())
    if (q.size() < limit || comp(result.begin()->second, q.top().first)) {
      if (q.size() >= limit)
        q.pop();
      q.emplace(std::move(result.begin()->second), id);
    }
  }

  // Reorder ids and collect scores
  vector<search::SortableValue> out(q.size());
  for (int i = 0; !q.empty(); i++) {
    auto [v, id] = q.top();
    (*ids)[i] = id;
    out[i] = std::move(v);
    q.pop();
  }
  return out;
}

SearchResult ShardDocIndex::Search(const OpArgs& op_args, const SearchParams& params,
                                   search::SearchAlgorithm* search_algo) const {
  size_t limit = params.limit_offset + params.limit_total;
  auto result = search_algo->Search(&*indices_);
  if (!result.error.empty())
    return {facade::ErrorReply(std::move(result.error))};

  if (limit == 0)
    return {result.total, {}, std::move(result.profile)};

  // Tune sort for KNN: Skip if it's on the knn field, otherwise extend the limit if needed
  bool skip_sort = false;
  if (auto ko = search_algo->GetKnnScoreSortOption(); ko) {
    skip_sort = !params.sort_option || params.sort_option->IsSame(*ko);
    if (!skip_sort)
      limit = max(limit, ko->limit);
  }

  auto return_fields = params.return_fields.value_or(vector<FieldReference>{});

  // Apply SORTBY
  // TODO(vlad): Write profiling up to here
  vector<search::SortableValue> sort_scores;
  if (params.sort_option && !skip_sort) {
    const auto& so = *params.sort_option;
    auto fident = so.field.Identifier(base_->schema, false);
    if (IsSortableField(fident, base_->schema)) {
      auto* idx = indices_->GetSortIndex(fident);
      sort_scores = idx->Sort(&result.ids, limit, so.order == SortOrder::DESC);
    } else {
      sort_scores = KeepTopKSorted(&result.ids, limit, so, op_args);
      if (params.ShouldReturnAllFields())
        return_fields.push_back(so.field);
    }

    // If we sorted with knn_scores present, rearrange them
    if (!sort_scores.empty() && !result.knn_scores.empty()) {
      unordered_map<DocId, size_t> score_lookup(result.knn_scores.begin(), result.knn_scores.end());
      for (size_t i = 0; i < min(limit, result.ids.size()); i++)
        result.knn_scores[i] = {result.ids[i], score_lookup[result.ids[i]]};
    }
  }

  // Cut off unnecessary items
  result.ids.resize(min(result.ids.size(), limit));

  // Serialize documents
  vector<SerializedSearchDoc> out;
  out.reserve(min(limit, result.ids.size()));

  size_t expired_count = 0;
  for (size_t i = 0; i < result.ids.size(); i++) {
    float knn_score = result.knn_scores.empty() ? 0 : result.knn_scores[i].second;
    auto sort_score = sort_scores.empty() ? std::monostate{} : std::move(sort_scores[i]);

    // Don't load entry if we need only its key. Ignore expiration.
    if (params.IdsOnly()) {
      string_view key = key_index_.Get(result.ids[i]);
      out.push_back({string{key}, {}, knn_score, sort_score});
      continue;
    }

    auto entry = LoadEntry(result.ids[i], op_args);
    if (!entry) {
      expired_count++;
      continue;
    }

    auto& [key, accessor] = *entry;

    // Load all specified fields from document
    SearchDocData fields{};
    if (params.ShouldReturnAllFields())
      fields = accessor->Serialize(base_->schema);

    auto more_fields = accessor->Serialize(base_->schema, return_fields);
    fields.insert(make_move_iterator(more_fields.begin()), make_move_iterator(more_fields.end()));
    out.push_back({string{key}, std::move(fields), knn_score, sort_score});
  }

  return {result.total - expired_count, std::move(out), std::move(result.profile)};
}

vector<SearchDocData> ShardDocIndex::SearchForAggregator(
    const OpArgs& op_args, const AggregateParams& params,
    search::SearchAlgorithm* search_algo) const {
  auto search_results = search_algo->Search(&*indices_);

  if (!search_results.error.empty())
    return {};

  auto [fields_to_load, sort_indicies] =
      PreprocessAggregateFields(base_->schema, params, params.load_fields);

  vector<absl::flat_hash_map<string, search::SortableValue>> out;
  for (DocId doc : search_results.ids) {
    auto entry = LoadEntry(doc, op_args);
    if (!entry)
      continue;
    auto& [_, accessor] = *entry;

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

join::Vector<join::OwnedEntry> ShardDocIndex::PreagregateDataForJoin(
    const OpArgs& op_args, absl::Span<const std::string_view> join_fields,
    search::SearchAlgorithm* search_algo) const {
  auto search_results = search_algo->Search(&*indices_);

  const size_t fields_count = join_fields.size();
  const auto [basic_fields, is_sortable_field] = GetBasicFields(join_fields, base_->schema);

  join::Vector<join::OwnedEntry> result;
  result.reserve(search_results.ids.size());

  const ShardId shard_id = op_args.shard->shard_id();
  for (DocId doc : search_results.ids) {
    auto entry = LoadEntry(doc, op_args);
    if (!entry)
      continue;

    auto& [key, accessor] = *entry;

    SearchDocData loaded_basic_fields = accessor->Serialize(base_->schema, basic_fields);

    bool insert_key = true;
    join::Vector<join::OwnedJoinableValue> join_fields_values(fields_count);
    for (size_t i = 0; i < fields_count; ++i) {
      search::SortableValue value;
      if (is_sortable_field[i]) {
        value = indices_->GetSortIndexValue(doc, join_fields[i]);
      } else {
        value = loaded_basic_fields[join_fields[i]];
      }

      auto copy = [&](auto&& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (!std::is_same_v<T, std::monostate>) {
          join_fields_values[i] = v;
        } else {
          // If the value is nil, we skip this key
          insert_key = false;
        }
      };

      std::visit(std::move(copy), value);
    }

    if (insert_key) {
      result.emplace_back(std::piecewise_construct, std::forward_as_tuple(shard_id, doc),
                          std::forward_as_tuple(std::make_move_iterator(join_fields_values.begin()),
                                                std::make_move_iterator(join_fields_values.end())));
    }
  }

  return result;
}

ShardDocIndex::FieldsValuesPerDocId ShardDocIndex::LoadKeysData(
    const OpArgs& op_args, const absl::flat_hash_set<search::DocId>& doc_ids,
    absl::Span<const std::string_view> fields_to_load) const {
  const size_t fields_count = fields_to_load.size();
  const auto [basic_fields, is_sortable_field] = GetBasicFields(fields_to_load, base_->schema);

  FieldsValuesPerDocId result;
  result.reserve(doc_ids.size());

  for (DocId doc : doc_ids) {
    auto entry = LoadEntry(doc, op_args);
    if (!entry)
      continue;

    auto& [key, accessor] = *entry;

    SearchDocData loaded_basic_fields = accessor->Serialize(base_->schema, basic_fields);

    FieldsValues fields_values(fields_count);
    for (size_t i = 0; i < fields_count; ++i) {
      if (is_sortable_field[i]) {
        fields_values[i] = indices_->GetSortIndexValue(doc, fields_to_load[i]);
      } else {
        fields_values[i] = loaded_basic_fields[fields_to_load[i]];
      }
    }

    result.emplace(std::piecewise_construct, std::forward_as_tuple(doc),
                   std::forward_as_tuple(std::make_move_iterator(fields_values.begin()),
                                         std::make_move_iterator(fields_values.end())));
  }

  return result;
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

unique_ptr<ShardDocIndex> ShardDocIndices::DropIndex(string_view name) {
  auto it = indices_.find(name);
  if (it == indices_.end())
    return nullptr;

  DropIndexCache(*it->second);
  auto index = std::move(it->second);
  indices_.erase(it);

  return index;
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
