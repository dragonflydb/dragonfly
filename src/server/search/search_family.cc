// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_format.h>

#include <atomic>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/search/aggregator.h"
#include "server/search/doc_index.h"
#include "server/transaction.h"
#include "src/core/overloaded.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

static const set<string_view> kIgnoredOptions = {"WEIGHT", "SEPARATOR"};

bool IsValidJsonPath(string_view path) {
  error_code ec;
  MakeJsonPathExpr(path, ec);
  return !ec;
}

search::SchemaField::VectorParams ParseVectorParams(CmdArgParser* parser) {
  search::SchemaField::VectorParams params{};

  params.use_hnsw = parser->ToUpper().Switch("HNSW", true, "FLAT", false);
  size_t num_args = parser->Next<size_t>();

  for (size_t i = 0; i * 2 < num_args; i++) {
    parser->ToUpper();

    if (parser->Check("DIM").ExpectTail(1)) {
      params.dim = parser->Next<size_t>();
      continue;
    }

    if (parser->Check("DISTANCE_METRIC").ExpectTail(1)) {
      params.sim = parser->ToUpper().Switch("L2", search::VectorSimilarity::L2, "COSINE",
                                            search::VectorSimilarity::COSINE);
      continue;
    }

    if (parser->Check("INITIAL_CAP").ExpectTail(1)) {
      params.capacity = parser->Next<size_t>();
      continue;
    }

    if (parser->Check("M").ExpectTail(1)) {
      params.hnsw_m = parser->Next<size_t>();
      continue;
    }

    if (parser->Check("EF_CONSTRUCTION").ExpectTail(1)) {
      params.hnsw_ef_construction = parser->Next<size_t>();
      continue;
    }

    if (parser->Check("EF_RUNTIME").ExpectTail(1)) {
      parser->Next<size_t>();
      LOG(WARNING) << "EF_RUNTIME not supported";
      continue;
    }

    if (parser->Check("EPSILON").ExpectTail(1)) {
      parser->Next<double>();
      LOG(WARNING) << "EPSILON not supported";
      continue;
    }

    parser->Skip(2);
  }

  return params;
}

search::SchemaField::TagParams ParseTagParams(CmdArgParser* parser) {
  search::SchemaField::TagParams params{};
  while (parser->HasNext()) {
    if (parser->Check("SEPARATOR").IgnoreCase().ExpectTail(1)) {
      string_view separator = parser->Next();
      params.separator = separator.front();
      continue;
    }

    if (parser->Check("CASESENSITIVE").IgnoreCase()) {
      params.case_sensitive = true;
      continue;
    }

    break;
  }

  return params;
}

// breaks on ParamsVariant initialization
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

optional<search::Schema> ParseSchemaOrReply(DocIndex::DataType type, CmdArgParser parser,
                                            ConnectionContext* cntx) {
  search::Schema schema;

  while (parser.HasNext()) {
    string_view field = parser.Next();
    string_view field_alias = field;

    // Verify json path is correct
    if (type == DocIndex::JSON && !IsValidJsonPath(field)) {
      cntx->SendError("Bad json path: " + string{field});
      return nullopt;
    }

    parser.ToUpper();

    // AS [alias]
    if (parser.Check("AS").ExpectTail(1).NextUpper())
      field_alias = parser.Next();

    // Determine type
    string_view type_str = parser.Next();
    auto type = ParseSearchFieldType(type_str);
    if (!type) {
      cntx->SendError("Invalid field type: " + string{type_str});
      return nullopt;
    }

    // Tag fields include: [separator char] [casesensitive]
    // Vector fields include: {algorithm} num_args args...
    search::SchemaField::ParamsVariant params(monostate{});
    if (*type == search::SchemaField::TAG) {
      params = ParseTagParams(&parser);
    } else if (*type == search::SchemaField::VECTOR) {
      auto vector_params = ParseVectorParams(&parser);
      if (parser.HasError()) {
        auto err = *parser.Error();
        VLOG(1) << "Could not parse vector param " << err.index;
        cntx->SendError("Parse error of vector parameters", kSyntaxErrType);
        return nullopt;
      }

      if (vector_params.dim == 0) {
        cntx->SendError("Knn vector dimension cannot be zero", kSyntaxErrType);
        return nullopt;
      }
      params = vector_params;
    }

    // Flags: check for SORTABLE and NOINDEX
    uint8_t flags = 0;
    while (parser.HasNext()) {
      if (parser.Check("NOINDEX").IgnoreCase()) {
        flags |= search::SchemaField::NOINDEX;
        continue;
      }

      if (parser.Check("SORTABLE").IgnoreCase()) {
        flags |= search::SchemaField::SORTABLE;
        continue;
      }

      break;
    }

    // Skip all trailing ignored parameters
    while (kIgnoredOptions.count(parser.Peek()) > 0)
      parser.Skip(2);

    schema.fields[field] = {*type, flags, string{field_alias}, std::move(params)};
  }

  // Build field name mapping table
  for (const auto& [field_ident, field_info] : schema.fields)
    schema.field_names[field_info.short_name] = field_ident;

  if (auto err = parser.Error(); err) {
    cntx->SendError(err->MakeReply());
    return nullopt;
  }

  return schema;
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

search::QueryParams ParseQueryParams(CmdArgParser* parser) {
  search::QueryParams params;
  size_t num_args = parser->Next<size_t>();
  while (parser->HasNext() && params.Size() * 2 < num_args) {
    auto [k, v] = parser->Next<string_view, string_view>();
    params[k] = v;
  }
  return params;
}

optional<SearchParams> ParseSearchParamsOrReply(CmdArgParser parser, ConnectionContext* cntx) {
  SearchParams params;

  while (parser.ToUpper().HasNext()) {
    // [LIMIT offset total]
    if (parser.Check("LIMIT").ExpectTail(2)) {
      params.limit_offset = parser.Next<size_t>();
      params.limit_total = parser.Next<size_t>();
      continue;
    }

    // RETURN {num} [{ident} AS {name}...]
    if (parser.Check("RETURN").ExpectTail(1)) {
      size_t num_fields = parser.Next<size_t>();
      params.return_fields = SearchParams::FieldReturnList{};
      while (params.return_fields->size() < num_fields) {
        string_view ident = parser.Next();
        string_view alias = parser.Check("AS").IgnoreCase().ExpectTail(1) ? parser.Next() : ident;
        params.return_fields->emplace_back(ident, alias);
      }
      continue;
    }

    // NOCONTENT
    if (parser.Check("NOCONTENT")) {
      params.return_fields = SearchParams::FieldReturnList{};
      continue;
    }

    // [PARAMS num(ignored) name(ignored) knn_vector]
    if (parser.Check("PARAMS").ExpectTail(1)) {
      params.query_params = ParseQueryParams(&parser);
      continue;
    }

    if (parser.Check("SORTBY").ExpectTail(1)) {
      params.sort_option =
          search::SortOption{string{parser.Next()}, bool(parser.Check("DESC").IgnoreCase())};
      continue;
    }

    // Unsupported parameters are ignored for now
    parser.Skip(1);
  }

  if (auto err = parser.Error(); err) {
    cntx->SendError(err->MakeReply());
    return nullopt;
  }

  return params;
}

struct AggregateParams {
  string_view index, query;
  search::QueryParams params;

  vector<string_view> load_fields;
  vector<aggregate::PipelineStep> steps;
};

optional<AggregateParams> ParseAggregatorParamsOrReply(CmdArgParser parser,
                                                       ConnectionContext* cntx) {
  AggregateParams params;
  tie(params.index, params.query) = parser.Next<string_view, string_view>();

  while (parser.ToUpper().HasNext()) {
    // LOAD count field [field ...]
    if (parser.Check("LOAD").ExpectTail(1)) {
      params.load_fields.resize(parser.Next<size_t>());
      for (string_view& field : params.load_fields)
        field = parser.Next();
      continue;
    }

    // GROUPBY nargs property [property ...]
    if (parser.Check("GROUPBY").ExpectTail(1)) {
      vector<string_view> fields(parser.Next<size_t>());
      for (string_view& field : fields)
        field = parser.Next();

      vector<aggregate::Reducer> reducers;
      while (parser.ToUpper().Check("REDUCE").ExpectTail(2)) {
        parser.ToUpper();  // uppercase for func_name
        auto [func_name, nargs] = parser.Next<string_view, size_t>();
        auto func = aggregate::FindReducerFunc(func_name);

        if (!parser.HasError() && !func) {
          cntx->SendError(absl::StrCat("reducer function ", func_name, " not found"));
          return nullopt;
        }

        string source_field = "";
        if (nargs > 0) {
          source_field = parser.Next<string>();
        }

        parser.ExpectTag("AS");
        string result_field = parser.Next<string>();

        reducers.push_back(aggregate::Reducer{source_field, result_field, std::move(func)});
      }

      params.steps.push_back(aggregate::MakeGroupStep(fields, std::move(reducers)));
      continue;
    }

    // SORTBY nargs
    if (parser.Check("SORTBY").ExpectTail(1)) {
      parser.ExpectTag("1");
      string_view field = parser.Next();
      bool desc = bool(parser.Check("DESC").IgnoreCase());

      params.steps.push_back(aggregate::MakeSortStep(field, desc));
      continue;
    }

    // LIMIT
    if (parser.Check("LIMIT").ExpectTail(2)) {
      auto [offset, num] = parser.Next<size_t, size_t>();
      params.steps.push_back(aggregate::MakeLimitStep(offset, num));
      continue;
    }

    // PARAMS
    if (parser.Check("PARAMS").ExpectTail(1)) {
      params.params = ParseQueryParams(&parser);
      continue;
    }

    cntx->SendError(absl::StrCat("Unknown clause: ", parser.Peek()));
    return nullopt;
  }

  if (auto err = parser.Error(); err) {
    cntx->SendError(err->MakeReply());
    return nullopt;
  }

  return params;
}

void SendSerializedDoc(const DocResult::SerializedValue& value, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendBulkString(value.key);
  rb->StartCollection(value.values.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : value.values) {
    rb->SendBulkString(k);
    rb->SendBulkString(v);
  }
}

struct MultishardSearch {
  MultishardSearch(ConnectionContext* cntx, std::string_view index_name,
                   search::SearchAlgorithm* search_algo, SearchParams params)
      : cntx_{cntx},
        index_name_{index_name},
        search_algo_{search_algo},
        params_{std::move(params)} {
    sharded_results_.resize(shard_set->size());
    if (search_algo_->IsProfilingEnabled())
      sharded_times_.resize(shard_set->size());
  }

  void RunAndReply() {
    params_.enable_cutoff = true;
    params_.num_shards = shard_set->size();

    if (auto err = RunSearch(); err)
      return (*cntx_)->SendError(std::move(*err));

    auto incomplete_shards = BuildOrder();
    if (incomplete_shards.empty())
      return Reply();

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(reply_size);
  rb->SendLong(total_count);
    params_.enable_cutoff = false;

    //VLOG(0) << "Failed completness check, refilling";

    auto refill_res = RunRefill();
    if (!refill_res.has_value())
      return (*cntx_)->SendError(std::move(refill_res.error()));

    if (bool no_reordering = refill_res.value(); no_reordering)
      return Reply();

    //VLOG(0) << "Failed refill, rebuilding";

    if (auto incomplete_shards = BuildOrder(); incomplete_shards.empty())
      return Reply();

    //VLOG(0) << "Failed rebuild, re-searching";

    if (auto err = RunSearch(); err)
      return (*cntx_)->SendError(std::move(*err));
    incomplete_shards = BuildOrder();
    DCHECK(incomplete_shards.empty());
    Reply();
  }

  struct ProfileInfo {
    size_t total = 0;
    size_t serialized = 0;
    size_t cutoff = 0;
    size_t hops = 0;
    std::vector<pair<search::AlgorithmProfile, absl::Duration>> profiles;
  };

  ProfileInfo GetProfileInfo() {
    ProfileInfo info;
    info.hops = hops_;

    for (size_t i = 0; i < sharded_results_.size(); i++) {
      const auto& sd = sharded_results_[i];
      size_t serialized = count_if(sd.docs.begin(), sd.docs.end(), [](const auto& doc_res) {
        return holds_alternative<DocResult::SerializedValue>(doc_res.value);
      });

      info.total += sd.total_hits;
      info.serialized += serialized;
      info.cutoff += sd.docs.size() - serialized;

      DCHECK(sd.profile);
      info.profiles.push_back({std::move(*sd.profile), sharded_times_[i]});
    }

    return info;
  }

 private:
  void Reply() {
    size_t total_count = 0;
    for (const auto& shard_docs : sharded_results_)
      total_count += shard_docs.total_hits;

    auto agg_info = search_algo_->HasAggregation();
    if (agg_info && agg_info->limit)
      total_count = min(total_count, *agg_info->limit);

    if (agg_info && !params_.ShouldReturnField(agg_info->alias))
      agg_info->alias = ""sv;

    size_t result_count =
        min(total_count - min(total_count, params_.limit_offset), params_.limit_total);

    facade::SinkReplyBuilder::ReplyAggregator agg{cntx_->reply_builder()};

    bool ids_only = params_.IdsOnly();
    size_t reply_size = ids_only ? (result_count + 1) : (result_count * 2 + 1);

    VLOG(0) << "Reply size " << reply_size << " total count " << total_count;

    (*cntx_)->StartArray(reply_size);
    (*cntx_)->SendLong(total_count);

    for (size_t i = params_.limit_offset; i < ordered_docs_.size(); i++) {
      auto& value = get<DocResult::SerializedValue>(ordered_docs_[i]->value);
      if (ids_only) {
        (*cntx_)->SendBulkString(value.key);
        continue;
      }

      if (agg_info && !agg_info->alias.empty())
        value.values[agg_info->alias] = absl::StrCat(get<float>(ordered_docs_[i]->score));

      if (ids_only)
        rb->SendBulkString(serialized_doc.key);
      SendSerializedDoc(value, cntx_);
    }
  }

  template <typename F> optional<facade::ErrorReply> RunHandler(F&& f) {
    hops_++;
    AggregateValue<optional<facade::ErrorReply>> err;
    cntx_->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
      optional<absl::Time> start;
      if (search_algo_->IsProfilingEnabled())
        start = absl::Now();

      if (auto* index = es->search_indices()->GetIndex(index_name_); index)
        err = f(es, index);
      else
        err = facade::ErrorReply(string{index_name_} + ": no such index");

      if (start.has_value())
        sharded_times_[es->shard_id()] += (absl::Now() - *start);

      return OpStatus::OK;
    });
    return *err;
  }

  optional<facade::ErrorReply> RunSearch() {
    cntx_->transaction->Refurbish();

    return RunHandler([this](EngineShard* es, ShardDocIndex* index) -> optional<ErrorReply> {
      auto res = index->Search(cntx_->transaction->GetOpArgs(es), params_, search_algo_);
      if (!res.has_value())
        return std::move(res.error());
      sharded_results_[es->shard_id()] = std::move(res.value());
      return nullopt;
    });
  }

  io::Result<bool, facade::ErrorReply> RunRefill() {
    cntx_->transaction->Refurbish();

    atomic_uint failed_refills = 0;
    auto err = RunHandler([this, &failed_refills](EngineShard* es, ShardDocIndex* index) {
      bool refilled = index->Refill(cntx_->transaction->GetOpArgs(es), params_, search_algo_,
                                    &sharded_results_[es->shard_id()]);
      if (!refilled)
        failed_refills.fetch_add(1u);
      return nullopt;
    });

    if (err)
      return nonstd::make_unexpected(std::move(*err));
    return failed_refills == 0;
  }

  absl::flat_hash_set<ShardId> BuildOrder() {
    ordered_docs_.clear();
    if (auto agg = search_algo_->HasAggregation(); agg) {
      BuildSortedOrder(*agg);
    } else {
      BuildLinearOrder();
    }
    return VerifyOrderCompletness();
  }

  void BuildLinearOrder() {
    size_t required = params_.limit_offset + params_.limit_total;

    VLOG(0) << "Linear order";
    for (auto& shard_result : sharded_results_)
      VLOG(0) << "|->source " << shard_result.docs.size();

    for (size_t idx = 0;; idx++) {
      bool added = false;
      for (auto& shard_result : sharded_results_) {
        if (idx < shard_result.docs.size() && ordered_docs_.size() < required) {
          ordered_docs_.push_back(&shard_result.docs[idx]);
          added = true;
        }
      }
      if (!added)
        return;
    }
  }

  void BuildSortedOrder(search::AggregationInfo agg) {
    for (auto& shard_result : sharded_results_) {
      for (auto& doc : shard_result.docs) {
        ordered_docs_.push_back(&doc);
      }
    }

    size_t agg_limit = agg.limit.value_or(ordered_docs_.size());
    size_t prefix = min(params_.limit_offset + params_.limit_total, agg_limit);

    partial_sort(ordered_docs_.begin(), ordered_docs_.begin() + min(ordered_docs_.size(), prefix),
                 ordered_docs_.end(), [desc = agg.descending](const auto* l, const auto* r) {
                   return desc ? (l->score >= r->score) : (l->score < r->score);
                 });

    ordered_docs_.resize(min(ordered_docs_.size(), prefix));
  }

  absl::flat_hash_set<ShardId> VerifyOrderCompletness() {
    VLOG(0) << "Verifying order completness of " << ordered_docs_.size();
    absl::flat_hash_set<ShardId> incomplete_shards;
    for (auto* doc : ordered_docs_) {
      if (auto* ref = get_if<DocResult::DocReference>(&doc->value); ref) {
        incomplete_shards.insert(ref->shard_id);
        ref->requested = true;
      }
    }
    VLOG(0) << "Num incomplete shards " << incomplete_shards.size();
    return incomplete_shards;
  }

 private:
  ConnectionContext* cntx_;
  std::string_view index_name_;
  search::SearchAlgorithm* search_algo_;
  SearchParams params_;

  size_t hops_ = 0;

  std::vector<absl::Duration> sharded_times_;
  std::vector<DocResult*> ordered_docs_;
  std::vector<SearchResult> sharded_results_;
};

}  // namespace

void SearchFamily::FtCreate(CmdArgList args, ConnectionContext* cntx) {
  DocIndex index{};

  CmdArgParser parser{args};
  string_view idx_name = parser.Next();

  while (parser.ToUpper().HasNext()) {
    // ON HASH | JSON
    if (parser.Check("ON").ExpectTail(1)) {
      index.type = parser.ToUpper().Switch("HASH"sv, DocIndex::HASH, "JSON"sv, DocIndex::JSON);
      continue;
    }

    // PREFIX count prefix [prefix ...]
    if (parser.Check("PREFIX").ExpectTail(2)) {
      if (size_t num = parser.Next<size_t>(); num != 1)
        return cntx->SendError("Multiple prefixes are not supported");
      index.prefix = string(parser.Next());
      continue;
    }

    // SCHEMA
    if (parser.Check("SCHEMA")) {
      auto schema = ParseSchemaOrReply(index.type, parser.Tail(), cntx);
      if (!schema)
        return;
      index.schema = std::move(*schema);
      break;  // SCHEMA always comes last
    }

    // Unsupported parameters are ignored for now
    parser.Skip(1);
  }

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  // Check if index already exists
  atomic_uint exists_cnt = 0;
  cntx->transaction->Execute(
      [idx_name, &exists_cnt](auto* tx, auto* es) {
        if (es->search_indices()->GetIndex(idx_name) != nullptr)
          exists_cnt.fetch_add(1, std::memory_order_relaxed);
        return OpStatus::OK;
      },
      false);

  DCHECK(exists_cnt == 0u || exists_cnt == shard_set->size());

  if (exists_cnt.load(memory_order_relaxed) > 0) {
    cntx->transaction->Conclude();
    return cntx->SendError("Index already exists");
  }

  auto idx_ptr = make_shared<DocIndex>(std::move(index));
  cntx->transaction->Execute(
      [idx_name, idx_ptr](auto* tx, auto* es) {
        es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, idx_ptr);
        return OpStatus::OK;
      },
      true);

  cntx->SendOk();
}

void SearchFamily::FtAlter(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view idx_name = parser.Next();
  parser.ExpectTag("SCHEMA");
  parser.ExpectTag("ADD");

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  // First, extract existing index info
  shared_ptr<DocIndex> index_info;
  auto idx_cb = [idx_name, &index_info](auto* tx, EngineShard* es) {
    if (es->shard_id() > 0)  // all shards have the same data, fetch from first
      return OpStatus::OK;

    if (auto* idx = es->search_indices()->GetIndex(idx_name); idx != nullptr)
      index_info = make_shared<DocIndex>(idx->GetInfo().base_index);
    return OpStatus::OK;
  };
  cntx->transaction->Execute(idx_cb, false);

  if (!index_info) {
    cntx->transaction->Conclude();
    return cntx->SendError("Index not found");
  }

  // Parse additional schema
  optional<search::Schema> new_fields = ParseSchemaOrReply(index_info->type, parser, cntx);
  if (!new_fields) {
    cntx->transaction->Conclude();
    return;
  }

  LOG(INFO) << "Adding "
            << DocIndexInfo{.base_index = DocIndex{.schema = *new_fields}}.BuildRestoreCommand();

  // Merge schemas
  search::Schema& schema = index_info->schema;
  schema.fields.insert(new_fields->fields.begin(), new_fields->fields.end());
  schema.field_names.insert(new_fields->field_names.begin(), new_fields->field_names.end());

  // Rebuild index
  // TODO: Introduce partial rebuild
  auto upd_cb = [idx_name, index_info](Transaction* tx, EngineShard* es) {
    es->search_indices()->DropIndex(idx_name);
    es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, index_info);
    return OpStatus::OK;
  };
  cntx->transaction->Execute(upd_cb, true);

  cntx->SendOk();
}

void SearchFamily::FtDropIndex(CmdArgList args, ConnectionContext* cntx) {
  string_view idx_name = ArgS(args, 0);
  // TODO: Handle optional DD param

  atomic_uint num_deleted{0};
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (es->search_indices()->DropIndex(idx_name))
      num_deleted.fetch_add(1);
    return OpStatus::OK;
  });

  DCHECK(num_deleted == 0u || num_deleted == shard_set->size());
  if (num_deleted == 0u)
    return cntx->SendError("-Unknown Index name");
  return cntx->SendOk();
}

void SearchFamily::FtInfo(CmdArgList args, ConnectionContext* cntx) {
  string_view idx_name = ArgS(args, 0);

  atomic_uint num_notfound{0};
  vector<DocIndexInfo> infos(shard_set->size());

  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(idx_name);
    if (index == nullptr)
      num_notfound.fetch_add(1);
    else
      infos[es->shard_id()] = index->GetInfo();
    return OpStatus::OK;
  });

  DCHECK(num_notfound == 0u || num_notfound == shard_set->size());

  if (num_notfound > 0u)
    return cntx->SendError("Unknown Index name");

  DCHECK(infos.front().base_index.schema.fields.size() ==
         infos.back().base_index.schema.fields.size());

  size_t total_num_docs = 0;
  for (const auto& info : infos)
    total_num_docs += info.num_docs;

  const auto& info = infos.front();
  const auto& schema = info.base_index.schema;

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartCollection(4, RedisReplyBuilder::MAP);

  rb->SendSimpleString("index_name");
  rb->SendSimpleString(idx_name);

  rb->SendSimpleString("index_definition");
  {
    rb->StartCollection(2, RedisReplyBuilder::MAP);
    rb->SendSimpleString("key_type");
    rb->SendSimpleString(info.base_index.type == DocIndex::JSON ? "JSON" : "HASH");
    rb->SendSimpleString("prefix");
    rb->SendSimpleString(info.base_index.prefix);
  }

  rb->SendSimpleString("attributes");
  rb->StartArray(schema.fields.size());
  for (const auto& [field_ident, field_info] : schema.fields) {
    vector<string> info;

    string_view base[] = {"identifier"sv, string_view{field_ident},
                          "attribute",    field_info.short_name,
                          "type"sv,       SearchFieldTypeToString(field_info.type)};
    info.insert(info.end(), base, base + ABSL_ARRAYSIZE(base));

    if (field_info.flags & search::SchemaField::NOINDEX)
      info.push_back("NOINDEX");

    if (field_info.flags & search::SchemaField::SORTABLE)
      info.push_back("SORTABLE");

    rb->SendSimpleStrArr(info);
  }

  rb->SendSimpleString("num_docs");
  rb->SendLong(total_num_docs);
}

void SearchFamily::FtList(CmdArgList args, ConnectionContext* cntx) {
  atomic_int first{0};
  vector<string> names;

  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    // Using `first` to assign `names` only once without a race
    if (first.fetch_add(1) == 0)
      names = es->search_indices()->GetIndexNames();
    return OpStatus::OK;
  });
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendStringArr(names);
}

void SearchFamily::FtSearch(CmdArgList args, ConnectionContext* cntx) {
  string_view index_name = ArgS(args, 0);
  string_view query_str = ArgS(args, 1);

  auto params = ParseSearchParamsOrReply(args.subspan(2), cntx);
  if (!params.has_value())
    return;

  search::SearchAlgorithm search_algo;
  search::SortOption* sort_opt = params->sort_option.has_value() ? &*params->sort_option : nullptr;
  if (!search_algo.Init(query_str, &params->query_params, sort_opt))
    return cntx->SendError("Query syntax error");

  MultishardSearch{cntx, index_name, &search_algo, std::move(*params)}.RunAndReply();
}

void SearchFamily::FtProfile(CmdArgList args, ConnectionContext* cntx) {
  string_view index_name = ArgS(args, 0);
  string_view query_str = ArgS(args, 3);

  optional<SearchParams> params = ParseSearchParamsOrReply(args.subspan(4), cntx);
  if (!params.has_value())
    return;

  search::SearchAlgorithm search_algo;
  search::SortOption* sort_opt = params->sort_option.has_value() ? &*params->sort_option : nullptr;
  if (!search_algo.Init(query_str, &params->query_params, sort_opt))
    return cntx->SendError("Query syntax error");

  search_algo.EnableProfiling();

  absl::Time start = absl::Now();

  CapturingReplyBuilder crb{facade::ReplyMode::ONLY_ERR};
  MultishardSearch mss{cntx, index_name, &search_algo, std::move(*params)};

  {
    CapturingReplyBuilder::ScopeCapture capture{&crb, cntx};
    mss.RunAndReply();
  }

  auto reply = crb.Take();
  if (auto err = CapturingReplyBuilder::GetError(reply); err)
    return (*cntx)->SendError(err->first, err->second);

  auto took = absl::Now() - start;

  auto profile = mss.GetProfileInfo();

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(profile.profiles.size() + 1);

  // General stats
  rb->StartCollection(5, RedisReplyBuilder::MAP);
  rb->SendBulkString("took");
  rb->SendLong(absl::ToInt64Microseconds(took));
  rb->SendBulkString("hits");
  rb->SendLong(profile.total);
  rb->SendBulkString("serialized");
  rb->SendLong(profile.serialized);
  rb->SendSimpleString("cutoff");
  rb->SendLong(profile.cutoff);
  rb->SendSimpleString("hops");
  rb->SendLong(profile.hops);

  // Per-shard stats
  for (const auto& [profile, shard_took] : profile.profiles) {
    rb->StartCollection(2, RedisReplyBuilder::MAP);
    rb->SendBulkString("took");
    rb->SendLong(absl::ToInt64Microseconds(shard_took));
    rb->SendBulkString("tree");

    for (size_t i = 0; i < profile.events.size(); i++) {
      const auto& event = profile.events[i];

      size_t children = 0;
      for (size_t j = i + 1; j < profile.events.size(); j++) {
        if (profile.events[j].depth == event.depth)
          break;
        if (profile.events[j].depth == event.depth + 1)
          children++;
      }

      if (children > 0)
        rb->StartArray(2);

      rb->SendSimpleString(
          absl::StrFormat("t=%-10u n=%-10u %s", event.micros, event.num_processed, event.descr));

      if (children > 0)
        rb->StartArray(children);
    }
  }
}

void SearchFamily::FtAggregate(CmdArgList args, ConnectionContext* cntx) {
  const auto params = ParseAggregatorParamsOrReply(args, cntx);
  if (!params)
    return;

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(params->query, &params->params, nullptr))
    return cntx->SendError("Query syntax error");

  using ResultContainer =
      decltype(declval<ShardDocIndex>().SearchForAggregator(declval<OpArgs>(), {}, &search_algo));

  vector<ResultContainer> query_results(shard_set->size());
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(params->index); index) {
      query_results[es->shard_id()] =
          index->SearchForAggregator(t->GetOpArgs(es), params->load_fields, &search_algo);
    }
    return OpStatus::OK;
  });

  vector<aggregate::DocValues> values;
  for (auto& sub_results : query_results) {
    values.insert(values.end(), make_move_iterator(sub_results.begin()),
                  make_move_iterator(sub_results.end()));
  }

  auto agg_results = aggregate::Process(std::move(values), params->steps);
  if (!agg_results.has_value())
    return cntx->SendError(agg_results.error());

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  Overloaded replier{
      [rb](monostate) { rb->SendNull(); },
      [rb](double d) { rb->SendDouble(d); },
      [rb](const string& s) { rb->SendBulkString(s); },
  };

  rb->StartArray(agg_results->size());
  for (const auto& result : agg_results.value()) {
    rb->StartArray(result.size());
    for (const auto& [k, v] : result) {
      rb->StartArray(2);
      rb->SendBulkString(k);
      visit(replier, v);
    }
  }
}

#define HFUNC(x) SetHandler(&SearchFamily::x)

// Redis search is a module. Therefore we introduce dragonfly extension search
// to set as the default for the search family of commands. More sensible defaults,
// should also be considered in the future

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  // Disable journaling, because no-key-transactional enables it by default
  const uint32_t kReadOnlyMask =
      CO::NO_KEY_TRANSACTIONAL | CO::NO_KEY_TX_SPAN_ALL | CO::NO_AUTOJOURNAL;

  registry->StartFamily();
  *registry << CI{"FT.CREATE", CO::WRITE | CO::GLOBAL_TRANS, -2, 0, 0, acl::FT_SEARCH}.HFUNC(
                   FtCreate)
            << CI{"FT.ALTER", CO::WRITE | CO::GLOBAL_TRANS, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtAlter)
            << CI{"FT.DROPINDEX", CO::WRITE | CO::GLOBAL_TRANS, -2, 0, 0, acl::FT_SEARCH}.HFUNC(
                   FtDropIndex)
            << CI{"FT.INFO", kReadOnlyMask, 2, 0, 0, acl::FT_SEARCH}.HFUNC(FtInfo)
            // Underscore same as in RediSearch because it's "temporary" (long time already)
            << CI{"FT._LIST", kReadOnlyMask, 1, 0, 0, acl::FT_SEARCH}.HFUNC(FtList)
            << CI{"FT.SEARCH", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtSearch)
            << CI{"FT.AGGREGATE", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtAggregate)
            << CI{"FT.PROFILE", kReadOnlyMask, -4, 0, 0, acl::FT_SEARCH}.HFUNC(FtProfile);
}

}  // namespace dfly
