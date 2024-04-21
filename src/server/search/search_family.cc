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
      params.sim = parser->Switch("L2", search::VectorSimilarity::L2, "COSINE",
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

    parser->Skip(2);
  }

  return params;
}

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

    // Vector fields include: {algorithm} num_args args...
    search::SchemaField::ParamsVariant params = std::monostate{};
    if (*type == search::SchemaField::VECTOR) {
      auto vector_params = ParseVectorParams(&parser);
      if (!parser.HasError() && vector_params.dim == 0) {
        cntx->SendError("Knn vector dimension cannot be zero");
        return nullopt;
      }
      params = std::move(vector_params);
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

void SendSerializedDoc(const SerializedSearchDoc& doc, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendBulkString(doc.key);
  rb->StartCollection(doc.values.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : doc.values) {
    rb->SendBulkString(k);
    rb->SendBulkString(v);
  }
}

void ReplyWithResults(const SearchParams& params, absl::Span<SearchResult> results,
                      ConnectionContext* cntx) {
  size_t total_count = 0;
  for (const auto& shard_docs : results)
    total_count += shard_docs.total_hits;

  size_t result_count =
      min(total_count - min(total_count, params.limit_offset), params.limit_total);

  facade::SinkReplyBuilder::ReplyAggregator agg{cntx->reply_builder()};

  bool ids_only = params.IdsOnly();
  size_t reply_size = ids_only ? (result_count + 1) : (result_count * 2 + 1);

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(reply_size);
  rb->SendLong(total_count);

  size_t sent = 0;
  size_t to_skip = params.limit_offset;
  for (const auto& shard_docs : results) {
    for (const auto& serialized_doc : shard_docs.docs) {
      // Scoring is not implemented yet, so we just cut them in the order they were retrieved
      if (to_skip > 0) {
        to_skip--;
        continue;
      }

      if (sent++ >= result_count)
        return;

      if (ids_only)
        rb->SendBulkString(serialized_doc.key);
      else
        SendSerializedDoc(serialized_doc, cntx);
    }
  }
}

void ReplySorted(search::AggregationInfo agg, const SearchParams& params,
                 absl::Span<SearchResult> results, ConnectionContext* cntx) {
  size_t total = 0;
  vector<SerializedSearchDoc*> docs;
  for (auto& shard_results : results) {
    total += shard_results.total_hits;
    for (auto& doc : shard_results.docs) {
      docs.push_back(&doc);
    }
  }

  size_t agg_limit = agg.limit.value_or(total);
  size_t prefix = min(params.limit_offset + params.limit_total, agg_limit);

  partial_sort(docs.begin(), docs.begin() + min(docs.size(), prefix), docs.end(),
               [desc = agg.descending](const auto* l, const auto* r) {
                 return desc ? (*l >= *r) : (*l < *r);
               });

  docs.resize(min(docs.size(), agg_limit));

  size_t start_idx = min(params.limit_offset, docs.size());
  size_t result_count = min(docs.size() - start_idx, params.limit_total);
  bool ids_only = params.IdsOnly();
  size_t reply_size = ids_only ? (result_count + 1) : (result_count * 2 + 1);

  // Clear score alias if it's excluded from return values
  if (!params.ShouldReturnField(agg.alias))
    agg.alias = "";

  facade::SinkReplyBuilder::ReplyAggregator agg_reply{cntx->reply_builder()};
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(reply_size);
  rb->SendLong(min(total, agg_limit));
  for (auto* doc : absl::MakeSpan(docs).subspan(start_idx, result_count)) {
    if (ids_only) {
      rb->SendBulkString(doc->key);
      continue;
    }

    if (!agg.alias.empty() && holds_alternative<float>(doc->score))
      doc->values[agg.alias] = absl::StrCat(get<float>(doc->score));

    SendSerializedDoc(*doc, cntx);
  }
}

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

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  atomic<bool> index_not_found{false};
  vector<SearchResult> docs(shard_set->size());

  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(index_name); index)
      docs[es->shard_id()] = index->Search(t->GetOpArgs(es), *params, &search_algo);
    else
      index_not_found.store(true, memory_order_relaxed);
    return OpStatus::OK;
  });

  if (index_not_found.load())
    return cntx->SendError(string{index_name} + ": no such index");

  for (const auto& res : docs) {
    if (res.error)
      return cntx->SendError(*res.error);
  }

  if (auto agg = search_algo.HasAggregation(); agg)
    ReplySorted(std::move(*agg), *params, absl::MakeSpan(docs), cntx);
  else
    ReplyWithResults(*params, absl::MakeSpan(docs), cntx);
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
  atomic_uint total_docs = 0;
  atomic_uint total_serialized = 0;

  vector<pair<search::AlgorithmProfile, absl::Duration>> results(shard_set->size());

  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(index_name);
    if (!index)
      return OpStatus::OK;

    auto shard_start = absl::Now();
    auto res = index->Search(t->GetOpArgs(es), *params, &search_algo);

    total_docs.fetch_add(res.total_hits);
    total_serialized.fetch_add(res.docs.size());

    DCHECK(res.profile);
    results[es->shard_id()] = {std::move(*res.profile), absl::Now() - shard_start};

    return OpStatus::OK;
  });

  auto took = absl::Now() - start;
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(results.size() + 1);

  // General stats
  rb->StartCollection(3, RedisReplyBuilder::MAP);
  rb->SendBulkString("took");
  rb->SendLong(absl::ToInt64Microseconds(took));
  rb->SendBulkString("hits");
  rb->SendLong(total_docs);
  rb->SendBulkString("serialized");
  rb->SendLong(total_serialized);

  // Per-shard stats
  for (const auto& [profile, shard_took] : results) {
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
