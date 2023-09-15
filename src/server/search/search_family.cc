// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_format.h>

#include <atomic>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/json_object.h"
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
#include "server/search/doc_index.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

static const set<string_view> kIgnoredOptions = {"WEIGHT", "SEPARATOR", "TYPE", "DIM",
                                                 "DISTANCE_METRIC"};

bool IsValidJsonPath(string_view path) {
  error_code ec;
  jsoncons::jsonpath::make_expression<JsonType>(path, ec);
  return !ec;
}

search::SchemaField::VectorParams ParseVectorParams(CmdArgParser* parser) {
  size_t dim = 0;
  auto sim = search::VectorSimilarity::L2;
  size_t capacity = 1000;

  bool use_hnsw = parser->ToUpper().Next().Case("HNSW", true).Case("FLAT", false);
  size_t num_args = parser->Next().Int<size_t>();

  for (size_t i = 0; i * 2 < num_args; i++) {
    parser->ToUpper();

    if (parser->Check("DIM").ExpectTail(1)) {
      dim = parser->Next().Int<size_t>();
      continue;
    }

    if (parser->Check("DISTANCE_METRIC").ExpectTail(1)) {
      sim = parser->Next()
                .Case("L2", search::VectorSimilarity::L2)
                .Case("COSINE", search::VectorSimilarity::COSINE);
      continue;
    }

    if (parser->Check("INITIAL_CAP").ExpectTail(1)) {
      capacity = parser->Next().Int<size_t>();
      continue;
    }

    parser->Skip(2);
  }

  return {use_hnsw, dim, sim, capacity};
}

optional<search::Schema> ParseSchemaOrReply(DocIndex::DataType type, CmdArgParser parser,
                                            ConnectionContext* cntx) {
  search::Schema schema;

  while (parser.HasNext()) {
    string_view field = parser.Next();
    string_view field_alias = field;

    // Verify json path is correct
    if (type == DocIndex::JSON && !IsValidJsonPath(field)) {
      (*cntx)->SendError("Bad json path: " + string{field});
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
      (*cntx)->SendError("Invalid field type: " + string{type_str});
      return nullopt;
    }

    // Vector fields include: {algorithm} num_args args...
    search::SchemaField::ParamsVariant params = std::monostate{};
    if (*type == search::SchemaField::VECTOR) {
      auto vector_params = ParseVectorParams(&parser);
      if (!parser.HasError() && vector_params.dim == 0) {
        (*cntx)->SendError("Knn vector dimension cannot be zero");
        return nullopt;
      }

      params = std::move(vector_params);
    }

    // Skip all trailing ignored parameters
    while (kIgnoredOptions.count(parser.Peek()) > 0)
      parser.Skip(2);

    schema.fields[field] = {*type, string{field_alias}, std::move(params)};
  }

  // Build field name mapping table
  for (const auto& [field_ident, field_info] : schema.fields)
    schema.field_names[field_info.short_name] = field_ident;

  if (auto err = parser.Error(); err) {
    (*cntx)->SendError(err->MakeReply());
    return nullopt;
  }

  return schema;
}

optional<SearchParams> ParseSearchParamsOrReply(CmdArgParser parser, ConnectionContext* cntx) {
  size_t limit_offset = 0, limit_total = 10;

  optional<SearchParams::FieldReturnList> return_list;
  search::QueryParams query_params;

  while (parser.ToUpper().HasNext()) {
    // [LIMIT offset total]
    if (parser.Check("LIMIT").ExpectTail(2)) {
      limit_offset = parser.Next().Int<size_t>();
      limit_total = parser.Next().Int<size_t>();
      continue;
    }

    // RETURN {num} [{ident} AS {name}...]
    if (parser.Check("RETURN").ExpectTail(1)) {
      size_t num_fields = parser.Next().Int<size_t>();
      return_list = SearchParams::FieldReturnList{};
      while (return_list->size() < num_fields) {
        string_view ident = parser.Next();
        string_view alias = parser.Check("AS").IgnoreCase().ExpectTail(1) ? parser.Next() : ident;
        return_list->emplace_back(ident, alias);
      }
      continue;
    }

    // NOCONTENT
    if (parser.Check("NOCONTENT")) {
      return_list = SearchParams::FieldReturnList{};
      continue;
    }

    // [PARAMS num(ignored) name(ignored) knn_vector]
    if (parser.Check("PARAMS").ExpectTail(1)) {
      size_t num_args = parser.Next().Int<size_t>();
      while (parser.HasNext() && query_params.Size() * 2 < num_args) {
        string_view k = parser.Next();
        string_view v = parser.Next();
        query_params[k] = v;
      }
      continue;
    }

    // Unsupported parameters are ignored for now
    parser.Skip(1);
  }

  if (auto err = parser.Error(); err) {
    (*cntx)->SendError(err->MakeReply());
    return nullopt;
  }

  return SearchParams{limit_offset, limit_total, std::move(return_list), std::move(query_params)};
}

void SendSerializedDoc(const SerializedSearchDoc& doc, ConnectionContext* cntx) {
  (*cntx)->SendBulkString(doc.key);
  (*cntx)->StartCollection(doc.values.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : doc.values) {
    (*cntx)->SendBulkString(k);
    (*cntx)->SendBulkString(v);
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

  (*cntx)->StartArray(reply_size);
  (*cntx)->SendLong(total_count);

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
        (*cntx)->SendBulkString(serialized_doc.key);
      else
        SendSerializedDoc(serialized_doc, cntx);
    }
  }
}

void ReplyKnn(size_t knn_limit, const SearchParams& params, absl::Span<SearchResult> results,
              ConnectionContext* cntx) {
  vector<const SerializedSearchDoc*> docs;
  for (const auto& shard_results : results) {
    for (const auto& doc : shard_results.docs) {
      docs.push_back(&doc);
    }
  }

  size_t prefix = min(params.limit_offset + params.limit_total, knn_limit);

  partial_sort(docs.begin(), docs.begin() + min(docs.size(), prefix), docs.end(),
               [](const auto* l, const auto* r) { return l->knn_distance < r->knn_distance; });
  docs.resize(min(docs.size(), knn_limit));

  size_t result_count =
      min(docs.size() - min(docs.size(), params.limit_offset), params.limit_total);

  bool ids_only = params.IdsOnly();
  size_t reply_size = ids_only ? (result_count + 1) : (result_count * 2 + 1);

  facade::SinkReplyBuilder::ReplyAggregator agg{cntx->reply_builder()};

  (*cntx)->StartArray(reply_size);
  (*cntx)->SendLong(docs.size());
  for (auto* doc : absl::MakeSpan(docs).subspan(params.limit_offset, result_count)) {
    if (ids_only)
      (*cntx)->SendBulkString(doc->key);
    else
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
      index.type =
          parser.ToUpper().Next().Case("HASH"sv, DocIndex::HASH).Case("JSON"sv, DocIndex::JSON);
      continue;
    }

    // PREFIX count prefix [prefix ...]
    if (parser.Check("PREFIX").ExpectTail(2)) {
      if (size_t num = parser.Next().Int<size_t>(); num != 1)
        return (*cntx)->SendError("Multiple prefixes are not supported");
      index.prefix = string(parser.Next());
      continue;
    }

    // SCHEMA
    if (parser.Check("SCHEMA")) {
      auto schema = ParseSchemaOrReply(index.type, parser.Tail(), cntx);
      if (!schema)
        return;
      index.schema = move(*schema);
      break;  // SCHEMA always comes last
    }

    // Unsupported parameters are ignored for now
    parser.Skip(1);
  }

  if (auto err = parser.Error(); err)
    return (*cntx)->SendError(err->MakeReply());

  auto idx_ptr = make_shared<DocIndex>(move(index));
  cntx->transaction->ScheduleSingleHop([idx_name, idx_ptr](auto* tx, auto* es) {
    es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, idx_ptr);
    return OpStatus::OK;
  });

  (*cntx)->SendOk();
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
  if (num_deleted == shard_set->size())
    return (*cntx)->SendOk();
  (*cntx)->SendError("Unknown Index name");
}

void SearchFamily::FtInfo(CmdArgList args, ConnectionContext* cntx) {
  string_view idx_name = ArgS(args, 0);

  atomic_uint num_notfound{0};
  vector<DocIndexInfo> infos(shard_set->size());

  cntx->transaction->EnableShards();
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
    return (*cntx)->SendError("Unknown index name");

  DCHECK(infos.front().base_index.schema.fields.size() ==
         infos.back().base_index.schema.fields.size());

  size_t total_num_docs = 0;
  for (const auto& info : infos)
    total_num_docs += info.num_docs;

  const auto& schema = infos.front().base_index.schema;

  (*cntx)->StartCollection(3, RedisReplyBuilder::MAP);

  (*cntx)->SendSimpleString("index_name");
  (*cntx)->SendSimpleString(idx_name);

  (*cntx)->SendSimpleString("fields");
  (*cntx)->StartArray(schema.fields.size());
  for (const auto& [field_ident, field_info] : schema.fields) {
    string_view reply[6] = {"identifier", string_view{field_ident},
                            "attribute",  field_info.short_name,
                            "type"sv,     SearchFieldTypeToString(field_info.type)};
    (*cntx)->SendSimpleStrArr(reply);
  }

  (*cntx)->SendSimpleString("num_docs");
  (*cntx)->SendLong(total_num_docs);
}

void SearchFamily::FtList(CmdArgList args, ConnectionContext* cntx) {
  atomic_int first{0};
  vector<string> names;

  cntx->transaction->EnableShards();
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    // Using `first` to assign `names` only once without a race
    if (first.fetch_add(1) == 0)
      names = es->search_indices()->GetIndexNames();
    return OpStatus::OK;
  });

  (*cntx)->SendStringArr(names);
}

void SearchFamily::FtSearch(CmdArgList args, ConnectionContext* cntx) {
  string_view index_name = ArgS(args, 0);
  string_view query_str = ArgS(args, 1);

  auto params = ParseSearchParamsOrReply(args.subspan(2), cntx);
  if (!params.has_value())
    return;

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(query_str, &params->query_params))
    return (*cntx)->SendError("Query syntax error");

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  atomic<bool> index_not_found{false};
  vector<SearchResult> docs(shard_set->size());

  cntx->transaction->EnableShards();
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(index_name); index)
      docs[es->shard_id()] = index->Search(t->GetOpArgs(es), *params, &search_algo);
    else
      index_not_found.store(true, memory_order_relaxed);
    return OpStatus::OK;
  });

  if (index_not_found.load())
    return (*cntx)->SendError(string{index_name} + ": no such index");

  if (auto knn_limit = search_algo.HasKnn(); knn_limit)
    ReplyKnn(*knn_limit, *params, absl::MakeSpan(docs), cntx);
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
  if (!search_algo.Init(query_str, &params->query_params))
    return (*cntx)->SendError("Query syntax error");

  search_algo.EnableProfiling();

  absl::Time start = absl::Now();
  atomic_uint total_docs = 0;
  atomic_uint total_serialized = 0;

  vector<pair<search::AlgorithmProfile, absl::Duration>> results(shard_set->size());

  cntx->transaction->EnableShards();
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

  (*cntx)->StartArray(results.size() + 1);

  // General stats
  (*cntx)->StartCollection(3, RedisReplyBuilder::MAP);
  (*cntx)->SendBulkString("took");
  (*cntx)->SendLong(absl::ToInt64Microseconds(took));
  (*cntx)->SendBulkString("hits");
  (*cntx)->SendLong(total_docs);
  (*cntx)->SendBulkString("serialized");
  (*cntx)->SendLong(total_serialized);

  // Per-shard stats
  for (const auto& [profile, shard_took] : results) {
    (*cntx)->StartCollection(2, RedisReplyBuilder::MAP);
    (*cntx)->SendBulkString("took");
    (*cntx)->SendLong(absl::ToInt64Microseconds(shard_took));
    (*cntx)->SendBulkString("tree");

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
        (*cntx)->StartArray(2);

      (*cntx)->SendSimpleString(
          absl::StrFormat("t=%-10u n=%-10u %s", event.micros, event.num_processed, event.descr));

      if (children > 0)
        (*cntx)->StartArray(children);
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
  const uint32_t kReadOnlyMask = CO::NO_KEY_TRANSACTIONAL | CO::NO_AUTOJOURNAL;

  registry->StartFamily();
  *registry << CI{"FT.CREATE", CO::GLOBAL_TRANS, -2, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtCreate)
            << CI{"FT.DROPINDEX", CO::GLOBAL_TRANS, -2, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtDropIndex)
            << CI{"FT.INFO", kReadOnlyMask, 2, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtInfo)
            // Underscore same as in RediSearch because it's "temporary" (long time already)
            << CI{"FT._LIST", kReadOnlyMask, 1, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtList)
            << CI{"FT.SEARCH", kReadOnlyMask, -3, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtSearch)
            << CI{"FT.PROFILE", kReadOnlyMask, -4, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtProfile);
}

}  // namespace dfly
