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

  params.use_hnsw = parser->MapNext("HNSW", true, "FLAT", false);
  const size_t num_args = parser->Next<size_t>();

  for (size_t i = 0; i * 2 < num_args; i++) {
    if (parser->Check("DIM", &params.dim)) {
    } else if (parser->Check("DISTANCE_METRIC")) {
      params.sim = parser->MapNext("L2", search::VectorSimilarity::L2, "COSINE",
                                   search::VectorSimilarity::COSINE);
    } else if (parser->Check("INITIAL_CAP", &params.capacity)) {
    } else if (parser->Check("M", &params.hnsw_m)) {
    } else if (parser->Check("EF_CONSTRUCTION", &params.hnsw_ef_construction)) {
    } else if (parser->Check("EF_RUNTIME")) {
      parser->Next<size_t>();
      LOG(WARNING) << "EF_RUNTIME not supported";
    } else if (parser->Check("EPSILON")) {
      parser->Next<double>();
      LOG(WARNING) << "EPSILON not supported";
    } else {
      parser->Skip(2);
    }
  }

  return params;
}

search::SchemaField::TagParams ParseTagParams(CmdArgParser* parser) {
  search::SchemaField::TagParams params{};
  while (parser->HasNext()) {
    if (parser->Check("SEPARATOR")) {
      string_view separator = parser->Next();
      params.separator = separator.front();
      continue;
    }

    if (parser->Check("CASESENSITIVE")) {
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

    // AS [alias]
    parser.Check("AS", &field_alias);

    // Determine type
    using search::SchemaField;
    auto type = parser.MapNext("TAG", SchemaField::TAG, "TEXT", SchemaField::TEXT, "NUMERIC",
                               SchemaField::NUMERIC, "VECTOR", SchemaField::VECTOR);
    if (auto err = parser.Error(); err) {
      cntx->SendError(err->MakeReply());
      return nullopt;
    }

    // Tag fields include: [separator char] [casesensitive]
    // Vector fields include: {algorithm} num_args args...
    search::SchemaField::ParamsVariant params(monostate{});
    if (type == search::SchemaField::TAG) {
      params = ParseTagParams(&parser);
    } else if (type == search::SchemaField::VECTOR) {
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
      if (parser.Check("NOINDEX")) {
        flags |= search::SchemaField::NOINDEX;
        continue;
      }

      if (parser.Check("SORTABLE")) {
        flags |= search::SchemaField::SORTABLE;
        continue;
      }

      break;
    }

    // Skip all trailing ignored parameters
    while (kIgnoredOptions.count(parser.Peek()) > 0)
      parser.Skip(2);

    schema.fields[field] = {type, flags, string{field_alias}, std::move(params)};
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

  while (parser.HasNext()) {
    // [LIMIT offset total]
    if (parser.Check("LIMIT")) {
      params.limit_offset = parser.Next<size_t>();
      params.limit_total = parser.Next<size_t>();
    } else if (parser.Check("RETURN")) {
      // RETURN {num} [{ident} AS {name}...]
      size_t num_fields = parser.Next<size_t>();
      params.return_fields.fields.emplace();
      while (params.return_fields->size() < num_fields) {
        string_view ident = parser.Next();
        string_view alias = parser.Check("AS") ? parser.Next() : ident;
        params.return_fields->emplace_back(ident, alias);
      }
    } else if (parser.Check("NOCONTENT")) {  // NOCONTENT
      params.return_fields.fields.emplace();
    } else if (parser.Check("PARAMS")) {  // [PARAMS num(ignored) name(ignored) knn_vector]
      params.query_params = ParseQueryParams(&parser);
    } else if (parser.Check("SORTBY")) {
      params.sort_option = search::SortOption{string{parser.Next()}, bool(parser.Check("DESC"))};
    } else {
      // Unsupported parameters are ignored for now
      parser.Skip(1);
    }
  }

  if (auto err = parser.Error(); err) {
    cntx->SendError(err->MakeReply());
    return nullopt;
  }

  return params;
}

std::string_view ParseField(CmdArgParser* parser) {
  std::string_view field = parser->Next();
  if (field.front() == '@') {
    field.remove_prefix(1);  // remove leading @ if exists
  }
  return field;
}

std::optional<std::string_view> ParseFieldWithAtSign(CmdArgParser* parser) {
  std::string_view field = parser->Next();
  if (field.front() != '@') {
    return std::nullopt;  // if we expect @, but it's not there, return nullopt
  }
  field.remove_prefix(1);  // remove leading @
  return field;
}

optional<AggregateParams> ParseAggregatorParamsOrReply(CmdArgParser parser,
                                                       ConnectionContext* cntx) {
  AggregateParams params;
  tie(params.index, params.query) = parser.Next<string_view, string_view>();

  while (parser.HasNext()) {
    // LOAD count field [field ...]
    if (parser.Check("LOAD")) {
      size_t num_fields = parser.Next<size_t>();
      params.load_fields.fields.emplace();
      while (params.load_fields->size() < num_fields) {
        string_view field = ParseField(&parser);
        string_view alias = parser.Check("AS") ? parser.Next() : field;
        params.load_fields->emplace_back(field, alias);
      }
      continue;
    }

    // GROUPBY nargs property [property ...]
    if (parser.Check("GROUPBY")) {
      vector<string_view> fields(parser.Next<size_t>());
      for (string_view& field : fields) {
        auto parsed_field = ParseFieldWithAtSign(&parser);
        if (!parsed_field) {
          cntx->SendError(absl::StrCat("bad arguments for GROUPBY: Unknown property '", field,
                                       "'. Did you mean '@", field, "`?"));
          return nullopt;
        }
        field = parsed_field.value();
      }

      vector<aggregate::Reducer> reducers;
      while (parser.Check("REDUCE")) {
        using RF = aggregate::ReducerFunc;
        auto func_name =
            parser.TryMapNext("COUNT", RF::COUNT, "COUNT_DISTINCT", RF::COUNT_DISTINCT, "SUM",
                              RF::SUM, "AVG", RF::AVG, "MAX", RF::MAX, "MIN", RF::MIN);

        if (!func_name) {
          cntx->SendError(absl::StrCat("reducer function ", parser.Next(), " not found"));
          return nullopt;
        }

        auto func = aggregate::FindReducerFunc(*func_name);
        auto nargs = parser.Next<size_t>();

        string source_field;
        if (nargs > 0) {
          source_field = ParseField(&parser);
        }

        parser.ExpectTag("AS");
        string result_field = parser.Next<string>();

        reducers.push_back(
            aggregate::Reducer{std::move(source_field), std::move(result_field), std::move(func)});
      }

      params.steps.push_back(aggregate::MakeGroupStep(fields, std::move(reducers)));
      continue;
    }

    // SORTBY nargs
    if (parser.Check("SORTBY")) {
      parser.ExpectTag("1");
      string_view field = parser.Next();
      bool desc = bool(parser.Check("DESC"));

      params.steps.push_back(aggregate::MakeSortStep(field, desc));
      continue;
    }

    // LIMIT
    if (parser.Check("LIMIT")) {
      auto [offset, num] = parser.Next<size_t, size_t>();
      params.steps.push_back(aggregate::MakeLimitStep(offset, num));
      continue;
    }

    // PARAMS
    if (parser.Check("PARAMS")) {
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

auto SortableValueSender(RedisReplyBuilder* rb) {
  return Overloaded{
      [rb](monostate) { rb->SendNull(); },
      [rb](double d) { rb->SendDouble(d); },
      [rb](const string& s) { rb->SendBulkString(s); },
  };
}

void SendSerializedDoc(const SerializedSearchDoc& doc, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  auto sortable_value_sender = SortableValueSender(rb);

  rb->SendBulkString(doc.key);
  rb->StartCollection(doc.values.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : doc.values) {
    rb->SendBulkString(k);
    visit(sortable_value_sender, v);
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
  if (cntx->conn_state.db_index != 0) {
    return cntx->SendError("Cannot create index on db != 0"sv);
  }

  DocIndex index{};

  CmdArgParser parser{args};
  string_view idx_name = parser.Next();

  while (parser.HasNext()) {
    // ON HASH | JSON
    if (parser.Check("ON")) {
      index.type = parser.MapNext("HASH"sv, DocIndex::HASH, "JSON"sv, DocIndex::JSON);
      continue;
    }

    // PREFIX count prefix [prefix ...]
    if (parser.Check("PREFIX")) {
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

void SearchFamily::FtTagVals(CmdArgList args, ConnectionContext* cntx) {
  string_view index_name = ArgS(args, 0);
  string_view field_name = ArgS(args, 1);
  VLOG(1) << "FtTagVals: " << index_name << " " << field_name;

  vector<io::Result<StringVec, ErrorReply>> shard_results(shard_set->size(), StringVec{});

  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(index_name); index)
      shard_results[es->shard_id()] = index->GetTagVals(field_name);
    else
      shard_results[es->shard_id()] = nonstd::make_unexpected(ErrorReply("-Unknown Index name"));

    return OpStatus::OK;
  });

  absl::flat_hash_set<string> result_set;

  // Check first if either shard had errors. Also merge the results into a single set.
  for (auto& res : shard_results) {
    if (res) {
      result_set.insert(make_move_iterator(res->begin()), make_move_iterator(res->end()));
    } else {
      res.error().kind = facade::kSearchErrType;
      return cntx->SendError(res.error());
    }
  }

  shard_results.clear();
  vector<string> vec(result_set.begin(), result_set.end());

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendStringArr(vec, RedisReplyBuilder::SET);
}

void SearchFamily::FtAggregate(CmdArgList args, ConnectionContext* cntx) {
  const auto params = ParseAggregatorParamsOrReply(args, cntx);
  if (!params)
    return;

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(params->query, &params->params, nullptr))
    return cntx->SendError("Query syntax error");

  using ResultContainer = decltype(declval<ShardDocIndex>().SearchForAggregator(
      declval<OpArgs>(), params.value(), &search_algo));

  vector<ResultContainer> query_results(shard_set->size());
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(params->index); index) {
      query_results[es->shard_id()] =
          index->SearchForAggregator(t->GetOpArgs(es), params.value(), &search_algo);
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

  size_t result_size = agg_results->size();
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  auto sortable_value_sender = SortableValueSender(rb);

  rb->StartArray(result_size + 1);
  rb->SendLong(result_size);

  for (const auto& result : agg_results.value()) {
    rb->StartArray(result.size() * 2);
    for (const auto& [k, v] : result) {
      rb->SendBulkString(k);
      std::visit(sortable_value_sender, v);
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
            << CI{"FT.PROFILE", kReadOnlyMask, -4, 0, 0, acl::FT_SEARCH}.HFUNC(FtProfile)
            << CI{"FT.TAGVALS", kReadOnlyMask, 3, 0, 0, acl::FT_SEARCH}.HFUNC(FtTagVals);
}

}  // namespace dfly
