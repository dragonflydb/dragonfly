// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <atomic>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/json_object.h"
#include "core/search/search.h"
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

optional<search::Schema> ParseSchemaOrReply(DocIndex::DataType type, CmdArgList args,
                                            ConnectionContext* cntx) {
  search::Schema schema;

  for (size_t i = 0; i < args.size(); i++) {
    string_view field = ArgS(args, i++);

    // Verify json path is correct
    if (type == DocIndex::JSON) {
      error_code ec;
      jsoncons::jsonpath::make_expression<JsonType>(field, ec);
      if (ec) {
        (*cntx)->SendError("Bad json path: " + string{field});
        return nullopt;
      }
    }

    // Check optional AS [alias]
    string_view field_alias = field;  // by default "alias" is same as identifier
    if (i + 1 < args.size() && absl::AsciiStrToUpper(ArgS(args, i)) == "AS") {
      field_alias = ArgS(args, i + 1);
      i += 2;
    }

    if (i >= args.size()) {
      (*cntx)->SendError("No field type for field: " + string{field});
      return nullopt;
    }

    // Determine type
    ToUpper(&args[i]);
    auto type_str = ArgS(args, i);
    auto type = ParseSearchFieldType(type_str);
    if (!type) {
      (*cntx)->SendError("Invalid field type: " + string{type_str});
      return nullopt;
    }

    // Skip {algorithm} {dim} flags
    if (*type == search::SchemaField::VECTOR)
      i += 2;

    // Skip all trailing ignored parameters
    while (i + 2 < args.size() && kIgnoredOptions.count(ArgS(args, i + 1)) > 0) {
      i += 2;
    }

    schema.fields[field] = {*type, string{field_alias}};
  }

  // Build field name mapping table
  for (const auto& [field_ident, field_info] : schema.fields)
    schema.field_names[field_info.short_name] = field_ident;

  return schema;
}

optional<SearchParams> ParseSearchParamsOrReply(CmdArgList args, ConnectionContext* cntx) {
  size_t limit_offset = 0, limit_total = 10;
  search::FtVector knn_vector;
  optional<SearchParams::FieldAliasList> alias_list;

  for (size_t i = 0; i < args.size(); i++) {
    ToUpper(&args[i]);

    // [LIMIT offset total]
    if (ArgS(args, i) == "LIMIT") {
      if (i + 2 >= args.size()) {
        (*cntx)->SendError(kSyntaxErr);
        return nullopt;
      }
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &limit_offset) ||
          !absl::SimpleAtoi(ArgS(args, i + 2), &limit_total)) {
        (*cntx)->SendError(kInvalidIntErr);
        return nullopt;
      }
      i += 2;
      continue;
    }

    // RETURN {num} [{ident} AS {name}...]
    if (ArgS(args, i) == "RETURN") {
      if (i + 1 >= args.size()) {
        (*cntx)->SendError(kSyntaxErr);
        return nullopt;
      }

      uint64_t num_fields = args.size();
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &num_fields)) {
        (*cntx)->SendError(kSyntaxErr);
        return nullopt;
      }

      i += 1;

      alias_list = SearchParams::FieldAliasList{};
      while (alias_list->size() < num_fields) {
        if (++i >= args.size()) {
          (*cntx)->SendError(kSyntaxErr);
          return nullopt;
        }

        string_view ident = ArgS(args, i);
        string_view alias = ident;

        if (i + 2 < args.size() && absl::EqualsIgnoreCase(ArgS(args, i + 1), "AS")) {
          alias = ArgS(args, i + 2);
          i += 2;
        }

        alias_list->emplace_back(ident, alias);
      }
      continue;
    }

    // NOCONTENT
    if (ArgS(args, i) == "NOCONTENT") {
      alias_list = SearchParams::FieldAliasList{};
      continue;
    }

    // [PARAMS num(ignored) name(ignored) knn_vector]
    if (ArgS(args, i) == "PARAMS") {
      if (i + 3 >= args.size()) {
        (*cntx)->SendError(kSyntaxErr);
        return nullopt;
      }
      knn_vector = BytesToFtVector(ArgS(args, i + 3));
      i += 3;
      continue;
    }
  }

  return SearchParams{limit_offset, limit_total, std::move(alias_list), std::move(knn_vector)};
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

  partial_sort(docs.begin(),
               docs.begin() + min(params.limit_offset + params.limit_total, knn_limit), docs.end(),
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
  string_view idx_name = ArgS(args, 0);

  DocIndex index{};

  for (size_t i = 1; i < args.size(); i++) {
    ToUpper(&args[i]);

    // [ON HASH | JSON]
    if (ArgS(args, i) == "ON") {
      if (++i >= args.size())
        return (*cntx)->SendError(kSyntaxErr);

      ToUpper(&args[i]);
      string_view type = ArgS(args, i);
      if (type == "HASH")
        index.type = DocIndex::HASH;
      else if (type == "JSON")
        index.type = DocIndex::JSON;
      else
        return (*cntx)->SendError("Invalid rule type: " + string{type});
      continue;
    }

    // [PREFIX count prefix [prefix ...]]
    if (ArgS(args, i) == "PREFIX") {
      if (i + 2 >= args.size())
        return (*cntx)->SendError(kSyntaxErr);

      if (ArgS(args, ++i) != "1")
        return (*cntx)->SendError("Multiple prefixes are not supported");

      index.prefix = ArgS(args, ++i);
      continue;
    }

    // [SCHEMA]
    if (ArgS(args, i) == "SCHEMA") {
      if (i++ >= args.size())
        return (*cntx)->SendError("Empty schema");

      auto schema = ParseSchemaOrReply(index.type, args.subspan(i), cntx);
      if (!schema)
        return;
      index.schema = move(*schema);
      break;  // SCHEMA always comes last
    }
  }

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
  if (!search_algo.Init(query_str, {move(params->knn_vector)}))
    return (*cntx)->SendError("Query syntax error");

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
    return (*cntx)->SendError(string{index_name} + ": no such index");

  if (auto knn_limit = search_algo.HasKnn(); knn_limit)
    ReplyKnn(*knn_limit, *params, absl::MakeSpan(docs), cntx);
  else
    ReplyWithResults(*params, absl::MakeSpan(docs), cntx);
}

#define HFUNC(x) SetHandler(&SearchFamily::x)

// Redis search is a module. Therefore we introduce dragonfly extension search
// to set as the default for the search family of commands. More sensible defaults,
// should also be considered in the future

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"FT.CREATE", CO::GLOBAL_TRANS, -2, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtCreate)
            << CI{"FT.DROPINDEX", CO::GLOBAL_TRANS, -2, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtDropIndex)
            << CI{"FT.INFO", CO::GLOBAL_TRANS, 2, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtInfo)
            // Underscore same as in RediSearch because it's "temporary" (long time already)
            << CI{"FT._LIST", CO::GLOBAL_TRANS, 1, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtList)
            << CI{"FT.SEARCH", CO::GLOBAL_TRANS, -3, 0, 0, 0, acl::FT_SEARCH}.HFUNC(FtSearch);
}

}  // namespace dfly
