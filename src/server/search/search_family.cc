// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/strings/ascii.h>

#include <atomic>
#include <jsoncons/json.hpp>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/json_object.h"
#include "core/search/search.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
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

unordered_map<string_view, search::Schema::FieldType> kSchemaTypes = {
    {"TAG"sv, search::Schema::TAG},
    {"TEXT"sv, search::Schema::TEXT},
    {"NUMERIC"sv, search::Schema::NUMERIC},
    {"VECTOR"sv, search::Schema::VECTOR}};

static const set<string_view> kIgnoredOptions = {"WEIGHT", "SEPARATOR", "TYPE", "DIM",
                                                 "DISTANCE_METRIC"};

optional<search::Schema> ParseSchemaOrReply(CmdArgList args, ConnectionContext* cntx) {
  search::Schema schema;
  for (size_t i = 0; i < args.size(); i++) {
    string_view field = ArgS(args, i);
    if (i++ >= args.size()) {
      (*cntx)->SendError("No field type for field: " + string{field});
      return nullopt;
    }

    ToUpper(&args[i]);
    string_view type_str = ArgS(args, i);
    auto it = kSchemaTypes.find(type_str);
    if (it == kSchemaTypes.end()) {
      (*cntx)->SendError("Invalid field type: " + string{type_str});
      return nullopt;
    }

    // Skip {algorithm} {dim} flags
    if (it->second == search::Schema::VECTOR)
      i += 2;

    // Skip all trailing ignored parameters
    while (i + 2 < args.size() && kIgnoredOptions.count(ArgS(args, i + 1)) > 0) {
      i += 2;
    }

    schema.fields[field] = it->second;
  }

  return schema;
}

optional<SearchParams> ParseSearchParamsOrReply(CmdArgList args, ConnectionContext* cntx) {
  size_t limit_offset = 0, limit_total = 10;
  search::FtVector knn_vector;

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

  return SearchParams{limit_offset, limit_total, move(knn_vector)};
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

  size_t response_count =
      min(total_count - min(total_count, params.limit_offset), params.limit_total);

  facade::SinkReplyBuilder::ReplyAggregator agg{cntx->reply_builder()};

  (*cntx)->StartArray(response_count * 2 + 1);
  (*cntx)->SendLong(total_count);

  size_t sent = 0;
  size_t to_skip = 0;
  for (const auto& shard_docs : results) {
    for (const auto& serialized_doc : shard_docs.docs) {
      // Scoring is not implemented yet, so we just cut them in the order they were retrieved
      if (to_skip > 0) {
        to_skip--;
        continue;
      }

      if (sent++ >= response_count)
        return;

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

  partial_sort(docs.begin(), docs.end(),
               docs.begin() + min(params.limit_offset + params.limit_total, knn_limit),
               [](const auto* l, const auto* r) { return l->knn_distance < r->knn_distance; });
  docs.resize(min(docs.size(), knn_limit));

  size_t response_count =
      min(docs.size() - min(docs.size(), params.limit_offset), params.limit_total);

  (*cntx)->StartArray(response_count * 2 + 1);
  (*cntx)->SendLong(docs.size());
  for (auto* doc : absl::MakeSpan(docs).subspan(params.limit_offset, response_count)) {
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

      auto schema = ParseSchemaOrReply(args.subspan(i), cntx);
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

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"FT.CREATE", CO::GLOBAL_TRANS, -2, 0, 0, 0}.HFUNC(FtCreate)
            << CI{"FT.SEARCH", CO::GLOBAL_TRANS, -3, 0, 0, 0}.HFUNC(FtSearch);
}

}  // namespace dfly
