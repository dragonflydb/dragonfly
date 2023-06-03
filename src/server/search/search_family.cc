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
    {"NUMERIC"sv, search::Schema::NUMERIC}};

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

    // Skip optional WEIGHT or SEPARATOR flags
    if (i + 2 < args.size() &&
        (ArgS(args, i + 1) == "WEIGHT" || ArgS(args, i + 1) == "SEPARATOR")) {
      i += 2;
    }

    schema.fields[field] = it->second;
  }

  return schema;
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
    es->search_indices()->Init(tx->GetOpArgs(es), idx_name, idx_ptr);
    return OpStatus::OK;
  });

  (*cntx)->SendOk();
}

void SearchFamily::FtSearch(CmdArgList args, ConnectionContext* cntx) {
  string_view index_name = ArgS(args, 0);
  string_view query_str = ArgS(args, 1);

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(query_str))
    return (*cntx)->SendError("Query syntax error");

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  atomic<bool> index_not_found{false};
  vector<vector<SerializedSearchDoc>> docs(shard_set->size());

  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->Get(index_name); index)
      docs[es->shard_id()] = index->Search(t->GetOpArgs(es), &search_algo);
    else
      index_not_found.store(true, memory_order_relaxed);
    return OpStatus::OK;
  });

  if (index_not_found.load())
    return (*cntx)->SendError(string{index_name} + ": no such index");

  size_t total_count = 0;
  for (const auto& shard_docs : docs)
    total_count += shard_docs.size();

  (*cntx)->StartArray(total_count * 2 + 1);
  (*cntx)->SendLong(total_count);
  for (const auto& shard_docs : docs) {
    for (const auto& [key, doc] : shard_docs) {
      (*cntx)->SendBulkString(key);
      (*cntx)->StartCollection(doc.size(), RedisReplyBuilder::MAP);
      for (const auto& [k, v] : doc) {
        (*cntx)->SendBulkString(k);
        (*cntx)->SendBulkString(v);
      }
    }
  }
}

#define HFUNC(x) SetHandler(&SearchFamily::x)

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"FT.CREATE", CO::GLOBAL_TRANS, -2, 0, 0, 0}.HFUNC(FtCreate)
            << CI{"FT.SEARCH", CO::GLOBAL_TRANS, -3, 0, 0, 0}.HFUNC(FtSearch);
}

}  // namespace dfly
