// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/topk_family.h"

#include "base/cycle_clock.h"
#include "base/logging.h"
#include "core/topk.h"
#include "error.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

// Default TOPK parameters
constexpr uint32_t kDefaultWidth = 8;
constexpr uint32_t kDefaultDepth = 7;
constexpr double kDefaultDecay = 0.9;

OpStatus OpReserve(const OpArgs& op_args, string_view key, uint32_t k, uint32_t width,
                   uint32_t depth, double decay) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_TOPK);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new) {
    return OpStatus::KEY_EXISTS;
  }

  op_res->it->second.SetTOPK(k, width, depth, decay);
  return OpStatus::OK;
}

OpResult<vector<optional<string>>> OpAdd(const OpArgs& op_args, string_view key,
                                         const vector<string_view>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  TOPK* topk = op_res->it->second.GetTOPK();
  vector<optional<string>> result;
  result.reserve(items.size());

  for (const auto& item : items) {
    auto expelled = topk->Add(item);
    if (expelled.empty()) {
      result.push_back(nullopt);
    } else {
      result.push_back(expelled[0]);
    }
  }

  return result;
}

OpResult<vector<optional<string>>> OpIncrBy(const OpArgs& op_args, string_view key,
                                            const vector<pair<string_view, uint32_t>>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  TOPK* topk = op_res->it->second.GetTOPK();
  vector<optional<string>> result;
  result.reserve(items.size());

  for (const auto& [item, incr] : items) {
    auto expelled = topk->IncrBy(item, incr);
    if (expelled.empty()) {
      result.emplace_back(nullopt);
    } else {
      result.emplace_back(expelled[0]);
    }
  }

  return result;
}

OpResult<vector<int>> OpQuery(const OpArgs& op_args, string_view key,
                              const vector<string_view>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = op_res.value()->second.GetTOPK();
  return topk->Query(items);
}

OpResult<vector<uint32_t>> OpCount(const OpArgs& op_args, string_view key,
                                   const vector<string_view>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = op_res.value()->second.GetTOPK();
  return topk->Count(items);
}

OpResult<vector<TOPK::TopKItem>> OpList(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = op_res.value()->second.GetTOPK();
  return topk->List();
}

struct TopkInfo {
  uint32_t k;
  uint32_t width;
  uint32_t depth;
  double decay;
};

OpResult<TopkInfo> OpInfo(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = op_res.value()->second.GetTOPK();
  TopkInfo info;
  info.k = topk->K();
  info.width = topk->Width();
  info.depth = topk->Depth();
  info.decay = topk->Decay();
  return info;
}

}  // namespace

void TopkFamily::Reserve(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  uint32_t k = parser.Next<uint32_t>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (k == 0) {
    return rb->SendError("k must be greater than 0");
  }

  // Optional parameters
  uint32_t width = kDefaultWidth;
  uint32_t depth = kDefaultDepth;
  double decay = kDefaultDecay;

  if (parser.HasNext()) {
    width = parser.Next<uint32_t>();
    RETURN_ON_PARSE_ERROR(parser, rb);

    if (parser.HasNext()) {
      depth = parser.Next<uint32_t>();
      RETURN_ON_PARSE_ERROR(parser, rb);

      if (parser.HasNext()) {
        decay = parser.Next<double>();
        RETURN_ON_PARSE_ERROR(parser, rb);

        if (decay < 0.0 || decay > 1.0) {
          return rb->SendError("decay must be between 0 and 1");
        }
      }
    }
  }

  if (width == 0 || depth == 0) {
    return rb->SendError("width and depth must be greater than 0");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, k, width, depth, decay);
  };

  OpStatus result = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));

  if (result == OpStatus::KEY_EXISTS) {
    return rb->SendError("item exists");
  }

  rb->SendOk();
}

void TopkFamily::Add(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  vector<string_view> items;
  while (parser.HasNext()) {
    items.push_back(parser.Next());
  }

  if (items.empty()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, items);
  };

  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx->SendError(kKeyNotFoundErr);
  }

  if (result.status() == OpStatus::WRONG_TYPE) {
    return cmd_cntx->SendError(kWrongTypeErr);
  }

  // Build array response
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (const auto& expelled : *result) {
      if (expelled.has_value()) {
        rb->SendBulkString(*expelled);
      } else {
        rb->SendNull();
      }
    }
  }
}

void TopkFamily::IncrBy(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  vector<pair<string_view, uint32_t>> items;
  while (parser.HasNext()) {
    string_view item = parser.Next();
    if (!parser.HasNext()) {
      return cmd_cntx->SendError(kSyntaxErr);
    }
    uint32_t incr = parser.Next<uint32_t>();
    if (parser.HasError()) {
      return cmd_cntx->SendError(parser.TakeError().MakeReply());
    }
    if (incr < 1 || incr > 100000) {  // Redis limits increment to [1, 100000]
      return cmd_cntx->SendError("increment must be between 1 and 100000");
    }
    items.emplace_back(item, incr);
  }

  if (items.empty()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, items);
  };

  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx->SendError(kKeyNotFoundErr);
  }

  if (result.status() == OpStatus::WRONG_TYPE) {
    return cmd_cntx->SendError(kWrongTypeErr);
  }

  // Build array response
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (const auto& expelled : *result) {
      if (expelled.has_value()) {
        rb->SendBulkString(*expelled);
      } else {
        rb->SendNull();
      }
    }
  }
}

void TopkFamily::Query(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  vector<string_view> items;
  while (parser.HasNext()) {
    items.push_back(parser.Next());
  }

  if (items.empty()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpQuery(t->GetOpArgs(shard), key, items);
  };

  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx->SendError(kKeyNotFoundErr);
  }

  if (result.status() == OpStatus::WRONG_TYPE) {
    return cmd_cntx->SendError(kWrongTypeErr);
  }

  // Build array response
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (int present : *result) {
      rb->SendLong(present);
    }
  }
}

void TopkFamily::Count(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  vector<string_view> items;
  while (parser.HasNext()) {
    items.push_back(parser.Next());
  }

  if (items.empty()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCount(t->GetOpArgs(shard), key, items);
  };

  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx->SendError(kKeyNotFoundErr);
  }

  if (result.status() == OpStatus::WRONG_TYPE) {
    return cmd_cntx->SendError(kWrongTypeErr);
  }

  // Build array response
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (uint32_t count : *result) {
      rb->SendLong(count);
    }
  }
}

void TopkFamily::List(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  bool with_count = false;
  if (parser.HasNext()) {
    string_view flag = parser.Next();
    if (absl::EqualsIgnoreCase(flag, "WITHCOUNT")) {
      with_count = true;
    } else {
      return cmd_cntx->SendError(kSyntaxErr);
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpList(t->GetOpArgs(shard), key); };

  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx->SendError(kKeyNotFoundErr);
  }

  if (result.status() == OpStatus::WRONG_TYPE) {
    return cmd_cntx->SendError(kWrongTypeErr);
  }

  // Build array response
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  {
    SinkReplyBuilder::ReplyScope scope(rb);
    if (with_count) {
      // Flat array: [item1, count1, item2, count2, ...]
      rb->StartArray(result->size() * 2);
      for (const auto& topk_item : *result) {
        rb->SendBulkString(topk_item.item);
        rb->SendLong(topk_item.count);
      }
    } else {
      // Array of items only
      rb->StartArray(result->size());
      for (const auto& topk_item : *result) {
        rb->SendBulkString(topk_item.item);
      }
    }
  }
}

void TopkFamily::Info(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpInfo(t->GetOpArgs(shard), key); };

  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx->SendError(kKeyNotFoundErr);
  }

  if (result.status() == OpStatus::WRONG_TYPE) {
    return cmd_cntx->SendError(kWrongTypeErr);
  }

  // Build array response: [k, <k>, width, <width>, depth, <depth>, decay, <decay>]
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(8);
    rb->SendBulkString("k");
    rb->SendLong(result->k);
    rb->SendBulkString("width");
    rb->SendLong(result->width);
    rb->SendBulkString("depth");
    rb->SendLong(result->depth);
    rb->SendBulkString("decay");
    rb->SendDouble(result->decay);
  }
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&TopkFamily::x)

void RegisterTopkFamily(CommandRegistry* registry) {
  registry->StartFamily();
  *registry
      << CI{"TOPK.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(
             Reserve)
      << CI{"TOPK.ADD", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(Add)
      << CI{"TOPK.INCRBY", CO::JOURNALED | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::BLOOM}.HFUNC(
             IncrBy)
      << CI{"TOPK.QUERY", CO::READONLY | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(Query)
      << CI{"TOPK.COUNT", CO::READONLY | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(Count)
      << CI{"TOPK.LIST", CO::READONLY | CO::FAST, -2, 1, 1, acl::BLOOM}.HFUNC(List)
      << CI{"TOPK.INFO", CO::READONLY | CO::FAST, 2, 1, 1, acl::BLOOM}.HFUNC(Info);
}

}  // namespace dfly
