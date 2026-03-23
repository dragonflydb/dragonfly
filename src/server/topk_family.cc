// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/topk_family.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/topk.h"
#include "facade/cmd_arg_parser.h"
#include "facade/reply_builder.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/error.h"
#include "server/transaction.h"
#include "server/tx_base.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

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
    result.push_back(topk->Add(item));
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
    result.push_back(topk->IncrBy(item, incr));
  }

  return result;
}

OpResult<vector<int>> OpQuery(const OpArgs& op_args, string_view key,
                              const vector<string_view>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = (*op_res)->second.GetTOPK();
  vector<int> result;
  result.reserve(items.size());

  for (const auto& item : items) {
    result.push_back(topk->Query(item));
  }

  return result;
}

OpResult<vector<uint32_t>> OpCount(const OpArgs& op_args, string_view key,
                                   const vector<string_view>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = (*op_res)->second.GetTOPK();
  vector<uint32_t> result;
  result.reserve(items.size());

  for (const auto& s : items) {
    result.push_back(topk->Count(s));
  }

  return result;
}

OpResult<vector<TOPK::TopKItem>> OpList(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_TOPK);
  if (!op_res)
    return op_res.status();

  const TOPK* topk = (*op_res)->second.GetTOPK();
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

  const TOPK* topk = (*op_res)->second.GetTOPK();
  TopkInfo info;
  info.k = topk->K();
  info.width = topk->Width();
  info.depth = topk->Depth();
  info.decay = topk->Decay();
  return info;
}

// Returns true if an error occurred and a reply was sent.
bool HandleOpError(OpStatus status, CommandContext* cmd_cntx) {
  if (status == OpStatus::OK)
    return false;

  if (status == OpStatus::KEY_NOTFOUND) {
    cmd_cntx->SendError(kKeyNotFoundErr);
  } else if (status == OpStatus::WRONG_TYPE) {
    cmd_cntx->SendError(kWrongTypeErr);
  } else if (status == OpStatus::KEY_EXISTS) {
    cmd_cntx->SendError("item exists");  // Specific to TOPK.RESERVE
  } else {
    cmd_cntx->SendError(status);  // Catch OOM, Timeout, etc.
  }
  return true;
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

  static constexpr uint32_t kMax = 100'000;  // Limit to prevent excessive memory usage (OOM)
  if (k > kMax) {
    return rb->SendError(absl::StrCat("k exceeds maximum allowed value of ", kMax));
  }

  // Optional parameters
  uint32_t width = TOPK::kDefaultWidth;
  uint32_t depth = TOPK::kDefaultDepth;
  double decay = TOPK::kDefaultDecay;

  if (parser.HasNext()) {
    width = parser.Next<uint32_t>();
    depth = parser.Next<uint32_t>();
    decay = parser.Next<double>();
    RETURN_ON_PARSE_ERROR(parser, rb);

    if ((width == 0) || (depth == 0)) {
      return rb->SendError("width and depth must be greater than 0");
    }

    // Width capped at 1M. Depth capped at 100 (which is already overkill).
    // Max theoretical memory: 1,000,000 * 100 * 4 bytes = ~400 MB. (Safe!)
    static constexpr uint32_t kMaxWidth = 1'000'000;
    static constexpr uint32_t kMaxDepth = 100;
    if ((width > kMaxWidth) || (depth > kMaxDepth)) {
      return rb->SendError(absl::StrCat("width must not exceed ", kMaxWidth,
                                        " and depth must not exceed ", kMaxDepth));
    }

    if (!std::isfinite(decay) || (decay < 0.0) || (decay > 1.0)) {
      return rb->SendError("decay must be between 0 and 1");
    }
  }

  if (parser.HasNext()) {
    return rb->SendError(kSyntaxErr);
  }

  // Guard against overflow: width * depth * sizeof(uint32_t) must fit in size_t.
  if (width > SIZE_MAX / sizeof(uint32_t) / depth) {
    return rb->SendError("width * depth is too large");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, k, width, depth, decay);
  };

  OpStatus result = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (HandleOpError(result, cmd_cntx))
    return;
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
  if (HandleOpError(result.status(), cmd_cntx))
    return;

  // Build array response
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (const auto& evicted : *result) {
      if (evicted.has_value()) {
        rb->SendBulkString(*evicted);
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
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  while (parser.HasNext()) {
    string_view item = parser.Next();
    if (!parser.HasNext()) {
      return cmd_cntx->SendError(kSyntaxErr);
    }
    uint32_t incr = parser.Next<uint32_t>();
    RETURN_ON_PARSE_ERROR(parser, rb);
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
  if (HandleOpError(result.status(), cmd_cntx))
    return;

  // Build array response
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (const auto& evicted : *result) {
      if (evicted.has_value()) {
        rb->SendBulkString(*evicted);
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
  if (HandleOpError(result.status(), cmd_cntx))
    return;

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
  if (HandleOpError(result.status(), cmd_cntx))
    return;

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

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (parser.HasNext()) {
    return rb->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpList(t->GetOpArgs(shard), key); };
  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (HandleOpError(result.status(), cmd_cntx))
    return;

  // Build array response
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size() * (with_count ? 2 : 1));

    for (const auto& topk_item : *result) {
      rb->SendBulkString(topk_item.item);
      if (with_count) {
        rb->SendLong(topk_item.count);
      }
    }
  }
}

void TopkFamily::Info(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (parser.HasNext()) {
    return rb->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpInfo(t->GetOpArgs(shard), key); };
  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (HandleOpError(result.status(), cmd_cntx))
    return;

  // Build array response: [k, <k>, width, <width>, depth, <depth>, decay, <decay>]
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
  registry->StartFamily(acl::TOPK);
  *registry << CI{"TOPK.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1}.HFUNC(Reserve)
            << CI{"TOPK.ADD", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1}.HFUNC(Add)
            << CI{"TOPK.INCRBY", CO::JOURNALED | CO::DENYOOM | CO::FAST, -4, 1, 1}.HFUNC(IncrBy)
            << CI{"TOPK.QUERY", CO::READONLY | CO::FAST, -3, 1, 1}.HFUNC(Query)
            << CI{"TOPK.COUNT", CO::READONLY | CO::FAST, -3, 1, 1}.HFUNC(Count)
            << CI{"TOPK.LIST", CO::READONLY, -2, 1, 1}.HFUNC(List)
            << CI{"TOPK.INFO", CO::READONLY | CO::FAST, 2, 1, 1}.HFUNC(Info);
}

}  // namespace dfly
