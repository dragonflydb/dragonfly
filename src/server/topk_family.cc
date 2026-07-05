// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/topk.h"
#include "facade/cmd_arg_parser.h"
#include "facade/reply_builder.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/error.h"
#include "server/transaction.h"
#include "server/tx_base.h"

namespace dfly {

using namespace std;
using namespace facade;

struct TopkFamily {
  static void Reserve(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Add(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void IncrBy(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Query(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Count(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void List(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Info(facade::CmdArgParser parser, CommandContext* cmd_cntx);
};

namespace {

constexpr char kDecayRangeErr[] = "decay must be between 0 and 1";
constexpr char kIncrRangeErr[] = "increment must be between 1 and 100000";

RuleError DecayRange(double v) {
  return {!(v >= 0 && v <= 1), kDecayRangeErr};
}

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

void TopkFamily::Reserve(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
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
    decay = parser.Next<Validated<double, DecayRange>>();
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
  }

  if (!parser.Finalize())
    return rb->SendError(parser.TakeError().MakeReply());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, k, width, depth, decay);
  };

  OpStatus result = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (HandleOpError(result, cmd_cntx))
    return;
  rb->SendOk();
}

void TopkFamily::Add(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  CmdArgParser::Range item_range = parser.RemainingRange(kSyntaxErr);
  RETURN_ON_PARSE_ERROR(parser, rb);

  vector<string_view> items{item_range.begin(), item_range.end()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, items);
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

void TopkFamily::IncrBy(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  vector<pair<string_view, uint32_t>> items;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  items.reserve(parser.UnparsedArgs().size() / 2);
  while (parser.HasNext()) {
    auto [item, incr] =
        parser.Next<string_view,
                    Validated<uint32_t, Bounded<uint32_t{1}, uint32_t{100000}, kIncrRangeErr>>>();
    items.emplace_back(item, incr);
  }
  RETURN_ON_PARSE_ERROR(parser, rb);

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

void TopkFamily::Query(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  CmdArgParser::Range item_range = parser.RemainingRange(kSyntaxErr);
  RETURN_ON_PARSE_ERROR(parser, rb);

  vector<string_view> items{item_range.begin(), item_range.end()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpQuery(t->GetOpArgs(shard), key, items);
  };
  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (HandleOpError(result.status(), cmd_cntx))
    return;

  // Build array response
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (int present : *result) {
      rb->SendLong(present);
    }
  }
}

void TopkFamily::Count(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  CmdArgParser::Range item_range = parser.RemainingRange(kSyntaxErr);
  RETURN_ON_PARSE_ERROR(parser, rb);

  vector<string_view> items{item_range.begin(), item_range.end()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCount(t->GetOpArgs(shard), key, items);
  };
  auto result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (HandleOpError(result.status(), cmd_cntx))
    return;

  // Build array response
  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(result->size());
    for (uint32_t count : *result) {
      rb->SendLong(count);
    }
  }
}

void TopkFamily::List(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  bool with_count = false;

  parser.Apply(OneOf(Exist("WITHCOUNT", &with_count)));

  if (!parser.Finalize())
    return rb->SendError(parser.TakeError().MakeReply());

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

void TopkFamily::Info(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (!parser.Finalize())
    return rb->SendError(parser.TakeError().MakeReply());

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
