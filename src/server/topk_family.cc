// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/topk_family.h"

#include <type_traits>

#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

void TopKeysFamily::Reserve(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  auto total_elements = parser.Next<size_t>();
  if (parser.HasError()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  auto cb = [key, total_elements](Transaction* tx, EngineShard* es) -> OpResult<bool> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto res = db_slice.AddOrFind(db_cntx, key);
    if (!res) {
      return res.status();
    }

    if (!res->is_new) {
      return OpStatus::KEY_EXISTS;
    }

    res->it->second.SetTopK(total_elements, (1 << 16), 4, 1.08);
    return OpStatus::OK;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  if (res.status() == OpStatus::KEY_EXISTS) {
    return cmd_cntx.rb->SendError("key already exists");
  }
  return cmd_cntx.rb->SendOk();
}

void TopKeysFamily::Add(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<std::string_view> values;
  while (parser.HasNext()) {
    auto val = parser.Next<std::string_view>();
    values.push_back(val);
  }

  if (parser.HasError()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  using Result = std::vector<std::string>;
  auto cb = [key, &values](Transaction* tx, EngineShard* es) -> OpResult<Result> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TOPK);
    if (!it) {
      return it.status();
    }
    auto* topk = it->it->second.GetTopK();
    Result results;
    results.reserve(values.size());
    for (auto val : values) {
      // TODO return key if removed because of an exponential delay
      topk->Touch(val);
      results.push_back("nil");
    }
    return results;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  // TODO fix return reply once Touch signature changes. See comment in cb above
  rb->StartArray(res->size());
  for ([[maybe_unused]] const auto& reply : *res) {
    rb->SendNull();
  }
}

void TopKeysFamily::Query(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  absl::flat_hash_map<std::string_view, bool> items;
  std::vector<std::string_view> results;
  while (parser.HasNext()) {
    auto val = parser.Next<std::string_view>();
    items[val] = false;
    results.push_back(val);
  }

  if (parser.HasError()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  auto cb = [key, &items](Transaction* tx, EngineShard* es) -> OpResult<bool> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TOPK);
    if (!it) {
      return it.status();
    }
    auto* topk = it->it->second.GetTopK();
    topk->Query(&items);
    return OpStatus::OK;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(results.size());
  for (auto res : results) {
    DCHECK(items.contains(res));
    if (items.find(res)->second) {
      rb->SendLong(1);
      continue;
    }
    rb->SendLong(0);
  }
}

void TopKeysFamily::List(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  // if (parser.HasError()) {
  //   return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  // }

  // even declval() is too verbose here :scream:
  // using Result = std::invoke_result_t<decltype(&TopKeys::GetTopKeys), TopKeys>;
  using Result = absl::flat_hash_map<std::string, uint64_t>;
  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<Result> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TOPK);
    if (!it) {
      return it.status();
    }
    auto* topk = it->it->second.GetTopK();
    return topk->GetTopKeys();
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(res->size());
  for (const auto& reply : *res) {
    rb->SendSimpleString(reply.first);
  }
}

void TopKeysFamily::Info(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();

  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<TopKeys::Options> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TOPK);
    if (!it) {
      return it.status();
    }
    auto* topk = it->it->second.GetTopK();
    return topk->GetOptions();
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(4 * 2);
  rb->SendSimpleString("k");
  rb->SendLong(res->buckets * res->depth);
  rb->SendSimpleString("width");
  rb->SendLong(res->buckets);
  rb->SendSimpleString("depth");
  rb->SendLong(res->depth);
  rb->SendSimpleString("decay");
  rb->SendDouble(res->decay_base);
}

struct IncrByT {
  std::string_view name;
  size_t incr;
};

void TopKeysFamily::IncrBy(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<IncrByT> incrs;
  while (parser.HasNext()) {
    auto name = parser.Next<std::string_view>();
    auto val = parser.Next<size_t>();
    if (val < 1 || val > 100000) {
      return cmd_cntx.rb->SendError(facade::kInvalidIntErr);
    }
    incrs.push_back({name, val});
  }

  if (parser.HasError()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  using Result = std::vector<std::string>;
  auto cb = [key, &incrs](Transaction* tx, EngineShard* es) -> OpResult<Result> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TOPK);
    if (!it) {
      return it.status();
    }
    auto* topk = it->it->second.GetTopK();
    Result results;
    for (auto [name, incr] : incrs) {
      // TODO return key if removed because of an exponential delay
      topk->Touch(name, incr);
      results.push_back("nil");
    }
    return results;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // Todo we should sent an array reply instead. If a key got evicted from TopKeys while it was
  // touched/incremented we should return that. Otherwise `nill`
  return cmd_cntx.rb->SendError(res.status());
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&TopKeysFamily::x)

void TopKeysFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"TOPK.RESERVE", CO::WRITE | CO::DENYOOM, -3, 1, 1, acl::TOPK}.HFUNC(Reserve)
            << CI{"TOPK.LIST", CO::READONLY, -2, 1, 1, acl::TOPK}.HFUNC(List)
            << CI{"TOPK.QUERY", CO::READONLY, -2, 1, 1, acl::TOPK}.HFUNC(Query)
            << CI{"TOPK.INFO", CO::READONLY, 2, 1, 1, acl::TOPK}.HFUNC(Info)
            << CI{"TOPK.INCRBY", CO::WRITE | CO::DENYOOM, -4, 1, 1, acl::TOPK}.HFUNC(IncrBy)
            << CI{"TOPK.ADD", CO::WRITE | CO::DENYOOM, -3, 1, 1, acl::TOPK}.HFUNC(Add);
};

}  // namespace dfly
