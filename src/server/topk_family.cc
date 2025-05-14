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

using CI = CommandId;

#define HFUNC(x) SetHandler(&TopKeysFamily::x)

void TopKeysFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"TOPK.RESERVE", CO::WRITE | CO::DENYOOM, -2, 1, 1, acl::TOPK}.HFUNC(Reserve)
            << CI{"TOPK.LIST", CO::WRITE | CO::DENYOOM, -1, 1, 1, acl::TOPK}.HFUNC(List)
            << CI{"TOPK.ADD", CO::WRITE | CO::DENYOOM, -2, 1, 1, acl::TOPK}.HFUNC(Add);
};

}  // namespace dfly
