// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/bloom.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_families.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

constexpr double kDefaultFpProb = 0.01;
constexpr double kDefaultGrowFactor = 2;
struct SbfParams {
  uint32_t init_capacity;
  double error;
  double grow_factor = kDefaultGrowFactor;

  bool ok() const {
    return error > 0 and error < 0.5;
  }
};

using AddResult = absl::InlinedVector<OpResult<bool>, 4>;
using ExistsResult = absl::InlinedVector<bool, 4>;

OpStatus OpReserve(const SbfParams& params, const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_SBF);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  PrimeValue& pv = op_res->it->second;
  pv.SetSBF(params.init_capacity, params.error, params.grow_factor);

  return OpStatus::OK;
}

// Returns true, if item was added, false if it was already "present".
OpResult<AddResult> OpAdd(const OpArgs& op_args, string_view key, CmdArgList items) {
  auto& db_slice = op_args.GetDbSlice();

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_SBF);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue& pv = op_res->it->second;

  if (op_res->is_new) {
    pv.SetSBF(0, kDefaultFpProb, kDefaultGrowFactor);
  }

  SBF* sbf = pv.GetSBF();
  AddResult result(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    result[i] = sbf->Add(ToSV(items[i]));
  }
  return result;
}

OpResult<ExistsResult> OpExists(const OpArgs& op_args, string_view key, CmdArgList items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_SBF);
  if (!op_res)
    return op_res.status();
  auto it = (*op_res);

  const SBF* sbf = it->second.GetSBF();
  ExistsResult result(items.size());

  for (size_t i = 0; i < items.size(); ++i) {
    result[i] = sbf->Exists(ToSV(items[i]));
  }

  return result;
}

void CmdReserve(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();
  SbfParams params;

  tie(params.error, params.init_capacity) = parser.Next<double, uint32_t>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (parser.TakeError())
    return rb->SendError(kSyntaxErr);

  if (!params.ok())
    return rb->SendError("error rate is out of range", kSyntaxErrType);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(params, t->GetOpArgs(shard), key);
  };

  OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("item exists");
  }
  return rb->SendError(res);
}

void CmdAdd(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  OpStatus status = res.status();
  if (res) {
    if (res->front())
      return cmd_cntx->SendLong(*res->front());
    else
      status = res->front().status();
  }

  return cmd_cntx->SendError(status);
}

void CmdExists(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);
  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExists(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  return cmd_cntx->SendLong(res ? res->front() : 0);
}

void CmdMAdd(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, args);
  };

  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  OpResult res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res) {
    return rb->SendError(res.status());
  }
  const AddResult& add_res = *res;

  RedisReplyBuilder::ArrayScope scope{rb, add_res.size()};
  for (const OpResult<bool>& val : add_res) {
    if (val) {
      rb->SendLong(*val);
    } else {
      rb->SendError(val.status());
    }
  }
}

void CmdScanDump(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  const string_view key = parser.Next();
  const int64_t cursor = parser.Next<int64_t>();
  if (cursor < 0)
    return rb->SendError(kInvalidIntErr);

  if (const auto err = parser.TakeError(); err)
    return rb->SendError(err.MakeReply());

  const auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<SBFChunk> {
    const auto& db_slice = t->GetDbSlice(shard->shard_id());
    OpResult op_res = db_slice.FindReadOnly(t->GetOpArgs(shard).db_cntx, key, OBJ_SBF);
    if (!op_res)
      return op_res.status();

    const SBF* sbf = op_res.value()->second.GetSBF();
    SBFDumpIterator it(*sbf, cursor);
    return it.Next();
  };

  OpResult<SBFChunk> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res) {
    return rb->SendError(res.status());
  }

  RedisReplyBuilder::ArrayScope scope{rb, 2};
  rb->SendLong(res->cursor);
  if (res->cursor == 0)
    DCHECK(res->data.empty()) << " scan ended with inconsistent state";
  rb->SendBulkString(res->data);
}

void CmdLoadChunk(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  const std::string_view key = parser.Next();

  const int64_t cursor = parser.Next<int64_t>();
  if (const auto err = parser.TakeError(); err)
    return rb->SendError(err.MakeReply());

  if (cursor <= 0)
    return rb->SendError(kInvalidIntErr);

  const std::string_view blob = parser.Next();

  const auto cb = [&](Transaction* t, EngineShard* shard) -> OpStatus {
    auto op_args = t->GetOpArgs(shard);
    auto& db_slice = op_args.GetDbSlice();

    if (cursor == 1) {
      auto load_result = LoadSBFHeader(blob, CompactObj::memory_resource());
      if (!load_result.has_value()) {
        LOG_EVERY_T(WARNING, 10) << "BF.LOADCHUNK invalid header"
                                 << " key=" << key << " cursor=" << cursor
                                 << " blob_size=" << blob.size()
                                 << " load_res=" << ToString(load_result.error());
        return OpStatus::INVALID_VALUE;
      }

      // type set to nullopt to find any type key and overwrite it, not just SBF
      auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, std::nullopt);
      if (!op_res) {
        CompactObj::DeleteMR<SBF>(load_result.value());
        return op_res.status();
      }

      // LOADCHUNK overwrites existing key
      if (!op_res->is_new) {
        // existing key might not necessarily be SBF, it could be HASH/JSON, and indexed
        RemoveKeyFromIndexesIfNeeded(key, op_args.db_cntx, op_res->it->second, op_args.shard);
        db_slice.RemoveExpire(op_args.db_cntx.db_index, op_res->it);
      }

      op_res->it->second.SetSBF(load_result.value());
      return OpStatus::OK;
    }

    auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_SBF);
    if (!op_res)
      return op_res.status();

    SBF* sbf = op_res->it->second.GetSBF();
    if (auto load_res = LoadSBFChunk(cursor, blob, sbf); load_res != SBFLoadResult::kOk) {
      LOG_EVERY_T(WARNING, 10) << "BF.LOADCHUNK invalid chunk"
                               << " key=" << key << " cursor=" << cursor
                               << " blob_size=" << blob.size()
                               << " load_res=" << ToString(load_res);
      return OpStatus::OUT_OF_RANGE;
    }

    return OpStatus::OK;
  };

  const OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::OK)
    return rb->SendOk();
  if (res == OpStatus::INVALID_VALUE)
    return rb->SendError("INVALIDOBJ invalid bloom dump payload");
  return rb->SendError(res);
}

void CmdMExists(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExists(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RedisReplyBuilder::ArrayScope scope{rb, args.size()};
  for (size_t i = 0; i < args.size(); ++i) {
    rb->SendLong(res ? res->at(i) : 0);
  }
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterBloomFamily(CommandRegistry* registry) {
  registry->StartFamily();

  *registry
      << CI{"BF.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::BLOOM}.HFUNC(
             Reserve)
      << CI{"BF.ADD", CO::JOURNALED | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Add)
      << CI{"BF.MADD", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(MAdd)
      << CI{"BF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Exists)
      << CI{"BF.MEXISTS", CO::READONLY | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(MExists)
      << CI{"BF.SCANDUMP", CO::READONLY, 3, 1, 1, acl::BLOOM}.HFUNC(ScanDump)
      << CI{"BF.LOADCHUNK", CO::JOURNALED | CO::DENYOOM, 4, 1, 1, acl::BLOOM}.HFUNC(LoadChunk);
};

}  // namespace dfly
