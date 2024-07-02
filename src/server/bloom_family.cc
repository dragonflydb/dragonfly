// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bloom_family.h"

#include "core/bloom.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
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
  auto& db_slice = op_args.db_cntx.ns->GetCurrentDbSlice();
  OpResult op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  if (!op_res)
    return op_res.status();
  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  PrimeValue& pv = op_res->it->second;
  pv.SetSBF(params.init_capacity, params.error, params.grow_factor);

  return OpStatus::OK;
}

// Returns true, if item was added, false if it was already "present".
OpResult<AddResult> OpAdd(const OpArgs& op_args, string_view key, CmdArgList items) {
  auto& db_slice = op_args.db_cntx.ns->GetCurrentDbSlice();

  OpResult op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  if (!op_res)
    return op_res.status();
  PrimeValue& pv = op_res->it->second;

  if (op_res->is_new) {
    pv.SetSBF(0, kDefaultFpProb, kDefaultGrowFactor);
  } else {
    if (op_res->it->second.ObjType() != OBJ_SBF)
      return OpStatus::WRONG_TYPE;
  }

  SBF* sbf = pv.GetSBF();
  AddResult result(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    result[i] = sbf->Add(ToSV(items[i]));
  }
  return result;
}

OpResult<ExistsResult> OpExists(const OpArgs& op_args, string_view key, CmdArgList items) {
  auto& db_slice = op_args.db_cntx.ns->GetCurrentDbSlice();
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

}  // namespace

void BloomFamily::Reserve(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();
  SbfParams params;

  tie(params.error, params.init_capacity) = parser.Next<double, uint32_t>();

  if (parser.Error())
    return cntx->SendError(kSyntaxErr);

  if (!params.ok())
    return cntx->SendError("error rate is out of range", kSyntaxErrType);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(params, t->GetOpArgs(shard), key);
  };

  OpStatus res = cntx->transaction->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return cntx->SendError("item exists");
  }
  return cntx->SendError(res);
}

void BloomFamily::Add(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  OpStatus status = res.status();
  if (res) {
    if (res->front())
      return cntx->SendLong(*res->front());
    else
      status = res->front().status();
  }

  return cntx->SendError(status);
}

void BloomFamily::Exists(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);
  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExists(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  return cntx->SendLong(res ? res->front() : 0);
}

void BloomFamily::MAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (!res) {
    return cntx->SendError(res.status());
  }
  const AddResult& add_res = *res;
  RedisReplyBuilder* rb = (RedisReplyBuilder*)cntx->reply_builder();
  rb->StartArray(add_res.size());
  for (const OpResult<bool>& val : add_res) {
    if (val) {
      cntx->SendLong(*val);
    } else {
      cntx->SendError(val.status());
    }
  }
}

void BloomFamily::MExists(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExists(t->GetOpArgs(shard), key, args);
  };

  OpResult res = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  RedisReplyBuilder* rb = (RedisReplyBuilder*)cntx->reply_builder();
  rb->StartArray(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    cntx->SendLong(res ? res->at(i) : 0);
  }
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&BloomFamily::x)

void BloomFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"BF.RESERVE", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::BLOOM}.HFUNC(
                   Reserve)
            << CI{"BF.ADD", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Add)
            << CI{"BF.MADD", CO::WRITE | CO::DENYOOM | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(MAdd)
            << CI{"BF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Exists)
            << CI{"BF.MEXISTS", CO::READONLY | CO::FAST, -3, 1, 1, acl::BLOOM}.HFUNC(MExists);
};

}  // namespace dfly
