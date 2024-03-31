// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bloom_family.h"

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

struct SbfParams {
  uint32_t init_capacity;
  double error;
  double grow_factor = 2.0;

  bool ok() const {
    return error > 0 and error < 0.5;
  }
};

OpStatus OpReserve(const SbfParams& params, const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  if (!op_res)
    return op_res.status();
  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  PrimeValue& pv = op_res->it->second;
  pv.SetSBF(params.init_capacity, params.error, params.grow_factor);

  return OpStatus::OK;
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
  cntx->SendError(kSyntaxErr);
}

void BloomFamily::Exists(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendError(kSyntaxErr);
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&BloomFamily::x)

void BloomFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"BF.RESERVE", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::BLOOM}.HFUNC(
                   Reserve)
            << CI{"BF.ADD", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Add)
            << CI{"BF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1, acl::BLOOM}.HFUNC(Exists);
};

}  // namespace dfly
