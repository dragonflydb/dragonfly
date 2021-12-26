// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"
#include "util/varz.h"

DEFINE_uint32(dbnum, 16, "Number of databases");

namespace dfly {
using namespace std;

namespace {

DEFINE_VARZ(VarzQps, ping_qps);

}  // namespace

void GenericFamily::Init(util::ProactorPool* pp) {
  ping_qps.Init(pp);
}

void GenericFamily::Shutdown() {
  ping_qps.Shutdown();
}

void GenericFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Del " << ArgS(args, 1);

  atomic_uint32_t result{0};
  auto cb = [&result](const Transaction* t, EngineShard* shard) {
    ArgSlice args = t->ShardArgsInShard(shard->shard_id());
    auto res = OpDel(OpArgs{shard, t->db_index()}, args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  DVLOG(2) << "Del ts " << transaction->txid();

  cntx->SendLong(result.load(memory_order_release));
}

void GenericFamily::Ping(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 2) {
    return cntx->SendError("wrong number of arguments for 'ping' command");
  }
  ping_qps.Inc();

  // We synchronously block here until the engine sends us the payload and notifies that
  // the I/O operation has been processed.
  if (args.size() == 1) {
    return cntx->SendSimpleRespString("PONG");
  } else {
    std::string_view arg = ArgS(args, 1);
    DVLOG(2) << "Ping " << arg;

    return cntx->SendBulkString(arg);
  }
}

void GenericFamily::Exists(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Exists " << ArgS(args, 1);

  atomic_uint32_t result{0};

  auto cb = [&result](Transaction* t, EngineShard* shard) {
    ArgSlice args = t->ShardArgsInShard(shard->shard_id());
    auto res = OpExists(OpArgs{shard, t->db_index()}, args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  return cntx->SendLong(result.load(memory_order_release));
}

void GenericFamily::Expire(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view sec = ArgS(args, 2);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return cntx->SendError(kInvalidIntErr);
  }

  int_arg = std::max(int_arg, -1L);
  ExpireParams params{.ts = int_arg};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(OpArgs{shard, t->db_index()}, key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  cntx->SendLong(status == OpStatus::OK);
}

void GenericFamily::ExpireAt(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view sec = ArgS(args, 2);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return cntx->SendError(kInvalidIntErr);
  }
  int_arg = std::max(int_arg, 0L);
  ExpireParams params{.ts = int_arg, .absolute = true};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(OpArgs{shard, t->db_index()}, key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));
  cntx->SendLong(status == OpStatus::OK);
}

void GenericFamily::Ttl(CmdArgList args, ConnectionContext* cntx) {
  TtlGeneric(args, cntx, TimeUnit::SEC);
}

void GenericFamily::Pttl(CmdArgList args, ConnectionContext* cntx) {
  TtlGeneric(args, cntx, TimeUnit::MSEC);
}

void GenericFamily::TtlGeneric(CmdArgList args, ConnectionContext* cntx, TimeUnit unit) {
  std::string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpTtl(t, shard, key); };
  OpResult<uint64_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    long ttl = (unit == TimeUnit::SEC) ? (result.value() + 500) / 1000 : result.value();
    cntx->SendLong(ttl);
  } else {
    switch (result.status()) {
      case OpStatus::KEY_NOTFOUND:
        cntx->SendLong(-1);
        break;
      default:
        cntx->SendLong(-2);
    }
  }
}

void GenericFamily::Select(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  int64_t index;
  if (!absl::SimpleAtoi(key, &index)) {
    return cntx->SendError(kInvalidDbIndErr);
  }
  if (index < 0 || index >= FLAGS_dbnum) {
    return cntx->SendError(kDbIndOutOfRangeErr);
  }
  cntx->conn_state.db_index = index;
  auto cb = [index](EngineShard* shard) {
    shard->db_slice().ActivateDb(index);
    return OpStatus::OK;
  };
  cntx->shard_set->RunBriefInParallel(std::move(cb));

  return cntx->SendOk();
}

void GenericFamily::Echo(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  return cntx->SendBulkString(key);
}

OpStatus GenericFamily::OpExpire(const OpArgs& op_args, std::string_view key,
                                 const ExpireParams& params) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(op_args.db_ind, key);
  if (it == MainIterator{})
    return OpStatus::KEY_NOTFOUND;

  int64_t abs_msec = (params.unit == TimeUnit::SEC) ? params.ts * 1000 : params.ts;

  if (!params.absolute) {
    abs_msec += db_slice.Now();
  }

  if (abs_msec <= int64_t(db_slice.Now())) {
    CHECK(db_slice.Del(op_args.db_ind, it));
  } else if (expire_it != ExpireIterator{}) {
    expire_it->second = abs_msec;
  } else {
    db_slice.Expire(op_args.db_ind, it, abs_msec);
  }

  return OpStatus::OK;
}

OpResult<uint64_t> GenericFamily::OpTtl(Transaction* t, EngineShard* shard, std::string_view key) {
  auto& db_slice = shard->db_slice();
  auto [it, expire] = db_slice.FindExt(t->db_index(), key);
  if (it == MainIterator{})
    return OpStatus::KEY_NOTFOUND;

  if (expire == ExpireIterator{})
    return OpStatus::SKIPPED;

  int64_t ttl_ms = expire->second - db_slice.Now();
  DCHECK_GT(ttl_ms, 0);  // Otherwise FindExt would return null.
  return ttl_ms;
}

OpResult<uint32_t> GenericFamily::OpDel(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Del: " << keys[0];
  auto& db_slice = op_args.shard->db_slice();

  uint32_t res = 0;

  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto fres = db_slice.FindExt(op_args.db_ind, keys[i]);
    if (fres.first == MainIterator{})
      continue;
    res += int(db_slice.Del(op_args.db_ind, fres.first));
  }

  return res;
}

OpResult<uint32_t> GenericFamily::OpExists(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Exists: " << keys[0];
  auto& db_slice = op_args.shard->db_slice();
  uint32_t res = 0;

  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto find_res = db_slice.FindExt(op_args.db_ind, keys[i]);
    res += (find_res.first != MainIterator{});
  }
  return res;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&GenericFamily::x)

void GenericFamily::Register(CommandRegistry* registry) {
  constexpr auto kSelectOpts = CO::LOADING | CO::FAST | CO::STALE;
  *registry << CI{"DEL", CO::WRITE, -2, 1, -1, 1}.HFUNC(Del)
            << CI{"PING", CO::STALE | CO::FAST, -1, 0, 0, 0}.HFUNC(Ping)
            << CI{"ECHO", CO::READONLY | CO::FAST, 2, 0, 0, 0}.HFUNC(Echo)
            << CI{"EXISTS", CO::READONLY | CO::FAST, -2, 1, -1, 1}.HFUNC(Exists)
            << CI{"EXPIRE", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Expire)
            << CI{"EXPIREAT", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(ExpireAt)
            << CI{"SELECT", kSelectOpts, 2, 0, 0, 0}.HFUNC(Select)
            << CI{"TTL", CO::READONLY | CO::FAST | CO::RANDOM, 2, 1, 1, 1}.HFUNC(Ttl)
            << CI{"PTTL", CO::READONLY | CO::FAST | CO::RANDOM, 2, 1, 1, 1}.HFUNC(Pttl);
}

}  // namespace dfly
