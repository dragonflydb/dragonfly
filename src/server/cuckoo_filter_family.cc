// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cuckoo.h"
#include "facade/cmd_arg_parser.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

constexpr uint64_t kDefaultCapacity = 1024;

OpResult<CuckooFilter*> FindOrCreate(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue& pv = op_res->it->second;
  if (op_res->is_new) {
    pv.SetCuckooFilter(CuckooFilterOptions{.capacity = kDefaultCapacity});
  }
  return pv.GetCuckooFilter();
}

OpResult<bool> OpAdd(const OpArgs& op_args, string_view key, string_view item) {
  OpResult<CuckooFilter*> cf = FindOrCreate(op_args, key);
  RETURN_ON_BAD_STATUS(cf);

  if (!(*cf)->Insert(CuckooFilter::Hash(item)))
    return OpStatus::CUCKOO_FILTER_FULL;
  return true;
}

OpResult<bool> OpAddNx(const OpArgs& op_args, string_view key, string_view item) {
  OpResult<CuckooFilter*> cf = FindOrCreate(op_args, key);
  RETURN_ON_BAD_STATUS(cf);

  uint64_t hash = CuckooFilter::Hash(item);
  if ((*cf)->Exists(hash))
    return false;

  if (!(*cf)->Insert(hash))
    return OpStatus::CUCKOO_FILTER_FULL;
  return true;
}

OpStatus OpReserve(const OpArgs& op_args, string_view key, const CuckooFilterOptions& options) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  op_res->it->second.SetCuckooFilter(options);
  return OpStatus::OK;
}

void CmdReserve(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(cmd_cntx->tail_args());
  string_view key = parser.Next();
  uint64_t capacity = parser.Next<uint64_t>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (capacity == 0) {
    return rb->SendError("CF: capacity must be greater than 0");
  }

  uint8_t bucket_size = CuckooFilterOptions::kDefaultSlotsPerBucket;
  uint16_t max_iterations = CuckooFilterOptions::kDefaultMaxIterations;
  uint16_t expansion = CuckooFilterOptions::kDefaultExpansion;

  parser.Apply(Tag("BUCKETSIZE", &bucket_size), Tag("MAXITERATIONS", &max_iterations),
               Tag("EXPANSION", &expansion));

  if (!parser.Finalize()) {
    return rb->SendError(parser.TakeError().MakeReply());
  }

  // The parser already rejects values that overflow the field width above (e.g. bucketsize
  // 256) with the standard "value is not an integer or out of range" error. Only the
  // business-rule bounds below (zero, and CF's tighter expansion cap) need manual checks.
  if (bucket_size == 0) {
    return rb->SendError("CF: bucket size must be between 1 and 255");
  }
  if (max_iterations == 0) {
    return rb->SendError("CF: max iterations must be between 1 and 65535");
  }
  if (expansion > 32767) {
    return rb->SendError("CF: expansion must be between 0 and 32767");
  }

  CuckooFilterOptions options{capacity, bucket_size, max_iterations, expansion};

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, options);
  };

  OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("item exists");
  }
  if (res == OpStatus::OK) {
    return rb->SendOk();
  }
  return rb->SendError(res);
}

void CmdAdd(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view item = ArgS(args, 1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, item);
  };

  OpResult<bool> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(*res);
}

void CmdAddNx(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view item = ArgS(args, 1);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAddNx(t->GetOpArgs(shard), key, item);
  };

  OpResult<bool> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(*res);
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterCuckooFilterFamily(CommandRegistry* registry) {
  registry->StartFamily(acl::CUCKOO_FILTER);

  *registry << CI{"CF.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1}.HFUNC(Reserve)
            << CI{"CF.ADD", CO::JOURNALED | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(Add)
            << CI{"CF.ADDNX", CO::JOURNALED | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(AddNx);
}

}  // namespace dfly
