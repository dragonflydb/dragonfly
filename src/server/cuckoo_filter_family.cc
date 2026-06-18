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

OpStatus OpReserve(const OpArgs& op_args, string_view key, uint64_t capacity, uint8_t bucket_size,
                   uint16_t max_iterations, uint16_t expansion) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  op_res->it->second.SetCuckooFilter(capacity, bucket_size, max_iterations, expansion);
  return OpStatus::OK;
}

void CmdReserve(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();
  uint64_t capacity = parser.Next<uint64_t>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (capacity == 0) {
    return rb->SendError("CF: capacity must be greater than 0");
  }

  uint32_t bucket_size = CuckooFilter::kDefaultSlotsPerBucket;
  uint32_t max_iterations = CuckooFilter::kDefaultMaxIterations;
  uint32_t expansion = CuckooFilter::kDefaultExpansion;

  parser.Apply(Tag("BUCKETSIZE", &bucket_size), Tag("MAXITERATIONS", &max_iterations),
               Tag("EXPANSION", &expansion));

  if (!parser.Finalize()) {
    return rb->SendError(parser.TakeError().MakeReply());
  }

  if (bucket_size == 0 || bucket_size > 255) {
    return rb->SendError("CF: bucket size must be between 1 and 255");
  }
  if (max_iterations == 0 || max_iterations > 65535) {
    return rb->SendError("CF: max iterations must be between 1 and 65535");
  }
  if (expansion > 32767) {
    return rb->SendError("CF: expansion must be between 0 and 32767");
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, capacity, static_cast<uint8_t>(bucket_size),
                     static_cast<uint16_t>(max_iterations), static_cast<uint16_t>(expansion));
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

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterCuckooFilterFamily(CommandRegistry* registry) {
  registry->StartFamily(acl::CUCKOO_FILTER);

  *registry << CI{"CF.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1}.HFUNC(Reserve);
}

}  // namespace dfly
