// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

extern "C" {
#include "redis/zset.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"

namespace dfly {

using namespace std;

namespace {

using CI = CommandId;

}  // namespace

void ZSetFamily::ZCard(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void ZSetFamily::ZAdd(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void ZSetFamily::ZIncrBy(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void ZSetFamily::ZRange(CmdArgList args, ConnectionContext* cntx) {
}

void ZSetFamily::ZRangeByScore(CmdArgList args, ConnectionContext* cntx) {
}

void ZSetFamily::ZRem(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void ZSetFamily::ZScore(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendDouble(0);
}

#define HFUNC(x) SetHandler(&ZSetFamily::x)

void ZSetFamily::Register(CommandRegistry* registry) {
  *registry << CI{"ZCARD", CO::FAST | CO::READONLY, 2, 1, 1, 1}.HFUNC(ZCard)
            << CI{"ZADD", CO::FAST | CO::WRITE | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(ZAdd)
            << CI{"ZINCRBY", CO::FAST | CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(ZIncrBy)
            << CI{"ZREM", CO::FAST | CO::WRITE, -3, 1, 1, 1}.HFUNC(ZRem)
            << CI{"ZRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRange)
            << CI{"ZRANGEBYSCORE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRangeByScore)
            << CI{"ZSCORE", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ZScore);
}

}  // namespace dfly
