// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

#include "server/command_registry.h"

namespace dfly {

void HSetFamily::HDel(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HLen(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HExists(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HGet(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HIncrBy(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HSet(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HSetNx(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HStrLen(CmdArgList args, ConnectionContext* cntx) {
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&HSetFamily::x)

void HSetFamily::Register(CommandRegistry* registry) {
  *registry << CI{"HDEL", CO::FAST | CO::WRITE, -3, 1, 1, 1}.HFUNC(HDel)
            << CI{"HLEN", CO::FAST | CO::READONLY, 2, 1, 1, 1}.HFUNC(HLen)
            << CI{"HEXISTS", CO::FAST | CO::READONLY, 3, 1, 1, 1}.HFUNC(HExists)
            << CI{"HGET", CO::FAST | CO::READONLY, 3, 1, 1, 1}.HFUNC(HGet)
            << CI{"HINCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HIncrBy)
            << CI{"HSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(HSet)
            << CI{"HSETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HSetNx)
            << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(HStrLen);
}

}  // namespace dfly
