// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/redis_aux.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

namespace {

bool IsGoodForListpack(CmdArgList args, const uint8_t* lp) {
  size_t sum = 0;
  for (auto s : args) {
    if (s.size() > server.hash_max_listpack_value)
      return false;
    sum += s.size();
  }

  return lpSafeToAdd(const_cast<uint8_t*>(lp), sum);
}

}  // namespace

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
  string_view key = ArgS(args, 1);

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHSet(OpArgs{shard, t->db_index()}, key, args, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HSetNx(CmdArgList args, ConnectionContext* cntx) {
}

void HSetFamily::HStrLen(CmdArgList args, ConnectionContext* cntx) {
}

OpResult<uint32_t> HSetFamily::OpHSet(const OpArgs& op_args, std::string_view key,
                                      CmdArgList values, bool skip_if_exists) {
  DCHECK(!values.empty() && 0 == values.size() % 2);

  auto& db_slice = op_args.shard->db_slice();
  const auto [it, inserted] = db_slice.AddOrFind(op_args.db_ind, key);

  if (inserted) {
    robj* ro = createHashObject();
    it->second.ImportRObj(ro);
  } else {
    if (it->second.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;
  }

  robj* hset = it->second.AsRObj();
  uint8_t* lp = (uint8_t*)hset->ptr;

  if (hset->encoding == OBJ_ENCODING_LISTPACK && !IsGoodForListpack(values, lp)) {
    hashTypeConvert(hset, OBJ_ENCODING_HT);
  }
  unsigned created = 0;

  // TODO: we could avoid double copying by reimplementing hashTypeSet with better interface.
  for (size_t i = 0; i < values.size(); i += 2) {
    op_args.shard->tmp_str1 =
        sdscpylen(op_args.shard->tmp_str1, values[i].data(), values[i].size());
    op_args.shard->tmp_str2 =
        sdscpylen(op_args.shard->tmp_str2, values[i + 1].data(), values[i + 1].size());

    created += !hashTypeSet(hset, op_args.shard->tmp_str1, op_args.shard->tmp_str2, HASH_SET_COPY);
  }
  it->second.SyncRObj();

  return created;
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
