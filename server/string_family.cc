// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

#include <absl/container/inlined_vector.h>

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"
#include "util/varz.h"

namespace dfly {

namespace {

using namespace std;

using CI = CommandId;
DEFINE_VARZ(VarzQps, set_qps);
DEFINE_VARZ(VarzQps, get_qps);

}  // namespace

SetCmd::SetCmd(DbSlice* db_slice) : db_slice_(db_slice) {
}

SetCmd::~SetCmd() {
}

OpResult<void> SetCmd::Set(const SetParams& params, std::string_view key, std::string_view value) {
  DCHECK_LT(params.db_index, db_slice_->db_array_size());
  DCHECK(db_slice_->IsDbValid(params.db_index));

  VLOG(2) << "Set (" << db_slice_->shard_id() << ") ";

  auto [it, expire_it] = db_slice_->FindExt(params.db_index, key);
  uint64_t at_ms = params.expire_after_ms ? params.expire_after_ms + db_slice_->Now() : 0;

  if (it != MainIterator{}) {  // existing
    if (params.how == SET_IF_NOTEXIST)
      return OpStatus::SKIPPED;

    if (params.prev_val) {
      params.prev_val->emplace(it->second.str);
    }

    return SetExisting(params.db_index, value, at_ms, it, expire_it);
  }

  if (params.how == SET_IF_EXISTS)
    return OpStatus::SKIPPED;

  db_slice_->AddNew(params.db_index, key, value, at_ms);

  return OpStatus::OK;
}

OpResult<void> SetCmd::SetExisting(DbIndex db_ind, std::string_view value, uint64_t expire_at_ms,
                                   MainIterator dest, ExpireIterator exp_it) {
  if (exp_it != ExpireIterator{} && expire_at_ms) {
    exp_it->second = expire_at_ms;
  } else {
    db_slice_->Expire(db_ind, dest, expire_at_ms);
  }

  dest->second = value;

  return OpStatus::OK;
}

void StringFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  set_qps.Inc();

  std::string_view key = ArgS(args, 1);
  std::string_view value = ArgS(args, 2);
  VLOG(2) << "Set " << key << " " << value;

  SetCmd::SetParams sparams{0};  // TODO: db_index.
  int64_t int_arg;

  for (size_t i = 3; i < args.size(); ++i) {
    ToUpper(&args[i]);

    std::string_view cur_arg = ArgS(args, i);

    if (cur_arg == "EX" || cur_arg == "PX") {
      bool is_ms = (cur_arg == "PX");
      ++i;
      if (i == args.size()) {
        cntx->SendError(kSyntaxErr);
      }
      std::string_view ex = ArgS(args, i);
      if (!absl::SimpleAtoi(ex, &int_arg)) {
        return cntx->SendError(kInvalidIntErr);
      }
      if (int_arg <= 0 || (!is_ms && int_arg >= 500000000)) {
        return cntx->SendError("invalid expire time in set");
      }
      if (!is_ms) {
        int_arg *= 1000;
      }
      sparams.expire_after_ms = int_arg;
    } else if (cur_arg == "NX") {
      sparams.how = SetCmd::SET_IF_NOTEXIST;
    } else if (cur_arg == "XX") {
      sparams.how = SetCmd::SET_IF_EXISTS;
    } else if (cur_arg == "KEEPTTL") {
      sparams.keep_expire = true;
    } else {
      return cntx->SendError(kSyntaxErr);
    }
  }

  DCHECK(cntx->transaction);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd sg(&shard->db_slice());
    auto status = sg.Set(sparams, key, value).status();
    return status;
  };
  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (result == OpStatus::OK) {
    return cntx->SendStored();
  }

  CHECK_EQ(result, OpStatus::SKIPPED);  // in case of NX option
  return cntx->SendNull();
}

void StringFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  get_qps.Inc();

  std::string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<string> {
    OpResult<MainIterator> it_res = shard->db_slice().Find(0, key);
    if (!it_res.ok())
      return it_res.status();

    string val = it_res.value()->second.str;

    return val;
  };

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    DVLOG(1) << "GET " << trans->DebugId() << ": " << key << " " << result.value();
    cntx->SendGetReply(key, 0, result.value());
  } else {
    DVLOG(1) << "GET " << key << " nil";
    cntx->SendGetNotFound();
  }
}

void StringFamily::GetSet(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view value = ArgS(args, 2);
  std::optional<string> prev_val;

  SetCmd::SetParams sparams{0};
  sparams.prev_val = &prev_val;

  ShardId sid = Shard(key, cntx->shard_set->size());
  OpResult<void> result = cntx->shard_set->Await(sid, [&] {
  EngineShard* es = EngineShard::tlocal();
    SetCmd cmd(&es->db_slice());

    return cmd.Set(sparams, key, value);
  });

  if (!result) {
    cntx->SendError(result.status());
    return;
  }

  if (prev_val) {
    cntx->SendGetReply(key, 0, *prev_val);
    return;
  }
  return cntx->SendNull();
}

void StringFamily::Init(util::ProactorPool* pp) {
  set_qps.Init(pp);
  get_qps.Init(pp);
}

void StringFamily::Shutdown() {
  set_qps.Shutdown();
  get_qps.Shutdown();
}

#define HFUNC(x) SetHandler(&StringFamily::x)

void StringFamily::Register(CommandRegistry* registry) {
  *registry << CI{"SET", CO::WRITE | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(Set)
            << CI{"GET", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Get)
            << CI{"GETSET", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(GetSet);
}

}  // namespace dfly
