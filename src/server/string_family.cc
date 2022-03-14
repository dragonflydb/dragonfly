// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

extern "C" {
#include "redis/object.h"
}

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
using facade::Protocol;
using facade::SinkReplyBuilder;

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

  VLOG(2) << "Set " << key << "(" << db_slice_->shard_id() << ") ";

  auto [it, expire_it] = db_slice_->FindExt(params.db_index, key);
  uint64_t at_ms = params.expire_after_ms ? params.expire_after_ms + db_slice_->Now() : 0;

  if (IsValid(it)) {  // existing
    if (params.how == SET_IF_NOTEXIST)
      return OpStatus::SKIPPED;

    PrimeValue& prime_value = it->second;
    if (params.prev_val) {
      if (prime_value.ObjType() != OBJ_STRING)
        return OpStatus::WRONG_TYPE;

      string val;
      prime_value.GetString(&val);
      params.prev_val->emplace(move(val));
    }

    if (IsValid(expire_it) && at_ms) {
      expire_it->second.Set(at_ms - db_slice_->expire_base());
    } else {
      bool changed = db_slice_->Expire(params.db_index, it, at_ms);
      if (changed && at_ms == 0)  // erased.
        return OpStatus::OK;
    }
    db_slice_->PreUpdate(params.db_index, it);

    // Check whether we need to update flags table.
    bool req_flag_update = (params.memcache_flags != 0) != prime_value.HasFlag();
    if (req_flag_update) {
      prime_value.SetFlag(params.memcache_flags != 0);
      db_slice_->SetMCFlag(params.db_index, it->first.AsRef(), params.memcache_flags);
    }

    prime_value.SetString(value);   // erases all masks.
    prime_value.SetExpire(at_ms != 0);  // set expire mask.

    db_slice_->PostUpdate(params.db_index, it);

    return OpStatus::OK;
  }

  // New entry
  if (params.how == SET_IF_EXISTS)
    return OpStatus::SKIPPED;

  PrimeValue tvalue{value};
  tvalue.SetFlag(params.memcache_flags != 0);
  it = db_slice_->AddNew(params.db_index, key, std::move(tvalue), at_ms);

  if (params.memcache_flags)
    db_slice_->SetMCFlag(params.db_index, it->first.AsRef(), params.memcache_flags);

  return OpStatus::OK;
}

void StringFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  set_qps.Inc();

  string_view key = ArgS(args, 1);
  string_view value = ArgS(args, 2);

  SetCmd::SetParams sparams{cntx->db_index()};
  sparams.memcache_flags = cntx->conn_state.memcache_flag;

  int64_t int_arg;
  SinkReplyBuilder* builder = cntx->reply_builder();

  for (size_t i = 3; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);

    if (cur_arg == "EX" || cur_arg == "PX") {
      bool is_ms = (cur_arg == "PX");
      ++i;
      if (i == args.size()) {
        builder->SendError(kSyntaxErr);
      }

      std::string_view ex = ArgS(args, i);
      if (!absl::SimpleAtoi(ex, &int_arg)) {
        return builder->SendError(kInvalidIntErr);
      }

      if (int_arg <= 0 || (!is_ms && int_arg >= int64_t(kMaxExpireDeadlineSec))) {
        return builder->SendError(facade::kExpiryOutOfRange);
      }

      if (!is_ms) {
        int_arg *= 1000;
      }
      if (int_arg >= int64_t(kMaxExpireDeadlineSec * 1000)) {
        return builder->SendError(facade::kExpiryOutOfRange);
      }
      sparams.expire_after_ms = int_arg;
    } else if (cur_arg == "NX") {
      sparams.how = SetCmd::SET_IF_NOTEXIST;
    } else if (cur_arg == "XX") {
      sparams.how = SetCmd::SET_IF_EXISTS;
    } else if (cur_arg == "KEEPTTL") {
      sparams.keep_expire = true;  // TODO
    } else {
      return builder->SendError(kSyntaxErr);
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
    return builder->SendStored();
  }

  CHECK_EQ(result, OpStatus::SKIPPED);  // in case of NX option

  return builder->SendSetSkipped();
}

void StringFamily::SetEx(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view ex = ArgS(args, 2);
  string_view value = ArgS(args, 3);
  uint32_t secs;

  if (!absl::SimpleAtoi(ex, &secs)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  SetCmd::SetParams sparams{cntx->db_index()};
  sparams.expire_after_ms = uint64_t(secs) * 1000;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd sg(&shard->db_slice());
    auto status = sg.Set(sparams, key, value).status();
    return status;
  };

  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));

  return (*cntx)->SendError(result.status());
}

void StringFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  get_qps.Inc();

  std::string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<string> {
    OpResult<MainIterator> it_res = shard->db_slice().Find(t->db_index(), key, OBJ_STRING);
    if (!it_res.ok())
      return it_res.status();

    string val;
    it_res.value()->second.GetString(&val);
    return val;
  };

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    DVLOG(1) << "GET " << trans->DebugId() << ": " << key << " " << result.value();
    (*cntx)->SendBulkString(*result);
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      default:
        DVLOG(1) << "GET " << key << " nil";
        (*cntx)->SendNull();
    }
  }
}

void StringFamily::GetSet(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view value = ArgS(args, 2);
  std::optional<string> prev_val;

  SetCmd::SetParams sparams{cntx->db_index()};
  sparams.prev_val = &prev_val;

  ShardId sid = Shard(key, cntx->shard_set->size());
  OpResult<void> result = cntx->shard_set->Await(sid, [&] {
    EngineShard* es = EngineShard::tlocal();
    SetCmd cmd(&es->db_slice());

    return cmd.Set(sparams, key, value);
  });

  if (!result) {
    (*cntx)->SendError(result.status());
    return;
  }

  if (prev_val) {
    (*cntx)->SendBulkString(*prev_val);
    return;
  }
  return (*cntx)->SendNull();
}

void StringFamily::Incr(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  return IncrByGeneric(key, 1, cntx);
}

void StringFamily::IncrBy(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_EQ(3u, args.size());

  std::string_view key = ArgS(args, 1);
  std::string_view sval = ArgS(args, 2);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  return IncrByGeneric(key, val, cntx);
}

void StringFamily::Decr(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  return IncrByGeneric(key, -1, cntx);
}

void StringFamily::DecrBy(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view sval = ArgS(args, 2);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  return IncrByGeneric(key, -val, cntx);
}

void StringFamily::Append(CmdArgList args, ConnectionContext* cntx) {
  ExtendGeneric(std::move(args), false, cntx);
}

void StringFamily::Prepend(CmdArgList args, ConnectionContext* cntx) {
  ExtendGeneric(std::move(args), true, cntx);
}

void StringFamily::IncrByGeneric(std::string_view key, int64_t val, ConnectionContext* cntx) {
  bool skip_on_missing = cntx->protocol() == Protocol::MEMCACHE;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpResult<int64_t> res = OpIncrBy(OpArgs{shard, t->db_index()}, key, val, skip_on_missing);
    return res;
  };

  OpResult<int64_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* builder = cntx->reply_builder();

  DVLOG(2) << "IncrByGeneric " << key << "/" << result.value();
  switch (result.status()) {
    case OpStatus::OK:
      return builder->SendLong(result.value());
    case OpStatus::INVALID_VALUE:
      return builder->SendError(kInvalidIntErr);
    case OpStatus::OUT_OF_RANGE:
      return builder->SendError("increment or decrement would overflow");
    case OpStatus::WRONG_TYPE:
      return builder->SendError(kWrongTypeErr);
    case OpStatus::KEY_NOTFOUND:  // Relevant only for MC
      return reinterpret_cast<facade::MCReplyBuilder*>(builder)->SendNotFound();
    default:;
  }
  __builtin_unreachable();
}

void StringFamily::ExtendGeneric(CmdArgList args, bool prepend, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view sval = ArgS(args, 2);

  if (cntx->protocol() == Protocol::REDIS) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return ExtendOrSet(OpArgs{shard, t->db_index()}, key, sval, prepend);
    };

    OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
    if (!result)
      return (*cntx)->SendError(result.status());
    else
      return (*cntx)->SendLong(result.value());
  }
  DCHECK(cntx->protocol() == Protocol::MEMCACHE);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return ExtendOrSkip(OpArgs{shard, t->db_index()}, key, sval, prepend);
  };

  OpResult<bool> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  SinkReplyBuilder* builder = cntx->reply_builder();
  if (result.value_or(false)) {
    return builder->SendStored();
  }

  builder->SendSetSkipped();
}

void StringFamily::MGet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GT(args.size(), 1U);

  Transaction* transaction = cntx->transaction;
  unsigned shard_count = transaction->shard_set()->size();
  std::vector<MGetResponse> mget_resp(shard_count);

  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  bool fetch_mcflag = cntx->protocol() == Protocol::MEMCACHE;
  bool fetch_mcver =
      fetch_mcflag && (dfly_cntx->conn_state.memcache_flag & ConnectionState::FETCH_CAS_VER);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    mget_resp[sid] = OpMGet(fetch_mcflag, fetch_mcver, t, shard);
    return OpStatus::OK;
  };

  // MGet requires locking as well. For example, if coordinator A applied W(x) and then W(y)
  // it necessarily means that whoever observed y, must observe x.
  // Without locking, mget x y could read stale x but latest y.
  OpStatus result = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, result);

  // reorder the responses back according to the order of their corresponding keys.
  vector<SinkReplyBuilder::OptResp> res(args.size() - 1);

  for (ShardId sid = 0; sid < shard_count; ++sid) {
    if (!transaction->IsActive(sid))
      continue;

    MGetResponse& results = mget_resp[sid];
    ArgSlice slice = transaction->ShardArgsInShard(sid);

    DCHECK(!slice.empty());
    DCHECK_EQ(slice.size(), results.size());

    for (size_t j = 0; j < slice.size(); ++j) {
      if (!results[j])
        continue;

      uint32_t indx = transaction->ReverseArgIndex(sid, j);

      auto& dest = res[indx].emplace();
      auto& src = *results[j];
      dest.key = ArgS(args, indx + 1);
      dest.value = std::move(src.value);
      dest.mc_flag = src.mc_flag;
      dest.mc_ver = src.mc_ver;
    }
  }

  return cntx->reply_builder()->SendMGetResponse(res.data(), res.size());
}

void StringFamily::MSet(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;

  if (VLOG_IS_ON(2)) {
    string str;
    for (size_t i = 1; i < args.size(); ++i) {
      absl::StrAppend(&str, " ", ArgS(args, i));
    }
    LOG(INFO) << "MSET/" << transaction->unique_shard_cnt() << str;
  }

  OpStatus status = transaction->ScheduleSingleHop(&OpMSet);
  CHECK_EQ(OpStatus::OK, status);

  DVLOG(2) << "MSet run  " << transaction->DebugId();

  return (*cntx)->SendOk();
}

auto StringFamily::OpMGet(bool fetch_mcflag, bool fetch_mcver, const Transaction* t,
                          EngineShard* shard) -> MGetResponse {
  auto args = t->ShardArgsInShard(shard->shard_id());
  DCHECK(!args.empty());

  MGetResponse response(args.size());

  auto& db_slice = shard->db_slice();
  for (size_t i = 0; i < args.size(); ++i) {
    OpResult<MainIterator> it_res = db_slice.Find(t->db_index(), args[i], OBJ_STRING);
    if (!it_res)
      continue;

    const MainIterator& it = *it_res;
    auto& dest = response[i].emplace();

    it->second.GetString(&dest.value);
    if (fetch_mcflag) {
      dest.mc_flag = db_slice.GetMCFlag(t->db_index(), it->first);
      if (fetch_mcver) {
        dest.mc_ver = it.GetVersion();
      }
    }
  }

  return response;
}

OpStatus StringFamily::OpMSet(const Transaction* t, EngineShard* es) {
  ArgSlice largs = t->ShardArgsInShard(es->shard_id());

  DCHECK(!largs.empty() && largs.size() % 2 == 0);

  SetCmd::SetParams params{t->db_index()};
  SetCmd sg(&es->db_slice());
  for (size_t i = 0; i < largs.size(); i += 2) {
    DVLOG(1) << "MSet " << largs[i] << ":" << largs[i + 1];
    auto res = sg.Set(params, largs[i], largs[i + 1]);
    CHECK(res.ok()) << res << " " << largs[i];  // TODO - handle OOM etc.
  }

  return OpStatus::OK;
}

OpResult<int64_t> StringFamily::OpIncrBy(const OpArgs& op_args, std::string_view key, int64_t incr,
                                         bool skip_on_missing) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(op_args.db_ind, key);

  if (!IsValid(it)) {
    if (skip_on_missing)
      return OpStatus::KEY_NOTFOUND;

    CompactObj cobj;
    cobj.SetInt(incr);

    db_slice.AddNew(op_args.db_ind, key, std::move(cobj), 0);
    return incr;
  }

  if (it->second.ObjType() != OBJ_STRING) {
    return OpStatus::WRONG_TYPE;
  }

  auto opt_prev = it->second.TryGetInt();
  if (!opt_prev) {
    return OpStatus::INVALID_VALUE;
  }

  long long prev = *opt_prev;
  if ((incr < 0 && prev < 0 && incr < (LLONG_MIN - prev)) ||
      (incr > 0 && prev > 0 && incr > (LLONG_MAX - prev))) {
    return OpStatus::OUT_OF_RANGE;
  }

  int64_t new_val = prev + incr;
  db_slice.PreUpdate(op_args.db_ind, it);
  it->second.SetInt(new_val);
  db_slice.PostUpdate(op_args.db_ind, it);
  return new_val;
}

OpResult<uint32_t> StringFamily::ExtendOrSet(const OpArgs& op_args, std::string_view key,
                                             std::string_view val, bool prepend) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, inserted] = db_slice.AddOrFind(op_args.db_ind, key);
  if (inserted) {
    it->second.SetString(val);
    return val.size();
  }

  if (it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  string tmp, new_val;
  string_view slice = it->second.GetSlice(&tmp);
  if (prepend)
    new_val = absl::StrCat(val, slice);
  else
    new_val = absl::StrCat(slice, val);

  db_slice.PreUpdate(op_args.db_ind, it);
  it->second.SetString(new_val);
  db_slice.PostUpdate(op_args.db_ind, it);

  return new_val.size();
}

OpResult<bool> StringFamily::ExtendOrSkip(const OpArgs& op_args, std::string_view key,
                                          std::string_view val, bool prepend) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<MainIterator> it_res = db_slice.Find(op_args.db_ind, key, OBJ_STRING);
  if (!it_res) {
    return false;
  }

  CompactObj& cobj = (*it_res)->second;

  string tmp, new_val;
  string_view slice = cobj.GetSlice(&tmp);
  if (prepend)
    new_val = absl::StrCat(val, slice);
  else
    new_val = absl::StrCat(slice, val);

  db_slice.PreUpdate(op_args.db_ind, *it_res);
  cobj.SetString(new_val);
  db_slice.PostUpdate(op_args.db_ind, *it_res);

  return new_val.size();
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
            << CI{"SETEX", CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(SetEx)
            << CI{"APPEND", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Append)
            << CI{"PREPEND", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Prepend)
            << CI{"INCR", CO::WRITE | CO::DENYOOM | CO::FAST, 2, 1, 1, 1}.HFUNC(Incr)
            << CI{"DECR", CO::WRITE | CO::DENYOOM | CO::FAST, 2, 1, 1, 1}.HFUNC(Decr)
            << CI{"INCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(IncrBy)
            << CI{"DECRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(DecrBy)
            << CI{"GET", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Get)
            << CI{"GETSET", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(GetSet)
            << CI{"MGET", CO::READONLY | CO::FAST, -2, 1, -1, 1}.HFUNC(MGet)
            << CI{"MSET", CO::WRITE | CO::DENYOOM, -3, 1, -1, 2}.HFUNC(MSet);
}

}  // namespace dfly
