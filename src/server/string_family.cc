// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

extern "C" {
#include "redis/object.h"
}

#include <absl/container/inlined_vector.h>
#include <double-conversion/string-to-double.h>

#include <chrono>

#include "base/logging.h"
#include "redis/util.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/io_mgr.h"
#include "server/journal/journal.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "util/varz.h"

namespace dfly {

namespace {

using namespace std;
using namespace facade;
using namespace double_conversion;

using CI = CommandId;
DEFINE_VARZ(VarzQps, set_qps);
DEFINE_VARZ(VarzQps, get_qps);

constexpr uint32_t kMaxStrLen = 1 << 28;
constexpr uint32_t kMinTieredLen = TieredStorage::kMinBlobLen;

string GetString(EngineShard* shard, const PrimeValue& pv) {
  string res;
  if (pv.IsExternal()) {
    auto* tiered = shard->tiered_storage();
    auto [offset, size] = pv.GetExternalPtr();
    res.resize(size);

    error_code ec = tiered->Read(offset, size, res.data());
    CHECK(!ec) << "TBD: " << ec;
  } else {
    pv.GetString(&res);
  }

  return res;
}

string_view GetSlice(EngineShard* shard, const PrimeValue& pv, string* tmp) {
  if (pv.IsExternal()) {
    *tmp = GetString(shard, pv);
    return *tmp;
  }
  return pv.GetSlice(tmp);
}

inline void RecordJournal(const OpArgs& op_args, string_view key, const PrimeKey& pvalue) {
  if (op_args.shard->journal()) {
    journal::Entry entry{op_args.db_cntx.db_index, op_args.txid, key, pvalue};
    op_args.shard->journal()->RecordEntry(entry);
  }
}

OpResult<uint32_t> OpSetRange(const OpArgs& op_args, string_view key, size_t start,
                              string_view value) {
  auto& db_slice = op_args.shard->db_slice();
  size_t range_len = start + value.size();

  if (range_len == 0) {
    auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_STRING);
    if (it_res) {
      return it_res.value()->second.Size();
    } else {
      return it_res.status();
    }
  }

  auto [it, added] = db_slice.AddOrFind(op_args.db_cntx, key);

  string s;

  if (added) {
    s.resize(range_len);
  } else {
    if (it->second.ObjType() != OBJ_STRING)
      return OpStatus::WRONG_TYPE;

    s = GetString(op_args.shard, it->second);
    if (s.size() < range_len)
      s.resize(range_len);

    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  }

  memcpy(s.data() + start, value.data(), value.size());
  it->second.SetString(s);
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, !added);
  RecordJournal(op_args, key, it->second);

  return it->second.Size();
}

OpResult<string> OpGetRange(const OpArgs& op_args, string_view key, int32_t start, int32_t end) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> it_res = db_slice.Find(op_args.db_cntx, key, OBJ_STRING);
  if (!it_res.ok())
    return it_res.status();

  const CompactObj& co = it_res.value()->second;
  size_t strlen = co.Size();

  if (start < 0)
    start = strlen + start;
  if (end < 0)
    end = strlen + end;

  if (start < 0)
    start = 0;
  if (end < 0)
    end = 0;

  if (strlen == 0 || start > end || size_t(start) >= strlen) {
    return OpStatus::OK;
  }

  if (size_t(end) >= strlen)
    end = strlen - 1;

  string tmp;
  string_view slice = GetSlice(op_args.shard, co, &tmp);

  return string(slice.substr(start, end - start + 1));
};

size_t ExtendExisting(const OpArgs& op_args, PrimeIterator it, string_view key, string_view val,
                      bool prepend) {
  string tmp, new_val;
  auto* shard = op_args.shard;
  string_view slice = GetSlice(shard, it->second, &tmp);
  if (prepend)
    new_val = absl::StrCat(val, slice);
  else
    new_val = absl::StrCat(slice, val);

  auto& db_slice = shard->db_slice();
  db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  it->second.SetString(new_val);
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, true);
  RecordJournal(op_args, key, it->second);

  return new_val.size();
}

// Returns the length of the extended string. if prepend is false - appends the val.
OpResult<uint32_t> ExtendOrSet(const OpArgs& op_args, string_view key, string_view val,
                               bool prepend) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  auto [it, inserted] = db_slice.AddOrFind(op_args.db_cntx, key);
  if (inserted) {
    it->second.SetString(val);
    db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, false);
    RecordJournal(op_args, key, it->second);

    return val.size();
  }

  if (it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  return ExtendExisting(op_args, it, key, val, prepend);
}

OpResult<bool> ExtendOrSkip(const OpArgs& op_args, string_view key, string_view val, bool prepend) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> it_res = db_slice.Find(op_args.db_cntx, key, OBJ_STRING);
  if (!it_res) {
    return false;
  }

  return ExtendExisting(op_args, *it_res, key, val, prepend);
}

OpResult<string> OpGet(const OpArgs& op_args, string_view key, bool del_hit = false,
                       const DbSlice::ExpireParams& exp_params = {}) {
  /*Get primeIterator and ExpireIterator at the same time*/
  auto [it, it_expire] = op_args.shard->db_slice().FindExt(op_args.db_cntx, key);

  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  const PrimeValue& pv = it->second;

  if (del_hit) {
    string key_bearer = GetString(op_args.shard, pv);

    DVLOG(1) << "Del: " << key;
    auto& db_slice = op_args.shard->db_slice();

    CHECK(db_slice.Del(op_args.db_cntx.db_index, it));

    return key_bearer;
  }

  /*Get value before expire*/
  string ret_val = GetString(op_args.shard, pv);

  if (exp_params.IsDefined()) {
    DVLOG(1) << "Expire: " << key;
    auto& db_slice = op_args.shard->db_slice();
    OpStatus status = db_slice.UpdateExpire(op_args.db_cntx, it, it_expire, exp_params);
    if (status != OpStatus::OK)
      return status;
  }

  return ret_val;
}

OpResult<double> OpIncrFloat(const OpArgs& op_args, string_view key, double val) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, inserted] = db_slice.AddOrFind(op_args.db_cntx, key);

  char buf[128];

  if (inserted) {
    char* str = RedisReplyBuilder::FormatDouble(val, buf, sizeof(buf));
    it->second.SetString(str);
    db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, false);
    RecordJournal(op_args, key, it->second);

    return val;
  }

  if (it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  if (it->second.Size() == 0)
    return OpStatus::INVALID_FLOAT;

  string tmp;
  string_view slice = GetSlice(op_args.shard, it->second, &tmp);

  StringToDoubleConverter stod(StringToDoubleConverter::NO_FLAGS, 0, 0, NULL, NULL);
  int processed_digits = 0;
  double base = stod.StringToDouble(slice.data(), slice.size(), &processed_digits);
  if (unsigned(processed_digits) != slice.size()) {
    return OpStatus::INVALID_FLOAT;
  }

  base += val;

  if (isnan(base) || isinf(base)) {
    return OpStatus::INVALID_FLOAT;
  }

  char* str = RedisReplyBuilder::FormatDouble(base, buf, sizeof(buf));

  db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  it->second.SetString(str);
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, true);
  RecordJournal(op_args, key, it->second);

  return base;
}

// if skip_on_missing - returns KEY_NOTFOUND.
OpResult<int64_t> OpIncrBy(const OpArgs& op_args, string_view key, int64_t incr,
                           bool skip_on_missing) {
  auto& db_slice = op_args.shard->db_slice();

  // we avoid using AddOrFind because of skip_on_missing option for memcache.
  auto [it, expire_it] = db_slice.FindExt(op_args.db_cntx, key);

  if (!IsValid(it)) {
    if (skip_on_missing)
      return OpStatus::KEY_NOTFOUND;

    CompactObj cobj;
    cobj.SetInt(incr);

    // AddNew calls PostUpdate inside.
    try {
      it = db_slice.AddNew(op_args.db_cntx, key, std::move(cobj), 0);
    } catch (bad_alloc&) {
      return OpStatus::OUT_OF_MEMORY;
    }

    RecordJournal(op_args, key, it->second);

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
  DCHECK(!it->second.IsExternal());
  db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  it->second.SetInt(new_val);
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);
  RecordJournal(op_args, key, it->second);

  return new_val;
}

int64_t CalculateAbsTime(int64_t unix_time, bool as_milli) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::seconds;
  using std::chrono::system_clock;

  if (as_milli) {
    return unix_time - duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  } else {
    return unix_time - duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  }
}

// Returns true if keys were set, false otherwise.
OpStatus OpMSet(const OpArgs& op_args, ArgSlice args) {
  DCHECK(!args.empty() && args.size() % 2 == 0);

  SetCmd::SetParams params;
  SetCmd sg(op_args);

  for (size_t i = 0; i < args.size(); i += 2) {
    DVLOG(1) << "MSet " << args[i] << ":" << args[i + 1];
    OpStatus res = sg.Set(params, args[i], args[i + 1]);
    if (res != OpStatus::OK) {  // OOM for example.
      return res;
    }
  }

  return OpStatus::OK;
}

OpResult<void> SetGeneric(ConnectionContext* cntx, const SetCmd::SetParams& sparams,
                          string_view key, string_view value) {
  DCHECK(cntx->transaction);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd sg(t->GetOpArgs(shard));
    return sg.Set(sparams, key, value);
  };
  return cntx->transaction->ScheduleSingleHop(std::move(cb));
}

}  // namespace

OpStatus SetCmd::Set(const SetParams& params, string_view key, string_view value) {
  EngineShard* shard = op_args_.shard;
  auto& db_slice = shard->db_slice();

  DCHECK(db_slice.IsDbValid(op_args_.db_cntx.db_index));

  VLOG(2) << "Set " << key << "(" << db_slice.shard_id() << ") ";

  if (params.IsConditionalSet()) {
    const auto [it, expire_it] = db_slice.FindExt(op_args_.db_cntx, key);
    // Make sure that we have this key, and only add it if it does exists
    if (params.how == SET_IF_EXISTS) {
      if (IsValid(it)) {
        return SetExisting(params, it, expire_it, key, value);
      } else {
        return OpStatus::SKIPPED;
      }
    } else {
      if (IsValid(it)) {  // if the policy is not to overide and have the key, just return
        return OpStatus::SKIPPED;
      }
    }
  }
  // At this point we either need to add missing entry, or we
  // will override an existing one
  // Trying to add a new entry.
  tuple<PrimeIterator, ExpireIterator, bool> add_res;
  try {
    add_res = db_slice.AddOrFind2(op_args_.db_cntx, key);
  } catch (bad_alloc& e) {
    return OpStatus::OUT_OF_MEMORY;
  }

  PrimeIterator it = get<0>(add_res);
  if (!get<2>(add_res)) {  // Existing.
    return SetExisting(params, it, get<1>(add_res), key, value);
  }
  //
  // Adding new value.
  PrimeValue tvalue{value};
  tvalue.SetFlag(params.memcache_flags != 0);
  it->second = std::move(tvalue);
  db_slice.PostUpdate(op_args_.db_cntx.db_index, it, key, false);

  if (params.expire_after_ms) {
    db_slice.UpdateExpire(op_args_.db_cntx.db_index, it,
                          params.expire_after_ms + op_args_.db_cntx.time_now_ms);
  }

  if (params.memcache_flags)
    db_slice.SetMCFlag(op_args_.db_cntx.db_index, it->first.AsRef(), params.memcache_flags);

  if (shard->tiered_storage()) {  // external storage enabled.
    // TODO: we may have a bug if we block the fiber inside UnloadItem - "it" may be invalid
    // afterwards.
    if (value.size() >= kMinTieredLen) {
      shard->tiered_storage()->UnloadItem(op_args_.db_cntx.db_index, it);
    }
  }

  RecordJournal(op_args_, key, it->second);
  return OpStatus::OK;
}

OpStatus SetCmd::SetExisting(const SetParams& params, PrimeIterator it, ExpireIterator e_it,
                             string_view key, string_view value) {
  if (params.how == SET_IF_NOTEXIST)
    return OpStatus::SKIPPED;

  PrimeValue& prime_value = it->second;
  EngineShard* shard = op_args_.shard;

  if (params.prev_val) {
    if (prime_value.ObjType() != OBJ_STRING)
      return OpStatus::WRONG_TYPE;

    string val = GetString(shard, prime_value);
    params.prev_val->emplace(move(val));
  }

  DbSlice& db_slice = shard->db_slice();
  uint64_t at_ms =
      params.expire_after_ms ? params.expire_after_ms + op_args_.db_cntx.time_now_ms : 0;
  if (IsValid(e_it) && at_ms) {
    e_it->second = db_slice.FromAbsoluteTime(at_ms);
  } else {
    // We need to update expiry, or maybe erase the object if it was expired.
    bool changed = db_slice.UpdateExpire(op_args_.db_cntx.db_index, it, at_ms);
    if (changed && at_ms == 0)  // erased.
      return OpStatus::OK;      // TODO: to update journal with deletion.
  }

  db_slice.PreUpdate(op_args_.db_cntx.db_index, it);

  // Check whether we need to update flags table.
  bool req_flag_update = (params.memcache_flags != 0) != prime_value.HasFlag();
  if (req_flag_update) {
    prime_value.SetFlag(params.memcache_flags != 0);
    db_slice.SetMCFlag(op_args_.db_cntx.db_index, it->first.AsRef(), params.memcache_flags);
  }

  // overwrite existing entry.
  prime_value.SetString(value);

  if (value.size() >= kMinTieredLen) {  // external storage enabled.

    // TODO: if UnloadItem can block the calling fiber, then we have the bug because then "it"
    // can be invalid after the function returns and the functions that follow may access invalid
    // entry.
    if (shard->tiered_storage()) {
      shard->tiered_storage()->UnloadItem(op_args_.db_cntx.db_index, it);
    }
  }

  db_slice.PostUpdate(op_args_.db_cntx.db_index, it, key);
  RecordJournal(op_args_, key, it->second);

  return OpStatus::OK;
}

void StringFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  set_qps.Inc();

  string_view key = ArgS(args, 1);
  string_view value = ArgS(args, 2);

  SetCmd::SetParams sparams;
  sparams.memcache_flags = cntx->conn_state.memcache_flag;

  int64_t int_arg;
  SinkReplyBuilder* builder = cntx->reply_builder();

  for (size_t i = 3; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);

    if (cur_arg == "EX" || cur_arg == "PX" || cur_arg == "EXAT" || cur_arg == "PXAT") {
      bool is_ms = (cur_arg == "PX" || cur_arg == "PXAT");
      ++i;
      if (i == args.size()) {
        builder->SendError(kSyntaxErr);
      }

      string_view ex = ArgS(args, i);
      if (!absl::SimpleAtoi(ex, &int_arg)) {
        return builder->SendError(kInvalidIntErr);
      }

      // Since PXAT/EXAT can change this, we need to check this ahead
      if (int_arg <= 0) {
        return builder->SendError(InvalidExpireTime("set"));
      }
      // for []AT we need to take expiration time as absolute from the value given
      // check here and if the time is in the past, return OK but don't set it
      // Note that the time pass here for PXAT is in milliseconds, we must not change it!
      if (cur_arg == "EXAT" || cur_arg == "PXAT") {
        int_arg = CalculateAbsTime(int_arg, is_ms);
        if (int_arg < 0) {
          // this happened in the past, just return, for some reason Redis reports OK in this case
          return builder->SendStored();
        }
      }
      if (!is_ms && int_arg >= kMaxExpireDeadlineSec) {
        return builder->SendError(InvalidExpireTime("set"));
      }

      if (!is_ms) {
        int_arg *= 1000;
      }
      if (int_arg >= kMaxExpireDeadlineSec * 1000) {
        return builder->SendError(InvalidExpireTime("set"));
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

  const auto result{SetGeneric(cntx, sparams, key, value)};

  if (result == OpStatus::OK) {
    return builder->SendStored();
  }

  if (result == OpStatus::OUT_OF_MEMORY) {
    return builder->SendError(kOutOfMemory);
  }

  CHECK_EQ(result, OpStatus::SKIPPED);  // in case of NX option

  return builder->SendSetSkipped();
}

void StringFamily::SetEx(CmdArgList args, ConnectionContext* cntx) {
  SetExGeneric(true, std::move(args), cntx);
}

void StringFamily::SetNx(CmdArgList args, ConnectionContext* cntx) {
  // This is the same as calling the "Set" function, only in this case we are
  // change the value only if the key does not exist. Otherwise the function
  // will not modify it. in which case it would return 0
  // it would return to the caller 1 in case the key did not exists and was added
  string_view key = ArgS(args, 1);
  string_view value = ArgS(args, 2);

  SetCmd::SetParams sparams;
  sparams.how = SetCmd::SET_IF_NOTEXIST;
  sparams.memcache_flags = cntx->conn_state.memcache_flag;
  const auto results{SetGeneric(cntx, std::move(sparams), key, value)};
  SinkReplyBuilder* builder = cntx->reply_builder();
  if (results == OpStatus::OK) {
    return builder->SendLong(1);  // this means that we successfully set the value
  }
  if (results == OpStatus::OUT_OF_MEMORY) {
    return builder->SendError(kOutOfMemory);
  }
  CHECK_EQ(results, OpStatus::SKIPPED);  // in this case it must be skipped!
  return builder->SendLong(0);  // value do exists, we need to report that we didn't change it
}

void StringFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  get_qps.Inc();

  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpGet(t->GetOpArgs(shard), key); };

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

void StringFamily::GetDel(CmdArgList args, ConnectionContext* cntx) {
  get_qps.Inc();

  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    bool run_del = true;
    return OpGet(t->GetOpArgs(shard), key, run_del);
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
  string_view key = ArgS(args, 1);
  string_view value = ArgS(args, 2);
  std::optional<string> prev_val;

  SetCmd::SetParams sparams;
  sparams.prev_val = &prev_val;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd cmd(t->GetOpArgs(shard));

    return cmd.Set(sparams, key, value);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status != OpStatus::OK) {
    (*cntx)->SendError(status);
    return;
  }

  if (prev_val) {
    (*cntx)->SendBulkString(*prev_val);
    return;
  }

  return (*cntx)->SendNull();
}

void StringFamily::GetEx(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  DbSlice::ExpireParams exp_params;
  int64_t int_arg = 0;

  for (size_t i = 2; i < args.size(); i++) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);

    if (cur_arg == "EX" || cur_arg == "PX" || cur_arg == "EXAT" || cur_arg == "PXAT") {
      i++;
      if (i >= args.size()) {
        return (*cntx)->SendError(kSyntaxErr);
      }

      string_view ex = ArgS(args, i);
      if (!absl::SimpleAtoi(ex, &int_arg)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }

      if (int_arg <= 0) {
        return (*cntx)->SendError(InvalidExpireTime("getex"));
      }

      if (cur_arg == "EXAT" || cur_arg == "PXAT") {
        exp_params.absolute = true;
      }

      exp_params.value = int_arg;
      if (cur_arg == "EX" || cur_arg == "EXAT") {
        exp_params.unit = TimeUnit::SEC;
      } else {
        exp_params.unit = TimeUnit::MSEC;
      }
    } else if (cur_arg == "PERSIST") {
      exp_params.persist = true;
    } else {
      return (*cntx)->SendError(kSyntaxErr);
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGet(t->GetOpArgs(shard), key, false, exp_params);
  };

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result)
    return (*cntx)->SendBulkString(*result);

  switch (result.status()) {
    case OpStatus::WRONG_TYPE:
      (*cntx)->SendError(kWrongTypeErr);
      break;
    default:
      DVLOG(1) << "GET " << key << " nil";
      (*cntx)->SendNull();
  }
}

void StringFamily::Incr(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  return IncrByGeneric(key, 1, cntx);
}

void StringFamily::IncrBy(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_EQ(3u, args.size());

  string_view key = ArgS(args, 1);
  string_view sval = ArgS(args, 2);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  return IncrByGeneric(key, val, cntx);
}

void StringFamily::IncrByFloat(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view sval = ArgS(args, 2);
  double val;

  if (!absl::SimpleAtod(sval, &val)) {
    return (*cntx)->SendError(kInvalidFloatErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrFloat(t->GetOpArgs(shard), key, val);
  };

  OpResult<double> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* builder = (RedisReplyBuilder*)cntx->reply_builder();

  DVLOG(2) << "IncrByGeneric " << key << "/" << result.value();
  if (!result) {
    return (*cntx)->SendError(result.status());
  }

  builder->SendDouble(result.value());
}

void StringFamily::Decr(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  return IncrByGeneric(key, -1, cntx);
}

void StringFamily::DecrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view sval = ArgS(args, 2);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  if (val == INT64_MIN) {
    return (*cntx)->SendError(kIncrOverflow);
  }

  return IncrByGeneric(key, -val, cntx);
}

void StringFamily::Append(CmdArgList args, ConnectionContext* cntx) {
  ExtendGeneric(std::move(args), false, cntx);
}

void StringFamily::Prepend(CmdArgList args, ConnectionContext* cntx) {
  ExtendGeneric(std::move(args), true, cntx);
}

void StringFamily::IncrByGeneric(string_view key, int64_t val, ConnectionContext* cntx) {
  bool skip_on_missing = cntx->protocol() == Protocol::MEMCACHE;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpResult<int64_t> res = OpIncrBy(t->GetOpArgs(shard), key, val, skip_on_missing);
    return res;
  };

  OpResult<int64_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* builder = cntx->reply_builder();

  DVLOG(2) << "IncrByGeneric " << key << "/" << result.value();
  switch (result.status()) {
    case OpStatus::OK:
      builder->SendLong(result.value());
      break;
    case OpStatus::INVALID_VALUE:
      builder->SendError(kInvalidIntErr);
      break;
    case OpStatus::OUT_OF_RANGE:
      builder->SendError(kIncrOverflow);
      break;
    case OpStatus::KEY_NOTFOUND:  // Relevant only for MC
      reinterpret_cast<MCReplyBuilder*>(builder)->SendNotFound();
      break;
    default:
      reinterpret_cast<RedisReplyBuilder*>(builder)->SendError(result.status());
      break;
  }
}

void StringFamily::ExtendGeneric(CmdArgList args, bool prepend, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view sval = ArgS(args, 2);

  if (cntx->protocol() == Protocol::REDIS) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return ExtendOrSet(t->GetOpArgs(shard), key, sval, prepend);
    };

    OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
    if (!result)
      return (*cntx)->SendError(result.status());
    else
      return (*cntx)->SendLong(result.value());
  }
  DCHECK(cntx->protocol() == Protocol::MEMCACHE);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return ExtendOrSkip(t->GetOpArgs(shard), key, sval, prepend);
  };

  OpResult<bool> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  SinkReplyBuilder* builder = cntx->reply_builder();
  if (result.value_or(false)) {
    return builder->SendStored();
  }

  builder->SendSetSkipped();
}

/// (P)SETEX key seconds value
void StringFamily::SetExGeneric(bool seconds, CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view ex = ArgS(args, 2);
  string_view value = ArgS(args, 3);
  int32_t unit_vals;

  if (!absl::SimpleAtoi(ex, &unit_vals)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  if (unit_vals < 1) {
    ToLower(&args[0]);
    return (*cntx)->SendError(InvalidExpireTime(ArgS(args, 0)));
  }

  SetCmd::SetParams sparams;
  if (seconds)
    sparams.expire_after_ms = uint64_t(unit_vals) * 1000;
  else
    sparams.expire_after_ms = unit_vals;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd sg(t->GetOpArgs(shard));
    return sg.Set(sparams, key, value);
  };

  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));

  return (*cntx)->SendError(result.status());
}

void StringFamily::MGet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GT(args.size(), 1U);

  Transaction* transaction = cntx->transaction;
  unsigned shard_count = shard_set->size();
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

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto args = t->ShardArgsInShard(shard->shard_id());
    return OpMSet(t->GetOpArgs(shard), args);
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  if (status == OpStatus::OK) {
    (*cntx)->SendOk();
  } else {
    (*cntx)->SendError(status);
  }
}

void StringFamily::MSetNx(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;

  transaction->Schedule();

  atomic_bool exists{false};

  auto cb = [&](Transaction* t, EngineShard* es) {
    auto args = t->ShardArgsInShard(es->shard_id());
    for (size_t i = 0; i < args.size(); i += 2) {
      auto it = es->db_slice().FindExt(t->db_context(), args[i]).first;
      if (IsValid(it)) {
        exists.store(true, memory_order_relaxed);
        break;
      }
    }

    return OpStatus::OK;
  };

  transaction->Execute(std::move(cb), false);
  bool to_skip = exists.load(memory_order_relaxed) == true;

  auto epilog_cb = [&](Transaction* t, EngineShard* shard) {
    if (to_skip)
      return OpStatus::OK;

    auto args = t->ShardArgsInShard(shard->shard_id());
    return OpMSet(t->GetOpArgs(shard), std::move(args));
  };

  transaction->Execute(std::move(epilog_cb), true);

  (*cntx)->SendLong(to_skip ? 0 : 1);
}

void StringFamily::StrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<size_t> {
    OpResult<PrimeIterator> it_res = shard->db_slice().Find(t->db_context(), key, OBJ_STRING);
    if (!it_res.ok())
      return it_res.status();

    return it_res.value()->second.Size();
  };

  Transaction* trans = cntx->transaction;
  OpResult<size_t> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(result.status());
  } else {
    (*cntx)->SendLong(result.value());
  }
}

void StringFamily::GetRange(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view from = ArgS(args, 2);
  string_view to = ArgS(args, 3);
  int32_t start, end;

  if (!absl::SimpleAtoi(from, &start) || !absl::SimpleAtoi(to, &end)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGetRange(t->GetOpArgs(shard), key, start, end);
  };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(result.status());
  } else {
    (*cntx)->SendBulkString(result.value());
  }
}

void StringFamily::SetRange(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view offset = ArgS(args, 2);
  string_view value = ArgS(args, 3);
  int32_t start;

  if (!absl::SimpleAtoi(offset, &start)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  if (start < 0) {
    return (*cntx)->SendError("offset is out of range");
  }

  size_t min_size = start + value.size();
  if (min_size > kMaxStrLen) {
    return (*cntx)->SendError("string exceeds maximum allowed size");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    return OpSetRange(t->GetOpArgs(shard), key, start, value);
  };

  Transaction* trans = cntx->transaction;
  OpResult<uint32_t> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(result.status());
  } else {
    (*cntx)->SendLong(result.value());
  }
}

void StringFamily::PSetEx(CmdArgList args, ConnectionContext* cntx) {
  SetExGeneric(false, std::move(args), cntx);
}

auto StringFamily::OpMGet(bool fetch_mcflag, bool fetch_mcver, const Transaction* t,
                          EngineShard* shard) -> MGetResponse {
  auto args = t->ShardArgsInShard(shard->shard_id());
  DCHECK(!args.empty());

  MGetResponse response(args.size());

  auto& db_slice = shard->db_slice();
  for (size_t i = 0; i < args.size(); ++i) {
    OpResult<PrimeIterator> it_res = db_slice.Find(t->db_context(), args[i], OBJ_STRING);
    if (!it_res)
      continue;

    const PrimeIterator& it = *it_res;
    auto& dest = response[i].emplace();

    dest.value = GetString(shard, it->second);
    if (fetch_mcflag) {
      dest.mc_flag = db_slice.GetMCFlag(t->db_index(), it->first);
      if (fetch_mcver) {
        dest.mc_ver = it.GetVersion();
      }
    }
  }

  return response;
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
            << CI{"SETNX", CO::WRITE | CO::DENYOOM, 3, 1, 1, 1}.HFUNC(SetNx)
            << CI{"PSETEX", CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(PSetEx)
            << CI{"APPEND", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Append)
            << CI{"PREPEND", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Prepend)
            << CI{"INCR", CO::WRITE | CO::DENYOOM | CO::FAST, 2, 1, 1, 1}.HFUNC(Incr)
            << CI{"DECR", CO::WRITE | CO::DENYOOM | CO::FAST, 2, 1, 1, 1}.HFUNC(Decr)
            << CI{"INCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(IncrBy)
            << CI{"INCRBYFLOAT", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(IncrByFloat)
            << CI{"DECRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(DecrBy)
            << CI{"GET", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Get)
            << CI{"GETDEL", CO::WRITE | CO::DENYOOM | CO::FAST, 2, 1, 1, 1}.HFUNC(GetDel)
            << CI{"GETEX", CO::WRITE | CO::DENYOOM | CO::FAST, -1, 1, 1, 1}.HFUNC(GetEx)
            << CI{"GETSET", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(GetSet)
            << CI{"MGET", CO::READONLY | CO::FAST | CO::REVERSE_MAPPING, -2, 1, -1, 1}.HFUNC(MGet)
            << CI{"MSET", CO::WRITE | CO::DENYOOM, -3, 1, -1, 2}.HFUNC(MSet)
            << CI{"MSETNX", CO::WRITE | CO::DENYOOM, -3, 1, -1, 2}.HFUNC(MSetNx)
            << CI{"STRLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(StrLen)
            << CI{"GETRANGE", CO::READONLY | CO::FAST, 4, 1, 1, 1}.HFUNC(GetRange)
            << CI{"SUBSTR", CO::READONLY | CO::FAST, 4, 1, 1, 1}.HFUNC(
                   GetRange)  // Alias for GetRange
            << CI{"SETRANGE", CO::WRITE | CO::FAST | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(SetRange);
}

}  // namespace dfly
