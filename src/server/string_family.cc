// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_family.h"

#include <absl/container/inlined_vector.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <tuple>

#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/cmd_arg_parser.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"

ABSL_FLAG(bool, tiered_skip_prefetch, false,
          "If true, does not load offloaded string back to in-memory store during GET command."
          "For testing/development purposes only.");

namespace dfly {

namespace {

using namespace std;
using namespace facade;

using CI = CommandId;

constexpr uint32_t kMaxStrLen = 1 << 28;
[[maybe_unused]] constexpr size_t kMinTieredLen = TieredStorage::kMinBlobLen;

size_t CopyValueToBuffer(const PrimeValue& pv, char* dest) {
  DCHECK_EQ(pv.ObjType(), OBJ_STRING);
  DCHECK(!pv.IsExternal());
  pv.GetString(dest);
  return pv.Size();
}

string GetString(const PrimeValue& pv) {
  string res;
  if (pv.ObjType() != OBJ_STRING)
    return res;
  res.resize(pv.Size());
  CopyValueToBuffer(pv, res.data());

  return res;
}

OpResult<uint32_t> OpSetRange(const OpArgs& op_args, string_view key, size_t start,
                              string_view value) {
  VLOG(2) << "SetRange(" << key << ", " << start << ", " << value << ")";
  auto& db_slice = op_args.shard->db_slice();
  size_t range_len = start + value.size();

  if (range_len == 0) {
    auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STRING);
    if (it_res) {
      return it_res.value()->second.Size();
    } else {
      return it_res.status();
    }
  }

  auto op_res = db_slice.AddOrFindAndFetch(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& res = *op_res;

  string s;

  if (res.is_new) {
    s.resize(range_len);
  } else {
    if (res.it->second.ObjType() != OBJ_STRING)
      return OpStatus::WRONG_TYPE;

    s = GetString(res.it->second);
    if (s.size() < range_len)
      s.resize(range_len);
  }

  memcpy(s.data() + start, value.data(), value.size());
  res.it->second.SetString(s);
  return res.it->second.Size();
}

OpResult<string> OpGetRange(const OpArgs& op_args, string_view key, int32_t start, int32_t end) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.FindAndFetchReadOnly(op_args.db_cntx, key, OBJ_STRING);
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
  string_view slice = co.GetSlice(&tmp);

  return string(slice.substr(start, end - start + 1));
};

size_t ExtendExisting(const OpArgs& op_args, DbSlice::Iterator it, string_view key, string_view val,
                      bool prepend) {
  string tmp, new_val;
  string_view slice = it->second.GetSlice(&tmp);

  if (prepend)
    new_val = absl::StrCat(val, slice);
  else
    new_val = absl::StrCat(slice, val);

  it->second.SetString(new_val);

  return new_val.size();
}

// Returns the length of the extended string. if prepend is false - appends the val.
OpResult<uint32_t> ExtendOrSet(const OpArgs& op_args, string_view key, string_view val,
                               bool prepend) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  auto op_res = db_slice.AddOrFindAndFetch(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;
  if (add_res.is_new) {
    add_res.it->second.SetString(val);
    return val.size();
  }

  if (add_res.it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  return ExtendExisting(op_args, add_res.it, key, val, prepend);
}

OpResult<bool> ExtendOrSkip(const OpArgs& op_args, string_view key, string_view val, bool prepend) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.FindAndFetchMutable(op_args.db_cntx, key, OBJ_STRING);
  if (!it_res) {
    return false;
  }

  return ExtendExisting(op_args, it_res->it, key, val, prepend);
}

OpResult<string> OpMutableGet(const OpArgs& op_args, string_view key, bool del_hit = false,
                              const DbSlice::ExpireParams& exp_params = {}) {
  auto res = op_args.shard->db_slice().FindAndFetchMutable(op_args.db_cntx, key);
  res.post_updater.Run();

  if (!IsValid(res.it))
    return OpStatus::KEY_NOTFOUND;

  if (res.it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  const PrimeValue& pv = res.it->second;

  if (del_hit) {
    string key_bearer = GetString(pv);

    DVLOG(1) << "Del: " << key;
    auto& db_slice = op_args.shard->db_slice();

    CHECK(db_slice.Del(op_args.db_cntx.db_index, res.it));

    return key_bearer;
  }

  /*Get value before expire*/
  string ret_val = GetString(pv);

  if (exp_params.IsDefined()) {
    DVLOG(1) << "Expire: " << key;
    auto& db_slice = op_args.shard->db_slice();
    OpStatus status =
        db_slice.UpdateExpire(op_args.db_cntx, res.it, res.exp_it, exp_params).status();
    if (status != OpStatus::OK)
      return status;
  }

  return ret_val;
}

OpResult<double> OpIncrFloat(const OpArgs& op_args, string_view key, double val) {
  auto& db_slice = op_args.shard->db_slice();

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  char buf[128];

  if (add_res.is_new) {
    char* str = RedisReplyBuilder::FormatDouble(val, buf, sizeof(buf));
    add_res.it->second.SetString(str);

    return val;
  }

  if (add_res.it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  if (add_res.it->second.Size() == 0)
    return OpStatus::INVALID_FLOAT;

  string tmp;
  string_view slice = add_res.it->second.GetSlice(&tmp);

  double base = 0;
  if (!ParseDouble(slice, &base)) {
    return OpStatus::INVALID_FLOAT;
  }

  base += val;

  if (isnan(base) || isinf(base)) {
    return OpStatus::INVALID_FLOAT;
  }

  char* str = RedisReplyBuilder::FormatDouble(base, buf, sizeof(buf));

  add_res.it->second.SetString(str);

  return base;
}

// if skip_on_missing - returns KEY_NOTFOUND.
OpResult<int64_t> OpIncrBy(const OpArgs& op_args, string_view key, int64_t incr,
                           bool skip_on_missing) {
  auto& db_slice = op_args.shard->db_slice();

  // we avoid using AddOrFind because of skip_on_missing option for memcache.
  auto res = db_slice.FindMutable(op_args.db_cntx, key);

  if (!IsValid(res.it)) {
    if (skip_on_missing)
      return OpStatus::KEY_NOTFOUND;

    CompactObj cobj;
    cobj.SetInt(incr);

    auto op_result = db_slice.AddNew(op_args.db_cntx, key, std::move(cobj), 0);
    RETURN_ON_BAD_STATUS(op_result);

    return incr;
  }

  if (res.it->second.ObjType() != OBJ_STRING) {
    return OpStatus::WRONG_TYPE;
  }

  auto opt_prev = res.it->second.TryGetInt();
  if (!opt_prev) {
    return OpStatus::INVALID_VALUE;
  }

  long long prev = *opt_prev;
  if ((incr < 0 && prev < 0 && incr < (LLONG_MIN - prev)) ||
      (incr > 0 && prev > 0 && incr > (LLONG_MAX - prev))) {
    return OpStatus::OUT_OF_RANGE;
  }

  int64_t new_val = prev + incr;
  DCHECK(!res.it->second.IsExternal());
  res.it->second.SetInt(new_val);

  return new_val;
}

int64_t AbsExpiryToTtl(int64_t abs_expiry_time, bool as_milli) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::seconds;
  using std::chrono::system_clock;

  if (as_milli) {
    return abs_expiry_time -
           duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  } else {
    return abs_expiry_time - duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  }
}

// Returns true if keys were set, false otherwise.
void OpMSet(const OpArgs& op_args, ArgSlice args, atomic_bool* success) {
  DCHECK(!args.empty() && args.size() % 2 == 0);

  SetCmd::SetParams params;
  SetCmd sg(op_args, false);

  size_t i = 0;
  for (; i < args.size(); i += 2) {
    DVLOG(1) << "MSet " << args[i] << ":" << args[i + 1];
    OpResult<optional<string>> res = sg.Set(params, args[i], args[i + 1]);
    if (res.status() != OpStatus::OK) {  // OOM for example.
      success->store(false);
      break;
    }
  }

  if (auto journal = op_args.shard->journal(); journal) {
    // We write a custom journal because an OOM in the above loop could lead to partial success, so
    // we replicate only what was changed.
    string_view cmd;
    ArgSlice cmd_args;
    if (i == 0) {
      // All shards must record the tx was executed for the replica to execute it, so we send a PING
      // in case nothing was changed
      cmd = "PING";
    } else {
      // journal [0, i)
      cmd = "MSET";
      cmd_args = ArgSlice(&args[0], i);
    }
    RecordJournal(op_args, cmd, cmd_args, op_args.tx->GetUniqueShardCnt());
  }
}

// See comment for SetCmd::Set() for when and how OpResult's value (i.e. optional<string>) is set.
OpResult<optional<string>> SetGeneric(ConnectionContext* cntx, const SetCmd::SetParams& sparams,
                                      string_view key, string_view value, bool manual_journal) {
  DCHECK(cntx->transaction);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd sg(t->GetOpArgs(shard), manual_journal);
    return sg.Set(sparams, key, value);
  };
  return cntx->transaction->ScheduleSingleHopT(std::move(cb));
}

// emission_interval_ms assumed to be positive
// limit is assumed to be positive
OpResult<array<int64_t, 5>> OpThrottle(const OpArgs& op_args, const string_view key,
                                       const int64_t limit, const int64_t emission_interval_ms,
                                       const uint64_t quantity) {
  auto& db_slice = op_args.shard->db_slice();

  if (emission_interval_ms > INT64_MAX / limit) {
    return OpStatus::INVALID_INT;
  }
  const int64_t delay_variation_tolerance_ms = emission_interval_ms * limit;  // should be positive

  int64_t remaining = 0;
  int64_t reset_after_ms = -1000;
  int64_t retry_after_ms = -1000;

  if (quantity != 0 && static_cast<uint64_t>(emission_interval_ms) > INT64_MAX / quantity) {
    return OpStatus::INVALID_INT;
  }
  const int64_t increment_ms = emission_interval_ms * quantity;  // should be nonnegative

  auto res = db_slice.FindMutable(op_args.db_cntx, key);
  const int64_t now_ms = op_args.db_cntx.time_now_ms;

  int64_t tat_ms = now_ms;
  if (IsValid(res.it)) {
    if (res.it->second.ObjType() != OBJ_STRING) {
      return OpStatus::WRONG_TYPE;
    }

    auto opt_prev = res.it->second.TryGetInt();
    if (!opt_prev) {
      return OpStatus::INVALID_VALUE;
    }
    tat_ms = *opt_prev;
  }

  int64_t new_tat_ms = max(tat_ms, now_ms);
  if (new_tat_ms > INT64_MAX - increment_ms) {
    return OpStatus::INVALID_INT;
  }
  new_tat_ms += increment_ms;

  if (new_tat_ms < INT64_MIN + delay_variation_tolerance_ms) {
    return OpStatus::INVALID_INT;
  }
  const int64_t allow_at_ms = new_tat_ms - delay_variation_tolerance_ms;

  if (allow_at_ms >= 0 ? now_ms < INT64_MIN + allow_at_ms : now_ms > INT64_MAX + allow_at_ms) {
    return OpStatus::INVALID_INT;
  }
  const int64_t diff_ms = now_ms - allow_at_ms;

  const bool limited = diff_ms < 0;
  int64_t ttl_ms;
  if (limited) {
    if (increment_ms <= delay_variation_tolerance_ms) {
      if (diff_ms == INT64_MIN) {
        return OpStatus::INVALID_INT;
      }
      retry_after_ms = -diff_ms;
    }

    if (now_ms >= 0 ? tat_ms < INT64_MIN + now_ms : tat_ms > INT64_MAX + now_ms) {
      return OpStatus::INVALID_INT;
    }
    ttl_ms = tat_ms - now_ms;
  } else {
    if (now_ms >= 0 ? new_tat_ms < INT64_MIN + now_ms : new_tat_ms > INT64_MAX + now_ms) {
      return OpStatus::INVALID_INT;
    }
    ttl_ms = new_tat_ms - now_ms;
  }

  if (ttl_ms < delay_variation_tolerance_ms - INT64_MAX) {
    return OpStatus::INVALID_INT;
  }
  const int64_t next_ms = delay_variation_tolerance_ms - ttl_ms;
  if (next_ms > -emission_interval_ms) {
    remaining = next_ms / emission_interval_ms;
  }
  reset_after_ms = ttl_ms;

  if (!limited) {
    if (IsValid(res.it)) {
      if (IsValid(res.exp_it)) {
        res.exp_it->second = db_slice.FromAbsoluteTime(new_tat_ms);
      } else {
        db_slice.AddExpire(op_args.db_cntx.db_index, res.it, new_tat_ms);
      }

      res.it->second.SetInt(new_tat_ms);
    } else {
      CompactObj cobj;
      cobj.SetInt(new_tat_ms);

      auto res = db_slice.AddNew(op_args.db_cntx, key, std::move(cobj), new_tat_ms);
      if (!res) {
        return res.status();
      }
    }
  }

  return array<int64_t, 5>{limited ? 1 : 0, limit, remaining, retry_after_ms, reset_after_ms};
}

class SetResultBuilder {
 public:
  explicit SetResultBuilder(bool return_prev_value) : return_prev_value_(return_prev_value) {
  }

  void CachePrevValueIfNeeded(const PrimeValue& pv) {
    if (return_prev_value_) {
      // We call lazily call GetString() here to save string copying when not needed.
      prev_value_ = GetString(pv);
    }
  }

  // Returns either the previous value or `status`, depending on return_prev_value_.
  OpResult<optional<string>> Return(OpStatus status) && {
    if (return_prev_value_) {
      return std::move(prev_value_);
    } else {
      return status;
    }
  }

 private:
  bool return_prev_value_;
  std::optional<string> prev_value_;
};

SinkReplyBuilder::MGetResponse OpMGet(bool fetch_mcflag, bool fetch_mcver, const Transaction* t,
                                      EngineShard* shard) {
  auto keys = t->GetShardArgs(shard->shard_id());
  DCHECK(!keys.empty());

  auto& db_slice = shard->db_slice();

  SinkReplyBuilder::MGetResponse response(keys.size());
  absl::InlinedVector<DbSlice::ConstIterator, 32> iters(keys.size());

  size_t total_size = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    auto it_res = db_slice.FindAndFetchReadOnly(t->GetDbContext(), keys[i], OBJ_STRING);
    if (!it_res)
      continue;
    iters[i] = *it_res;
    total_size += (*it_res)->second.Size();
  }

  response.storage_list = SinkReplyBuilder::AllocMGetStorage(total_size);
  char* next = response.storage_list->data;

  for (size_t i = 0; i < keys.size(); ++i) {
    auto it = iters[i];
    if (it.is_done())
      continue;

    auto& resp = response.resp_arr[i].emplace();

    size_t size = CopyValueToBuffer(it->second, next);
    resp.value = string_view(next, size);
    next += size;

    if (fetch_mcflag) {
      if (it->second.HasFlag()) {
        resp.mc_flag = db_slice.GetMCFlag(t->GetDbIndex(), it->first);
      }

      if (fetch_mcver) {
        resp.mc_ver = it.GetVersion();
      }
    }
  }

  return response;
}

// Either string or future from tiered storage
struct StringValue {
  StringValue() : v_{} {
  }
  StringValue(std::string s) : v_{std::move(s)} {
  }
  StringValue(util::fb2::Future<std::string> f) : v_{std::move(f)} {
  }

  std::string Get() && {
    DCHECK(!holds_alternative<monostate>(v_));

    auto prev = exchange(v_, monostate{});
    if (holds_alternative<string>(prev))
      return std::move(std::get<string>(prev));
    return std::get<util::fb2::Future<std::string>>(prev).get();
  }

 private:
  std::variant<std::monostate, std::string, util::fb2::Future<std::string>> v_;
};

}  // namespace

OpResult<optional<string>> SetCmd::Set(const SetParams& params, string_view key,
                                       string_view value) {
  bool fetch_val = params.flags & SET_GET;
  SetResultBuilder result_builder(fetch_val);

  EngineShard* shard = op_args_.shard;
  auto& db_slice = shard->db_slice();

  DCHECK(db_slice.IsDbValid(op_args_.db_cntx.db_index));

  VLOG(2) << "Set " << key << "(" << db_slice.shard_id() << ") ";

  // if SET_GET is not set then prev_val is null.
  DCHECK(fetch_val || params.prev_val == nullptr);

  if (params.IsConditionalSet()) {
    // We do not always set prev_val and we use result_builder for that.
    bool fetch_value = params.prev_val || fetch_val;
    DbSlice::ItAndUpdater find_res;
    if (fetch_value) {
      find_res = db_slice.FindAndFetchMutable(op_args_.db_cntx, key);
    } else {
      find_res = db_slice.FindMutable(op_args_.db_cntx, key);
    }

    if (IsValid(find_res.it)) {
      if (find_res.it->second.ObjType() != OBJ_STRING) {
        return OpStatus::WRONG_TYPE;
      }
      result_builder.CachePrevValueIfNeeded(find_res.it->second);
    }

    // Make sure that we have this key, and only add it if it does exists
    if (params.flags & SET_IF_EXISTS) {
      if (IsValid(find_res.it)) {
        return std::move(result_builder)
            .Return(SetExisting(params, find_res.it, find_res.exp_it, key, value));
      } else {
        return std::move(result_builder).Return(OpStatus::SKIPPED);
      }
    } else {
      if (IsValid(find_res.it)) {  // if the policy is not to overide and have the key, just return
        return std::move(result_builder).Return(OpStatus::SKIPPED);
      }
    }
  }

  // At this point we either need to add missing entry, or we
  // will override an existing one
  // Trying to add a new entry.
  auto op_res = db_slice.AddOrFind(op_args_.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  auto it = add_res.it;
  if (!add_res.is_new) {
    if (fetch_val && it->second.ObjType() != OBJ_STRING) {
      return OpStatus::WRONG_TYPE;
    }
    result_builder.CachePrevValueIfNeeded(it->second);
    return std::move(result_builder).Return(SetExisting(params, it, add_res.exp_it, key, value));
  }

  // Adding new value.
  PrimeValue tvalue{value};
  tvalue.SetFlag(params.memcache_flags != 0);
  it->second = std::move(tvalue);

  if (params.expire_after_ms) {
    db_slice.AddExpire(op_args_.db_cntx.db_index, it,
                       params.expire_after_ms + op_args_.db_cntx.time_now_ms);
  }

  if (params.memcache_flags)
    db_slice.SetMCFlag(op_args_.db_cntx.db_index, it->first.AsRef(), params.memcache_flags);

  if (params.flags & SET_STICK) {
    it->first.SetSticky(true);
  }

  if (shard->tiered_storage() &&
      TieredStorage::EligibleForOffload(value.size())) {  // external storage enabled.
    shard->tiered_storage()->ScheduleOffloadWithThrottle(op_args_.db_cntx.db_index, it.GetInnerIt(),
                                                         key);
  }

  if (shard->tiered_storage_v2()) {  // external storage enabled
    shard->tiered_storage_v2()->Stash(key, &it->second);
  }

  if (manual_journal_ && op_args_.shard->journal()) {
    RecordJournal(params, key, value);
  }

  return std::move(result_builder).Return(OpStatus::OK);
}

OpStatus SetCmd::SetExisting(const SetParams& params, DbSlice::Iterator it,
                             DbSlice::ExpIterator e_it, string_view key, string_view value) {
  if (params.flags & SET_IF_NOTEXIST)
    return OpStatus::SKIPPED;

  PrimeValue& prime_value = it->second;
  EngineShard* shard = op_args_.shard;

  if (params.prev_val) {
    if (prime_value.ObjType() != OBJ_STRING)
      return OpStatus::WRONG_TYPE;

    string val = GetString(prime_value);
    params.prev_val->emplace(std::move(val));
  }

  DbSlice& db_slice = shard->db_slice();
  uint64_t at_ms =
      params.expire_after_ms ? params.expire_after_ms + op_args_.db_cntx.time_now_ms : 0;

  if (!(params.flags & SET_KEEP_EXPIRE)) {
    if (at_ms) {  // Command has an expiry paramater.
      if (IsValid(e_it)) {
        // Updated existing expiry information.
        e_it->second = db_slice.FromAbsoluteTime(at_ms);
      } else {
        // Add new expiry information.
        db_slice.AddExpire(op_args_.db_cntx.db_index, it, at_ms);
      }
    } else {
      db_slice.RemoveExpire(op_args_.db_cntx.db_index, it);
    }
  }

  if (params.flags & SET_STICK) {
    it->first.SetSticky(true);
  }

  // Update flags
  prime_value.SetFlag(params.memcache_flags != 0);
  db_slice.SetMCFlag(op_args_.db_cntx.db_index, it->first.AsRef(), params.memcache_flags);

  db_slice.RemoveFromTiered(it, op_args_.db_cntx.db_index);
  // overwrite existing entry.
  prime_value.SetString(value);
  DCHECK(!prime_value.HasIoPending());

  if (manual_journal_ && op_args_.shard->journal()) {
    RecordJournal(params, key, value);
  }

  return OpStatus::OK;
}

void SetCmd::RecordJournal(const SetParams& params, string_view key, string_view value) {
  absl::InlinedVector<string_view, 5> cmds({key, value});  // 5 is theoretical maximum;

  std::string exp_str;
  if (params.flags & SET_EXPIRE_AFTER_MS) {
    exp_str = absl::StrCat(params.expire_after_ms + op_args_.db_cntx.time_now_ms);
    cmds.insert(cmds.end(), {"PXAT", exp_str});
  } else if (params.flags & SET_KEEP_EXPIRE) {
    cmds.push_back("KEEPTTL");
  }

  if (params.flags & SET_STICK) {
    cmds.push_back("STICK");
  }
  if (params.memcache_flags) {
    cmds.push_back("_MCFLAGS");
    cmds.push_back(absl::StrCat(params.memcache_flags));
  }

  // Skip NX/XX because SET operation was exectued.
  // Skip GET, because its not important on replica.

  dfly::RecordJournal(op_args_, "SET", ArgSlice{cmds});
}

void StringFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  facade::CmdArgParser parser{args};

  auto [key, value] = parser.Next<string_view, string_view>();

  SetCmd::SetParams sparams;
  sparams.memcache_flags = cntx->conn_state.memcache_flag;

  facade::SinkReplyBuilder* builder = cntx->reply_builder();

  while (parser.HasNext()) {
    parser.ToUpper();
    if (base::_in(parser.Peek(), {"EX", "PX", "EXAT", "PXAT"})) {
      auto [opt, int_arg] = parser.Next<string_view, int64_t>();

      if (auto err = parser.Error(); err) {
        return builder->SendError(err->MakeReply());
      }

      // We can set expiry only once.
      if (sparams.flags & SetCmd::SET_EXPIRE_AFTER_MS)
        return builder->SendError(kSyntaxErr);

      sparams.flags |= SetCmd::SET_EXPIRE_AFTER_MS;

      // Since PXAT/EXAT can change this, we need to check this ahead
      if (int_arg <= 0) {
        return builder->SendError(InvalidExpireTime("set"));
      }

      bool is_ms = (opt[0] == 'P');

      // for []AT we need to take expiration time as absolute from the value given
      // check here and if the time is in the past, return OK but don't set it
      // Note that the time pass here for PXAT is in milliseconds, we must not change it!
      if (absl::EndsWith(opt, "AT")) {
        int_arg = AbsExpiryToTtl(int_arg, is_ms);
        if (int_arg < 0) {
          // this happened in the past, just return, for some reason Redis reports OK in this case
          return builder->SendStored();
        }
      }

      if (is_ms) {
        if (int_arg > kMaxExpireDeadlineMs) {
          int_arg = kMaxExpireDeadlineMs;
        }
      } else {
        if (int_arg > kMaxExpireDeadlineSec) {
          int_arg = kMaxExpireDeadlineSec;
        }
        int_arg *= 1000;
      }
      sparams.expire_after_ms = int_arg;
    } else if (parser.Check("_MCFLAGS").ExpectTail(1)) {
      sparams.memcache_flags = parser.Next<uint16_t>();
    } else {
      uint16_t flag = parser.Switch(  //
          "GET", SetCmd::SET_GET, "STICK", SetCmd::SET_STICK, "KEEPTTL", SetCmd::SET_KEEP_EXPIRE,
          "XX", SetCmd::SET_IF_EXISTS, "NX", SetCmd::SET_IF_NOTEXIST);
      sparams.flags |= flag;
    }
  }

  if (auto err = parser.Error(); err) {
    return builder->SendError(err->MakeReply());
  }

  auto has_mask = [&](uint16_t m) { return (sparams.flags & m) == m; };

  if (has_mask(SetCmd::SET_IF_EXISTS | SetCmd::SET_IF_NOTEXIST) ||
      has_mask(SetCmd::SET_KEEP_EXPIRE | SetCmd::SET_EXPIRE_AFTER_MS)) {
    return builder->SendError(kSyntaxErr);
  }

  OpResult result{SetGeneric(cntx, sparams, key, value, true)};

  if (result == OpStatus::WRONG_TYPE) {
    return cntx->SendError(kWrongTypeErr);
  }

  if (sparams.flags & SetCmd::SET_GET) {
    auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
    // When SET_GET is used, the reply is not affected by whether anything was set.
    if (result->has_value()) {
      rb->SendBulkString(result->value());
    } else {
      rb->SendNull();
    }
    return;
  }

  if (result == OpStatus::OK) {
    return builder->SendStored();
  }

  if (result == OpStatus::OUT_OF_MEMORY) {
    return builder->SendError(kOutOfMemory);
  }

  DCHECK_EQ(result, OpStatus::SKIPPED);  // in case of NX option

  builder->SendSetSkipped();
}

void StringFamily::SetEx(CmdArgList args, ConnectionContext* cntx) {
  SetExGeneric(true, std::move(args), cntx);
}

void StringFamily::PSetEx(CmdArgList args, ConnectionContext* cntx) {
  SetExGeneric(false, std::move(args), cntx);
}

void StringFamily::SetNx(CmdArgList args, ConnectionContext* cntx) {
  // This is the same as calling the "Set" function, only in this case we are
  // change the value only if the key does not exist. Otherwise the function
  // will not modify it. in which case it would return 0
  // it would return to the caller 1 in case the key did not exists and was added
  string_view key = ArgS(args, 0);
  string_view value = ArgS(args, 1);

  SetCmd::SetParams sparams;
  sparams.flags |= SetCmd::SET_IF_NOTEXIST;
  sparams.memcache_flags = cntx->conn_state.memcache_flag;
  const auto results{SetGeneric(cntx, std::move(sparams), key, value, false)};
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
  auto cb = [key = ArgS(args, 0)](Transaction* tx, EngineShard* es) -> OpResult<StringValue> {
    auto it_res = es->db_slice().FindReadOnly(tx->GetDbContext(), key, OBJ_STRING);
    if (!it_res.ok())
      return it_res.status();

    if (const PrimeValue& pv = (*it_res)->second; pv.IsExternal()) {
      return {es->tiered_storage_v2()->Read(key, pv)};
    } else {
      std::string buf;
      pv.GetString(&buf);
      return {std::move(buf)};
    }
  };

  auto res = cntx->transaction->ScheduleSingleHopT(cb);

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  switch (res.status()) {
    case OpStatus::OK:
      rb->SendBulkString(std::move(res.value()).Get());
      break;
    case OpStatus::WRONG_TYPE:
      rb->SendError(kWrongTypeErr);
      break;
    default:
      rb->SendNull();
  }
}

void StringFamily::GetDel(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    bool run_del = true;
    return OpMutableGet(t->GetOpArgs(shard), key, run_del);
  };

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    DVLOG(1) << "GET " << trans->DebugId() << ": " << key << " " << result.value();
    rb->SendBulkString(*result);
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        rb->SendError(kWrongTypeErr);
        break;
      default:
        DVLOG(1) << "GET " << key << " nil";
        rb->SendNull();
    }
  }
}

void StringFamily::GetSet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view value = ArgS(args, 1);
  std::optional<string> prev_val;

  SetCmd::SetParams sparams;
  sparams.prev_val = &prev_val;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd cmd(t->GetOpArgs(shard), false);

    return cmd.Set(sparams, key, value).status();
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status != OpStatus::OK) {
    cntx->SendError(status);
    return;
  }

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (prev_val) {
    rb->SendBulkString(*prev_val);
    return;
  }

  return rb->SendNull();
}

void StringFamily::GetEx(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  DbSlice::ExpireParams exp_params;
  int64_t int_arg = 0;

  for (size_t i = 1; i < args.size(); i++) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);

    if (cur_arg == "EX" || cur_arg == "PX" || cur_arg == "EXAT" || cur_arg == "PXAT") {
      i++;
      if (i >= args.size()) {
        return cntx->SendError(kSyntaxErr);
      }

      string_view ex = ArgS(args, i);
      if (!absl::SimpleAtoi(ex, &int_arg)) {
        return cntx->SendError(kInvalidIntErr);
      }

      if (int_arg <= 0) {
        return cntx->SendError(InvalidExpireTime("getex"));
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
      return cntx->SendError(kSyntaxErr);
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto op_args = t->GetOpArgs(shard);
    auto result = OpMutableGet(op_args, key, false, exp_params);

    // Replicate GETEX as PEXPIREAT or PERSIST
    if (result.ok() && shard->journal()) {
      if (exp_params.persist) {
        RecordJournal(op_args, "PERSIST", {key});
      } else {
        auto [ignore, abs_time] = exp_params.Calculate(op_args.db_cntx.time_now_ms);
        auto abs_time_str = absl::StrCat(abs_time);
        RecordJournal(op_args, "PEXPIREAT", {key, abs_time_str});
      }
    }

    return result;
  };

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result)
    return rb->SendBulkString(*result);

  switch (result.status()) {
    case OpStatus::WRONG_TYPE:
      rb->SendError(kWrongTypeErr);
      break;
    default:
      DVLOG(1) << "GET " << key << " nil";
      rb->SendNull();
  }
}

void StringFamily::Incr(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  return IncrByGeneric(key, 1, cntx);
}

void StringFamily::IncrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return cntx->SendError(kInvalidIntErr);
  }
  return IncrByGeneric(key, val, cntx);
}

void StringFamily::IncrByFloat(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  double val;

  if (!absl::SimpleAtod(sval, &val)) {
    return cntx->SendError(kInvalidFloatErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrFloat(t->GetOpArgs(shard), key, val);
  };

  OpResult<double> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* builder = (RedisReplyBuilder*)cntx->reply_builder();

  DVLOG(2) << "IncrByGeneric " << key << "/" << result.value();
  if (!result) {
    return cntx->SendError(result.status());
  }

  builder->SendDouble(result.value());
}

void StringFamily::Decr(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  return IncrByGeneric(key, -1, cntx);
}

void StringFamily::DecrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return cntx->SendError(kInvalidIntErr);
  }
  if (val == INT64_MIN) {
    return cntx->SendError(kIncrOverflow);
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
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  VLOG(2) << "ExtendGeneric(" << key << ", " << sval << ")";

  if (cntx->protocol() == Protocol::REDIS) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return ExtendOrSet(t->GetOpArgs(shard), key, sval, prepend);
    };

    OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
    if (!result)
      return cntx->SendError(result.status());
    else
      return cntx->SendLong(result.value());
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
  string_view key = ArgS(args, 0);
  string_view ex = ArgS(args, 1);
  string_view value = ArgS(args, 2);
  int64_t unit_vals;

  if (!absl::SimpleAtoi(ex, &unit_vals)) {
    return cntx->SendError(kInvalidIntErr);
  }

  if (unit_vals < 1) {
    return cntx->SendError(InvalidExpireTime(cntx->cid->name()));
  }

  SetCmd::SetParams sparams;
  sparams.flags |= SetCmd::SET_EXPIRE_AFTER_MS;
  if (seconds) {
    if (unit_vals > kMaxExpireDeadlineSec) {
      unit_vals = kMaxExpireDeadlineSec;
    }
    sparams.expire_after_ms = uint64_t(unit_vals) * 1000;
  } else {
    if (unit_vals > kMaxExpireDeadlineMs) {
      unit_vals = kMaxExpireDeadlineMs;
    }
    sparams.expire_after_ms = unit_vals;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    SetCmd sg(t->GetOpArgs(shard), true);
    return sg.Set(sparams, key, value).status();
  };

  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));

  return cntx->SendError(result.status());
}

void StringFamily::MGet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);

  Transaction* transaction = cntx->transaction;
  unsigned shard_count = shard_set->size();
  std::vector<SinkReplyBuilder::MGetResponse> mget_resp(shard_count);

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
  SinkReplyBuilder::MGetResponse res(args.size());

  for (ShardId sid = 0; sid < shard_count; ++sid) {
    if (!transaction->IsActive(sid))
      continue;

    SinkReplyBuilder::MGetResponse& src = mget_resp[sid];
    src.storage_list->next = res.storage_list;
    res.storage_list = src.storage_list;
    src.storage_list = nullptr;

    ArgSlice slice = transaction->GetShardArgs(sid);

    DCHECK(!slice.empty());
    DCHECK_EQ(slice.size(), src.resp_arr.size());

    for (size_t j = 0; j < slice.size(); ++j) {
      if (!src.resp_arr[j])
        continue;

      uint32_t indx = transaction->ReverseArgIndex(sid, j);

      res.resp_arr[indx] = std::move(src.resp_arr[j]);
      if (cntx->protocol() == Protocol::MEMCACHE) {
        res.resp_arr[indx]->key = ArgS(args, indx);
      }
    }
  }

  return cntx->reply_builder()->SendMGetResponse(std::move(res));
}

void StringFamily::MSet(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;

  if (VLOG_IS_ON(2)) {
    string str;
    for (size_t i = 1; i < args.size(); ++i) {
      absl::StrAppend(&str, " ", ArgS(args, i));
    }
    LOG(INFO) << "MSET/" << transaction->GetUniqueShardCnt() << str;
  }

  atomic_bool success = true;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    OpMSet(t->GetOpArgs(shard), args, &success);
    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  if (success.load()) {
    cntx->SendOk();
  } else {
    cntx->SendError(status);
  }
}

void StringFamily::MSetNx(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;

  transaction->Schedule();

  atomic_bool exists{false};

  auto cb = [&](Transaction* t, EngineShard* es) {
    auto args = t->GetShardArgs(es->shard_id());
    for (size_t i = 0; i < args.size(); i += 2) {
      auto it = es->db_slice().FindReadOnly(t->GetDbContext(), args[i]).it;
      if (IsValid(it)) {
        exists.store(true, memory_order_relaxed);
        break;
      }
    }

    return OpStatus::OK;
  };

  transaction->Execute(std::move(cb), false);
  const bool to_skip = exists.load(memory_order_relaxed);

  atomic_bool success = true;
  auto epilog_cb = [&](Transaction* t, EngineShard* shard) {
    if (to_skip)
      return OpStatus::OK;

    auto args = t->GetShardArgs(shard->shard_id());
    OpMSet(t->GetOpArgs(shard), std::move(args), &success);
    return OpStatus::OK;
  };
  transaction->Execute(std::move(epilog_cb), true);

  cntx->SendLong(to_skip || !success.load() ? 0 : 1);
}

void StringFamily::StrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<size_t> {
    auto it_res = shard->db_slice().FindReadOnly(t->GetDbContext(), key, OBJ_STRING);
    if (!it_res.ok())
      return it_res.status();

    return it_res.value()->second.Size();
  };

  Transaction* trans = cntx->transaction;
  OpResult<size_t> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    cntx->SendError(result.status());
  } else {
    cntx->SendLong(result.value());
  }
}

void StringFamily::GetRange(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view from = ArgS(args, 1);
  string_view to = ArgS(args, 2);
  int32_t start, end;

  if (!absl::SimpleAtoi(from, &start) || !absl::SimpleAtoi(to, &end)) {
    return cntx->SendError(kInvalidIntErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGetRange(t->GetOpArgs(shard), key, start, end);
  };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    cntx->SendError(result.status());
  } else {
    auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
    rb->SendBulkString(result.value());
  }
}

void StringFamily::SetRange(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view offset = ArgS(args, 1);
  string_view value = ArgS(args, 2);
  int32_t start;

  if (!absl::SimpleAtoi(offset, &start)) {
    return cntx->SendError(kInvalidIntErr);
  }

  if (start < 0) {
    return cntx->SendError("offset is out of range");
  }

  size_t min_size = start + value.size();
  if (min_size > kMaxStrLen) {
    return cntx->SendError("string exceeds maximum allowed size");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    return OpSetRange(t->GetOpArgs(shard), key, start, value);
  };

  Transaction* trans = cntx->transaction;
  OpResult<uint32_t> result = trans->ScheduleSingleHopT(std::move(cb));

  if (!result.ok()) {
    cntx->SendError(result.status());
  } else {
    cntx->SendLong(result.value());
  }
}

/* CL.THROTTLE <key> <max_burst> <count per period> <period> [<quantity>] */
/* Response is array of 5 integers. The meaning of each array item is:
 *  1. Whether the action was limited:
 *   - 0 indicates the action is allowed.
 *   - 1 indicates that the action was limited/blocked.
 *  2. The total limit of the key (max_burst + 1). This is equivalent to the common
 * X-RateLimit-Limit HTTP header.
 *  3. The remaining limit of the key. Equivalent to X-RateLimit-Remaining.
 *  4. The number of seconds until the user should retry, and always -1 if the action was allowed.
 * Equivalent to Retry-After.
 *  5. The number of seconds until the limit will reset to its maximum capacity. Equivalent to
 * X-RateLimit-Reset.
 */
void StringFamily::ClThrottle(CmdArgList args, ConnectionContext* cntx) {
  const string_view key = ArgS(args, 0);

  // Allow max burst in number of tokens
  uint64_t max_burst;
  const string_view max_burst_str = ArgS(args, 1);
  if (!absl::SimpleAtoi(max_burst_str, &max_burst)) {
    return cntx->SendError(kInvalidIntErr);
  }

  // Emit count of tokens per period
  uint64_t count;
  const string_view count_str = ArgS(args, 2);
  if (!absl::SimpleAtoi(count_str, &count)) {
    return cntx->SendError(kInvalidIntErr);
  }

  // Period of emitting count of tokens
  uint64_t period;
  const string_view period_str = ArgS(args, 3);
  if (!absl::SimpleAtoi(period_str, &period)) {
    return cntx->SendError(kInvalidIntErr);
  }

  // Apply quantity of tokens now
  uint64_t quantity = 1;
  if (args.size() > 4) {
    const string_view quantity_str = ArgS(args, 4);

    if (!absl::SimpleAtoi(quantity_str, &quantity)) {
      return cntx->SendError(kInvalidIntErr);
    }
  }

  if (max_burst > INT64_MAX - 1) {
    return cntx->SendError(kInvalidIntErr);
  }
  const int64_t limit = max_burst + 1;

  if (period > UINT64_MAX / 1000 || count == 0 || period * 1000 / count > INT64_MAX) {
    return cntx->SendError(kInvalidIntErr);
  }
  const int64_t emission_interval_ms = period * 1000 / count;

  if (emission_interval_ms == 0) {
    return cntx->SendError("zero rates are not supported");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<array<int64_t, 5>> {
    return OpThrottle(t->GetOpArgs(shard), key, limit, emission_interval_ms, quantity);
  };

  Transaction* trans = cntx->transaction;
  OpResult<array<int64_t, 5>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
    rb->StartArray(result->size());
    auto& array = result.value();

    int64_t retry_after_s = array[3] / 1000;
    if (array[3] > 0) {
      retry_after_s += 1;
    }
    array[3] = retry_after_s;

    int64_t reset_after_s = array[4] / 1000;
    if (array[4] > 0) {
      reset_after_s += 1;
    }
    array[4] = reset_after_s;

    for (const auto& v : array) {
      rb->SendLong(v);
    }
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        cntx->SendError(kWrongTypeErr);
        break;
      case OpStatus::INVALID_INT:
      case OpStatus::INVALID_VALUE:
        cntx->SendError(kInvalidIntErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        cntx->SendError(kOutOfMemory);
        break;
      default:
        cntx->SendError(result.status());
        break;
    }
  }
}

void StringFamily::Init(util::ProactorPool* pp) {
}

void StringFamily::Shutdown() {
}

#define HFUNC(x) SetHandler(&StringFamily::x)

namespace acl {
constexpr uint32_t kSet = WRITE | STRING | SLOW;
constexpr uint32_t kSetEx = WRITE | STRING | SLOW;
constexpr uint32_t kPSetEx = WRITE | STRING | SLOW;
constexpr uint32_t kSetNx = WRITE | STRING | FAST;
constexpr uint32_t kAppend = WRITE | STRING | FAST;
constexpr uint32_t kPrepend = WRITE | STRING | FAST;
constexpr uint32_t kIncr = WRITE | STRING | FAST;
constexpr uint32_t kDecr = WRITE | STRING | FAST;
constexpr uint32_t kIncrBy = WRITE | STRING | FAST;
constexpr uint32_t kIncrByFloat = WRITE | STRING | FAST;
constexpr uint32_t kDecrBy = WRITE | STRING | FAST;
constexpr uint32_t kGet = READ | STRING | FAST;
constexpr uint32_t kGetDel = WRITE | STRING | FAST;
constexpr uint32_t kGetEx = WRITE | STRING | FAST;
constexpr uint32_t kGetSet = WRITE | STRING | FAST;
constexpr uint32_t kMGet = READ | STRING | FAST;
constexpr uint32_t kMSet = WRITE | STRING | SLOW;
constexpr uint32_t kMSetNx = WRITE | STRING | SLOW;
constexpr uint32_t kStrLen = READ | STRING | FAST;
constexpr uint32_t kGetRange = READ | STRING | SLOW;
constexpr uint32_t kSubStr = READ | STRING | SLOW;
constexpr uint32_t kSetRange = WRITE | STRING | SLOW;
// ClThrottle is a module in redis. Therefore we introduce a new extension
// to the category. We should consider other defaults as well
constexpr uint32_t kClThrottle = THROTTLE;
}  // namespace acl

void StringFamily::Register(CommandRegistry* registry) {
  constexpr uint32_t kMSetMask =
      CO::WRITE | CO::DENYOOM | CO::INTERLEAVED_KEYS | CO::NO_AUTOJOURNAL;

  registry->StartFamily();
  *registry
      << CI{"SET", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, -3, 1, 1, acl::kSet}.HFUNC(Set)
      << CI{"SETEX", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, 4, 1, 1, acl::kSetEx}.HFUNC(
             SetEx)
      << CI{"PSETEX", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, 4, 1, 1, acl::kPSetEx}.HFUNC(
             PSetEx)
      << CI{"SETNX", CO::WRITE | CO::DENYOOM, 3, 1, 1, acl::kSetNx}.HFUNC(SetNx)
      << CI{"APPEND", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::kAppend}.HFUNC(Append)
      << CI{"PREPEND", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::kPrepend}.HFUNC(Prepend)
      << CI{"INCR", CO::WRITE | CO::FAST, 2, 1, 1, acl::kIncr}.HFUNC(Incr)
      << CI{"DECR", CO::WRITE | CO::FAST, 2, 1, 1, acl::kDecr}.HFUNC(Decr)
      << CI{"INCRBY", CO::WRITE | CO::FAST, 3, 1, 1, acl::kIncrBy}.HFUNC(IncrBy)
      << CI{"INCRBYFLOAT", CO::WRITE | CO::FAST, 3, 1, 1, acl::kIncrByFloat}.HFUNC(IncrByFloat)
      << CI{"DECRBY", CO::WRITE | CO::FAST, 3, 1, 1, acl::kDecrBy}.HFUNC(DecrBy)
      << CI{"GET", CO::READONLY | CO::FAST, 2, 1, 1, acl::kGet}.HFUNC(Get)
      << CI{"GETDEL", CO::WRITE | CO::FAST, 2, 1, 1, acl::kGetDel}.HFUNC(GetDel)
      << CI{"GETEX", CO::WRITE | CO::DENYOOM | CO::FAST | CO::NO_AUTOJOURNAL, -1, 1, 1, acl::kGetEx}
             .HFUNC(GetEx)
      << CI{"GETSET", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, acl::kGetSet}.HFUNC(GetSet)
      << CI{"MGET",    CO::READONLY | CO::FAST | CO::REVERSE_MAPPING | CO::IDEMPOTENT, -2, 1, -1,
            acl::kMGet}
             .HFUNC(MGet)
      << CI{"MSET", kMSetMask, -3, 1, -1, acl::kMSet}.HFUNC(MSet)
      << CI{"MSETNX", kMSetMask, -3, 1, -1, acl::kMSetNx}.HFUNC(MSetNx)
      << CI{"STRLEN", CO::READONLY | CO::FAST, 2, 1, 1, acl::kStrLen}.HFUNC(StrLen)
      << CI{"GETRANGE", CO::READONLY | CO::FAST, 4, 1, 1, acl::kGetRange}.HFUNC(GetRange)
      << CI{"SUBSTR", CO::READONLY | CO::FAST, 4, 1, 1, acl::kSubStr}.HFUNC(
             GetRange)  // Alias for GetRange
      << CI{"SETRANGE", CO::WRITE | CO::FAST | CO::DENYOOM, 4, 1, 1, acl::kSetRange}.HFUNC(SetRange)
      << CI{"CL.THROTTLE", CO::WRITE | CO::DENYOOM | CO::FAST, -5, 1, 1, acl::kClThrottle}.HFUNC(
             ClThrottle);
}

}  // namespace dfly
