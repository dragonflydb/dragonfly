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
#include <variant>

#include "absl/strings/str_cat.h"
#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "core/overloaded.h"
#include "facade/cmd_arg_parser.h"
#include "facade/op_status.h"
#include "facade/reply_builder.h"
#include "redis/redis_aux.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/journal/journal.h"
#include "server/table.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "util/fibers/future.h"

namespace dfly {

namespace {

using namespace std;
using namespace facade;
using namespace util;

using CI = CommandId;

enum class ExpT { EX, PX, EXAT, PXAT };

constexpr uint32_t kMaxStrLen = 1 << 28;

// Stores a string, the pending result of a tiered read or nothing
struct StringValue {
  StringValue() : v_{} {
  }
  StringValue(std::string s) : v_{std::move(s)} {
  }
  StringValue(fb2::Future<std::string> f) : v_{std::move(f)} {
  }

  // Get and consume value. If backed by a future, blocks until resolved.
  std::string Get() &&;

  // If no value is stored
  bool IsEmpty() const;

  // Read string from prime value - either from memory or issue tiered storage read
  static StringValue Read(DbIndex dbid, std::string_view key, const PrimeValue& pv,
                          EngineShard* es);

  bool IsFuturized() const {
    return std::holds_alternative<fb2::Future<std::string>>(v_);
  }

 private:
  std::variant<std::monostate, std::string, fb2::Future<std::string>> v_;
};

// Helper for performing SET operations with various options
class SetCmd {
 public:
  explicit SetCmd(OpArgs op_args, bool manual_journal)
      : op_args_(op_args), manual_journal_{manual_journal} {
  }

  enum SetFlags {
    SET_ALWAYS = 0,
    SET_IF_NOTEXIST = 1 << 0,     /* NX: Set if key not exists. */
    SET_IF_EXISTS = 1 << 1,       /* XX: Set if key exists. */
    SET_KEEP_EXPIRE = 1 << 2,     /* KEEPTTL: Set and keep the ttl */
    SET_GET = 1 << 3,             /* GET: Set if want to get key before set */
    SET_EXPIRE_AFTER_MS = 1 << 4, /* EX,PX,EXAT,PXAT: Expire after ms. */
    SET_STICK = 1 << 5,           /* Set STICK flag */
  };

  struct SetParams {
    uint16_t flags = SET_ALWAYS;
    uint32_t memcache_flags = 0;
    uint64_t expire_after_ms = 0;     // Relative value based on now. 0 means no expiration.
    StringValue* prev_val = nullptr;  // If set, previous value is stored at pointer

    constexpr bool IsConditionalSet() const {
      return flags & SET_IF_NOTEXIST || flags & SET_IF_EXISTS;
    }
  };

  OpStatus Set(const SetParams& params, std::string_view key, std::string_view value);

 private:
  OpStatus SetExisting(const SetParams& params, DbSlice::Iterator it, DbSlice::ExpIterator e_it,
                       std::string_view key, std::string_view value);

  void AddNew(const SetParams& params, DbSlice::Iterator it, DbSlice::ExpIterator e_it,
              std::string_view key, std::string_view value);

  // Called at the end of AddNew of SetExisting
  void PostEdit(const SetParams& params, std::string_view key, std::string_view value,
                PrimeValue* pv);

  void RecordJournal(const SetParams& params, std::string_view key, std::string_view value);

  OpStatus CachePrevIfNeeded(const SetParams& params, DbSlice::Iterator it);

  const OpArgs op_args_;
  bool manual_journal_;
};

void CopyValueToBuffer(const PrimeValue& pv, char* dest) {
  DCHECK_EQ(pv.ObjType(), OBJ_STRING);
  DCHECK(!pv.IsExternal());
  pv.GetString(dest);
}

string GetString(const PrimeValue& pv) {
  string res;
  DCHECK_EQ(pv.ObjType(), OBJ_STRING);

  if (pv.ObjType() != OBJ_STRING)
    return res;
  res.resize(pv.Size());
  CopyValueToBuffer(pv, res.data());

  return res;
}

size_t SetRange(std::string* value, size_t start, std::string_view range) {
  value->resize(max(value->size(), start + range.size()));
  memcpy(value->data() + start, range.data(), range.size());
  return value->size();
}

template <typename T> using TResult = std::variant<T, fb2::Future<T>>;

template <typename T> T GetResult(TResult<T> v) {
  Overloaded ov{
      [](T&& t) { return t; },
      [](fb2::Future<T>&& future) { return future.Get(); },
  };
  return std::visit(ov, std::move(v));
}

OpResult<TResult<size_t>> OpStrLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STRING);
  if (it_res == OpStatus::KEY_NOTFOUND) {
    return TResult<size_t>{0u};
  }
  RETURN_ON_BAD_STATUS(it_res);

  // For external entries we have to enqueue reads because modify operations like append could be
  // already pending.
  // TODO: Optimize to return co.Size() if no modify operations are present
  if (const auto& co = it_res.value()->second; co.IsExternal()) {
    fb2::Future<size_t> fut;
    op_args.shard->tiered_storage()->Read(
        op_args.db_cntx.db_index, key, co,
        [fut](const std::string& s) mutable { fut.Resolve(s.size()); });
    return {std::move(fut)};
  } else {
    return {co.Size()};
  }
}

OpResult<TResult<size_t>> OpSetRange(const OpArgs& op_args, string_view key, size_t start,
                                     string_view range) {
  VLOG(2) << "SetRange(" << key << ", " << start << ", " << range << ")";
  auto& db_slice = op_args.GetDbSlice();

  if (range.empty()) {
    return OpStrLen(op_args, key);
  }

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& res = *op_res;

  if (res.it->second.IsExternal()) {
    return {op_args.shard->tiered_storage()->Modify<size_t>(
        op_args.db_cntx.db_index, key, res.it->second,
        [start = start, range = string(range)](std::string* s) {
          return SetRange(s, start, range);
        })};
  } else {
    string value;
    if (!res.is_new && res.it->second.ObjType() != OBJ_STRING)
      return OpStatus::WRONG_TYPE;

    if (!res.is_new)
      value = GetString(res.it->second);

    size_t len = SetRange(&value, start, range);
    res.it->second.SetString(value);
    return {len};
  }
}

OpResult<StringValue> OpGetRange(const OpArgs& op_args, string_view key, int32_t start,
                                 int32_t end) {
  auto read = [start, end](std::string_view slice) mutable -> string_view {
    int32_t strlen = slice.size();
    if (strlen == 0)
      return "";

    if (start < 0) {
      if (end < start) {
        return "";
      }
      start = strlen + start;
      start = max(start, 0);
    }

    if (end < 0) {
      end = strlen + end;
      end = max(end, 0);
    } else {
      end = min(end, strlen - 1);
    }

    if (start > end) {
      return "";
    }

    return slice.substr(start, end - start + 1);
  };

  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STRING);
  if (it_res == OpStatus::KEY_NOTFOUND) {
    return StringValue(string{});
  }
  RETURN_ON_BAD_STATUS(it_res);

  if (const CompactObj& co = it_res.value()->second; co.IsExternal()) {
    fb2::Future<std::string> fut;
    op_args.shard->tiered_storage()->Read(
        op_args.db_cntx.db_index, key, co,
        [read, fut](const std::string& s) mutable { fut.Resolve(string{read(s)}); });
    return {std::move(fut)};
  } else {
    string tmp;
    string_view slice = co.GetSlice(&tmp);
    return {string{read(slice)}};
  }
};

size_t ExtendExisting(DbSlice::Iterator it, string_view key, string_view val, bool prepend) {
  string tmp, new_val;
  string_view slice = it->second.GetSlice(&tmp);

  if (prepend)
    new_val = absl::StrCat(val, slice);
  else
    new_val = absl::StrCat(slice, val);

  it->second.SetString(new_val);

  return new_val.size();
}

OpResult<bool> ExtendOrSkip(const OpArgs& op_args, string_view key, string_view val, bool prepend) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_STRING);
  if (!it_res) {
    return false;
  }

  return ExtendExisting(it_res->it, key, val, prepend);
}

OpResult<double> OpIncrFloat(const OpArgs& op_args, string_view key, double val) {
  auto& db_slice = op_args.GetDbSlice();

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
  auto& db_slice = op_args.GetDbSlice();

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

// Returns true if keys were set, false otherwise.
OpStatus OpMSet(const OpArgs& op_args, const ShardArgs& args) {
  DCHECK(!args.Empty() && args.Size() % 2 == 0);

  SetCmd::SetParams params;
  SetCmd sg(op_args, false);

  OpStatus result = OpStatus::OK;
  size_t stored = 0;
  for (auto it = args.begin(); it != args.end();) {
    string_view key = *(it++);
    string_view value = *(it++);
    if (auto status = sg.Set(params, key, value); status != OpStatus::OK) {
      result = status;
      break;
    }

    stored++;
  }

  // Above loop could have parial success (e.g. OOM), so replicate only what was
  // changed
  if (auto journal = op_args.shard->journal(); journal) {
    if (stored * 2 == args.Size()) {
      RecordJournal(op_args, "MSET", args, op_args.tx->GetUniqueShardCnt());
      DCHECK_EQ(result, OpStatus::OK);
    } else if (stored > 0) {
      vector<string_view> store_args(args.begin(), args.end());
      store_args.resize(stored * 2);
      RecordJournal(op_args, "MSET", store_args, op_args.tx->GetUniqueShardCnt());
    }
  }
  return result;
}

// emission_interval_ms assumed to be positive
// limit is assumed to be positive
OpResult<array<int64_t, 5>> OpThrottle(const OpArgs& op_args, const string_view key,
                                       const int64_t limit, const int64_t emission_interval_ms,
                                       const uint64_t quantity) {
  auto& db_slice = op_args.GetDbSlice();

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

struct GetResp {
  string key;  // TODO: to use backing storage to optimize this as well.
  string_view value;
  uint64_t mc_ver = 0;  // 0 means we do not output it (i.e has not been requested).
  uint32_t mc_flag = 0;
};

struct MGetResponse {
  explicit MGetResponse(size_t size = 0) : resp_arr(size) {
  }

  std::unique_ptr<char[]> storage;
  absl::InlinedVector<std::optional<GetResp>, 2> resp_arr;
};

// fetch_mask values
constexpr uint8_t FETCH_MCFLAG = 0x1;
constexpr uint8_t FETCH_MCVER = 0x2;
MGetResponse OpMGet(fb2::BlockingCounter wait_bc, uint8_t fetch_mask, const Transaction* t,
                    EngineShard* shard) {
  ShardArgs keys = t->GetShardArgs(shard->shard_id());
  DCHECK(!keys.Empty());

  auto& db_slice = t->GetDbSlice(shard->shard_id());

  MGetResponse response(keys.Size());
  struct Item {
    DbSlice::ConstIterator it;
    int source_index = -1;  // in case of duplicate keys, points to the first occurrence.
  };

  absl::InlinedVector<Item, 32> items(keys.Size());

  // We can not make it thread-local because we may preempt during the Find loop due to
  // serialization with the bumpup calls.

  // TODO: consider separating BumpUps from finds because it becomes too complicated
  // to reason about.
  absl::flat_hash_map<string_view, unsigned> key_index;

  // First, fetch all iterators and count total size ahead
  size_t total_size = 0;
  unsigned index = 0;
  key_index.reserve(keys.Size());

  for (string_view key : keys) {
    auto [it, inserted] = key_index.try_emplace(key, index);
    if (!inserted) {  // duplicate -> point to the first occurrence.
      items[index++].source_index = it->second;
      continue;
    }

    auto it_res = db_slice.FindReadOnly(t->GetDbContext(), key, OBJ_STRING);
    auto& dest = items[index++];
    if (it_res) {
      dest.it = *it_res;
      total_size += (*it_res)->second.Size();
    }
  }

  VLOG_IF(1, total_size > 10000000) << "OpMGet: allocating " << total_size << " bytes";

  // Allocate enough for all values
  response.storage = make_unique<char[]>(total_size);
  char* next = response.storage.get();
  bool fetch_mcflag = fetch_mask & FETCH_MCFLAG;
  bool fetch_mcver = fetch_mask & FETCH_MCVER;
  for (size_t i = 0; i < items.size(); ++i) {
    auto it = items[i].it;
    if (it.is_done()) {
      if (items[i].source_index >= 0) {
        response.resp_arr[i] = response.resp_arr[items[i].source_index];
      }
      continue;
    }
    auto& resp = response.resp_arr[i].emplace();

    // Copy to buffer or trigger tiered read that will eventually write to
    // buffer
    if (it->second.IsExternal()) {
      wait_bc->Add(1);
      auto cb = [next, wait_bc](const string& v) mutable {
        memcpy(next, v.data(), v.size());
        wait_bc->Dec();
      };
      shard->tiered_storage()->Read(t->GetDbIndex(), it.key(), it->second, std::move(cb));
    } else {
      CopyValueToBuffer(it->second, next);
    }

    size_t size = it->second.Size();
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
  key_index.clear();

  return response;
}

// Extend key with value, either prepend or append. Return size of stored string
// after modification
OpResult<TResult<size_t>> OpExtend(const OpArgs& op_args, std::string_view key,
                                   std::string_view value, bool prepend) {
  auto* shard = op_args.shard;
  auto it_res = op_args.GetDbSlice().AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(it_res);

  if (it_res->is_new) {
    it_res->it->second.SetString(value);
    return {it_res->it->second.Size()};
  }

  if (it_res->it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  if (const PrimeValue& pv = it_res->it->second; pv.IsExternal()) {
    auto modf = [value = string{value}, prepend](std::string* v) {
      *v = prepend ? absl::StrCat(value, *v) : absl::StrCat(*v, value);
      return v->size();
    };
    return {shard->tiered_storage()->Modify<size_t>(op_args.db_cntx.db_index, key, pv,
                                                    std::move(modf))};
  } else {
    return {ExtendExisting(it_res->it, key, value, prepend)};
  }
}

// Helper for building replies for strings
struct GetReplies {
  GetReplies(SinkReplyBuilder* rb) : rb{static_cast<RedisReplyBuilder*>(rb)} {
    DCHECK(dynamic_cast<RedisReplyBuilder*>(rb));
  }

  template <typename T> void Send(OpResult<T>&& res) const {
    switch (res.status()) {
      case OpStatus::OK:
        return Send(std::move(res.value()));
      case OpStatus::WRONG_TYPE:
        return rb->SendError(kWrongTypeErr);
      default:
        rb->SendNull();
    }
  }

  void Send(TResult<size_t>&& val) const {
    rb->SendLong(GetResult(std::move(val)));
  }

  void Send(StringValue&& val) const {
    if (val.IsEmpty()) {
      rb->SendNull();
    } else {
      rb->SendBulkString(std::move(val).Get());
    }
  }

  RedisReplyBuilder* rb;
};

void ExtendGeneric(CmdArgList args, bool prepend, Transaction* tx, SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);
  string_view value = ArgS(args, 1);
  VLOG(2) << "ExtendGeneric(" << key << ", " << value << ")";

  if (builder->GetProtocol() == Protocol::REDIS) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return OpExtend(t->GetOpArgs(shard), key, value, prepend);
    };

    RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(builder);
    auto res = tx->ScheduleSingleHopT(cb);
    if (!res)
      return rb->SendError(res.status());
    rb->SendLong(GetResult(std::move(res.value())));
  } else {
    // Memcached skips if key is missing
    DCHECK(builder->GetProtocol() == Protocol::MEMCACHE);

    auto cb = [&](Transaction* t, EngineShard* shard) {
      return ExtendOrSkip(t->GetOpArgs(shard), key, value, prepend);
    };

    OpResult<bool> result = tx->ScheduleSingleHopT(std::move(cb));

    if (result.value_or(false)) {
      return builder->SendStored();
    }

    builder->SendSetSkipped();
  }
}

// Wrapper to call SetCmd::Set in ScheduleSingleHop
OpStatus SetGeneric(const SetCmd::SetParams& sparams, string_view key, string_view value,
                    bool manual_journal, Transaction* tx) {
  DCHECK(tx);

  return tx->ScheduleSingleHop([&](Transaction* t, EngineShard* shard) {
    return SetCmd(t->GetOpArgs(shard), manual_journal).Set(sparams, key, value);
  });
}

/// (P)SETEX key seconds value
void SetExGeneric(bool seconds, CmdArgList args, const CommandId* cid, Transaction* tx,
                  SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);
  string_view ex = ArgS(args, 1);
  string_view value = ArgS(args, 2);
  int64_t unit_vals;

  if (!absl::SimpleAtoi(ex, &unit_vals)) {
    return builder->SendError(kInvalidIntErr, kSyntaxErrType);
  }

  if (unit_vals < 1) {
    return builder->SendError(InvalidExpireTime(cid->name()));
  }

  DbSlice::ExpireParams expiry{
      .value = unit_vals,
      .unit = seconds ? TimeUnit::SEC : TimeUnit::MSEC,
      .absolute = false,
  };

  int64_t now_ms = GetCurrentTimeMs();
  auto [rel_ms, abs_ms] = expiry.Calculate(now_ms, false);
  if (abs_ms < 0)
    return builder->SendError(InvalidExpireTime("set"));

  SetCmd::SetParams sparams;
  sparams.flags |= SetCmd::SET_EXPIRE_AFTER_MS;
  sparams.expire_after_ms = expiry.Calculate(now_ms, true).first;
  bool manual_journal = cid->opt_mask() & CO::NO_AUTOJOURNAL;
  builder->SendError(SetGeneric(sparams, key, value, manual_journal, tx));
}

void IncrByGeneric(string_view key, int64_t val, Transaction* tx, SinkReplyBuilder* builder) {
  bool skip_on_missing = (builder->GetProtocol() == Protocol::MEMCACHE);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpResult<int64_t> res = OpIncrBy(t->GetOpArgs(shard), key, val, skip_on_missing);
    return res;
  };

  OpResult<int64_t> result = tx->ScheduleSingleHopT(std::move(cb));

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
      static_cast<MCReplyBuilder*>(builder)->SendNotFound();
      break;
    default:
      builder->SendError(result.status());
      break;
  }
}

}  // namespace

StringValue StringValue::Read(DbIndex dbid, string_view key, const PrimeValue& pv,
                              EngineShard* es) {
  return pv.IsExternal() ? StringValue{es->tiered_storage()->Read(dbid, key, pv)}
                         : StringValue(GetString(pv));
}

string StringValue::Get() && {
  DCHECK(!holds_alternative<monostate>(v_));

  auto prev = exchange(v_, monostate{});
  if (holds_alternative<string>(prev))
    return std::move(std::get<string>(prev));
  return std::get<fb2::Future<std::string>>(prev).Get();
}

bool StringValue::IsEmpty() const {
  return holds_alternative<monostate>(v_);
}

OpStatus SetCmd::Set(const SetParams& params, string_view key, string_view value) {
  auto& db_slice = op_args_.GetDbSlice();

  DCHECK(db_slice.IsDbValid(op_args_.db_cntx.db_index));
  VLOG(2) << "Set " << key << "(" << db_slice.shard_id() << ") ";

  if (params.IsConditionalSet()) {
    auto find_res = db_slice.FindMutable(op_args_.db_cntx, key);
    if (auto status = CachePrevIfNeeded(params, find_res.it); status != OpStatus::OK)
      return status;

    if (params.flags & SET_IF_EXISTS) {
      if (IsValid(find_res.it)) {
        return SetExisting(params, find_res.it, find_res.exp_it, key, value);
      } else {
        return OpStatus::SKIPPED;
      }
    } else {
      DCHECK(params.flags & SET_IF_NOTEXIST) << params.flags;
      if (IsValid(find_res.it)) {
        return OpStatus::SKIPPED;
      }  // else AddNew() is called below
    }
  }

  auto op_res = db_slice.AddOrFind(op_args_.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new) {
    if (auto status = CachePrevIfNeeded(params, op_res->it); status != OpStatus::OK)
      return status;

    return SetExisting(params, op_res->it, op_res->exp_it, key, value);
  } else {
    AddNew(params, op_res->it, op_res->exp_it, key, value);
    return OpStatus::OK;
  }
}

OpStatus SetCmd::SetExisting(const SetParams& params, DbSlice::Iterator it,
                             DbSlice::ExpIterator e_it, string_view key, string_view value) {
  DCHECK_EQ(params.flags & SET_IF_NOTEXIST, 0);

  PrimeValue& prime_value = it->second;
  EngineShard* shard = op_args_.shard;

  auto& db_slice = op_args_.GetDbSlice();
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

  // If value is external, mark it as deleted
  if (prime_value.IsExternal()) {
    shard->tiered_storage()->Delete(op_args_.db_cntx.db_index, &prime_value);
  }

  // overwrite existing entry.
  prime_value.SetString(value);

  PostEdit(params, key, value, &prime_value);
  return OpStatus::OK;
}

void SetCmd::AddNew(const SetParams& params, DbSlice::Iterator it, DbSlice::ExpIterator e_it,
                    std::string_view key, std::string_view value) {
  auto& db_slice = op_args_.GetDbSlice();

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

  PostEdit(params, key, value, &it->second);
}

void SetCmd::PostEdit(const SetParams& params, std::string_view key, std::string_view value,
                      PrimeValue* pv) {
  EngineShard* shard = op_args_.shard;

  // Currently we always try to offload, but Stash may ignore it, if disk I/O is overloaded.
  if (auto* ts = shard->tiered_storage(); ts)
    ts->TryStash(op_args_.db_cntx.db_index, key, pv);

  if (manual_journal_ && op_args_.shard->journal()) {
    RecordJournal(params, key, value);
  }
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

  // Skip NX/XX because SET operation was executed.
  // Skip GET, because its not important on replica.

  dfly::RecordJournal(op_args_, "SET", ArgSlice{cmds});
}

OpStatus SetCmd::CachePrevIfNeeded(const SetCmd::SetParams& params, DbSlice::Iterator it) {
  if (!params.prev_val || !IsValid(it))
    return OpStatus::OK;
  if (it->second.ObjType() != OBJ_STRING)
    return OpStatus::WRONG_TYPE;

  *params.prev_val =
      StringValue::Read(op_args_.db_cntx.db_index, it.key(), it->second, EngineShard::tlocal());
  return OpStatus::OK;
}

void StringFamily::Set(CmdArgList args, const CommandContext& cmnd_cntx) {
  facade::CmdArgParser parser{args};

  auto [key, value] = parser.Next<string_view, string_view>();

  SetCmd::SetParams sparams;
  sparams.memcache_flags = cmnd_cntx.conn_cntx->conn_state.memcache_flag;
  auto* builder = cmnd_cntx.rb;

  while (parser.HasNext()) {
    if (auto exp_type = parser.TryMapNext("EX", ExpT::EX, "PX", ExpT::PX, "EXAT", ExpT::EXAT,
                                          "PXAT", ExpT::PXAT);
        exp_type) {
      auto int_arg = parser.Next<int64_t>();

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

      DbSlice::ExpireParams expiry{
          .value = int_arg,
          .unit = *exp_type == ExpT::PX || *exp_type == ExpT::PXAT ? TimeUnit::MSEC : TimeUnit::SEC,
          .absolute = *exp_type == ExpT::EXAT || *exp_type == ExpT::PXAT,
      };

      int64_t now_ms = GetCurrentTimeMs();
      auto [rel_ms, abs_ms] = expiry.Calculate(now_ms, false);
      if (abs_ms < 0)
        return builder->SendError(InvalidExpireTime("set"));

      // Remove existed key if the key is expired already
      if (rel_ms < 0) {
        cmnd_cntx.tx->ScheduleSingleHop([](const Transaction* tx, EngineShard* es) {
          ShardArgs args = tx->GetShardArgs(es->shard_id());
          GenericFamily::OpDel(tx->GetOpArgs(es), args);
          return OpStatus::OK;
        });
        return builder->SendStored();
      }

      tie(sparams.expire_after_ms, ignore) = expiry.Calculate(now_ms, true);
    } else if (parser.Check("_MCFLAGS")) {
      sparams.memcache_flags = parser.Next<uint32_t>();
    } else {
      uint16_t flag = parser.MapNext(  //
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

  StringValue prev;
  if (sparams.flags & SetCmd::SET_GET)
    sparams.prev_val = &prev;
  bool manual_journal = cmnd_cntx.conn_cntx->cid->opt_mask() & CO::NO_AUTOJOURNAL;
  OpStatus result = SetGeneric(sparams, key, value, manual_journal, cmnd_cntx.tx);

  if (result == OpStatus::WRONG_TYPE) {
    return builder->SendError(kWrongTypeErr);
  }

  if (sparams.flags & SetCmd::SET_GET) {
    return GetReplies{cmnd_cntx.rb}.Send(std::move(prev));
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

void StringFamily::SetEx(CmdArgList args, const CommandContext& cmnd_cntx) {
  SetExGeneric(true, std::move(args), cmnd_cntx.conn_cntx->cid, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::PSetEx(CmdArgList args, const CommandContext& cmnd_cntx) {
  SetExGeneric(false, std::move(args), cmnd_cntx.conn_cntx->cid, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::SetNx(CmdArgList args, const CommandContext& cmnd_cntx) {
  // This is the same as calling the "Set" function, only in this case we are
  // change the value only if the key does not exist. Otherwise the function
  // will not modify it. in which case it would return 0
  // it would return to the caller 1 in case the key did not exists and was
  // added
  string_view key = ArgS(args, 0);
  string_view value = ArgS(args, 1);

  SetCmd::SetParams sparams;
  sparams.flags |= SetCmd::SET_IF_NOTEXIST;
  sparams.memcache_flags = cmnd_cntx.conn_cntx->conn_state.memcache_flag;
  bool manual_journal = cmnd_cntx.conn_cntx->cid->opt_mask() & CO::NO_AUTOJOURNAL;
  const auto results{SetGeneric(sparams, key, value, manual_journal, cmnd_cntx.tx)};
  auto* builder = cmnd_cntx.rb;
  if (results == OpStatus::OK) {
    return builder->SendLong(1);  // this means that we successfully set the value
  }
  if (results == OpStatus::OUT_OF_MEMORY) {
    return builder->SendError(kOutOfMemory);
  }

  CHECK_EQ(results, OpStatus::SKIPPED);  // in this case it must be skipped!
  return builder->SendLong(0);  // value do exists, we need to report that we didn't change it
}

void StringFamily::Get(CmdArgList args, const CommandContext& cmnd_cntx) {
  auto cb = [key = ArgS(args, 0)](Transaction* tx, EngineShard* es) -> OpResult<StringValue> {
    auto it_res = tx->GetDbSlice(es->shard_id()).FindReadOnly(tx->GetDbContext(), key, OBJ_STRING);
    if (!it_res.ok())
      return it_res.status();

    return StringValue::Read(tx->GetDbIndex(), key, (*it_res)->second, es);
  };

  GetReplies{cmnd_cntx.rb}.Send(cmnd_cntx.tx->ScheduleSingleHopT(cb));
}

void StringFamily::GetDel(CmdArgList args, const CommandContext& cmnd_cntx) {
  auto cb = [key = ArgS(args, 0)](Transaction* tx, EngineShard* es) -> OpResult<StringValue> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.FindMutable(tx->GetDbContext(), key, OBJ_STRING);
    if (!it_res.ok())
      return it_res.status();

    auto value = StringValue::Read(tx->GetDbIndex(), key, it_res->it->second, es);
    it_res->post_updater.Run();  // Run manually before delete
    db_slice.Del(tx->GetDbContext(), it_res->it);
    return value;
  };

  GetReplies{cmnd_cntx.rb}.Send(cmnd_cntx.tx->ScheduleSingleHopT(cb));
}

void StringFamily::GetSet(CmdArgList args, const CommandContext& cmnd_cntx) {
  string_view key = ArgS(args, 0);
  string_view value = ArgS(args, 1);

  StringValue prev;
  SetCmd::SetParams sparams;
  sparams.prev_val = &prev;
  bool manual_journal = cmnd_cntx.conn_cntx->cid->opt_mask() & CO::NO_AUTOJOURNAL;
  if (OpStatus status = SetGeneric(sparams, key, value, manual_journal, cmnd_cntx.tx);
      status != OpStatus::OK) {
    auto* builder = cmnd_cntx.rb;
    return builder->SendError(status);
  }

  GetReplies{cmnd_cntx.rb}.Send(std::move(prev));
}

void StringFamily::Append(CmdArgList args, const CommandContext& cmnd_cntx) {
  ExtendGeneric(args, false, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::Prepend(CmdArgList args, const CommandContext& cmnd_cntx) {
  ExtendGeneric(args, true, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::GetEx(CmdArgList args, const CommandContext& cmnd_cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();

  DbSlice::ExpireParams exp_params;
  bool defined = false;
  auto* builder = cmnd_cntx.rb;
  while (parser.HasNext()) {
    if (auto exp_type = parser.TryMapNext("EX", ExpT::EX, "PX", ExpT::PX, "EXAT", ExpT::EXAT,
                                          "PXAT", ExpT::PXAT);
        exp_type) {
      auto int_arg = parser.Next<int64_t>();
      if (auto err = parser.Error(); err) {
        return builder->SendError(err->MakeReply());
      }

      if (defined) {
        return builder->SendError(kSyntaxErr, kSyntaxErrType);
      }

      if (int_arg <= 0) {
        return builder->SendError(InvalidExpireTime("getex"));
      }

      exp_params.absolute = *exp_type == ExpT::EXAT || *exp_type == ExpT::PXAT;
      exp_params.value = int_arg;
      exp_params.unit =
          *exp_type == ExpT::PX || *exp_type == ExpT::PXAT ? TimeUnit::MSEC : TimeUnit::SEC;
      defined = true;
    } else if (parser.Check("PERSIST")) {
      exp_params.persist = true;
    } else {
      return builder->SendError(kSyntaxErr);
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<StringValue> {
    auto op_args = t->GetOpArgs(shard);

    auto it_res = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_STRING);
    if (!it_res)
      return it_res.status();

    StringValue value = StringValue::Read(t->GetDbIndex(), key, it_res->it->second, shard);

    if (exp_params.IsDefined()) {
      it_res->post_updater.Run();  // Run manually before possible delete due to negative expire
      RETURN_ON_BAD_STATUS(op_args.GetDbSlice().UpdateExpire(op_args.db_cntx, it_res->it,
                                                             it_res->exp_it, exp_params));
    }

    // Replicate GETEX as PEXPIREAT or PERSIST
    if (shard->journal()) {
      if (exp_params.persist) {
        RecordJournal(op_args, "PERSIST", {key});
      } else {
        auto [ignore, abs_time] = exp_params.Calculate(op_args.db_cntx.time_now_ms, false);
        auto abs_time_str = absl::StrCat(abs_time);
        RecordJournal(op_args, "PEXPIREAT", {key, abs_time_str});
      }
    }

    return value;
  };

  GetReplies{cmnd_cntx.rb}.Send(cmnd_cntx.tx->ScheduleSingleHopT(cb));
}

void StringFamily::Incr(CmdArgList args, const CommandContext& cmnd_cntx) {
  string_view key = ArgS(args, 0);
  return IncrByGeneric(key, 1, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::IncrBy(CmdArgList args, const CommandContext& cmnd_cntx) {
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }
  return IncrByGeneric(key, val, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::IncrByFloat(CmdArgList args, const CommandContext& cmnd_cntx) {
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  double val;

  if (!absl::SimpleAtod(sval, &val)) {
    return cmnd_cntx.rb->SendError(kInvalidFloatErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrFloat(t->GetOpArgs(shard), key, val);
  };

  OpResult<double> result = cmnd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmnd_cntx.rb);

  DVLOG(2) << "IncrByGeneric " << key << "/" << result.value();
  if (!result) {
    return rb->SendError(result.status());
  }

  rb->SendDouble(result.value());
}

void StringFamily::Decr(CmdArgList args, const CommandContext& cmnd_cntx) {
  string_view key = ArgS(args, 0);
  return IncrByGeneric(key, -1, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::DecrBy(CmdArgList args, const CommandContext& cmnd_cntx) {
  string_view key = ArgS(args, 0);
  string_view sval = ArgS(args, 1);
  int64_t val;

  if (!absl::SimpleAtoi(sval, &val)) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }
  if (val == INT64_MIN) {
    return cmnd_cntx.rb->SendError(kIncrOverflow);
  }

  return IncrByGeneric(key, -val, cmnd_cntx.tx, cmnd_cntx.rb);
}

void StringFamily::MGet(CmdArgList args, const CommandContext& cmnd_cntx) {
  DCHECK_GE(args.size(), 1U);

  uint8_t fetch_mask = 0;
  auto* builder = cmnd_cntx.rb;
  if (builder->GetProtocol() == Protocol::MEMCACHE) {
    fetch_mask |= FETCH_MCFLAG;
    if (cmnd_cntx.conn_cntx->conn_state.memcache_flag & ConnectionState::FETCH_CAS_VER)
      fetch_mask |= FETCH_MCVER;
  }

  // Count of pending tiered reads
  fb2::BlockingCounter tiering_bc{0};
  std::vector<MGetResponse> mget_resp(shard_set->size());
  auto cb = [&](Transaction* t, EngineShard* shard) {
    mget_resp[shard->shard_id()] = OpMGet(tiering_bc, fetch_mask, t, shard);
    return OpStatus::OK;
  };

  OpStatus result = cmnd_cntx.tx->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, result);

  // wait for all tiered reads to finish
  tiering_bc->Wait();

  // reorder shard results back according to argument order
  absl::FixedArray<optional<GetResp>, 8> res(args.size());

  for (ShardId sid = 0; sid < mget_resp.size(); ++sid) {
    if (!cmnd_cntx.tx->IsActive(sid))
      continue;

    auto& src = mget_resp[sid];
    ShardArgs shard_args = cmnd_cntx.tx->GetShardArgs(sid);
    unsigned src_indx = 0;
    for (auto it = shard_args.begin(); it != shard_args.end(); ++it, ++src_indx) {
      if (!src.resp_arr[src_indx])
        continue;

      uint32_t indx = it.index();

      res[indx] = std::move(src.resp_arr[src_indx]);
      if (builder->GetProtocol() == Protocol::MEMCACHE) {
        res[indx]->key = *it;
      }
    }
  }

  // The code below is safe in the context of squashing (uses CapturingReplyBuilder).
  // Specifically:
  // 1. For Memcache:
  //    builder != CapturingReplyBuilder here because this is only used in squashing
  //    and there are only two cases:
  //    * Squashing the pipeline something that is turned off when using MEMCACHE.
  //    * Squashing a multi/exec block. There exist no such command in MEMCACHE.
  //    Therefore this path is safe, and the DCHECK in the if statement below shall
  //    never trigger.
  // 2. For Redis:
  //    * Call to StartArray() is safe because it calls RedisReplyBuilder::StartCollection which
  //      calls CapturingReplyBuilder::StartCollection
  //    * Calls to SendBulkString() and SendNull() find and if builder is CapturingReplyBuilder
  //      then the right member gets called.
  //
  // Finally, the ReplyScope will trigger a Flush() on scope's end. What that means is,
  // for CapturingReplyBuilder the internal vec is empty and therefore we should skip the call
  // to Send because sink_ is nullptr and there is no payload to Send since it was captured.
  SinkReplyBuilder::ReplyScope scope(builder);
  if (builder->GetProtocol() == Protocol::MEMCACHE) {
    auto* rb = static_cast<MCReplyBuilder*>(builder);
    DCHECK(dynamic_cast<CapturingReplyBuilder*>(builder) == nullptr);
    for (const auto& entry : res) {
      if (entry) {
        rb->SendValue(entry->key, entry->value, entry->mc_ver, entry->mc_flag);
      } else {
        rb->SendMiss();
      }
    }
    rb->SendGetEnd();
  } else {
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    rb->StartArray(res.size());
    for (const auto& entry : res) {
      if (entry)
        rb->SendBulkString(entry->value);
      else
        rb->SendNull();
    }
  }
}

void StringFamily::MSet(CmdArgList args, const CommandContext& cmnd_cntx) {
  if (VLOG_IS_ON(2)) {
    string str;
    for (size_t i = 1; i < args.size(); ++i) {
      absl::StrAppend(&str, " ", ArgS(args, i));
    }
    LOG(INFO) << "MSET/" << cmnd_cntx.tx->GetUniqueShardCnt() << str;
  }

  AggregateStatus result;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardArgs args = t->GetShardArgs(shard->shard_id());
    if (auto status = OpMSet(t->GetOpArgs(shard), args); status != OpStatus::OK)
      result = status;
    return OpStatus::OK;
  };

  if (auto status = cmnd_cntx.tx->ScheduleSingleHop(std::move(cb)); status != OpStatus::OK)
    result = status;

  if (*result == OpStatus::OK) {
    cmnd_cntx.rb->SendOk();
  } else {
    cmnd_cntx.rb->SendError(*result);
  }
}

void StringFamily::MSetNx(CmdArgList args, const CommandContext& cmnd_cntx) {
  atomic_bool exists{false};

  auto cb = [&](Transaction* t, EngineShard* es) {
    auto sid = es->shard_id();
    auto args = t->GetShardArgs(sid);
    auto op_args = t->GetOpArgs(es);
    for (auto arg_it = args.begin(); arg_it != args.end(); ++arg_it) {
      auto it = op_args.GetDbSlice().FindReadOnly(t->GetDbContext(), *arg_it).it;
      ++arg_it;
      if (IsValid(it)) {
        exists.store(true, memory_order_relaxed);
        break;
      }
    }

    return OpStatus::OK;
  };

  cmnd_cntx.tx->Execute(std::move(cb), false);
  const bool to_skip = exists.load(memory_order_relaxed);

  AggregateStatus result;
  auto epilog_cb = [&](Transaction* t, EngineShard* shard) {
    if (to_skip)
      return OpStatus::OK;

    auto args = t->GetShardArgs(shard->shard_id());
    if (auto status = OpMSet(t->GetOpArgs(shard), args); status != OpStatus::OK)
      result = status;
    return OpStatus::OK;
  };
  cmnd_cntx.tx->Execute(std::move(epilog_cb), true);

  cmnd_cntx.rb->SendLong(to_skip || (*result != OpStatus::OK) ? 0 : 1);
}

void StringFamily::StrLen(CmdArgList args, const CommandContext& cmnd_cntx) {
  auto cb = [key = ArgS(args, 0)](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key);
  };
  GetReplies{cmnd_cntx.rb}.Send(cmnd_cntx.tx->ScheduleSingleHopT(cb));
}

void StringFamily::GetRange(CmdArgList args, const CommandContext& cmnd_cntx) {
  CmdArgParser parser(args);
  auto [key, start, end] = parser.Next<string_view, int32_t, int32_t>();

  if (auto err = parser.Error(); err) {
    return cmnd_cntx.rb->SendError(err->MakeReply());
  }

  auto cb = [&, &key = key, &start = start, &end = end](Transaction* t, EngineShard* shard) {
    return OpGetRange(t->GetOpArgs(shard), key, start, end);
  };

  GetReplies{cmnd_cntx.rb}.Send(cmnd_cntx.tx->ScheduleSingleHopT(cb));
}

void StringFamily::SetRange(CmdArgList args, const CommandContext& cmnd_cntx) {
  CmdArgParser parser(args);
  auto [key, start, value] = parser.Next<string_view, int32_t, string_view>();
  auto* builder = cmnd_cntx.rb;

  if (auto err = parser.Error(); err) {
    return builder->SendError(err->MakeReply());
  }

  if (start < 0) {
    return builder->SendError("offset is out of range");
  }

  if (size_t min_size = start + value.size(); min_size > kMaxStrLen) {
    return builder->SendError("string exceeds maximum allowed size");
  }

  auto cb = [&, &key = key, &start = start, &value = value](Transaction* t, EngineShard* shard) {
    return OpSetRange(t->GetOpArgs(shard), key, start, value);
  };
  auto res = cmnd_cntx.tx->ScheduleSingleHopT(cb);

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (res.ok())
    rb->SendLong(GetResult(std::move(*res)));
  else
    rb->SendError(res.status());
}

/* CL.THROTTLE <key> <max_burst> <count per period> <period> [<quantity>] */
/* Response is array of 5 integers. The meaning of each array item is:
 *  1. Whether the action was limited:
 *   - 0 indicates the action is allowed.
 *   - 1 indicates that the action was limited/blocked.
 *  2. The total limit of the key (max_burst + 1). This is equivalent to the
 * common X-RateLimit-Limit HTTP header.
 *  3. The remaining limit of the key. Equivalent to X-RateLimit-Remaining.
 *  4. The number of seconds until the user should retry, and always -1 if the
 * action was allowed. Equivalent to Retry-After.
 *  5. The number of seconds until the limit will reset to its maximum capacity.
 * Equivalent to X-RateLimit-Reset.
 */
void StringFamily::ClThrottle(CmdArgList args, const CommandContext& cmnd_cntx) {
  const string_view key = ArgS(args, 0);

  // Allow max burst in number of tokens
  uint64_t max_burst;
  const string_view max_burst_str = ArgS(args, 1);
  if (!absl::SimpleAtoi(max_burst_str, &max_burst)) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }

  // Emit count of tokens per period
  uint64_t count;
  const string_view count_str = ArgS(args, 2);
  if (!absl::SimpleAtoi(count_str, &count)) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }

  // Period of emitting count of tokens
  uint64_t period;
  const string_view period_str = ArgS(args, 3);
  if (!absl::SimpleAtoi(period_str, &period)) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }

  // Apply quantity of tokens now
  uint64_t quantity = 1;
  if (args.size() > 4) {
    const string_view quantity_str = ArgS(args, 4);

    if (!absl::SimpleAtoi(quantity_str, &quantity)) {
      return cmnd_cntx.rb->SendError(kInvalidIntErr);
    }
  }

  if (max_burst > INT64_MAX - 1) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }
  const int64_t limit = max_burst + 1;

  if (period > UINT64_MAX / 1000 || count == 0 || period * 1000 / count > INT64_MAX) {
    return cmnd_cntx.rb->SendError(kInvalidIntErr);
  }
  const int64_t emission_interval_ms = period * 1000 / count;

  if (emission_interval_ms == 0) {
    return cmnd_cntx.rb->SendError("zero rates are not supported");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<array<int64_t, 5>> {
    return OpThrottle(t->GetOpArgs(shard), key, limit, emission_interval_ms, quantity);
  };

  Transaction* trans = cmnd_cntx.tx;
  OpResult<array<int64_t, 5>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    RedisReplyBuilder* redis_builder = static_cast<RedisReplyBuilder*>(cmnd_cntx.rb);
    redis_builder->StartArray(result->size());
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
      redis_builder->SendLong(v);
    }
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        cmnd_cntx.rb->SendError(kWrongTypeErr);
        break;
      case OpStatus::INVALID_INT:
      case OpStatus::INVALID_VALUE:
        cmnd_cntx.rb->SendError(kInvalidIntErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        cmnd_cntx.rb->SendError(kOutOfMemory);
        break;
      default:
        cmnd_cntx.rb->SendError(result.status());
        break;
    }
  }
}

#define HFUNC(x) SetHandler(&StringFamily::x)

void StringFamily::Register(CommandRegistry* registry) {
  constexpr uint32_t kMSetMask =
      CO::WRITE | CO::DENYOOM | CO::INTERLEAVED_KEYS | CO::NO_AUTOJOURNAL;

  registry->StartFamily(acl::STRING);
  *registry << CI{"SET", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, -3, 1, 1}.HFUNC(Set)
            << CI{"SETEX", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, 4, 1, 1}.HFUNC(SetEx)
            << CI{"PSETEX", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, 4, 1, 1}.HFUNC(PSetEx)
            << CI{"SETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(SetNx)
            << CI{"APPEND", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(Append)
            << CI{"PREPEND", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(Prepend)
            << CI{"INCR", CO::WRITE | CO::FAST, 2, 1, 1}.HFUNC(Incr)
            << CI{"DECR", CO::WRITE | CO::FAST, 2, 1, 1}.HFUNC(Decr)
            << CI{"INCRBY", CO::WRITE | CO::FAST, 3, 1, 1}.HFUNC(IncrBy)
            << CI{"INCRBYFLOAT", CO::WRITE | CO::FAST, 3, 1, 1}.HFUNC(IncrByFloat)
            << CI{"DECRBY", CO::WRITE | CO::FAST, 3, 1, 1}.HFUNC(DecrBy)
            << CI{"GET", CO::READONLY | CO::FAST, 2, 1, 1}.HFUNC(Get)
            << CI{"GETDEL", CO::WRITE | CO::FAST, 2, 1, 1}.HFUNC(GetDel)
            << CI{"GETEX", CO::WRITE | CO::DENYOOM | CO::FAST | CO::NO_AUTOJOURNAL, -2, 1, 1}.HFUNC(
                   GetEx)
            << CI{"GETSET", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(GetSet)
            << CI{"MGET", CO::READONLY | CO::FAST | CO::IDEMPOTENT, -2, 1, -1}.HFUNC(MGet)
            << CI{"MSET", kMSetMask, -3, 1, -1}.HFUNC(MSet)
            << CI{"MSETNX", kMSetMask, -3, 1, -1}.HFUNC(MSetNx)
            << CI{"STRLEN", CO::READONLY | CO::FAST, 2, 1, 1}.HFUNC(StrLen)
            << CI{"GETRANGE", CO::READONLY, 4, 1, 1}.HFUNC(GetRange)
            << CI{"SUBSTR", CO::READONLY, 4, 1, 1}.HFUNC(GetRange)  // Alias for GetRange
            << CI{"SETRANGE", CO::WRITE | CO::DENYOOM, 4, 1, 1}.HFUNC(SetRange)
            << CI{"CL.THROTTLE", CO::WRITE | CO::DENYOOM | CO::FAST, -5, 1, 1, acl::THROTTLE}.HFUNC(
                   ClThrottle);
}

}  // namespace dfly
