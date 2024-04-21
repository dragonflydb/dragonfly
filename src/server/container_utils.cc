// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/container_utils.h"

#include "base/flags.h"
#include "base/logging.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "src/facade/op_status.h"

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zset.h"
}

ABSL_FLAG(bool, singlehop_blocking, true, "Use single hop optimization for blocking commands");

namespace dfly::container_utils {

namespace {

struct ShardFFResult {
  PrimeKey key;
  ShardId sid = kInvalidSid;
};

// Find first non-empty key of a single shard transaction, pass it to `func` and return the key.
// If no such key exists or a wrong type is found, the apropriate status is returned.
// Optimized version of `FindFirstNonEmpty` below.
OpResult<std::string> FindFirstNonEmptySingleShard(Transaction* trans, int req_obj_type,
                                                   BlockingResultCb func) {
  DCHECK_EQ(trans->GetUniqueShardCnt(), 1u);
  std::string key;
  auto cb = [&](Transaction* t, EngineShard* shard) -> Transaction::RunnableResult {
    auto args = t->GetShardArgs(shard->shard_id());
    auto ff_res = shard->db_slice().FindFirstReadOnly(t->GetDbContext(), args, req_obj_type);

    if (ff_res == OpStatus::WRONG_TYPE)
      return OpStatus::WRONG_TYPE;

    if (ff_res == OpStatus::KEY_NOTFOUND)
      return {OpStatus::KEY_NOTFOUND, Transaction::RunnableResult::AVOID_CONCLUDING};

    CHECK(ff_res.ok());  // No other errors possible
    ff_res->first->first.GetString(&key);
    func(t, shard, key);
    return OpStatus::OK;
  };

  // Schedule single hop and hopefully find a key, otherwise avoid concluding
  OpStatus status = trans->ScheduleSingleHop(cb);
  if (status == OpStatus::OK)
    return key;
  return status;
}

// Find first non-empty key (sorted by order in command arguments) and return it,
// otherwise return not found or wrong type error.
OpResult<ShardFFResult> FindFirstNonEmpty(Transaction* trans, int req_obj_type) {
  DCHECK_GT(trans->GetUniqueShardCnt(), 1u);

  using FFResult = std::tuple<PrimeKey, unsigned, ShardId>;  // key, argument index, sid
  VLOG(2) << "FindFirst::Find " << trans->DebugId();

  // Holds Find results: (iterator to a found key, and its index in the passed arguments).
  // See DbSlice::FindFirst for more details.
  std::vector<OpResult<FFResult>> find_res(shard_set->size());
  std::fill(find_res.begin(), find_res.end(), OpStatus::KEY_NOTFOUND);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    auto ff_res = shard->db_slice().FindFirstReadOnly(t->GetDbContext(), args, req_obj_type);
    if (ff_res) {
      find_res[shard->shard_id()] =
          FFResult{ff_res->first->first.AsRef(), ff_res->second, shard->shard_id()};
    } else {
      find_res[shard->shard_id()] = ff_res.status();
    }
    return OpStatus::OK;
  };

  trans->Execute(std::move(cb), false);

  // If any key is of the wrong type, report it immediately
  if (std::find(find_res.begin(), find_res.end(), OpStatus::WRONG_TYPE) != find_res.end())
    return OpStatus::WRONG_TYPE;

  // Order result by their keys position in the command arguments, push errors to back
  auto comp = [trans](const OpResult<FFResult>& lhs, const OpResult<FFResult>& rhs) {
    if (!lhs || !rhs)
      return lhs.ok();
    size_t i1 = trans->ReverseArgIndex(std::get<ShardId>(*lhs), std::get<unsigned>(*lhs));
    size_t i2 = trans->ReverseArgIndex(std::get<ShardId>(*rhs), std::get<unsigned>(*rhs));
    return i1 < i2;
  };

  // Find first element by the order above, so the first key. Returns error only if all are errors
  auto it = std::min_element(find_res.begin(), find_res.end(), comp);
  DCHECK(it != find_res.end());

  if (*it == OpStatus::KEY_NOTFOUND)
    return OpStatus::KEY_NOTFOUND;

  CHECK(it->ok());  // No other errors than WRONG_TYPE and KEY_NOTFOUND
  FFResult& res = **it;
  return ShardFFResult{std::get<PrimeKey>(res).AsRef(), std::get<ShardId>(res)};
}

}  // namespace

using namespace std;

quicklistEntry QLEntry() {
  quicklistEntry res{.quicklist = NULL,
                     .node = NULL,
                     .zi = NULL,
                     .value = NULL,
                     .longval = 0,
                     .sz = 0,
                     .offset = 0};
  return res;
}

bool IterateList(const PrimeValue& pv, const IterateFunc& func, long start, long end) {
  quicklist* ql = static_cast<quicklist*>(pv.RObjPtr());
  long llen = quicklistCount(ql);
  if (end < 0 || end >= llen)
    end = llen - 1;

  quicklistIter* qiter = quicklistGetIteratorAtIdx(ql, AL_START_HEAD, start);
  quicklistEntry entry = QLEntry();
  long lrange = end - start + 1;

  bool success = true;
  while (success && quicklistNext(qiter, &entry) && lrange-- > 0) {
    if (entry.value) {
      success = func(ContainerEntry{reinterpret_cast<char*>(entry.value), entry.sz});
    } else {
      success = func(ContainerEntry{entry.longval});
    }
  }
  quicklistReleaseIterator(qiter);
  return success;
}

bool IterateSet(const PrimeValue& pv, const IterateFunc& func) {
  bool success = true;
  if (pv.Encoding() == kEncodingIntSet) {
    intset* is = static_cast<intset*>(pv.RObjPtr());
    int64_t ival;
    int ii = 0;

    while (success && intsetGet(is, ii++, &ival)) {
      success = func(ContainerEntry{ival});
    }
  } else {
    for (sds ptr : *static_cast<StringSet*>(pv.RObjPtr())) {
      if (!func(ContainerEntry{ptr, sdslen(ptr)})) {
        success = false;
        break;
      }
    }
  }

  return success;
}

bool IterateSortedSet(const detail::RobjWrapper* robj_wrapper, const IterateSortedFunc& func,
                      int32_t start, int32_t end, bool reverse, bool use_score) {
  unsigned long llen = robj_wrapper->Size();
  if (end < 0 || unsigned(end) >= llen)
    end = llen - 1;

  unsigned rangelen = unsigned(end - start) + 1;

  if (robj_wrapper->encoding() == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = static_cast<uint8_t*>(robj_wrapper->inner_obj());
    uint8_t *eptr, *sptr;
    uint8_t* vstr;
    unsigned int vlen;
    long long vlong;
    double score = 0.0;

    if (reverse) {
      eptr = lpSeek(zl, -2 - long(2 * start));
    } else {
      eptr = lpSeek(zl, 2 * start);
    }
    DCHECK(eptr);

    sptr = lpNext(zl, eptr);

    bool success = true;
    while (success && rangelen--) {
      DCHECK(eptr != NULL && sptr != NULL);
      vstr = lpGetValue(eptr, &vlen, &vlong);

      // don't bother to extract the score if it's gonna be ignored.
      if (use_score)
        score = zzlGetScore(sptr);

      if (vstr == NULL) {
        success = func(ContainerEntry{vlong}, score);
      } else {
        success = func(ContainerEntry{reinterpret_cast<const char*>(vstr), vlen}, score);
      }

      if (reverse) {
        zzlPrev(zl, &eptr, &sptr);
      } else {
        zzlNext(zl, &eptr, &sptr);
      };
    }
    return success;
  } else {
    CHECK_EQ(robj_wrapper->encoding(), OBJ_ENCODING_SKIPLIST);
    detail::SortedMap* smap = (detail::SortedMap*)robj_wrapper->inner_obj();
    return smap->Iterate(start, rangelen, reverse, [&](sds ele, double score) {
      return func(ContainerEntry{ele, sdslen(ele)}, score);
    });
  }
  return false;
}

StringMap* GetStringMap(const PrimeValue& pv, const DbContext& db_context) {
  DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
  StringMap* res = static_cast<StringMap*>(pv.RObjPtr());
  uint32_t map_time = MemberTimeSeconds(db_context.time_now_ms);
  res->set_time(map_time);
  return res;
}

optional<string_view> LpFind(uint8_t* lp, string_view key, uint8_t int_buf[]) {
  uint8_t* fptr = lpFirst(lp);
  DCHECK(fptr);

  fptr = lpFind(lp, fptr, (unsigned char*)key.data(), key.size(), 1);
  if (!fptr)
    return std::nullopt;
  uint8_t* vptr = lpNext(lp, fptr);
  return LpGetView(vptr, int_buf);
}

string_view LpGetView(uint8_t* lp_it, uint8_t int_buf[]) {
  int64_t ele_len = 0;
  uint8_t* elem = lpGet(lp_it, &ele_len, int_buf);
  DCHECK(elem);
  return std::string_view{reinterpret_cast<char*>(elem), size_t(ele_len)};
}

OpResult<string> RunCbOnFirstNonEmptyBlocking(Transaction* trans, int req_obj_type,
                                              BlockingResultCb func, unsigned limit_ms,
                                              bool* block_flag, bool* pause_flag) {
  string result_key;

  // Fast path. If we have only a single shard, we can run opportunistically with a single hop.
  // If we don't find anything, we abort concluding and keep scheduled.
  // Slow path: schedule, find results from shards, execute action if found.
  OpResult<ShardFFResult> result;
  if (trans->GetUniqueShardCnt() == 1 && absl::GetFlag(FLAGS_singlehop_blocking)) {
    auto res = FindFirstNonEmptySingleShard(trans, req_obj_type, func);
    if (res.ok())
      return res;
    else
      result = res.status();
  } else {
    result = FindFirstNonEmpty(trans, req_obj_type);
  }

  // If a non-empty key exists, execute the callback immediately
  if (result.ok()) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == result->sid) {
        result->key.GetString(&result_key);
        func(t, shard, result_key);
      }
      return OpStatus::OK;
    };
    trans->Execute(std::move(cb), true);
    return result_key;
  }

  // Abort on possible errors: wrong type, etc
  if (result.status() != OpStatus::KEY_NOTFOUND) {
    trans->Conclude();
    return result.status();
  }

  // Multi transactions are not allowed to block
  if (trans->IsMulti()) {
    trans->Conclude();
    return OpStatus::TIMED_OUT;
  }

  DCHECK(trans->IsScheduled());  // single shard optimization didn't forget to schedule
  VLOG(1) << "Blocking " << trans->DebugId();

  // If timeout (limit_ms) is zero, block indefinitely
  auto limit_tp = Transaction::time_point::max();
  if (limit_ms > 0) {
    using namespace std::chrono;
    limit_tp = steady_clock::now() + milliseconds(limit_ms);
  }

  auto wcb = [](Transaction* t, EngineShard* shard) { return t->GetShardArgs(shard->shard_id()); };
  const auto key_checker = [req_obj_type](EngineShard* owner, const DbContext& context,
                                          Transaction*, std::string_view key) -> bool {
    return owner->db_slice().FindReadOnly(context, key, req_obj_type).ok();
  };

  auto status = trans->WaitOnWatch(limit_tp, std::move(wcb), key_checker, block_flag, pause_flag);

  if (status != OpStatus::OK)
    return status;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    if (auto wake_key = t->GetWakeKey(shard->shard_id()); wake_key) {
      result_key = *wake_key;
      func(t, shard, result_key);
    }
    return OpStatus::OK;
  };
  trans->Execute(std::move(cb), true);

  return result_key;
}

}  // namespace dfly::container_utils
