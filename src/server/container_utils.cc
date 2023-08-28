// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/container_utils.h"

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
#include "redis/object.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zset.h"
}

namespace dfly::container_utils {

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
    if (pv.Encoding() == kEncodingStrMap2) {
      for (sds ptr : *static_cast<StringSet*>(pv.RObjPtr())) {
        if (!func(ContainerEntry{ptr, sdslen(ptr)})) {
          success = false;
          break;
        }
      }
    } else {
      dict* ds = static_cast<dict*>(pv.RObjPtr());
      dictIterator* di = dictGetIterator(ds);
      dictEntry* de = nullptr;
      while (success && (de = dictNext(di))) {
        sds ptr = static_cast<sds>(de->key);
        success = func(ContainerEntry{ptr, sdslen(ptr)});
      }
      dictReleaseIterator(di);
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

OpResult<ShardFFResult> FindFirstNonEmptyKey(Transaction* trans, int req_obj_type) {
  using FFResult = std::pair<PrimeKey, unsigned>;  // key, argument index.
  VLOG(2) << "FindFirst::Find " << trans->DebugId();

  // Holds Find results: (iterator to a found key, and its index in the passed arguments).
  // See DbSlice::FindFirst for more details.
  // spans all the shards for now.
  std::vector<OpResult<FFResult>> find_res(shard_set->size());
  std::fill(find_res.begin(), find_res.end(), OpStatus::KEY_NOTFOUND);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    OpResult<std::pair<PrimeIterator, unsigned>> ff_res =
        shard->db_slice().FindFirst(t->GetDbContext(), args, req_obj_type);

    if (ff_res) {
      FFResult ff_result(ff_res->first->first.AsRef(), ff_res->second);
      find_res[shard->shard_id()] = std::move(ff_result);
    } else {
      find_res[shard->shard_id()] = ff_res.status();
    }

    return OpStatus::OK;
  };

  trans->Execute(std::move(cb), false);

  uint32_t min_arg_indx = UINT32_MAX;
  ShardFFResult shard_result;

  // We iterate over all results to find the key with the minimal arg_index
  // after reversing the arg indexing permutation.
  for (size_t sid = 0; sid < find_res.size(); ++sid) {
    const auto& fr = find_res[sid];
    auto status = fr.status();
    if (status == OpStatus::KEY_NOTFOUND)
      continue;
    if (status == OpStatus::WRONG_TYPE) {
      return status;
    }
    CHECK(fr);

    const auto& it_pos = fr.value();

    size_t arg_indx = trans->ReverseArgIndex(sid, it_pos.second);
    if (arg_indx < min_arg_indx) {
      min_arg_indx = arg_indx;
      shard_result.sid = sid;

      // we do not dereference the key, do not extract the string value, so it it
      // ok to just move it. We can not dereference it due to limitations of SmallString
      // that rely on thread-local data-structure for pointer translation.
      shard_result.key = it_pos.first.AsRef();
    }
  }

  if (shard_result.sid == kInvalidSid) {
    return OpStatus::KEY_NOTFOUND;
  }

  return OpResult<ShardFFResult>{std::move(shard_result)};
}

OpResult<string> RunCbOnFirstNonEmptyBlocking(Transaction* trans, int req_obj_type,
                                              BlockingResultCb func, unsigned limit_ms) {
  trans->Schedule();

  string result_key;
  OpResult<ShardFFResult> result = FindFirstNonEmptyKey(trans, req_obj_type);

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

  VLOG(1) << "Blocking " << trans->DebugId();

  // If timeout (limit_ms) is zero, block indefinitely
  auto limit_tp = Transaction::time_point::max();
  if (limit_ms > 0) {
    using namespace std::chrono;
    limit_tp = steady_clock::now() + milliseconds(limit_ms);
  }

  auto wcb = [](Transaction* t, EngineShard* shard) { return t->GetShardArgs(shard->shard_id()); };

  bool wait_succeeded = trans->WaitOnWatch(limit_tp, std::move(wcb));
  if (!wait_succeeded)
    return OpStatus::TIMED_OUT;

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
