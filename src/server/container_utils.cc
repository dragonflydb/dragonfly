// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/container_utils.h"

#include <bits/chrono.h>

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <ostream>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/dash.h"
#include "core/dash_internal.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "glog/logging.h"
#include "redis/dict.h"
#include "redis/sds.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"
#include "src/facade/op_status.h"

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/object.h"
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

bool IterateSortedSet(robj* zobj, const IterateSortedFunc& func, int32_t start, int32_t end,
                      bool reverse, bool use_score) {
  unsigned long llen = zsetLength(zobj);
  if (end < 0 || unsigned(end) >= llen)
    end = llen - 1;

  unsigned rangelen = unsigned(end - start) + 1;

  if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = static_cast<uint8_t*>(zobj->ptr);
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
    CHECK_EQ(zobj->encoding, OBJ_ENCODING_SKIPLIST);
    zset* zs = static_cast<zset*>(zobj->ptr);
    zskiplist* zsl = zs->zsl;
    zskiplistNode* ln;

    /* Check if starting point is trivial, before doing log(N) lookup. */
    if (reverse) {
      ln = zsl->tail;
      unsigned long llen = zsetLength(zobj);
      if (start > 0)
        ln = zslGetElementByRank(zsl, llen - start);
    } else {
      ln = zsl->header->level[0].forward;
      if (start > 0)
        ln = zslGetElementByRank(zsl, start + 1);
    }

    bool success = true;
    while (success && rangelen--) {
      DCHECK(ln != NULL);
      success = func(ContainerEntry{ln->ele, sdslen(ln->ele)}, ln->score);
      ln = reverse ? ln->backward : ln->level[0].forward;
    }
    return success;
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

// If OK is returned then cb was called on the first non empty key and `out_key` is set to the key.
OpResult<string> RunCbOnFirstNonEmptyBlocking(Transaction* trans, int req_obj_type,
                                              BlockingResultCb func, unsigned limit_ms) {
  auto limit_tp = limit_ms ? std::chrono::steady_clock::now() + std::chrono::milliseconds(limit_ms)
                           : Transaction::time_point::max();
  bool is_multi = trans->IsMulti();
  trans->Schedule();

  ShardFFResult ff_result;
  OpResult<ShardFFResult> result = FindFirstNonEmptyKey(trans, req_obj_type);

  if (result.ok()) {
    ff_result = std::move(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    // Close transaction and return.
    if (is_multi) {
      auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      trans->Execute(std::move(cb), true);
      return OpStatus::TIMED_OUT;
    }

    auto wcb = [](Transaction* t, EngineShard* shard) {
      return t->GetShardArgs(shard->shard_id());
    };

    VLOG(1) << "Blocking BLPOP " << trans->DebugId();

    bool wait_succeeded = trans->WaitOnWatch(limit_tp, std::move(wcb));
    if (!wait_succeeded)
      return OpStatus::TIMED_OUT;
  } else {
    // Could be the wrong-type error.
    // cleanups, locks removal etc.
    auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
    trans->Execute(std::move(cb), true);

    DCHECK_NE(result.status(), OpStatus::KEY_NOTFOUND);
    return result.status();
  }

  string result_key;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    if (auto wake_key = t->GetWakeKey(shard->shard_id()); wake_key) {
      result_key = *wake_key;
      func(t, shard, result_key);
    } else if (shard->shard_id() == ff_result.sid) {
      ff_result.key.GetString(&result_key);
      func(t, shard, result_key);
    }
    return OpStatus::OK;
  };
  trans->Execute(std::move(cb), true);

  return result_key;
}

}  // namespace dfly::container_utils
