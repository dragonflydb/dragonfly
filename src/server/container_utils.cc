// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/container_utils.h"

#include "base/logging.h"
#include "core/string_map.h"
#include "core/string_set.h"

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

}  // namespace dfly::container_utils
