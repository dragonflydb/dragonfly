// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/sorted_map.h"

#include <cmath>

extern "C" {
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zmalloc.h"
}

#include "base/endian.h"
#include "base/logging.h"

using namespace std;
namespace dfly {
namespace detail {

namespace {

// Approximated dictionary size.
size_t DictMallocSize(dict* d) {
  size_t res = zmalloc_usable_size(d->ht_table[0]) + zmalloc_usable_size(d->ht_table[1]) +
               znallocx(sizeof(dict));

  return res + dictSize(d) * 16;  // approximation.
}

unsigned char* zzlInsertAt(unsigned char* zl, unsigned char* eptr, sds ele, double score) {
  unsigned char* sptr;
  char scorebuf[128];
  int scorelen;

  scorelen = d2string(scorebuf, sizeof(scorebuf), score);
  if (eptr == NULL) {
    zl = lpAppend(zl, (unsigned char*)ele, sdslen(ele));
    zl = lpAppend(zl, (unsigned char*)scorebuf, scorelen);
  } else {
    /* Insert member before the element 'eptr'. */
    zl = lpInsertString(zl, (unsigned char*)ele, sdslen(ele), eptr, LP_BEFORE, &sptr);

    /* Insert score after the member. */
    zl = lpInsertString(zl, (unsigned char*)scorebuf, scorelen, sptr, LP_AFTER, NULL);
  }
  return zl;
}

inline zskiplistNode* Next(bool reverse, zskiplistNode* ln) {
  return reverse ? ln->backward : ln->level[0].forward;
}

inline bool IsUnder(bool reverse, double score, const zrangespec& spec) {
  return reverse ? zslValueGteMin(score, &spec) : zslValueLteMax(score, &spec);
}

union DoubleIntUnion {
  double score;
  uint64_t u64;
};

double GetObjScore(const void* obj) {
  DoubleIntUnion u;
  sds s = (sds)obj;
  char* ptr = s + sdslen(s) + 1;
  u.u64 = absl::little_endian::Load64(ptr);

  return u.score;
}

}  // namespace

void SortedMap::RdImpl::Init() {
  dict = dictCreate(&zsetDictType);
  zsl = zslCreate();
}

size_t SortedMap::RdImpl::MallocSize() const {
  return DictMallocSize(dict) + zmalloc_size(zsl);
}

bool SortedMap::RdImpl::Insert(double score, sds member) {
  zskiplistNode* znode = zslInsert(zsl, score, member);
  int ret = dictAdd(dict, member, &znode->score);
  return ret == DICT_OK;
}

int SortedMap::RdImpl::Add(double score, sds ele, int in_flags, int* out_flags, double* newscore) {
  zskiplistNode* znode;

  /* Turn options into simple to check vars. */
  const bool incr = (in_flags & ZADD_IN_INCR) != 0;
  const bool nx = (in_flags & ZADD_IN_NX) != 0;
  const bool xx = (in_flags & ZADD_IN_XX) != 0;
  const bool gt = (in_flags & ZADD_IN_GT) != 0;
  const bool lt = (in_flags & ZADD_IN_LT) != 0;

  *out_flags = 0; /* We'll return our response flags. */
  double curscore;

  dictEntry* de = dictFind(dict, ele);
  if (de != NULL) {
    /* NX? Return, same element already exists. */
    if (nx) {
      *out_flags |= ZADD_OUT_NOP;
      return 1;
    }

    curscore = *(double*)dictGetVal(de);

    /* Prepare the score for the increment if needed. */
    if (incr) {
      score += curscore;
      if (isnan(score)) {
        *out_flags |= ZADD_OUT_NAN;
        return 0;
      }
    }

    /* GT/LT? Only update if score is greater/less than current. */
    if ((lt && score >= curscore) || (gt && score <= curscore)) {
      *out_flags |= ZADD_OUT_NOP;
      return 1;
    }

    *newscore = score;

    /* Remove and re-insert when score changes. */
    if (score != curscore) {
      znode = zslUpdateScore(zsl, curscore, ele, score);
      /* Note that we did not removed the original element from
       * the hash table representing the sorted set, so we just
       * update the score. */
      dictGetVal(de) = &znode->score; /* Update score ptr. */
      *out_flags |= ZADD_OUT_UPDATED;
    }
    return 1;
  } else if (!xx) {
    ele = sdsdup(ele);
    znode = zslInsert(zsl, score, ele);
    CHECK_EQ(DICT_OK, dictAdd(dict, ele, &znode->score));

    *out_flags |= ZADD_OUT_ADDED;
    *newscore = score;
    return 1;
  }

  *out_flags |= ZADD_OUT_NOP;
  return 1;
}

optional<unsigned> SortedMap::RdImpl::GetRank(sds ele, bool reverse) const {
  dictEntry* de = dictFind(dict, ele);
  if (de == NULL)
    return std::nullopt;

  double score = *(double*)dictGetVal(de);
  unsigned rank = zslGetRank(zsl, score, ele);

  /* Existing elements always have a rank. */
  DCHECK(rank != 0);
  return reverse ? zsl->length - rank : rank - 1;
}

optional<double> SortedMap::RdImpl::GetScore(sds member) const {
  dictEntry* de = dictFind(dict, member);
  if (de == NULL)
    return std::nullopt;

  return *(double*)dictGetVal(de);
}

SortedMap::ScoredArray SortedMap::RdImpl::GetRange(const zrangespec& range, unsigned offset,
                                                   unsigned limit, bool reverse) const {
  /* If reversed, get the last node in range as starting point. */
  zskiplistNode* ln;

  if (reverse) {
    ln = zslLastInRange(zsl, &range);
  } else {
    ln = zslFirstInRange(zsl, &range);
  }

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (ln && offset--) {
    ln = Next(reverse, ln);
  }

  ScoredArray result;
  while (ln && limit--) {
    /* Abort when the node is no longer in range. */
    if (!IsUnder(reverse, ln->score, range))
      break;

    result.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* Move to next node */
    ln = Next(reverse, ln);
  }
  return result;
}

SortedMap::ScoredArray SortedMap::RdImpl::GetLexRange(const zlexrangespec& range, unsigned offset,
                                                      unsigned limit, bool reverse) const {
  zskiplistNode* ln;
  /* If reversed, get the last node in range as starting point. */
  if (reverse) {
    ln = zslLastInLexRange(zsl, &range);
  } else {
    ln = zslFirstInLexRange(zsl, &range);
  }

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (ln && offset--) {
    ln = Next(reverse, ln);
  }

  ScoredArray result;
  while (ln && limit--) {
    /* Abort when the node is no longer in range. */
    if (reverse) {
      if (!zslLexValueGteMin(ln->ele, &range))
        break;
    } else {
      if (!zslLexValueLteMax(ln->ele, &range))
        break;
    }

    result.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* Move to next node */
    ln = Next(reverse, ln);
  }

  return result;
}

uint8_t* SortedMap::RdImpl::ToListPack() const {
  uint8_t* lp = lpNew(0);

  /* Approach similar to zslFree(), since we want to free the skiplist at
   * the same time as creating the listpack. */
  zskiplistNode* node = zsl->header->level[0].forward;

  while (node) {
    lp = zzlInsertAt(lp, NULL, node->ele, node->score);
    node = node->level[0].forward;
  }

  return lp;
}

bool SortedMap::RdImpl::Delete(sds member) {
  dictEntry* de = dictUnlink(dict, member);
  if (de == NULL)
    return false;

  /* Get the score in order to delete from the skiplist later. */
  double score = *(double*)dictGetVal(de);

  /* Delete from the hash table and later from the skiplist.
   * Note that the order is important: deleting from the skiplist
   * actually releases the SDS string representing the element,
   * which is shared between the skiplist and the hash table, so
   * we need to delete from the skiplist as the final step. */
  dictFreeUnlinkedEntry(dict, de);
  if (htNeedsResize(dict))
    dictResize(dict);

  /* Delete from skiplist. */
  int retval = zslDelete(zsl, score, member, NULL);
  DCHECK(retval);

  return true;
}

SortedMap::ScoredArray SortedMap::RdImpl::PopTopScores(unsigned count, bool reverse) {
  zskiplistNode* ln;
  if (reverse) {
    ln = zsl->tail;
  } else {
    ln = zsl->header->level[0].forward;
  }

  ScoredArray result;
  while (ln && count--) {
    result.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* we can delete the element now */
    CHECK(Delete(ln->ele));

    ln = Next(reverse, ln);
  }
  return result;
}

size_t SortedMap::RdImpl::Count(const zrangespec& range) const {
  /* Find first element in range */
  zskiplistNode* zn = zslFirstInRange(zsl, &range);

  /* Use rank of first element, if any, to determine preliminary count */
  if (zn == NULL)
    return 0;

  unsigned long rank = zslGetRank(zsl, zn->score, zn->ele);
  size_t count = (zsl->length - (rank - 1));

  /* Find last element in range */
  zn = zslLastInRange(zsl, &range);

  /* Use rank of last element, if any, to determine the actual count */
  if (zn != NULL) {
    rank = zslGetRank(zsl, zn->score, zn->ele);
    count -= (zsl->length - rank);
  }

  return count;
}

size_t SortedMap::RdImpl::LexCount(const zlexrangespec& range) const {
  /* Find first element in range */
  zskiplistNode* zn = zslFirstInLexRange(zsl, &range);

  /* Use rank of first element, if any, to determine preliminary count */
  if (zn == NULL)
    return 0;

  unsigned long rank = zslGetRank(zsl, zn->score, zn->ele);
  size_t count = (zsl->length - (rank - 1));

  /* Find last element in range */
  zn = zslLastInLexRange(zsl, &range);

  /* Use rank of last element, if any, to determine the actual count */
  if (zn != NULL) {
    rank = zslGetRank(zsl, zn->score, zn->ele);
    count -= (zsl->length - rank);
  }
  return count;
}

bool SortedMap::RdImpl::Iterate(unsigned start_rank, unsigned len, bool reverse,
                                absl::FunctionRef<bool(sds, double)> cb) const {
  zskiplistNode* ln;

  /* Check if starting point is trivial, before doing log(N) lookup. */
  if (reverse) {
    ln = zsl->tail;
    unsigned long llen = zsl->length;
    if (start_rank > 0)
      ln = zslGetElementByRank(zsl, llen - start_rank);
  } else {
    ln = zsl->header->level[0].forward;
    if (start_rank > 0)
      ln = zslGetElementByRank(zsl, start_rank + 1);
  }

  bool success = true;
  while (success && len--) {
    DCHECK(ln != NULL);
    success = cb(ln->ele, ln->score);
    ln = reverse ? ln->backward : ln->level[0].forward;
    if (!ln)
      break;
  }
  return success;
}

uint64_t SortedMap::RdImpl::Scan(uint64_t cursor,
                                 absl::FunctionRef<void(std::string_view, double)> cb) const {
  auto scanCb = [](void* privdata, const dictEntry* de) {
    auto* cb = (absl::FunctionRef<void(std::string_view, double)>*)privdata;

    sds key = (sds)de->key;
    double score = *(double*)dictGetVal(de);
    (*cb)(std::string_view(key, sdslen(key)), score);
  };

  return dictScan(this->dict, cursor, scanCb, NULL, &cb);
}

int SortedMap::DfImpl::ScoreSdsPolicy::KeyCompareTo::operator()(ScoreSds a, ScoreSds b) const {
  double sa = GetObjScore(a);
  double sb = GetObjScore(b);

  if (sa < sb)
    return -1;
  if (sa > sb)
    return 1;

  sds sdsa = (sds)a;
  sds sdsb = (sds)b;
  return sdscmp(sdsa, sdsb);
}

int SortedMap::DfImpl::Add(double score, sds ele, int in_flags, int* out_flags, double* newscore) {
  LOG(FATAL) << "TBD";
  return 0;
}

optional<double> SortedMap::DfImpl::GetScore(sds ele) const {
  LOG(FATAL) << "TBD";
  return std::nullopt;
}

void SortedMap::DfImpl::Init(PMR_NS::memory_resource* mr) {
  LOG(FATAL) << "TBD";
}

void SortedMap::DfImpl::Free() {
  LOG(FATAL) << "TBD";
}

bool SortedMap::DfImpl::Insert(double score, sds member) {
  LOG(FATAL) << "TBD";
  return false;
}

optional<unsigned> SortedMap::DfImpl::GetRank(sds ele, bool reverse) const {
  LOG(FATAL) << "TBD";
  return std::nullopt;
}

SortedMap::ScoredArray SortedMap::DfImpl::GetRange(const zrangespec& range, unsigned offset,
                                                   unsigned limit, bool reverse) const {
  LOG(FATAL) << "TBD";
  return {};
}

SortedMap::ScoredArray SortedMap::DfImpl::GetLexRange(const zlexrangespec& range, unsigned offset,
                                                      unsigned limit, bool reverse) const {
  LOG(FATAL) << "TBD";
  return {};
}

uint8_t* SortedMap::DfImpl::ToListPack() const {
  LOG(FATAL) << "TBD";
  return nullptr;
}

bool SortedMap::DfImpl::Delete(sds ele) {
  LOG(FATAL) << "TBD";
  return false;
}

SortedMap::ScoredArray SortedMap::DfImpl::PopTopScores(unsigned count, bool reverse) {
  LOG(FATAL) << "TBD";
  return {};
}

size_t SortedMap::DfImpl::Count(const zrangespec& range) const {
  LOG(FATAL) << "TBD";
  return 0;
}

size_t SortedMap::DfImpl::LexCount(const zlexrangespec& range) const {
  LOG(FATAL) << "TBD";
  return 0;
}

bool SortedMap::DfImpl::Iterate(unsigned start_rank, unsigned len, bool reverse,
                                absl::FunctionRef<bool(sds, double)> cb) const {
  LOG(FATAL) << "TBD";
  return false;
}

uint64_t SortedMap::DfImpl::Scan(uint64_t cursor,
                                 absl::FunctionRef<void(std::string_view, double)> cb) const {
  LOG(FATAL) << "TBD";
  return 0;
}

/***************************************************************************/
/* SortedMap */
/***************************************************************************/
SortedMap::SortedMap(PMR_NS::memory_resource* mr) : impl_(RdImpl()), mr_res_(mr) {
  std::visit(Overload{[](RdImpl& impl) { impl.Init(); }, [mr](DfImpl& impl) { impl.Init(mr); }},
             impl_);
}

SortedMap::~SortedMap() {
  std::visit(Overload{[](auto& impl) { impl.Free(); }}, impl_);
}

// taken from zsetConvert
unique_ptr<SortedMap> SortedMap::FromListPack(PMR_NS::memory_resource* res, const uint8_t* lp) {
  uint8_t* zl = (uint8_t*)lp;
  unsigned char *eptr, *sptr;
  unsigned char* vstr;
  unsigned int vlen;
  long long vlong;
  sds ele;

  unique_ptr<SortedMap> zs(new SortedMap(res));

  eptr = lpSeek(zl, 0);
  if (eptr != NULL) {
    sptr = lpNext(zl, eptr);
    CHECK(sptr != NULL);
  }

  while (eptr != NULL) {
    double score = zzlGetScore(sptr);
    vstr = lpGetValue(eptr, &vlen, &vlong);
    if (vstr == NULL)
      ele = sdsfromlonglong(vlong);
    else
      ele = sdsnewlen((char*)vstr, vlen);

    CHECK(zs->Insert(score, ele));
    zzlNext(zl, &eptr, &sptr);
  }

  return zs;
}

}  // namespace detail
}  // namespace dfly
