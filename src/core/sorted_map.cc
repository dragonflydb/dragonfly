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

}  // namespace

SortedMap::SortedMap() {
  dict_ = dictCreate(&zsetDictType);
  zsl_ = zslCreate();
}

SortedMap::~SortedMap() {
  dictRelease(dict_);
  zslFree(zsl_);
}

size_t SortedMap::MallocSize() const {
  // TODO: introduce a proper malloc usage tracking.
  return DictMallocSize(dict_) + zmalloc_size(zsl_);
}

bool SortedMap::Insert(double score, sds member) {
  zskiplistNode* znode = zslInsert(zsl_, score, member);
  int ret = dictAdd(dict_, member, &znode->score);
  return ret == DICT_OK;
}

int SortedMap::Add(double score, sds ele, int in_flags, int* out_flags, double* newscore) {
  zskiplistNode* znode;

  /* Turn options into simple to check vars. */
  const bool incr = (in_flags & ZADD_IN_INCR) != 0;
  const bool nx = (in_flags & ZADD_IN_NX) != 0;
  const bool xx = (in_flags & ZADD_IN_XX) != 0;
  const bool gt = (in_flags & ZADD_IN_GT) != 0;
  const bool lt = (in_flags & ZADD_IN_LT) != 0;

  *out_flags = 0; /* We'll return our response flags. */
  double curscore;

  dictEntry* de = dictFind(dict_, ele);
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

    if (newscore)
      *newscore = score;

    /* Remove and re-insert when score changes. */
    if (score != curscore) {
      znode = zslUpdateScore(zsl_, curscore, ele, score);
      /* Note that we did not removed the original element from
       * the hash table representing the sorted set, so we just
       * update the score. */
      dictGetVal(de) = &znode->score; /* Update score ptr. */
      *out_flags |= ZADD_OUT_UPDATED;
    }
    return 1;
  } else if (!xx) {
    ele = sdsdup(ele);
    znode = zslInsert(zsl_, score, ele);
    CHECK_EQ(DICT_OK, dictAdd(dict_, ele, &znode->score));

    *out_flags |= ZADD_OUT_ADDED;
    if (newscore)
      *newscore = score;
    return 1;
  }

  *out_flags |= ZADD_OUT_NOP;
  return 1;
}

// taken from zsetConvert
unique_ptr<SortedMap> SortedMap::FromListPack(const uint8_t* lp) {
  uint8_t* zl = (uint8_t*)lp;
  unsigned char *eptr, *sptr;
  unsigned char* vstr;
  unsigned int vlen;
  long long vlong;
  sds ele;

  unique_ptr<SortedMap> zs(new SortedMap());

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

// taken from zsetConvert
uint8_t* SortedMap::ToListPack() const {
  uint8_t* lp = lpNew(0);

  /* Approach similar to zslFree(), since we want to free the skiplist at
   * the same time as creating the listpack. */
  zskiplistNode* node = zsl_->header->level[0].forward;

  while (node) {
    lp = zzlInsertAt(lp, NULL, node->ele, node->score);
    node = node->level[0].forward;
  }

  return lp;
}

// returns true if the element was deleted.
bool SortedMap::Delete(sds ele) {
  // Taken from zsetRemoveFromSkiplist.

  dictEntry* de = dictUnlink(dict_, ele);
  if (de == NULL)
    return false;

  /* Get the score in order to delete from the skiplist later. */
  double score = *(double*)dictGetVal(de);

  /* Delete from the hash table and later from the skiplist.
   * Note that the order is important: deleting from the skiplist
   * actually releases the SDS string representing the element,
   * which is shared between the skiplist and the hash table, so
   * we need to delete from the skiplist as the final step. */
  dictFreeUnlinkedEntry(dict_, de);
  if (htNeedsResize(dict_))
    dictResize(dict_);

  /* Delete from skiplist. */
  int retval = zslDelete(zsl_, score, ele, NULL);
  DCHECK(retval);

  return true;
}

std::optional<double> SortedMap::GetScore(sds ele) const {
  dictEntry* de = dictFind(dict_, ele);
  if (de == NULL)
    return std::nullopt;

  return *(double*)dictGetVal(de);
}

std::optional<unsigned> SortedMap::GetRank(sds ele, bool reverse) const {
  dictEntry* de = dictFind(dict_, ele);
  if (de == NULL)
    return nullopt;

  double score = *(double*)dictGetVal(de);
  unsigned rank = zslGetRank(zsl_, score, ele);

  /* Existing elements always have a rank. */
  DCHECK(rank != 0);
  if (reverse)
    return zsl_->length - rank;
  else
    return rank - 1;
}

auto SortedMap::GetRange(const zrangespec& range, unsigned offset, unsigned limit,
                         bool reverse) const -> ScoredArray {
  /* If reversed, get the last node in range as starting point. */
  zskiplistNode* ln;

  if (reverse) {
    ln = zslLastInRange(zsl_, &range);
  } else {
    ln = zslFirstInRange(zsl_, &range);
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

auto SortedMap::GetLexRange(const zlexrangespec& range, unsigned offset, unsigned limit,
                            bool reverse) const -> ScoredArray {
  zskiplistNode* ln;
  /* If reversed, get the last node in range as starting point. */
  if (reverse) {
    ln = zslLastInLexRange(zsl_, &range);
  } else {
    ln = zslFirstInLexRange(zsl_, &range);
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

auto SortedMap::PopTopScores(unsigned count, bool reverse) -> ScoredArray {
  zskiplistNode* ln;
  if (reverse) {
    ln = zsl_->tail;
  } else {
    ln = zsl_->header->level[0].forward;
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

size_t SortedMap::Count(const zrangespec& range) const {
  /* Find first element in range */
  zskiplistNode* zn = zslFirstInRange(zsl_, &range);

  /* Use rank of first element, if any, to determine preliminary count */
  if (zn == NULL)
    return 0;

  unsigned long rank = zslGetRank(zsl_, zn->score, zn->ele);
  size_t count = (zsl_->length - (rank - 1));

  /* Find last element in range */
  zn = zslLastInRange(zsl_, &range);

  /* Use rank of last element, if any, to determine the actual count */
  if (zn != NULL) {
    rank = zslGetRank(zsl_, zn->score, zn->ele);
    count -= (zsl_->length - rank);
  }

  return count;
}

size_t SortedMap::LexCount(const zlexrangespec& range) const {
  /* Find first element in range */
  zskiplistNode* zn = zslFirstInLexRange(zsl_, &range);

  /* Use rank of first element, if any, to determine preliminary count */
  if (zn == NULL)
    return 0;

  unsigned long rank = zslGetRank(zsl_, zn->score, zn->ele);
  size_t count = (zsl_->length - (rank - 1));

  /* Find last element in range */
  zn = zslLastInLexRange(zsl_, &range);

  /* Use rank of last element, if any, to determine the actual count */
  if (zn != NULL) {
    rank = zslGetRank(zsl_, zn->score, zn->ele);
    count -= (zsl_->length - rank);
  }
  return count;
}

bool SortedMap::Iterate(unsigned start_rank, unsigned len, bool reverse,
                        absl::FunctionRef<bool(sds, double)> cb) const {
  zskiplistNode* ln;

  /* Check if starting point is trivial, before doing log(N) lookup. */
  if (reverse) {
    ln = zsl_->tail;
    unsigned long llen = zsl_->length;
    if (start_rank > 0)
      ln = zslGetElementByRank(zsl_, llen - start_rank);
  } else {
    ln = zsl_->header->level[0].forward;
    if (start_rank > 0)
      ln = zslGetElementByRank(zsl_, start_rank + 1);
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

}  // namespace detail
}  // namespace dfly
