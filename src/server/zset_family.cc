// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/util.h"
#include "redis/zset.h"
}

#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/error.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;
using absl::SimpleAtoi;
namespace {

using CI = CommandId;

static const char kNxXxErr[] = "XX and NX options at the same time are not compatible";
static const char kScoreNaN[] = "resulting score is not a number (NaN)";
static const char kFloatRangeErr[] = "min or max is not a float";
static const char kLexRangeErr[] = "min or max not valid string range item";

constexpr unsigned kMaxListPackValue = 64;

inline zrangespec GetZrangeSpec(bool reverse, const ZSetFamily::ScoreInterval& si) {
  auto interval = si;
  if (reverse)
    swap(interval.first, interval.second);

  zrangespec range;
  range.min = interval.first.val;
  range.max = interval.second.val;
  range.minex = interval.first.is_open;
  range.maxex = interval.second.is_open;

  return range;
}

sds GetLexStr(const ZSetFamily::LexBound& bound) {
  if (bound.type == ZSetFamily::LexBound::MINUS_INF)
    return cminstring;

  if (bound.type == ZSetFamily::LexBound::PLUS_INF)
    return cmaxstring;

  return sdsnewlen(bound.val.data(), bound.val.size());
};

zlexrangespec GetLexRange(bool reverse, const ZSetFamily::LexInterval& li) {
  auto interval = li;
  if (reverse)
    swap(interval.first, interval.second);

  zlexrangespec range;
  range.minex = 0;
  range.maxex = 0;

  range.min = GetLexStr(interval.first);
  range.max = GetLexStr(interval.second);
  range.minex = (interval.first.type == ZSetFamily::LexBound::OPEN);
  range.maxex = (li.second.type == ZSetFamily::LexBound::OPEN);

  return range;
}

struct ZParams {
  unsigned flags = 0;  // mask of ZADD_IN_ macros.
  bool ch = false;     // Corresponds to CH option.
  bool override = false;
};

OpResult<PrimeIterator> FindZEntry(const ZParams& zparams, const OpArgs& op_args, string_view key,
                                   size_t member_len) {
  auto& db_slice = op_args.shard->db_slice();
  if (zparams.flags & ZADD_IN_XX) {
    return db_slice.Find(op_args.db_cntx, key, OBJ_ZSET);
  }

  pair<PrimeIterator, bool> add_res;

  try {
    add_res = db_slice.AddOrFind(op_args.db_cntx, key);
  } catch (bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }

  PrimeIterator& it = add_res.first;
  if (add_res.second || zparams.override) {
    robj* zobj = nullptr;

    if (member_len > kMaxListPackValue) {
      zobj = createZsetObject();
    } else {
      zobj = createZsetListpackObject();
    }

    DVLOG(2) << "Created zset " << zobj->ptr;
    if (!add_res.second) {
      db_slice.PreUpdate(op_args.db_cntx.db_index, it);
    }
    it->second.ImportRObj(zobj);
  } else {
    if (it->second.ObjType() != OBJ_ZSET)
      return OpStatus::WRONG_TYPE;
    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  }

  if (add_res.second && op_args.shard->blocking_controller()) {
    string tmp;
    string_view key = it->first.GetSlice(&tmp);
    op_args.shard->blocking_controller()->AwakeWatched(op_args.db_cntx.db_index, key);
  }

  return it;
}

enum class Action { RANGE = 0, REMOVE = 1, POP = 2 };

class IntervalVisitor {
 public:
  IntervalVisitor(Action action, const ZSetFamily::RangeParams& params, robj* o)
      : action_(action), params_(params), zobj_(o) {
  }

  void operator()(const ZSetFamily::IndexInterval& ii);

  void operator()(const ZSetFamily::ScoreInterval& si);

  void operator()(const ZSetFamily::LexInterval& li);

  void operator()(ZSetFamily::TopNScored sc);

  ZSetFamily::ScoredArray PopResult() {
    return std::move(result_);
  }

  unsigned removed() const {
    return removed_;
  }

 private:
  void ExtractListPack(const zrangespec& range);
  void ExtractSkipList(const zrangespec& range);

  void ExtractListPack(const zlexrangespec& range);
  void ExtractSkipList(const zlexrangespec& range);

  void PopListPack(ZSetFamily::TopNScored sc);
  void PopSkipList(ZSetFamily::TopNScored sc);

  void ActionRange(unsigned start, unsigned end);  // rank
  void ActionRange(const zrangespec& range);       // score
  void ActionRange(const zlexrangespec& range);    // lex

  void ActionRem(unsigned start, unsigned end);  // rank
  void ActionRem(const zrangespec& range);       // score
  void ActionRem(const zlexrangespec& range);    // lex

  void ActionPop(ZSetFamily::TopNScored sc);

  void Next(uint8_t* zl, uint8_t** eptr, uint8_t** sptr) const {
    if (params_.reverse) {
      zzlPrev(zl, eptr, sptr);
    } else {
      zzlNext(zl, eptr, sptr);
    }
  }

  zskiplistNode* Next(zskiplistNode* ln) const {
    return params_.reverse ? ln->backward : ln->level[0].forward;
  }

  bool IsUnder(double score, const zrangespec& spec) const {
    return params_.reverse ? zslValueGteMin(score, &spec) : zslValueLteMax(score, &spec);
  }

  void AddResult(const uint8_t* vstr, unsigned vlen, long long vlon, double score);

  Action action_;
  ZSetFamily::RangeParams params_;
  robj* zobj_;

  ZSetFamily::ScoredArray result_;
  unsigned removed_ = 0;
};

void IntervalVisitor::operator()(const ZSetFamily::IndexInterval& ii) {
  unsigned long llen = zsetLength(zobj_);
  int32_t start = ii.first;
  int32_t end = ii.second;

  if (start < 0)
    start = llen + start;
  if (end < 0)
    end = llen + end;
  if (start < 0)
    start = 0;

  if (start > end || unsigned(start) >= llen) {
    return;
  }

  if (unsigned(end) >= llen)
    end = llen - 1;

  switch (action_) {
    case Action::RANGE:
      ActionRange(start, end);
      break;
    case Action::REMOVE:
      ActionRem(start, end);
      break;
    default:
      break;
  }
}

void IntervalVisitor::operator()(const ZSetFamily::ScoreInterval& si) {
  zrangespec range = GetZrangeSpec(params_.reverse, si);

  switch (action_) {
    case Action::RANGE:
      ActionRange(range);
      break;
    case Action::REMOVE:
      ActionRem(range);
      break;
    default:
      break;
  }
}

void IntervalVisitor::operator()(const ZSetFamily::LexInterval& li) {
  zlexrangespec range = GetLexRange(params_.reverse, li);

  switch (action_) {
    case Action::RANGE:
      ActionRange(range);
      break;
    case Action::REMOVE:
      ActionRem(range);
      break;
    default:
      break;
  }
  zslFreeLexRange(&range);
}

void IntervalVisitor::operator()(ZSetFamily::TopNScored sc) {
  switch (action_) {
    case Action::POP:
      ActionPop(sc);
      break;
    default:
      break;
  }
}

void IntervalVisitor::ActionRange(unsigned start, unsigned end) {
  if (params_.limit == 0)
    return;
  // Calculate new start and end given offset and limit.
  start += params_.offset;
  end = static_cast<uint32_t>(min(1ULL * start + params_.limit - 1, 1ULL * end));

  container_utils::IterateSortedSet(
      zobj_,
      [this](container_utils::ContainerEntry ce, double score) {
        result_.emplace_back(ce.ToString(), score);
        return true;
      },
      start, end, params_.reverse, params_.with_scores);
}

void IntervalVisitor::ActionRange(const zrangespec& range) {
  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    ExtractListPack(range);
  } else {
    CHECK_EQ(zobj_->encoding, OBJ_ENCODING_SKIPLIST);
    ExtractSkipList(range);
  }
}

void IntervalVisitor::ActionRange(const zlexrangespec& range) {
  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    ExtractListPack(range);
  } else {
    CHECK_EQ(zobj_->encoding, OBJ_ENCODING_SKIPLIST);
    ExtractSkipList(range);
  }
}

void IntervalVisitor::ActionRem(unsigned start, unsigned end) {
  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = (uint8_t*)zobj_->ptr;

    removed_ = (end - start) + 1;
    zl = lpDeleteRange(zl, 2 * start, 2 * removed_);
    zobj_->ptr = zl;
  } else {
    CHECK_EQ(OBJ_ENCODING_SKIPLIST, zobj_->encoding);
    zset* zs = (zset*)zobj_->ptr;
    removed_ = zslDeleteRangeByRank(zs->zsl, start + 1, end + 1, zs->dict);
  }
}

void IntervalVisitor::ActionRem(const zrangespec& range) {
  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = (uint8_t*)zobj_->ptr;
    unsigned long deleted = 0;
    zl = zzlDeleteRangeByScore(zl, &range, &deleted);
    zobj_->ptr = zl;
    removed_ = deleted;
  } else {
    CHECK_EQ(OBJ_ENCODING_SKIPLIST, zobj_->encoding);
    zset* zs = (zset*)zobj_->ptr;
    removed_ = zslDeleteRangeByScore(zs->zsl, &range, zs->dict);
  }
}

void IntervalVisitor::ActionRem(const zlexrangespec& range) {
  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = (uint8_t*)zobj_->ptr;
    unsigned long deleted = 0;
    zl = zzlDeleteRangeByLex(zl, &range, &deleted);
    zobj_->ptr = zl;
    removed_ = deleted;
  } else {
    CHECK_EQ(OBJ_ENCODING_SKIPLIST, zobj_->encoding);
    zset* zs = (zset*)zobj_->ptr;
    removed_ = zslDeleteRangeByLex(zs->zsl, &range, zs->dict);
  }
}

void IntervalVisitor::ActionPop(ZSetFamily::TopNScored sc) {
  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    PopListPack(sc);
  } else {
    CHECK_EQ(zobj_->encoding, OBJ_ENCODING_SKIPLIST);
    PopSkipList(sc);
  }
}

void IntervalVisitor::ExtractListPack(const zrangespec& range) {
  uint8_t* zl = (uint8_t*)zobj_->ptr;
  uint8_t *eptr, *sptr;
  uint8_t* vstr;
  unsigned int vlen = 0;
  long long vlong = 0;
  unsigned offset = params_.offset;
  unsigned limit = params_.limit;

  /* If reversed, get the last node in range as starting point. */
  if (params_.reverse) {
    eptr = zzlLastInRange(zl, &range);
  } else {
    eptr = zzlFirstInRange(zl, &range);
  }

  /* Get score pointer for the first element. */
  if (eptr)
    sptr = lpNext(zl, eptr);

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (eptr && offset--) {
    Next(zl, &eptr, &sptr);
  }

  while (eptr && limit--) {
    double score = zzlGetScore(sptr);

    /* Abort when the node is no longer in range. */
    if (!IsUnder(score, range))
      break;

    /* We know the element exists, so lpGetValue should always
     * succeed */
    vstr = lpGetValue(eptr, &vlen, &vlong);

    AddResult(vstr, vlen, vlong, score);

    /* Move to next node */
    Next(zl, &eptr, &sptr);
  }
}

void IntervalVisitor::ExtractSkipList(const zrangespec& range) {
  zset* zs = (zset*)zobj_->ptr;
  zskiplist* zsl = zs->zsl;
  zskiplistNode* ln;
  unsigned offset = params_.offset;
  unsigned limit = params_.limit;

  /* If reversed, get the last node in range as starting point. */
  if (params_.reverse) {
    ln = zslLastInRange(zsl, &range);
  } else {
    ln = zslFirstInRange(zsl, &range);
  }

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (ln && offset--) {
    ln = Next(ln);
  }

  while (ln && limit--) {
    /* Abort when the node is no longer in range. */
    if (!IsUnder(ln->score, range))
      break;

    result_.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* Move to next node */
    ln = Next(ln);
  }
}

void IntervalVisitor::ExtractListPack(const zlexrangespec& range) {
  uint8_t* zl = (uint8_t*)zobj_->ptr;
  uint8_t *eptr, *sptr = nullptr;
  uint8_t* vstr = nullptr;
  unsigned int vlen = 0;
  long long vlong = 0;
  unsigned offset = params_.offset;
  unsigned limit = params_.limit;

  /* If reversed, get the last node in range as starting point. */
  if (params_.reverse) {
    eptr = zzlLastInLexRange(zl, &range);
  } else {
    eptr = zzlFirstInLexRange(zl, &range);
  }

  /* Get score pointer for the first element. */
  if (eptr)
    sptr = lpNext(zl, eptr);

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (eptr && offset--) {
    Next(zl, &eptr, &sptr);
  }

  while (eptr && limit--) {
    double score = 0;
    if (params_.with_scores) /* don't bother to extract the score if it's gonna be ignored. */
      score = zzlGetScore(sptr);

    /* Abort when the node is no longer in range. */
    if (params_.reverse) {
      if (!zzlLexValueGteMin(eptr, &range))
        break;
    } else {
      if (!zzlLexValueLteMax(eptr, &range))
        break;
    }

    vstr = lpGetValue(eptr, &vlen, &vlong);
    AddResult(vstr, vlen, vlong, score);

    /* Move to next node */
    Next(zl, &eptr, &sptr);
  }
}

void IntervalVisitor::ExtractSkipList(const zlexrangespec& range) {
  zset* zs = (zset*)zobj_->ptr;
  zskiplist* zsl = zs->zsl;
  zskiplistNode* ln;
  unsigned offset = params_.offset;
  unsigned limit = params_.limit;

  /* If reversed, get the last node in range as starting point. */
  if (params_.reverse) {
    ln = zslLastInLexRange(zsl, &range);
  } else {
    ln = zslFirstInLexRange(zsl, &range);
  }

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (ln && offset--) {
    ln = Next(ln);
  }

  while (ln && limit--) {
    /* Abort when the node is no longer in range. */
    if (params_.reverse) {
      if (!zslLexValueGteMin(ln->ele, &range))
        break;
    } else {
      if (!zslLexValueLteMax(ln->ele, &range))
        break;
    }

    result_.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* Move to next node */
    ln = Next(ln);
  }
}

void IntervalVisitor::PopListPack(ZSetFamily::TopNScored sc) {
  uint8_t* zl = (uint8_t*)zobj_->ptr;
  uint8_t *eptr, *sptr;
  uint8_t* vstr;
  unsigned int vlen = 0;
  long long vlong = 0;

  if (params_.reverse) {
    eptr = lpSeek(zl, -2);
  } else {
    eptr = lpSeek(zl, 0);
  }

  /* Get score pointer for the first element. */
  if (eptr)
    sptr = lpNext(zl, eptr);

  /* First we get the entries */
  unsigned int num = sc;
  while (eptr && num--) {
    double score = zzlGetScore(sptr);
    vstr = lpGetValue(eptr, &vlen, &vlong);
    AddResult(vstr, vlen, vlong, score);

    /* Move to next node */
    Next(zl, &eptr, &sptr);
  }

  int start = 0;
  if (params_.reverse) {
    /* If the number of elements to delete is greater than the listpack length,
     * we set the start to 0 because lpseek fails to search beyond length in reverse */
    start = (2 * sc > lpLength(zl)) ? 0 : -2 * sc;
  }

  /* We can finally delete the elements */
  zobj_->ptr = lpDeleteRange(zl, start, 2 * sc);
}

void IntervalVisitor::PopSkipList(ZSetFamily::TopNScored sc) {
  zset* zs = (zset*)zobj_->ptr;
  zskiplist* zsl = zs->zsl;
  zskiplistNode* ln;

  /* We start from the header, or the tail if reversed. */
  if (params_.reverse) {
    ln = zsl->tail;
  } else {
    ln = zsl->header->level[0].forward;
  }

  while (ln && sc--) {
    result_.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* we can delete the element now */
    zsetDel(zobj_, ln->ele);

    ln = Next(ln);
  }
}

void IntervalVisitor::AddResult(const uint8_t* vstr, unsigned vlen, long long vlong, double score) {
  if (vstr == NULL) {
    result_.emplace_back(absl::StrCat(vlong), score);
  } else {
    result_.emplace_back(string{reinterpret_cast<const char*>(vstr), vlen}, score);
  }
}

bool ParseBound(string_view src, ZSetFamily::Bound* bound) {
  if (src.empty())
    return false;

  if (src[0] == '(') {
    bound->is_open = true;
    src.remove_prefix(1);
  }

  return ParseDouble(src, &bound->val);
}

bool ParseLexBound(string_view src, ZSetFamily::LexBound* bound) {
  if (src.empty())
    return false;

  if (src == "+") {
    bound->type = ZSetFamily::LexBound::PLUS_INF;
  } else if (src == "-") {
    bound->type = ZSetFamily::LexBound::MINUS_INF;
  } else if (src[0] == '(') {
    bound->type = ZSetFamily::LexBound::OPEN;
    src.remove_prefix(1);
    bound->val = src;
  } else if (src[0] == '[') {
    bound->type = ZSetFamily::LexBound::CLOSED;
    src.remove_prefix(1);
    bound->val = src;
  } else {
    return false;
  }

  return true;
}

void SendAtLeastOneKeyError(ConnectionContext* cntx) {
  string name{cntx->cid->name()};
  absl::AsciiStrToLower(&name);
  (*cntx)->SendError(absl::StrCat("at least 1 input key is needed for ", name));
}

enum class AggType : uint8_t { SUM, MIN, MAX, NOOP };
using ScoredMap = absl::flat_hash_map<std::string, double>;

ScoredMap FromObject(const CompactObj& co, double weight) {
  robj* obj = co.AsRObj();
  ZSetFamily::RangeParams params;
  params.with_scores = true;
  IntervalVisitor vis(Action::RANGE, params, obj);
  vis(ZSetFamily::IndexInterval(0, -1));

  ZSetFamily::ScoredArray arr = vis.PopResult();
  ScoredMap res;
  res.reserve(arr.size());

  for (auto& elem : arr) {
    elem.second *= weight;
    res.emplace(move(elem));
  }

  return res;
}

double Aggregate(double v1, double v2, AggType atype) {
  switch (atype) {
    case AggType::SUM:
      return v1 + v2;
    case AggType::MAX:
      return max(v1, v2);
    case AggType::MIN:
      return min(v1, v2);
    case AggType::NOOP:
      return 0;
  }
  return 0;
}

// the result is in the destination.
void UnionScoredMap(ScoredMap* dest, ScoredMap* src, AggType agg_type) {
  ScoredMap* target = dest;
  ScoredMap* iter = src;

  if (iter->size() > target->size())
    swap(target, iter);

  for (const auto& elem : *iter) {
    auto [it, inserted] = target->emplace(elem);
    if (!inserted) {
      it->second = Aggregate(it->second, elem.second, agg_type);
    }
  }

  if (target != dest)
    dest->swap(*src);
}

void InterScoredMap(ScoredMap* dest, ScoredMap* src, AggType agg_type) {
  ScoredMap* target = dest;
  ScoredMap* iter = src;

  if (iter->size() > target->size())
    swap(target, iter);

  auto it = iter->begin();
  while (it != iter->end()) {
    auto inter_it = target->find(it->first);
    if (inter_it == target->end()) {
      auto copy_it = it++;
      iter->erase(copy_it);
    } else {
      it->second = Aggregate(it->second, inter_it->second, agg_type);
      ++it;
    }
  }

  if (iter != dest)
    dest->swap(*src);
}

using KeyIterWeightVec = vector<pair<PrimeIterator, double>>;

ScoredMap UnionShardKeysWithScore(const KeyIterWeightVec& key_iter_weight_vec, AggType agg_type) {
  ScoredMap result;
  for (const auto& key_iter_wieght : key_iter_weight_vec) {
    if (key_iter_wieght.first.is_done()) {
      continue;
    }

    ScoredMap sm = FromObject(key_iter_wieght.first->second, key_iter_wieght.second);
    if (result.empty()) {
      result.swap(sm);
    } else {
      UnionScoredMap(&result, &sm, agg_type);
    }
  }
  return result;
}

double GetKeyWeight(Transaction* t, ShardId shard_id, const vector<double>& weights,
                    unsigned key_index, unsigned cmdargs_keys_offset) {
  if (weights.empty()) {
    return 1;
  }

  unsigned windex = t->ReverseArgIndex(shard_id, key_index) - cmdargs_keys_offset;
  DCHECK_LT(windex, weights.size());
  return weights[windex];
}

OpResult<ScoredMap> OpUnion(EngineShard* shard, Transaction* t, string_view dest, AggType agg_type,
                            const vector<double>& weights, bool store) {
  ArgSlice keys = t->GetShardArgs(shard->shard_id());
  DVLOG(1) << "shard:" << shard->shard_id() << ", keys " << vector(keys.begin(), keys.end());
  DCHECK(!keys.empty());

  unsigned cmdargs_keys_offset = 1;  // after {numkeys} for ZUNION
  unsigned removed_keys = 0;

  if (store) {
    // first global index is 2 after {destkey, numkeys}.
    ++cmdargs_keys_offset;
    if (keys.front() == dest) {
      keys.remove_prefix(1);
      ++removed_keys;
    }

    // In case ONLY the destination key is hosted in this shard no work on this shard should be
    // done in this step
    if (keys.empty()) {
      return OpStatus::OK;
    }
  }

  auto& db_slice = shard->db_slice();
  KeyIterWeightVec key_weight_vec(keys.size());
  for (unsigned j = 0; j < keys.size(); ++j) {
    auto it_res = db_slice.Find(t->GetDbContext(), keys[j], OBJ_ZSET);
    if (it_res == OpStatus::WRONG_TYPE)  // TODO: support sets with default score 1.
      return it_res.status();
    if (!it_res)
      continue;

    key_weight_vec[j] = {*it_res, GetKeyWeight(t, shard->shard_id(), weights, j + removed_keys,
                                               cmdargs_keys_offset)};
  }

  return UnionShardKeysWithScore(key_weight_vec, agg_type);
}

ScoredMap ZSetFromSet(const PrimeValue& pv, double weight) {
  ScoredMap result;
  container_utils::IterateSet(pv, [&result, weight](container_utils::ContainerEntry ce) {
    result.emplace(ce.ToString(), weight);
    return true;
  });
  return result;
}

OpResult<ScoredMap> OpInter(EngineShard* shard, Transaction* t, string_view dest, AggType agg_type,
                            const vector<double>& weights, bool store) {
  ArgSlice keys = t->GetShardArgs(shard->shard_id());
  DVLOG(1) << "shard:" << shard->shard_id() << ", keys " << vector(keys.begin(), keys.end());
  DCHECK(!keys.empty());

  unsigned removed_keys = 0;
  unsigned cmdargs_keys_offset = 1;

  if (store) {
    // first global index is 2 after {destkey, numkeys}.
    ++cmdargs_keys_offset;

    if (keys.front() == dest) {
      keys.remove_prefix(1);
      ++removed_keys;
    }

    // In case ONLY the destination key is hosted in this shard no work on this shard should be
    // done in this step
    if (keys.empty()) {
      return OpStatus::SKIPPED;
    }
  }

  auto& db_slice = shard->db_slice();
  vector<pair<PrimeIterator, double>> it_arr(keys.size());
  if (it_arr.empty())          // could be when only the dest key is hosted in this shard
    return OpStatus::SKIPPED;  // return noop

  for (unsigned j = 0; j < keys.size(); ++j) {
    auto it_res = db_slice.FindExt(t->GetDbContext(), keys[j]).first;
    if (!IsValid(it_res))
      continue;  // we exit in the next loop

    // sets are supported for ZINTER* commands:
    auto obj_type = it_res->second.ObjType();
    if (obj_type != OBJ_ZSET && obj_type != OBJ_SET)
      return OpStatus::WRONG_TYPE;

    it_arr[j] = {
        it_res, GetKeyWeight(t, shard->shard_id(), weights, j + removed_keys, cmdargs_keys_offset)};
  }

  ScoredMap result;
  for (auto it = it_arr.begin(); it != it_arr.end(); ++it) {
    if (it->first.is_done()) {
      return ScoredMap{};
    }

    ScoredMap sm;
    if (it->first->second.ObjType() == OBJ_ZSET)
      sm = FromObject(it->first->second, it->second);
    else
      sm = ZSetFromSet(it->first->second, it->second);

    if (result.empty())
      result.swap(sm);
    else
      InterScoredMap(&result, &sm, agg_type);

    if (result.empty())
      return result;
  }

  return result;
}

using ScoredMemberView = std::pair<double, std::string_view>;
using ScoredMemberSpan = absl::Span<ScoredMemberView>;

struct AddResult {
  double new_score = 0;
  unsigned num_updated = 0;

  bool is_nan = false;
};

OpResult<AddResult> OpAdd(const OpArgs& op_args, const ZParams& zparams, string_view key,
                          ScoredMemberSpan members) {
  DCHECK(!members.empty() || zparams.override);
  auto& db_slice = op_args.shard->db_slice();

  if (zparams.override && members.empty()) {
    auto it = db_slice.FindExt(op_args.db_cntx, key).first;
    db_slice.Del(op_args.db_cntx.db_index, it);
    return OpStatus::OK;
  }

  OpResult<PrimeIterator> res_it = FindZEntry(zparams, op_args, key, members.front().second.size());

  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();

  unsigned added = 0;
  unsigned updated = 0;
  unsigned processed = 0;

  sds& tmp_str = op_args.shard->tmp_str1;
  double new_score = 0;
  int retflags = 0;

  OpStatus op_status = OpStatus::OK;
  AddResult aresult;

  for (size_t j = 0; j < members.size(); j++) {
    const auto& m = members[j];
    tmp_str = sdscpylen(tmp_str, m.second.data(), m.second.size());

    int retval = zsetAdd(zobj, m.first, tmp_str, zparams.flags, &retflags, &new_score);

    if (zparams.flags & ZADD_IN_INCR) {
      if (retval == 0) {
        CHECK_EQ(1u, members.size());

        aresult.is_nan = true;
        break;
      }

      if (retflags & ZADD_OUT_NOP) {
        op_status = OpStatus::SKIPPED;
      }
    }

    if (retflags & ZADD_OUT_ADDED)
      added++;
    if (retflags & ZADD_OUT_UPDATED)
      updated++;
    if (!(retflags & ZADD_OUT_NOP))
      processed++;
  }

  DVLOG(2) << "ZAdd " << zobj->ptr;

  res_it.value()->second.SyncRObj();
  op_args.shard->db_slice().PostUpdate(op_args.db_cntx.db_index, *res_it, key);

  if (zparams.flags & ZADD_IN_INCR) {
    aresult.new_score = new_score;
  } else {
    aresult.num_updated = zparams.ch ? added + updated : added;
  }

  if (op_status != OpStatus::OK)
    return op_status;
  return aresult;
}

struct SetOpArgs {
  AggType agg_type = AggType::SUM;
  unsigned num_keys;
  vector<double> weights;
  bool with_scores = false;
};

OpResult<void> FillAggType(string_view agg, SetOpArgs* op_args) {
  if (agg == "SUM") {
    op_args->agg_type = AggType::SUM;
  } else if (agg == "MIN") {
    op_args->agg_type = AggType::MIN;
  } else if (agg == "MAX") {
    op_args->agg_type = AggType::MAX;
  } else {
    return OpStatus::SYNTAX_ERR;
  }
  return OpStatus::OK;
}

// Parse functions return the number of arguments read from CmdArgList
OpResult<unsigned> ParseAggregate(CmdArgList args, bool store, SetOpArgs* op_args) {
  if (args.size() < 1) {
    return OpStatus::SYNTAX_ERR;
  }

  ToUpper(&args[1]);
  auto filled = FillAggType(ArgS(args, 1), op_args);
  if (!filled) {
    return filled.status();
  }
  return 1;
}

OpResult<unsigned> ParseWeights(CmdArgList args, SetOpArgs* op_args) {
  if (args.size() <= op_args->num_keys) {
    return OpStatus::SYNTAX_ERR;
  }

  op_args->weights.resize(op_args->num_keys, 1);
  for (unsigned i = 0; i < op_args->num_keys; ++i) {
    string_view weight = ArgS(args, i + 1);
    if (!absl::SimpleAtod(weight, &op_args->weights[i])) {
      return OpStatus::INVALID_FLOAT;
    }
  }

  return op_args->num_keys;
}

OpResult<void> ParseKeyCount(string_view arg_num_keys, SetOpArgs* op_args) {
  // we parsed the structure before, when transaction has been initialized.
  if (!absl::SimpleAtoi(arg_num_keys, &op_args->num_keys)) {
    return OpStatus::SYNTAX_ERR;
  }
  return OpStatus::OK;
}

OpResult<unsigned> ParseWithScores(CmdArgList args, SetOpArgs* op_args) {
  op_args->with_scores = true;
  return 0;
}

OpResult<SetOpArgs> ParseSetOpArgs(CmdArgList args, bool store) {
  string_view num_keys_str = store ? ArgS(args, 1) : ArgS(args, 0);
  SetOpArgs op_args;

  auto parsed = ParseKeyCount(num_keys_str, &op_args);
  if (!parsed) {
    return parsed.status();
  }

  unsigned opt_args_start = op_args.num_keys + (store ? 2 : 1);
  DCHECK_LE(opt_args_start, args.size());  // Checked inside DetermineKeys

  for (size_t i = opt_args_start; i < args.size(); ++i) {
    ToUpper(&args[i]);
    string_view arg = ArgS(args, i);
    if (arg == "WEIGHTS") {
      auto parsed_cnt = ParseWeights(args.subspan(i), &op_args);
      if (!parsed_cnt) {
        return parsed_cnt.status();
      }
      i += *parsed_cnt;
    } else if (arg == "AGGREGATE") {
      auto parsed_cnt = ParseAggregate(args.subspan(i), store, &op_args);
      if (!parsed_cnt) {
        return parsed_cnt.status();
      }
      i += *parsed_cnt;
    } else if (arg == "WITHSCORES") {
      // Commands with store capability does not offer WITHSCORES option
      if (store) {
        return OpStatus::SYNTAX_ERR;
      }
      auto parsed_cnt = ParseWithScores(args.subspan(i), &op_args);
      if (!parsed_cnt) {
        return parsed_cnt.status();
      }
      i += *parsed_cnt;
    } else {
      return OpStatus::SYNTAX_ERR;
    }
  }
  return op_args;
}

void ZUnionFamilyInternal(CmdArgList args, bool store, ConnectionContext* cntx) {
  OpResult<SetOpArgs> op_args_res = ParseSetOpArgs(args, store);
  if (!op_args_res) {
    switch (op_args_res.status()) {
      case OpStatus::INVALID_FLOAT:
        return (*cntx)->SendError("weight value is not a float", kSyntaxErrType);
      default:
        return (*cntx)->SendError(op_args_res.status());
    }
  }
  const auto& op_args = *op_args_res;
  if (op_args.num_keys == 0) {
    return SendAtLeastOneKeyError(cntx);
  }

  vector<OpResult<ScoredMap>> maps(shard_set->size());

  string_view dest_key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] = OpUnion(shard, t, dest_key, op_args.agg_type, op_args.weights, store);
    return OpStatus::OK;
  };

  cntx->transaction->Schedule();

  // For commands not storing computed result, this should be
  // the last transaction hop (e.g. ZUNION)
  cntx->transaction->Execute(std::move(cb), !store);

  ScoredMap result;
  for (auto& op_res : maps) {
    if (!op_res)
      return (*cntx)->SendError(op_res.status());
    UnionScoredMap(&result, &op_res.value(), op_args.agg_type);
  }

  vector<ScoredMemberView> smvec;
  for (const auto& elem : result) {
    smvec.emplace_back(elem.second, elem.first);
  }

  if (store) {
    ShardId dest_shard = Shard(dest_key, maps.size());
    AddResult add_result;
    auto store_cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == dest_shard) {
        ZParams zparams;
        zparams.override = true;
        add_result = OpAdd(t->GetOpArgs(shard), zparams, dest_key, ScoredMemberSpan{smvec}).value();
      }
      return OpStatus::OK;
    };
    cntx->transaction->Execute(std::move(store_cb), true);
    (*cntx)->SendLong(smvec.size());
  } else {
    std::sort(std::begin(smvec), std::end(smvec));
    (*cntx)->StartArray(smvec.size() * (op_args.with_scores ? 2 : 1));
    for (const auto& elem : smvec) {
      (*cntx)->SendBulkString(elem.second);
      if (op_args.with_scores) {
        (*cntx)->SendDouble(elem.first);
      }
    }
  }
}

bool ParseLimit(string_view offset_str, string_view limit_str, ZSetFamily::RangeParams* params) {
  int64_t limit_arg;
  if (!SimpleAtoi(offset_str, &params->offset) || !SimpleAtoi(limit_str, &limit_arg) ||
      limit_arg > UINT32_MAX) {
    return false;
  }
  params->limit = limit_arg < 0 ? UINT32_MAX : static_cast<uint32_t>(limit_arg);
  return true;
}

ZSetFamily::ScoredArray OpBZPop(Transaction* t, EngineShard* shard, std::string_view key,
                                bool is_max) {
  auto& db_slice = shard->db_slice();
  auto it_res = db_slice.Find(t->GetDbContext(), key, OBJ_ZSET);
  CHECK(it_res) << t->DebugId() << " " << key;  // must exist and must be ok.
  PrimeIterator it = *it_res;

  ZSetFamily::RangeParams range_params;
  range_params.reverse = is_max;
  range_params.with_scores = true;
  ZSetFamily::ZRangeSpec range_spec;
  range_spec.params = range_params;
  range_spec.interval = ZSetFamily::TopNScored(1);

  DVLOG(2) << "popping from " << key << " " << t->DebugId();
  db_slice.PreUpdate(t->GetDbIndex(), it);
  robj* zobj = it_res.value()->second.AsRObj();

  IntervalVisitor iv{Action::POP, range_spec.params, zobj};
  std::visit(iv, range_spec.interval);

  it_res.value()->second.SyncRObj();
  db_slice.PostUpdate(t->GetDbIndex(), *it_res, key);

  auto zlen = zsetLength(zobj);
  if (zlen == 0) {
    DVLOG(1) << "deleting key " << key << " " << t->DebugId();
    CHECK(db_slice.Del(t->GetDbIndex(), *it_res));
  }

  OpArgs op_args = t->GetOpArgs(shard);
  if (op_args.shard->journal()) {
    string command = is_max ? "ZPOPMAX" : "ZPOPMIN";
    RecordJournal(op_args, command, ArgSlice{key}, 1);
  }

  return iv.PopResult();
}

void BZPopMinMax(CmdArgList args, ConnectionContext* cntx, bool is_max) {
  DCHECK_GE(args.size(), 2u);

  float timeout;
  auto timeout_str = ArgS(args, args.size() - 1);
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return (*cntx)->SendError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return (*cntx)->SendError("timeout is negative");
  }
  VLOG(1) << "BZPop timeout(" << timeout << ")";

  Transaction* transaction = cntx->transaction;
  std::string popped_key;
  OpResult<ZSetFamily::ScoredArray> popped_array;
  OpStatus result = container_utils::RunCbOnFirstNonEmptyBlocking(
      [is_max, &popped_array](Transaction* t, EngineShard* shard, std::string_view key) {
        popped_array = OpBZPop(t, shard, key, is_max);
      },
      &popped_key, transaction, OBJ_ZSET, unsigned(timeout * 1000));

  if (result == OpStatus::OK) {
    DVLOG(1) << "BZPop " << transaction->DebugId() << " popped from key " << popped_key;  // key.
    CHECK(popped_array->size() == 1);
    (*cntx)->StartArray(3);
    (*cntx)->SendBulkString(popped_key);
    (*cntx)->SendBulkString(popped_array->front().first);
    return (*cntx)->SendDouble(popped_array->front().second);
  }

  DVLOG(1) << "result for " << transaction->DebugId() << " is " << result;

  switch (result) {
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    case OpStatus::TIMED_OUT:
      return (*cntx)->SendNullArray();
    default:
      LOG(ERROR) << "Unexpected error " << result;
  }
  return (*cntx)->SendNullArray();
}

}  // namespace

void ZSetFamily::BZPopMin(CmdArgList args, ConnectionContext* cntx) {
  BZPopMinMax(args, cntx, false);
}

void ZSetFamily::BZPopMax(CmdArgList args, ConnectionContext* cntx) {
  BZPopMinMax(args, cntx, true);
}

void ZSetFamily::ZAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  ZParams zparams;
  size_t i = 1;
  for (; i < args.size() - 1; ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);

    if (cur_arg == "XX") {
      zparams.flags |= ZADD_IN_XX;  // update only
    } else if (cur_arg == "NX") {
      zparams.flags |= ZADD_IN_NX;  // add new only.
    } else if (cur_arg == "GT") {
      zparams.flags |= ZADD_IN_GT;
    } else if (cur_arg == "LT") {
      zparams.flags |= ZADD_IN_LT;
    } else if (cur_arg == "CH") {
      zparams.ch = true;
    } else if (cur_arg == "INCR") {
      zparams.flags |= ZADD_IN_INCR;
    } else {
      break;
    }
  }

  if ((args.size() - i) % 2 != 0) {
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  if ((zparams.flags & ZADD_IN_INCR) && (i + 2 < args.size())) {
    (*cntx)->SendError("INCR option supports a single increment-element pair");
    return;
  }

  unsigned insert_mask = zparams.flags & (ZADD_IN_NX | ZADD_IN_XX);
  if (insert_mask == (ZADD_IN_NX | ZADD_IN_XX)) {
    (*cntx)->SendError(kNxXxErr);
    return;
  }

  constexpr auto kRangeOpt = ZADD_IN_GT | ZADD_IN_LT;
  if (((zparams.flags & ZADD_IN_NX) && (zparams.flags & kRangeOpt)) ||
      ((zparams.flags & kRangeOpt) == kRangeOpt)) {
    (*cntx)->SendError("GT, LT, and/or NX options at the same time are not compatible");
    return;
  }

  absl::InlinedVector<ScoredMemberView, 4> members;
  for (; i < args.size(); i += 2) {
    string_view cur_arg = ArgS(args, i);
    double val = 0;

    if (!ParseDouble(cur_arg, &val)) {
      VLOG(1) << "Bad score:" << cur_arg << "|";
      return (*cntx)->SendError(kInvalidFloatErr);
    }
    if (isnan(val)) {
      return (*cntx)->SendError(kScoreNaN);
    }
    string_view member = ArgS(args, i + 1);
    members.emplace_back(val, member);
  }
  DCHECK(cntx->transaction);

  absl::Span memb_sp{members.data(), members.size()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), zparams, key, memb_sp);
  };

  OpResult<AddResult> add_result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (base::_in(add_result.status(), {OpStatus::WRONG_TYPE, OpStatus::OUT_OF_MEMORY})) {
    return (*cntx)->SendError(add_result.status());
  }

  // KEY_NOTFOUND may happen in case of XX flag.
  if (add_result.status() == OpStatus::KEY_NOTFOUND) {
    if (zparams.flags & ZADD_IN_INCR)
      (*cntx)->SendNull();
    else
      (*cntx)->SendLong(0);
  } else if (add_result.status() == OpStatus::SKIPPED) {
    (*cntx)->SendNull();
  } else if (add_result->is_nan) {
    (*cntx)->SendError(kScoreNaN);
  } else {
    if (zparams.flags & ZADD_IN_INCR) {
      (*cntx)->SendDouble(add_result->new_score);
    } else {
      (*cntx)->SendLong(add_result->num_updated);
    }
  }
}

void ZSetFamily::ZCard(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    OpResult<PrimeIterator> find_res = shard->db_slice().Find(t->GetDbContext(), key, OBJ_ZSET);
    if (!find_res) {
      return find_res.status();
    }

    return zsetLength(find_res.value()->second.AsRObj());
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
    return;
  }

  (*cntx)->SendLong(result.value());
}

void ZSetFamily::ZCount(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  ScoreInterval si;
  if (!ParseBound(min_s, &si.first) || !ParseBound(max_s, &si.second)) {
    return (*cntx)->SendError(kFloatRangeErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCount(t->GetOpArgs(shard), key, si);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else {
    (*cntx)->SendLong(*result);
  }
}

vector<ScoredMap> OpFetch(EngineShard* shard, Transaction* t) {
  ArgSlice keys = t->GetShardArgs(shard->shard_id());
  DVLOG(1) << "shard:" << shard->shard_id() << ", keys " << vector(keys.begin(), keys.end());
  DCHECK(!keys.empty());

  vector<ScoredMap> results;
  results.reserve(keys.size());

  auto& db_slice = shard->db_slice();
  for (size_t i = 0; i < keys.size(); ++i) {
    auto it = db_slice.Find(t->GetDbContext(), keys[i], OBJ_ZSET);
    if (!it) {
      results.push_back({});
      continue;
    }

    ScoredMap sm = FromObject((*it)->second, 1);
    results.push_back(std::move(sm));
  }

  return results;
}

void ZSetFamily::ZDiff(CmdArgList args, ConnectionContext* cntx) {
  vector<vector<ScoredMap>> maps(shard_set->size());
  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] = OpFetch(shard, t);
    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));

  const string_view key = ArgS(args, 1);
  const ShardId sid = Shard(key, maps.size());
  // Extract the ScoredMap of the first key
  auto& sm = maps[sid];
  if (sm.empty()) {
    (*cntx)->SendEmptyArray();
    return;
  }
  auto result = std::move(sm[0]);
  sm.erase(sm.begin());

  auto filter = [&result](const auto& key) mutable {
    auto it = result.find(key);
    if (it != result.end()) {
      result.erase(it);
    }
  };

  // Total O(L)
  // Iterate over the results of each shard
  for (auto& vsm : maps) {
    // Iterate over each fetched set
    for (auto& sm : vsm) {
      // Iterate over each key in the fetched set and filter
      for (auto& [key, value] : sm) {
        filter(key);
      }
    }
  }

  vector<ScoredMemberView> smvec;
  for (const auto& elem : result) {
    smvec.emplace_back(elem.second, elem.first);
  }

  // Total O(KlogK)
  std::sort(std::begin(smvec), std::end(smvec));

  const bool with_scores = ArgS(args, args.size() - 1) == "WITHSCORES";
  (*cntx)->StartArray(result.size() * (with_scores ? 2 : 1));
  for (const auto& [score, key] : smvec) {
    (*cntx)->SendBulkString(key);
    if (with_scores) {
      (*cntx)->SendDouble(score);
    }
  }
}

void ZSetFamily::ZIncrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view score_arg = ArgS(args, 1);

  ScoredMemberView scored_member;
  scored_member.second = ArgS(args, 2);

  if (!absl::SimpleAtod(score_arg, &scored_member.first)) {
    VLOG(1) << "Bad score:" << score_arg << "|";
    return (*cntx)->SendError(kInvalidFloatErr);
  }

  if (isnan(scored_member.first)) {
    return (*cntx)->SendError(kScoreNaN);
  }

  ZParams zparams;
  zparams.flags = ZADD_IN_INCR;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), zparams, key, ScoredMemberSpan{&scored_member, 1});
  };

  OpResult<AddResult> add_result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (add_result.status() == OpStatus::WRONG_TYPE) {
    return (*cntx)->SendError(kWrongTypeErr);
  }

  if (add_result.status() == OpStatus::SKIPPED) {
    return (*cntx)->SendNull();
  }

  if (add_result->is_nan) {
    return (*cntx)->SendError(kScoreNaN);
  }

  (*cntx)->SendDouble(add_result->new_score);
}

void ZSetFamily::ZInterStore(CmdArgList args, ConnectionContext* cntx) {
  string_view dest_key = ArgS(args, 0);
  OpResult<SetOpArgs> op_args_res = ParseSetOpArgs(args, true);

  if (!op_args_res) {
    switch (op_args_res.status()) {
      case OpStatus::INVALID_FLOAT:
        return (*cntx)->SendError("weight value is not a float", kSyntaxErrType);
      default:
        return (*cntx)->SendError(op_args_res.status());
    }
  }
  const auto& op_args = *op_args_res;
  if (op_args.num_keys == 0) {
    return SendAtLeastOneKeyError(cntx);
  }

  vector<OpResult<ScoredMap>> maps(shard_set->size(), OpStatus::SKIPPED);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] = OpInter(shard, t, dest_key, op_args.agg_type, op_args.weights, true);
    return OpStatus::OK;
  };

  cntx->transaction->Schedule();
  cntx->transaction->Execute(std::move(cb), false);

  ScoredMap result;
  for (auto& op_res : maps) {
    if (op_res.status() == OpStatus::SKIPPED)
      continue;

    if (!op_res)
      return (*cntx)->SendError(op_res.status());

    if (result.empty()) {
      result.swap(op_res.value());
    } else {
      InterScoredMap(&result, &op_res.value(), op_args.agg_type);
    }

    if (result.empty())
      break;
  }

  ShardId dest_shard = Shard(dest_key, maps.size());
  AddResult add_result;
  vector<ScoredMemberView> smvec;
  for (const auto& elem : result) {
    smvec.emplace_back(elem.second, elem.first);
  }

  auto store_cb = [&](Transaction* t, EngineShard* shard) {
    if (shard->shard_id() == dest_shard) {
      ZParams zparams;
      zparams.override = true;
      add_result = OpAdd(t->GetOpArgs(shard), zparams, dest_key, ScoredMemberSpan{smvec}).value();
    }
    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);

  (*cntx)->SendLong(smvec.size());
}

void ZSetFamily::ZInterCard(CmdArgList args, ConnectionContext* cntx) {
  unsigned num_keys;
  if (!absl::SimpleAtoi(ArgS(args, 0), &num_keys)) {
    return (*cntx)->SendError(OpStatus::SYNTAX_ERR);
  }

  uint64_t limit = 0;
  if (args.size() == (1 + num_keys + 2) && ArgS(args, 1 + num_keys) == "LIMIT") {
    if (!absl::SimpleAtoi(ArgS(args, 1 + num_keys + 1), &limit)) {
      return (*cntx)->SendError("limit value is not a positive integer", kSyntaxErrType);
    }
  } else if (args.size() != 1 + num_keys) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  vector<OpResult<ScoredMap>> maps(shard_set->size(), OpStatus::SKIPPED);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] = OpInter(shard, t, "", AggType::NOOP, {}, false);
    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));

  ScoredMap result;
  for (auto& op_res : maps) {
    if (op_res.status() == OpStatus::SKIPPED)
      continue;

    if (!op_res)
      return (*cntx)->SendError(op_res.status());

    if (result.empty()) {
      result.swap(op_res.value());
    } else {
      InterScoredMap(&result, &op_res.value(), AggType::NOOP);
    }

    if (result.empty())
      break;
  }

  if (0 < limit && limit < result.size()) {
    return (*cntx)->SendLong(limit);
  }
  (*cntx)->SendLong(result.size());
}

void ZSetFamily::ZPopMax(CmdArgList args, ConnectionContext* cntx) {
  ZPopMinMax(std::move(args), true, cntx);
}

void ZSetFamily::ZPopMin(CmdArgList args, ConnectionContext* cntx) {
  ZPopMinMax(std::move(args), false, cntx);
}

void ZSetFamily::ZLexCount(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  LexInterval li;
  if (!ParseLexBound(min_s, &li.first) || !ParseLexBound(max_s, &li.second)) {
    return (*cntx)->SendError(kLexRangeErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpLexCount(t->GetOpArgs(shard), key, li);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else {
    (*cntx)->SendLong(*result);
  }
}

void ZSetFamily::ZRange(CmdArgList args, ConnectionContext* cntx) {
  RangeParams range_params;

  for (size_t i = 3; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);
    if (cur_arg == "BYSCORE") {
      if (range_params.interval_type == RangeParams::IntervalType::LEX) {
        return (*cntx)->SendError("BYSCORE and BYLEX options are not compatible");
      }
      range_params.interval_type = RangeParams::IntervalType::SCORE;
    } else if (cur_arg == "BYLEX") {
      if (range_params.interval_type == RangeParams::IntervalType::SCORE) {
        return (*cntx)->SendError("BYSCORE and BYLEX options are not compatible");
      }
      range_params.interval_type = RangeParams::IntervalType::LEX;
    } else if (cur_arg == "REV") {
      range_params.reverse = true;
    } else if (cur_arg == "WITHSCORES") {
      range_params.with_scores = true;
    } else if (cur_arg == "LIMIT") {
      if (i + 3 > args.size()) {
        return (*cntx)->SendError(kSyntaxErr);
      }
      if (!ParseLimit(ArgS(args, i + 1), ArgS(args, i + 2), &range_params)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
      i += 2;
    } else {
      return cntx->reply_builder()->SendError(absl::StrCat("unsupported option ", cur_arg));
    }
  }
  ZRangeGeneric(std::move(args), range_params, cntx);
}

void ZSetFamily::ZRank(CmdArgList args, ConnectionContext* cntx) {
  ZRankGeneric(std::move(args), false, cntx);
}

void ZSetFamily::ZRevRange(CmdArgList args, ConnectionContext* cntx) {
  RangeParams range_params;
  range_params.reverse = true;

  for (size_t i = 3; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);
    if (cur_arg == "WITHSCORES") {
      range_params.with_scores = true;
    } else {
      return cntx->reply_builder()->SendError(absl::StrCat("unsupported option ", cur_arg));
    }
  }

  ZRangeGeneric(std::move(args), range_params, cntx);
}

void ZSetFamily::ZRevRangeByScore(CmdArgList args, ConnectionContext* cntx) {
  ZRangeByScoreInternal(std::move(args), true, cntx);
}

void ZSetFamily::ZRevRank(CmdArgList args, ConnectionContext* cntx) {
  ZRankGeneric(std::move(args), true, cntx);
}

void ZSetFamily::ZRangeByLex(CmdArgList args, ConnectionContext* cntx) {
  ZRangeByLexInternal(std::move(args), false, cntx);
}
void ZSetFamily::ZRevRangeByLex(CmdArgList args, ConnectionContext* cntx) {
  ZRangeByLexInternal(std::move(args), true, cntx);
}

void ZSetFamily::ZRangeByLexInternal(CmdArgList args, bool reverse, ConnectionContext* cntx) {
  uint32_t offset = 0;
  uint32_t count = kuint32max;

  RangeParams range_params;
  range_params.interval_type = RangeParams::IntervalType::LEX;
  range_params.reverse = reverse;

  if (args.size() > 3) {
    if (args.size() != 6)
      return (*cntx)->SendError(kSyntaxErr);

    ToUpper(&args[3]);
    if (ArgS(args, 3) != "LIMIT")
      return (*cntx)->SendError(kSyntaxErr);

    if (!ParseLimit(ArgS(args, 4), ArgS(args, 5), &range_params))
      return (*cntx)->SendError(kInvalidIntErr);
  }
  range_params.offset = offset;
  range_params.limit = count;

  ZRangeGeneric(args, range_params, cntx);
}

void ZSetFamily::ZRangeByScore(CmdArgList args, ConnectionContext* cntx) {
  ZRangeByScoreInternal(std::move(args), false, cntx);
}

void ZSetFamily::ZRemRangeByRank(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  IndexInterval ii;
  if (!SimpleAtoi(min_s, &ii.first) || !SimpleAtoi(max_s, &ii.second)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  ZRangeSpec range_spec;
  range_spec.interval = ii;
  ZRemRangeGeneric(key, range_spec, cntx);
}

void ZSetFamily::ZRemRangeByScore(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  ScoreInterval si;
  if (!ParseBound(min_s, &si.first) || !ParseBound(max_s, &si.second)) {
    return (*cntx)->SendError(kFloatRangeErr);
  }

  ZRangeSpec range_spec;

  range_spec.interval = si;

  ZRemRangeGeneric(key, range_spec, cntx);
}

void ZSetFamily::ZRemRangeByLex(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  LexInterval li;
  if (!ParseLexBound(min_s, &li.first) || !ParseLexBound(max_s, &li.second)) {
    return (*cntx)->SendError(kLexRangeErr);
  }

  ZRangeSpec range_spec;

  range_spec.interval = li;

  ZRemRangeGeneric(key, range_spec, cntx);
}

void ZSetFamily::ZRem(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  absl::InlinedVector<string_view, 8> members(args.size() - 1);
  for (size_t i = 1; i < args.size(); ++i) {
    members[i - 1] = ArgS(args, i);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(t->GetOpArgs(shard), key, members);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else {
    (*cntx)->SendLong(*result);
  }
}

void ZSetFamily::ZScore(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view member = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScore(t->GetOpArgs(shard), key, member);
  };

  OpResult<double> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else if (!result) {
    (*cntx)->SendNull();
  } else {
    (*cntx)->SendDouble(*result);
  }
}

void ZSetFamily::ZMScore(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  absl::InlinedVector<string_view, 8> members(args.size() - 1);
  for (size_t i = 1; i < args.size(); ++i) {
    members[i - 1] = ArgS(args, i);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpMScore(t->GetOpArgs(shard), key, members);
  };

  OpResult<MScoreResponse> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    return (*cntx)->SendError(kWrongTypeErr);
  }

  (*cntx)->StartArray(result->size());  // Array return type.
  const MScoreResponse& array = result.value();
  for (const auto& p : array) {
    if (p) {
      (*cntx)->SendDouble(*p);
    } else {
      (*cntx)->SendNull();
    }
  }
}

void ZSetFamily::ZScan(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view token = ArgS(args, 1);

  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(2));
  if (!ops) {
    DVLOG(1) << "Scan invalid args - return " << ops << " to the user";
    return (*cntx)->SendError(ops.status());
  }
  ScanOpts scan_op = ops.value();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(t->GetOpArgs(shard), key, &cursor, scan_op);
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() != OpStatus::WRONG_TYPE) {
    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString(absl::StrCat(cursor));
    (*cntx)->StartArray(result->size());  // Within scan the returned page is of type array.
    for (const auto& k : *result) {
      (*cntx)->SendBulkString(k);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void ZSetFamily::ZUnion(CmdArgList args, ConnectionContext* cntx) {
  ZUnionFamilyInternal(args, false, cntx);
}

void ZSetFamily::ZUnionStore(CmdArgList args, ConnectionContext* cntx) {
  ZUnionFamilyInternal(args, true, cntx);
}

void ZSetFamily::ZRangeByScoreInternal(CmdArgList args, bool reverse, ConnectionContext* cntx) {
  RangeParams range_params;
  range_params.interval_type = RangeParams::IntervalType::SCORE;
  range_params.reverse = reverse;
  if (!ParseRangeByScoreParams(args.subspan(3), &range_params)) {
    return (*cntx)->SendError(kSyntaxErr);
  }
  ZRangeGeneric(args, range_params, cntx);
}

void ZSetFamily::OutputScoredArrayResult(const OpResult<ScoredArray>& result,
                                         const RangeParams& params, ConnectionContext* cntx) {
  if (result.status() == OpStatus::WRONG_TYPE) {
    return (*cntx)->SendError(kWrongTypeErr);
  }

  LOG_IF(WARNING, !result && result.status() != OpStatus::KEY_NOTFOUND)
      << "Unexpected status " << result.status();
  (*cntx)->SendScoredArray(result.value(), params.with_scores);
}

void ZSetFamily::ZRemRangeGeneric(string_view key, const ZRangeSpec& range_spec,
                                  ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRemRange(t->GetOpArgs(shard), key, range_spec);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else {
    (*cntx)->SendLong(*result);
  }
}

void ZSetFamily::ZRangeGeneric(CmdArgList args, RangeParams range_params, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  ZRangeSpec range_spec;
  range_spec.params = range_params;

  switch (range_params.interval_type) {
    case RangeParams::IntervalType::SCORE: {
      ScoreInterval si;
      if (!ParseBound(min_s, &si.first) || !ParseBound(max_s, &si.second)) {
        return (*cntx)->SendError(kFloatRangeErr);
      }
      range_spec.interval = si;
      break;
    }
    case RangeParams::IntervalType::LEX: {
      LexInterval li;
      if (!ParseLexBound(min_s, &li.first) || !ParseLexBound(max_s, &li.second)) {
        return (*cntx)->SendError(kLexRangeErr);
      }
      range_spec.interval = li;
      break;
    }
    case RangeParams::IntervalType::RANK: {
      IndexInterval ii;
      if (!SimpleAtoi(min_s, &ii.first) || !SimpleAtoi(max_s, &ii.second)) {
        (*cntx)->SendError(kInvalidIntErr);
        return;
      }
      range_spec.interval = ii;
      break;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(range_spec, t->GetOpArgs(shard), key);
  };

  OpResult<ScoredArray> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  OutputScoredArrayResult(result, range_params, cntx);
}

void ZSetFamily::ZRankGeneric(CmdArgList args, bool reverse, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view member = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRank(t->GetOpArgs(shard), key, member, reverse);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    (*cntx)->SendNull();
  } else {
    (*cntx)->SendError(result.status());
  }
}

bool ZSetFamily::ParseRangeByScoreParams(CmdArgList args, RangeParams* params) {
  for (size_t i = 0; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);
    if (cur_arg == "WITHSCORES") {
      params->with_scores = true;
    } else if (cur_arg == "LIMIT") {
      if (i + 3 > args.size())
        return false;
      if (!ParseLimit(ArgS(args, i + 1), ArgS(args, i + 2), params))
        return false;

      i += 2;
    } else {
      return false;
    }
  }

  return true;
}

void ZSetFamily::ZPopMinMax(CmdArgList args, bool reverse, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  RangeParams range_params;
  range_params.reverse = reverse;
  range_params.with_scores = true;
  ZRangeSpec range_spec;
  range_spec.params = range_params;

  TopNScored sc = 1;
  if (args.size() > 1) {
    string_view count = ArgS(args, 1);
    if (!SimpleAtoi(count, &sc)) {
      return (*cntx)->SendError(kUintErr);
    }
  }

  range_spec.interval = sc;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPopCount(range_spec, t->GetOpArgs(shard), key);
  };

  OpResult<ScoredArray> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  OutputScoredArrayResult(result, range_params, cntx);
}

OpResult<StringVec> ZSetFamily::OpScan(const OpArgs& op_args, std::string_view key,
                                       uint64_t* cursor, const ScanOpts& scan_op) {
  OpResult<PrimeIterator> find_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);

  if (!find_res)
    return find_res.status();

  PrimeIterator it = find_res.value();
  StringVec res;
  robj* zobj = it->second.AsRObj();
  char buf[128];

  if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
    RangeParams params;
    params.with_scores = true;
    IntervalVisitor iv{Action::RANGE, params, zobj};

    iv(IndexInterval{0, kuint32max});
    ScoredArray arr = iv.PopResult();

    for (size_t i = 0; i < arr.size(); ++i) {
      if (!scan_op.Matches(arr[i].first)) {
        continue;
      }
      res.emplace_back(std::move(arr[i].first));
      char* str = RedisReplyBuilder::FormatDouble(arr[i].second, buf, sizeof(buf));
      res.emplace_back(str);
    }
    *cursor = 0;
  } else {
    CHECK_EQ(unsigned(OBJ_ENCODING_SKIPLIST), zobj->encoding);
    uint32_t count = scan_op.limit;
    zset* zs = (zset*)zobj->ptr;

    dict* ht = zs->dict;
    long maxiterations = count * 10;

    struct ScanArgs {
      char* sbuf;
      StringVec* res;
      const ScanOpts* scan_op;
    } sargs = {buf, &res, &scan_op};

    auto scanCb = [](void* privdata, const dictEntry* de) {
      ScanArgs* sargs = (ScanArgs*)privdata;

      sds key = (sds)de->key;
      if (!sargs->scan_op->Matches(key)) {
        return;
      }

      double score = *(double*)dictGetVal(de);

      sargs->res->emplace_back(key, sdslen(key));
      char* str = RedisReplyBuilder::FormatDouble(score, sargs->sbuf, sizeof(buf));
      sargs->res->emplace_back(str);
    };

    do {
      *cursor = dictScan(ht, *cursor, scanCb, NULL, &sargs);
    } while (*cursor && maxiterations-- && res.size() < count);
  }

  return res;
}

OpResult<unsigned> ZSetFamily::OpRem(const OpArgs& op_args, string_view key, ArgSlice members) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  db_slice.PreUpdate(op_args.db_cntx.db_index, *res_it);
  robj* zobj = res_it.value()->second.AsRObj();
  sds& tmp_str = op_args.shard->tmp_str1;
  unsigned deleted = 0;
  for (string_view member : members) {
    tmp_str = sdscpylen(tmp_str, member.data(), member.size());
    deleted += zsetDel(zobj, tmp_str);
  }
  auto zlen = zsetLength(zobj);
  res_it.value()->second.SyncRObj();
  db_slice.PostUpdate(op_args.db_cntx.db_index, *res_it, key);

  if (zlen == 0) {
    CHECK(op_args.shard->db_slice().Del(op_args.db_cntx.db_index, res_it.value()));
  }

  return deleted;
}

OpResult<double> ZSetFamily::OpScore(const OpArgs& op_args, string_view key, string_view member) {
  OpResult<PrimeIterator> res_it = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  sds& tmp_str = op_args.shard->tmp_str1;
  tmp_str = sdscpylen(tmp_str, member.data(), member.size());
  double score;
  int retval = zsetScore(zobj, tmp_str, &score);
  if (retval != C_OK) {
    return OpStatus::KEY_NOTFOUND;
  }
  return score;
}

OpResult<ZSetFamily::MScoreResponse> ZSetFamily::OpMScore(const OpArgs& op_args, string_view key,
                                                          ArgSlice members) {
  OpResult<PrimeIterator> res_it = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  MScoreResponse scores(members.size());

  robj* zobj = res_it.value()->second.AsRObj();
  sds& tmp_str = op_args.shard->tmp_str1;

  for (size_t i = 0; i < members.size(); i++) {
    const auto& m = members[i];

    tmp_str = sdscpylen(tmp_str, m.data(), m.size());
    double score;
    int retval = zsetScore(zobj, tmp_str, &score);
    if (retval == C_OK) {
      scores[i] = score;
    } else {
      scores[i] = std::nullopt;
    }
  }

  return scores;
}

auto ZSetFamily::OpPopCount(const ZRangeSpec& range_spec, const OpArgs& op_args, string_view key)
    -> OpResult<ScoredArray> {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  db_slice.PreUpdate(op_args.db_cntx.db_index, *res_it);

  robj* zobj = res_it.value()->second.AsRObj();

  IntervalVisitor iv{Action::POP, range_spec.params, zobj};
  std::visit(iv, range_spec.interval);

  res_it.value()->second.SyncRObj();
  db_slice.PostUpdate(op_args.db_cntx.db_index, *res_it, key);

  auto zlen = zsetLength(zobj);
  if (zlen == 0) {
    CHECK(op_args.shard->db_slice().Del(op_args.db_cntx.db_index, res_it.value()));
  }

  return iv.PopResult();
}

auto ZSetFamily::OpRange(const ZRangeSpec& range_spec, const OpArgs& op_args, string_view key)
    -> OpResult<ScoredArray> {
  OpResult<PrimeIterator> res_it = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  IntervalVisitor iv{Action::RANGE, range_spec.params, zobj};

  std::visit(iv, range_spec.interval);

  return iv.PopResult();
}

OpResult<unsigned> ZSetFamily::OpRemRange(const OpArgs& op_args, string_view key,
                                          const ZRangeSpec& range_spec) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  db_slice.PreUpdate(op_args.db_cntx.db_index, *res_it);

  robj* zobj = res_it.value()->second.AsRObj();

  IntervalVisitor iv{Action::REMOVE, range_spec.params, zobj};
  std::visit(iv, range_spec.interval);

  res_it.value()->second.SyncRObj();
  db_slice.PostUpdate(op_args.db_cntx.db_index, *res_it, key);

  auto zlen = zsetLength(zobj);
  if (zlen == 0) {
    CHECK(op_args.shard->db_slice().Del(op_args.db_cntx.db_index, res_it.value()));
  }

  return iv.removed();
}

OpResult<unsigned> ZSetFamily::OpRank(const OpArgs& op_args, string_view key, string_view member,
                                      bool reverse) {
  OpResult<PrimeIterator> res_it = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  op_args.shard->tmp_str1 = sdscpylen(op_args.shard->tmp_str1, member.data(), member.size());

  long res = zsetRank(zobj, op_args.shard->tmp_str1, reverse);
  if (res < 0)
    return OpStatus::KEY_NOTFOUND;
  return res;
}

OpResult<unsigned> ZSetFamily::OpCount(const OpArgs& op_args, std::string_view key,
                                       const ScoreInterval& interval) {
  OpResult<PrimeIterator> res_it = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  zrangespec range = GetZrangeSpec(false, interval);
  unsigned count = 0;

  if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = (uint8_t*)zobj->ptr;
    uint8_t *eptr, *sptr;
    double score;

    /* Use the first element in range as the starting point */
    eptr = zzlFirstInRange(zl, &range);

    /* No "first" element */
    if (eptr == NULL) {
      return 0;
    }

    /* First element is in range */
    sptr = lpNext(zl, eptr);
    score = zzlGetScore(sptr);

    DCHECK(zslValueLteMax(score, &range));

    /* Iterate over elements in range */
    while (eptr) {
      score = zzlGetScore(sptr);

      /* Abort when the node is no longer in range. */
      if (!zslValueLteMax(score, &range)) {
        break;
      } else {
        count++;
        zzlNext(zl, &eptr, &sptr);
      }
    }
  } else {
    CHECK_EQ(unsigned(OBJ_ENCODING_SKIPLIST), zobj->encoding);
    zset* zs = (zset*)zobj->ptr;
    zskiplist* zsl = zs->zsl;
    zskiplistNode* zn;
    unsigned long rank;

    /* Find first element in range */
    zn = zslFirstInRange(zsl, &range);

    /* Use rank of first element, if any, to determine preliminary count */
    if (zn == NULL)
      return 0;

    rank = zslGetRank(zsl, zn->score, zn->ele);
    count = (zsl->length - (rank - 1));

    /* Find last element in range */
    zn = zslLastInRange(zsl, &range);

    /* Use rank of last element, if any, to determine the actual count */
    if (zn != NULL) {
      rank = zslGetRank(zsl, zn->score, zn->ele);
      count -= (zsl->length - rank);
    }
  }

  return count;
}

OpResult<unsigned> ZSetFamily::OpLexCount(const OpArgs& op_args, string_view key,
                                          const ZSetFamily::LexInterval& interval) {
  OpResult<PrimeIterator> res_it = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  zlexrangespec range = GetLexRange(false, interval);
  unsigned count = 0;
  if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* zl = (uint8_t*)zobj->ptr;
    uint8_t *eptr, *sptr;

    /* Use the first element in range as the starting point */
    eptr = zzlFirstInLexRange(zl, &range);

    /* No "first" element */
    if (eptr) {
      /* First element is in range */
      sptr = lpNext(zl, eptr);
      serverAssertWithInfo(c, zobj, zzlLexValueLteMax(eptr, &range));

      /* Iterate over elements in range */
      while (eptr) {
        /* Abort when the node is no longer in range. */
        if (!zzlLexValueLteMax(eptr, &range)) {
          break;
        } else {
          count++;
          zzlNext(zl, &eptr, &sptr);
        }
      }
    }
  } else {
    DCHECK_EQ(OBJ_ENCODING_SKIPLIST, zobj->encoding);
    zset* zs = (zset*)zobj->ptr;
    zskiplist* zsl = zs->zsl;
    zskiplistNode* zn;
    unsigned long rank;

    /* Find first element in range */
    zn = zslFirstInLexRange(zsl, &range);

    /* Use rank of first element, if any, to determine preliminary count */
    if (zn != NULL) {
      rank = zslGetRank(zsl, zn->score, zn->ele);
      count = (zsl->length - (rank - 1));

      /* Find last element in range */
      zn = zslLastInLexRange(zsl, &range);

      /* Use rank of last element, if any, to determine the actual count */
      if (zn != NULL) {
        rank = zslGetRank(zsl, zn->score, zn->ele);
        count -= (zsl->length - rank);
      }
    }
  }

  zslFreeLexRange(&range);
  return count;
}

#define HFUNC(x) SetHandler(&ZSetFamily::x)

void ZSetFamily::Register(CommandRegistry* registry) {
  constexpr uint32_t kStoreMask = CO::WRITE | CO::VARIADIC_KEYS | CO::REVERSE_MAPPING;

  *registry
      << CI{"ZADD", CO::FAST | CO::WRITE | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(ZAdd)
      << CI{"BZPOPMIN", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2, 1}
             .HFUNC(BZPopMin)
      << CI{"BZPOPMAX", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2, 1}
             .HFUNC(BZPopMax)
      << CI{"ZCARD", CO::FAST | CO::READONLY, 2, 1, 1, 1}.HFUNC(ZCard)
      << CI{"ZCOUNT", CO::FAST | CO::READONLY, 4, 1, 1, 1}.HFUNC(ZCount)
      << CI{"ZDIFF", CO::READONLY | CO::VARIADIC_KEYS, -3, 2, 2, 1}.HFUNC(ZDiff)
      << CI{"ZINCRBY", CO::FAST | CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(ZIncrBy)
      << CI{"ZINTERSTORE", kStoreMask, -4, 3, 3, 1}.HFUNC(ZInterStore)
      << CI{"ZINTERCARD", CO::READONLY | CO::REVERSE_MAPPING | CO::VARIADIC_KEYS, -3, 2, 2, 1}
             .HFUNC(ZInterCard)
      << CI{"ZLEXCOUNT", CO::READONLY, 4, 1, 1, 1}.HFUNC(ZLexCount)
      << CI{"ZPOPMAX", CO::FAST | CO::WRITE, -2, 1, 1, 1}.HFUNC(ZPopMax)
      << CI{"ZPOPMIN", CO::FAST | CO::WRITE, -2, 1, 1, 1}.HFUNC(ZPopMin)
      << CI{"ZREM", CO::FAST | CO::WRITE, -3, 1, 1, 1}.HFUNC(ZRem)
      << CI{"ZRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRange)
      << CI{"ZRANK", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ZRank)
      << CI{"ZRANGEBYLEX", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRangeByLex)
      << CI{"ZRANGEBYSCORE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRangeByScore)
      << CI{"ZSCORE", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ZScore)
      << CI{"ZMSCORE", CO::READONLY | CO::FAST, -3, 1, 1, 1}.HFUNC(ZMScore)
      << CI{"ZREMRANGEBYRANK", CO::WRITE, 4, 1, 1, 1}.HFUNC(ZRemRangeByRank)
      << CI{"ZREMRANGEBYSCORE", CO::WRITE, 4, 1, 1, 1}.HFUNC(ZRemRangeByScore)
      << CI{"ZREMRANGEBYLEX", CO::WRITE, 4, 1, 1, 1}.HFUNC(ZRemRangeByLex)
      << CI{"ZREVRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRevRange)
      << CI{"ZREVRANGEBYLEX", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRevRangeByLex)
      << CI{"ZREVRANGEBYSCORE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRevRangeByScore)
      << CI{"ZREVRANK", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ZRevRank)
      << CI{"ZSCAN", CO::READONLY, -3, 1, 1, 1}.HFUNC(ZScan)
      << CI{"ZUNION", CO::READONLY | CO::REVERSE_MAPPING | CO::VARIADIC_KEYS, -3, 2, 2, 1}.HFUNC(
             ZUnion)
      << CI{"ZUNIONSTORE", kStoreMask, -4, 3, 3, 1}.HFUNC(ZUnionStore);
}

}  // namespace dfly
