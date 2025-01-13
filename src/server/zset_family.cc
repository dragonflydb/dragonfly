// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

#include "server/acl/acl_commands_def.h"

extern "C" {
#include "redis/geo.h"
#include "redis/geohash.h"
#include "redis/geohash_helper.h"
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

#include "base/logging.h"
#include "base/stl_util.h"
#include "core/sorted_map.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;
using absl::SimpleAtoi;
namespace {

using CI = CommandId;

static const char kNxXxErr[] = "XX and NX options at the same time are not compatible";
static const char kLexRangeErr[] = "min or max not valid string range item";
static const char kFloatRangeErr[] = "min or max is not a float";
static const char kScoreNaN[] = "resulting score is not a number (NaN)";

using MScoreResponse = std::vector<std::optional<double>>;
using ScoredMember = ZSetFamily::ScoredMember;
using ScoredArray = ZSetFamily::ScoredArray;
using ScoredMemberView = ZSetFamily::ScoredMemberView;
using ScoredMemberSpan = ZSetFamily::ScoredMemberSpan;

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
  range.maxex = (interval.second.type == ZSetFamily::LexBound::OPEN);

  return range;
}

bool IsListPack(const detail::RobjWrapper* robj_wrapper) {
  return robj_wrapper->encoding() == OBJ_ENCODING_LISTPACK;
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there).
 * taken from t_zset.c
 */

int ZsetDel(detail::RobjWrapper* robj_wrapper, sds ele) {
  if (IsListPack(robj_wrapper)) {
    unsigned char* eptr;
    uint8_t* lp = (uint8_t*)robj_wrapper->inner_obj();
    if ((eptr = zzlFind(lp, ele, NULL)) != NULL) {
      lp = lpDeleteRangeWithEntry(lp, &eptr, 2);
      robj_wrapper->set_inner_obj(lp);
      return 1;
    }
  } else if (robj_wrapper->encoding() == OBJ_ENCODING_SKIPLIST) {
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper->inner_obj();
    if (zs->Delete(ele))
      return 1;
  }
  return 0; /* No such element found. */
}

// taken from t_zset.c
std::optional<double> GetZsetScore(const detail::RobjWrapper* robj_wrapper, sds member) {
  if (IsListPack(robj_wrapper)) {
    double score;
    if (zzlFind((uint8_t*)robj_wrapper->inner_obj(), member, &score) == NULL)
      return std::nullopt;
    return score;
  }

  if (robj_wrapper->encoding() == OBJ_ENCODING_SKIPLIST) {
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper->inner_obj();
    return zs->GetScore(member);
  }

  LOG(FATAL) << "Unknown sorted set encoding";
  return 0;
}

void OutputScoredArrayResult(const OpResult<ScoredArray>& result, SinkReplyBuilder* builder) {
  if (result.status() == OpStatus::WRONG_TYPE) {
    return builder->SendError(kWrongTypeErr);
  }

  LOG_IF(WARNING, !result && result.status() != OpStatus::KEY_NOTFOUND)
      << "Unexpected status " << result.status();
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendScoredArray(result.value(), true /* with scores */);
}

OpResult<DbSlice::ItAndUpdater> FindZEntry(const ZSetFamily::ZParams& zparams,
                                           const OpArgs& op_args, string_view key,
                                           size_t member_len) {
  auto& db_slice = op_args.GetDbSlice();
  if (zparams.flags & ZADD_IN_XX) {
    return db_slice.FindMutable(op_args.db_cntx, key, OBJ_ZSET);
  }

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  auto& it = add_res.it;
  PrimeValue& pv = it->second;
  DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);
  if (add_res.is_new || zparams.override) {
    if (member_len > server.max_map_field_len) {
      pv.InitRobj(OBJ_ZSET, OBJ_ENCODING_SKIPLIST, CompactObj::AllocateMR<detail::SortedMap>());
    } else {
      unsigned char* lp = lpNew(0);
      pv.InitRobj(OBJ_ZSET, OBJ_ENCODING_LISTPACK, lp);
      stats->listpack_blob_cnt++;
    }
  } else {
    if (it->second.ObjType() != OBJ_ZSET)
      return OpStatus::WRONG_TYPE;
  }

  auto* blocking_controller = op_args.db_cntx.ns->GetBlockingController(op_args.shard->shard_id());
  if (add_res.is_new && blocking_controller) {
    string tmp;
    string_view key = it->first.GetSlice(&tmp);
    blocking_controller->AwakeWatched(op_args.db_cntx.db_index, key);
  }

  return DbSlice::ItAndUpdater{add_res.it, add_res.exp_it, std::move(add_res.post_updater)};
}

enum class Action { RANGE = 0, REMOVE = 1, POP = 2 };

class IntervalVisitor {
 public:
  IntervalVisitor(Action action, const ZSetFamily::RangeParams& params, PrimeValue* pv)
      : action_(action), params_(params), robj_wrapper_(pv->GetRobjWrapper()) {
  }

  void operator()(const ZSetFamily::IndexInterval& ii);

  void operator()(const ZSetFamily::ScoreInterval& si);

  void operator()(const ZSetFamily::LexInterval& li);

  void operator()(ZSetFamily::TopNScored sc);

  ScoredArray PopResult() {
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

  bool IsUnder(double score, const zrangespec& spec) const {
    return params_.reverse ? zslValueGteMin(score, &spec) : zslValueLteMax(score, &spec);
  }

  void AddResult(const uint8_t* vstr, unsigned vlen, long long vlon, double score);

  Action action_;
  ZSetFamily::RangeParams params_;
  detail::RobjWrapper* robj_wrapper_;

  ScoredArray result_;
  unsigned removed_ = 0;
};

void IntervalVisitor::operator()(const ZSetFamily::IndexInterval& ii) {
  unsigned long llen = robj_wrapper_->Size();
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
      robj_wrapper_,
      [this](container_utils::ContainerEntry ce, double score) {
        result_.emplace_back(ce.ToString(), score);
        return true;
      },
      start, end, params_.reverse, params_.with_scores);
}

void IntervalVisitor::ActionRange(const zrangespec& range) {
  if (IsListPack(robj_wrapper_)) {
    ExtractListPack(range);
  } else {
    CHECK_EQ(robj_wrapper_->encoding(), OBJ_ENCODING_SKIPLIST);
    ExtractSkipList(range);
  }
}

void IntervalVisitor::ActionRange(const zlexrangespec& range) {
  if (IsListPack(robj_wrapper_)) {
    ExtractListPack(range);
  } else {
    CHECK_EQ(robj_wrapper_->encoding(), OBJ_ENCODING_SKIPLIST);
    ExtractSkipList(range);
  }
}

void IntervalVisitor::ActionRem(unsigned start, unsigned end) {
  if (IsListPack(robj_wrapper_)) {
    uint8_t* zl = (uint8_t*)robj_wrapper_->inner_obj();

    removed_ = (end - start) + 1;
    zl = lpDeleteRange(zl, 2 * start, 2 * removed_);
    robj_wrapper_->set_inner_obj(zl);
  } else {
    CHECK_EQ(OBJ_ENCODING_SKIPLIST, robj_wrapper_->encoding());
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper_->inner_obj();
    removed_ = zs->DeleteRangeByRank(start, end);
  }
}

void IntervalVisitor::ActionRem(const zrangespec& range) {
  if (IsListPack(robj_wrapper_)) {
    uint8_t* zl = (uint8_t*)robj_wrapper_->inner_obj();
    unsigned long deleted = 0;
    zl = zzlDeleteRangeByScore(zl, &range, &deleted);
    robj_wrapper_->set_inner_obj(zl);
    removed_ = deleted;
  } else {
    CHECK_EQ(OBJ_ENCODING_SKIPLIST, robj_wrapper_->encoding());
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper_->inner_obj();
    removed_ = zs->DeleteRangeByScore(range);
  }
}

void IntervalVisitor::ActionRem(const zlexrangespec& range) {
  if (IsListPack(robj_wrapper_)) {
    uint8_t* zl = (uint8_t*)robj_wrapper_->inner_obj();
    unsigned long deleted = 0;
    zl = zzlDeleteRangeByLex(zl, &range, &deleted);
    robj_wrapper_->set_inner_obj(zl);
    removed_ = deleted;
  } else {
    CHECK_EQ(OBJ_ENCODING_SKIPLIST, robj_wrapper_->encoding());
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper_->inner_obj();
    removed_ = zs->DeleteRangeByLex(range);
  }
}

void IntervalVisitor::ActionPop(ZSetFamily::TopNScored sc) {
  if (sc > 0) {
    if (IsListPack(robj_wrapper_)) {
      PopListPack(sc);
    } else {
      CHECK_EQ(robj_wrapper_->encoding(), OBJ_ENCODING_SKIPLIST);
      PopSkipList(sc);
    }
  }
}

void IntervalVisitor::ExtractListPack(const zrangespec& range) {
  uint8_t* zl = (uint8_t*)robj_wrapper_->inner_obj();
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
  detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper_->inner_obj();

  unsigned offset = params_.offset;
  unsigned limit = params_.limit;

  result_ = zs->GetRange(range, offset, limit, params_.reverse);
}

void IntervalVisitor::ExtractListPack(const zlexrangespec& range) {
  uint8_t* zl = (uint8_t*)robj_wrapper_->inner_obj();
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
  detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper_->inner_obj();
  unsigned offset = params_.offset;
  unsigned limit = params_.limit;
  result_ = zs->GetLexRange(range, offset, limit, params_.reverse);
}

void IntervalVisitor::PopListPack(ZSetFamily::TopNScored sc) {
  uint8_t* zl = (uint8_t*)robj_wrapper_->inner_obj();
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
  robj_wrapper_->set_inner_obj(lpDeleteRange(zl, start, 2 * sc));
}

void IntervalVisitor::PopSkipList(ZSetFamily::TopNScored sc) {
  detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper_->inner_obj();

  /* We start from the header, or the tail if reversed. */
  result_ = zs->PopTopScores(sc, params_.reverse);
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

void SendAtLeastOneKeyError(string_view cmd, SinkReplyBuilder* builder) {
  builder->SendError(absl::StrCat("at least 1 input key is needed for ", cmd));
}

enum class AggType : uint8_t { SUM, MIN, MAX, NOOP };
using ScoredMap = absl::flat_hash_map<std::string, double>;

ScoredMap FromObject(const CompactObj& co, double weight) {
  ZSetFamily::RangeParams params;
  params.with_scores = true;
  // RANGE is a read-only operation, but requires const_cast
  IntervalVisitor vis(Action::RANGE, params, &const_cast<CompactObj&>(co));
  vis(ZSetFamily::IndexInterval(0, -1));

  ScoredArray arr = vis.PopResult();
  ScoredMap res;
  res.reserve(arr.size());

  for (auto& elem : arr) {
    elem.second *= weight;
    if (isnan(elem.second))
      elem.second = 0;
    res.emplace(std::move(elem));
  }

  return res;
}

ScoredMap ScoreMapFromSet(const PrimeValue& pv, double weight) {
  ScoredMap result;
  container_utils::IterateSet(pv, [&result, weight](container_utils::ContainerEntry ce) {
    result.emplace(ce.ToString(), weight);
    return true;
  });
  return result;
}

double Aggregate(double v1, double v2, AggType atype) {
  switch (atype) {
    case AggType::SUM:
      v1 += v2;
      return isnan(v1) ? 0 : v1;
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

using KeyIterWeightVec = vector<pair<DbSlice::ConstIterator, double>>;

ScoredMap UnionShardKeysWithScore(const KeyIterWeightVec& key_iter_weight_vec, AggType agg_type) {
  ScoredMap result;
  for (const auto& [it, weight] : key_iter_weight_vec) {
    if (it.is_done()) {
      continue;
    }

    ScoredMap sm;
    if (it->second.ObjType() == OBJ_ZSET)
      sm = FromObject(it->second, weight);
    else {
      DCHECK_EQ(it->second.ObjType(), OBJ_SET);
      sm = ScoreMapFromSet(it->second, weight);
    }
    if (result.empty()) {
      result.swap(sm);
    } else {
      UnionScoredMap(&result, &sm, agg_type);
    }
  }
  return result;
}

double GetKeyWeight(const vector<double>& weights, unsigned windex) {
  if (weights.empty()) {
    return 1;
  }

  DCHECK_LT(windex, weights.size());
  return weights[windex];
}

OpResult<KeyIterWeightVec> PrepareWeightedSets(const Transaction& trans, bool store,
                                               string_view dest, const vector<double>& weights,
                                               EngineShard* shard) {
  ShardArgs keys = trans.GetShardArgs(shard->shard_id());
  DCHECK(!keys.Empty());

  unsigned cmdargs_keys_offset = 1;  // after {numkeys} for ZUNION/ZINTER
  unsigned removed_keys = 0;

  ShardArgs::Iterator start = keys.begin(), end = keys.end();

  if (store) {
    // first global index is 2 after {destkey, numkeys}.
    ++cmdargs_keys_offset;
    if (*start == dest) {
      ++start;
      ++removed_keys;
    }

    // In case ONLY the destination key is hosted in this shard no work on this shard should be
    // done in this step
    if (start == end) {
      return OpStatus::OK;
    }
  }

  auto& db_slice = trans.GetDbSlice(shard->shard_id());
  KeyIterWeightVec key_weight_vec(keys.Size() - removed_keys);
  unsigned index = 0;
  DCHECK_GE(start.index(), cmdargs_keys_offset);

  for (; start != end; ++start) {
    auto it_res = db_slice.FindReadOnly(trans.GetDbContext(), *start);

    if (!IsValid(it_res.it)) {
      ++index;
      continue;
    }

    auto obj_type = it_res.it->second.ObjType();
    if (obj_type != OBJ_ZSET && obj_type != OBJ_SET)
      return OpStatus::WRONG_TYPE;

    key_weight_vec[index] = {it_res.it, GetKeyWeight(weights, start.index() - cmdargs_keys_offset)};
    ++index;
  }

  return key_weight_vec;
}

OpResult<ScoredMap> OpUnion(EngineShard* shard, Transaction* t, string_view dest, AggType agg_type,
                            const vector<double>& weights, bool store) {
  OpResult<KeyIterWeightVec> key_vec_res = PrepareWeightedSets(*t, store, dest, weights, shard);
  if (!key_vec_res)
    return key_vec_res.status();

  // Only dest is hosted on this shard.
  if (key_vec_res->empty())
    return OpStatus::OK;

  return UnionShardKeysWithScore(*key_vec_res, agg_type);
}

OpResult<ScoredMap> OpInter(EngineShard* shard, Transaction* t, string_view dest, AggType agg_type,
                            const vector<double>& weights, bool store) {
  OpResult<KeyIterWeightVec> key_vec_res = PrepareWeightedSets(*t, store, dest, weights, shard);
  if (!key_vec_res)
    return key_vec_res.status();

  // Only dest is hosted on this shard.
  if (key_vec_res->empty())
    return OpStatus::SKIPPED;

  ScoredMap result;
  for (const auto& [it, weight] : *key_vec_res) {
    if (it.is_done()) {
      return ScoredMap{};
    }

    ScoredMap sm;
    if (it->second.ObjType() == OBJ_ZSET)
      sm = FromObject(it->second, weight);
    else {
      DCHECK_EQ(it->second.ObjType(), OBJ_SET);
      sm = ScoreMapFromSet(it->second, weight);
    }
    if (result.empty())
      result.swap(sm);
    else
      InterScoredMap(&result, &sm, agg_type);

    if (result.empty())
      return result;
  }

  return result;
}

size_t EstimateListpackMinBytes(ScoredMemberSpan members) {
  size_t bytes = members.size() * 2;  // at least 2 bytes per score;
  for (const auto& member : members) {
    bytes += (member.second.size() + 1);  // string + at least 1 byte for string header.
  }
  return bytes;
}

struct SetOpArgs {
  AggType agg_type = AggType::SUM;
  unsigned num_keys;
  vector<double> weights;
  bool with_scores = false;
};

void HandleOpStatus(OpStatus op_status, SinkReplyBuilder* builder) {
  switch (op_status) {
    case OpStatus::INVALID_FLOAT:
      return builder->SendError("weight value is not a float", kSyntaxErrType);
    default:
      return builder->SendError(op_status);
  }
}

OpResult<ScoredMap> IntersectResults(vector<OpResult<ScoredMap>>& results, AggType agg_type) {
  ScoredMap result;
  for (auto& op_res : results) {
    if (op_res.status() == OpStatus::SKIPPED)
      continue;

    if (!op_res) {
      return op_res.status();
    }

    if (op_res->empty()) {
      return ScoredMap{};
    }

    if (result.empty()) {
      result.swap(op_res.value());
    } else {
      InterScoredMap(&result, &op_res.value(), agg_type);
    }

    if (result.empty())
      break;
  }
  return result;
}

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
  if (args.size() <= 1) {
    return OpStatus::SYNTAX_ERR;
  }

  string agg_type = absl::AsciiStrToUpper(ArgS(args, 1));
  auto filled = FillAggType(agg_type, op_args);
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
    string arg = absl::AsciiStrToUpper(ArgS(args, i));
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

ScoredArray OpBZPop(Transaction* t, EngineShard* shard, std::string_view key, bool is_max) {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto it_res = db_slice.FindMutable(t->GetDbContext(), key, OBJ_ZSET);
  CHECK(it_res) << t->DebugId() << " " << key;  // must exist and must be ok.
  auto it = it_res->it;

  ZSetFamily::RangeParams range_params;
  range_params.reverse = is_max;
  range_params.with_scores = true;
  ZSetFamily::ZRangeSpec range_spec;
  range_spec.params = range_params;
  range_spec.interval = ZSetFamily::TopNScored(1);

  DVLOG(2) << "popping from " << key << " " << t->DebugId();

  PrimeValue& pv = it->second;
  CHECK_GT(pv.Size(), 0u) << key << " " << pv.GetRobjWrapper()->encoding();

  IntervalVisitor iv{Action::POP, range_spec.params, &pv};
  std::visit(iv, range_spec.interval);

  it_res->post_updater.Run();

  auto res = iv.PopResult();

  // We don't store empty keys
  CHECK(!res.empty()) << key << " failed to pop from type " << pv.GetRobjWrapper()->encoding()
                      << " now size is " << pv.Size();

  auto zlen = pv.Size();
  if (zlen == 0) {
    DVLOG(1) << "deleting key " << key << " " << t->DebugId();
    db_slice.Del(t->GetDbContext(), it_res->it);
  }

  OpArgs op_args = t->GetOpArgs(shard);
  if (op_args.shard->journal()) {
    string command = is_max ? "ZPOPMAX" : "ZPOPMIN";
    RecordJournal(op_args, command, ArgSlice{key}, 1);
  }

  return res;
}

void BZPopMinMax(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                 ConnectionContext* cntx, bool is_max) {
  DCHECK_GE(args.size(), 2u);

  float timeout;
  auto timeout_str = ArgS(args, args.size() - 1);
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return builder->SendError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return builder->SendError("timeout is negative");
  }
  VLOG(1) << "BZPop timeout(" << timeout << ")";

  std::string dinfo;
  optional<std::string> callback_ran_key;
  OpResult<ScoredArray> popped_array;
  auto cb = [is_max, &popped_array, &callback_ran_key](Transaction* t, EngineShard* shard,
                                                       std::string_view key) {
    callback_ran_key = key;
    popped_array = OpBZPop(t, shard, key, is_max);
  };

  OpResult<string> popped_key = container_utils::RunCbOnFirstNonEmptyBlocking(
      tx, OBJ_ZSET, std::move(cb), unsigned(timeout * 1000), &cntx->blocked, &cntx->paused, &dinfo);

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (popped_key) {
    if (!callback_ran_key) {
      LOG(ERROR) << "BUG: Callback didn't run! " << popped_key.value() << " " << dinfo;
      return rb->SendNullArray();
    }

    DVLOG(1) << "BZPop " << tx->DebugId() << " popped from key " << popped_key;  // key.
    CHECK(popped_array.ok()) << dinfo;
    CHECK_EQ(popped_array->size(), 1u)
        << popped_key << " ran " << *callback_ran_key << " info " << dinfo;
    rb->StartArray(3);
    rb->SendBulkString(*popped_key);
    rb->SendBulkString(popped_array->front().first);
    return rb->SendDouble(popped_array->front().second);
  }

  DVLOG(1) << "result for " << tx->DebugId() << " is " << popped_key.status();
  switch (popped_key.status()) {
    case OpStatus::WRONG_TYPE:
      return builder->SendError(kWrongTypeErr);
    case OpStatus::CANCELLED:
    case OpStatus::TIMED_OUT:
      return rb->SendNullArray();
    case OpStatus::KEY_MOVED: {
      auto error = cluster::SlotOwnershipError(*tx->GetUniqueSlotId());
      CHECK(!error.status.has_value() || error.status.value() != facade::OpStatus::OK);
      return builder->SendError(std::move(error));
    }
    default:
      LOG(ERROR) << "Unexpected error " << popped_key.status();
  }
  return rb->SendNullArray();
}

vector<ScoredMap> OpFetch(EngineShard* shard, Transaction* t) {
  ShardArgs keys = t->GetShardArgs(shard->shard_id());
  DCHECK(!keys.Empty());

  vector<ScoredMap> results;
  results.reserve(keys.Size());

  auto& db_slice = t->GetDbSlice(shard->shard_id());
  for (string_view key : keys) {
    auto it = db_slice.FindReadOnly(t->GetDbContext(), key, OBJ_ZSET);
    if (!it) {
      results.push_back({});
      continue;
    }

    ScoredMap sm = FromObject((*it)->second, 1);
    results.push_back(std::move(sm));
  }

  return results;
}

auto OpPopCount(const ZSetFamily::ZRangeSpec& range_spec, const OpArgs& op_args, string_view key)
    -> OpResult<ScoredArray> {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  PrimeValue& pv = res_it->it->second;

  IntervalVisitor iv{Action::POP, range_spec.params, &pv};
  std::visit(iv, range_spec.interval);

  res_it->post_updater.Run();

  auto zlen = pv.Size();
  if (zlen == 0) {
    op_args.GetDbSlice().Del(op_args.db_cntx, res_it->it);
  }

  return iv.PopResult();
}

auto OpRange(const ZSetFamily::ZRangeSpec& range_spec, const OpArgs& op_args, string_view key)
    -> OpResult<ScoredArray> {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  // Action::RANGE is read-only, but requires mutable pointer, thus const_cast
  PrimeValue& pv = const_cast<PrimeValue&>(res_it.value()->second);
  IntervalVisitor iv{Action::RANGE, range_spec.params, &pv};

  std::visit(iv, range_spec.interval);

  return iv.PopResult();
}

OpResult<unsigned> OpRemRange(const OpArgs& op_args, string_view key,
                              const ZSetFamily::ZRangeSpec& range_spec) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  PrimeValue& pv = res_it->it->second;
  IntervalVisitor iv{Action::REMOVE, range_spec.params, &pv};
  std::visit(iv, range_spec.interval);

  res_it->post_updater.Run();

  auto zlen = pv.Size();
  if (zlen == 0) {
    op_args.GetDbSlice().Del(op_args.db_cntx, res_it->it);
  }

  return iv.removed();
}

struct RankResult {
  unsigned rank;
  double score = 0;
};

OpResult<RankResult> OpRank(const OpArgs& op_args, string_view key, string_view member,
                            bool reverse, bool with_score) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  const detail::RobjWrapper* robj_wrapper = res_it.value()->second.GetRobjWrapper();
  if (IsListPack(robj_wrapper)) {
    unsigned char* zl = (uint8_t*)robj_wrapper->inner_obj();
    unsigned char *eptr, *sptr;

    eptr = lpSeek(zl, 0);
    DCHECK(eptr != NULL);
    sptr = lpNext(zl, eptr);
    DCHECK(sptr != NULL);

    unsigned rank = 1;
    if (member.empty())
      member = ""sv;

    while (eptr != NULL) {
      if (lpCompare(eptr, (const uint8_t*)member.data(), member.size()))
        break;
      rank++;
      zzlNext(zl, &eptr, &sptr);
    }

    if (eptr == NULL)
      return OpStatus::KEY_NOTFOUND;

    RankResult res{};
    res.rank = reverse ? lpLength(zl) / 2 - rank : rank - 1;
    if (with_score) {
      res.score = zzlGetScore(sptr);
    }
    return res;
  }
  DCHECK_EQ(robj_wrapper->encoding(), OBJ_ENCODING_SKIPLIST);
  detail::SortedMap* ss = (detail::SortedMap*)robj_wrapper->inner_obj();

  RankResult res{};

  if (with_score) {
    auto rankAndScore = ss->GetRankAndScore(WrapSds(member), reverse);
    if (!rankAndScore) {
      return OpStatus::KEY_NOTFOUND;
    }
    res.rank = rankAndScore->first;
    res.score = rankAndScore->second;
  } else {
    std::optional<unsigned> rank = ss->GetRank(WrapSds(member), reverse);
    if (!rank) {
      return OpStatus::KEY_NOTFOUND;
    }
    res.rank = *rank;
  }

  return res;
}

OpResult<unsigned> OpCount(const OpArgs& op_args, std::string_view key,
                           const ZSetFamily::ScoreInterval& interval) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  const detail::RobjWrapper* robj_wrapper = res_it.value()->second.GetRobjWrapper();
  zrangespec range = GetZrangeSpec(false, interval);
  unsigned count = 0;

  if (IsListPack(robj_wrapper)) {
    uint8_t* zl = (uint8_t*)robj_wrapper->inner_obj();
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
    CHECK_EQ(unsigned(OBJ_ENCODING_SKIPLIST), robj_wrapper->encoding());
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper->inner_obj();
    count = zs->Count(range);
  }

  return count;
}

OpResult<unsigned> OpLexCount(const OpArgs& op_args, string_view key,
                              const ZSetFamily::LexInterval& interval) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  zlexrangespec range = GetLexRange(false, interval);
  unsigned count = 0;
  const detail::RobjWrapper* robj_wrapper = res_it.value()->second.GetRobjWrapper();

  if (IsListPack(robj_wrapper)) {
    uint8_t* zl = (uint8_t*)robj_wrapper->inner_obj();
    uint8_t *eptr, *sptr;

    /* Use the first element in range as the starting point */
    eptr = zzlFirstInLexRange(zl, &range);

    if (eptr) {
      /* First element is in range */
      sptr = lpNext(zl, eptr);
      DCHECK(zzlLexValueLteMax(eptr, &range));

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
    DCHECK_EQ(OBJ_ENCODING_SKIPLIST, robj_wrapper->encoding());
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper->inner_obj();
    count = zs->LexCount(range);
  }

  zslFreeLexRange(&range);
  return count;
}

OpResult<unsigned> OpRem(const OpArgs& op_args, string_view key, facade::ArgRange members) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  detail::RobjWrapper* robj_wrapper = res_it->it->second.GetRobjWrapper();
  unsigned deleted = 0;
  for (string_view member : members)
    deleted += ZsetDel(robj_wrapper, WrapSds(member));

  auto zlen = robj_wrapper->Size();
  res_it->post_updater.Run();

  if (zlen == 0) {
    op_args.GetDbSlice().Del(op_args.db_cntx, res_it->it);
  }

  return deleted;
}

OpResult<MScoreResponse> OpMScore(const OpArgs& op_args, string_view key,
                                  facade::ArgRange members) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  MScoreResponse scores(members.Size());

  const detail::RobjWrapper* robj_wrapper = res_it.value()->second.GetRobjWrapper();

  size_t i = 0;
  for (string_view member : members.Range())
    scores[i++] = GetZsetScore(robj_wrapper, WrapSds(member));

  return scores;
}

OpResult<StringVec> OpScan(const OpArgs& op_args, std::string_view key, uint64_t* cursor,
                           const ScanOpts& scan_op) {
  auto find_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);

  if (!find_res)
    return find_res.status();

  auto it = find_res.value();
  const PrimeValue& pv = it->second;
  StringVec res;
  char buf[128];

  if (IsListPack(pv.GetRobjWrapper())) {
    ZSetFamily::RangeParams params;
    params.with_scores = true;
    IntervalVisitor iv{Action::RANGE, params, const_cast<PrimeValue*>(&pv)};

    iv(ZSetFamily::IndexInterval{0, kuint32max});
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
    CHECK_EQ(unsigned(OBJ_ENCODING_SKIPLIST), pv.Encoding());
    uint32_t count = scan_op.limit;
    detail::SortedMap* sm = (detail::SortedMap*)pv.RObjPtr();
    long maxiterations = count * 10;
    uint64_t cur = *cursor;

    auto cb = [&](string_view str, double score) {
      if (scan_op.Matches(str)) {
        res.emplace_back(str);
        char* str = RedisReplyBuilder::FormatDouble(score, buf, sizeof(buf));
        res.emplace_back(str);
      }
    };
    do {
      cur = sm->Scan(cur, cb);
    } while (cur && maxiterations-- && res.size() < count);
    *cursor = cur;
  }

  return res;
}

OpResult<ScoredArray> OpRandMember(int count, const ZSetFamily::RangeParams& params,
                                   const OpArgs& op_args, string_view key) {
  auto it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!it)
    return it.status();

  // Action::RANGE is a read-only operation, but requires const_cast
  PrimeValue& pv = const_cast<PrimeValue&>(it.value()->second);

  const std::size_t size = pv.Size();
  const std::size_t picks_count =
      count >= 0 ? std::min(static_cast<std::size_t>(count), size) : std::abs(count);

  ScoredArray result{picks_count};
  std::unique_ptr<PicksGenerator> generator =
      count >= 0 ? static_cast<std::unique_ptr<PicksGenerator>>(
                       std::make_unique<UniquePicksGenerator>(picks_count, size))
                 : std::make_unique<NonUniquePicksGenerator>(size);

  if (picks_count * static_cast<std::uint64_t>(std::log2(size)) < size) {
    for (std::size_t i = 0; i < picks_count; i++) {
      const std::size_t picked_index = generator->Generate();

      IntervalVisitor iv{Action::RANGE, params, &pv};
      iv(ZSetFamily::IndexInterval{picked_index, picked_index});

      result[i] = iv.PopResult().front();
    }
  } else {
    IntervalVisitor iv{Action::RANGE, params, &pv};
    iv(ZSetFamily::IndexInterval{0, -1});

    ScoredArray all_elements = iv.PopResult();

    for (std::size_t i = 0; i < picks_count; i++) {
      result[i] = all_elements[generator->Generate()];
    }
  }

  return result;
}

// Boolean operation: union or intersection, optionally storing output to destination key
void ZBooleanOperation(CmdArgList args, string_view cmd, bool is_union, bool store, Transaction* tx,
                       SinkReplyBuilder* builder) {
  auto shard_func = is_union ? OpUnion : OpInter;
  auto merge_func = is_union ? UnionScoredMap : InterScoredMap;

  string_view dest_key = ArgS(args, 0);
  OpResult<SetOpArgs> op_args = ParseSetOpArgs(args, store);
  if (!op_args)
    return HandleOpStatus(op_args.status(), builder);

  if (op_args->num_keys == 0)
    return SendAtLeastOneKeyError(cmd, builder);

  vector<OpResult<ScoredMap>> maps(shard_set->size(), OpStatus::SKIPPED);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] =
        shard_func(shard, t, dest_key, op_args->agg_type, op_args->weights, store);
    return OpStatus::OK;
  };
  tx->Execute(cb, !store /* if we don't store, conclude */);

  // Merge results from all shards
  ScoredMap result;
  for (auto& op_res : maps) {
    if (op_res.status() == OpStatus::SKIPPED)
      continue;
    if (!op_res) {
      if (store) {
        tx->Conclude();
      }
      return builder->SendError(op_res.status());
    }

    if (result.empty())
      result = std::move(op_res.value());
    else
      merge_func(&result, &op_res.value(), op_args->agg_type);

    if (result.empty() && !is_union)  // intersection only shrinks
      break;
  }

  // Copy to vector for sorting
  vector<ScoredMemberView> smvec(result.size());
  size_t i = 0;
  for (const auto& [str, score] : result)
    smvec[i++] = {score, str};

  if (store) {
    // TODO: Use variant collection to avoid smvec copy for store operation
    auto store_cb = [&, dest_shard = Shard(dest_key, maps.size())](Transaction* t,
                                                                   EngineShard* shard) {
      if (shard->shard_id() == dest_shard)
        ZSetFamily::OpAdd(t->GetOpArgs(shard), ZSetFamily::ZParams{.override = true}, dest_key,
                          smvec);
      return OpStatus::OK;
    };
    tx->Execute(store_cb, true);
    builder->SendLong(smvec.size());
  } else {
    std::sort(std::begin(smvec), std::end(smvec));

    // We can't use SendScoredArray because it expects strings, not string_views
    // TOOD: Not longer relevant with new io, use scoping
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    rb->StartArray(smvec.size() * (op_args->with_scores ? 2 : 1));
    for (const auto& elem : smvec) {
      rb->SendBulkString(elem.second);
      if (op_args->with_scores) {
        rb->SendDouble(elem.first);
      }
    }
  }
}

enum class FilterShards { NO = 0, YES = 1 };

OpResult<ScoredArray> ZPopMinMaxInternal(std::string_view key, FilterShards should_filter_shards,
                                         uint32 count, bool reverse, Transaction* tx) {
  ZSetFamily::RangeParams range_params;
  range_params.reverse = reverse;
  range_params.with_scores = true;
  ZSetFamily::ZRangeSpec range_spec;
  range_spec.params = range_params;

  range_spec.interval = count;

  OpResult<ScoredArray> result;

  std::optional<ShardId> key_shard;
  if (should_filter_shards == FilterShards::YES) {
    key_shard = Shard(key, shard_set->size());
  }
  auto cb = [&](Transaction* t, EngineShard* shard) {
    if (!key_shard.has_value() || *key_shard == shard->shard_id()) {
      result = OpPopCount(range_spec, t->GetOpArgs(shard), key);
    }
    return OpStatus::OK;
  };

  tx->Execute(std::move(cb), true);

  return result;
}

void ZPopMinMaxFromArgs(CmdArgList args, bool reverse, Transaction* tx, SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);
  uint32 count = 1;
  if (args.size() > 1) {
    string_view count_str = ArgS(args, 1);
    if (!SimpleAtoi(count_str, &count)) {
      return builder->SendError(kUintErr);
    }
  }

  OutputScoredArrayResult(ZPopMinMaxInternal(key, FilterShards::NO, count, reverse, tx), builder);
}

void ZRangeInternal(CmdArgList args, ZSetFamily::RangeParams range_params, Transaction* tx,
                    SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  ZSetFamily::ZRangeSpec range_spec;
  range_spec.params = range_params;
  using RP = ZSetFamily::RangeParams;

  switch (range_params.interval_type) {
    case RP::IntervalType::SCORE: {
      ZSetFamily::ScoreInterval si;
      if (!ParseBound(min_s, &si.first) || !ParseBound(max_s, &si.second)) {
        return builder->SendError(kFloatRangeErr);
      }
      range_spec.interval = si;
      break;
    }
    case RP::IntervalType::LEX: {
      ZSetFamily::LexInterval li;
      if (!ParseLexBound(min_s, &li.first) || !ParseLexBound(max_s, &li.second)) {
        return builder->SendError(kLexRangeErr);
      }
      range_spec.interval = li;
      break;
    }
    case RP::IntervalType::RANK: {
      ZSetFamily::IndexInterval ii;
      if (!SimpleAtoi(min_s, &ii.first) || !SimpleAtoi(max_s, &ii.second)) {
        builder->SendError(kInvalidIntErr);
        return;
      }
      range_spec.interval = ii;
      break;
    }
  }

  OpResult<ScoredArray> range_result;
  ShardId src_shard = Shard(key, shard_set->size());
  auto range_cb = [&](Transaction* t, EngineShard* shard) {
    if (shard->shard_id() != src_shard) {
      // Only run ZRANGE on the source shard.
      return OpStatus::OK;
    }
    range_result = OpRange(range_spec, t->GetOpArgs(shard), key);
    return OpStatus::OK;
  };
  // Don't conclude the transaction if we're storing the result.
  tx->Execute(std::move(range_cb), !range_params.store_key);

  if (range_result.status() == OpStatus::WRONG_TYPE) {
    if (range_params.store_key) {
      tx->Conclude();
    }
    return builder->SendError(kWrongTypeErr);
  }
  LOG_IF(WARNING, !range_result && range_result.status() != OpStatus::KEY_NOTFOUND)
      << "Unexpected status " << range_result.status();

  if (!range_params.store_key) {
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    rb->SendScoredArray(range_result.value(), range_params.with_scores);
    return;
  }

  OpResult<ZSetFamily::AddResult> add_result;
  ShardId dest_shard = Shard(*range_params.store_key, shard_set->size());
  auto add_cb = [&](Transaction* t, EngineShard* shard) {
    if (shard->shard_id() != dest_shard) {
      // Only write the result on the target shard.
      return OpStatus::OK;
    }

    std::vector<ScoredMemberView> mvec(range_result->size());
    size_t i = 0;
    for (const auto& [str, score] : *range_result) {
      mvec[i++] = {score, str};
    }

    add_result = ZSetFamily::OpAdd(t->GetOpArgs(shard), ZSetFamily::ZParams{.override = true},
                                   *range_params.store_key, mvec);

    return OpStatus::OK;
  };
  tx->Execute(std::move(add_cb), true);

  if (add_result.status() == OpStatus::OUT_OF_MEMORY) {
    return builder->SendError(add_result.status());
  }
  LOG_IF(WARNING, !add_result) << "Unexpected status " << add_result.status();

  return builder->SendLong(range_result->size());
}

void ZRangeGeneric(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                   ZSetFamily::RangeParams range_params) {
  facade::CmdArgParser parser{args.subspan(3)};
  using RP = ZSetFamily::RangeParams;

  while (true) {
    if (auto err = parser.Error(); err)
      return builder->SendError(err->MakeReply());

    if (!parser.HasNext())
      break;

    if (parser.Check("BYSCORE")) {
      if (exchange(range_params.interval_type, RP::SCORE) == RP::LEX)
        return builder->SendError("BYSCORE and BYLEX options are not compatible");
      continue;
    }

    if (parser.Check("BYLEX")) {
      if (exchange(range_params.interval_type, RP::LEX) == RP::SCORE)
        return builder->SendError("BYSCORE and BYLEX options are not compatible");
      continue;
    }
    if (parser.Check("REV")) {
      range_params.reverse = true;
      continue;
    }
    if (parser.Check("WITHSCORES")) {
      range_params.with_scores = true;
      continue;
    }

    if (parser.Check("LIMIT")) {
      auto [offset, limit] = parser.Next<int32_t, int32_t>();

      range_params.limit = limit < 0 ? UINT32_MAX : static_cast<uint32_t>(limit);
      range_params.offset = offset < 0 ? UINT32_MAX : static_cast<uint32_t>(offset);
      continue;
    }

    return builder->SendError(absl::StrCat("unsupported option ", parser.Peek()));
  }

  if (range_params.offset == UINT32_MAX) {
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    return rb->SendEmptyArray();
  }

  ZRangeInternal(args.subspan(0, 3), range_params, tx, builder);
}

void ZRankGeneric(CmdArgList args, bool reverse, Transaction* tx, SinkReplyBuilder* builder) {
  // send this error exact as redis does, it checks number of arguments first
  if (args.size() > 3) {
    return builder->SendError(WrongNumArgsError(reverse ? "ZREVRANK" : "ZRANK"));
  }

  facade::CmdArgParser parser(args);

  string_view key = parser.Next();
  string_view member = parser.Next();
  bool with_score = false;

  if (parser.HasNext()) {
    parser.ExpectTag("WITHSCORE");
    with_score = true;
  }

  if (!parser.Finalize()) {
    return builder->SendError(parser.Error()->MakeReply());
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRank(t->GetOpArgs(shard), key, member, reverse, with_score);
  };

  OpResult<RankResult> result = tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (result) {
    if (with_score) {
      rb->StartArray(2);
      rb->SendLong(result->rank);
      rb->SendDouble(result->score);
    } else {
      rb->SendLong(result->rank);
    }
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->SendNull();
  } else {
    builder->SendError(result.status());
  }
}

void ZRemRangeGeneric(string_view key, const ZSetFamily::ZRangeSpec& range_spec, Transaction* tx,
                      SinkReplyBuilder* builder) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRemRange(t->GetOpArgs(shard), key, range_spec);
  };

  OpResult<unsigned> result = tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    builder->SendError(kWrongTypeErr);
  } else {
    builder->SendLong(*result);
  }
}

// Returns the key of the first non empty set found in the list of shard arguments.
// Returns nullopt if none.
std::optional<std::string_view> GetFirstNonEmptyKeyFound(EngineShard* shard, Transaction* t) {
  ShardArgs keys = t->GetShardArgs(shard->shard_id());
  DCHECK(!keys.Empty());

  auto& db_slice = t->GetDbSlice(shard->shard_id());

  for (string_view key : keys) {
    auto it = db_slice.FindReadOnly(t->GetDbContext(), key, OBJ_ZSET);
    if (!it) {
      continue;
    }
    return std::optional<std::string_view>(key);
  }

  return std::nullopt;
}

// Validates the ZMPop command arguments and extracts the values to the output params.
// If the arguments are invalid sends the appropiate error to builder and returns false.
bool ValidateZMPopCommand(CmdArgList args, uint32* num_keys, bool* is_max, int* pop_count,
                          SinkReplyBuilder* builder) {
  CmdArgParser parser{args};

  if (!SimpleAtoi(parser.Next(), num_keys)) {
    builder->SendError(kUintErr);
    return false;
  }

  if (*num_keys <= 0 || !parser.HasAtLeast(*num_keys + 1)) {
    // We should have at least num_keys keys + a MIN/MAX arg.
    builder->SendError(kSyntaxErr);
    return false;
  }
  // Skip over the keys themselves.
  parser.Skip(*num_keys);

  // We know we have at least one more arg (we checked above).
  if (parser.Check("MAX")) {
    *is_max = true;
  } else if (parser.Check("MIN")) {
    *is_max = false;
  } else {
    builder->SendError(kSyntaxErr);
    return false;
  }

  *pop_count = 1;
  // Check if we have additional COUNT argument.
  if (parser.HasNext()) {
    if (!parser.Check("COUNT", pop_count)) {
      builder->SendError(kSyntaxErr);
      return false;
    }
  }

  if (!parser.Finalize()) {
    builder->SendError(parser.Error()->MakeReply());
    return false;
  }

  return true;
}

}  // namespace

void ZSetFamily::ZAddGeneric(string_view key, const ZParams& zparams, ScoredMemberSpan memb_sp,
                             Transaction* tx, SinkReplyBuilder* builder) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return ZSetFamily::OpAdd(t->GetOpArgs(shard), zparams, key, memb_sp);
  };

  OpResult<AddResult> add_result = tx->ScheduleSingleHopT(std::move(cb));
  if (base::_in(add_result.status(), {OpStatus::WRONG_TYPE, OpStatus::OUT_OF_MEMORY})) {
    return builder->SendError(add_result.status());
  }

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  // KEY_NOTFOUND may happen in case of XX flag.
  if (add_result.status() == OpStatus::KEY_NOTFOUND) {
    if (zparams.flags & ZADD_IN_INCR)
      rb->SendNull();
    else
      rb->SendLong(0);
  } else if (add_result.status() == OpStatus::SKIPPED) {
    rb->SendNull();
  } else if (add_result->is_nan) {
    builder->SendError(kScoreNaN);
  } else {
    if (zparams.flags & ZADD_IN_INCR) {
      rb->SendDouble(add_result->new_score);
    } else {
      rb->SendLong(add_result->num_updated);
    }
  }
}

OpResult<MScoreResponse> ZSetFamily::ZGetMembers(CmdArgList args, Transaction* tx,
                                                 SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);
  auto members = args.subspan(1);
  auto cb = [key, members](Transaction* t, EngineShard* shard) {
    return OpMScore(t->GetOpArgs(shard), key, members);
  };

  return tx->ScheduleSingleHopT(std::move(cb));
}

auto ZSetFamily::OpRanges(const std::vector<ZSetFamily::ZRangeSpec>& range_specs,
                          const OpArgs& op_args, string_view key) -> OpResult<vector<ScoredArray>> {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  // Action::RANGE is read-only, but requires mutable pointer, thus const_cast
  PrimeValue& pv = const_cast<PrimeValue&>(res_it.value()->second);
  vector<ScoredArray> result_arrays;
  for (auto& range_spec : range_specs) {
    IntervalVisitor iv{Action::RANGE, range_spec.params, &pv};
    std::visit(iv, range_spec.interval);
    result_arrays.push_back(iv.PopResult());
  }

  return result_arrays;
}

OpResult<ZSetFamily::AddResult> ZSetFamily::OpAdd(const OpArgs& op_args,
                                                  const ZSetFamily::ZParams& zparams,
                                                  string_view key, ScoredMemberSpan members) {
  DCHECK(!members.empty() || zparams.override);
  auto& db_slice = op_args.GetDbSlice();

  if (zparams.override && members.empty()) {
    auto it = db_slice.FindMutable(op_args.db_cntx, key).it;  // post_updater will run immediately
    if (IsValid(it)) {
      db_slice.Del(op_args.db_cntx, it);
    }
    return OpStatus::OK;
  }

  // When we have too many members to add, make sure field_len is large enough to use
  // skiplist encoding.
  size_t field_len = members.size() > server.zset_max_listpack_entries
                         ? UINT32_MAX
                         : members.front().second.size();
  auto res_it = FindZEntry(zparams, op_args, key, field_len);

  if (!res_it)
    return res_it.status();

  unsigned added = 0;
  unsigned updated = 0;

  double new_score = 0;
  int retflags = 0;

  OpStatus op_status = OpStatus::OK;
  AddResult aresult;
  detail::RobjWrapper* robj_wrapper = res_it->it->second.GetRobjWrapper();
  bool is_list_pack = IsListPack(robj_wrapper);

  // opportunistically reserve space if multiple entries are about to be added.
  if ((zparams.flags & ZADD_IN_XX) == 0 && members.size() > 2) {
    if (is_list_pack) {
      uint8_t* zl = (uint8_t*)robj_wrapper->inner_obj();
      size_t malloc_reserved = zmalloc_size(zl);
      size_t min_sz = EstimateListpackMinBytes(members);
      if (min_sz > malloc_reserved) {
        zl = (uint8_t*)zrealloc(zl, min_sz);
        robj_wrapper->set_inner_obj(zl);
      }
    } else {
      detail::SortedMap* sm = (detail::SortedMap*)robj_wrapper->inner_obj();
      sm->Reserve(members.size());
    }
  }

  for (size_t j = 0; j < members.size(); j++) {
    const auto& m = members[j];
    int retval =
        robj_wrapper->ZsetAdd(m.first, WrapSds(m.second), zparams.flags, &retflags, &new_score);

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
  }

  // if we migrated to skip_list - update listpack stats.
  if (is_list_pack && !IsListPack(robj_wrapper)) {
    DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);
    --stats->listpack_blob_cnt;
  }

  if (zparams.flags & ZADD_IN_INCR) {
    aresult.new_score = new_score;
  } else {
    aresult.num_updated = zparams.ch ? added + updated : added;
  }

  if (op_status != OpStatus::OK)
    return op_status;
  return aresult;
}

OpResult<void> ZSetFamily::OpKeyExisted(const OpArgs& op_args, string_view key) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  return res_it.status();
}

OpResult<double> ZSetFamily::OpScore(const OpArgs& op_args, string_view key, string_view member) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  const PrimeValue& pv = res_it.value()->second;
  const detail::RobjWrapper* robj_wrapper = pv.GetRobjWrapper();
  auto res = GetZsetScore(robj_wrapper, WrapSds(member));
  if (!res) {
    return OpStatus::MEMBER_NOTFOUND;
  }
  return *res;
}

void ZSetFamily::BZPopMin(CmdArgList args, const CommandContext& cmd_cntx) {
  BZPopMinMax(args, cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx, false);
}

void ZSetFamily::BZPopMax(CmdArgList args, const CommandContext& cmd_cntx) {
  BZPopMinMax(args, cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx, true);
}

void ZSetFamily::ZAdd(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  ZSetFamily::ZParams zparams;
  size_t i = 1;
  for (; i < args.size() - 1; ++i) {
    string cur_arg = absl::AsciiStrToUpper(ArgS(args, i));

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

  auto* builder = cmd_cntx.rb;
  if ((args.size() - i) % 2 != 0) {
    builder->SendError(kSyntaxErr);
    return;
  }

  if ((zparams.flags & ZADD_IN_INCR) && (i + 2 < args.size())) {
    builder->SendError("INCR option supports a single increment-element pair");
    return;
  }

  unsigned insert_mask = zparams.flags & (ZADD_IN_NX | ZADD_IN_XX);
  if (insert_mask == (ZADD_IN_NX | ZADD_IN_XX)) {
    builder->SendError(kNxXxErr);
    return;
  }

  constexpr auto kRangeOpt = ZADD_IN_GT | ZADD_IN_LT;
  if (((zparams.flags & ZADD_IN_NX) && (zparams.flags & kRangeOpt)) ||
      ((zparams.flags & kRangeOpt) == kRangeOpt)) {
    builder->SendError("GT, LT, and/or NX options at the same time are not compatible");
    return;
  }

  absl::flat_hash_set<string_view> members_set;
  absl::InlinedVector<ScoredMemberView, 4> members;

  unsigned num_members = (args.size() - i) / 2;

  // We sort the fields if the expected encoding could be listpack.
  bool to_sort_fields = false;

  if (num_members > 2) {
    members.reserve(num_members);

    members_set.reserve(num_members);
    to_sort_fields = true;
  }

  for (; i < args.size(); i += 2) {
    string_view cur_arg = ArgS(args, i);
    double val = 0;

    // Parse the score. Treats Nan as invalid double.
    if (!ParseDouble(cur_arg, &val)) {
      VLOG(1) << "Bad score:" << cur_arg << "|";
      return builder->SendError(kInvalidFloatErr);
    }

    string_view member = ArgS(args, i + 1);
    if (to_sort_fields) {
      auto [_, inserted] = members_set.insert(member);
      to_sort_fields &= inserted;
    }
    members.emplace_back(val, member);
  }
  DCHECK(cmd_cntx.tx);

  if (to_sort_fields) {
    if (num_members == 2) {  // fix unique_members for this special case.
      if (members[0].second == members[1].second) {
        to_sort_fields = false;
      }
    }
    if (to_sort_fields) {
      std::sort(members.begin(), members.end());
    }
  }

  absl::Span memb_sp{members.data(), members.size()};
  ZAddGeneric(key, zparams, memb_sp, cmd_cntx.tx, builder);
}

void ZSetFamily::ZCard(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    auto find_res = t->GetDbSlice(shard->shard_id()).FindReadOnly(t->GetDbContext(), key, OBJ_ZSET);
    if (!find_res) {
      return find_res.status();
    }

    return find_res.value()->second.Size();
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    cmd_cntx.rb->SendError(kWrongTypeErr);
    return;
  }

  cmd_cntx.rb->SendLong(result.value());
}

void ZSetFamily::ZCount(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  ScoreInterval si;
  if (!ParseBound(min_s, &si.first) || !ParseBound(max_s, &si.second)) {
    return cmd_cntx.rb->SendError(kFloatRangeErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCount(t->GetOpArgs(shard), key, si);
  };

  OpResult<unsigned> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    cmd_cntx.rb->SendError(kWrongTypeErr);
  } else {
    cmd_cntx.rb->SendLong(*result);
  }
}

void ZSetFamily::ZDiff(CmdArgList args, const CommandContext& cmd_cntx) {
  vector<vector<ScoredMap>> maps(shard_set->size());
  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] = OpFetch(shard, t);
    return OpStatus::OK;
  };

  cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  const string_view key = ArgS(args, 1);
  const ShardId sid = Shard(key, maps.size());
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  // Extract the ScoredMap of the first key
  auto& sm = maps[sid];
  if (sm.empty()) {
    rb->SendEmptyArray();
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
  rb->StartArray(result.size() * (with_scores ? 2 : 1));
  for (const auto& [score, key] : smvec) {
    rb->SendBulkString(key);
    if (with_scores) {
      rb->SendDouble(score);
    }
  }
}

void ZSetFamily::ZIncrBy(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view score_arg = ArgS(args, 1);

  ScoredMemberView scored_member;
  scored_member.second = ArgS(args, 2);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (!absl::SimpleAtod(score_arg, &scored_member.first)) {
    VLOG(1) << "Bad score:" << score_arg << "|";
    return rb->SendError(kInvalidFloatErr);
  }

  if (isnan(scored_member.first)) {
    return rb->SendError(kScoreNaN);
  }

  ZSetFamily::ZParams zparams;
  zparams.flags = ZADD_IN_INCR;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), zparams, key, ScoredMemberSpan{&scored_member, 1});
  };

  OpResult<AddResult> add_result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (add_result.status() == OpStatus::WRONG_TYPE) {
    return rb->SendError(kWrongTypeErr);
  }

  if (add_result.status() == OpStatus::SKIPPED) {
    return rb->SendNull();
  }

  if (add_result->is_nan) {
    return rb->SendError(kScoreNaN);
  }

  rb->SendDouble(add_result->new_score);
}

void ZSetFamily::ZInter(CmdArgList args, const CommandContext& cmd_cntx) {
  ZBooleanOperation(args, "zinter", false, false, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZInterStore(CmdArgList args, const CommandContext& cmd_cntx) {
  ZBooleanOperation(args, "zinterstore", false, true, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZInterCard(CmdArgList args, const CommandContext& cmd_cntx) {
  unsigned num_keys;
  auto* builder = cmd_cntx.rb;

  if (!absl::SimpleAtoi(ArgS(args, 0), &num_keys)) {
    return builder->SendError(OpStatus::SYNTAX_ERR);
  }

  uint64_t limit = 0;
  if (args.size() == (1 + num_keys + 2) && ArgS(args, 1 + num_keys) == "LIMIT") {
    if (!absl::SimpleAtoi(ArgS(args, 1 + num_keys + 1), &limit)) {
      return builder->SendError("limit value is not a positive integer", kSyntaxErrType);
    }
  } else if (args.size() != 1 + num_keys) {
    return builder->SendError(kSyntaxErr);
  }

  vector<OpResult<ScoredMap>> maps(shard_set->size(), OpStatus::SKIPPED);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    maps[shard->shard_id()] = OpInter(shard, t, "", AggType::NOOP, {}, false);
    return OpStatus::OK;
  };

  cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  OpResult<ScoredMap> result = IntersectResults(maps, AggType::NOOP);
  if (!result)
    return builder->SendError(result.status());

  if (0 < limit && limit < result.value().size()) {
    return builder->SendLong(limit);
  }
  builder->SendLong(result.value().size());
}

void ZSetFamily::ZMPop(CmdArgList args, const CommandContext& cmd_cntx) {
  uint32 num_keys;
  bool is_max;
  int pop_count;
  if (!ValidateZMPopCommand(args, &num_keys, &is_max, &pop_count, cmd_cntx.rb)) {
    return;
  }
  auto* response_builder = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  // From the list of input keys, keep the first (in the order of keys in the command) key found in
  // the current shard.
  std::vector<std::optional<std::string_view>> first_found_key_per_shard_vec(shard_set->size(),
                                                                             std::nullopt);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    std::optional<std::string_view> result = GetFirstNonEmptyKeyFound(shard, t);
    if (result.has_value()) {
      first_found_key_per_shard_vec[shard->shard_id()] = result;
    }
    return OpStatus::OK;
  };

  cmd_cntx.tx->Execute(std::move(cb), false /* possibly another hop */);

  // Keep all the keys found (first only for each shard) in a set for fast lookups.
  absl::flat_hash_set<std::string_view> first_found_keys_for_shard;
  // We can have at most one result from each shard.
  first_found_keys_for_shard.reserve(std::min(shard_set->size(), num_keys));
  for (const auto& key : first_found_key_per_shard_vec) {
    if (!key.has_value()) {
      continue;
    }
    first_found_keys_for_shard.insert(*key);
  }

  // Now that we have the first non empty key from each shard, find the first overall first key and
  // pop elements from it.
  std::optional<std::string_view> key_to_pop = std::nullopt;
  ArgRange arg_keys(args.subspan(1, num_keys));
  // Find the first arg_key which exists in any shard and is not empty.
  for (std::string_view key : arg_keys) {
    if (first_found_keys_for_shard.contains(key)) {
      key_to_pop = key;
      break;
    }
  }

  if (!key_to_pop.has_value()) {
    cmd_cntx.tx->Conclude();
    response_builder->SendNull();
    return;
  }

  // Pop elements from relevant set.
  OpResult<ScoredArray> pop_result =
      ZPopMinMaxInternal(*key_to_pop, FilterShards::YES, pop_count, is_max, cmd_cntx.tx);

  if (pop_result.status() == OpStatus::WRONG_TYPE) {
    return response_builder->SendError(kWrongTypeErr);
  }

  LOG_IF(WARNING, !pop_result) << "Unexpected status " << pop_result.status();
  response_builder->SendLabeledScoredArray(*key_to_pop, pop_result.value());
}

void ZSetFamily::ZPopMax(CmdArgList args, const CommandContext& cmd_cntx) {
  ZPopMinMaxFromArgs(std::move(args), true, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZPopMin(CmdArgList args, const CommandContext& cmd_cntx) {
  ZPopMinMaxFromArgs(std::move(args), false, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZLexCount(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  LexInterval li;
  if (!ParseLexBound(min_s, &li.first) || !ParseLexBound(max_s, &li.second)) {
    return cmd_cntx.rb->SendError(kLexRangeErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpLexCount(t->GetOpArgs(shard), key, li);
  };

  OpResult<unsigned> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    cmd_cntx.rb->SendError(kWrongTypeErr);
  } else {
    cmd_cntx.rb->SendLong(*result);
  }
}

void ZSetFamily::ZRange(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args, cmd_cntx.tx, cmd_cntx.rb, RangeParams{});
}

void ZSetFamily::ZRank(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRankGeneric(args, false, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZRevRange(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args, cmd_cntx.tx, cmd_cntx.rb, RangeParams{.reverse = true});
}

void ZSetFamily::ZRangeByScore(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args, cmd_cntx.tx, cmd_cntx.rb, RangeParams{.interval_type = RangeParams::SCORE});
}

void ZSetFamily::ZRangeStore(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args.subspan(1), cmd_cntx.tx, cmd_cntx.rb,
                RangeParams{.with_scores = true, .store_key = ArgS(args, 0)});
}

void ZSetFamily::ZRevRangeByScore(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args, cmd_cntx.tx, cmd_cntx.rb,
                RangeParams{.reverse = true, .interval_type = RangeParams::SCORE});
}

void ZSetFamily::ZRevRank(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRankGeneric(args, true, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZRangeByLex(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args, cmd_cntx.tx, cmd_cntx.rb, RangeParams{.interval_type = RangeParams::LEX});
}

void ZSetFamily::ZRevRangeByLex(CmdArgList args, const CommandContext& cmd_cntx) {
  ZRangeGeneric(args, cmd_cntx.tx, cmd_cntx.rb,
                RangeParams{.reverse = true, .interval_type = RangeParams::LEX});
}

void ZSetFamily::ZRemRangeByRank(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  IndexInterval ii;
  if (!SimpleAtoi(min_s, &ii.first) || !SimpleAtoi(max_s, &ii.second)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  ZRangeSpec range_spec;
  range_spec.interval = ii;
  ZRemRangeGeneric(key, range_spec, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZRemRangeByScore(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  ScoreInterval si;
  if (!ParseBound(min_s, &si.first) || !ParseBound(max_s, &si.second)) {
    return cmd_cntx.rb->SendError(kFloatRangeErr);
  }

  ZRangeSpec range_spec;

  range_spec.interval = si;

  ZRemRangeGeneric(key, range_spec, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZRemRangeByLex(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view min_s = ArgS(args, 1);
  string_view max_s = ArgS(args, 2);

  LexInterval li;
  if (!ParseLexBound(min_s, &li.first) || !ParseLexBound(max_s, &li.second)) {
    return cmd_cntx.rb->SendError(kLexRangeErr);
  }

  ZRangeSpec range_spec;

  range_spec.interval = li;

  ZRemRangeGeneric(key, range_spec, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZRem(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  auto members = args.subspan(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(t->GetOpArgs(shard), key, members);
  };

  OpResult<unsigned> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    cmd_cntx.rb->SendError(kWrongTypeErr);
  } else {
    cmd_cntx.rb->SendLong(*result);
  }
}

void ZSetFamily::ZRandMember(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (args.size() > 3)
    return rb->SendError(WrongNumArgsError("ZRANDMEMBER"));

  CmdArgParser parser{args};
  string_view key = parser.Next();

  bool is_count = parser.HasNext();
  int count = is_count ? parser.Next<int>() : 1;

  ZSetFamily::RangeParams params;
  params.with_scores = static_cast<bool>(parser.Check("WITHSCORES"));

  if (parser.HasNext())
    return rb->SendError(absl::StrCat("Unsupported option:", string_view(parser.Next())));

  if (auto err = parser.Error(); err)
    return rb->SendError(err->MakeReply());

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRandMember(count, params, t->GetOpArgs(shard), key);
  };

  OpResult<ScoredArray> result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (result) {
    rb->SendScoredArray(result.value(), params.with_scores);
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    if (is_count) {
      rb->SendScoredArray(ScoredArray(), params.with_scores);
    } else {
      rb->SendNull();
    }
  } else {
    rb->SendError(result.status());
  }
}

void ZSetFamily::ZScore(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view member = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScore(t->GetOpArgs(shard), key, member);
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  OpResult<double> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    rb->SendError(kWrongTypeErr);
  } else if (!result) {
    rb->SendNull();
  } else {
    rb->SendDouble(*result);
  }
}

void ZSetFamily::ZMScore(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  OpResult<MScoreResponse> result = ZGetMembers(args, cmd_cntx.tx, rb);

  if (result.status() == OpStatus::WRONG_TYPE) {
    return rb->SendError(kWrongTypeErr);
  }
  rb->StartArray(result->size());  // Array return type.
  const MScoreResponse& array = result.value();
  for (const auto& p : array) {
    if (p) {
      rb->SendDouble(*p);
    } else {
      rb->SendNull();
    }
  }
}

void ZSetFamily::ZScan(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view token = ArgS(args, 1);

  uint64_t cursor = 0;

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (!absl::SimpleAtoi(token, &cursor)) {
    return rb->SendError("invalid cursor");
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(2));
  if (!ops) {
    DVLOG(1) << "Scan invalid args - return " << ops << " to the user";
    return rb->SendError(ops.status());
  }
  ScanOpts scan_op = ops.value();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(t->GetOpArgs(shard), key, &cursor, scan_op);
  };

  OpResult<StringVec> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() != OpStatus::WRONG_TYPE) {
    rb->StartArray(2);
    rb->SendBulkString(absl::StrCat(cursor));
    rb->StartArray(result->size());  // Within scan the returned page is of type array.
    for (const auto& k : *result) {
      rb->SendBulkString(k);
    }
  } else {
    rb->SendError(result.status());
  }
}

void ZSetFamily::ZUnion(CmdArgList args, const CommandContext& cmd_cntx) {
  ZBooleanOperation(args, "zunion", true, false, cmd_cntx.tx, cmd_cntx.rb);
}

void ZSetFamily::ZUnionStore(CmdArgList args, const CommandContext& cmd_cntx) {
  ZBooleanOperation(args, "zunionstore", true, true, cmd_cntx.tx, cmd_cntx.rb);
}

#define HFUNC(x) SetHandler(&ZSetFamily::x)

namespace acl {
constexpr uint32_t kZAdd = WRITE | SORTEDSET | FAST;
constexpr uint32_t kBZPopMin = WRITE | SORTEDSET | FAST | BLOCKING;
constexpr uint32_t kBZPopMax = WRITE | SORTEDSET | FAST | BLOCKING;
constexpr uint32_t kZCard = READ | SORTEDSET | FAST;
constexpr uint32_t kZCount = READ | SORTEDSET | FAST;
constexpr uint32_t kZDiff = READ | SORTEDSET | SLOW;
constexpr uint32_t kZIncrBy = WRITE | SORTEDSET | FAST;
constexpr uint32_t kZInterStore = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZInter = READ | SORTEDSET | SLOW;
constexpr uint32_t kZInterCard = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZLexCount = READ | SORTEDSET | FAST;
constexpr uint32_t kZMPop = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZPopMax = WRITE | SORTEDSET | FAST;
constexpr uint32_t kZPopMin = WRITE | SORTEDSET | FAST;
constexpr uint32_t kZRem = WRITE | SORTEDSET | FAST;
constexpr uint32_t kZRange = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRandMember = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRank = READ | SORTEDSET | FAST;
constexpr uint32_t kZRangeByLex = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRangeByScore = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRangeStore = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZScore = READ | SORTEDSET | FAST;
constexpr uint32_t kZMScore = READ | SORTEDSET | FAST;
constexpr uint32_t kZRemRangeByRank = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZRemRangeByScore = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZRemRangeByLex = WRITE | SORTEDSET | SLOW;
constexpr uint32_t kZRevRange = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRevRangeByLex = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRevRangeByScore = READ | SORTEDSET | SLOW;
constexpr uint32_t kZRevRank = READ | SORTEDSET | FAST;
constexpr uint32_t kZScan = READ | SORTEDSET | SLOW;
constexpr uint32_t kZUnion = READ | SORTEDSET | SLOW;
constexpr uint32_t kZUnionStore = WRITE | SORTEDSET | SLOW;
}  // namespace acl

void ZSetFamily::Register(CommandRegistry* registry) {
  constexpr uint32_t kStoreMask = CO::WRITE | CO::VARIADIC_KEYS | CO::DENYOOM;
  registry->StartFamily();
  // TODO: to add support for SCRIPT for BZPOPMIN, BZPOPMAX similarly to BLPOP.
  *registry
      << CI{"ZADD", CO::FAST | CO::WRITE | CO::DENYOOM, -4, 1, 1, acl::kZAdd}.HFUNC(ZAdd)
      << CI{"BZPOPMIN",    CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2,
            acl::kBZPopMin}
             .HFUNC(BZPopMin)
      << CI{"BZPOPMAX",    CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2,
            acl::kBZPopMax}
             .HFUNC(BZPopMax)
      << CI{"ZCARD", CO::FAST | CO::READONLY, 2, 1, 1, acl::kZCard}.HFUNC(ZCard)
      << CI{"ZCOUNT", CO::FAST | CO::READONLY, 4, 1, 1, acl::kZCount}.HFUNC(ZCount)
      << CI{"ZDIFF", CO::READONLY | CO::VARIADIC_KEYS, -3, 2, 2, acl::kZDiff}.HFUNC(ZDiff)
      << CI{"ZINCRBY", CO::FAST | CO::WRITE, 4, 1, 1, acl::kZIncrBy}.HFUNC(ZIncrBy)
      << CI{"ZINTERSTORE", kStoreMask, -4, 3, 3, acl::kZInterStore}.HFUNC(ZInterStore)
      << CI{"ZINTER", CO::READONLY | CO::VARIADIC_KEYS, -3, 2, 2, acl::kZInter}.HFUNC(ZInter)
      << CI{"ZINTERCARD", CO::READONLY | CO::VARIADIC_KEYS, -3, 2, 2, acl::kZInterCard}.HFUNC(
             ZInterCard)
      << CI{"ZLEXCOUNT", CO::READONLY, 4, 1, 1, acl::kZLexCount}.HFUNC(ZLexCount)
      << CI{"ZMPOP", CO::SLOW | CO::WRITE | CO::VARIADIC_KEYS, -4, 2, 2, acl::kZMPop}.HFUNC(ZMPop)

      << CI{"ZPOPMAX", CO::FAST | CO::WRITE, -2, 1, 1, acl::kZPopMax}.HFUNC(ZPopMax)
      << CI{"ZPOPMIN", CO::FAST | CO::WRITE, -2, 1, 1, acl::kZPopMin}.HFUNC(ZPopMin)
      << CI{"ZREM", CO::FAST | CO::WRITE, -3, 1, 1, acl::kZRem}.HFUNC(ZRem)
      << CI{"ZRANGE", CO::READONLY, -4, 1, 1, acl::kZRange}.HFUNC(ZRange)
      << CI{"ZRANDMEMBER", CO::READONLY, -2, 1, 1, acl::kZRandMember}.HFUNC(ZRandMember)
      << CI{"ZRANK", CO::READONLY | CO::FAST, -3, 1, 1, acl::kZRank}.HFUNC(ZRank)
      << CI{"ZRANGEBYLEX", CO::READONLY, -4, 1, 1, acl::kZRangeByLex}.HFUNC(ZRangeByLex)
      << CI{"ZRANGEBYSCORE", CO::READONLY, -4, 1, 1, acl::kZRangeByScore}.HFUNC(ZRangeByScore)
      << CI{"ZRANGESTORE", CO::WRITE | CO::DENYOOM, -5, 1, 2, acl::kZRangeStore}.HFUNC(ZRangeStore)
      << CI{"ZSCORE", CO::READONLY | CO::FAST, 3, 1, 1, acl::kZScore}.HFUNC(ZScore)
      << CI{"ZMSCORE", CO::READONLY | CO::FAST, -3, 1, 1, acl::kZMScore}.HFUNC(ZMScore)
      << CI{"ZREMRANGEBYRANK", CO::WRITE, 4, 1, 1, acl::kZRemRangeByRank}.HFUNC(ZRemRangeByRank)
      << CI{"ZREMRANGEBYSCORE", CO::WRITE, 4, 1, 1, acl::kZRemRangeByScore}.HFUNC(ZRemRangeByScore)
      << CI{"ZREMRANGEBYLEX", CO::WRITE, 4, 1, 1, acl::kZRemRangeByLex}.HFUNC(ZRemRangeByLex)
      << CI{"ZREVRANGE", CO::READONLY, -4, 1, 1, acl::kZRevRange}.HFUNC(ZRevRange)
      << CI{"ZREVRANGEBYLEX", CO::READONLY, -4, 1, 1, acl::kZRevRangeByLex}.HFUNC(ZRevRangeByLex)
      << CI{"ZREVRANGEBYSCORE", CO::READONLY, -4, 1, 1, acl::kZRevRangeByScore}.HFUNC(
             ZRevRangeByScore)
      << CI{"ZREVRANK", CO::READONLY | CO::FAST, -3, 1, 1, acl::kZRevRank}.HFUNC(ZRevRank)
      << CI{"ZSCAN", CO::READONLY, -3, 1, 1, acl::kZScan}.HFUNC(ZScan)
      << CI{"ZUNION", CO::READONLY | CO::VARIADIC_KEYS, -3, 2, 2, acl::kZUnion}.HFUNC(ZUnion)
      << CI{"ZUNIONSTORE", kStoreMask, -4, 3, 3, acl::kZUnionStore}.HFUNC(ZUnionStore);
}

}  // namespace dfly
