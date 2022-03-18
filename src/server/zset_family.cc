// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/zset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/zset.h"
}

#include "base/logging.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

using CI = CommandId;

static const char kNxXxErr[] = "XX and NX options at the same time are not compatible";
constexpr unsigned kMaxZiplistValue = 64;

OpResult<MainIterator> FindZEntry(unsigned flags, const OpArgs& op_args, string_view key,
                                  size_t member_len) {
  auto& db_slice = op_args.shard->db_slice();
  if (flags & ZADD_IN_XX) {
    return db_slice.Find(op_args.db_ind, key, OBJ_ZSET);
  }

  auto [it, inserted] = db_slice.AddOrFind(op_args.db_ind, key);
  if (inserted) {
    robj* zobj = nullptr;

    if (member_len > kMaxZiplistValue) {
      zobj = createZsetObject();
    } else {
      zobj = createZsetListpackObject();
    }
    it->second.ImportRObj(zobj);
  } else {
    if (it->second.ObjType() != OBJ_ZSET)
      return OpStatus::WRONG_TYPE;
  }
  return it;
}

class IntervalVisitor {
 public:
  IntervalVisitor(const ZSetFamily::RangeParams& params, robj* o) : params_(params), zobj_(o) {
  }

  void operator()(const ZSetFamily::IndexInterval& ii);

  void operator()(const ZSetFamily::ScoreInterval& si);

  ZSetFamily::ScoredArray PopResult() {
    return std::move(result_);
  }

 private:
  void ExtractListPack(const zrangespec& range);
  void ExtractSkipList(const zrangespec& range);

  void Next(uint8_t* zl, uint8_t** eptr, uint8_t** sptr) const {
    if (reverse_) {
      zzlPrev(zl, eptr, sptr);
    } else {
      zzlNext(zl, eptr, sptr);
    }
  }

  bool IsUnder(double score, const zrangespec& spec) const {
    return reverse_ ? zslValueGteMin(score, &spec) : zslValueLteMax(score, &spec);
  }

  void AddResult(const uint8_t* vstr, unsigned vlen, long long vlon, double score);

  ZSetFamily::RangeParams params_;
  robj* zobj_;

  bool reverse_ = false;
  ZSetFamily::ScoredArray result_;
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

  unsigned rangelen = (end - start) + 1;

  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    unsigned char* zl = (uint8_t*)zobj_->ptr;
    unsigned char *eptr, *sptr;
    unsigned char* vstr;
    unsigned int vlen;
    long long vlong;
    double score = 0.0;

    if (reverse_)
      eptr = lpSeek(zl, -2 - (2 * start));
    else
      eptr = lpSeek(zl, 2 * start);

    sptr = lpNext(zl, eptr);

    while (rangelen--) {
      DCHECK(eptr != NULL && sptr != NULL);
      vstr = lpGetValue(eptr, &vlen, &vlong);

      if (params_.with_scores) /* don't bother to extract the score if it's gonna be ignored. */
        score = zzlGetScore(sptr);

      AddResult(vstr, vlen, vlong, score);

      Next(zl, &eptr, &sptr);
    }
  } else if (zobj_->encoding == OBJ_ENCODING_SKIPLIST) {
    zset* zs = (zset*)zobj_->ptr;
    zskiplist* zsl = zs->zsl;
    zskiplistNode* ln;

    /* Check if starting point is trivial, before doing log(N) lookup. */
    if (reverse_) {
      ln = zsl->tail;
      if (start > 0)
        ln = zslGetElementByRank(zsl, llen - start);
    } else {
      ln = zsl->header->level[0].forward;
      if (start > 0)
        ln = zslGetElementByRank(zsl, start + 1);
    }

    while (rangelen--) {
      DCHECK(ln != NULL);
      sds ele = ln->ele;
      result_.emplace_back(string(ele, sdslen(ele)), ln->score);
      ln = reverse_ ? ln->backward : ln->level[0].forward;
    }
  } else {
    LOG(FATAL) << "Unknown sorted set encoding" << zobj_->encoding;
  }
}

void IntervalVisitor::ExtractListPack(const zrangespec& range) {
  uint8_t* zl = (uint8_t*)zobj_->ptr;
  uint8_t *eptr, *sptr;
  uint8_t* vstr;
  unsigned int vlen;
  long long vlong;
  unsigned rangelen = 0;
  unsigned offset = params_.offset;
  unsigned limit = params_.limit;

  /* If reversed, get the last node in range as starting point. */
  if (reverse_) {
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

    rangelen++;
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
  unsigned rangelen = 0;

  /* If reversed, get the last node in range as starting point. */
  if (reverse_) {
    ln = zslLastInRange(zsl, &range);
  } else {
    ln = zslFirstInRange(zsl, &range);
  }

  /* If there is an offset, just traverse the number of elements without
   * checking the score because that is done in the next loop. */
  while (ln && offset--) {
    if (reverse_) {
      ln = ln->backward;
    } else {
      ln = ln->level[0].forward;
    }
  }

  while (ln && limit--) {
    /* Abort when the node is no longer in range. */
    if (!IsUnder(ln->score, range))
      break;

    rangelen++;
    result_.emplace_back(string{ln->ele, sdslen(ln->ele)}, ln->score);

    /* Move to next node */
    if (reverse_) {
      ln = ln->backward;
    } else {
      ln = ln->level[0].forward;
    }
  }
}

void IntervalVisitor::operator()(const ZSetFamily::ScoreInterval& si) {
  zrangespec range;
  range.min = si.first.val;
  range.max = si.second.val;
  range.minex = si.first.is_open;
  range.maxex = si.second.is_open;

  if (zobj_->encoding == OBJ_ENCODING_LISTPACK) {
    ExtractListPack(range);
  } else if (zobj_->encoding == OBJ_ENCODING_SKIPLIST) {
    ExtractSkipList(range);
  } else {
    LOG(FATAL) << "Unknown sorted set encoding " << zobj_->encoding;
  }
}

void IntervalVisitor::AddResult(const uint8_t* vstr, unsigned vlen, long long vlong, double score) {
  if (vstr == NULL) {
    result_.emplace_back(absl::StrCat(vlong), score);
  } else {
    result_.emplace_back(string{reinterpret_cast<const char*>(vstr), vlen}, score);
  }
}

bool ParseScore(string_view src, double* score) {
  if (src == "-inf") {
    *score = -HUGE_VAL;
  } else if (src == "+inf") {
    *score = HUGE_VAL;
  } else {
    return absl::SimpleAtod(src, score);
  }
  return true;
};

bool ParseBound(string_view src, ZSetFamily::Bound* bound) {
  if (src.empty())
    return false;

  if (src[0] == '(') {
    bound->is_open = true;
    src.remove_prefix(1);
  }

  return ParseScore(src, &bound->val);
}

}  // namespace

void ZSetFamily::ZCard(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    OpResult<MainIterator> find_res = shard->db_slice().Find(t->db_index(), key, OBJ_ZSET);
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

void ZSetFamily::ZAdd(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);

  ZParams zparams;
  size_t i = 2;
  for (; i < args.size() - 1; ++i) {
    ToUpper(&args[i]);

    std::string_view cur_arg = ArgS(args, i);

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

  if ((zparams.flags & ZADD_IN_NX) && (zparams.flags & (ZADD_IN_GT | ZADD_IN_LT))) {
    (*cntx)->SendError("GT, LT, and/or NX options at the same time are not compatible");
    return;
  }

  absl::InlinedVector<ScoredMemberView, 4> members;
  for (; i < args.size(); i += 2) {
    std::string_view cur_arg = ArgS(args, i);
    double val;
    if (!ParseScore(cur_arg, &val)) {
      (*cntx)->SendError(kInvalidFloatErr);
      return;
    }
    std::string_view member = ArgS(args, i + 1);
    members.emplace_back(val, member);
  }
  DCHECK(cntx->transaction);

  if (zparams.flags & ZADD_IN_INCR) {
    LOG(FATAL) << "TBD";
    return;
  }

  absl::Span memb_sp{members.data(), members.size()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpAdd(zparams, op_args, key, memb_sp);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else {
    (*cntx)->SendLong(result.value());
  }
}

void ZSetFamily::ZIncrBy(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void ZSetFamily::ZRange(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view min_s = ArgS(args, 2);
  std::string_view max_s = ArgS(args, 3);

  bool parse_score = false;
  RangeParams range_params;

  for (size_t i = 4; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);
    if (cur_arg == "BYSCORE") {
      parse_score = true;
    } else if (cur_arg == "WITHSCORES") {
      range_params.with_scores = true;
    } else {
      return cntx->reply_builder()->SendError(absl::StrCat("unsupported option ", cur_arg));
    }
  }

  if (parse_score) {
    ZRangeByScoreInternal(key, min_s, max_s, range_params, cntx);
    return;
  }

  IndexInterval ii;

  if (!absl::SimpleAtoi(min_s, &ii.first) || !absl::SimpleAtoi(max_s, &ii.second)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  ZRangeSpec range_spec;
  range_spec.params = range_params;
  range_spec.interval = ii;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpRange(range_spec, op_args, key);
  };

  OpResult<ScoredArray> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  OutputScoredArrayResult(result, range_params.with_scores, cntx);
}

void ZSetFamily::ZRangeByScore(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view min_s = ArgS(args, 2);
  std::string_view max_s = ArgS(args, 3);

  RangeParams range_params;

  for (size_t i = 4; i < args.size(); ++i) {
    ToUpper(&args[i]);

    string_view cur_arg = ArgS(args, i);
    if (cur_arg == "WITHSCORES") {
      range_params.with_scores = true;
    } else {
      return cntx->reply_builder()->SendError(absl::StrCat("unsupported option ", cur_arg));
    }
  }

  ZRangeByScoreInternal(key, min_s, max_s, range_params, cntx);
}

void ZSetFamily::ZRem(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);

  absl::InlinedVector<std::string_view, 8> members(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    members[i - 2] = ArgS(args, i);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpRem(op_args, key, members);
  };

  OpResult<unsigned> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else {
    (*cntx)->SendLong(result.value());
  }
}

void ZSetFamily::ZScore(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view member = ArgS(args, 2);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpScore(op_args, key, member);
  };

  OpResult<double> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(kWrongTypeErr);
  } else if (!result) {
    (*cntx)->SendNull();
  } else {
    (*cntx)->SendDouble(result.value());
  }
}

void ZSetFamily::ZRangeByScoreInternal(std::string_view key, std::string_view min_s,
                                       std::string_view max_s, const RangeParams& params,
                                       ConnectionContext* cntx) {
  ZRangeSpec range_spec;
  range_spec.params = params;

  ScoreInterval si;
  if (!ParseBound(min_s, &si.first) ||
      !ParseBound(max_s, &si.second)) {
    return (*cntx)->SendError("min or max is not a float");
  }
  range_spec.interval = si;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpRange(range_spec, op_args, key);
  };

  OpResult<ScoredArray> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  OutputScoredArrayResult(result, params.with_scores, cntx);
}

void ZSetFamily::OutputScoredArrayResult(const OpResult<ScoredArray>& result, bool with_scores,
                                         ConnectionContext* cntx) {
  if (result.status() == OpStatus::WRONG_TYPE) {
    return (*cntx)->SendError(kWrongTypeErr);
  }

  LOG_IF(WARNING, !result && result.status() != OpStatus::KEY_NOTFOUND)
      << "Unexpected status " << result.status();

  (*cntx)->StartArray(result->size() * (with_scores ? 2 : 1));
  for (const auto& p : result.value()) {
    (*cntx)->SendBulkString(p.first);

    if (with_scores) {
      (*cntx)->SendDouble(p.second);
    }
  }
}

OpResult<unsigned> ZSetFamily::OpAdd(const ZParams& zparams, const OpArgs& op_args, string_view key,
                                     ScoredMemberSpan members) {
  DCHECK(!members.empty());
  OpResult<MainIterator> res_it =
      FindZEntry(zparams.flags, op_args, key, members.front().second.size());

  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();

  unsigned added = 0;
  unsigned updated = 0;
  unsigned processed = 0;

  sds& tmp_str = op_args.shard->tmp_str1;

  for (size_t j = 0; j < members.size(); j++) {
    const auto& m = members[j];
    tmp_str = sdscpylen(tmp_str, m.second.data(), m.second.size());

    int retflags = 0;
    int retval = zsetAdd(zobj, m.first, tmp_str, zparams.flags, &retflags, nullptr);

    if (retval == 0) {
      LOG(FATAL) << "unexpected error in zsetAdd: " << m.first;
    }

    if (retflags & ZADD_OUT_ADDED)
      added++;
    if (retflags & ZADD_OUT_UPDATED)
      updated++;
    if (!(retflags & ZADD_OUT_NOP))
      processed++;
  }
  res_it.value()->second.SyncRObj();

  return zparams.ch ? added + updated : added;
}

OpResult<unsigned> ZSetFamily::OpRem(const OpArgs& op_args, string_view key, ArgSlice members) {
  OpResult<MainIterator> res_it = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  sds& tmp_str = op_args.shard->tmp_str1;
  unsigned deleted = 0;
  for (string_view member : members) {
    tmp_str = sdscpylen(tmp_str, member.data(), member.size());
    deleted += zsetDel(zobj, tmp_str);
  }
  auto zlen = zsetLength(zobj);
  res_it.value()->second.SyncRObj();

  if (zlen == 0) {
    CHECK(op_args.shard->db_slice().Del(op_args.db_ind, res_it.value()));
  }

  return deleted;
}

OpResult<double> ZSetFamily::OpScore(const OpArgs& op_args, string_view key, string_view member) {
  OpResult<MainIterator> res_it = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_ZSET);
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

auto ZSetFamily::OpRange(const ZRangeSpec& range_spec, const OpArgs& op_args, std::string_view key)
    -> OpResult<ScoredArray> {
  OpResult<MainIterator> res_it = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_ZSET);
  if (!res_it)
    return res_it.status();

  robj* zobj = res_it.value()->second.AsRObj();
  IntervalVisitor iv{range_spec.params, zobj};

  absl::visit(iv, range_spec.interval);

  return iv.PopResult();
}

#define HFUNC(x) SetHandler(&ZSetFamily::x)

void ZSetFamily::Register(CommandRegistry* registry) {
  *registry << CI{"ZCARD", CO::FAST | CO::READONLY, 2, 1, 1, 1}.HFUNC(ZCard)
            << CI{"ZADD", CO::FAST | CO::WRITE | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(ZAdd)
            << CI{"ZINCRBY", CO::FAST | CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(ZIncrBy)
            << CI{"ZREM", CO::FAST | CO::WRITE, -3, 1, 1, 1}.HFUNC(ZRem)
            << CI{"ZRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRange)
            << CI{"ZRANGEBYSCORE", CO::READONLY, -4, 1, 1, 1}.HFUNC(ZRangeByScore)
            << CI{"ZSCORE", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ZScore);
}

}  // namespace dfly
