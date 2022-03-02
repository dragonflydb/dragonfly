// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/set_family.h"

extern "C" {
#include "redis/intset.h"
#include "redis/object.h"
#include "redis/util.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using ResultStringVec = vector<OpResult<vector<string>>>;
using ResultSetView = OpResult<absl::flat_hash_set<std::string_view>>;
using SvArray = vector<std::string_view>;

namespace {

void FillSet(robj* src, absl::flat_hash_set<string>* dest) {
  auto* si = setTypeInitIterator(src);
  sds ele;
  int64_t llele;
  int encoding;
  while ((encoding = setTypeNext(si, &ele, &llele)) != -1) {
    if (encoding == OBJ_ENCODING_HT) {
      std::string_view sv{ele, sdslen(ele)};
      dest->emplace(sv);
    } else {
      dest->emplace(absl::StrCat(llele));
    }
  }
  setTypeReleaseIterator(si);
}

vector<string> ToVec(absl::flat_hash_set<string>&& set) {
  vector<string> result(set.size());
  size_t i = 0;

  // extract invalidates current iterator. therefore, we increment it first before extracting.
  // hence the weird loop.
  for (auto it = set.begin(); it != set.end();) {
    result[i] = std::move(set.extract(it++).value());
    ++i;
  }

  return result;
}

bool IsInSet(const robj* s, int64_t val) {
  if (s->encoding == OBJ_ENCODING_INTSET)
    return intsetFind((intset*)s->ptr, val);

  /* in order to compare an integer with an object we
   * have to use the generic function, creating an object
   * for this */
  DCHECK_EQ(s->encoding, OBJ_ENCODING_HT);
  sds elesds = sdsfromlonglong(val);
  bool res = setTypeIsMember(s, elesds);
  sdsfree(elesds);

  return res;
}

ResultSetView UnionResultVec(const ResultStringVec& result_vec) {
  absl::flat_hash_set<std::string_view> uniques;

  for (const auto& val : result_vec) {
    if (val || val.status() == OpStatus::SKIPPED) {
      for (const string& s : val.value()) {
        uniques.emplace(s);
      }
      continue;
    }

    if (val.status() != OpStatus::KEY_NOTFOUND) {
      return val.status();
    }
  }

  return uniques;
}

ResultSetView DiffResultVec(const ResultStringVec& result_vec, ShardId src_shard) {
  for (const auto& res : result_vec) {
    if (res.status() == OpStatus::WRONG_TYPE)
      return res.status();
  }

  absl::flat_hash_set<std::string_view> uniques;

  for (const auto& val : result_vec[src_shard].value()) {
    uniques.emplace(val);
  }

  for (unsigned i = 0; i < result_vec.size(); ++i) {
    if (i == src_shard)
      continue;

    if (result_vec[i]) {
      for (const string& s : result_vec[i].value()) {
        uniques.erase(s);
      }
    }
  }
  return uniques;
}

OpResult<SvArray> InterResultVec(const ResultStringVec& result_vec, unsigned required_shard_cnt) {
  absl::flat_hash_map<std::string_view, unsigned> uniques;

  bool first = true;
  for (const auto& res : result_vec) {
    if (res.status() == OpStatus::SKIPPED)
      continue;
    if (res.status() == OpStatus::KEY_NOTFOUND)
      return SvArray{};
    if (!res) {
      return res.status();
    }

    // I use this awkward 'first' condition instead of table[s]++ deliberately.
    // I do not want to add keys that I know will not stay in the set.
    if (first) {
      for (const string& s : res.value()) {
        uniques.emplace(s, 1);
      }
      first = false;
    } else {
      for (const string& s : res.value()) {
        auto it = uniques.find(s);
        if (it != uniques.end()) {
          ++it->second;
        }
      }
    }
  }

  SvArray result;
  result.reserve(uniques.size());

  for (const auto& k_v : uniques) {
    if (k_v.second == required_shard_cnt) {
      result.push_back(k_v.first);
    }
  }

  return result;
}

SvArray ToSvArray(const absl::flat_hash_set<std::string_view>& set) {
  SvArray result;
  result.reserve(set.size());
  copy(set.begin(), set.end(), back_inserter(result));
  return result;
}

OpStatus NoOpCb(Transaction* t, EngineShard* shard) {
  return OpStatus::OK;
};

// if overwrite is true then OpAdd writes vals into the key and discards its previous value.
OpResult<uint32_t> OpAdd(const OpArgs& op_args, std::string_view key, const ArgSlice& vals,
                         bool overwrite) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();
  if (overwrite && vals.empty()) {
    auto it = db_slice.FindExt(op_args.db_ind, key).first;
    db_slice.Del(op_args.db_ind, it);
    return 0;
  }

  const auto [it, inserted] = db_slice.AddOrFind(op_args.db_ind, key);

  robj* o = nullptr;

  if (!inserted) {
    db_slice.PreUpdate(op_args.db_ind, it);
  }

  if (inserted || overwrite) {
    bool int_set = true;
    long long intv;

    for (auto v : vals) {
      if (!string2ll(v.data(), v.size(), &intv)) {
        int_set = false;
        break;
      }
    }

    if (int_set) {
      o = createIntsetObject();
    } else {
      o = createSetObject();
    }

    it->second.ImportRObj(o);
  } else {
    // We delibirately check only now because with othewrite=true
    // we may write into object of a different type via ImportRObj above.
    if (it->second.ObjType() != OBJ_SET)
      return OpStatus::WRONG_TYPE;
  }

  o = it->second.AsRObj();

  uint32_t res = 0;
  for (auto val : vals) {
    es->tmp_str1 = sdscpylen(es->tmp_str1, val.data(), val.size());
    res += setTypeAdd(o, es->tmp_str1);
  }
  it->second.SyncRObj();

  db_slice.PostUpdate(op_args.db_ind, it);

  return res;
}

OpResult<uint32_t> OpRem(const OpArgs& op_args, std::string_view key, const ArgSlice& vals) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();
  OpResult<MainIterator> find_res = db_slice.Find(op_args.db_ind, key, OBJ_SET);
  if (!find_res) {
    return find_res.status();
  }

  db_slice.PreUpdate(op_args.db_ind, *find_res);
  uint32_t res = 0;
  robj* o = find_res.value()->second.AsRObj();

  for (auto val : vals) {
    es->tmp_str1 = sdscpylen(es->tmp_str1, val.data(), val.size());
    res += setTypeRemove(o, es->tmp_str1);
  }

  if (res && setTypeSize(o) == 0) {
    CHECK(db_slice.Del(op_args.db_ind, find_res.value()));
  } else {
    db_slice.PostUpdate(op_args.db_ind, *find_res);
  }

  return res;
}

class Mover {
 public:
  Mover(std::string_view src, std::string_view dest, std::string_view member)
      : src_(src), dest_(dest), member_(member) {
  }

  void Find(Transaction* t);
  OpResult<unsigned> Commit(Transaction* t);

 private:
  OpStatus OpFind(Transaction* t, EngineShard* es);
  OpStatus OpMutate(Transaction* t, EngineShard* es);

  std::string_view src_, dest_, member_;
  OpResult<bool> found_[2];
};

OpStatus Mover::OpFind(Transaction* t, EngineShard* es) {
  ArgSlice largs = t->ShardArgsInShard(es->shard_id());
  for (auto k : largs) {
    bool index = (k == src_) ? 0 : 1;
    OpResult<MainIterator> res = es->db_slice().Find(t->db_index(), k, OBJ_SET);
    if (res && index == 0) {
      CHECK(!res->is_done());
      es->tmp_str1 = sdscpylen(es->tmp_str1, member_.data(), member_.size());
      int found_memb = setTypeIsMember(res.value()->second.AsRObj(), es->tmp_str1);
      found_[0] = (found_memb == 1);
    } else {
      found_[index] = res.status();
    }
  }

  return OpStatus::OK;
}

OpStatus Mover::OpMutate(Transaction* t, EngineShard* es) {
  ArgSlice largs = t->ShardArgsInShard(es->shard_id());
  OpArgs op_args{es, t->db_index()};
  for (auto k : largs) {
    if (k == src_) {
      CHECK_EQ(1u, OpRem(op_args, k, {member_}).value());
    } else {
      CHECK_EQ(k, dest_);
      OpAdd(op_args, k, {member_}, false);
    }
  }

  return OpStatus::OK;
}

void Mover::Find(Transaction* t) {
  t->Execute([this](Transaction* t, EngineShard* es) { return this->OpFind(t, es); }, false);
}

OpResult<unsigned> Mover::Commit(Transaction* t) {
  OpResult<unsigned> res;
  bool return_early = false;

  if (found_[0].status() == OpStatus::WRONG_TYPE || found_[1].status() == OpStatus::WRONG_TYPE) {
    res = OpStatus::WRONG_TYPE;
    return_early = true;
  } else if (!found_[0].value_or(false)) {
    res = 0;
    return_early = true;
  } else {
    res = 1;
    return_early = (src_ == dest_);
  }

  if (return_early) {
    t->Execute(&NoOpCb, true);
    return res;
  }

  t->Execute([this](Transaction* t, EngineShard* es) { return this->OpMutate(t, es); }, true);
  return res;
}

}  // namespace

void SetFamily::SAdd(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  vector<std::string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }
  ArgSlice arg_slice{vals.data(), vals.size()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpAdd(op_args, key, arg_slice, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
    return;
  }

  switch (result.status()) {
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    default:
      LOG(ERROR) << "unexpected opstatus " << result.status();
  }

  return (*cntx)->SendNull();
}

void SetFamily::SIsMember(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view val = ArgS(args, 2);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpResult<MainIterator> find_res = shard->db_slice().Find(t->db_index(), key, OBJ_SET);

    shard->tmp_str1 = sdscpylen(shard->tmp_str1, val.data(), val.size());

    int res = setTypeIsMember(find_res.value()->second.AsRObj(), shard->tmp_str1);

    return res == 1 ? OpStatus::OK : OpStatus::INVALID_VALUE;
  };

  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result.status()) {
    case OpStatus::OK:
      return (*cntx)->SendLong(1);
    default:
      return (*cntx)->SendLong(0);
  }
}

void SetFamily::SMove(CmdArgList args, ConnectionContext* cntx) {
  std::string_view src = ArgS(args, 1);
  std::string_view dest = ArgS(args, 2);
  std::string_view member = ArgS(args, 3);

  Mover mover{src, dest, member};
  cntx->transaction->Schedule();

  mover.Find(cntx->transaction);

  OpResult<unsigned> result = mover.Commit(cntx->transaction);
  if (!result) {
    return (*cntx)->SendError(result.status());
    return;
  }

  (*cntx)->SendLong(result.value());
}

void SetFamily::SRem(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  vector<std::string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }
  ArgSlice span{vals.data(), vals.size()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(OpArgs{shard, t->db_index()}, key, span);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    case OpStatus::OK:
      return (*cntx)->SendLong(result.value());
    default:
      return (*cntx)->SendLong(0);
  }
}

void SetFamily::SCard(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    OpResult<MainIterator> find_res = shard->db_slice().Find(t->db_index(), key, OBJ_SET);
    if (!find_res) {
      return find_res.status();
    }

    return setTypeSize(find_res.value()->second.AsRObj());
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::OK:
      return (*cntx)->SendLong(result.value());
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    default:
      return (*cntx)->SendLong(0);
  }
}

void SetFamily::SPop(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  unsigned count = 1;
  if (args.size() > 2) {
    std::string_view arg = ArgS(args, 2);
    if (!absl::SimpleAtoi(arg, &count)) {
      (*cntx)->SendError(kInvalidIntErr);
      return;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPop(OpArgs{shard, t->db_index()}, key, count);
  };

  OpResult<StringSet> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    if (args.size() == 2) {  // SPOP key
      if (result.status() == OpStatus::KEY_NOTFOUND) {
        (*cntx)->SendNull();
      } else {
        DCHECK_EQ(1u, result.value().size());
        (*cntx)->SendBulkString(result.value().front());
      }
    } else {  // SPOP key cnt
      SvArray vec{result->begin(), result->end()};
      (*cntx)->SendStringArr(vec);
    }
    return;
  }

  (*cntx)->SendError(result.status());
}

void SetFamily::SDiff(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(cntx->transaction->shard_set()->size(), OpStatus::SKIPPED);
  std::string_view src_key = ArgS(args, 1);
  ShardId src_shard = Shard(src_key, result_set.size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->ShardArgsInShard(shard->shard_id());
    if (shard->shard_id() == src_shard) {
      CHECK_EQ(src_key, largs.front());
      result_set[shard->shard_id()] = OpDiff(t, shard);
    } else {
      result_set[shard->shard_id()] = OpUnion(OpArgs{shard, t->db_index()}, largs);
    }

    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));
  ResultSetView rsv = DiffResultVec(result_set, src_shard);
  if (!rsv) {
    (*cntx)->SendError(rsv.status());
    return;
  }

  SvArray arr = ToSvArray(rsv.value());
  if (cntx->conn_state.script_info) {  // sort under script
    sort(arr.begin(), arr.end());
  }
  (*cntx)->SendStringArr(arr);
}

void SetFamily::SDiffStore(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(cntx->transaction->shard_set()->size(), OpStatus::SKIPPED);
  std::string_view dest_key = ArgS(args, 1);
  ShardId dest_shard = Shard(dest_key, result_set.size());
  std::string_view src_key = ArgS(args, 2);
  ShardId src_shard = Shard(src_key, result_set.size());

  auto diff_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->ShardArgsInShard(shard->shard_id());
    DCHECK(!largs.empty());

    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      largs.remove_prefix(1);
      if (largs.empty())
        return OpStatus::OK;
    }

    if (shard->shard_id() == src_shard) {
      CHECK_EQ(src_key, largs.front());
      result_set[shard->shard_id()] = OpDiff(t, shard);
    } else {
      result_set[shard->shard_id()] = OpUnion(OpArgs{shard, t->db_index()}, largs);
    }
    return OpStatus::OK;
  };

  cntx->transaction->Schedule();
  cntx->transaction->Execute(std::move(diff_cb), false);
  ResultSetView rsv = DiffResultVec(result_set, src_shard);
  if (!rsv) {
    cntx->transaction->Execute(NoOpCb, true);
    (*cntx)->SendError(rsv.status());
    return;
  }

  SvArray result = ToSvArray(rsv.value());
  auto store_cb = [&](Transaction* t, EngineShard* shard) {
    if (shard->shard_id() == dest_shard) {
      OpAdd(OpArgs{shard, t->db_index()}, dest_key, result, true);
    }

    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);
  (*cntx)->SendLong(result.size());
}

void SetFamily::SMembers(CmdArgList args, ConnectionContext* cntx) {
  auto cb = [](Transaction* t, EngineShard* shard) { return OpInter(t, shard, false); };
  OpResult<StringSet> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    SvArray arr{result->begin(), result->end()};
    if (cntx->conn_state.script_info) {  // sort under script
      sort(arr.begin(), arr.end());
    }
    (*cntx)->SendStringArr(arr);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void SetFamily::SInter(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(cntx->transaction->shard_set()->size(), OpStatus::SKIPPED);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    result_set[shard->shard_id()] = OpInter(t, shard, false);

    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));
  OpResult<SvArray> result = InterResultVec(result_set, cntx->transaction->unique_shard_cnt());
  if (result) {
    SvArray arr = std::move(*result);
    if (cntx->conn_state.script_info) {  // sort under script
      sort(arr.begin(), arr.end());
    }
    (*cntx)->SendStringArr(arr);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void SetFamily::SInterStore(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(cntx->transaction->shard_set()->size(), OpStatus::SKIPPED);
  std::string_view dest_key = ArgS(args, 1);
  ShardId dest_shard = Shard(dest_key, result_set.size());
  atomic_uint32_t inter_shard_cnt{0};

  auto inter_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->ShardArgsInShard(shard->shard_id());
    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      if (largs.size() == 1)
        return OpStatus::OK;
    }
    inter_shard_cnt.fetch_add(1, memory_order_relaxed);
    result_set[shard->shard_id()] = OpInter(t, shard, shard->shard_id() == dest_shard);
    return OpStatus::OK;
  };

  cntx->transaction->Schedule();
  cntx->transaction->Execute(std::move(inter_cb), false);

  OpResult<SvArray> result = InterResultVec(result_set, inter_shard_cnt.load(memory_order_relaxed));
  if (!result) {
    cntx->transaction->Execute(NoOpCb, true);
    (*cntx)->SendError(result.status());
    return;
  }

  auto store_cb = [&](Transaction* t, EngineShard* shard) {
    if (shard->shard_id() == dest_shard) {
      OpAdd(OpArgs{shard, t->db_index()}, dest_key, result.value(), true);
    }

    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);
  (*cntx)->SendLong(result->size());
}

void SetFamily::SUnion(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(cntx->transaction->shard_set()->size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->ShardArgsInShard(shard->shard_id());
    result_set[shard->shard_id()] = OpUnion(OpArgs{shard, t->db_index()}, largs);
    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));

  ResultSetView unionset = UnionResultVec(result_set);
  if (unionset) {
    SvArray arr = ToSvArray(*unionset);
    if (cntx->conn_state.script_info) {  // sort under script
      sort(arr.begin(), arr.end());
    }
    (*cntx)->SendStringArr(arr);
  } else {
    (*cntx)->SendError(unionset.status());
  }
}

void SetFamily::SUnionStore(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(cntx->transaction->shard_set()->size(), OpStatus::SKIPPED);
  std::string_view dest_key = ArgS(args, 1);
  ShardId dest_shard = Shard(dest_key, result_set.size());

  auto union_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->ShardArgsInShard(shard->shard_id());
    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      largs.remove_prefix(1);
      if (largs.empty())
        return OpStatus::OK;
    }
    result_set[shard->shard_id()] = OpUnion(OpArgs{shard, t->db_index()}, largs);
    return OpStatus::OK;
  };

  cntx->transaction->Schedule();
  cntx->transaction->Execute(std::move(union_cb), false);

  ResultSetView unionset = UnionResultVec(result_set);
  if (!unionset) {
    cntx->transaction->Execute(NoOpCb, true);
    (*cntx)->SendError(unionset.status());
    return;
  }

  SvArray result = ToSvArray(unionset.value());

  auto store_cb = [&](Transaction* t, EngineShard* shard) {
    if (shard->shard_id() == dest_shard) {
      OpAdd(OpArgs{shard, t->db_index()}, dest_key, result, true);
    }

    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);
  (*cntx)->SendLong(result.size());
}

auto SetFamily::OpUnion(const OpArgs& op_args, const ArgSlice& keys) -> OpResult<StringSet> {
  DCHECK(!keys.empty());
  absl::flat_hash_set<string> uniques;

  for (std::string_view key : keys) {
    OpResult<MainIterator> find_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_SET);
    if (find_res) {
      robj* sobj = find_res.value()->second.AsRObj();
      FillSet(sobj, &uniques);
      continue;
    }

    if (find_res.status() != OpStatus::KEY_NOTFOUND) {
      return find_res.status();
    }
  }

  return ToVec(std::move(uniques));
}

auto SetFamily::OpDiff(const Transaction* t, EngineShard* es) -> OpResult<StringSet> {
  ArgSlice keys = t->ShardArgsInShard(es->shard_id());
  DCHECK(!keys.empty());

  OpResult<MainIterator> find_res = es->db_slice().Find(t->db_index(), keys.front(), OBJ_SET);

  if (!find_res) {
    return find_res.status();
  }

  absl::flat_hash_set<string> uniques;

  robj* sobj = find_res.value()->second.AsRObj();
  FillSet(sobj, &uniques);
  DCHECK(!uniques.empty());  // otherwise the key would not exist.

  for (size_t i = 1; i < keys.size(); ++i) {
    OpResult<MainIterator> diff_res = es->db_slice().Find(t->db_index(), keys[i], OBJ_SET);
    if (!find_res) {
      if (find_res.status() == OpStatus::WRONG_TYPE) {
        return OpStatus::WRONG_TYPE;
      }
      continue;
    }

    sobj = diff_res.value()->second.AsRObj();
    auto* si = setTypeInitIterator(sobj);
    sds ele;
    int64_t llele;
    int encoding;
    while ((encoding = setTypeNext(si, &ele, &llele)) != -1) {
      if (encoding == OBJ_ENCODING_HT) {
        std::string_view sv{ele, sdslen(ele)};
        uniques.erase(sv);
      } else {
        absl::AlphaNum an(llele);
        uniques.erase(an.Piece());
      }
    }
    setTypeReleaseIterator(si);
  }

  return ToVec(std::move(uniques));
}

auto SetFamily::OpPop(const OpArgs& op_args, std::string_view key, unsigned count)
    -> OpResult<StringSet> {
  auto* es = op_args.shard;
  OpResult<MainIterator> find_res = es->db_slice().Find(op_args.db_ind, key, OBJ_SET);
  if (!find_res)
    return find_res.status();

  if (count == 0)
    return StringSet{};

  MainIterator it = find_res.value();
  robj* sobj = it->second.AsRObj();
  auto slen = setTypeSize(sobj);
  StringSet result;

  /* CASE 1:
   * The number of requested elements is greater than or equal to
   * the number of elements inside the set: simply return the whole set. */
  if (count >= slen) {
    /* We just return the entire set */
    auto* si = setTypeInitIterator(sobj);
    sds ele;
    while ((ele = setTypeNextObject(si)) != NULL) {
      std::string_view sv{ele, sdslen(ele)};
      result.emplace_back(sv);
      sdsfree(ele);
    }

    setTypeReleaseIterator(si);

    /* Delete the set as it is now empty */
    CHECK(es->db_slice().Del(op_args.db_ind, it));
  } else {
    sds sdsele;
    int64_t llele;

    while (count--) {
      /* Emit and remove. */
      int encoding = setTypeRandomElement(sobj, &sdsele, &llele);
      if (encoding == OBJ_ENCODING_INTSET) {
        result.emplace_back(absl::StrCat(llele));
        sobj->ptr = intsetRemove((intset*)sobj->ptr, llele, NULL);
      } else {
        result.emplace_back(std::string_view{sdsele, sdslen(sdsele)});
        setTypeRemove(sobj, sdsele);
      }
    }
    it->second.SyncRObj();
  }
  return result;
}

auto SetFamily::OpInter(const Transaction* t, EngineShard* es, bool remove_first)
    -> OpResult<StringSet> {
  ArgSlice keys = t->ShardArgsInShard(es->shard_id());
  if (remove_first) {
    keys.remove_prefix(1);
  }
  DCHECK(!keys.empty());
  vector<robj> sets(keys.size());  // we must copy by value because AsRObj is temporary.

  for (size_t i = 0; i < keys.size(); ++i) {
    OpResult<MainIterator> find_res = es->db_slice().Find(t->db_index(), keys[i], OBJ_SET);
    if (!find_res)
      return find_res.status();
    robj* sobj = find_res.value()->second.AsRObj();
    sets[i] = *sobj;
  }

  auto comp = [](const robj& left, const robj& right) {
    return setTypeSize(&left) < setTypeSize(&right);
  };

  std::sort(sets.begin(), sets.end(), comp);
  int encoding;
  sds elesds;
  int64_t intobj;

  StringSet result;
  // TODO: the whole code is awful. imho, the encoding is the same for the same object.
  /* Iterate all the elements of the first (smallest) set, and test
   * the element against all the other sets, if at least one set does
   * not include the element it is discarded */
  auto* si = setTypeInitIterator(&sets[0]);
  while ((encoding = setTypeNext(si, &elesds, &intobj)) != -1) {
    size_t j = 1;
    for (; j < sets.size(); j++) {
      if (sets[j].ptr == sets[0].ptr)  //  when provide the same key several times.
        continue;
      if (encoding == OBJ_ENCODING_INTSET) {
        /* intset with intset is simple... and fast */
        if (!IsInSet(&sets[j], intobj))
          break;
      } else if (encoding == OBJ_ENCODING_HT) {
        if (!setTypeIsMember(&sets[j], elesds)) {
          break;
        }
      }
    }

    /* Only take action when all sets contain the member */
    if (j == sets.size()) {
      if (encoding == OBJ_ENCODING_HT) {
        result.emplace_back(std::string_view{elesds, sdslen(elesds)});
      } else {
        DCHECK_EQ(unsigned(encoding), OBJ_ENCODING_INTSET);
        result.push_back(absl::StrCat(intobj));
      }
    }
  }
  setTypeReleaseIterator(si);

  return result;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&SetFamily::x)

void SetFamily::Register(CommandRegistry* registry) {
  *registry << CI{"SADD", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(SAdd)
            << CI{"SDIFF", CO::READONLY, -2, 1, -1, 1}.HFUNC(SDiff)
            << CI{"SDIFFSTORE", CO::WRITE | CO::DENYOOM, -3, 1, -1, 1}.HFUNC(SDiffStore)
            << CI{"SINTER", CO::READONLY, -2, 1, -1, 1}.HFUNC(SInter)
            << CI{"SINTERSTORE", CO::WRITE | CO::DENYOOM, -3, 1, -1, 1}.HFUNC(SInterStore)
            << CI{"SMEMBERS", CO::READONLY, 2, 1, 1, 1}.HFUNC(SMembers)
            << CI{"SISMEMBER", CO::FAST | CO::READONLY, 3, 1, 1, 1}.HFUNC(SIsMember)
            << CI{"SMOVE", CO::FAST | CO::WRITE, 4, 1, 2, 1}.HFUNC(SMove)
            << CI{"SREM", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(SRem)
            << CI{"SCARD", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(SCard)
            << CI{"SPOP", CO::WRITE | CO::RANDOM | CO::FAST, -2, 1, 1, 1}.HFUNC(SPop)
            << CI{"SUNION", CO::READONLY, -2, 1, -1, 1}.HFUNC(SUnion)
            << CI{"SUNIONSTORE", CO::WRITE | CO::DENYOOM, -3, 1, -1, 1}.HFUNC(SUnionStore);
}

}  // namespace dfly
