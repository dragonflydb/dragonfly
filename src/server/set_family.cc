// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/set_family.h"

extern "C" {
#include "redis/intset.h"
#include "redis/object.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
}

#include "base/logging.h"
#include "base/stl_util.h"
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

constexpr uint32_t kMaxIntSetEntries = 256;

intset* IntsetAddSafe(string_view val, intset* is, bool* success, bool* added) {
  long long llval;
  *added = false;
  if (!string2ll(val.data(), val.size(), &llval)) {
    *success = false;
    return is;
  }

  uint8_t inserted = 0;
  is = intsetAdd(is, llval, &inserted);
  if (inserted) {
    *added = true;
    *success = intsetLen(is) <= kMaxIntSetEntries;
  } else {
    *added = false;
    *success = true;
  }

  return is;
}

// returns (removed, isempty)
pair<unsigned, bool> RemoveSet(ArgSlice vals, CompactObj* set) {
  bool isempty = false;
  unsigned removed = 0;

  if (set->Encoding() == kEncodingIntSet) {
    intset* is = (intset*)set->RObjPtr();
    long long llval;

    for (auto val : vals) {
      if (!string2ll(val.data(), val.size(), &llval)) {
        continue;
      }

      int is_removed = 0;
      is = intsetRemove(is, llval, &is_removed);
      removed += is_removed;
    }
    isempty = (intsetLen(is) == 0);
    set->SetRObjPtr(is);
  } else {
    dict* d = (dict*)set->RObjPtr();
    auto* shard = EngineShard::tlocal();
    for (auto member : vals) {
      shard->tmp_str1 = sdscpylen(shard->tmp_str1, member.data(), member.size());
      int result = dictDelete(d, shard->tmp_str1);
      removed += (result == DICT_OK);
    }
    isempty = (dictSize(d) == 0);
  }
  return make_pair(removed, isempty);
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

  for (const auto& res : result_vec) {
    if (!res && !base::_in(res.status(), {OpStatus::SKIPPED, OpStatus::KEY_NOTFOUND}))
      return res.status();
  }

  for (const auto& res : result_vec) {
    if (res.status() == OpStatus::KEY_NOTFOUND)
      return OpStatus::OK;  // empty set.
  }

  bool first = true;
  for (const auto& res : result_vec) {
    if (res.status() == OpStatus::SKIPPED)
        continue;

    DCHECK(res);  // we handled it above.

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

using SetType = pair<void*, unsigned>;

uint32_t SetTypeLen(const SetType& set) {
  if (set.second == kEncodingStrMap) {
    return dictSize((const dict*)set.first);
  }
  DCHECK_EQ(set.second, kEncodingIntSet);
  return intsetLen((const intset*)set.first);
};

bool dictContains(const dict* d, string_view key) {
  uint64_t h = dictGenHashFunction(key.data(), key.size());

  for (unsigned table = 0; table <= 1; table++) {
    uint64_t idx = h & DICTHT_SIZE_MASK(d->ht_size_exp[table]);
    dictEntry* he = d->ht_table[table][idx];
    while (he) {
      sds dkey = (sds)he->key;
      if (sdslen(dkey) == key.size() && (key.empty() || memcmp(dkey, key.data(), key.size()) == 0))
        return true;
      he = he->next;
    }
    if (!dictIsRehashing(d))
      break;
  }
  return false;
}

bool IsInSet(const SetType& st, int64_t val) {
  if (st.second == kEncodingIntSet)
    return intsetFind((intset*)st.first, val);

  DCHECK_EQ(st.second, kEncodingStrMap);
  char buf[32];
  char* next = absl::numbers_internal::FastIntToBuffer(val, buf);

  return dictContains((dict*)st.first, string_view{buf, size_t(next - buf)});
}

bool IsInSet(const SetType& st, string_view member) {
  if (st.second == kEncodingIntSet) {
    long long llval;
    if (!string2ll(member.data(), member.size(), &llval))
      return false;

    return intsetFind((intset*)st.first, llval);
  }
  DCHECK_EQ(st.second, kEncodingStrMap);

  return dictContains((dict*)st.first, member);
}

template <typename F> void FillSet(const SetType& set, F&& f) {
  if (set.second == kEncodingIntSet) {
    intset* is = (intset*)set.first;
    int64_t ival;
    int ii = 0;
    char buf[32];

    while (intsetGet(is, ii++, &ival)) {
      char* next = absl::numbers_internal::FastIntToBuffer(ival, buf);
      f(string{buf, size_t(next - buf)});
    }
  } else {
    dict* ds = (dict*)set.first;
    string str;
    dictIterator* di = dictGetIterator(ds);
    dictEntry* de = nullptr;
    while ((de = dictNext(di))) {
      str.assign((sds)de->key, sdslen((sds)de->key));
      f(move(str));
    }
    dictReleaseIterator(di);
  }
}

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
  if (!inserted) {
    db_slice.PreUpdate(op_args.db_ind, it);
  }

  CompactObj& co = it->second;

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
      intset* is = intsetNew();
      co.InitRobj(OBJ_SET, kEncodingIntSet, is);
    } else {
      dict* ds = dictCreate(&setDictType);
      co.InitRobj(OBJ_SET, kEncodingStrMap, ds);
    }
  } else {
    // We delibirately check only now because with othewrite=true
    // we may write into object of a different type via ImportRObj above.
    if (co.ObjType() != OBJ_SET)
      return OpStatus::WRONG_TYPE;
  }

  void* inner_obj = co.RObjPtr();
  uint32_t res = 0;

  if (co.Encoding() == kEncodingIntSet) {
    intset* is = (intset*)inner_obj;
    bool success = true;

    for (auto val : vals) {
      bool added = false;
      is = IntsetAddSafe(val, is, &success, &added);
      res += added;

      if (!success) {
        dict* ds = dictCreate(&setDictType);
        SetFamily::ConvertTo(is, ds);

        co.SetRObjPtr(is);
        co.InitRobj(OBJ_SET, kEncodingStrMap, ds);  // 'is' is deleted by co.
        inner_obj = ds;
        break;
      }
    }

    if (success)
      co.SetRObjPtr(is);
  }

  if (co.Encoding() == kEncodingStrMap) {
    dict* ds = (dict*)inner_obj;

    for (auto member : vals) {
      es->tmp_str1 = sdscpylen(es->tmp_str1, member.data(), member.size());
      dictEntry* de = dictAddRaw(ds, es->tmp_str1, NULL);
      if (de) {
        de->key = sdsdup(es->tmp_str1);
        ++res;
      }
    }
  }

  db_slice.PostUpdate(op_args.db_ind, it);

  return res;
}

OpResult<uint32_t> OpRem(const OpArgs& op_args, std::string_view key, const ArgSlice& vals) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();
  OpResult<PrimeIterator> find_res = db_slice.Find(op_args.db_ind, key, OBJ_SET);
  if (!find_res) {
    return find_res.status();
  }

  db_slice.PreUpdate(op_args.db_ind, *find_res);
  CompactObj& co = find_res.value()->second;
  auto [removed, isempty] = RemoveSet(vals, &co);

  if (isempty) {
    CHECK(db_slice.Del(op_args.db_ind, find_res.value()));
  } else {
    db_slice.PostUpdate(op_args.db_ind, *find_res);
  }

  return removed;
}

// For SMOVE. Comprised of 2 transactional steps: Find and Commit.
// After Find Mover decides on the outcome of the operation, applies it in commit
// and reports the result.
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

  // In case both src and dest are in the same shard, largs size will be 2.
  DCHECK_LE(largs.size(), 2u);

  for (auto k : largs) {
    unsigned index = (k == src_) ? 0 : 1;
    OpResult<PrimeIterator> res = es->db_slice().Find(t->db_index(), k, OBJ_SET);
    if (res && index == 0) {  // succesful src find.
      DCHECK(!res->is_done());
      const CompactObj& val = res.value()->second;
      SetType st{val.RObjPtr(), val.Encoding()};
      found_[0] = IsInSet(st, member_);
    } else {
      found_[index] = res.status();
    }
  }

  return OpStatus::OK;
}

OpStatus Mover::OpMutate(Transaction* t, EngineShard* es) {
  ArgSlice largs = t->ShardArgsInShard(es->shard_id());
  DCHECK_LE(largs.size(), 2u);

  OpArgs op_args{es, t->db_index()};
  for (auto k : largs) {
    if (k == src_) {
      CHECK_EQ(1u, OpRem(op_args, k, {member_}).value());  // must succeed.
    } else {
      DCHECK_EQ(k, dest_);
      OpAdd(op_args, k, {member_}, false);
    }
  }

  return OpStatus::OK;
}

void Mover::Find(Transaction* t) {
  // non-concluding step.
  t->Execute([this](Transaction* t, EngineShard* es) { return this->OpFind(t, es); }, false);
}

OpResult<unsigned> Mover::Commit(Transaction* t) {
  OpResult<unsigned> res;
  bool noop = false;

  if (found_[0].status() == OpStatus::WRONG_TYPE || found_[1].status() == OpStatus::WRONG_TYPE) {
    res = OpStatus::WRONG_TYPE;
    noop = true;
  } else if (!found_[0].value_or(false)) {
    res = 0;
    noop = true;
  } else {
    res = 1;
    noop = (src_ == dest_);
  }

  if (noop) {
    t->Execute(&NoOpCb, true);
  } else {
    t->Execute([this](Transaction* t, EngineShard* es) { return this->OpMutate(t, es); }, true);
  }

  return res;
}

void ScanCallback(void* privdata, const dictEntry* de) {
  StringVec* sv = (StringVec*)privdata;
  sds key = (sds)de->key;
  sv->push_back(string(key, sdslen(key)));
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
    OpResult<PrimeIterator> find_res = shard->db_slice().Find(t->db_index(), key, OBJ_SET);

    if (find_res) {
      SetType st{find_res.value()->second.RObjPtr(), find_res.value()->second.Encoding()};
      return IsInSet(st, val) ? OpStatus::OK : OpStatus::KEY_NOTFOUND;
    }

    return find_res.status();
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
    OpResult<PrimeIterator> find_res = shard->db_slice().Find(t->db_index(), key, OBJ_SET);
    if (!find_res) {
      return find_res.status();
    }

    return find_res.value()->second.Size();
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

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    if (args.size() == 2) {  // SPOP key
      if (result.status() == OpStatus::KEY_NOTFOUND) {
        (*cntx)->SendNull();
      } else {
        DCHECK_EQ(1u, result.value().size());
        (*cntx)->SendBulkString(result.value().front());
      }
    } else {  // SPOP key cnt
      (*cntx)->SendStringArr(*result);
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
      result_set[shard->shard_id()] = OpDiff(OpArgs{shard, t->db_index()}, largs);
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

  VLOG(1) << "SDiffStore " << src_key << " " << src_shard;

  auto diff_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->ShardArgsInShard(shard->shard_id());
    DCHECK(!largs.empty());

    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      largs.remove_prefix(1);
      if (largs.empty())
        return OpStatus::OK;
    }

    OpArgs op_args{shard, t->db_index()};
    if (shard->shard_id() == src_shard) {
      CHECK_EQ(src_key, largs.front());
      result_set[shard->shard_id()] = OpDiff(op_args, largs);
    } else {
      result_set[shard->shard_id()] = OpUnion(op_args, largs);
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

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    StringVec& svec = result.value();

    if (cntx->conn_state.script_info) {  // sort under script
      sort(svec.begin(), svec.end());
    }
    (*cntx)->SendStringArr(*result);
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

void SetFamily::SScan(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view token = ArgS(args, 2);

  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  if (args.size() > 3) {
    return (*cntx)->SendError("scan options are not supported yet");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(OpArgs{shard, t->db_index()}, key, &cursor);
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() != OpStatus::WRONG_TYPE) {
    (*cntx)->StartArray(2);
    (*cntx)->SendSimpleString(absl::StrCat(cursor));
    (*cntx)->StartArray(result->size());
    for (const auto& k : *result) {
      (*cntx)->SendBulkString(k);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

OpResult<StringVec> SetFamily::OpUnion(const OpArgs& op_args, ArgSlice keys) {
  DCHECK(!keys.empty());
  absl::flat_hash_set<string> uniques;

  for (std::string_view key : keys) {
    OpResult<PrimeIterator> find_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_SET);
    if (find_res) {
      SetType st{find_res.value()->second.RObjPtr(), find_res.value()->second.Encoding()};
      FillSet(st, [&uniques](string s) { uniques.emplace(move(s)); });
      continue;
    }

    if (find_res.status() != OpStatus::KEY_NOTFOUND) {
      return find_res.status();
    }
  }

  return ToVec(std::move(uniques));
}

OpResult<StringVec> SetFamily::OpDiff(const OpArgs& op_args, ArgSlice keys) {
  DCHECK(!keys.empty());
  DVLOG(1) << "OpDiff from " << keys.front();
  EngineShard* es = op_args.shard;
  OpResult<PrimeIterator> find_res = es->db_slice().Find(op_args.db_ind, keys.front(), OBJ_SET);

  if (!find_res) {
    return find_res.status();
  }

  absl::flat_hash_set<string> uniques;
  SetType st{find_res.value()->second.RObjPtr(), find_res.value()->second.Encoding()};

  FillSet(st, [&uniques](string s) { uniques.insert(move(s)); });

  DCHECK(!uniques.empty());  // otherwise the key would not exist.

  for (size_t i = 1; i < keys.size(); ++i) {
    OpResult<PrimeIterator> diff_res = es->db_slice().Find(op_args.db_ind, keys[i], OBJ_SET);
    if (!diff_res) {
      if (diff_res.status() == OpStatus::WRONG_TYPE) {
        return OpStatus::WRONG_TYPE;
      }
      continue;  // KEY_NOTFOUND
    }

    SetType st2{diff_res.value()->second.RObjPtr(), diff_res.value()->second.Encoding()};
    if (st2.second == kEncodingIntSet) {
      int ii = 0;
      intset* is = (intset*)st2.first;
      int64_t intele;
      char buf[32];

      while (intsetGet(is, ii++, &intele)) {
        char* next = absl::numbers_internal::FastIntToBuffer(intele, buf);
        uniques.erase(string_view{buf, size_t(next - buf)});
      }
    } else {
      DCHECK_EQ(kEncodingStrMap, st2.second);
      dict* ds = (dict*)st2.first;
      dictIterator* di = dictGetIterator(ds);
      dictEntry* de = nullptr;
      while ((de = dictNext(di))) {
        sds key = (sds)de->key;
        uniques.erase(string_view{key, sdslen(key)});
      }
      dictReleaseIterator(di);
    }
  }

  return ToVec(std::move(uniques));
}

OpResult<StringVec> SetFamily::OpPop(const OpArgs& op_args, std::string_view key, unsigned count) {
  auto* es = op_args.shard;
  OpResult<PrimeIterator> find_res = es->db_slice().Find(op_args.db_ind, key, OBJ_SET);
  if (!find_res)
    return find_res.status();

  StringVec result;
  if (count == 0)
    return result;

  PrimeIterator it = find_res.value();
  size_t slen = it->second.Size();
  SetType st{it->second.RObjPtr(), it->second.Encoding()};

  /* CASE 1:
   * The number of requested elements is greater than or equal to
   * the number of elements inside the set: simply return the whole set. */
  if (count >= slen) {
    FillSet(st, [&result](string s) { result.push_back(move(s)); });
    /* Delete the set as it is now empty */
    CHECK(es->db_slice().Del(op_args.db_ind, it));
  } else {
    if (st.second == kEncodingIntSet) {
      intset* is = (intset*)st.first;
      int64_t val = 0;

      // copy last count values.
      for (uint32_t i = slen - count; i < slen; ++i) {
        intsetGet(is, i, &val);
        result.push_back(absl::StrCat(val));
      }

      is = intsetTrimTail(is, count);  // now remove last count items
      it->second.SetRObjPtr(is);
    } else {
      dict* ds = (dict*)st.first;
      string str;
      dictIterator* di = dictGetSafeIterator(ds);
      for (uint32_t i = 0; i < count; ++i) {
        dictEntry* de = dictNext(di);
        DCHECK(de);
        result.emplace_back((sds)de->key, sdslen((sds)de->key));
        dictDelete(ds, de->key);
      }
      dictReleaseIterator(di);
    }
  }
  return result;
}

OpResult<StringVec> SetFamily::OpInter(const Transaction* t, EngineShard* es, bool remove_first) {
  ArgSlice keys = t->ShardArgsInShard(es->shard_id());
  if (remove_first) {
    keys.remove_prefix(1);
  }
  DCHECK(!keys.empty());

  StringVec result;
  if (keys.size() == 1) {
    OpResult<PrimeIterator> find_res = es->db_slice().Find(t->db_index(), keys.front(), OBJ_SET);
    if (!find_res)
      return find_res.status();

    SetType st{find_res.value()->second.RObjPtr(), find_res.value()->second.Encoding()};

    FillSet(st, [&result](string s) { result.push_back(move(s)); });
    return result;
  }

  // we must copy by value because AsRObj is temporary.
  vector<SetType> sets(keys.size());

  OpStatus status = OpStatus::OK;

  for (size_t i = 0; i < keys.size(); ++i) {
    OpResult<PrimeIterator> find_res = es->db_slice().Find(t->db_index(), keys[i], OBJ_SET);
    if (!find_res) {
      if (status == OpStatus::OK || status == OpStatus::KEY_NOTFOUND ||
        find_res.status() != OpStatus::KEY_NOTFOUND) {
        status = find_res.status();
      }
      continue;
    }
    const PrimeValue& pv = find_res.value()->second;
    void* ptr = pv.RObjPtr();
    sets[i] = make_pair(ptr, pv.Encoding());
  }

  if (status != OpStatus::OK)
    return status;

  auto comp = [](const SetType& left, const SetType& right) {
    return SetTypeLen(left) < SetTypeLen(right);
  };

  std::sort(sets.begin(), sets.end(), comp);

  int encoding = sets.front().second;
  if (encoding == kEncodingIntSet) {
    int ii = 0;
    intset* is = (intset*)sets.front().first;
    int64_t intele;

    while (intsetGet(is, ii++, &intele)) {
      size_t j = 1;
      for (j = 1; j < sets.size(); j++) {
        if (sets[j].first != is && !IsInSet(sets[j], intele))
          break;
      }

      /* Only take action when all sets contain the member */
      if (j == sets.size()) {
        result.push_back(absl::StrCat(intele));
      }
    }
  } else {
    dict* ds = (dict*)sets.front().first;
    dictIterator* di = dictGetIterator(ds);
    dictEntry* de = nullptr;
    while ((de = dictNext(di))) {
      size_t j = 1;
      sds key = (sds)de->key;
      string_view member{key, sdslen(key)};

      for (j = 1; j < sets.size(); j++) {
        if (sets[j].first != ds && !IsInSet(sets[j], member))
          break;
      }

      /* Only take action when all sets contain the member */
      if (j == sets.size()) {
        result.push_back(string(member));
      }
    }
    dictReleaseIterator(di);
  }

  return result;
}

OpResult<StringVec> SetFamily::OpScan(const OpArgs& op_args, std::string_view key,
                                      uint64_t* cursor) {
  OpResult<PrimeIterator> find_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_SET);

  if (!find_res)
    return find_res.status();

  PrimeIterator it = find_res.value();
  StringVec res;
  uint32_t count = 10;

  if (it->second.Encoding() == kEncodingIntSet) {
    intset* is = (intset*)it->second.RObjPtr();
    int64_t intele;
    uint32_t pos = 0;
    while (intsetGet(is, pos++, &intele)) {
      res.push_back(absl::StrCat(intele));
    }
    *cursor = 0;
  } else {
    DCHECK_EQ(kEncodingStrMap, it->second.Encoding());
    long maxiterations = count * 10;

    dict* ds = (dict*)it->second.RObjPtr();
    uint64_t cur = *cursor;
    do {
      cur = dictScan(ds, cur, ScanCallback, NULL, &res);
    } while (cur && maxiterations-- && res.size() < count);
    *cursor = cur;
  }

  return res;
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
            << CI{"SUNIONSTORE", CO::WRITE | CO::DENYOOM, -3, 1, -1, 1}.HFUNC(SUnionStore)
            << CI{"SSCAN", CO::READONLY | CO::RANDOM, -3, 1, 1, 1}.HFUNC(SScan);
}

uint32_t SetFamily::MaxIntsetEntries() {
  return kMaxIntSetEntries;
}

void SetFamily::ConvertTo(intset* src, dict* dest) {
  int64_t intele;
  char buf[32];

  /* To add the elements we extract integers and create redis objects */
  int ii = 0;
  while (intsetGet(src, ii++, &intele)) {
    char* next = absl::numbers_internal::FastIntToBuffer(intele, buf);
    sds s = sdsnewlen(buf, next - buf);
    CHECK(dictAddRaw(dest, s, NULL));
  }
}

}  // namespace dfly
