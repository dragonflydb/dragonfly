// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/set_family.h"

extern "C" {
#include "redis/intset.h"
#include "redis/object.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "core/string_set.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/transaction.h"

ABSL_DECLARE_FLAG(bool, use_set2);

namespace dfly {

using namespace facade;

using namespace std;
using absl::GetFlag;

using ResultStringVec = vector<OpResult<StringVec>>;
using ResultSetView = OpResult<absl::flat_hash_set<std::string_view>>;
using SvArray = vector<std::string_view>;
using SetType = pair<void*, unsigned>;

namespace {

constexpr uint32_t kMaxIntSetEntries = 256;

bool IsDenseEncoding(const CompactObj& co) {
  return co.Encoding() == kEncodingStrMap2;
}

bool IsDenseEncoding(const SetType& co) {
  return co.second == kEncodingStrMap2;
}

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

bool dictContains(const dict* d, string_view key) {
  uint64_t h = dictGenHashFunction(key.data(), key.size());

  for (unsigned table = 0; table <= 1; table++) {
    uint64_t idx = h & DICTHT_SIZE_MASK(d->ht_size_exp[table]);
    dictEntry* he = d->ht_table[table][idx];
    while (he) {
      sds dkey = (sds)he->key;
      if (sdslen(dkey) == key.size() &&
          (key.empty() || memcmp(dkey, key.data(), key.size()) == 0)) {
        return true;
      }
      he = he->next;
    }
    if (!dictIsRehashing(d)) {
      break;
    }
  }
  return false;
}

pair<unsigned, bool> RemoveStrSet(uint32_t now_sec, ArgSlice vals, CompactObj* set) {
  unsigned removed = 0;
  bool isempty = false;
  auto* shard = EngineShard::tlocal();

  if (IsDenseEncoding(*set)) {
    StringSet* ss = ((StringSet*)set->RObjPtr());
    ss->set_time(now_sec);

    for (auto member : vals) {
      removed += ss->Erase(member);
    }

    isempty = ss->Empty();
  } else {
    DCHECK_EQ(set->Encoding(), kEncodingStrMap);
    dict* d = (dict*)set->RObjPtr();
    for (auto member : vals) {
      shard->tmp_str1 = sdscpylen(shard->tmp_str1, member.data(), member.size());
      int result = dictDelete(d, shard->tmp_str1);
      removed += (result == DICT_OK);
    }

    isempty = dictSize(d) == 0;
  }

  return make_pair(removed, isempty);
}

unsigned AddStrSet(const DbContext& db_context, ArgSlice vals, uint32_t ttl_sec, CompactObj* dest) {
  unsigned res = 0;

  if (IsDenseEncoding(*dest)) {
    StringSet* ss = (StringSet*)dest->RObjPtr();
    uint32_t time_now = MemberTimeSeconds(db_context.time_now_ms);

    ss->set_time(time_now);

    for (auto member : vals) {
      res += ss->Add(member, ttl_sec);
    }
  } else {
    DCHECK_EQ(dest->Encoding(), kEncodingStrMap);
    dict* ds = (dict*)dest->RObjPtr();
    auto* es = EngineShard::tlocal();

    for (auto member : vals) {
      es->tmp_str1 = sdscpylen(es->tmp_str1, member.data(), member.size());
      dictEntry* de = dictAddRaw(ds, es->tmp_str1, NULL);
      if (de) {
        de->key = sdsdup(es->tmp_str1);
        ++res;
      }
    }
  }

  return res;
}

void InitStrSet(CompactObj* set) {
  if (GetFlag(FLAGS_use_set2)) {
    StringSet* ss = new StringSet{CompactObj::memory_resource()};
    set->InitRobj(OBJ_SET, kEncodingStrMap2, ss);
  } else {
    dict* ds = dictCreate(&setDictType);
    set->InitRobj(OBJ_SET, kEncodingStrMap, ds);
  }
}

// returns (removed, isempty)
pair<unsigned, bool> RemoveSet(const DbContext& db_context, ArgSlice vals, CompactObj* set) {
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
    return RemoveStrSet(MemberTimeSeconds(db_context.time_now_ms), vals, set);
  }
  return make_pair(removed, isempty);
}

void InitSet(ArgSlice vals, CompactObj* set) {
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
    set->InitRobj(OBJ_SET, kEncodingIntSet, is);
  } else {
    InitStrSet(set);
  }
}

uint64_t ScanStrSet(const DbContext& db_context, const CompactObj& co, uint64_t curs,
                    const ScanOpts& scan_op, StringVec* res) {
  uint32_t count = scan_op.limit;
  long maxiterations = count * 10;

  if (IsDenseEncoding(co)) {
    StringSet* set = (StringSet*)co.RObjPtr();
    set->set_time(MemberTimeSeconds(db_context.time_now_ms));

    do {
      auto scan_callback = [&](const sds ptr) {
        string_view str{ptr, sdslen(ptr)};
        if (scan_op.Matches(str)) {
          res->push_back(std::string(str));
        }
      };

      curs = set->Scan(curs, scan_callback);

    } while (curs && maxiterations-- && res->size() < count);
  } else {
    DCHECK_EQ(co.Encoding(), kEncodingStrMap);
    using PrivateDataRef = std::tuple<StringVec*, const ScanOpts&>;
    PrivateDataRef private_data_ref(res, scan_op);
    void* private_data = &private_data_ref;
    dict* ds = (dict*)co.RObjPtr();

    auto scan_callback = [](void* private_data, const dictEntry* de) {
      StringVec* sv = std::get<0>(*(PrivateDataRef*)private_data);
      const ScanOpts& scan_op = std::get<1>(*(PrivateDataRef*)private_data);

      sds key = (sds)de->key;
      auto len = sdslen(key);
      if (scan_op.Matches(std::string_view(key, len))) {
        sv->emplace_back(key, len);
      }
    };

    do {
      curs = dictScan(ds, curs, scan_callback, NULL, private_data);
    } while (curs && maxiterations-- && res->size() < count);
  }

  return curs;
}

uint32_t SetTypeLen(const DbContext& db_context, const SetType& set) {
  if (set.second == kEncodingIntSet) {
    return intsetLen((const intset*)set.first);
  }

  if (IsDenseEncoding(set)) {
    StringSet* ss = (StringSet*)set.first;
    ss->set_time(MemberTimeSeconds(db_context.time_now_ms));
    return ss->Size();
  }

  DCHECK_EQ(set.second, kEncodingStrMap);
  return dictSize((const dict*)set.first);
}

bool IsInSet(const DbContext& db_context, const SetType& st, int64_t val) {
  if (st.second == kEncodingIntSet)
    return intsetFind((intset*)st.first, val);

  char buf[32];
  char* next = absl::numbers_internal::FastIntToBuffer(val, buf);
  string_view str{buf, size_t(next - buf)};

  if (IsDenseEncoding(st)) {
    StringSet* ss = (StringSet*)st.first;
    ss->set_time(MemberTimeSeconds(db_context.time_now_ms));
    return ss->Contains(str);
  }

  DCHECK_EQ(st.second, kEncodingStrMap);
  return dictContains((dict*)st.first, str);
}

bool IsInSet(const DbContext& db_context, const SetType& st, string_view member) {
  if (st.second == kEncodingIntSet) {
    long long llval;
    if (!string2ll(member.data(), member.size(), &llval))
      return false;

    return intsetFind((intset*)st.first, llval);
  }

  if (IsDenseEncoding(st)) {
    StringSet* ss = (StringSet*)st.first;
    ss->set_time(MemberTimeSeconds(db_context.time_now_ms));

    return ss->Contains(member);
  } else {
    DCHECK_EQ(st.second, kEncodingStrMap);
    return dictContains((dict*)st.first, member);
  }
}

void FindInSet(StringVec& memberships, const DbContext& db_context, const SetType& st,
               const vector<string_view>& members) {
  for (const auto& member : members) {
    bool status = IsInSet(db_context, st, member);
    memberships.emplace_back(to_string(status));
  }
}

// Removes arg from result.
void DiffStrSet(const DbContext& db_context, const SetType& st,
                absl::flat_hash_set<string>* result) {
  if (IsDenseEncoding(st)) {
    StringSet* ss = (StringSet*)st.first;
    ss->set_time(MemberTimeSeconds(db_context.time_now_ms));
    for (sds ptr : *ss) {
      result->erase(string_view{ptr, sdslen(ptr)});
    }
  } else {
    DCHECK_EQ(st.second, kEncodingStrMap);
    dict* ds = (dict*)st.first;
    dictIterator* di = dictGetIterator(ds);
    dictEntry* de = nullptr;
    while ((de = dictNext(di))) {
      sds key = (sds)de->key;
      result->erase(string_view{key, sdslen(key)});
    }
    dictReleaseIterator(di);
  }
}

void InterStrSet(const DbContext& db_context, const vector<SetType>& vec, StringVec* result) {
  if (IsDenseEncoding(vec.front())) {
    StringSet* ss = (StringSet*)vec.front().first;
    ss->set_time(MemberTimeSeconds(db_context.time_now_ms));
    for (const sds ptr : *ss) {
      std::string_view str{ptr, sdslen(ptr)};
      size_t j = 1;
      for (j = 1; j < vec.size(); ++j) {
        if (vec[j].first != ss && !IsInSet(db_context, vec[j], str)) {
          break;
        }
      }

      if (j == vec.size()) {
        result->push_back(std::string(str));
      }
    }
  } else {
    DCHECK_EQ(vec.front().second, kEncodingStrMap);
    dict* ds = (dict*)vec.front().first;
    dictIterator* di = dictGetIterator(ds);
    dictEntry* de = nullptr;
    while ((de = dictNext(di))) {
      size_t j = 1;
      sds key = (sds)de->key;
      string_view member{key, sdslen(key)};

      for (j = 1; j < vec.size(); j++) {
        if (vec[j].first != ds && !IsInSet(db_context, vec[j], member))
          break;
      }

      /* Only take action when all vec contain the member */
      if (j == vec.size()) {
        result->push_back(string(member));
      }
    }
    dictReleaseIterator(di);
  }
}

StringVec PopStrSet(const DbContext& db_context, unsigned count, const SetType& st) {
  StringVec result;

  if (IsDenseEncoding(st)) {
    StringSet* ss = (StringSet*)st.first;
    ss->set_time(MemberTimeSeconds(db_context.time_now_ms));

    // TODO: this loop is inefficient because Pop searches again and again an occupied bucket.
    for (unsigned i = 0; i < count && !ss->Empty(); ++i) {
      result.push_back(ss->Pop().value());
    }
  } else {
    DCHECK_EQ(st.second, kEncodingStrMap);
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

  return result;
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

// if overwrite is true then OpAdd writes vals into the key and discards its previous value.
OpResult<uint32_t> OpAdd(const OpArgs& op_args, std::string_view key, ArgSlice vals, bool overwrite,
                         bool journal_update) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();

  // overwrite - meaning we run in the context of 2-hop operation and we want
  // to overwrite the key. However, if the set is empty it means we should delete the
  // key if it exists.
  if (overwrite && vals.empty()) {
    auto it = db_slice.FindExt(op_args.db_cntx, key).first;
    db_slice.Del(op_args.db_cntx.db_index, it);
    if (journal_update && op_args.shard->journal()) {
      RecordJournal(op_args, "DEL"sv, ArgSlice{key});
    }
    return 0;
  }

  PrimeIterator it;
  bool new_key = false;

  try {
    tie(it, new_key) = db_slice.AddOrFind(op_args.db_cntx, key);
  } catch (bad_alloc& e) {
    return OpStatus::OUT_OF_MEMORY;
  }

  CompactObj& co = it->second;

  if (!new_key) {
    // for non-overwrite case it must be set.
    if (!overwrite && co.ObjType() != OBJ_SET)
      return OpStatus::WRONG_TYPE;

    // Update stats and trigger any handle the old value if needed.
    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  }

  if (new_key || overwrite) {
    // does not store the values, merely sets the encoding.
    // TODO: why not store the values as well?
    InitSet(vals, &co);
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
        co.SetRObjPtr(is);

        robj tmp;
        if (!SetFamily::ConvertToStrSet(is, intsetLen(is), &tmp)) {
          return OpStatus::OUT_OF_MEMORY;
        }

        // frees 'is' on a way.
        if (GetFlag(FLAGS_use_set2)) {
          co.InitRobj(OBJ_SET, kEncodingStrMap2, tmp.ptr);
        } else {
          co.InitRobj(OBJ_SET, kEncodingStrMap, tmp.ptr);
        }

        inner_obj = co.RObjPtr();
        break;
      }
    }

    if (success)
      co.SetRObjPtr(is);
  }

  if (co.Encoding() != kEncodingIntSet) {
    res = AddStrSet(op_args.db_cntx, std::move(vals), UINT32_MAX, &co);
  }

  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, !new_key);
  if (journal_update && op_args.shard->journal()) {
    if (overwrite) {
      RecordJournal(op_args, "DEL"sv, ArgSlice{key});
    }
    vector<string_view> mapped(vals.size() + 1);
    mapped[0] = key;
    std::copy(vals.begin(), vals.end(), mapped.begin() + 1);
    RecordJournal(op_args, "SADD"sv, mapped);
  }
  return res;
}

OpResult<uint32_t> OpAddEx(const OpArgs& op_args, string_view key, uint32_t ttl_sec,
                           ArgSlice vals) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();

  PrimeIterator it;
  bool new_key = false;

  try {
    tie(it, new_key) = db_slice.AddOrFind(op_args.db_cntx, key);
  } catch (bad_alloc& e) {
    return OpStatus::OUT_OF_MEMORY;
  }

  CompactObj& co = it->second;

  if (new_key) {
    CHECK(absl::GetFlag(FLAGS_use_set2));
    InitStrSet(&co);
  } else {
    // for non-overwrite case it must be set.
    if (co.ObjType() != OBJ_SET)
      return OpStatus::WRONG_TYPE;

    // Update stats and trigger any handle the old value if needed.
    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
    if (co.Encoding() == kEncodingIntSet) {
      intset* is = (intset*)co.RObjPtr();
      robj tmp;
      if (!SetFamily::ConvertToStrSet(is, intsetLen(is), &tmp)) {
        return OpStatus::OUT_OF_MEMORY;
      }
      co.InitRobj(OBJ_SET, kEncodingStrMap2, tmp.ptr);
    }

    CHECK(IsDenseEncoding(co));
  }

  uint32_t res = AddStrSet(op_args.db_cntx, std::move(vals), ttl_sec, &co);

  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, !new_key);

  return res;
}

OpResult<uint32_t> OpRem(const OpArgs& op_args, string_view key, const ArgSlice& vals,
                         bool journal_rewrite) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();
  OpResult<PrimeIterator> find_res = db_slice.Find(op_args.db_cntx, key, OBJ_SET);
  if (!find_res) {
    return find_res.status();
  }

  db_slice.PreUpdate(op_args.db_cntx.db_index, *find_res);

  CompactObj& co = find_res.value()->second;
  auto [removed, isempty] = RemoveSet(op_args.db_cntx, vals, &co);

  db_slice.PostUpdate(op_args.db_cntx.db_index, *find_res, key);

  if (isempty) {
    CHECK(db_slice.Del(op_args.db_cntx.db_index, find_res.value()));
  }
  if (journal_rewrite && op_args.shard->journal()) {
    vector<string_view> mapped(vals.size() + 1);
    mapped[0] = key;
    std::copy(vals.begin(), vals.end(), mapped.begin() + 1);
    RecordJournal(op_args, "SREM"sv, mapped);
  }

  return removed;
}

// For SMOVE. Comprised of 2 transactional steps: Find and Commit.
// After Find Mover decides on the outcome of the operation, applies it in commit
// and reports the result.
class Mover {
 public:
  Mover(string_view src, string_view dest, string_view member, bool journal_rewrite)
      : src_(src), dest_(dest), member_(member), journal_rewrite_(journal_rewrite) {
  }

  void Find(Transaction* t);
  OpResult<unsigned> Commit(Transaction* t);

 private:
  OpStatus OpFind(Transaction* t, EngineShard* es);
  OpStatus OpMutate(Transaction* t, EngineShard* es);

  string_view src_, dest_, member_;
  OpResult<bool> found_[2];
  bool journal_rewrite_;
};

OpStatus Mover::OpFind(Transaction* t, EngineShard* es) {
  ArgSlice largs = t->GetShardArgs(es->shard_id());

  // In case both src and dest are in the same shard, largs size will be 2.
  DCHECK_LE(largs.size(), 2u);

  for (auto k : largs) {
    unsigned index = (k == src_) ? 0 : 1;
    OpResult<PrimeIterator> res = es->db_slice().Find(t->GetDbContext(), k, OBJ_SET);
    if (res && index == 0) {  // successful src find.
      DCHECK(!res->is_done());
      const CompactObj& val = res.value()->second;
      SetType st{val.RObjPtr(), val.Encoding()};
      found_[0] = IsInSet(t->GetDbContext(), st, member_);
    } else {
      found_[index] = res.status();
    }
  }

  return OpStatus::OK;
}

OpStatus Mover::OpMutate(Transaction* t, EngineShard* es) {
  ArgSlice largs = t->GetShardArgs(es->shard_id());
  DCHECK_LE(largs.size(), 2u);

  OpArgs op_args = t->GetOpArgs(es);
  for (auto k : largs) {
    if (k == src_) {
      CHECK_EQ(1u, OpRem(op_args, k, {member_}, journal_rewrite_).value());  // must succeed.
    } else {
      DCHECK_EQ(k, dest_);
      OpAdd(op_args, k, {member_}, false, journal_rewrite_);
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

// Read-only OpUnion op on sets.
OpResult<StringVec> OpUnion(const OpArgs& op_args, ArgSlice keys) {
  DCHECK(!keys.empty());
  absl::flat_hash_set<string> uniques;

  for (string_view key : keys) {
    OpResult<PrimeIterator> find_res =
        op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_SET);
    if (find_res) {
      PrimeValue& pv = find_res.value()->second;
      if (IsDenseEncoding(pv)) {
        StringSet* ss = (StringSet*)pv.RObjPtr();
        ss->set_time(MemberTimeSeconds(op_args.db_cntx.time_now_ms));
      }
      container_utils::IterateSet(pv, [&uniques](container_utils::ContainerEntry ce) {
        uniques.emplace(ce.ToString());
        return true;
      });
      continue;
    }

    if (find_res.status() != OpStatus::KEY_NOTFOUND) {
      return find_res.status();
    }
  }

  return ToVec(std::move(uniques));
}

// Read-only OpDiff op on sets.
OpResult<StringVec> OpDiff(const OpArgs& op_args, ArgSlice keys) {
  DCHECK(!keys.empty());
  DVLOG(1) << "OpDiff from " << keys.front();
  EngineShard* es = op_args.shard;
  OpResult<PrimeIterator> find_res = es->db_slice().Find(op_args.db_cntx, keys.front(), OBJ_SET);

  if (!find_res) {
    return find_res.status();
  }

  absl::flat_hash_set<string> uniques;
  PrimeValue& pv = find_res.value()->second;
  if (IsDenseEncoding(pv)) {
    StringSet* ss = (StringSet*)pv.RObjPtr();
    ss->set_time(MemberTimeSeconds(op_args.db_cntx.time_now_ms));
  }

  container_utils::IterateSet(pv, [&uniques](container_utils::ContainerEntry ce) {
    uniques.emplace(ce.ToString());
    return true;
  });

  DCHECK(!uniques.empty());  // otherwise the key would not exist.

  for (size_t i = 1; i < keys.size(); ++i) {
    OpResult<PrimeIterator> diff_res = es->db_slice().Find(op_args.db_cntx, keys[i], OBJ_SET);
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
      DiffStrSet(op_args.db_cntx, st2, &uniques);
    }
  }

  return ToVec(std::move(uniques));
}

// Read-only OpInter op on sets.
OpResult<StringVec> OpInter(const Transaction* t, EngineShard* es, bool remove_first) {
  ArgSlice keys = t->GetShardArgs(es->shard_id());
  if (remove_first) {
    keys.remove_prefix(1);
  }
  DCHECK(!keys.empty());

  StringVec result;
  if (keys.size() == 1) {
    OpResult<PrimeIterator> find_res =
        es->db_slice().Find(t->GetDbContext(), keys.front(), OBJ_SET);
    if (!find_res)
      return find_res.status();

    PrimeValue& pv = find_res.value()->second;
    if (IsDenseEncoding(pv)) {
      StringSet* ss = (StringSet*)pv.RObjPtr();
      ss->set_time(MemberTimeSeconds(t->GetDbContext().time_now_ms));
    }

    container_utils::IterateSet(find_res.value()->second,
                                [&result](container_utils::ContainerEntry ce) {
                                  result.push_back(ce.ToString());
                                  return true;
                                });
    return result;
  }

  // we must copy by value because AsRObj is temporary.
  vector<SetType> sets(keys.size());

  OpStatus status = OpStatus::OK;

  for (size_t i = 0; i < keys.size(); ++i) {
    OpResult<PrimeIterator> find_res = es->db_slice().Find(t->GetDbContext(), keys[i], OBJ_SET);
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

  auto comp = [db_contx = t->GetDbContext()](const SetType& left, const SetType& right) {
    return SetTypeLen(db_contx, left) < SetTypeLen(db_contx, right);
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
        if (sets[j].first != is && !IsInSet(t->GetDbContext(), sets[j], intele))
          break;
      }

      /* Only take action when all sets contain the member */
      if (j == sets.size()) {
        result.push_back(absl::StrCat(intele));
      }
    }
  } else {
    InterStrSet(t->GetDbContext(), sets, &result);
  }

  return result;
}

// count - how many elements to pop.
OpResult<StringVec> OpPop(const OpArgs& op_args, string_view key, unsigned count) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> find_res = db_slice.Find(op_args.db_cntx, key, OBJ_SET);
  if (!find_res)
    return find_res.status();

  StringVec result;
  if (count == 0)
    return result;

  PrimeIterator it = find_res.value();
  size_t slen = it->second.Size();

  /* CASE 1:
   * The number of requested elements is greater than or equal to
   * the number of elements inside the set: simply return the whole set. */
  if (count >= slen) {
    PrimeValue& pv = it->second;
    if (IsDenseEncoding(pv)) {
      StringSet* ss = (StringSet*)pv.RObjPtr();
      ss->set_time(MemberTimeSeconds(op_args.db_cntx.time_now_ms));
    }

    container_utils::IterateSet(it->second, [&result](container_utils::ContainerEntry ce) {
      result.push_back(ce.ToString());
      return true;
    });

    // Delete the set as it is now empty
    CHECK(db_slice.Del(op_args.db_cntx.db_index, it));

    // Replicate as DEL.
    if (op_args.shard->journal()) {
      RecordJournal(op_args, "DEL"sv, ArgSlice{key});
    }
  } else {
    SetType st{it->second.RObjPtr(), it->second.Encoding()};
    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
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
      result = PopStrSet(op_args.db_cntx, count, st);
    }

    // Replicate as SREM with removed keys, because SPOP is not deterministic.
    if (op_args.shard->journal()) {
      vector<string_view> mapped(result.size() + 1);
      mapped[0] = key;
      std::copy(result.begin(), result.end(), mapped.begin() + 1);
      RecordJournal(op_args, "SREM"sv, mapped);
    }

    db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);
  }
  return result;
}

OpResult<StringVec> OpScan(const OpArgs& op_args, string_view key, uint64_t* cursor,
                           const ScanOpts& scan_op) {
  OpResult<PrimeIterator> find_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_SET);

  if (!find_res)
    return find_res.status();

  PrimeIterator it = find_res.value();
  StringVec res;

  if (it->second.Encoding() == kEncodingIntSet) {
    intset* is = (intset*)it->second.RObjPtr();
    int64_t intele;
    uint32_t pos = 0;
    while (intsetGet(is, pos++, &intele)) {
      std::string int_str = absl::StrCat(intele);
      if (scan_op.Matches(int_str)) {
        res.push_back(int_str);
      }
    }
    *cursor = 0;
  } else {
    *cursor = ScanStrSet(op_args.db_cntx, it->second, *cursor, scan_op, &res);
  }

  return res;
}

void SAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  vector<string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }
  ArgSlice arg_slice{vals.data(), vals.size()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, arg_slice, false, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return (*cntx)->SendLong(result.value());
  }

  (*cntx)->SendError(result.status());
}

void SIsMember(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view val = ArgS(args, 2);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpResult<PrimeIterator> find_res = shard->db_slice().Find(t->GetDbContext(), key, OBJ_SET);

    if (find_res) {
      SetType st{find_res.value()->second.RObjPtr(), find_res.value()->second.Encoding()};
      return IsInSet(t->GetDbContext(), st, val) ? OpStatus::OK : OpStatus::KEY_NOTFOUND;
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

void SMIsMember(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  vector<string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }

  StringVec memberships;
  memberships.reserve(vals.size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpResult<PrimeIterator> find_res = shard->db_slice().Find(t->GetDbContext(), key, OBJ_SET);
    if (find_res) {
      SetType st{find_res.value()->second.RObjPtr(), find_res.value()->second.Encoding()};
      FindInSet(memberships, t->GetDbContext(), st, vals);
      return OpStatus::OK;
    }
    return find_res.status();
  };

  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  if (result == OpStatus::KEY_NOTFOUND) {
    memberships.assign(vals.size(), "0");
    return (*cntx)->SendStringArr(memberships);
  } else if (result == OpStatus::OK) {
    return (*cntx)->SendStringArr(memberships);
  }
  (*cntx)->SendError(result.status());
}

void SMove(CmdArgList args, ConnectionContext* cntx) {
  string_view src = ArgS(args, 1);
  string_view dest = ArgS(args, 2);
  string_view member = ArgS(args, 3);

  Mover mover{src, dest, member, true};
  cntx->transaction->Schedule();

  mover.Find(cntx->transaction);

  OpResult<unsigned> result = mover.Commit(cntx->transaction);
  if (!result) {
    return (*cntx)->SendError(result.status());
    return;
  }

  (*cntx)->SendLong(result.value());
}

void SRem(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  vector<string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }
  ArgSlice span{vals.data(), vals.size()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(t->GetOpArgs(shard), key, span, false);
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

void SCard(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<uint32_t> {
    OpResult<PrimeIterator> find_res = shard->db_slice().Find(t->GetDbContext(), key, OBJ_SET);
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

void SPop(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  unsigned count = 1;
  if (args.size() > 2) {
    string_view arg = ArgS(args, 2);
    if (!absl::SimpleAtoi(arg, &count)) {
      (*cntx)->SendError(kInvalidIntErr);
      return;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPop(t->GetOpArgs(shard), key, count);
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
      (*cntx)->SendStringArr(*result, RedisReplyBuilder::SET);
    }
    return;
  }

  (*cntx)->SendError(result.status());
}

void SDiff(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(shard_set->size(), OpStatus::SKIPPED);
  string_view src_key = ArgS(args, 1);
  ShardId src_shard = Shard(src_key, result_set.size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
    if (shard->shard_id() == src_shard) {
      CHECK_EQ(src_key, largs.front());
      result_set[shard->shard_id()] = OpDiff(t->GetOpArgs(shard), largs);
    } else {
      result_set[shard->shard_id()] = OpUnion(t->GetOpArgs(shard), largs);
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
  (*cntx)->SendStringArr(arr, RedisReplyBuilder::SET);
}

void SDiffStore(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(shard_set->size(), OpStatus::SKIPPED);
  string_view dest_key = ArgS(args, 1);
  ShardId dest_shard = Shard(dest_key, result_set.size());
  string_view src_key = ArgS(args, 2);
  ShardId src_shard = Shard(src_key, result_set.size());

  VLOG(1) << "SDiffStore " << src_key << " " << src_shard;

  // read-only op
  auto diff_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
    DCHECK(!largs.empty());

    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      largs.remove_prefix(1);
      if (largs.empty())
        return OpStatus::OK;
    }

    OpArgs op_args = t->GetOpArgs(shard);
    if (shard->shard_id() == src_shard) {
      CHECK_EQ(src_key, largs.front());
      result_set[shard->shard_id()] = OpDiff(op_args, largs);  // Diff
    } else {
      result_set[shard->shard_id()] = OpUnion(op_args, largs);  // Union
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
      OpAdd(t->GetOpArgs(shard), dest_key, result, true, true);
    }

    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);
  (*cntx)->SendLong(result.size());
}

void SMembers(CmdArgList args, ConnectionContext* cntx) {
  auto cb = [](Transaction* t, EngineShard* shard) { return OpInter(t, shard, false); };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    StringVec& svec = result.value();

    if (cntx->conn_state.script_info) {  // sort under script
      sort(svec.begin(), svec.end());
    }
    (*cntx)->SendStringArr(*result, RedisReplyBuilder::SET);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void SInter(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(shard_set->size(), OpStatus::SKIPPED);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    result_set[shard->shard_id()] = OpInter(t, shard, false);

    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));
  OpResult<SvArray> result = InterResultVec(result_set, cntx->transaction->GetUniqueShardCnt());
  if (result) {
    SvArray arr = std::move(*result);
    if (cntx->conn_state.script_info) {  // sort under script
      sort(arr.begin(), arr.end());
    }
    (*cntx)->SendStringArr(arr, RedisReplyBuilder::SET);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void SInterStore(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(shard_set->size(), OpStatus::SKIPPED);
  string_view dest_key = ArgS(args, 1);
  ShardId dest_shard = Shard(dest_key, result_set.size());
  atomic_uint32_t inter_shard_cnt{0};

  auto inter_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
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
      OpAdd(t->GetOpArgs(shard), dest_key, result.value(), true, true);
    }

    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);
  (*cntx)->SendLong(result->size());
}

void SUnion(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(shard_set->size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
    result_set[shard->shard_id()] = OpUnion(t->GetOpArgs(shard), largs);
    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));

  ResultSetView unionset = UnionResultVec(result_set);
  if (unionset) {
    SvArray arr = ToSvArray(*unionset);
    if (cntx->conn_state.script_info) {  // sort under script
      sort(arr.begin(), arr.end());
    }
    (*cntx)->SendStringArr(arr, RedisReplyBuilder::SET);
  } else {
    (*cntx)->SendError(unionset.status());
  }
}

void SUnionStore(CmdArgList args, ConnectionContext* cntx) {
  ResultStringVec result_set(shard_set->size(), OpStatus::SKIPPED);
  string_view dest_key = ArgS(args, 1);
  ShardId dest_shard = Shard(dest_key, result_set.size());

  auto union_cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      largs.remove_prefix(1);
      if (largs.empty())
        return OpStatus::OK;
    }
    result_set[shard->shard_id()] = OpUnion(t->GetOpArgs(shard), largs);
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
      OpAdd(t->GetOpArgs(shard), dest_key, result, true, true);
    }

    return OpStatus::OK;
  };

  cntx->transaction->Execute(std::move(store_cb), true);
  (*cntx)->SendLong(result.size());
}

void SScan(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view token = ArgS(args, 2);

  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  // SSCAN key cursor [MATCH pattern] [COUNT count]
  if (args.size() > 7) {
    DVLOG(1) << "got " << args.size() << " this is more than it should be";
    return (*cntx)->SendError(kSyntaxErr);
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(3));
  if (!ops) {
    DVLOG(1) << "SScan invalid args - return " << ops << " to the user";
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
    (*cntx)->StartArray(result->size());  // Within scan the return page is of type array
    for (const auto& k : *result) {
      (*cntx)->SendBulkString(k);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

// Syntax: saddex key ttl_sec member [member...]
void SAddEx(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view ttl_str = ArgS(args, 2);
  uint32_t ttl_sec;
  constexpr uint32_t kMaxTtl = (1UL << 26);

  if (!absl::SimpleAtoi(ttl_str, &ttl_sec) || ttl_sec == 0 || ttl_sec > kMaxTtl) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  vector<string_view> vals(args.size() - 3);
  for (size_t i = 3; i < args.size(); ++i) {
    vals[i - 3] = ArgS(args, i);
  }

  ArgSlice arg_slice{vals.data(), vals.size()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAddEx(t->GetOpArgs(shard), key, ttl_sec, arg_slice);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return (*cntx)->SendLong(result.value());
  }

  (*cntx)->SendError(result.status());
}

}  // namespace

bool SetFamily::ConvertToStrSet(const intset* is, size_t expected_len, robj* dest) {
  int64_t intele;
  char buf[32];
  int ii = 0;

  if (GetFlag(FLAGS_use_set2)) {
    StringSet* ss = new StringSet{CompactObj::memory_resource()};
    if (expected_len) {
      ss->Reserve(expected_len);
    }

    while (intsetGet(const_cast<intset*>(is), ii++, &intele)) {
      char* next = absl::numbers_internal::FastIntToBuffer(intele, buf);
      string_view str{buf, size_t(next - buf)};
      CHECK(ss->Add(str));
    }

    dest->ptr = ss;
    dest->encoding = kEncodingStrMap2;
  } else {
    dict* ds = dictCreate(&setDictType);

    if (expected_len) {
      if (dictTryExpand(ds, expected_len) != DICT_OK) {
        dictRelease(ds);
        return false;
      }
    }

    /* To add the elements we extract integers and create redis objects */
    while (intsetGet(const_cast<intset*>(is), ii++, &intele)) {
      char* next = absl::numbers_internal::FastIntToBuffer(intele, buf);
      sds s = sdsnewlen(buf, next - buf);
      CHECK(dictAddRaw(ds, s, NULL));
    }

    dest->ptr = ds;
    dest->encoding = OBJ_ENCODING_HT;
  }
  return true;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&x)

void SetFamily::Register(CommandRegistry* registry) {
  *registry << CI{"SADD", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(SAdd)
            << CI{"SDIFF", CO::READONLY, -2, 1, -1, 1}.HFUNC(SDiff)
            << CI{"SDIFFSTORE", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, -3, 1, -1, 1}.HFUNC(
                   SDiffStore)
            << CI{"SINTER", CO::READONLY, -2, 1, -1, 1}.HFUNC(SInter)
            << CI{"SINTERSTORE", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, -3, 1, -1, 1}.HFUNC(
                   SInterStore)
            << CI{"SMEMBERS", CO::READONLY, 2, 1, 1, 1}.HFUNC(SMembers)
            << CI{"SISMEMBER", CO::FAST | CO::READONLY, 3, 1, 1, 1}.HFUNC(SIsMember)
            << CI{"SMISMEMBER", CO::READONLY, -3, 1, 1, 1}.HFUNC(SMIsMember)
            << CI{"SMOVE", CO::FAST | CO::WRITE | CO::NO_AUTOJOURNAL, 4, 1, 2, 1}.HFUNC(SMove)
            << CI{"SREM", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(SRem)
            << CI{"SCARD", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(SCard)
            << CI{"SPOP", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, -2, 1, 1, 1}.HFUNC(SPop)
            << CI{"SUNION", CO::READONLY, -2, 1, -1, 1}.HFUNC(SUnion)
            << CI{"SUNIONSTORE", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, -3, 1, -1, 1}.HFUNC(
                   SUnionStore)
            << CI{"SSCAN", CO::READONLY, -3, 1, 1, 1}.HFUNC(SScan);

  if (absl::GetFlag(FLAGS_use_set2)) {
    *registry << CI{"SADDEX", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(SAddEx);
  }
}

uint32_t SetFamily::MaxIntsetEntries() {
  return kMaxIntSetEntries;
}

void SetFamily::ConvertTo(const intset* src, dict* dest) {
  int64_t intele;
  char buf[32];

  /* To add the elements we extract integers and create redis objects */
  int ii = 0;
  while (intsetGet(const_cast<intset*>(src), ii++, &intele)) {
    char* next = absl::numbers_internal::FastIntToBuffer(intele, buf);
    sds s = sdsnewlen(buf, next - buf);
    CHECK(dictAddRaw(dest, s, NULL));
  }
}

}  // namespace dfly
