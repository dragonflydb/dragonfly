// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/db_slice.h"

extern "C" {
#include "redis/object.h"
}

#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using facade::OpStatus;

namespace {

constexpr auto kPrimeSegmentSize = PrimeTable::kSegBytes;
constexpr auto kExpireSegmentSize = ExpireTable::kSegBytes;

// mi_malloc good size is 32768 just 48 bytes more.
static_assert(kPrimeSegmentSize == 32720);

// 20480 is the next goodsize so we are loosing ~300 bytes or 1.5%.
static_assert(kExpireSegmentSize == 20168);

class PrimeEvictionPolicy {
 public:
  static constexpr bool can_evict = false;
  static constexpr bool can_gc = true;

  PrimeEvictionPolicy(DbIndex db_indx, DbSlice* db_slice, int64_t mem_budget)
      : db_slice_(db_slice), mem_budget_(mem_budget) {
    db_indx_ = db_indx;
  }

  void RecordSplit() {
    mem_budget_ -= PrimeTable::kSegBytes;
  }

  bool CanGrow(const PrimeTable& tbl) const {
    return mem_budget_ > int64_t(PrimeTable::kSegBytes);
  }

  unsigned GarbageCollect(const PrimeTable::EvictionBuckets& eb, PrimeTable* me) {
    unsigned res = 0;
    for (unsigned i = 0; i < ABSL_ARRAYSIZE(eb.iter); ++i) {
      auto it = eb.iter[i];
      for (; !it.is_done(); ++it) {
        if (it->second.HasExpire()) {
          auto [prime_it, exp_it] = db_slice_->ExpireIfNeeded(db_indx_, it);
          if (prime_it.is_done())
            ++res;
        }
      }
    }

    gc_count_ += res;
    return res;
  }

  int64_t mem_budget() const {
    return mem_budget_;
  }
  unsigned gc_count() const {
    return gc_count_;
  }

 private:
  DbSlice* db_slice_;
  int64_t mem_budget_;
  DbIndex db_indx_;
  unsigned gc_count_ = 0;
};

}  // namespace

#define ADD(x) (x) += o.x

DbStats& DbStats::operator+=(const DbStats& o) {
  static_assert(sizeof(DbStats) == 72);

  ADD(key_count);
  ADD(expire_count);
  ADD(bucket_count);
  ADD(inline_keys);

  ADD(obj_memory_usage);
  ADD(table_mem_usage);
  ADD(small_string_bytes);
  ADD(listpack_blob_cnt);
  ADD(listpack_bytes);

  return *this;
}

SliceEvents& SliceEvents::operator+=(const SliceEvents& o) {
  static_assert(sizeof(SliceEvents) == 24, "You should update this function with new fields");

  ADD(evicted_keys);
  ADD(expired_keys);
  ADD(garbage_collected);

  return *this;
}

#undef ADD

DbSlice::DbWrapper::DbWrapper(std::pmr::memory_resource* mr)
    : prime_table(4, detail::PrimeTablePolicy{}, mr),
      expire_table(0, detail::ExpireTablePolicy{}, mr),
      mcflag_table(0, detail::ExpireTablePolicy{}, mr) {
}

DbSlice::DbSlice(uint32_t index, EngineShard* owner) : shard_id_(index), owner_(owner) {
  db_arr_.emplace_back();
  CreateDb(0);
  expire_base_[0] = expire_base_[1] = 0;
}

DbSlice::~DbSlice() {
  // we do not need this code but it's easier to debug in case we encounter
  // memory allocation bugs during delete operations.

  for (auto& db : db_arr_) {
    if (!db)
      continue;
    db.reset();
  }
}

auto DbSlice::GetStats() const -> Stats {
  Stats s;
  s.events = events_;

  for (const auto& db : db_arr_) {
    if (!db)
      continue;

    s.db.key_count += db->prime_table.size();
    s.db.bucket_count += db->prime_table.bucket_count();
    s.db.expire_count += db->expire_table.size();
    s.db.obj_memory_usage += db->stats.obj_memory_usage;
    s.db.inline_keys += db->stats.inline_keys;
    s.db.table_mem_usage += (db->prime_table.mem_usage() + db->expire_table.mem_usage());
    s.db.listpack_blob_cnt += db->stats.listpack_blob_cnt;
    s.db.listpack_bytes += db->stats.listpack_bytes;
  }
  s.db.small_string_bytes = CompactObj::GetStats().small_string_bytes;

  return s;
}

void DbSlice::Reserve(DbIndex db_ind, size_t key_size) {
  ActivateDb(db_ind);

  auto& db = db_arr_[db_ind];
  DCHECK(db);

  db->prime_table.Reserve(key_size);
}

auto DbSlice::Find(DbIndex db_index, string_view key, unsigned req_obj_type) const
    -> OpResult<MainIterator> {
  auto it = FindExt(db_index, key).first;

  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (it->second.ObjType() != req_obj_type) {
    return OpStatus::WRONG_TYPE;
  }

  return it;
}

pair<MainIterator, ExpireIterator> DbSlice::FindExt(DbIndex db_ind, string_view key) const {
  DCHECK(IsDbValid(db_ind));

  auto& db = db_arr_[db_ind];
  MainIterator it = db->prime_table.Find(key);

  if (!IsValid(it)) {
    return make_pair(it, ExpireIterator{});
  }

  if (it->second.HasExpire()) {  // check expiry state
    return ExpireIfNeeded(db_ind, it);
  }

  return make_pair(it, ExpireIterator{});
}

OpResult<pair<MainIterator, unsigned>> DbSlice::FindFirst(DbIndex db_index, const ArgSlice& args) {
  DCHECK(!args.empty());

  for (unsigned i = 0; i < args.size(); ++i) {
    string_view s = args[i];
    OpResult<MainIterator> res = Find(db_index, s, OBJ_LIST);
    if (res)
      return make_pair(res.value(), i);
    if (res.status() != OpStatus::KEY_NOTFOUND)
      return res.status();
  }

  VLOG(1) << "FindFirst " << args.front() << " not found";
  return OpStatus::KEY_NOTFOUND;
}

auto DbSlice::AddOrFind(DbIndex db_index, string_view key) -> pair<MainIterator, bool> {
  DCHECK(IsDbValid(db_index));

  auto& db = db_arr_[db_index];

  // If we have some registered onchange callbacks, we must know in advance whether its Find or Add.
  if (!change_cb_.empty()) {
    auto it = FindExt(db_index, key).first;
    if (IsValid(it)) {
      return make_pair(it, true);
    }

    // It's a new entry.
    for (const auto& ccb : change_cb_) {
      ccb.second(db_index, ChangeReq{key});
    }
  }

  PrimeEvictionPolicy evp{db_index, this, int64_t(memory_budget_ - key.size())};

  // Fast-path - change_cb_ is empty so we Find or Add using
  // the insert operation: twice more efficient.
  CompactObj co_key{key};

  auto [it, inserted] = db->prime_table.Insert(std::move(co_key), PrimeValue{});
  if (inserted) {  // new entry
    db->stats.inline_keys += it->first.IsInline();
    db->stats.obj_memory_usage += it->first.MallocUsed();
    it.SetVersion(NextVersion());
    memory_budget_ = evp.mem_budget();

    return make_pair(it, true);
  }
  auto& existing = it;

  DCHECK(IsValid(existing));

  if (existing->second.HasExpire()) {
    auto expire_it = db->expire_table.Find(existing->first);
    CHECK(IsValid(expire_it));

    // TODO: to implement the incremental update of expiry values using multi-generation
    // expire_base_ update. Right now we use only index 0.
    uint32_t delta_ms = now_ms_ - expire_base_[0];

    if (expire_it->second.duration() <= delta_ms) {
      db->expire_table.Erase(expire_it);

      if (existing->second.HasFlag()) {
        db->mcflag_table.Erase(existing->first);
      }

      // Keep the entry but reset the object.
      db->stats.obj_memory_usage -= existing->second.MallocUsed();
      existing->second.Reset();
      events_.expired_keys++;

      return make_pair(existing, true);
    }
  }

  return make_pair(existing, false);
}

void DbSlice::ActivateDb(DbIndex db_ind) {
  if (db_arr_.size() <= db_ind)
    db_arr_.resize(db_ind + 1);
  CreateDb(db_ind);
}

void DbSlice::CreateDb(DbIndex index) {
  auto& db = db_arr_[index];
  if (!db) {
    db.reset(new DbWrapper{owner_->memory_resource()});
  }
}

bool DbSlice::Del(DbIndex db_ind, MainIterator it) {
  if (!IsValid(it)) {
    return false;
  }

  auto& db = db_arr_[db_ind];
  if (it->second.HasExpire()) {
    CHECK_EQ(1u, db->expire_table.Erase(it->first));
  }

  if (it->second.HasFlag()) {
    CHECK_EQ(1u, db->mcflag_table.Erase(it->first));
  }

  db->stats.inline_keys -= it->first.IsInline();
  db->stats.obj_memory_usage -= (it->first.MallocUsed() + it->second.MallocUsed());
  db->prime_table.Erase(it);

  return true;
}

size_t DbSlice::FlushDb(DbIndex db_ind) {
  auto flush_single = [this](DbIndex id) {
    auto& db = db_arr_[id];

    CHECK(db);

    size_t removed = db->prime_table.size();
    db->prime_table.Clear();
    db->expire_table.Clear();
    db->mcflag_table.Clear();
    db->stats = InternalDbStats{};

    return removed;
  };

  if (db_ind != kDbAll) {
    CHECK_LT(db_ind, db_arr_.size());

    return flush_single(db_ind);
  }

  size_t removed = 0;
  for (size_t i = 0; i < db_arr_.size(); ++i) {
    if (db_arr_[i]) {
      removed += flush_single(i);
    }
  }
  return removed;
}

// Returns true if a state has changed, false otherwise.
bool DbSlice::Expire(DbIndex db_ind, MainIterator it, uint64_t at) {
  auto& db = *db_arr_[db_ind];
  if (at == 0 && it->second.HasExpire()) {
    CHECK_EQ(1u, db.expire_table.Erase(it->first));
    it->second.SetExpire(false);

    return true;
  }

  if (!it->second.HasExpire() && at) {
    uint64_t delta = at - expire_base_[0];  // TODO: employ multigen expire updates.

    CHECK(db.expire_table.Insert(it->first.AsRef(), ExpirePeriod(delta)).second);
    it->second.SetExpire(true);

    return true;
  }

  return false;
}

void DbSlice::SetMCFlag(DbIndex db_ind, PrimeKey key, uint32_t flag) {
  auto& db = *db_arr_[db_ind];
  if (flag == 0) {
    db.mcflag_table.Erase(key);
  } else {
    auto [it, inserted] = db.mcflag_table.Insert(std::move(key), flag);
    if (!inserted)
      it->second = flag;
  }
}

uint32_t DbSlice::GetMCFlag(DbIndex db_ind, const PrimeKey& key) const {
  auto& db = *db_arr_[db_ind];
  auto it = db.mcflag_table.Find(key);
  return it.is_done() ? 0 : it->second;
}

MainIterator DbSlice::AddNew(DbIndex db_ind, string_view key, PrimeValue obj,
                             uint64_t expire_at_ms) {
  for (const auto& ccb : change_cb_) {
    ccb.second(db_ind, ChangeReq{key});
  }

  auto [res, added] = AddIfNotExist(db_ind, key, std::move(obj), expire_at_ms);
  CHECK(added);

  return res;
}

pair<MainIterator, bool> DbSlice::AddIfNotExist(DbIndex db_ind, string_view key, PrimeValue obj,
                                                uint64_t expire_at_ms) {
  DCHECK(!obj.IsRef());

  auto& db = *db_arr_[db_ind];
  CompactObj co_key{key};
  memory_budget_ -= key.size();

  PrimeEvictionPolicy evp{db_ind, this, memory_budget_};
  auto [new_entry, inserted] = db.prime_table.Insert(std::move(co_key), std::move(obj), evp);

  // in this case obj won't be moved and will be destroyed during unwinding.
  if (!inserted)
    return make_pair(new_entry, false);

  events_.garbage_collected += evp.gc_count();
  memory_budget_ = evp.mem_budget();
  new_entry.SetVersion(NextVersion());

  db.stats.inline_keys += new_entry->first.IsInline();
  db.stats.obj_memory_usage += (new_entry->first.MallocUsed() + new_entry->second.MallocUsed());

  if (expire_at_ms) {
    new_entry->second.SetExpire(true);
    uint64_t delta = expire_at_ms - expire_base_[0];
    CHECK(db.expire_table.Insert(new_entry->first.AsRef(), ExpirePeriod(delta)).second);
  }

  return make_pair(new_entry, true);
}

size_t DbSlice::DbSize(DbIndex db_ind) const {
  DCHECK_LT(db_ind, db_array_size());

  if (IsDbValid(db_ind)) {
    return db_arr_[db_ind]->prime_table.size();
  }
  return 0;
}

bool DbSlice::Acquire(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
  DCHECK(!lock_args.args.empty());

  auto& lt = db_arr_[lock_args.db_index]->lock_table;
  bool lock_acquired = true;

  if (lock_args.args.size() == 1) {
    lock_acquired = lt[lock_args.args.front()].Acquire(mode);
  } else {
    uniq_keys_.clear();

    for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
      auto s = lock_args.args[i];
      if (uniq_keys_.insert(s).second) {
        bool res = lt[s].Acquire(mode);
        lock_acquired &= res;
      }
    }
  }

  DVLOG(2) << "Acquire " << IntentLock::ModeName(mode) << " for " << lock_args.args[0]
           << " has_acquired: " << lock_acquired;

  return lock_acquired;
}

void DbSlice::Release(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
  DCHECK(!lock_args.args.empty());

  DVLOG(2) << "Release " << IntentLock::ModeName(mode) << " for " << lock_args.args[0];
  if (lock_args.args.size() == 1) {
    Release(mode, lock_args.db_index, lock_args.args.front(), 1);
  } else {
    auto& lt = db_arr_[lock_args.db_index]->lock_table;
    uniq_keys_.clear();
    for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
      auto s = lock_args.args[i];
      if (uniq_keys_.insert(s).second) {
        auto it = lt.find(s);
        CHECK(it != lt.end());
        it->second.Release(mode);
        if (it->second.IsFree()) {
          lt.erase(it);
        }
      }
    }
  }
}

void DbSlice::Release(IntentLock::Mode mode, DbIndex db_index, string_view key, unsigned count) {
  DVLOG(1) << "Release " << IntentLock::ModeName(mode) << " " << count << " for " << key;

  auto& lt = db_arr_[db_index]->lock_table;
  auto it = lt.find(key);
  CHECK(it != lt.end()) << key;
  it->second.Release(mode, count);
  if (it->second.IsFree()) {
    lt.erase(it);
  }
}

bool DbSlice::CheckLock(IntentLock::Mode mode, const KeyLockArgs& lock_args) const {
  DCHECK(!lock_args.args.empty());

  const auto& lt = db_arr_[lock_args.db_index]->lock_table;
  for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
    auto s = lock_args.args[i];
    auto it = lt.find(s);
    if (it != lt.end() && !it->second.Check(mode)) {
      return false;
    }
  }
  return true;
}

void DbSlice::PreUpdate(DbIndex db_ind, MainIterator it) {
  auto& db = db_arr_[db_ind];
  for (const auto& ccb : change_cb_) {
    ccb.second(db_ind, ChangeReq{it});
  }
  db->stats.obj_memory_usage -= it->second.MallocUsed();
  it.SetVersion(NextVersion());
}

void DbSlice::PostUpdate(DbIndex db_ind, MainIterator it) {
  auto& db = db_arr_[db_ind];
  db->stats.obj_memory_usage += it->second.MallocUsed();
}

pair<MainIterator, ExpireIterator> DbSlice::ExpireIfNeeded(DbIndex db_ind, MainIterator it) const {
  DCHECK(it->second.HasExpire());
  auto& db = db_arr_[db_ind];

  auto expire_it = db->expire_table.Find(it->first);

  CHECK(IsValid(expire_it));

  // TODO: to employ multi-generation update of expire-base and the underlying values.
  uint32_t delta_ms = now_ms_ - expire_base_[0];

  if (expire_it->second.duration() > delta_ms)
    return make_pair(it, expire_it);

  db->expire_table.Erase(expire_it);

  db->stats.inline_keys -= it->first.IsInline();
  db->stats.obj_memory_usage -= (it->first.MallocUsed() + it->second.MallocUsed());
  db->prime_table.Erase(it);
  ++events_.expired_keys;

  return make_pair(MainIterator{}, ExpireIterator{});
}

uint64_t DbSlice::RegisterOnChange(ChangeCallback cb) {
  uint64_t ver = NextVersion();
  change_cb_.emplace_back(ver, std::move(cb));
  return ver;
}

//! Unregisters the callback.
void DbSlice::UnregisterOnChange(uint64_t id) {
  for (auto it = change_cb_.begin(); it != change_cb_.end(); ++it) {
    if (it->first == id) {
      change_cb_.erase(it);
      return;
    }
  }
  LOG(DFATAL) << "Could not find " << id << " to unregister";
}

}  // namespace dfly
