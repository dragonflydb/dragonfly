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
#include "server/tiered_storage.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using facade::OpStatus;

namespace {

constexpr auto kPrimeSegmentSize = PrimeTable::kSegBytes;
constexpr auto kExpireSegmentSize = ExpireTable::kSegBytes;
constexpr auto kTaxSize = PrimeTable::kTaxAmount;

// mi_malloc good size is 32768. i.e. we have malloc waste of 1.5%.
static_assert(kPrimeSegmentSize == 32288);

// 20480 is the next goodsize so we are loosing ~300 bytes or 1.5%.
// 24576
static_assert(kExpireSegmentSize == 23528);

void UpdateStatsOnDeletion(PrimeIterator it, DbTableStats* stats) {
  size_t value_heap_size = it->second.MallocUsed();
  stats->inline_keys -= it->first.IsInline();
  stats->obj_memory_usage -= (it->first.MallocUsed() + value_heap_size);
  if (it->second.ObjType() == OBJ_STRING)
    stats->strval_memory_usage -= value_heap_size;
}

class PrimeEvictionPolicy {
 public:
  static constexpr bool can_evict = true;  // we implement eviction functionality.
  static constexpr bool can_gc = true;

  PrimeEvictionPolicy(DbIndex db_indx, bool can_evict, DbSlice* db_slice, int64_t mem_budget)
      : db_slice_(db_slice), mem_budget_(mem_budget), db_indx_(db_indx), can_evict_(can_evict) {
  }

  void RecordSplit(PrimeTable::Segment_t* segment) {
    mem_budget_ -= PrimeTable::kSegBytes;
    DVLOG(1) << "split: " << segment->SlowSize() << "/" << segment->capacity();
  }

  bool CanGrow(const PrimeTable& tbl) const {
    return mem_budget_ > int64_t(PrimeTable::kSegBytes);
  }

  unsigned GarbageCollect(const PrimeTable::HotspotBuckets& eb, PrimeTable* me);
  unsigned Evict(const PrimeTable::HotspotBuckets& eb, PrimeTable* me);

  int64_t mem_budget() const {
    return mem_budget_;
  }

  unsigned evicted() const {
    return evicted_;
  }

  unsigned checked() const {
    return checked_;
  }

 private:
  DbSlice* db_slice_;
  int64_t mem_budget_;
  unsigned evicted_ = 0;
  unsigned checked_ = 0;
  const DbIndex db_indx_;

  // unlike static constexpr can_evict, this parameter tells whether we can evict
  // items in runtime.
  const bool can_evict_;
};


unsigned PrimeEvictionPolicy::GarbageCollect(const PrimeTable::HotspotBuckets& eb, PrimeTable* me) {
  unsigned res = 0;
  // bool should_print = (eb.key_hash % 128) == 0;

  // based on tests - it's more efficient to pass regular buckets to gc.
  // stash buckets are filled last so much smaller change they have expired items.
  unsigned num_buckets =
      std::min<unsigned>(PrimeTable::HotspotBuckets::kRegularBuckets, eb.num_buckets);
  for (unsigned i = 0; i < num_buckets; ++i) {
    auto bucket_it = eb.at(i);
    for (; !bucket_it.is_done(); ++bucket_it) {
      if (bucket_it->second.HasExpire()) {
        ++checked_;
        auto [prime_it, exp_it] = db_slice_->ExpireIfNeeded(db_indx_, bucket_it);
        if (prime_it.is_done())
          ++res;
      }
    }
  }

  return res;
}

unsigned PrimeEvictionPolicy::Evict(const PrimeTable::HotspotBuckets& eb, PrimeTable* me) {
  if (!can_evict_)
    return 0;

  constexpr size_t kNumStashBuckets = ABSL_ARRAYSIZE(eb.probes.by_type.stash_buckets);

  // choose "randomly" a stash bucket to evict an item.
  auto bucket_it = eb.probes.by_type.stash_buckets[eb.key_hash % kNumStashBuckets];
  auto last_slot_it = bucket_it;
  last_slot_it += (PrimeTable::kBucketWidth - 1);
  if (!last_slot_it.is_done()) {
    if (last_slot_it->second.HasExpire()) {
      ExpireTable* expire_tbl = db_slice_->GetTables(db_indx_).second;
      CHECK_EQ(1u, expire_tbl->Erase(last_slot_it->first));
    }
    UpdateStatsOnDeletion(last_slot_it, db_slice_->MutableStats(db_indx_));
  }
  CHECK(me->ShiftRight(bucket_it));
  ++evicted_;

  return 1;
}

}  // namespace

#define ADD(x) (x) += o.x

DbStats& DbStats::operator+=(const DbStats& o) {
  constexpr size_t kDbSz = sizeof(DbStats);
  static_assert(kDbSz == 96);

  DbTableStats::operator+=(o);

  ADD(key_count);
  ADD(expire_count);
  ADD(bucket_count);
  ADD(table_mem_usage);

  return *this;
}

SliceEvents& SliceEvents::operator+=(const SliceEvents& o) {
  static_assert(sizeof(SliceEvents) == 48, "You should update this function with new fields");

  ADD(evicted_keys);
  ADD(expired_keys);
  ADD(garbage_collected);
  ADD(stash_unloaded);
  ADD(bumpups);
  ADD(garbage_checked);

  return *this;
}

#undef ADD

DbSlice::DbSlice(uint32_t index, bool caching_mode, EngineShard* owner)
    : shard_id_(index), caching_mode_(caching_mode), owner_(owner) {
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
  s.db_stats.resize(db_arr_.size());

  for (size_t i = 0; i < db_arr_.size(); ++i) {
    if (!db_arr_[i])
      continue;
    const auto& db_wrap = *db_arr_[i];
    DbStats& stats = s.db_stats[i];
    stats = db_wrap.stats;
    stats.key_count = db_wrap.prime.size();
    stats.bucket_count = db_wrap.prime.bucket_count();
    stats.expire_count = db_wrap.expire.size();
    stats.table_mem_usage = (db_wrap.prime.mem_usage() + db_wrap.expire.mem_usage());
  }
  s.small_string_bytes = CompactObj::GetStats().small_string_bytes;

  return s;
}

void DbSlice::Reserve(DbIndex db_ind, size_t key_size) {
  ActivateDb(db_ind);

  auto& db = db_arr_[db_ind];
  DCHECK(db);

  db->prime.Reserve(key_size);
}

auto DbSlice::Find(DbIndex db_index, string_view key, unsigned req_obj_type) const
    -> OpResult<PrimeIterator> {
  auto it = FindExt(db_index, key).first;

  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (it->second.ObjType() != req_obj_type) {
    return OpStatus::WRONG_TYPE;
  }

  return it;
}

pair<PrimeIterator, ExpireIterator> DbSlice::FindExt(DbIndex db_ind, string_view key) const {
  pair<PrimeIterator, ExpireIterator> res;

  if (!IsDbValid(db_ind))
    return res;

  auto& db = *db_arr_[db_ind];
  res.first = db.prime.Find(key);

  if (!IsValid(res.first)) {
    return res;
  }

  if (res.first->second.HasExpire()) {  // check expiry state
    res = ExpireIfNeeded(db_ind, res.first);
  }

  if (caching_mode_ && IsValid(res.first)) {
    if (!change_cb_.empty()) {
      auto bump_cb = [&](PrimeTable::bucket_iterator bit) {
        for (const auto& ccb : change_cb_) {
          ccb.second(db_ind, bit);
        }
      };

      db.prime.CVCUponBump(change_cb_.front().first, res.first, bump_cb);
    }

    res.first = db.prime.BumpUp(res.first);
    ++events_.bumpups;
  }

  return res;
}

OpResult<pair<PrimeIterator, unsigned>> DbSlice::FindFirst(DbIndex db_index, ArgSlice args) {
  DCHECK(!args.empty());

  for (unsigned i = 0; i < args.size(); ++i) {
    string_view s = args[i];
    OpResult<PrimeIterator> res = Find(db_index, s, OBJ_LIST);
    if (res)
      return make_pair(res.value(), i);
    if (res.status() != OpStatus::KEY_NOTFOUND)
      return res.status();
  }

  VLOG(1) << "FindFirst " << args.front() << " not found";
  return OpStatus::KEY_NOTFOUND;
}

pair<PrimeIterator, bool> DbSlice::AddOrFind(DbIndex db_index, string_view key) noexcept(false) {
  auto res = AddOrFind2(db_index, key);
  return make_pair(get<0>(res), get<2>(res));
}

tuple<PrimeIterator, ExpireIterator, bool> DbSlice::AddOrFind2(
    DbIndex db_index, string_view key) noexcept(false) {
  DCHECK(IsDbValid(db_index));

  auto& db = db_arr_[db_index];

  // If we have some registered onchange callbacks, we must know in advance whether its Find or Add.
  if (!change_cb_.empty()) {
    auto res = FindExt(db_index, key);

    if (IsValid(res.first)) {
      return tuple_cat(res, make_tuple(true));
    }

    // It's a new entry.
    for (const auto& ccb : change_cb_) {
      ccb.second(db_index, key);
    }
  }

  PrimeEvictionPolicy evp{db_index, bool(caching_mode_), this,
                          int64_t(memory_budget_ - key.size())};

  // Fast-path if change_cb_ is empty so we Find or Add using
  // the insert operation: twice more efficient.
  CompactObj co_key{key};
  PrimeIterator it;
  bool inserted;

  // I try/catch just for sake of having a convenient place to set a breakpoint.
  try {
    tie(it, inserted) = db->prime.Insert(std::move(co_key), PrimeValue{}, evp);
  } catch (bad_alloc& e) {
    throw e;
  }

  if (inserted) {  // new entry
    db->stats.inline_keys += it->first.IsInline();
    db->stats.obj_memory_usage += it->first.MallocUsed();

    events_.garbage_collected = db->prime.garbage_collected();
    events_.stash_unloaded = db->prime.stash_unloaded();
    events_.evicted_keys += evp.evicted();
    events_.garbage_checked += evp.checked();

    it.SetVersion(NextVersion());
    memory_budget_ = evp.mem_budget();

    return make_tuple(it, ExpireIterator{}, true);
  }

  auto& existing = it;

  DCHECK(IsValid(existing));

  ExpireIterator expire_it;
  if (existing->second.HasExpire()) {
    expire_it = db->expire.Find(existing->first);
    CHECK(IsValid(expire_it));

    // TODO: to implement the incremental update of expiry values using multi-generation
    // expire_base_ update. Right now we use only index 0.
    uint64_t delta_ms = now_ms_ - expire_base_[0];

    if (expire_it->second.duration_ms() <= delta_ms) {
      db->expire.Erase(expire_it);

      if (existing->second.HasFlag()) {
        db->mcflag.Erase(existing->first);
      }

      // Keep the entry but reset the object.
      size_t value_heap_size = existing->second.MallocUsed();
      db->stats.obj_memory_usage -= value_heap_size;
      if (existing->second.ObjType() == OBJ_STRING)
        db->stats.obj_memory_usage -= value_heap_size;

      existing->second.Reset();
      events_.expired_keys++;

      return make_tuple(existing, ExpireIterator{}, true);
    }
  }

  return make_tuple(existing, expire_it, false);
}

void DbSlice::ActivateDb(DbIndex db_ind) {
  if (db_arr_.size() <= db_ind)
    db_arr_.resize(db_ind + 1);
  CreateDb(db_ind);
}

void DbSlice::CreateDb(DbIndex index) {
  auto& db = db_arr_[index];
  if (!db) {
    db.reset(new DbTable{owner_->memory_resource()});
  }
}

bool DbSlice::Del(DbIndex db_ind, PrimeIterator it) {
  if (!IsValid(it)) {
    return false;
  }

  auto& db = db_arr_[db_ind];
  if (it->second.HasExpire()) {
    CHECK_EQ(1u, db->expire.Erase(it->first));
  }

  if (it->second.HasFlag()) {
    CHECK_EQ(1u, db->mcflag.Erase(it->first));
  }

  UpdateStatsOnDeletion(it, &db->stats);
  db->prime.Erase(it);

  return true;
}

void DbSlice::FlushDb(DbIndex db_ind) {
  // TODO: to add preeemptiveness by yielding inside clear.

  if (db_ind != kDbAll) {
    auto& db = db_arr_[db_ind];
    auto db_ptr = std::move(db);
    DCHECK(!db);
    CreateDb(db_ind);
    db_arr_[db_ind]->trans_locks.swap(db_ptr->trans_locks);

    boost::fibers::fiber([db_ptr = std::move(db_ptr)]() mutable { db_ptr.reset(); }).detach();

    return;
  }

  auto all_dbs = std::move(db_arr_);
  db_arr_.resize(all_dbs.size());
  for (size_t i = 0; i < db_arr_.size(); ++i) {
    if (all_dbs[i]) {
      CreateDb(i);
      db_arr_[i]->trans_locks.swap(all_dbs[i]->trans_locks);
    }
  }

  boost::fibers::fiber([all_dbs = std::move(all_dbs)]() mutable {
    for (auto& db : all_dbs) {
      db.reset();
    }
  }).detach();
}

// Returns true if a state has changed, false otherwise.
bool DbSlice::UpdateExpire(DbIndex db_ind, PrimeIterator it, uint64_t at) {
  auto& db = *db_arr_[db_ind];
  if (at == 0 && it->second.HasExpire()) {
    CHECK_EQ(1u, db.expire.Erase(it->first));
    it->second.SetExpire(false);

    return true;
  }

  if (!it->second.HasExpire() && at) {
    uint64_t delta = at - expire_base_[0];  // TODO: employ multigen expire updates.
    CHECK(db.expire.Insert(it->first.AsRef(), ExpirePeriod(delta)).second);
    it->second.SetExpire(true);

    return true;
  }

  return false;
}

void DbSlice::SetMCFlag(DbIndex db_ind, PrimeKey key, uint32_t flag) {
  auto& db = *db_arr_[db_ind];
  if (flag == 0) {
    db.mcflag.Erase(key);
  } else {
    auto [it, inserted] = db.mcflag.Insert(std::move(key), flag);
    if (!inserted)
      it->second = flag;
  }
}

uint32_t DbSlice::GetMCFlag(DbIndex db_ind, const PrimeKey& key) const {
  auto& db = *db_arr_[db_ind];
  auto it = db.mcflag.Find(key);
  return it.is_done() ? 0 : it->second;
}

PrimeIterator DbSlice::AddNew(DbIndex db_ind, string_view key, PrimeValue obj,
                              uint64_t expire_at_ms) noexcept(false) {
  auto [it, added] = AddOrFind(db_ind, key, std::move(obj), expire_at_ms);
  CHECK(added);

  return it;
}

pair<PrimeIterator, bool> DbSlice::AddOrFind(DbIndex db_ind, string_view key, PrimeValue obj,
                                             uint64_t expire_at_ms) noexcept(false) {
  DCHECK_LT(db_ind, db_arr_.size());
  DCHECK(!obj.IsRef());

  pair<PrimeIterator, bool> res = AddOrFind(db_ind, key);
  if (!res.second)  // have not inserted.
    return res;

  auto& db = *db_arr_[db_ind];
  auto& it = res.first;

  it->second = std::move(obj);
  PostUpdate(db_ind, it, false);

  if (expire_at_ms) {
    it->second.SetExpire(true);
    uint64_t delta = expire_at_ms - expire_base_[0];
    CHECK(db.expire.Insert(it->first.AsRef(), ExpirePeriod(delta)).second);
  }

  return res;
}

size_t DbSlice::DbSize(DbIndex db_ind) const {
  DCHECK_LT(db_ind, db_array_size());

  if (IsDbValid(db_ind)) {
    return db_arr_[db_ind]->prime.size();
  }
  return 0;
}

bool DbSlice::Acquire(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
  DCHECK(!lock_args.args.empty());
  DCHECK_GT(lock_args.key_step, 0u);

  auto& lt = db_arr_[lock_args.db_index]->trans_locks;
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
    auto& lt = db_arr_[lock_args.db_index]->trans_locks;
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

bool DbSlice::CheckLock(IntentLock::Mode mode, const KeyLockArgs& lock_args) const {
  DCHECK(!lock_args.args.empty());

  const auto& lt = db_arr_[lock_args.db_index]->trans_locks;
  for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
    auto s = lock_args.args[i];
    auto it = lt.find(s);
    if (it != lt.end() && !it->second.Check(mode)) {
      return false;
    }
  }
  return true;
}

void DbSlice::PreUpdate(DbIndex db_ind, PrimeIterator it) {
  for (const auto& ccb : change_cb_) {
    ccb.second(db_ind, ChangeReq{it});
  }
  size_t value_heap_size = it->second.MallocUsed();
  auto* stats = MutableStats(db_ind);
  stats->obj_memory_usage -= value_heap_size;
  stats->update_value_amount -= value_heap_size;

  if (it->second.ObjType() == OBJ_STRING) {
    stats->strval_memory_usage -= value_heap_size;
    if (it->second.IsExternal()) {
      TieredStorage* tiered = shard_owner()->tiered_storage();
      auto [offset, size] = it->second.GetExternalPtr();
      tiered->Free(db_ind, offset, size);
      it->second.Reset();
    }
  }

  it.SetVersion(NextVersion());
}

void DbSlice::PostUpdate(DbIndex db_ind, PrimeIterator it, bool existing) {
  DbTableStats* stats = MutableStats(db_ind);

  size_t value_heap_size = it->second.MallocUsed();
  stats->obj_memory_usage += value_heap_size;
  if (it->second.ObjType() == OBJ_STRING)
    stats->strval_memory_usage += value_heap_size;
  if (existing)
    stats->update_value_amount += value_heap_size;
}

pair<PrimeIterator, ExpireIterator> DbSlice::ExpireIfNeeded(DbIndex db_ind,
                                                            PrimeIterator it) const {
  DCHECK(it->second.HasExpire());
  auto& db = db_arr_[db_ind];

  auto expire_it = db->expire.Find(it->first);

  CHECK(IsValid(expire_it));

  // TODO: to employ multi-generation update of expire-base and the underlying values.
  time_t expire_time = ExpireTime(expire_it);

  if (now_ms_ < expire_time)
    return make_pair(it, expire_it);

  db->expire.Erase(expire_it);
  UpdateStatsOnDeletion(it, &db->stats);
  db->prime.Erase(it);
  ++events_.expired_keys;

  return make_pair(PrimeIterator{}, ExpireIterator{});
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

auto DbSlice::DeleteExpired(DbIndex db_ind, unsigned count) -> DeleteExpiredStats {
  auto& db = *db_arr_[db_ind];
  DeleteExpiredStats result;

  auto cb = [&](ExpireIterator it) {
    result.traversed++;
    time_t ttl = ExpireTime(it) - Now();
    if (ttl <= 0) {
      auto prime_it = db.prime.Find(it->first);
      CHECK(!prime_it.is_done());
      ExpireIfNeeded(db_ind, prime_it);
      ++result.deleted;
    } else {
      result.survivor_ttl_sum += ttl;
    }
  };

  unsigned i = 0;
  for (; i < count / 3; ++i) {
    db.expire_cursor = db.expire.Traverse(db.expire_cursor, cb);
  }

  // continue traversing only if we had strong deletion rate based on the first sample.
  if (result.deleted * 4 > result.traversed) {
    for (; i < count; ++i) {
      db.expire_cursor = db.expire.Traverse(db.expire_cursor, cb);
    }
  }

  return result;
}

}  // namespace dfly
