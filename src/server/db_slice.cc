// Copyright 2022, DragonflyDB authors.  All rights reserved.
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
#include "server/journal/journal.h"
#include "server/server_state.h"
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

void PerformDeletion(PrimeIterator del_it, ExpireIterator exp_it, EngineShard* shard,
                     DbTable* table) {
  if (!exp_it.is_done()) {
    table->expire.Erase(exp_it);
  }

  DbTableStats& stats = table->stats;
  const PrimeValue& pv = del_it->second;
  if (pv.IsExternal()) {
    auto [offset, size] = pv.GetExternalSlice();

    stats.tiered_entries--;
    stats.tiered_size -= size;
    TieredStorage* tiered = shard->tiered_storage();
    tiered->Free(offset, size);
  }

  size_t value_heap_size = pv.MallocUsed();
  stats.inline_keys -= del_it->first.IsInline();
  stats.obj_memory_usage -= (del_it->first.MallocUsed() + value_heap_size);
  if (pv.ObjType() == OBJ_STRING)
    stats.strval_memory_usage -= value_heap_size;

  table->prime.Erase(del_it);
}

inline void PerformDeletion(PrimeIterator del_it, EngineShard* shard, DbTable* table) {
  ExpireIterator exp_it;
  if (del_it->second.HasExpire()) {
    exp_it = table->expire.Find(del_it->first);
    DCHECK(!exp_it.is_done());
  }

  PerformDeletion(del_it, exp_it, shard, table);
};

class PrimeEvictionPolicy {
 public:
  static constexpr bool can_evict = true;  // we implement eviction functionality.
  static constexpr bool can_gc = true;

  PrimeEvictionPolicy(const DbContext& cntx, bool can_evict, ssize_t mem_budget, ssize_t soft_limit,
                      DbSlice* db_slice)
      : db_slice_(db_slice), mem_budget_(mem_budget), soft_limit_(soft_limit), cntx_(cntx),
        can_evict_(can_evict) {
  }

  // A hook function that is called every time a segment is full and requires splitting.
  void RecordSplit(PrimeTable::Segment_t* segment) {
    mem_budget_ -= PrimeTable::kSegBytes;
    DVLOG(1) << "split: " << segment->SlowSize() << "/" << segment->capacity();
  }

  bool CanGrow(const PrimeTable& tbl) const;

  unsigned GarbageCollect(const PrimeTable::HotspotBuckets& eb, PrimeTable* me);
  unsigned Evict(const PrimeTable::HotspotBuckets& eb, PrimeTable* me);

  ssize_t mem_budget() const {
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
  ssize_t mem_budget_;
  ssize_t soft_limit_ = 0;
  const DbContext cntx_;

  unsigned evicted_ = 0;
  unsigned checked_ = 0;

  // unlike static constexpr can_evict, this parameter tells whether we can evict
  // items in runtime.
  const bool can_evict_;
};

class PrimeBumpPolicy {
 public:
  // returns true if key can be made less important for eviction (opposite of bump up)
  bool CanBumpDown(const CompactObj& key) const {
    return !key.IsSticky();
  }
};

bool PrimeEvictionPolicy::CanGrow(const PrimeTable& tbl) const {
  if (mem_budget_ > soft_limit_)
    return true;

  DCHECK_LT(tbl.size(), tbl.capacity());

  // We take a conservative stance here -
  // we estimate how much memory we will take with the current capacity
  // even though we may currently use less memory.
  // see https://github.com/dragonflydb/dragonfly/issues/256#issuecomment-1227095503
  size_t available = tbl.capacity() - tbl.size();
  return mem_budget_ > int64_t(PrimeTable::kSegBytes + db_slice_->bytes_per_object() * available);
}

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
        auto [prime_it, exp_it] = db_slice_->ExpireIfNeeded(cntx_, bucket_it);
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
    // don't evict sticky items
    if (last_slot_it->first.IsSticky()) {
      return 0;
    }

    DbTable* table = db_slice_->GetDBTable(cntx_.db_index);
    PerformDeletion(last_slot_it, db_slice_->shard_owner(), table);
    ++evicted_;
  }
  me->ShiftRight(bucket_it);

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
  static_assert(sizeof(SliceEvents) == 72, "You should update this function with new fields");

  ADD(evicted_keys);
  ADD(hard_evictions);
  ADD(expired_keys);
  ADD(garbage_collected);
  ADD(stash_unloaded);
  ADD(bumpups);
  ADD(garbage_checked);
  ADD(hits);
  ADD(misses);

  return *this;
}

#undef ADD

DbSlice::DbSlice(uint32_t index, bool caching_mode, EngineShard* owner)
    : shard_id_(index), caching_mode_(caching_mode), owner_(owner) {
  db_arr_.emplace_back();
  CreateDb(0);
  expire_base_[0] = expire_base_[1] = 0;
  soft_budget_limit_ = (0.1 * max_memory_limit / shard_set->size());
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

auto DbSlice::Find(const Context& cntx, string_view key, unsigned req_obj_type) const
    -> OpResult<PrimeIterator> {
  auto it = FindExt(cntx, key).first;

  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (it->second.ObjType() != req_obj_type) {
    return OpStatus::WRONG_TYPE;
  }

  return it;
}

pair<PrimeIterator, ExpireIterator> DbSlice::FindExt(const Context& cntx, string_view key) const {
  pair<PrimeIterator, ExpireIterator> res;

  if (!IsDbValid(cntx.db_index))
    return res;

  auto& db = *db_arr_[cntx.db_index];
  res.first = db.prime.Find(key);

  if (!IsValid(res.first)) {
    events_.misses++;
    return res;
  }

  if (res.first->second.HasExpire()) {  // check expiry state
    res = ExpireIfNeeded(cntx, res.first);
  }

  if (caching_mode_ && IsValid(res.first)) {
    if (!change_cb_.empty()) {
      auto bump_cb = [&](PrimeTable::bucket_iterator bit) {
        for (const auto& ccb : change_cb_) {
          ccb.second(cntx.db_index, bit);
        }
      };

      //
      db.prime.CVCUponBump(change_cb_.back().first, res.first, bump_cb);
    }

    res.first = db.prime.BumpUp(res.first, PrimeBumpPolicy{});
    ++events_.bumpups;
  }

  events_.hits++;
  return res;
}

OpResult<pair<PrimeIterator, unsigned>> DbSlice::FindFirst(const Context& cntx, ArgSlice args) {
  DCHECK(!args.empty());

  for (unsigned i = 0; i < args.size(); ++i) {
    string_view s = args[i];
    OpResult<PrimeIterator> res = Find(cntx, s, OBJ_LIST);
    if (res)
      return make_pair(res.value(), i);
    if (res.status() != OpStatus::KEY_NOTFOUND)
      return res.status();
  }

  VLOG(1) << "FindFirst " << args.front() << " not found";
  return OpStatus::KEY_NOTFOUND;
}

pair<PrimeIterator, bool> DbSlice::AddOrFind(const Context& cntx, string_view key) noexcept(false) {
  auto res = AddOrFind2(cntx, key);
  return make_pair(get<0>(res), get<2>(res));
}

tuple<PrimeIterator, ExpireIterator, bool> DbSlice::AddOrFind2(const Context& cntx,
                                                               string_view key) noexcept(false) {
  DCHECK(IsDbValid(cntx.db_index));

  DbTable& db = *db_arr_[cntx.db_index];

  // If we have some registered onchange callbacks, we must know in advance whether its Find or Add.
  if (!change_cb_.empty()) {
    auto res = FindExt(cntx, key);

    if (IsValid(res.first)) {
      return tuple_cat(res, make_tuple(false));
    }

    // It's a new entry.
    for (const auto& ccb : change_cb_) {
      ccb.second(cntx.db_index, key);
    }
  }

  PrimeEvictionPolicy evp{cntx, bool(caching_mode_), int64_t(memory_budget_ - key.size()),
                          ssize_t(soft_budget_limit_), this};

  // If we are over limit in non-cache scenario, just be conservative and throw.
  if (!caching_mode_ && evp.mem_budget() < 0) {
    throw bad_alloc();
  }

  // Fast-path if change_cb_ is empty so we Find or Add using
  // the insert operation: twice more efficient.
  CompactObj co_key{key};
  PrimeIterator it;
  bool inserted;

  // I try/catch just for sake of having a convenient place to set a breakpoint.
  try {
    tie(it, inserted) = db.prime.Insert(std::move(co_key), PrimeValue{}, evp);
  } catch (bad_alloc& e) {
    throw e;
  }

  size_t evicted_obj_bytes = 0;

  // We may still reach the state when our memory usage is above the limit even if we
  // do not add new segments. For example, we have half full segments
  // and we add new objects or update the existing ones and our memory usage grows.
  if (evp.mem_budget() < 0) {
    evicted_obj_bytes = EvictObjects(-evp.mem_budget(), it, &db);
  }

  if (inserted) {  // new entry
    db.stats.inline_keys += it->first.IsInline();
    db.stats.obj_memory_usage += it->first.MallocUsed();

    events_.garbage_collected = db.prime.garbage_collected();
    events_.stash_unloaded = db.prime.stash_unloaded();
    events_.evicted_keys += evp.evicted();
    events_.garbage_checked += evp.checked();

    it.SetVersion(NextVersion());
    memory_budget_ = evp.mem_budget() + evicted_obj_bytes;

    return make_tuple(it, ExpireIterator{}, true);
  }

  auto& existing = it;

  DCHECK(IsValid(existing));

  memory_budget_ += evicted_obj_bytes;

  ExpireIterator expire_it;
  if (existing->second.HasExpire()) {
    expire_it = db.expire.Find(existing->first);
    CHECK(IsValid(expire_it));

    // TODO: to implement the incremental update of expiry values using multi-generation
    // expire_base_ update. Right now we use only index 0.
    uint64_t delta_ms = cntx.time_now_ms - expire_base_[0];

    if (expire_it->second.duration_ms() <= delta_ms) {
      db.expire.Erase(expire_it);

      if (existing->second.HasFlag()) {
        db.mcflag.Erase(existing->first);
      }

      // Keep the entry but reset the object.
      size_t value_heap_size = existing->second.MallocUsed();
      db.stats.obj_memory_usage -= value_heap_size;

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

bool DbSlice::Del(DbIndex db_ind, PrimeIterator it) {
  if (!IsValid(it)) {
    return false;
  }

  auto& db = db_arr_[db_ind];
  if (it->second.HasFlag()) {
    CHECK_EQ(1u, db->mcflag.Erase(it->first));
  }

  PerformDeletion(it, shard_owner(), db.get());

  return true;
}

void DbSlice::FlushDb(DbIndex db_ind) {
  // TODO: to add preeemptiveness by yielding inside clear.

  if (db_ind != kDbAll) {
    auto& db = db_arr_[db_ind];
    if (db) {
      InvalidateDbWatches(db_ind);
    }

    auto db_ptr = std::move(db);
    DCHECK(!db);
    CreateDb(db_ind);
    db_arr_[db_ind]->trans_locks.swap(db_ptr->trans_locks);

    auto cb = [this, db_ptr = std::move(db_ptr)]() mutable {
      if (db_ptr->stats.tiered_entries > 0) {
        for (auto it = db_ptr->prime.begin(); it != db_ptr->prime.end(); ++it) {
          if (it->second.IsExternal()) {
            PerformDeletion(it, shard_owner(), db_ptr.get());
          }
        }
      }

      DCHECK_EQ(0u, db_ptr->stats.tiered_entries);

      db_ptr.reset();
      mi_heap_collect(ServerState::tlocal()->data_heap(), true);
    };

    boost::fibers::fiber(std::move(cb)).detach();

    return;
  }

  for (size_t i = 0; i < db_arr_.size(); i++) {
    if (db_arr_[i]) {
      InvalidateDbWatches(i);
    }
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

void DbSlice::AddExpire(DbIndex db_ind, PrimeIterator main_it, uint64_t at) {
  uint64_t delta = at - expire_base_[0];  // TODO: employ multigen expire updates.
  CHECK(db_arr_[db_ind]->expire.Insert(main_it->first.AsRef(), ExpirePeriod(delta)).second);
  main_it->second.SetExpire(true);
}

bool DbSlice::RemoveExpire(DbIndex db_ind, PrimeIterator main_it) {
  if (main_it->second.HasExpire()) {
    CHECK_EQ(1u, db_arr_[db_ind]->expire.Erase(main_it->first));
    main_it->second.SetExpire(false);
    return true;
  }
  return false;
}

// Returns true if a state has changed, false otherwise.
bool DbSlice::UpdateExpire(DbIndex db_ind, PrimeIterator it, uint64_t at) {
  if (at == 0) {
    return RemoveExpire(db_ind, it);
  }

  if (!it->second.HasExpire() && at) {
    AddExpire(db_ind, it, at);
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

PrimeIterator DbSlice::AddNew(const Context& cntx, string_view key, PrimeValue obj,
                              uint64_t expire_at_ms) noexcept(false) {
  auto [it, added] = AddOrSkip(cntx, key, std::move(obj), expire_at_ms);
  CHECK(added);

  return it;
}

pair<int64_t, int64_t> DbSlice::ExpireParams::Calculate(int64_t now_ms) const {
  int64_t msec = (unit == TimeUnit::SEC) ? value * 1000 : value;
  int64_t now_msec = now_ms;
  int64_t rel_msec = absolute ? msec - now_msec : msec;
  return make_pair(rel_msec, now_msec + rel_msec);
}

OpResult<int64_t> DbSlice::UpdateExpire(const Context& cntx, PrimeIterator prime_it,
                                        ExpireIterator expire_it, const ExpireParams& params) {
  DCHECK(params.IsDefined());
  DCHECK(IsValid(prime_it));

  auto [rel_msec, abs_msec] = params.Calculate(cntx.time_now_ms);
  if (rel_msec > kMaxExpireDeadlineSec * 1000) {
    return OpStatus::OUT_OF_RANGE;
  }

  if (rel_msec <= 0 && !params.persist) {
    CHECK(Del(cntx.db_index, prime_it));
    return -1;
  } else if (IsValid(expire_it)) {
    expire_it->second = FromAbsoluteTime(abs_msec);
    return abs_msec;
  } else {
    UpdateExpire(cntx.db_index, prime_it, params.persist ? 0 : abs_msec);
    return params.persist ? 0 : abs_msec;
  }
}

std::pair<PrimeIterator, bool> DbSlice::AddOrUpdateInternal(const Context& cntx,
                                                            std::string_view key, PrimeValue obj,
                                                            uint64_t expire_at_ms,
                                                            bool force_update) noexcept(false) {
  DCHECK(!obj.IsRef());

  pair<PrimeIterator, bool> res = AddOrFind(cntx, key);
  if (!res.second && !force_update)  // have not inserted.
    return res;

  auto& db = *db_arr_[cntx.db_index];
  auto& it = res.first;

  it->second = std::move(obj);
  PostUpdate(cntx.db_index, it, key, false);

  if (expire_at_ms) {
    it->second.SetExpire(true);
    uint64_t delta = expire_at_ms - expire_base_[0];
    auto [eit, inserted] = db.expire.Insert(it->first.AsRef(), ExpirePeriod(delta));
    CHECK(inserted || force_update);
    if (!inserted) {
      eit->second = ExpirePeriod(delta);
    }
  }

  return res;
}

pair<PrimeIterator, bool> DbSlice::AddOrUpdate(const Context& cntx, string_view key, PrimeValue obj,
                                               uint64_t expire_at_ms) noexcept(false) {
  return AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms, true);
}

pair<PrimeIterator, bool> DbSlice::AddOrSkip(const Context& cntx, string_view key, PrimeValue obj,
                                             uint64_t expire_at_ms) noexcept(false) {
  return AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms, false);
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
      // We assume here that the operation code either loaded the entry into memory
      // before calling to PreUpdate or it does not need to read it at all.
      // After this code executes, the external blob is lost.
      TieredStorage* tiered = shard_owner()->tiered_storage();
      auto [offset, size] = it->second.GetExternalSlice();
      tiered->Free(offset, size);
      it->second.Reset();

      stats->tiered_entries -= 1;
      stats->tiered_size -= size;
    } else if (it->second.HasIoPending()) {
      TieredStorage* tiered = shard_owner()->tiered_storage();
      tiered->CancelIo(db_ind, it);
    }
  }

  it.SetVersion(NextVersion());
}

void DbSlice::PostUpdate(DbIndex db_ind, PrimeIterator it, std::string_view key, bool existing) {
  DbTableStats* stats = MutableStats(db_ind);

  size_t value_heap_size = it->second.MallocUsed();
  stats->obj_memory_usage += value_heap_size;
  if (it->second.ObjType() == OBJ_STRING)
    stats->strval_memory_usage += value_heap_size;
  if (existing)
    stats->update_value_amount += value_heap_size;

  auto& watched_keys = db_arr_[db_ind]->watched_keys;
  if (!watched_keys.empty()) {
    // Check if the key is watched.
    if (auto wit = watched_keys.find(key); wit != watched_keys.end()) {
      for (auto conn_ptr : wit->second) {
        conn_ptr->watched_dirty.store(true, memory_order_relaxed);
      }
      // No connections need to watch it anymore.
      watched_keys.erase(wit);
    }
  }
}

pair<PrimeIterator, ExpireIterator> DbSlice::ExpireIfNeeded(const Context& cntx,
                                                            PrimeIterator it) const {
  DCHECK(it->second.HasExpire());
  auto& db = db_arr_[cntx.db_index];

  auto expire_it = db->expire.Find(it->first);

  CHECK(IsValid(expire_it));

  // TODO: to employ multi-generation update of expire-base and the underlying values.
  time_t expire_time = ExpireTime(expire_it);

  if (time_t(cntx.time_now_ms) < expire_time)
    return make_pair(it, expire_it);

  // Replicate any expiration as DEL command.
  // TODO: Pass optional key to skip decoding.
  if (auto journal = owner_->journal(); journal) {
    string scratch;
    journal->RecordEntry(0, cntx.db_index,
                         make_pair("DEL"sv, ArgSlice{it->first.GetSlice(&scratch)}), 1);
  }

  PerformDeletion(it, expire_it, shard_owner(), db.get());
  ++events_.expired_keys;

  return make_pair(PrimeIterator{}, ExpireIterator{});
}

void DbSlice::ExpireAllIfNeeded() {
  for (DbIndex db_index = 0; db_index < db_arr_.size(); db_index++) {
    if (!db_arr_[db_index])
      continue;
    auto& db = *db_arr_[db_index];

    auto cb = [&](ExpireTable::iterator exp_it) {
      auto prime_it = db.prime.Find(exp_it->first);
      if (!IsValid(prime_it)) {
        LOG(ERROR) << "Expire entry " << exp_it->first.ToString() << " not found in prime table";
        return;
      }
      ExpireIfNeeded(DbSlice::Context{db_index, GetCurrentTimeMs()}, prime_it);
    };

    ExpireTable::Cursor cursor;
    do {
      cursor = db.expire.Traverse(cursor, cb);
    } while (cursor);
  }
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

auto DbSlice::DeleteExpiredStep(const Context& cntx, unsigned count) -> DeleteExpiredStats {
  auto& db = *db_arr_[cntx.db_index];
  DeleteExpiredStats result;

  auto cb = [&](ExpireIterator it) {
    result.traversed++;
    time_t ttl = ExpireTime(it) - cntx.time_now_ms;
    if (ttl <= 0) {
      auto prime_it = db.prime.Find(it->first);
      CHECK(!prime_it.is_done());
      ExpireIfNeeded(cntx, prime_it);
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

// TODO: Design a better background evicting heuristic.
void DbSlice::FreeMemWithEvictionStep(DbIndex db_ind, size_t increase_goal_bytes) {
  if (!caching_mode_)
    return;
}

void DbSlice::CreateDb(DbIndex db_ind) {
  auto& db = db_arr_[db_ind];
  if (!db) {
    db.reset(new DbTable{owner_->memory_resource()});
  }
}

// "it" is the iterator that we just added/updated and it should not be deleted.
// "table" is the instance where we should delete the objects from.
size_t DbSlice::EvictObjects(size_t memory_to_free, PrimeIterator it, DbTable* table) {
  PrimeTable::Segment_t* segment = table->prime.GetSegment(it.segment_id());
  DCHECK(segment);

  constexpr unsigned kNumStashBuckets =
      PrimeTable::Segment_t::kTotalBuckets - PrimeTable::Segment_t::kNumBuckets;

  PrimeTable::bucket_iterator it2(it);
  unsigned evicted = 0;
  bool evict_succeeded = false;

  EngineShard* shard = owner_;
  size_t used_memory_start = shard->UsedMemory();

  auto freed_memory_fun = [&] {
    size_t current = shard->UsedMemory();
    return current < used_memory_start ? used_memory_start - current : 0;
  };

  for (unsigned i = 0; !evict_succeeded && i < kNumStashBuckets; ++i) {
    unsigned stash_bid = i + PrimeTable::Segment_t::kNumBuckets;
    const auto& bucket = segment->GetBucket(stash_bid);
    if (bucket.IsEmpty())
      continue;

    for (int slot_id = PrimeTable::Segment_t::kNumSlots - 1; slot_id >= 0; --slot_id) {
      if (!bucket.IsBusy(slot_id))
        continue;

      auto evict_it = table->prime.GetIterator(it.segment_id(), stash_bid, slot_id);
      // skip the iterator that we must keep or the sticky items.
      if (evict_it == it || evict_it->first.IsSticky())
        continue;

      PerformDeletion(evict_it, shard_owner(), table);
      ++evicted;
      if (freed_memory_fun() > memory_to_free) {
        evict_succeeded = true;
        break;
      }
    }
  }

  if (evicted) {
    DVLOG(1) << "Evicted " << evicted << " stashed items, freed " << freed_memory_fun() << " bytes";
  }

  // Try normal buckets now. We iterate from largest slot to smallest across the whole segment.
  for (int slot_id = PrimeTable::Segment_t::kNumSlots - 1; !evict_succeeded && slot_id >= 0;
       --slot_id) {
    for (unsigned i = 0; i < PrimeTable::Segment_t::kNumBuckets; ++i) {
      unsigned bid = (it.bucket_id() + i) % PrimeTable::Segment_t::kNumBuckets;
      const auto& bucket = segment->GetBucket(bid);
      if (!bucket.IsBusy(slot_id))
        continue;

      auto evict_it = table->prime.GetIterator(it.segment_id(), bid, slot_id);
      if (evict_it == it || evict_it->first.IsSticky())
        continue;

      PerformDeletion(evict_it, shard_owner(), table);
      ++evicted;

      if (freed_memory_fun() > memory_to_free) {
        evict_succeeded = true;
        break;
      }
    }
  }

  if (evicted) {
    DVLOG(1) << "Evicted total: " << evicted << " items, freed " << freed_memory_fun() << " bytes "
             << "success: " << evict_succeeded;

    events_.evicted_keys += evicted;
    events_.hard_evictions += evicted;
  }

  return freed_memory_fun();
};

void DbSlice::RegisterWatchedKey(DbIndex db_indx, std::string_view key,
                                 ConnectionState::ExecInfo* exec_info) {
  db_arr_[db_indx]->watched_keys[key].push_back(exec_info);
}

void DbSlice::UnregisterConnectionWatches(ConnectionState::ExecInfo* exec_info) {
  for (const auto& [db_indx, key] : exec_info->watched_keys) {
    auto& watched_keys = db_arr_[db_indx]->watched_keys;
    if (auto it = watched_keys.find(key); it != watched_keys.end()) {
      it->second.erase(std::remove(it->second.begin(), it->second.end(), exec_info),
                       it->second.end());
      if (it->second.empty())
        watched_keys.erase(it);
    }
  }
}

void DbSlice::InvalidateDbWatches(DbIndex db_indx) {
  for (const auto& [key, conn_list] : db_arr_[db_indx]->watched_keys) {
    for (auto conn_ptr : conn_list) {
      conn_ptr->watched_dirty.store(true, memory_order_relaxed);
    }
  }
}

}  // namespace dfly
