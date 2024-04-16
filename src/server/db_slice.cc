// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/db_slice.h"

#include <absl/cleanup/cleanup.h>

#include "base/flags.h"
#include "base/logging.h"
#include "generic_family.h"
#include "server/cluster/cluster_config.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "strings/human_readable.h"
#include "util/fibers/stacktrace.h"

ABSL_FLAG(bool, enable_heartbeat_eviction, true,
          "Enable eviction during heartbeat when memory is under pressure.");

ABSL_FLAG(uint32_t, max_eviction_per_heartbeat, 100,
          "The maximum number of key-value pairs that will be deleted in each eviction "
          "when heartbeat based eviction is triggered under memory pressure.");

ABSL_FLAG(uint32_t, max_segment_to_consider, 4,
          "The maximum number of dashtable segments to scan in each eviction "
          "when heartbeat based eviction is triggered under memory pressure.");

ABSL_FLAG(double, table_growth_margin, 1.1,
          "Prevents table from growing if number of free slots x average object size x this ratio "
          "is larger than memory budget.");

namespace dfly {

using namespace std;
using namespace util;
using absl::GetFlag;
using facade::OpStatus;

namespace {

constexpr auto kPrimeSegmentSize = PrimeTable::kSegBytes;
constexpr auto kExpireSegmentSize = ExpireTable::kSegBytes;

// mi_malloc good size is 32768. i.e. we have malloc waste of 1.5%.
static_assert(kPrimeSegmentSize == 32288);

// 20480 is the next goodsize so we are loosing ~300 bytes or 1.5%.
// 24576
static_assert(kExpireSegmentSize == 23528);

void AccountObjectMemory(string_view key, unsigned type, int64_t size, DbTable* db) {
  DCHECK_NE(db, nullptr);
  DbTableStats& stats = db->stats;
  DCHECK_GE(static_cast<int64_t>(stats.obj_memory_usage) + size, 0)
      << "Can't decrease " << size << " from " << stats.obj_memory_usage;

  stats.AddTypeMemoryUsage(type, size);

  if (ClusterConfig::IsEnabled()) {
    db->slots_stats[ClusterConfig::KeySlot(key)].memory_bytes += size;
  }
}

class PrimeEvictionPolicy {
 public:
  static constexpr bool can_evict = true;  // we implement eviction functionality.
  static constexpr bool can_gc = true;

  PrimeEvictionPolicy(const DbContext& cntx, bool can_evict, ssize_t mem_budget, ssize_t soft_limit,
                      DbSlice* db_slice, bool apply_memory_limit)
      : db_slice_(db_slice),
        mem_budget_(mem_budget),
        soft_limit_(soft_limit),
        cntx_(cntx),
        can_evict_(can_evict),
        apply_memory_limit_(apply_memory_limit) {
  }

  // A hook function that is called every time a segment is full and requires splitting.
  void RecordSplit(PrimeTable::Segment_t* segment) {
    mem_budget_ -= PrimeTable::kSegBytes;
    DVLOG(2) << "split: " << segment->SlowSize() << "/" << segment->capacity();
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
  const bool apply_memory_limit_;
};

class PrimeBumpPolicy {
 public:
  PrimeBumpPolicy(const absl::flat_hash_set<CompactObjectView>& fetched_items)
      : fetched_items_(fetched_items) {
  }
  // returns true if we can change the object location in dash table.
  bool CanBump(const CompactObj& obj) const {
    return !obj.IsSticky() && !fetched_items_.contains(obj);
  }

 private:
  const absl::flat_hash_set<CompactObjectView>& fetched_items_;
};

bool PrimeEvictionPolicy::CanGrow(const PrimeTable& tbl) const {
  if (!apply_memory_limit_ || mem_budget_ > soft_limit_)
    return true;

  DCHECK_LE(tbl.size(), tbl.capacity());

  // We take a conservative stance here -
  // we estimate how much memory we will take with the current capacity
  // even though we may currently use less memory.
  // see https://github.com/dragonflydb/dragonfly/issues/256#issuecomment-1227095503
  size_t new_available = (tbl.capacity() - tbl.size()) + PrimeTable::kSegCapacity;
  bool res =
      mem_budget_ > int64_t(PrimeTable::kSegBytes + db_slice_->bytes_per_object() * new_available *
                                                        GetFlag(FLAGS_table_growth_margin));
  VLOG(2) << "available: " << new_available << ", res: " << res;

  return res;
}

unsigned PrimeEvictionPolicy::GarbageCollect(const PrimeTable::HotspotBuckets& eb, PrimeTable* me) {
  unsigned res = 0;
  // bool should_print = (eb.key_hash % 128) == 0;

  // based on tests - it's more efficient to pass regular buckets to gc.
  // stash buckets are filled last so much smaller change they have expired items.
  string scratch;
  unsigned num_buckets =
      std::min<unsigned>(PrimeTable::HotspotBuckets::kRegularBuckets, eb.num_buckets);
  for (unsigned i = 0; i < num_buckets; ++i) {
    auto bucket_it = eb.at(i);
    for (; !bucket_it.is_done(); ++bucket_it) {
      if (bucket_it->second.HasExpire()) {
        string_view key = bucket_it->first.GetSlice(&scratch);
        ++checked_;
        auto [prime_it, exp_it] = db_slice_->ExpireIfNeeded(
            cntx_, DbSlice::Iterator(bucket_it, StringOrView::FromView(key)));
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
  last_slot_it += (PrimeTable::kSlotNum - 1);
  if (!last_slot_it.is_done()) {
    // don't evict sticky items
    if (last_slot_it->first.IsSticky()) {
      return 0;
    }

    DbTable* table = db_slice_->GetDBTable(cntx_.db_index);
    auto& lt = table->trans_locks;
    string scratch;
    string_view key = last_slot_it->first.GetSlice(&scratch);
    // do not evict locked keys
    if (lt.Find(LockTag(key)).has_value())
      return 0;

    // log the evicted keys to journal.
    if (auto journal = db_slice_->shard_owner()->journal(); journal) {
      ArgSlice delete_args(&key, 1);
      journal->RecordEntry(0, journal::Op::EXPIRED, cntx_.db_index, 1, ClusterConfig::KeySlot(key),
                           make_pair("DEL", delete_args), false);
    }

    db_slice_->PerformDeletion(DbSlice::Iterator(last_slot_it, StringOrView::FromView(key)), table);

    ++evicted_;
  }
  me->ShiftRight(bucket_it);

  return 1;
}

}  // namespace

#define ADD(x) (x) += o.x

DbStats& DbStats::operator+=(const DbStats& o) {
  constexpr size_t kDbSz = sizeof(DbStats) - sizeof(DbTableStats);
  static_assert(kDbSz == 32);

  DbTableStats::operator+=(o);

  ADD(key_count);
  ADD(expire_count);
  ADD(bucket_count);
  ADD(table_mem_usage);

  return *this;
}

SliceEvents& SliceEvents::operator+=(const SliceEvents& o) {
  static_assert(sizeof(SliceEvents) == 112, "You should update this function with new fields");

  ADD(evicted_keys);
  ADD(hard_evictions);
  ADD(expired_keys);
  ADD(garbage_collected);
  ADD(stash_unloaded);
  ADD(bumpups);
  ADD(garbage_checked);
  ADD(hits);
  ADD(misses);
  ADD(mutations);
  ADD(insertion_rejections);
  ADD(update);
  ADD(ram_hits);
  ADD(ram_misses);

  return *this;
}

#undef ADD

DbSlice::DbSlice(uint32_t index, bool caching_mode, EngineShard* owner)
    : shard_id_(index),
      caching_mode_(caching_mode),
      owner_(owner),
      client_tracking_map_(owner->memory_resource()) {
  db_arr_.emplace_back();
  CreateDb(0);
  expire_base_[0] = expire_base_[1] = 0;
  soft_budget_limit_ = (0.3 * max_memory_limit / shard_set->size());
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

SlotStats DbSlice::GetSlotStats(SlotId sid) const {
  CHECK(db_arr_[0]);
  return db_arr_[0]->slots_stats[sid];
}

void DbSlice::Reserve(DbIndex db_ind, size_t key_size) {
  ActivateDb(db_ind);

  auto& db = db_arr_[db_ind];
  DCHECK(db);

  db->prime.Reserve(key_size);
}

DbSlice::AutoUpdater::AutoUpdater() {
}

DbSlice::AutoUpdater::AutoUpdater(AutoUpdater&& o) {
  *this = std::move(o);
}

DbSlice::AutoUpdater& DbSlice::AutoUpdater::operator=(AutoUpdater&& o) {
  Run();
  fields_ = o.fields_;
  o.Cancel();
  return *this;
}

DbSlice::AutoUpdater::~AutoUpdater() {
  Run();
}

void DbSlice::AutoUpdater::Run() {
  if (fields_.action == DestructorAction::kDoNothing) {
    return;
  }

  // Check that AutoUpdater does not run after a key was removed.
  // If this CHECK() failed for you, it probably means that you deleted a key while having an auto
  // updater in scope. You'll probably want to call Run() (or Cancel() - but be careful).
  DCHECK(IsValid(fields_.db_slice->db_arr_[fields_.db_ind]->prime.Find(fields_.key)));

  DCHECK(fields_.action == DestructorAction::kRun);
  CHECK_NE(fields_.db_slice, nullptr);

  fields_.db_slice->PostUpdate(fields_.db_ind, fields_.it, fields_.key, fields_.orig_heap_size);
  Cancel();  // Reset to not run again
}

void DbSlice::AutoUpdater::Cancel() {
  this->fields_ = {};
}

DbSlice::AutoUpdater::AutoUpdater(const Fields& fields) : fields_(fields) {
  DCHECK(fields_.action == DestructorAction::kRun);
  DCHECK(IsValid(fields.it));
  fields_.orig_heap_size = fields.it->second.MallocUsed();
}

DbSlice::AddOrFindResult& DbSlice::AddOrFindResult::operator=(ItAndUpdater&& o) {
  it = o.it;
  exp_it = o.exp_it;
  is_new = false;
  post_updater = std::move(o).post_updater;
  return *this;
}

DbSlice::ItAndUpdater DbSlice::FindAndFetchMutable(const Context& cntx, string_view key) {
  return std::move(FindMutableInternal(cntx, key, std::nullopt, LoadExternalMode::kLoad).value());
}

DbSlice::ItAndUpdater DbSlice::FindMutable(const Context& cntx, string_view key) {
  return std::move(
      FindMutableInternal(cntx, key, std::nullopt, LoadExternalMode::kDontLoad).value());
}

OpResult<DbSlice::ItAndUpdater> DbSlice::FindMutable(const Context& cntx, string_view key,
                                                     unsigned req_obj_type) {
  return FindMutableInternal(cntx, key, req_obj_type, LoadExternalMode::kDontLoad);
}

OpResult<DbSlice::ItAndUpdater> DbSlice::FindAndFetchMutable(const Context& cntx, string_view key,
                                                             unsigned req_obj_type) {
  return FindMutableInternal(cntx, key, req_obj_type, LoadExternalMode::kLoad);
}

OpResult<DbSlice::ItAndUpdater> DbSlice::FindMutableInternal(const Context& cntx, string_view key,
                                                             std::optional<unsigned> req_obj_type,
                                                             LoadExternalMode load_mode) {
  auto res = FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kMutableStats, load_mode);
  if (!res.ok()) {
    return res.status();
  }

  auto it = Iterator(res->it, StringOrView::FromView(key));
  auto exp_it = ExpIterator(res->exp_it, StringOrView::FromView(key));
  PreUpdate(cntx.db_index, it);
  // PreUpdate() might have caused a deletion of `it`
  if (res->it.IsOccupied()) {
    return {{it, exp_it,
             AutoUpdater({.action = AutoUpdater::DestructorAction::kRun,
                          .db_slice = this,
                          .db_ind = cntx.db_index,
                          .it = it,
                          .key = key})}};
  } else {
    return OpStatus::KEY_NOTFOUND;
  }
}

DbSlice::ItAndExpConst DbSlice::FindReadOnly(const Context& cntx, std::string_view key) {
  auto res = FindInternal(cntx, key, std::nullopt, UpdateStatsMode::kReadStats,
                          LoadExternalMode::kDontLoad);
  return {ConstIterator(res->it, StringOrView::FromView(key)),
          ExpConstIterator(res->exp_it, StringOrView::FromView(key))};
}

OpResult<DbSlice::ConstIterator> DbSlice::FindReadOnly(const Context& cntx, string_view key,
                                                       unsigned req_obj_type) {
  auto res = FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kReadStats,
                          LoadExternalMode::kDontLoad);
  if (res.ok()) {
    return ConstIterator(res->it, StringOrView::FromView(key));
  }
  return res.status();
}

OpResult<DbSlice::ConstIterator> DbSlice::FindAndFetchReadOnly(const Context& cntx,
                                                               std::string_view key,
                                                               unsigned req_obj_type) {
  auto res =
      FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kReadStats, LoadExternalMode::kLoad);
  if (res.ok()) {
    return ConstIterator(res->it, StringOrView::FromView(key));
  }
  return res.status();
}

OpResult<DbSlice::PrimeItAndExp> DbSlice::FindInternal(const Context& cntx, std::string_view key,
                                                       std::optional<unsigned> req_obj_type,
                                                       UpdateStatsMode stats_mode,
                                                       LoadExternalMode load_mode) {
  if (!IsDbValid(cntx.db_index)) {
    return OpStatus::KEY_NOTFOUND;
  }

  DbSlice::PrimeItAndExp res;
  auto& db = *db_arr_[cntx.db_index];
  res.it = db.prime.Find(key);

  absl::Cleanup update_stats_on_miss = [&]() {
    switch (stats_mode) {
      case UpdateStatsMode::kMutableStats:
        events_.mutations++;
        break;
      case UpdateStatsMode::kReadStats:
        events_.misses++;
        break;
    }
  };

  if (!IsValid(res.it)) {
    return OpStatus::KEY_NOTFOUND;
  }

  if (req_obj_type.has_value() && res.it->second.ObjType() != req_obj_type.value()) {
    return OpStatus::WRONG_TYPE;
  }

  if (TieredStorage* tiered = shard_owner()->tiered_storage();
      tiered && load_mode == LoadExternalMode::kLoad) {
    if (res.it->second.IsExternal()) {
      // We need to move fetched_items because we preempt and some other
      // transaction might conclude and clear the fetched_items_ with OnCbFinish()
      auto tmp = std::move(fetched_items_);
      // Load reads data from disk therefore we will preempt in this function.
      // We will update the iterator if it changed during the preemption
      res.it = tiered->Load(cntx.db_index, res.it, key);
      if (!IsValid(res.it)) {
        return OpStatus::KEY_NOTFOUND;
      }
      events_.ram_misses++;
      fetched_items_ = std::move(tmp);
    } else {
      if (res.it->second.HasIoPending()) {
        tiered->CancelIo(cntx.db_index, res.it);
      }
      events_.ram_hits++;
    }
    res.it->first.SetTouched(true);
  }

  FiberAtomicGuard fg;
  if (res.it->second.HasExpire()) {  // check expiry state
    res = ExpireIfNeeded(cntx, res.it);
    if (!IsValid(res.it)) {
      return OpStatus::KEY_NOTFOUND;
    }
  }

  if (caching_mode_ && IsValid(res.it)) {
    if (!change_cb_.empty()) {
      auto bump_cb = [&](PrimeTable::bucket_iterator bit) {
        DVLOG(2) << "Running callbacks for key " << key << " in dbid " << cntx.db_index;
        for (const auto& ccb : change_cb_) {
          ccb.second(cntx.db_index, bit);
        }
      };
      db.prime.CVCUponBump(change_cb_.back().first, res.it, bump_cb);
    }
    auto bump_it = db.prime.BumpUp(res.it, PrimeBumpPolicy{fetched_items_});
    if (bump_it != res.it) {  // the item was bumped
      res.it = bump_it;
      ++events_.bumpups;
    }
    fetched_items_.insert(res.it->first.AsRef());
  }

  db.top_keys.Touch(key);

  std::move(update_stats_on_miss).Cancel();
  switch (stats_mode) {
    case UpdateStatsMode::kMutableStats:
      events_.mutations++;
      break;
    case UpdateStatsMode::kReadStats:
      events_.hits++;
      if (ClusterConfig::IsEnabled()) {
        db.slots_stats[ClusterConfig::KeySlot(key)].total_reads++;
      }
      break;
  }
  return res;
}

OpResult<pair<DbSlice::ConstIterator, unsigned>> DbSlice::FindFirstReadOnly(const Context& cntx,
                                                                            ArgSlice args,
                                                                            int req_obj_type) {
  DCHECK(!args.empty());

  for (unsigned i = 0; i < args.size(); ++i) {
    string_view s = args[i];
    OpResult<ConstIterator> res = FindReadOnly(cntx, s, req_obj_type);
    if (res)
      return make_pair(res.value(), i);
    if (res.status() != OpStatus::KEY_NOTFOUND)
      return res.status();
  }

  VLOG(2) << "FindFirst " << args.front() << " not found";
  return OpStatus::KEY_NOTFOUND;
}

OpResult<DbSlice::AddOrFindResult> DbSlice::AddOrFind(const Context& cntx, string_view key) {
  return AddOrFindInternal(cntx, key, LoadExternalMode::kDontLoad);
}

OpResult<DbSlice::AddOrFindResult> DbSlice::AddOrFindAndFetch(const Context& cntx,
                                                              string_view key) {
  return AddOrFindInternal(cntx, key, LoadExternalMode::kLoad);
}

OpResult<DbSlice::AddOrFindResult> DbSlice::AddOrFindInternal(const Context& cntx, string_view key,
                                                              LoadExternalMode load_mode) {
  DCHECK(IsDbValid(cntx.db_index));

  DbTable& db = *db_arr_[cntx.db_index];
  auto res = FindInternal(cntx, key, std::nullopt, UpdateStatsMode::kMutableStats, load_mode);

  if (res.ok()) {
    Iterator it(res->it, StringOrView::FromView(key));
    ExpIterator exp_it(res->exp_it, StringOrView::FromView(key));
    PreUpdate(cntx.db_index, it);
    // PreUpdate() might have caused a deletion of `it`
    if (res->it.IsOccupied()) {
      return DbSlice::AddOrFindResult{
          .it = it,
          .exp_it = exp_it,
          .is_new = false,
          .post_updater = AutoUpdater({.action = AutoUpdater::DestructorAction::kRun,
                                       .db_slice = this,
                                       .db_ind = cntx.db_index,
                                       .it = it,
                                       .key = key})};
    } else {
      res = OpStatus::KEY_NOTFOUND;
    }
  }
  auto status = res.status();
  CHECK(status == OpStatus::KEY_NOTFOUND || status == OpStatus::OUT_OF_MEMORY) << status;

  // It's a new entry.
  DVLOG(2) << "Running callbacks for key " << key << " in dbid " << cntx.db_index;
  for (const auto& ccb : change_cb_) {
    ccb.second(cntx.db_index, key);
  }

  // In case we are loading from rdb file or replicating we want to disable conservative memory
  // checks (inside PrimeEvictionPolicy::CanGrow) and reject insertions only after we pass max
  // memory limit. When loading a snapshot created by the same server configuration (memory and
  // number of shards) we will create a different dash table segment directory tree, because the
  // tree shape is related to the order of entries insertion. Therefore when loading data from
  // snapshot or from replication the conservative memory checks might fail as the new tree might
  // have more segments. Because we dont want to fail loading a snapshot from the same server
  // configuration we disable this checks on loading and replication.
  bool apply_memory_limit =
      !owner_->IsReplica() && !(ServerState::tlocal()->gstate() == GlobalState::LOADING);

  PrimeEvictionPolicy evp{cntx,
                          (bool(caching_mode_) && !owner_->IsReplica()),
                          int64_t(memory_budget_ - key.size()),
                          ssize_t(soft_budget_limit_),
                          this,
                          apply_memory_limit};

  // If we are over limit in non-cache scenario, just be conservative and throw.
  if (apply_memory_limit && !caching_mode_ && evp.mem_budget() < 0) {
    VLOG(2) << "AddOrFind: over limit, budget: " << evp.mem_budget();
    events_.insertion_rejections++;
    return OpStatus::OUT_OF_MEMORY;
  }

  // Fast-path if change_cb_ is empty so we Find or Add using
  // the insert operation: twice more efficient.
  CompactObj co_key{key};
  PrimeIterator it;

  // I try/catch just for sake of having a convenient place to set a breakpoint.
  try {
    it = db.prime.InsertNew(std::move(co_key), PrimeValue{}, evp);
  } catch (bad_alloc& e) {
    VLOG(2) << "AddOrFind2: bad alloc exception, budget: " << evp.mem_budget();
    events_.insertion_rejections++;
    return OpStatus::OUT_OF_MEMORY;
  }

  size_t evicted_obj_bytes = 0;

  // We may still reach the state when our memory usage is above the limit even if we
  // do not add new segments. For example, we have half full segments
  // and we add new objects or update the existing ones and our memory usage grows.
  if (evp.mem_budget() < 0) {
    // TODO(roman): EvictObjects is too aggressive and it's messing with cache hit-rate.
    // The regular eviction policy does a decent job though it may cross the passed limit
    // a little bit. I do not consider it as a serious bug.
    // evicted_obj_bytes = EvictObjects(-evp.mem_budget(), it, &db);
  }

  db.stats.inline_keys += it->first.IsInline();
  AccountObjectMemory(key, it->first.ObjType(), it->first.MallocUsed(), &db);  // Account for key

  DCHECK_EQ(it->second.MallocUsed(), 0UL);  // Make sure accounting is no-op
  it.SetVersion(NextVersion());

  events_.garbage_collected = db.prime.garbage_collected();
  events_.stash_unloaded = db.prime.stash_unloaded();
  events_.evicted_keys += evp.evicted();
  events_.garbage_checked += evp.checked();

  memory_budget_ = evp.mem_budget() + evicted_obj_bytes;
  if (ClusterConfig::IsEnabled()) {
    SlotId sid = ClusterConfig::KeySlot(key);
    db.slots_stats[sid].key_count += 1;
  }

  return DbSlice::AddOrFindResult{
      .it = Iterator(it, StringOrView::FromView(key)),
      .exp_it = ExpIterator{},
      .is_new = true,
      .post_updater = AutoUpdater({.action = AutoUpdater::DestructorAction::kRun,
                                   .db_slice = this,
                                   .db_ind = cntx.db_index,
                                   .it = Iterator(it, StringOrView::FromView(key)),
                                   .key = key})};
}

void DbSlice::ActivateDb(DbIndex db_ind) {
  if (db_arr_.size() <= db_ind)
    db_arr_.resize(db_ind + 1);
  CreateDb(db_ind);
}

bool DbSlice::Del(DbIndex db_ind, Iterator it) {
  if (!IsValid(it)) {
    return false;
  }

  auto& db = db_arr_[db_ind];
  auto obj_type = it->second.ObjType();

  if (doc_del_cb_ && (obj_type == OBJ_JSON || obj_type == OBJ_HASH)) {
    string tmp;
    string_view key = it->first.GetSlice(&tmp);
    DbContext cntx{db_ind, GetCurrentTimeMs()};
    doc_del_cb_(key, cntx, it->second);
  }
  fetched_items_.erase(it->first.AsRef());
  PerformDeletion(it, db.get());

  return true;
}

void DbSlice::FlushSlotsFb(const SlotSet& slot_ids) {
  // Slot deletion can take time as it traverses all the database, hence it runs in fiber.
  // We want to flush all the data of a slot that was added till the time the call to FlushSlotsFb
  // was made. Therefore we delete slots entries with version < next_version
  uint64_t next_version = 0;

  std::string tmp;
  auto del_entry_cb = [&](PrimeTable::iterator it) {
    std::string_view key = it->first.GetSlice(&tmp);
    SlotId sid = ClusterConfig::KeySlot(key);
    if (slot_ids.Contains(sid) && it.GetVersion() < next_version) {
      PerformDeletion(Iterator::FromPrime(it), db_arr_[0].get());
    }
    return true;
  };

  auto on_change = [&](DbIndex db_index, const ChangeReq& req) {
    FiberAtomicGuard fg;
    PrimeTable* table = GetTables(db_index).first;

    auto iterate_bucket = [&](DbIndex db_index, PrimeTable::bucket_iterator it) {
      while (!it.is_done()) {
        del_entry_cb(it);
        ++it;
      }
    };

    if (const PrimeTable::bucket_iterator* bit = req.update()) {
      if (bit->GetVersion() < next_version) {
        iterate_bucket(db_index, *bit);
      }
    } else {
      string_view key = get<string_view>(req.change);
      table->CVCUponInsert(
          next_version, key,
          [this, db_index, next_version, iterate_bucket](PrimeTable::bucket_iterator it) {
            DCHECK_LT(it.GetVersion(), next_version);
            iterate_bucket(db_index, it);
          });
    }
  };
  next_version = RegisterOnChange(std::move(on_change));

  ServerState& etl = *ServerState::tlocal();
  PrimeTable* pt = &db_arr_[0]->prime;
  PrimeTable::Cursor cursor;
  uint64_t i = 0;
  do {
    PrimeTable::Cursor next = pt->Traverse(cursor, del_entry_cb);
    ++i;
    cursor = next;
    if (i % 100 == 0) {
      ThisFiber::Yield();
    }

  } while (cursor && etl.gstate() != GlobalState::SHUTTING_DOWN);

  UnregisterOnChange(next_version);

  etl.DecommitMemory(ServerState::kDataHeap);
}

void DbSlice::FlushSlots(SlotRanges slot_ranges) {
  SlotSet slot_set(slot_ranges);
  InvalidateSlotWatches(slot_set);
  fb2::Fiber("flush_slots", [this, slot_set = std::move(slot_set)]() mutable {
    FlushSlotsFb(slot_set);
  }).Detach();
}

void DbSlice::FlushDbIndexes(const std::vector<DbIndex>& indexes) {
  // TODO: to add preeemptiveness by yielding inside clear.
  DbTableArray flush_db_arr(db_arr_.size());
  for (DbIndex index : indexes) {
    auto& db = db_arr_[index];
    CHECK(db);
    InvalidateDbWatches(index);
    flush_db_arr[index] = std::move(db);

    CreateDb(index);
    std::swap(db_arr_[index]->trans_locks, flush_db_arr[index]->trans_locks);
    if (TieredStorage* tiered = shard_owner()->tiered_storage(); tiered) {
      tiered->CancelAllIos(index);
    }
  }
  CHECK(fetched_items_.empty());
  auto cb = [this, flush_db_arr = std::move(flush_db_arr)]() mutable {
    for (auto& db_ptr : flush_db_arr) {
      if (db_ptr && db_ptr->stats.tiered_entries > 0) {
        for (auto it = db_ptr->prime.begin(); it != db_ptr->prime.end(); ++it) {
          if (it->second.IsExternal())
            PerformDeletion(Iterator::FromPrime(it), db_ptr.get());
        }

        DCHECK_EQ(0u, db_ptr->stats.tiered_entries);
        db_ptr.reset();
      }
    }
    flush_db_arr.clear();
    ServerState::tlocal()->DecommitMemory(ServerState::kDataHeap | ServerState::kBackingHeap |
                                          ServerState::kGlibcmalloc);
  };

  fb2::Fiber("flush_dbs", std::move(cb)).Detach();
}

void DbSlice::FlushDb(DbIndex db_ind) {
  // clear client tracking map.
  client_tracking_map_.clear();

  if (db_ind != kDbAll) {
    // Flush a single database if a specific index is provided
    FlushDbIndexes({db_ind});
    return;
  }

  std::vector<DbIndex> indexes;
  indexes.reserve(db_arr_.size());
  for (DbIndex i = 0; i < db_arr_.size(); ++i) {
    if (db_arr_[i]) {
      indexes.push_back(i);
    }
  }

  FlushDbIndexes(indexes);
}

void DbSlice::AddExpire(DbIndex db_ind, Iterator main_it, uint64_t at) {
  uint64_t delta = at - expire_base_[0];  // TODO: employ multigen expire updates.
  CHECK(db_arr_[db_ind]->expire.Insert(main_it->first.AsRef(), ExpirePeriod(delta)).second);
  main_it->second.SetExpire(true);
}

bool DbSlice::RemoveExpire(DbIndex db_ind, Iterator main_it) {
  if (main_it->second.HasExpire()) {
    CHECK_EQ(1u, db_arr_[db_ind]->expire.Erase(main_it->first));
    main_it->second.SetExpire(false);
    return true;
  }
  return false;
}

// Returns true if a state has changed, false otherwise.
bool DbSlice::UpdateExpire(DbIndex db_ind, Iterator it, uint64_t at) {
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
    auto [it, _] = db.mcflag.Insert(std::move(key), flag);
    it->second = flag;
  }
}

uint32_t DbSlice::GetMCFlag(DbIndex db_ind, const PrimeKey& key) const {
  auto& db = *db_arr_[db_ind];
  auto it = db.mcflag.Find(key);
  if (it.is_done()) {
    LOG(ERROR) << "Internal error, inconsistent state, mcflag should be present but not found "
               << key.ToString();
    return 0;
  }
  return it->second;
}

OpResult<DbSlice::ItAndUpdater> DbSlice::AddNew(const Context& cntx, string_view key,
                                                PrimeValue obj, uint64_t expire_at_ms) {
  auto op_result = AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms, false);
  RETURN_ON_BAD_STATUS(op_result);
  auto& res = *op_result;
  CHECK(res.is_new);

  return DbSlice::ItAndUpdater{
      .it = res.it, .exp_it = res.exp_it, .post_updater = std::move(res.post_updater)};
}

pair<int64_t, int64_t> DbSlice::ExpireParams::Calculate(int64_t now_ms) const {
  if (persist)
    return {0, 0};
  int64_t msec = (unit == TimeUnit::SEC) ? value * 1000 : value;
  int64_t now_msec = now_ms;
  int64_t rel_msec = absolute ? msec - now_msec : msec;
  return make_pair(rel_msec, now_msec + rel_msec);
}

OpResult<int64_t> DbSlice::UpdateExpire(const Context& cntx, Iterator prime_it,
                                        ExpIterator expire_it, const ExpireParams& params) {
  constexpr uint64_t kPersistValue = 0;
  DCHECK(params.IsDefined());
  DCHECK(IsValid(prime_it));
  // If this need to persist, then only set persist value and return
  if (params.persist) {
    RemoveExpire(cntx.db_index, prime_it);
    return kPersistValue;
  }

  auto [rel_msec, abs_msec] = params.Calculate(cntx.time_now_ms);
  if (rel_msec > kMaxExpireDeadlineSec * 1000) {
    return OpStatus::OUT_OF_RANGE;
  }

  if (rel_msec <= 0) {  // implicit - don't persist
    CHECK(Del(cntx.db_index, prime_it));
    return -1;
  } else if (IsValid(expire_it) && !params.persist) {
    auto current = ExpireTime(expire_it);
    if (params.expire_options & ExpireFlags::EXPIRE_NX) {
      return OpStatus::SKIPPED;
    }
    if ((params.expire_options & ExpireFlags::EXPIRE_LT) && current <= abs_msec) {
      return OpStatus::SKIPPED;
    } else if ((params.expire_options & ExpireFlags::EXPIRE_GT) && current >= abs_msec) {
      return OpStatus::SKIPPED;
    }

    expire_it->second = FromAbsoluteTime(abs_msec);
    return abs_msec;
  } else {
    if (params.expire_options & ExpireFlags::EXPIRE_XX) {
      return OpStatus::SKIPPED;
    }
    AddExpire(cntx.db_index, prime_it, abs_msec);
    return abs_msec;
  }
}

OpResult<DbSlice::AddOrFindResult> DbSlice::AddOrUpdateInternal(const Context& cntx,
                                                                std::string_view key,
                                                                PrimeValue obj,
                                                                uint64_t expire_at_ms,
                                                                bool force_update) {
  DCHECK(!obj.IsRef());

  auto op_result = AddOrFind(cntx, key);
  RETURN_ON_BAD_STATUS(op_result);

  auto& res = *op_result;
  if (!res.is_new && !force_update)  // have not inserted.
    return op_result;

  auto& db = *db_arr_[cntx.db_index];
  auto& it = res.it;

  it->second = std::move(obj);

  if (expire_at_ms) {
    it->second.SetExpire(true);
    uint64_t delta = expire_at_ms - expire_base_[0];
    if (IsValid(res.exp_it) && force_update) {
      res.exp_it->second = ExpirePeriod(delta);
    } else {
      auto exp_it = db.expire.InsertNew(it->first.AsRef(), ExpirePeriod(delta));
      res.exp_it = ExpIterator(exp_it, StringOrView::FromView(key));
    }
  }

  return op_result;
}

OpResult<DbSlice::AddOrFindResult> DbSlice::AddOrUpdate(const Context& cntx, string_view key,
                                                        PrimeValue obj, uint64_t expire_at_ms) {
  return AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms, true);
}

size_t DbSlice::DbSize(DbIndex db_ind) const {
  DCHECK_LT(db_ind, db_array_size());

  if (IsDbValid(db_ind)) {
    return db_arr_[db_ind]->prime.size();
  }
  return 0;
}

bool DbSlice::Acquire(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
  if (lock_args.args.empty()) {  // Can be empty for NO_KEY_TRANSACTIONAL commands.
    return true;
  }
  DCHECK_GT(lock_args.key_step, 0u);

  auto& lt = db_arr_[lock_args.db_index]->trans_locks;
  bool lock_acquired = true;

  if (lock_args.args.size() == 1) {
    LockTag tag(lock_args.args.front());
    lock_acquired = lt.Acquire(tag, mode);
    uniq_keys_ = {string_view(tag)};  // needed only for tests.
  } else {
    uniq_keys_.clear();

    for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
      LockTag tag(lock_args.args[i]);
      if (uniq_keys_.insert(string_view(tag)).second) {
        lock_acquired &= lt.Acquire(tag, mode);
      }
    }
  }

  DVLOG(2) << "Acquire " << IntentLock::ModeName(mode) << " for " << lock_args.args[0]
           << " has_acquired: " << lock_acquired;

  return lock_acquired;
}

void DbSlice::ReleaseNormalized(IntentLock::Mode mode, DbIndex db_index, LockTag tag) {
  DVLOG(2) << "Release " << IntentLock::ModeName(mode) << " "
           << " for " << string_view(tag);

  auto& lt = db_arr_[db_index]->trans_locks;
  lt.Release(tag, mode);
}

void DbSlice::Release(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
  if (lock_args.args.empty()) {  // Can be empty for NO_KEY_TRANSACTIONAL commands.
    return;
  }

  DVLOG(2) << "Release " << IntentLock::ModeName(mode) << " for " << lock_args.args[0];
  if (lock_args.args.size() == 1) {
    string_view key = lock_args.args.front();
    ReleaseNormalized(mode, lock_args.db_index, LockTag{key});
  } else {
    auto& lt = db_arr_[lock_args.db_index]->trans_locks;
    uniq_keys_.clear();
    for (size_t i = 0; i < lock_args.args.size(); i += lock_args.key_step) {
      LockTag tag(lock_args.args[i]);
      if (uniq_keys_.insert(string_view(tag)).second) {
        lt.Release(tag, mode);
      }
    }
  }
  uniq_keys_.clear();
}

bool DbSlice::CheckLock(IntentLock::Mode mode, DbIndex dbid, string_view key) const {
  const auto& lt = db_arr_[dbid]->trans_locks;
  LockTag tag(key);

  auto lock = lt.Find(tag);
  if (lock) {
    return lock->Check(mode);
  }
  return true;
}

void DbSlice::PreUpdate(DbIndex db_ind, Iterator it) {
  FiberAtomicGuard fg;

  DVLOG(2) << "Running callbacks in dbid " << db_ind;
  for (const auto& ccb : change_cb_) {
    ccb.second(db_ind, ChangeReq{it.GetInnerIt()});
  }

  it.GetInnerIt().SetVersion(NextVersion());
}

void DbSlice::PostUpdate(DbIndex db_ind, Iterator it, std::string_view key, size_t orig_size) {
  int64_t delta = static_cast<int64_t>(it->second.MallocUsed()) - static_cast<int64_t>(orig_size);
  AccountObjectMemory(key, it->second.ObjType(), delta, GetDBTable(db_ind));

  auto& db = *db_arr_[db_ind];
  auto& watched_keys = db.watched_keys;
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

  ++events_.update;

  if (ClusterConfig::IsEnabled()) {
    db.slots_stats[ClusterConfig::KeySlot(key)].total_writes += 1;
  }

  SendInvalidationTrackingMessage(key);
}

DbSlice::ItAndExp DbSlice::ExpireIfNeeded(const Context& cntx, Iterator it) {
  auto res = ExpireIfNeeded(cntx, it.GetInnerIt());
  return {.it = Iterator::FromPrime(res.it), .exp_it = ExpIterator::FromPrime(res.exp_it)};
}

DbSlice::PrimeItAndExp DbSlice::ExpireIfNeeded(const Context& cntx, PrimeIterator it) {
  if (!it->second.HasExpire()) {
    LOG(ERROR) << "Invalid call to ExpireIfNeeded";
    return {it, ExpireIterator{}};
  }

  auto& db = db_arr_[cntx.db_index];

  auto expire_it = db->expire.Find(it->first);

  // TODO: Accept Iterator instead of PrimeIterator, as this might save an allocation below.
  string scratch;
  string_view key = it->first.GetSlice(&scratch);

  if (IsValid(expire_it)) {
    // TODO: to employ multi-generation update of expire-base and the underlying values.
    time_t expire_time = ExpireTime(expire_it);

    // Never do expiration on replica or if expiration is disabled.
    if (time_t(cntx.time_now_ms) < expire_time || owner_->IsReplica() || !expire_allowed_)
      return {it, expire_it};
  } else {
    LOG(ERROR) << "Internal error, entry " << it->first.ToString()
               << " not found in expire table, db_index: " << cntx.db_index
               << ", expire table size: " << db->expire.size()
               << ", prime table size: " << db->prime.size() << util::fb2::GetStacktrace();
  }
  // Replicate expiry
  if (auto journal = owner_->journal(); journal) {
    RecordExpiry(cntx.db_index, key);
  }

  auto obj_type = it->second.ObjType();
  if (doc_del_cb_ && (obj_type == OBJ_JSON || obj_type == OBJ_HASH)) {
    doc_del_cb_(key, cntx, it->second);
  }

  PerformDeletion(Iterator(it, StringOrView::FromView(key)),
                  ExpIterator(expire_it, StringOrView::FromView(key)), db.get());
  ++events_.expired_keys;

  return {PrimeIterator{}, ExpireIterator{}};
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

void DbSlice::FlushChangeToEarlierCallbacks(DbIndex db_ind, Iterator it, uint64_t upper_bound) {
  FiberAtomicGuard fg;
  uint64_t bucket_version = it.GetVersion();
  // change_cb_ is ordered by version.
  DVLOG(2) << "Running callbacks in dbid " << db_ind << " with bucket_version=" << bucket_version
           << ", upper_bound=" << upper_bound;
  for (const auto& ccb : change_cb_) {
    uint64_t cb_version = ccb.first;
    DCHECK_LE(cb_version, upper_bound);
    if (cb_version == upper_bound) {
      return;
    }
    if (bucket_version < cb_version) {
      ccb.second(db_ind, ChangeReq{it.GetInnerIt()});
    }
  }
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

  std::string stash;

  auto cb = [&](ExpireIterator it) {
    auto key = it->first.GetSlice(&stash);
    if (!CheckLock(IntentLock::EXCLUSIVE, cntx.db_index, key))
      return;

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

int32_t DbSlice::GetNextSegmentForEviction(int32_t segment_id, DbIndex db_ind) const {
  // wraps around if we reached the end
  return db_arr_[db_ind]->prime.NextSeg((size_t)segment_id) %
         db_arr_[db_ind]->prime.GetSegmentCount();
}

void DbSlice::ScheduleForOffloadStep(DbIndex db_indx, size_t increase_goal_bytes) {
  VLOG(1) << "ScheduleForOffloadStep increase_goal_bytes:"
          << strings::HumanReadableNumBytes(increase_goal_bytes);
  DCHECK(shard_owner()->tiered_storage());
  FiberAtomicGuard guard;
  PrimeTable& pt = db_arr_[db_indx]->prime;

  static PrimeTable::Cursor cursor;

  size_t offloaded_bytes = 0;
  auto cb = [&](PrimeIterator it) {
    // TBD check we did not lock it for future transaction

    // If the item is cold (not touched) and can be externalized, schedule it for offload.
    if (increase_goal_bytes > offloaded_bytes && !(it->first.WasTouched()) &&
        TieredStorage::CanExternalizeEntry(it)) {
      shard_owner()->tiered_storage()->ScheduleOffload(db_indx, it);
      if (it->second.HasIoPending()) {
        offloaded_bytes += it->second.Size();
        VLOG(2) << "ScheduleOffload bytes:" << offloaded_bytes;
      }
    }
    it->first.SetTouched(false);
  };

  // Traverse a single segment every time this function is called.
  for (int i = 0; i < 60; ++i) {
    cursor = pt.TraverseBySegmentOrder(cursor, cb);
  }
}

void DbSlice::FreeMemWithEvictionStep(DbIndex db_ind, size_t increase_goal_bytes) {
  DCHECK(!owner_->IsReplica());
  if ((!caching_mode_) || !expire_allowed_ || !GetFlag(FLAGS_enable_heartbeat_eviction))
    return;

  auto max_eviction_per_hb = GetFlag(FLAGS_max_eviction_per_heartbeat);
  auto max_segment_to_consider = GetFlag(FLAGS_max_segment_to_consider);

  auto time_start = absl::GetCurrentTimeNanos();
  auto& db_table = db_arr_[db_ind];
  int32_t num_segments = db_table->prime.GetSegmentCount();
  int32_t num_buckets = PrimeTable::Segment_t::kTotalBuckets;
  int32_t num_slots = PrimeTable::Segment_t::kSlotNum;

  size_t used_memory_after;
  size_t evicted = 0;
  string tmp;
  int32_t starting_segment_id = rand() % num_segments;
  size_t used_memory_before = owner_->UsedMemory();
  vector<string> keys_to_journal;

  {
    FiberAtomicGuard guard;
    for (int32_t slot_id = num_slots - 1; slot_id >= 0; --slot_id) {
      for (int32_t bucket_id = num_buckets - 1; bucket_id >= 0; --bucket_id) {
        // pick a random segment to start with in each eviction,
        // as segment_id does not imply any recency, and random selection should be fair enough
        int32_t segment_id = starting_segment_id;
        for (size_t num_seg_visited = 0; num_seg_visited < max_segment_to_consider;
             ++num_seg_visited, segment_id = GetNextSegmentForEviction(segment_id, db_ind)) {
          const auto& bucket = db_table->prime.GetSegment(segment_id)->GetBucket(bucket_id);
          if (bucket.IsEmpty())
            continue;

          if (!bucket.IsBusy(slot_id))
            continue;

          auto evict_it = db_table->prime.GetIterator(segment_id, bucket_id, slot_id);
          if (evict_it->first.IsSticky())
            continue;

          // check if the key is locked by looking up transaction table.
          const auto& lt = db_table->trans_locks;
          string_view key = evict_it->first.GetSlice(&tmp);
          if (lt.Find(LockTag(key)).has_value())
            continue;

          if (auto journal = owner_->journal(); journal) {
            keys_to_journal.push_back(string(key));
          }

          PerformDeletion(Iterator(evict_it, StringOrView::FromView(key)), db_table.get());
          ++evicted;

          used_memory_after = owner_->UsedMemory();
          // returns when whichever condition is met first
          if ((evicted == max_eviction_per_hb) ||
              (used_memory_before - used_memory_after >= increase_goal_bytes))
            goto finish;
        }
      }
    }
  }

finish:
  // send the deletion to the replicas.
  // fiber preemption could happen in this phase.
  if (auto journal = owner_->journal(); journal) {
    for (string_view key : keys_to_journal) {
      ArgSlice delete_args(&key, 1);
      journal->RecordEntry(0, journal::Op::EXPIRED, db_ind, 1, ClusterConfig::KeySlot(key),
                           make_pair("DEL", delete_args), false);
    }
  }

  auto time_finish = absl::GetCurrentTimeNanos();
  events_.evicted_keys += evicted;
  DVLOG(2) << "Memory usage before eviction: " << used_memory_before;
  DVLOG(2) << "Memory usage after eviction: " << used_memory_after;
  DVLOG(2) << "Number of keys evicted / max eviction per hb: " << evicted << "/"
           << max_eviction_per_hb;
  DVLOG(2) << "Eviction time (us): " << (time_finish - time_start) / 1000;
}

void DbSlice::CreateDb(DbIndex db_ind) {
  auto& db = db_arr_[db_ind];
  if (!db) {
    db.reset(new DbTable{owner_->memory_resource(), db_ind});
  }
}

// "it" is the iterator that we just added/updated and it should not be deleted.
// "table" is the instance where we should delete the objects from.
size_t DbSlice::EvictObjects(size_t memory_to_free, Iterator it, DbTable* table) {
  if (owner_->IsReplica()) {
    return 0;
  }
  PrimeTable::Segment_t* segment = table->prime.GetSegment(it.GetInnerIt().segment_id());
  DCHECK(segment);

  constexpr unsigned kNumStashBuckets = PrimeTable::Segment_t::kStashBucketNum;
  constexpr unsigned kNumRegularBuckets = PrimeTable::Segment_t::kBucketNum;

  PrimeTable::bucket_iterator it2(it.GetInnerIt());
  unsigned evicted = 0;
  bool evict_succeeded = false;

  EngineShard* shard = owner_;
  size_t used_memory_start = shard->UsedMemory();

  auto freed_memory_fun = [&] {
    size_t current = shard->UsedMemory();
    return current < used_memory_start ? used_memory_start - current : 0;
  };

  for (unsigned i = 0; !evict_succeeded && i < kNumStashBuckets; ++i) {
    unsigned stash_bid = i + kNumRegularBuckets;
    const auto& bucket = segment->GetBucket(stash_bid);
    if (bucket.IsEmpty())
      continue;

    for (int slot_id = PrimeTable::Segment_t::kSlotNum - 1; slot_id >= 0; --slot_id) {
      if (!bucket.IsBusy(slot_id))
        continue;

      auto evict_it = table->prime.GetIterator(it.GetInnerIt().segment_id(), stash_bid, slot_id);
      // skip the iterator that we must keep or the sticky items.
      if (evict_it == it.GetInnerIt() || evict_it->first.IsSticky())
        continue;

      PerformDeletion(evict_it, table);
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
  for (int slot_id = PrimeTable::Segment_t::kSlotNum - 1; !evict_succeeded && slot_id >= 0;
       --slot_id) {
    for (unsigned i = 0; i < kNumRegularBuckets; ++i) {
      unsigned bid = (it.GetInnerIt().bucket_id() + i) % kNumRegularBuckets;
      const auto& bucket = segment->GetBucket(bid);
      if (!bucket.IsBusy(slot_id))
        continue;

      auto evict_it = table->prime.GetIterator(it.GetInnerIt().segment_id(), bid, slot_id);
      if (evict_it == it.GetInnerIt() || evict_it->first.IsSticky())
        continue;

      PerformDeletion(evict_it, table);
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

void DbSlice::UnregisterConnectionWatches(const ConnectionState::ExecInfo* exec_info) {
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

void DbSlice::InvalidateSlotWatches(const SlotSet& slot_ids) {
  for (const auto& [key, conn_list] : db_arr_[0]->watched_keys) {
    SlotId sid = ClusterConfig::KeySlot(key);
    if (!slot_ids.Contains(sid)) {
      continue;
    }
    for (auto conn_ptr : conn_list) {
      conn_ptr->watched_dirty.store(true, memory_order_relaxed);
    }
  }
}

void DbSlice::SetDocDeletionCallback(DocDeletionCallback ddcb) {
  doc_del_cb_ = std::move(ddcb);
}

void DbSlice::ResetUpdateEvents() {
  events_.update = 0;
}

void DbSlice::ResetEvents() {
  events_ = {};
}

void DbSlice::TrackKeys(const facade::Connection::WeakRef& conn, const ArgSlice& keys) {
  if (conn.IsExpired()) {
    DVLOG(2) << "Connection expired, exiting TrackKey function.";
    return;
  }

  DVLOG(2) << "Start tracking keys for client ID: " << conn.GetClientId()
           << " with thread ID: " << conn.Thread();
  for (auto key : keys) {
    DVLOG(2) << "Inserting client ID " << conn.GetClientId()
             << " into the tracking client set of key " << key;
    client_tracking_map_[key].insert(conn);
  }
}

void DbSlice::SendInvalidationTrackingMessage(std::string_view key) {
  if (client_tracking_map_.empty())
    return;

  auto it = client_tracking_map_.find(key);
  if (it != client_tracking_map_.end()) {
    // notify all the clients.
    auto& client_set = it->second;
    auto cb = [key, client_set = std::move(client_set)](unsigned idx, util::ProactorBase*) {
      for (auto it = client_set.begin(); it != client_set.end(); ++it) {
        if ((unsigned int)it->Thread() != idx)
          continue;
        facade::Connection* conn = it->Get();
        if ((conn != nullptr) && conn->IsTrackingOn()) {
          std::string key_str = {key.begin(), key.end()};
          conn->SendInvalidationMessageAsync({key_str});
        }
      }
    };
    shard_set->pool()->DispatchBrief(std::move(cb));
    // remove this key from the tracking table as the key no longer exists
    client_tracking_map_.erase(key);
  }
}

void DbSlice::RemoveFromTiered(Iterator it, DbIndex index) {
  DbTable* table = GetDBTable(index);
  RemoveFromTiered(it, table);
}

void DbSlice::RemoveFromTiered(Iterator it, DbTable* table) {
  DbTableStats& stats = table->stats;
  PrimeValue& pv = it->second;
  if (pv.IsExternal()) {
    TieredStorage* tiered = shard_owner()->tiered_storage();
    tiered->Free(it.GetInnerIt(), &stats);
  }
  if (pv.HasIoPending()) {
    TieredStorage* tiered = shard_owner()->tiered_storage();
    tiered->CancelIo(table->index, it.GetInnerIt());
  }
}

void DbSlice::PerformDeletion(PrimeIterator del_it, DbTable* table) {
  return PerformDeletion(Iterator::FromPrime(del_it), table);
}

void DbSlice::PerformDeletion(Iterator del_it, ExpIterator exp_it, DbTable* table) {
  if (!exp_it.is_done()) {
    table->expire.Erase(exp_it.GetInnerIt());
  }

  if (del_it->second.HasFlag()) {
    if (table->mcflag.Erase(del_it->first) == 0) {
      LOG(ERROR) << "Internal error, inconsistent state, mcflag should be present but not found "
                 << del_it->first.ToString();
    }
  }

  DbTableStats& stats = table->stats;
  const PrimeValue& pv = del_it->second;
  RemoveFromTiered(del_it, table);

  if (pv.IsExternal() && shard_owner()->tiered_storage_v2()) {
    shard_owner()->tiered_storage_v2()->Delete(del_it.key(), &del_it->second);
  }

  size_t value_heap_size = pv.MallocUsed();
  stats.inline_keys -= del_it->first.IsInline();
  AccountObjectMemory(del_it.key(), del_it->first.ObjType(), -del_it->first.MallocUsed(),
                      table);                                                // Key
  AccountObjectMemory(del_it.key(), pv.ObjType(), -value_heap_size, table);  // Value
  if (pv.ObjType() == OBJ_HASH && pv.Encoding() == kEncodingListPack) {
    --stats.listpack_blob_cnt;
  } else if (pv.ObjType() == OBJ_ZSET && pv.Encoding() == OBJ_ENCODING_LISTPACK) {
    --stats.listpack_blob_cnt;
  }

  if (ClusterConfig::IsEnabled()) {
    SlotId sid = ClusterConfig::KeySlot(del_it.key());
    table->slots_stats[sid].key_count -= 1;
  }

  table->prime.Erase(del_it.GetInnerIt());
  SendInvalidationTrackingMessage(del_it.key());
}

void DbSlice::PerformDeletion(Iterator del_it, DbTable* table) {
  ExpIterator exp_it;
  if (del_it->second.HasExpire()) {
    exp_it = ExpIterator::FromPrime(table->expire.Find(del_it->first));
    DCHECK(!exp_it.is_done());
  }

  PerformDeletion(del_it, exp_it, table);
}

void DbSlice::OnCbFinish() {
  // TBD update bumpups logic we can not clear now after cb finish as cb can preempt
  // btw what do we do with inline?
  fetched_items_.clear();
}

}  // namespace dfly
