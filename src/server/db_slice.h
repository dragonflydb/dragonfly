// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/mi_memory_resource.h"
#include "core/string_or_view.h"
#include "facade/dragonfly_connection.h"
#include "facade/op_status.h"
#include "server/cluster/slot_set.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/table.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

using facade::OpResult;

struct DbStats : public DbTableStats {
  // number of active keys.
  size_t key_count = 0;

  // number of keys that have expiry deadline.
  size_t expire_count = 0;

  // number of buckets in dictionary (key capacity)
  size_t bucket_count = 0;

  // Memory used by dictionaries.
  size_t table_mem_usage = 0;

  using DbTableStats::operator+=;
  using DbTableStats::operator=;

  DbStats& operator+=(const DbStats& o);
};

struct SliceEvents {
  // Number of eviction events.
  size_t evicted_keys = 0;

  // evictions that were performed when we have a negative memory budget.
  size_t hard_evictions = 0;
  size_t expired_keys = 0;
  size_t garbage_checked = 0;
  size_t garbage_collected = 0;
  size_t stash_unloaded = 0;
  size_t bumpups = 0;  // how many bump-upds we did.

  // hits/misses on keys
  size_t hits = 0;
  size_t misses = 0;
  size_t mutations = 0;

  // ram hit/miss when tiering is enabled
  size_t ram_hits = 0;
  size_t ram_cool_hits = 0;
  size_t ram_misses = 0;

  // how many insertions were rejected due to OOM.
  size_t insertion_rejections = 0;

  // how many updates and insertions of keys between snapshot intervals
  size_t update = 0;

  SliceEvents& operator+=(const SliceEvents& o);
};

class DbSlice {
  DbSlice(const DbSlice&) = delete;
  void operator=(const DbSlice&) = delete;

 public:
  // Auto-laundering iterator wrapper. Laundering means re-finding keys if they moved between
  // buckets.
  template <typename T> class IteratorT {
   public:
    IteratorT() = default;

    IteratorT(T it, StringOrView key)
        : it_(it), fiber_epoch_(util::fb2::FiberSwitchEpoch()), key_(std::move(key)) {
    }

    static IteratorT FromPrime(T it) {
      if (!IsValid(it)) {
        return IteratorT();
      }

      std::string key;
      it->first.GetString(&key);
      return IteratorT(it, StringOrView::FromString(std::move(key)));
    }

    IteratorT(const IteratorT& o) = default;
    IteratorT(IteratorT&& o) = default;
    IteratorT& operator=(const IteratorT& o) = default;
    IteratorT& operator=(IteratorT&& o) = default;

    // Do NOT store this iterator in a variable, as it will not be laundered automatically.
    T GetInnerIt() const {
      LaunderIfNeeded();
      return it_;
    }

    auto operator->() const {
      return GetInnerIt().operator->();
    }

    auto is_done() const {
      return GetInnerIt().is_done();
    }

    std::string_view key() const {
      return key_.view();
    }

    auto IsOccupied() const {
      return GetInnerIt().IsOccupied();
    }

    auto GetVersion() const {
      return GetInnerIt().GetVersion();
    }

   private:
    void LaunderIfNeeded() const;  // const is a lie

    mutable T it_;
    mutable uint64_t fiber_epoch_ = 0;
    StringOrView key_;
  };

  using Iterator = IteratorT<PrimeIterator>;
  using ConstIterator = IteratorT<PrimeConstIterator>;
  using ExpIterator = IteratorT<ExpireIterator>;
  using ExpConstIterator = IteratorT<ExpireConstIterator>;

  class AutoUpdater {
   public:
    AutoUpdater();
    AutoUpdater(const AutoUpdater& o) = delete;
    AutoUpdater& operator=(const AutoUpdater& o) = delete;
    AutoUpdater(AutoUpdater&& o);
    AutoUpdater& operator=(AutoUpdater&& o);
    ~AutoUpdater();

    void Run();
    void Cancel();

   private:
    enum class DestructorAction {
      kDoNothing,
      kRun,
    };

    // Wrap members in a struct to auto generate operator=
    struct Fields {
      DestructorAction action = DestructorAction::kDoNothing;

      DbSlice* db_slice = nullptr;
      DbIndex db_ind = 0;
      Iterator it;
      std::string_view key;

      // The following fields are calculated at init time
      size_t orig_heap_size = 0;
    };

    AutoUpdater(const Fields& fields);

    friend class DbSlice;

    Fields fields_ = {};
  };

  struct Stats {
    // DbStats db;
    std::vector<DbStats> db_stats;
    SliceEvents events;
    size_t small_string_bytes = 0;
  };

  using Context = DbContext;

  // ChangeReq - describes the change to the table.
  struct ChangeReq {
    // If iterator is set then it's an update to the existing bucket.
    // Otherwise (string_view is set) then it's a new key that is going to be added to the table.
    std::variant<PrimeTable::bucket_iterator, std::string_view> change;

    ChangeReq(PrimeTable::bucket_iterator it) : change(it) {
    }
    ChangeReq(std::string_view key) : change(key) {
    }

    const PrimeTable::bucket_iterator* update() const {
      return std::get_if<PrimeTable::bucket_iterator>(&change);
    }
  };

  // Called before deleting an element to notify the search indices.
  using DocDeletionCallback =
      std::function<void(std::string_view, const Context&, const PrimeValue& pv)>;

  struct ExpireParams {
    bool IsDefined() const {
      return persist || value > INT64_MIN;
    }

    static int64_t Cap(int64_t value, TimeUnit unit);

    // Calculate relative and absolue timepoints.
    std::pair<int64_t, int64_t> Calculate(uint64_t now_msec, bool cap) const;

    // Return true if relative expiration is in the past
    bool IsExpired(uint64_t now_msec) const {
      return Calculate(now_msec, false).first < 0;
    }

   public:
    int64_t value = INT64_MIN;  // undefined
    TimeUnit unit = TimeUnit::SEC;

    bool absolute = false;
    bool persist = false;
    int32_t expire_options = 0;  // ExpireFlags
  };

  DbSlice(uint32_t index, bool cache_mode, EngineShard* owner);
  ~DbSlice();

  // Activates `db_ind` database if it does not exist (see ActivateDb below).
  void Reserve(DbIndex db_ind, size_t key_size);

  // Returns statistics for the whole db slice. A bit heavy operation.
  Stats GetStats() const;

  // Returns slot statistics for db 0.
  SlotStats GetSlotStats(SlotId sid) const;

  void UpdateExpireBase(uint64_t now, unsigned generation) {
    expire_base_[generation & 1] = now;
  }

  // From time to time DbSlice is set with a new set of params needed to estimate its
  // memory usage.
  void SetCachedParams(int64_t budget, size_t bytes_per_object) {
    memory_budget_ = budget;
    bytes_per_object_ = bytes_per_object;
  }

  ssize_t memory_budget() const {
    return memory_budget_;
  }

  size_t bytes_per_object() const {
    return bytes_per_object_;
  }

  // returns absolute time of the expiration.
  time_t ExpireTime(const ExpConstIterator& it) const {
    return ExpireTime(it.GetInnerIt());
  }

  time_t ExpireTime(const ExpIterator& it) const {
    return ExpireTime(it.GetInnerIt());
  }

  time_t ExpireTime(const ExpireConstIterator& it) const {
    return it.is_done() ? 0 : expire_base_[0] + it->second.duration_ms();
  }

  ExpirePeriod FromAbsoluteTime(uint64_t time_ms) const {
    return ExpirePeriod{time_ms - expire_base_[0]};
  }

  struct ItAndUpdater {
    Iterator it;
    ExpIterator exp_it;
    AutoUpdater post_updater;

    bool IsValid() const {
      return !it.is_done();
    }
  };

  ItAndUpdater FindMutable(const Context& cntx, std::string_view key);
  OpResult<ItAndUpdater> FindMutable(const Context& cntx, std::string_view key,
                                     unsigned req_obj_type);

  struct ItAndExpConst {
    ConstIterator it;
    ExpConstIterator exp_it;
  };

  ItAndExpConst FindReadOnly(const Context& cntx, std::string_view key) const;
  OpResult<ConstIterator> FindReadOnly(const Context& cntx, std::string_view key,
                                       unsigned req_obj_type) const;

  struct AddOrFindResult {
    Iterator it;
    ExpIterator exp_it;
    bool is_new = false;
    AutoUpdater post_updater;

    AddOrFindResult& operator=(ItAndUpdater&& o);
  };

  OpResult<AddOrFindResult> AddOrFind(const Context& cntx, std::string_view key);

  // Same as AddOrSkip, but overwrites in case entry exists.
  OpResult<AddOrFindResult> AddOrUpdate(const Context& cntx, std::string_view key, PrimeValue obj,
                                        uint64_t expire_at_ms);

  // Adds a new entry. Requires: key does not exist in this slice.
  // Returns the iterator to the newly added entry.
  // Returns OpStatus::OUT_OF_MEMORY if bad_alloc is thrown
  OpResult<ItAndUpdater> AddNew(const Context& cntx, std::string_view key, PrimeValue obj,
                                uint64_t expire_at_ms);

  // Update entry expiration. Return epxiration timepoint in abs milliseconds, or -1 if the entry
  // already expired and was deleted;
  facade::OpResult<int64_t> UpdateExpire(const Context& cntx, Iterator prime_it, ExpIterator exp_it,
                                         const ExpireParams& params);

  // Adds expiry information.
  void AddExpire(DbIndex db_ind, Iterator main_it, uint64_t at);

  // Removes the corresponing expiry information if exists.
  // Returns true if expiry existed (and removed).
  bool RemoveExpire(DbIndex db_ind, Iterator main_it);

  // Either adds or removes (if at == 0) expiry. Returns true if a change was made.
  // Does not change expiry if at != 0 and expiry already exists.
  bool UpdateExpire(DbIndex db_ind, Iterator main_it, uint64_t at);

  void SetMCFlag(DbIndex db_ind, PrimeKey key, uint32_t flag);
  uint32_t GetMCFlag(DbIndex db_ind, const PrimeKey& key) const;

  // Creates a database with index `db_ind`. If such database exists does nothing.
  void ActivateDb(DbIndex db_ind);

  // Delete a key referred by its iterator.
  void PerformDeletion(Iterator del_it, DbTable* table);

  // Deletes the iterator. The iterator must be valid.
  void Del(Context cntx, Iterator it);

  constexpr static DbIndex kDbAll = 0xFFFF;

  // Flushes db_ind or all databases if kDbAll is passed
  void FlushDb(DbIndex db_ind);

  // Flushes the data of given slot ranges.
  void FlushSlots(cluster::SlotRanges slot_ranges);

  EngineShard* shard_owner() const {
    return owner_;
  }

  ShardId shard_id() const {
    return shard_id_;
  }

  void OnCbFinish();

  bool Acquire(IntentLock::Mode m, const KeyLockArgs& lock_args);
  void Release(IntentLock::Mode m, const KeyLockArgs& lock_args);

  // Returns true if the key can be locked under m. Does not lock.
  bool CheckLock(IntentLock::Mode mode, DbIndex dbid, uint64_t fp) const;
  bool CheckLock(IntentLock::Mode mode, DbIndex dbid, std::string_view key) const {
    return CheckLock(mode, dbid, LockTag(key).Fingerprint());
  }

  size_t db_array_size() const {
    return db_arr_.size();
  }

  bool IsDbValid(DbIndex id) const {
    return id < db_arr_.size() && bool(db_arr_[id]);
  }

  DbTable* GetDBTable(DbIndex id) {
    return db_arr_[id].get();
  }

  const DbTable* GetDBTable(DbIndex id) const {
    return db_arr_[id].get();
  }

  std::pair<PrimeTable*, ExpireTable*> GetTables(DbIndex id) {
    return std::pair<PrimeTable*, ExpireTable*>(&db_arr_[id]->prime, &db_arr_[id]->expire);
  }

  // Returns existing keys count in the db.
  size_t DbSize(DbIndex db_ind) const;

  DbTableStats* MutableStats(DbIndex db_ind) {
    return &db_arr_[db_ind]->stats;
  }

  // Check whether 'it' has not expired. Returns it if it's still valid. Otherwise, erases it
  // from both tables and return Iterator{}.
  struct ItAndExp {
    Iterator it;
    ExpIterator exp_it;
  };
  ItAndExp ExpireIfNeeded(const Context& cntx, Iterator it) const;

  // Iterate over all expire table entries and delete expired.
  void ExpireAllIfNeeded();

  // Current version of this slice.
  // We maintain a shared versioning scheme for all databases in the slice.
  uint64_t version() const {
    return version_;
  }

  size_t table_memory() const {
    return table_memory_;
  }

  size_t entries_count() const {
    return entries_count_;
  }

  using ChangeCallback = std::function<void(DbIndex, const ChangeReq&)>;

  //! Registers the callback to be called for each change.
  //! Returns the registration id which is also the unique version of the dbslice
  //! at a time of the call.
  uint64_t RegisterOnChange(ChangeCallback cb);

  bool HasRegisteredCallbacks() const {
    return !change_cb_.empty();
  }

  // Call registered callbacks with version less than upper_bound.
  void FlushChangeToEarlierCallbacks(DbIndex db_ind, Iterator it, uint64_t upper_bound);

  //! Unregisters the callback.
  void UnregisterOnChange(uint64_t id);

  struct DeleteExpiredStats {
    uint32_t deleted = 0;         // number of deleted items due to expiry (less than traversed).
    uint32_t deleted_bytes = 0;   // total bytes of deleted items.
    uint32_t traversed = 0;       // number of traversed items that have ttl bit
    size_t survivor_ttl_sum = 0;  // total sum of ttl of survivors (traversed - deleted).
  };

  // Deletes some amount of possible expired items.
  DeleteExpiredStats DeleteExpiredStep(const Context& cntx, unsigned count);

  // Evicts items with dynamically allocated data from the primary table.
  // Does not shrink tables.
  // Returnes number of (elements,bytes) freed due to evictions.
  std::pair<uint64_t, size_t> FreeMemWithEvictionStep(DbIndex db_indx, size_t starting_segment_id,
                                                      size_t increase_goal_bytes);

  int32_t GetNextSegmentForEviction(int32_t segment_id, DbIndex db_ind) const;

  const DbTableArray& databases() const {
    return db_arr_;
  }

  void TEST_EnableCacheMode() {
    cache_mode_ = 1;
  }

  bool IsCacheMode() const {
    // During loading time we never bump elements.
    return cache_mode_ && !load_in_progress_;
  }

  void SetLoadInProgress(bool in_progress) {
    load_in_progress_ = in_progress;
  }

  // Test hook to inspect last locked keys.
  const auto& TEST_GetLastLockedFps() const {
    return uniq_fps_;
  }

  void RegisterWatchedKey(DbIndex db_indx, std::string_view key,
                          ConnectionState::ExecInfo* exec_info);

  // Unregisted all watched key entries for connection.
  void UnregisterConnectionWatches(const ConnectionState::ExecInfo* exec_info);

  void SetDocDeletionCallback(DocDeletionCallback ddcb);

  // Resets the event counter for updates/insertions
  void ResetUpdateEvents();

  // Resets events_ member. Used by CONFIG RESETSTAT
  void ResetEvents();

  // Controls the expiry/eviction state. The server may enter states where
  // Both evictions and expiries will be stopped for a short period of time.
  void SetExpireAllowed(bool is_allowed) {
    expire_allowed_ = is_allowed;
  }

  // Track keys for the client represented by the the weak reference to its connection.
  void TrackKey(const facade::Connection::WeakRef& conn_ref, std::string_view key) {
    client_tracking_map_[key].insert(conn_ref);
  }

  // Does not check for non supported events. Callers must parse the string and reject it
  // if it's not empty and not EX.
  void SetNotifyKeyspaceEvents(std::string_view notify_keyspace_events);

  bool WillBlockOnJournalWrite() const {
    return block_counter_.IsBlocked();
  }

  LocalBlockingCounter* BlockingCounter() {
    return &block_counter_;
  }

 private:
  void PreUpdate(DbIndex db_ind, Iterator it, std::string_view key);
  void PostUpdate(DbIndex db_ind, Iterator it, std::string_view key, size_t orig_size);

  bool DelEmptyPrimeValue(const Context& cntx, Iterator it);

  OpResult<AddOrFindResult> AddOrUpdateInternal(const Context& cntx, std::string_view key,
                                                PrimeValue obj, uint64_t expire_at_ms,
                                                bool force_update);

  void FlushSlotsFb(const cluster::SlotSet& slot_ids);
  void FlushDbIndexes(const std::vector<DbIndex>& indexes);

  // Invalidate all watched keys in database. Used on FLUSH.
  void InvalidateDbWatches(DbIndex db_indx);

  // Invalidate all watched keys for given slots. Used on FlushSlots.
  void InvalidateSlotWatches(const cluster::SlotSet& slot_ids);

  // Clear tiered storage entries for the specified indices.
  void ClearOffloadedEntries(absl::Span<const DbIndex> indices, const DbTableArray& db_arr);

  void PerformDeletion(Iterator del_it, ExpIterator exp_it, DbTable* table);
  void PerformDeletion(PrimeIterator del_it, DbTable* table);

  // Send invalidation message to the clients that are tracking the change to a key.
  void SendInvalidationTrackingMessage(std::string_view key);

  void CreateDb(DbIndex index);

  enum class UpdateStatsMode {
    kReadStats,
    kMutableStats,
  };

  struct PrimeItAndExp {
    PrimeIterator it;
    ExpireIterator exp_it;
  };

  PrimeItAndExp ExpireIfNeeded(const Context& cntx, PrimeIterator it) const;

  OpResult<AddOrFindResult> AddOrFindInternal(const Context& cntx, std::string_view key);

  OpResult<PrimeItAndExp> FindInternal(const Context& cntx, std::string_view key,
                                       std::optional<unsigned> req_obj_type,
                                       UpdateStatsMode stats_mode) const;
  OpResult<ItAndUpdater> FindMutableInternal(const Context& cntx, std::string_view key,
                                             std::optional<unsigned> req_obj_type);

  uint64_t NextVersion() {
    return version_++;
  }

  void CallChangeCallbacks(DbIndex id, std::string_view key, const ChangeReq& cr) const;

  // We need this because registered callbacks might yield and when they do so we want
  // to avoid Heartbeat or Flushing the db.
  // This counter protects us against this case.
  mutable LocalBlockingCounter block_counter_;

  ShardId shard_id_;
  uint8_t cache_mode_ : 1;
  uint8_t load_in_progress_ : 1;

  EngineShard* owner_;

  time_t expire_base_[2];  // Used for expire logic, represents a real clock.
  bool expire_allowed_ = true;

  uint64_t version_ = 1;  // Used to version entries in the PrimeTable.
  ssize_t memory_budget_ = SSIZE_MAX / 2;
  size_t bytes_per_object_ = 0;
  size_t soft_budget_limit_ = 0;
  size_t table_memory_ = 0;
  uint64_t entries_count_ = 0;

  mutable SliceEvents events_;  // we may change this even for const operations.

  DbTableArray db_arr_;

  struct FpHasher {
    size_t operator()(uint64_t val) const {
      return val;
    }
  };

  // Used in temporary computations in Acquire/Release.
  mutable absl::flat_hash_set<uint64_t, FpHasher> uniq_fps_;

  // ordered from the smallest to largest version.
  std::list<std::pair<uint64_t, ChangeCallback>> change_cb_;

  // Used in temporary computations in Find item and CbFinish
  // This set is used to hold fingerprints of key accessed during the run of
  // a transaction callback (not the whole transaction).
  // We track them to avoid bumping them again (in any direction) so that the iterators to
  // the fetched keys will not be invalidated. We must do it for atomic operations,
  // for operations that preempt in the middle we have another mechanism -
  // auto laundering iterators, so in case of preemption we do not mind that fetched_items are
  // cleared or changed.
  mutable absl::flat_hash_set<uint64_t, FpHasher> fetched_items_;

  // Registered by shard indices on when first document index is created.
  DocDeletionCallback doc_del_cb_;

  // Record whenever a key expired to DbTable::expired_keys_events_ for keyspace notifications
  bool expired_keys_events_recording_ = true;

  struct Hash {
    size_t operator()(const facade::Connection::WeakRef& c) const {
      return std::hash<uint32_t>()(c.GetClientId());
    }
  };

  // the following type definitions are confusing, and they are for achieving memory
  // usage tracking for client_tracking_map_ data structure through C++'s memory resource and
  // and polymorphic allocator (new C++ features)
  // the declarations below meant to say:
  // absl::flat_hash_map<std::string,
  //                    absl::flat_hash_set<facade::Connection::WeakRef, Hash>> client_tracking_map_
  using HashSetAllocator = PMR_NS::polymorphic_allocator<facade::Connection::WeakRef>;

  using ConnectionHashSet =
      absl::flat_hash_set<facade::Connection::WeakRef, Hash,
                          absl::container_internal::hash_default_eq<facade::Connection::WeakRef>,
                          HashSetAllocator>;

  using AllocatorType = PMR_NS::polymorphic_allocator<std::pair<std::string, ConnectionHashSet>>;

  absl::flat_hash_map<std::string, ConnectionHashSet,
                      absl::container_internal::hash_default_hash<std::string>,
                      absl::container_internal::hash_default_eq<std::string>, AllocatorType>
      client_tracking_map_;

  class PrimeBumpPolicy {
   public:
    PrimeBumpPolicy(absl::flat_hash_set<uint64_t, FpHasher>* items) : fetched_items_(items) {
    }

    // returns true if we can change the object location in dash table.
    bool CanBump(const CompactObj& obj) const {
      if (obj.IsSticky()) {
        return false;
      }
      auto hc = obj.HashCode();
      return fetched_items_->insert(hc).second;
    }

   private:
    mutable absl::flat_hash_set<uint64_t, FpHasher>* fetched_items_;
  };
};

inline bool IsValid(const DbSlice::Iterator& it) {
  return dfly::IsValid(it.GetInnerIt());
}

inline bool IsValid(const DbSlice::ConstIterator& it) {
  return dfly::IsValid(it.GetInnerIt());
}

inline bool IsValid(const DbSlice::ExpIterator& it) {
  return dfly::IsValid(it.GetInnerIt());
}

inline bool IsValid(const DbSlice::ExpConstIterator& it) {
  return dfly::IsValid(it.GetInnerIt());
}

template <typename T> void DbSlice::IteratorT<T>::LaunderIfNeeded() const {
  if (!dfly::IsValid(it_)) {
    return;
  }

  uint64_t current_epoch = util::fb2::FiberSwitchEpoch();
  if (current_epoch != fiber_epoch_) {
    if (!it_.IsOccupied() || it_->first != key_.view()) {
      it_ = it_.owner().Find(key_.view());
    }
    fiber_epoch_ = current_epoch;
  }
}

}  // namespace dfly
