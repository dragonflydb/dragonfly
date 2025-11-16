// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/mi_memory_resource.h"
#include "core/string_or_view.h"
#include "facade/dragonfly_connection.h"
#include "facade/op_status.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/table.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

namespace cluster {
class SlotRanges;
class SlotSet;
}  // namespace cluster

using facade::OpResult;

struct DbStats : public DbTableStats {
  // number of active keys.
  size_t key_count = 0;

  // number of keys that have expiry deadline.
  size_t expire_count = 0;

  // total number of slots in prime dictionary (key capacity).
  size_t prime_capacity = 0;

  // total number of slots in prime dictionary (key capacity).
  size_t expire_capacity = 0;

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

  uint64_t huff_encode_total = 0, huff_encode_success = 0;

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
    const T& GetInnerIt() const {
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
    AutoUpdater(AutoUpdater&& o) noexcept;
    AutoUpdater& operator=(AutoUpdater&& o) noexcept;
    ~AutoUpdater();

    // Removes the memory usage attributed to the iterator and resets orig_heap_size.
    // Used when the existing object is overridden by a new one.
    void ReduceHeapUsage();

    void Run();
    void Cancel();

   private:
    // Wrap members in a struct to auto generate operator=
    struct Fields {
      DbSlice* db_slice = nullptr;
      DbIndex db_ind = 0;

      // TODO: remove `it` from ItAndUpdater as it's redundant with respect to this iterator.
      Iterator it;
      std::string_view key;

      // The following fields are calculated at init time
      size_t orig_value_heap_size = 0;
    };

    AutoUpdater(DbIndex db_ind, std::string_view key, const Iterator& it, DbSlice* db_slice);

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
  using ChangeReq = dfly::ChangeReq;

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

  void UpdateMemoryParams(int64_t budget, size_t bytes_per_object) {
    memory_budget_ = budget;
    bytes_per_object_ = bytes_per_object;
  }

  ssize_t memory_budget() const {
    return memory_budget_;
  }

  size_t bytes_per_object() const {
    return bytes_per_object_;
  }

  int64_t ExpireTime(const ExpirePeriod& val) const {
    return expire_base_[0] + val.duration_ms();
  }

  ExpirePeriod FromAbsoluteTime(uint64_t time_ms) const {
    return ExpirePeriod{time_ms - expire_base_[0]};
  }

  struct ItAndUpdater {
    Iterator it;
    ExpIterator exp_it;
    AutoUpdater post_updater;
    bool is_new = false;
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

  // Consider using req_obj_type to specify the type of object you expect.
  // Because it can evaluate to bugs like this:
  // - We already have a key but with another type you expect.
  // - During FindMutable we will not use req_obj_type, so the object type will not be checked.
  // - AddOrFind will return the object with this key but with a different type.
  // - Then you will update this object with a different type, which will lead to an error.
  // If you proved the key type on your own, please add a comment there why don't specify
  // req_obj_type
  OpResult<ItAndUpdater> AddOrFind(const Context& cntx, std::string_view key,
                                   std::optional<unsigned> req_obj_type);

  // Same as AddOrSkip, but overwrites in case entry exists.
  OpResult<ItAndUpdater> AddOrUpdate(const Context& cntx, std::string_view key, PrimeValue obj,
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
  void AddExpire(DbIndex db_ind, const Iterator& main_it, uint64_t at);

  // Removes the corresponing expiry information if exists.
  // Returns true if expiry existed (and removed).
  bool RemoveExpire(DbIndex db_ind, const Iterator& main_it);

  // Either adds or removes (if at == 0) expiry. Returns true if a change was made.
  // Does not change expiry if at != 0 and expiry already exists.
  bool UpdateExpire(DbIndex db_ind, const Iterator& main_it, uint64_t at);

  void SetMCFlag(DbIndex db_ind, PrimeKey key, uint32_t flag);
  uint32_t GetMCFlag(DbIndex db_ind, const PrimeKey& key) const;

  // Creates a database with index `db_ind`. If such database exists does nothing.
  void ActivateDb(DbIndex db_ind);

  // Delete a key referred by its iterator.
  void PerformDeletion(Iterator del_it, DbTable* table);

  // Deletes the iterator. The iterator must be valid.
  void Del(Context cntx, Iterator it);

  // Deletes a key after FindMutable(). Runs post_updater before deletion
  // to update memory accounting while the key is still valid.
  // Takes ownership of it_updater (pass by value with move semantics).
  void DelMutable(Context cntx, ItAndUpdater it_updater);

  constexpr static DbIndex kDbAll = 0xFFFF;

  // Flushes db_ind or all databases if kDbAll is passed
  util::fb2::Fiber FlushDb(DbIndex db_ind);

  // Flushes the data of given slot ranges.
  void FlushSlots(const cluster::SlotRanges& slot_ranges);

  EngineShard* shard_owner() const {
    return owner_;
  }

  ShardId shard_id() const {
    return shard_id_;
  }

  void OnCbFinishBlocking();

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

  auto CopyDBTablePtr(DbIndex id) {
    return db_arr_[id];
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
  // Holds pairs of source and destination cursors for items moved in the dash table
  using MovedItemsVec = std::vector<std::pair<PrimeTable::Cursor, PrimeTable::Cursor>>;
  using MovedCallback = std::function<void(DbIndex, const MovedItemsVec&)>;

  //! Registers the callback to be called for each change.
  //! Returns the registration id which is also the unique version of the dbslice
  //! at a time of the call.
  uint64_t RegisterOnChange(ChangeCallback cb);

  //! Registers the callback to be called after items are moved in table.
  //! Returns the registration id which is also the unique version of the dbslice
  //! at a time of the call.
  uint64_t RegisterOnMove(MovedCallback cb);

  bool HasRegisteredCallbacks() const {
    return !change_cb_.empty();
  }

  // Call registered callbacks with version less than upper_bound.
  void FlushChangeToEarlierCallbacks(DbIndex db_ind, Iterator it, uint64_t upper_bound);

  //! Unregisters the callback.
  void UnregisterOnChange(uint64_t id);

  void UnregisterOnMoved(uint64_t id);

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
  // Returns number of (elements,bytes) freed due to evictions.
  std::pair<uint64_t, size_t> FreeMemWithEvictionStepAtomic(DbIndex db_indx, const Context& cntx,
                                                            size_t starting_segment_id,
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
    return cache_mode_ && (load_ref_count_ == 0);
  }

  void IncrLoadInProgress() {
    ++load_ref_count_;
  }

  void DecrLoadInProgress() {
    --load_ref_count_;
  }

  bool IsLoadRefCountZero() const {
    return load_ref_count_ == 0;
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
    return serialization_latch_.IsBlocked();
  }

  LocalLatch* GetLatch() {
    return &serialization_latch_;
  }

  void StartSampleTopK(DbIndex db_ind, uint32_t min_freq);

  struct SamplingResult {
    std::vector<std::pair<std::string, uint64_t>> top_keys;  // key -> frequency pairs.
    uint64_t total_samples = 0;                              // Total number of keys sampled.
  };
  SamplingResult StopSampleTopK(DbIndex db_ind);

  void StartSampleKeys(DbIndex db_ind);

  // Returns number of unique keys sampled.
  struct UniqueSampleResult {
    uint64_t unique_keys_count = 0;  // Number of unique keys sampled.
    uint64_t total_samples = 0;      // Total number of keys sampled.
  };
  UniqueSampleResult StopSampleKeys(DbIndex db_ind);

  void StartSampleValues(DbIndex db_ind);

  // Returns a histogram of sampled values.
  std::unique_ptr<base::Histogram> StopSampleValues(DbIndex db_ind);

 private:
  void PreUpdateBlocking(DbIndex db_ind, const Iterator& it);
  void PostUpdate(DbIndex db_ind, std::string_view key);

  OpResult<ItAndUpdater> AddOrUpdateInternal(const Context& cntx, std::string_view key,
                                             PrimeValue obj, uint64_t expire_at_ms,
                                             bool force_update);

  void FlushSlotsFb(const cluster::SlotSet& slot_ids);
  util::fb2::Fiber FlushDbIndexes(const std::vector<DbIndex>& indexes);

  // Invalidate all watched keys in database. Used on FLUSH.
  void InvalidateDbWatches(DbIndex db_indx);

  // Invalidate all watched keys for given slots. Used on FlushSlots.
  void InvalidateSlotWatches(const cluster::SlotSet& slot_ids);

  // Clear tiered storage entries for the specified indices. Called during flushing some indices.
  void RemoveOffloadedEntriesFromTieredStorage(absl::Span<const DbIndex> indices,
                                               const DbTableArray& db_arr) const;

  void PerformDeletionAtomic(const Iterator& del_it, const ExpIterator& exp_it, DbTable* table);

  // Queues invalidation message to the clients that are tracking the change to a key.
  void QueueInvalidationTrackingMessageAtomic(std::string_view key);
  void SendQueuedInvalidationMessages();
  void SendQueuedInvalidationMessagesAsync();

  void CreateDb(DbIndex index);

  enum class UpdateStatsMode : uint8_t {
    kReadStats,
    kMutableStats,
  };

  struct PrimeItAndExp {
    PrimeIterator it;
    ExpireIterator exp_it;
  };

  PrimeItAndExp ExpireIfNeeded(const Context& cntx, PrimeIterator it) const;

  OpResult<ItAndUpdater> AddOrFindInternal(const Context& cntx, std::string_view key,
                                           std::optional<unsigned> req_obj_type);

  OpResult<PrimeItAndExp> FindInternal(const Context& cntx, std::string_view key,
                                       std::optional<unsigned> req_obj_type,
                                       UpdateStatsMode stats_mode) const;
  OpResult<ItAndUpdater> FindMutableInternal(const Context& cntx, std::string_view key,
                                             std::optional<unsigned> req_obj_type);

  uint64_t NextVersion() {
    return version_++;
  }

  void CallChangeCallbacks(DbIndex id, const ChangeReq& cr) const;
  void CallMovedCallbacks(DbIndex id, const MovedItemsVec& moved_items);

  // We need this because registered callbacks might yield and when they do so we want
  // to avoid Heartbeat or Flushing the db.
  // This latch protects us against this case.
  mutable LocalLatch serialization_latch_;

  ShardId shard_id_;
  uint8_t cache_mode_ : 1;

  EngineShard* owner_;

  int64_t expire_base_[2];  // Used for expire logic, represents a real clock.
  bool expire_allowed_ = true;

  uint64_t version_ = 1;  // Used to version entries in the PrimeTable.
  uint64_t next_moved_id_ = 1;

  // Estimation of available memory dedicated to this shard.
  // Recalculated periodically by dividing free memory left among all shards equally
  ssize_t memory_budget_ = SSIZE_MAX / 2;
  size_t bytes_per_object_ = 0;

  size_t table_memory_ = 0;
  uint64_t entries_count_ = 0;
  unsigned load_ref_count_ = 0;

  mutable SliceEvents events_;  // we may change this even for const operations.

  DbTableArray db_arr_;

  // key for bump up items pair contains <key hash, db_index>
  using FetchedItemKey = std::pair<uint64_t, DbIndex>;

  struct FpHasher {
    size_t operator()(uint64_t val) const {
      return val;
    }
    size_t operator()(const FetchedItemKey& val) const {
      return val.first;
    }
  };

  // Used in temporary computations in Acquire/Release.
  mutable absl::flat_hash_set<uint64_t, FpHasher> uniq_fps_;

  // ordered from the smallest to largest version.
  std::list<std::pair<uint64_t, ChangeCallback>> change_cb_;

  std::list<std::pair<uint32_t, MovedCallback>> moved_cb_;

  // Used in temporary computations in Find item and CbFinish
  // This set is used to hold fingerprints of key accessed during the run of
  // a transaction callback (not the whole transaction).
  // We track them to avoid bumping them again (in any direction) so that the iterators to
  // the fetched keys will not be invalidated. We must do it for atomic operations,
  // for operations that preempt in the middle we have another mechanism -
  // auto laundering iterators, so in case of preemption we do not mind that fetched_items are
  // cleared or changed.
  mutable absl::flat_hash_set<FetchedItemKey, FpHasher> fetched_items_;

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
  //                    absl::flat_hash_set<facade::Connection::WeakRef, Hash>>
  //                    client_tracking_map_
  using HashSetAllocator = PMR_NS::polymorphic_allocator<facade::Connection::WeakRef>;

  using ConnectionHashSet =
      absl::flat_hash_set<facade::Connection::WeakRef, Hash,
                          absl::container_internal::hash_default_eq<facade::Connection::WeakRef>,
                          HashSetAllocator>;

  using AllocatorType = PMR_NS::polymorphic_allocator<std::pair<std::string, ConnectionHashSet>>;

  using TrackingMap =
      absl::flat_hash_map<std::string, ConnectionHashSet,
                          absl::container_internal::hash_default_hash<std::string>,
                          absl::container_internal::hash_default_eq<std::string>, AllocatorType>;
  TrackingMap client_tracking_map_, pending_send_map_;

  void SendQueuedInvalidationMessagesCb(const TrackingMap& track_map, unsigned idx) const;

  class PrimeBumpPolicy;
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
