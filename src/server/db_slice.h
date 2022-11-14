// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include "facade/op_status.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/table.h"

namespace util {
class ProactorBase;
}

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

  SliceEvents& operator+=(const SliceEvents& o);
};

class DbSlice {
  DbSlice(const DbSlice&) = delete;
  void operator=(const DbSlice&) = delete;

 public:
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

  struct ExpireParams {
    int64_t value = INT64_MIN;  // undefined

    bool absolute = false;
    TimeUnit unit = TimeUnit::SEC;
    bool persist = false;

    bool IsDefined() const {
      return persist || value > INT64_MIN;
    }
  };

  DbSlice(uint32_t index, bool caching_mode, EngineShard* owner);
  ~DbSlice();

  // Activates `db_ind` database if it does not exist (see ActivateDb below).
  void Reserve(DbIndex db_ind, size_t key_size);

  // Returns statistics for the whole db slice. A bit heavy operation.
  Stats GetStats() const;

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
  time_t ExpireTime(ExpireIterator it) const {
    return it.is_done() ? 0 : expire_base_[0] + it->second.duration_ms();
  }

  ExpirePeriod FromAbsoluteTime(uint64_t time_ms) const {
    return ExpirePeriod{time_ms - expire_base_[0]};
  }

  OpResult<PrimeIterator> Find(const Context& cntx, std::string_view key,
                               unsigned req_obj_type) const;

  // Returns (value, expire) dict entries if key exists, null if it does not exist or has expired.
  std::pair<PrimeIterator, ExpireIterator> FindExt(const Context& cntx, std::string_view key) const;

  // Returns (iterator, args-index) if found, KEY_NOTFOUND otherwise.
  // If multiple keys are found, returns the first index in the ArgSlice.
  OpResult<std::pair<PrimeIterator, unsigned>> FindFirst(const Context& cntx, ArgSlice args);

  // Return .second=true if insertion occurred, false if we return the existing key.
  // throws: bad_alloc is insertion could not happen due to out of memory.
  std::pair<PrimeIterator, bool> AddOrFind(const Context& cntx,
                                           std::string_view key) noexcept(false);

  std::tuple<PrimeIterator, ExpireIterator, bool> AddOrFind2(const Context& cntx,
                                                             std::string_view key) noexcept(false);

  // Same as AddEntry, but overwrites in case entry exists.
  // Returns second=true if insertion took place.
  std::pair<PrimeIterator, bool> AddOrUpdate(const Context& cntx, std::string_view key,
                                             PrimeValue obj, uint64_t expire_at_ms) noexcept(false);

  // Returns second=true if insertion took place, false otherwise.
  // expire_at_ms equal to 0 - means no expiry.
  // throws: bad_alloc is insertion could not happen due to out of memory.
  std::pair<PrimeIterator, bool> AddEntry(const Context& cntx, std::string_view key, PrimeValue obj,
                                          uint64_t expire_at_ms) noexcept(false);

  // Adds a new entry. Requires: key does not exist in this slice.
  // Returns the iterator to the newly added entry.
  // throws: bad_alloc is insertion could not happen due to out of memory.
  PrimeIterator AddNew(const Context& cntx, std::string_view key, PrimeValue obj,
                       uint64_t expire_at_ms) noexcept(false);

  facade::OpStatus UpdateExpire(const Context& cntx, PrimeIterator prime_it, ExpireIterator exp_it,
                                const ExpireParams& params);

  // Either adds or removes (if at == 0) expiry. Returns true if a change was made.
  // Does not change expiry if at != 0 and expiry already exists.
  bool UpdateExpire(DbIndex db_ind, PrimeIterator main_it, uint64_t at);

  void SetMCFlag(DbIndex db_ind, PrimeKey key, uint32_t flag);
  uint32_t GetMCFlag(DbIndex db_ind, const PrimeKey& key) const;

  // Creates a database with index `db_ind`. If such database exists does nothing.
  void ActivateDb(DbIndex db_ind);

  bool Del(DbIndex db_ind, PrimeIterator it);

  constexpr static DbIndex kDbAll = 0xFFFF;

  /**
   * @brief Flushes the database of index db_ind. If kDbAll is passed then flushes all the
   * databases.
   *
   */
  void FlushDb(DbIndex db_ind);

  EngineShard* shard_owner() {
    return owner_;
  }

  ShardId shard_id() const {
    return shard_id_;
  }

  bool Acquire(IntentLock::Mode m, const KeyLockArgs& lock_args);

  void Release(IntentLock::Mode m, const KeyLockArgs& lock_args);

  void Release(IntentLock::Mode m, DbIndex db_index, std::string_view key, unsigned count) {
    db_arr_[db_index]->Release(m, key, count);
  }

  // Returns true if all keys can be locked under m. Does not lock them though.
  bool CheckLock(IntentLock::Mode m, const KeyLockArgs& lock_args) const;

  size_t db_array_size() const {
    return db_arr_.size();
  }

  bool IsDbValid(DbIndex id) const {
    return id < db_arr_.size() && bool(db_arr_[id]);
  }

  DbTable* GetDBTable(DbIndex id) {
    return db_arr_[id].get();
  }

  std::pair<PrimeTable*, ExpireTable*> GetTables(DbIndex id) {
    return std::pair<PrimeTable*, ExpireTable*>(&db_arr_[id]->prime, &db_arr_[id]->expire);
  }

  // Returns existing keys count in the db.
  size_t DbSize(DbIndex db_ind) const;

  // Callback functions called upon writing to the existing key.
  void PreUpdate(DbIndex db_ind, PrimeIterator it);
  void PostUpdate(DbIndex db_ind, PrimeIterator it, std::string_view key,
                  bool existing_entry = true);

  DbTableStats* MutableStats(DbIndex db_ind) {
    return &db_arr_[db_ind]->stats;
  }

  // Check whether 'it' has not expired. Returns it if it's still valid. Otherwise, erases it
  // from both tables and return PrimeIterator{}.
  std::pair<PrimeIterator, ExpireIterator> ExpireIfNeeded(const Context& cntx,
                                                          PrimeIterator it) const;

  // Iterate over all expire table entries and delete expired.
  void ExpireAllIfNeeded();

  // Current version of this slice.
  // We maintain a shared versioning scheme for all databases in the slice.
  uint64_t version() const {
    return version_;
  }

  using ChangeCallback = std::function<void(DbIndex, const ChangeReq&)>;

  //! Registers the callback to be called for each change.
  //! Returns the registration id which is also the unique version of the dbslice
  //! at a time of the call.
  uint64_t RegisterOnChange(ChangeCallback cb);

  //! Unregisters the callback.
  void UnregisterOnChange(uint64_t id);

  struct DeleteExpiredStats {
    uint32_t deleted = 0;         // number of deleted items due to expiry (less than traversed).
    uint32_t traversed = 0;       // number of traversed items that have ttl bit
    size_t survivor_ttl_sum = 0;  // total sum of ttl of survivors (traversed - deleted).
  };

  // Deletes some amount of possible expired items.
  DeleteExpiredStats DeleteExpiredStep(const Context& cntx, unsigned count);
  void FreeMemWithEvictionStep(DbIndex db_indx, size_t increase_goal_bytes);

  const DbTableArray& databases() const {
    return db_arr_;
  }

  void TEST_EnableCacheMode() {
    caching_mode_ = 1;
  }

  void RegisterWatchedKey(DbIndex db_indx, std::string_view key,
                          ConnectionState::ExecInfo* exec_info);

  // Unregisted all watched key entries for connection.
  void UnregisterConnectionWatches(ConnectionState::ExecInfo* exec_info);

  // Invalidate all watched keys in database. Used on FLUSH.
  void InvalidateDbWatches(DbIndex db_indx);

 private:
  std::pair<PrimeIterator, bool> AddOrUpdateInternal(const Context& cntx, std::string_view key,
                                                     PrimeValue obj, uint64_t expire_at_ms,
                                                     bool force_update) noexcept(false);

  void CreateDb(DbIndex index);
  size_t EvictObjects(size_t memory_to_free, PrimeIterator it, DbTable* table);

  uint64_t NextVersion() {
    return version_++;
  }

 private:
  ShardId shard_id_;
  uint8_t caching_mode_ : 1;

  EngineShard* owner_;

  time_t expire_base_[2];  // Used for expire logic, represents a real clock.

  uint64_t version_ = 1;  // Used to version entries in the PrimeTable.
  ssize_t memory_budget_ = SSIZE_MAX;
  size_t bytes_per_object_ = 0;
  size_t soft_budget_limit_ = 0;

  mutable SliceEvents events_;  // we may change this even for const operations.

  DbTableArray db_arr_;

  // Used in temporary computations in Acquire/Release.
  absl::flat_hash_set<std::string_view> uniq_keys_;

  // ordered from the smallest to largest version.
  std::vector<std::pair<uint64_t, ChangeCallback>> change_cb_;
};

}  // namespace dfly
