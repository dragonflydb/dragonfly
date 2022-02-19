// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include "core/intent_lock.h"
#include "core/op_status.h"
#include "server/common_types.h"
#include "server/table.h"

namespace util {
class ProactorBase;
}

namespace dfly {

struct DbStats {
  // number of active keys.
  size_t key_count = 0;

  // number of keys that have expiry deadline.
  size_t expire_count = 0;

  // number of buckets in dictionary (key capacity)
  size_t bucket_count = 0;

  // Number of inline keys.
  size_t inline_keys = 0;

  // Object memory usage besides hash-table capacity.
  // Applies for any non-inline objects.
  size_t obj_memory_usage = 0;

  // Memory used by dictionaries.
  size_t table_mem_usage = 0;

  DbStats& operator+=(const DbStats& o);
};

struct SliceEvents {
  // Number of eviction events.
  size_t evicted_keys = 0;
  size_t expired_keys = 0;

  SliceEvents& operator+=(const SliceEvents& o);
};

class DbSlice {
  struct InternalDbStats {
    // Number of inline keys.
    uint64_t inline_keys = 0;

    // Object memory usage besides hash-table capacity.
    // Applies for any non-inline objects.
    size_t obj_memory_usage = 0;
  };

  DbSlice(const DbSlice&) = delete;
  void operator=(const DbSlice&) = delete;

 public:
  struct Stats {
    DbStats db;
    SliceEvents events;
  };

  DbSlice(uint32_t index, EngineShard* owner);
  ~DbSlice();

  // Activates `db_ind` database if it does not exist (see ActivateDb below).
  void Reserve(DbIndex db_ind, size_t key_size);

  Stats GetStats() const;

  //! UpdateExpireClock updates the expire clock for this db slice.
  //! Must be a wall clock so we could replicate it betweeen machines.
  void UpdateExpireClock(uint64_t now_ms) {
    now_ms_ = now_ms;
  }

  // returns wall clock in millis as it has been set via UpdateExpireClock.
  uint64_t Now() const {
    return now_ms_;
  }

  OpResult<MainIterator> Find(DbIndex db_index, std::string_view key, unsigned req_obj_type) const;

  // Returns (value, expire) dict entries if key exists, null if it does not exist or has expired.
  std::pair<MainIterator, ExpireIterator> FindExt(DbIndex db_ind, std::string_view key) const;

  // Returns dictEntry, args-index if found, KEY_NOTFOUND otherwise.
  // If multiple keys are found, returns the first index in the ArgSlice.
  OpResult<std::pair<MainIterator, unsigned>> FindFirst(DbIndex db_index, const ArgSlice& args);

  // Return .second=true if insertion ocurred, false if we return the existing key.
  std::pair<MainIterator, bool> AddOrFind(DbIndex db_ind, std::string_view key);

  // Either adds or removes (if at == 0) expiry. Returns true if a change was made.
  // Does not change expiry if at != 0 and expiry already exists.
  bool Expire(DbIndex db_ind, MainIterator main_it, uint64_t at);

  // Adds a new entry. Requires: key does not exist in this slice.
  void AddNew(DbIndex db_ind, std::string_view key, PrimeValue obj, uint64_t expire_at_ms);

  // Adds a new entry if a key does not exists. Returns true if insertion took place,
  // false otherwise. expire_at_ms equal to 0 - means no expiry.
  bool AddIfNotExist(DbIndex db_ind, std::string_view key, PrimeValue obj, uint64_t expire_at_ms);

  // Creates a database with index `db_ind`. If such database exists does nothing.
  void ActivateDb(DbIndex db_ind);

  bool Del(DbIndex db_ind, MainIterator it);

  constexpr static DbIndex kDbAll = 0xFFFF;

  /**
   * @brief Flushes the database of index db_ind. If kDbAll is passed then flushes all the
   * databases.
   *
   */
  size_t FlushDb(DbIndex db_ind);

  ShardId shard_id() const {
    return shard_id_;
  }

  bool Acquire(IntentLock::Mode m, const KeyLockArgs& lock_args);

  void Release(IntentLock::Mode m, const KeyLockArgs& lock_args);
  void Release(IntentLock::Mode m, DbIndex db_index, std::string_view key, unsigned count);

  // Returns true if all keys can be locked under m. Does not lock them though.
  bool CheckLock(IntentLock::Mode m, const KeyLockArgs& lock_args) const;

  size_t db_array_size() const {
    return db_arr_.size();
  }

  bool IsDbValid(DbIndex id) const {
    return id < db_arr_.size() && bool(db_arr_[id]);
  }

  std::pair<PrimeTable*, ExpireTable*> GetTables(DbIndex id) {
    return std::pair<PrimeTable*, ExpireTable*>(&db_arr_[id]->prime_table,
                                                &db_arr_[id]->expire_table);
  }

  // Returns existing keys count in the db.
  size_t DbSize(DbIndex db_ind) const;

  // Callback functions called upon writing to the existing key.
  void PreUpdate(DbIndex db_ind, MainIterator it);
  void PostUpdate(DbIndex db_ind, MainIterator it);

  // Check whether 'it' has not expired. Returns it if it's still valid. Otherwise, erases it
  // from both tables and return MainIterator{}.
  std::pair<MainIterator, ExpireIterator> ExpireIfNeeded(DbIndex db_ind, MainIterator it) const;

  // Current version of this slice.
  // We maintain a shared versioning scheme for all databases in the slice.
  uint64_t version() const { return version_; }

  // ChangeReq - describes the change to the table. If MainIterator is defined then
  // it's an update on the existing entry, otherwise if string_view is defined then
  // it's a new key that is going to be added to the table.
  using ChangeReq = std::variant<MainIterator, std::string_view>;

  using ChangeCallback = std::function<void(DbIndex, const ChangeReq&)>;

  //! Registers the callback to be called for each change.
  //! Returns the registration id which is also the unique version of the dbslice
  //! at a time of the call.
  uint64_t RegisterOnChange(ChangeCallback cb);

  //! Unregisters the callback.
  void UnregisterOnChange(uint64_t id);

 private:
  void CreateDb(DbIndex index);

  uint64_t NextVersion() {
    return version_++;
  }

  ShardId shard_id_;

  EngineShard* owner_;

  uint64_t now_ms_ = 0;  // Used for expire logic, represents a real clock.
  uint64_t version_ = 1;  // Used to version entries in the PrimeTable.
  mutable SliceEvents events_;  // we may change this even for const operations.

  using LockTable = absl::flat_hash_map<std::string, IntentLock>;

  struct DbWrapper {
    PrimeTable prime_table;
    ExpireTable expire_table;
    LockTable lock_table;

    mutable InternalDbStats stats;

    explicit DbWrapper(std::pmr::memory_resource* mr)
    : prime_table(4, detail::PrimeTablePolicy{}, mr) {
    }
  };

  std::vector<std::unique_ptr<DbWrapper>> db_arr_;

  // Used in temporary computations in Acquire/Release.
  absl::flat_hash_set<std::string_view> uniq_keys_;

  std::vector<std::pair<uint64_t, ChangeCallback>> change_cb_;
};

}  // namespace dfly
