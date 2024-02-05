// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "core/expire_period.h"
#include "core/intent_lock.h"
#include "server/cluster/cluster_config.h"
#include "server/conn_context.h"
#include "server/detail/table.h"
#include "server/top_keys.h"

extern "C" {
#include "redis/redis_aux.h"
}
namespace dfly {

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using ExpireTable = DashTable<PrimeKey, ExpirePeriod, detail::ExpireTablePolicy>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid if a different entry in the table was mutated.
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;
using ExpireIterator = ExpireTable::iterator;
using ExpireConstIterator = ExpireTable::const_iterator;

inline bool IsValid(PrimeIterator it) {
  return !it.is_done();
}

inline bool IsValid(ExpireIterator it) {
  return !it.is_done();
}

inline bool IsValid(PrimeConstIterator it) {
  return !it.is_done();
}

inline bool IsValid(ExpireConstIterator it) {
  return !it.is_done();
}

struct SlotStats {
  uint64_t key_count = 0;
  uint64_t total_reads = 0;
  uint64_t total_writes = 0;
  uint64_t memory_bytes = 0;
  SlotStats& operator+=(const SlotStats& o);
};

struct DbTableStats {
  // Number of inline keys.
  uint64_t inline_keys = 0;

  // Object memory usage besides hash-table capacity.
  // Applies for any non-inline objects.
  size_t obj_memory_usage = 0;

  size_t listpack_blob_cnt = 0;
  size_t listpack_bytes = 0;
  size_t tiered_entries = 0;
  size_t tiered_size = 0;

  std::array<size_t, OBJ_TYPE_MAX> memory_usage_by_type = {};

  // Mostly used internally, exposed for tiered storage.
  void AddTypeMemoryUsage(unsigned type, int64_t delta);

  DbTableStats& operator+=(const DbTableStats& o);
};

// Table for recording locks that uses string_views where possible. LockTable falls back to
// strings for locks that are used by multiple transactions. Keys used with the lock table
// should be normalized with GetLockKey
class LockTable {
 public:
  size_t Size() const;
  std::optional<const IntentLock> Find(std::string_view key) const;

  bool Acquire(std::string_view key, IntentLock::Mode mode);
  void Release(std::string_view key, IntentLock::Mode mode);

  auto begin() const {
    return locks_.cbegin();
  }

  auto end() const {
    return locks_.cbegin();
  }

 private:
  struct Key {
    operator std::string_view() const {
      return visit([](const auto& s) -> std::string_view { return s; }, val_);
    }

    bool operator==(const Key& o) const {
      return *this == std::string_view(o);
    }

    friend std::ostream& operator<<(std::ostream& o, const Key& key) {
      return o << std::string_view(key);
    }

    // If the key is backed by a string_view, replace it with a string with the same value
    void MakeOwned() const;

    mutable std::variant<std::string_view, std::string> val_;
  };

  absl::flat_hash_map<Key, IntentLock> locks_;
};

// A single Db table that represents a table that can be chosen with "SELECT" command.
struct DbTable : boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> {
  PrimeTable prime;
  ExpireTable expire;
  DashTable<PrimeKey, uint32_t, detail::ExpireTablePolicy> mcflag;

  // Contains transaction locks
  LockTable trans_locks;

  // Stores a list of dependant connections for each watched key.
  absl::flat_hash_map<std::string, std::vector<ConnectionState::ExecInfo*>> watched_keys;

  mutable DbTableStats stats;
  std::vector<SlotStats> slots_stats;
  ExpireTable::Cursor expire_cursor;

  TopKeys top_keys;
  DbIndex index;

  explicit DbTable(PMR_NS::memory_resource* mr, DbIndex index);
  ~DbTable();

  void Clear();
  PrimeIterator Launder(PrimeIterator it, std::string_view key);
};

// We use reference counting semantics of DbTable when doing snapshotting.
// There we need to preserve the copy of the table in case someone flushes it during
// the snapshot process. We copy the pointers in StartSnapshotInShard function.
using DbTableArray = std::vector<boost::intrusive_ptr<DbTable>>;

}  // namespace dfly
