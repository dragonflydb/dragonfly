// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "core/expire_period.h"
#include "core/intent_lock.h"
#include "server/detail/table.h"

namespace dfly {

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using ExpireTable = DashTable<PrimeKey, ExpirePeriod, detail::ExpireTablePolicy>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid  if a different entry in the table was mutated.
using PrimeIterator = PrimeTable::iterator;
using ExpireIterator = ExpireTable::iterator;

inline bool IsValid(PrimeIterator it) {
  return !it.is_done();
}

inline bool IsValid(ExpireIterator it) {
  return !it.is_done();
}

struct DbTableStats {
  // Number of inline keys.
  uint64_t inline_keys = 0;

  // Object memory usage besides hash-table capacity.
  // Applies for any non-inline objects.
  size_t obj_memory_usage = 0;
  size_t strval_memory_usage = 0;
  size_t listpack_blob_cnt = 0;
  size_t listpack_bytes = 0;
  size_t external_entries = 0;
  size_t external_size = 0;

  DbTableStats& operator+=(const DbTableStats& o);
};

using LockTable = absl::flat_hash_map<std::string, IntentLock>;

// A single Db table that represents a table that can be chosen with "SELECT" command.
struct DbTable : boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> {
  PrimeTable prime;
  ExpireTable expire;
  DashTable<PrimeKey, uint32_t, detail::ExpireTablePolicy> mcflag;

  // Contains transaction locks
  LockTable trans_locks;

  mutable DbTableStats stats;
  ExpireTable::cursor expire_cursor;

  explicit DbTable(std::pmr::memory_resource* mr);
  ~DbTable();

  void Clear();
  void Release(IntentLock::Mode mode, std::string_view key, unsigned count);
};

}  // namespace dfly
