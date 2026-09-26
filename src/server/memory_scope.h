// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

#include "util/fibers/fibers.h"

namespace dfly {

class DbSlice;

// Scopes on shard form a chain through `MemoryScope::parent_`
// `tl_top_scope` points at the current scope
// `tl_tx_scope` at the single scope which may be suspended.
//
// In below examples A is AtomicMemoryScope, T is TxMemoryScope, and "X <-- Y" means Y.parent_ == X
//
// Transaction with nested atomic scopes eg SET command triggers evictions:
//   tl_tx_scope == T
//   T <-- A1 <-- A2 == tl_top_scope
//
// On destruction scope adds its total delta to its parent's `child_delta_` and records only
// `delta_ - child_delta_` for its own type. A2 is destroyed, then A1, then T.
//
// If T's fiber is suspended then T leaves the chain. Scopes of other fibers eg heartbeat eviction
// start with no parent, and being atomic will finish before T resumes:
//   tl_tx_scope == T (suspended)
//   null <-- B == tl_top_scope
//
// later, T's fiber resumed: the chain must be empty. T rejoins it:
//   tl_tx_scope == T == tl_top_scope

bool MemoryScopeEnabled();

class MemoryScope {
 public:
  MemoryScope(const MemoryScope&) = delete;
  MemoryScope& operator=(const MemoryScope&) = delete;

  ~MemoryScope();

 private:
  friend class TxMemoryScope;
  friend class AtomicMemoryScope;

  explicit MemoryScope(int obj_type, const DbSlice* db_slice = nullptr);

  void Checkpoint(int64_t used_memory);

  int obj_type_;
  // used to check for table memory usage. if null then table memory is not subtracted in scope
  // delta calculation. child scopes inherit it from their parent.
  const DbSlice* db_slice_;
  int64_t mem_baseline_ = 0;

  // naive computed delta by comparing baseline to current memory use
  int64_t delta_ = 0;
  // sum of all deltas that child scopes report
  int64_t child_delta_ = 0;

  MemoryScope* parent_ = nullptr;
};

// Sets fiber hook, can be suspended
class TxMemoryScope {
 public:
  TxMemoryScope(int obj_type, const DbSlice* db_slice);
  ~TxMemoryScope();

  TxMemoryScope(const TxMemoryScope&) = delete;
  TxMemoryScope& operator=(const TxMemoryScope&) = delete;

  void Suspend();
  void Resume();

 private:
  MemoryScope core_;
  util::fb2::FiberSwitchHook prev_hook_;
  bool suspended_ = false;
};

// must not suspend
class AtomicMemoryScope {
 public:
  explicit AtomicMemoryScope(int obj_type) : core_(obj_type) {
  }

 private:
  util::FiberAtomicGuard guard_;
  MemoryScope core_;
};

}  // namespace dfly
