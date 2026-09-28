// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

#include "util/fibers/fibers.h"

namespace dfly {

bool MemoryScopeEnabled();

// Excludes delta from the active (non-suspended) transaction scope, if any.
void DeductFromTxScope(int64_t delta);

// Owns the thread's single transaction scope and its fiber hook, including while suspended.
class TxMemoryScope {
 public:
  explicit TxMemoryScope(int obj_type);
  ~TxMemoryScope();

  TxMemoryScope(const TxMemoryScope&) = delete;
  TxMemoryScope& operator=(const TxMemoryScope&) = delete;

  void Suspend();
  void Resume();

  void Deduct(int64_t delta) {
    if (!suspended_)
      delta_ -= delta;
  }

 private:
  void Checkpoint(int64_t used_memory);

  int obj_type_;
  int64_t mem_baseline_ = 0;
  int64_t delta_ = 0;
  util::fb2::FiberSwitchHook prev_hook_;
  bool suspended_ = false;
};

// non-yielding operation scope records delta for own type, adjusts transaction scope if present
class AtomicMemoryScope {
 public:
  explicit AtomicMemoryScope(int obj_type);
  ~AtomicMemoryScope();

  AtomicMemoryScope(const AtomicMemoryScope&) = delete;
  AtomicMemoryScope& operator=(const AtomicMemoryScope&) = delete;

 private:
  util::FiberAtomicGuard guard_;
  bool enabled_;
  int obj_type_;
  int64_t mem_baseline_;
};

}  // namespace dfly
