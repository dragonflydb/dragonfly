// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

#include "core/compact_object.h"
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

// In a replacement operation, handles freeing of old value by wrapping it in the correct type
// scope. Use RAII to register and then de-register the hook. Also use guard to prevent fiber switch
// (which might leave a hook dangling in the compact object tls). Once this tracker object is
// created, the next free of old type will be wrapped in scope.
class TrackOldValueFree {
 public:
  TrackOldValueFree(const CompactObj& target, int old_type) : old_type_(old_type) {
    CompactObj::RegisterDeallocateHandler(target, CompactObj::FreeHook{*this});
  }

  ~TrackOldValueFree() {
    CompactObj::DeregisterDeallocateHandler();
  }

  TrackOldValueFree(const TrackOldValueFree&) = delete;
  TrackOldValueFree& operator=(const TrackOldValueFree&) = delete;
  TrackOldValueFree(TrackOldValueFree&&) = delete;
  TrackOldValueFree& operator=(TrackOldValueFree&&) = delete;

  // Attribute the old value's destruction to its original type.
  void operator()(CompactObj& obj) const {
    AtomicMemoryScope scope(old_type_);
    obj.Free();
  }

 private:
  util::FiberAtomicGuard guard_;
  int old_type_;
};

}  // namespace dfly
