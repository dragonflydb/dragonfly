// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>

#include "util/fibers/fibers.h"

extern "C" {
#include "redis/redis_aux.h"
}

namespace dfly {

class MiMemoryResource;

using TypeAttributedMemory = std::array<int64_t, OBJ_TYPE_MAX>;

// Memory bytes added in this category participate only in scope exclusion. This is a bucket in
// which we "throw away" objects which use the same mimalloc resource as the database, but we do not
// care for individual categories. Example: tiered storage wrapper, mcflag table
inline constexpr int kUntrackedMemory = OBJ_TYPE_MAX;

// memory resource must outlive accounting sessions
void InitMemoryAccounting(MiMemoryResource& resource);

// All accounting scopes on this thread must have ended
void ShutdownMemoryAccounting();

bool MemoryAccountingEnabled();

// Returns the current thread's cumulative charges split into types
const TypeAttributedMemory& GetTypeAttributedMemory();

// Accounting uses U, the tracked allocator usage, and A, the cumulative signed
// total in MemoryAccountingState::charged_total (memory_accounting.cc). And U is from
// UsedMemoryThreadLocal.
//
// UnattributedMemory returns U - A.
//
// ChargeMemory(type, D) adds D to the type's total and to A.
// For kUntrackedMemory, only A is updated.
//
// A scope saves B = U - A and later charges D = (U' - A') - B.
// Equivalently, D = (U' - U) - (A' - A), excluding intervening charges.
//
// For example, a parent allocates 10 bytes and a child allocates 100 bytes.
// The child charges 100 to its type and increases A by 100.
// The parent then charges 110 - 100 = 10 to its own type.
//
// U and A may start at different values because only their changes matter.
int64_t UnattributedMemory();
void ChargeMemory(int category, int64_t delta);

// Attributes allocator delta over this scope's non-yielding lifetime
class AtomicMemoryScope {
 public:
  explicit AtomicMemoryScope(int category);
  ~AtomicMemoryScope();

  AtomicMemoryScope(const AtomicMemoryScope&) = delete;
  AtomicMemoryScope& operator=(const AtomicMemoryScope&) = delete;

 private:
  util::FiberAtomicGuard guard_;
  int category_;
  int64_t baseline_;
};

// Attributes allocator delta during the owning fiber's running intervals for this scope's lifetime
class TxMemoryScope {
 public:
  // An enabled transaction scope exclusively owns the thread's tracking hook until destruction.
  explicit TxMemoryScope(int obj_type);
  ~TxMemoryScope();

  TxMemoryScope(const TxMemoryScope&) = delete;
  TxMemoryScope& operator=(const TxMemoryScope&) = delete;

 private:
  static void MemTrackerHook(util::fb2::FiberSwitchHookEvent event) noexcept;

  int obj_type_;
  int64_t mem_baseline_ = 0;
  util::fb2::FiberSwitchHook prev_hook_;
  bool enabled_ = false;
  bool suspended_ = false;
};

}  // namespace dfly
