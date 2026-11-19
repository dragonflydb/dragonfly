// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>

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

// Memory sources of truth:
//
// Total used memory: from UsedMemoryThreadLocal
//   Reported by mimalloc and thread-local counters.
//   Up to date on every call and read-only.
//   Referred to as "used".
//
// Accounted-for memory: from MemoryAccountingState::charged_total
//   Running counter, only updated by ChargeMemory.
//   Read when calculating the baseline and written after a scope ends.
//   Total of the memory tracked by this accounting system. Running total of allocations and frees
//   tracked by scopes and attributed to object types such as list, string, etc. Referred to as
//   "accounted".
//
// Memory usage broken down by type: MemoryAccountingState::by_type
//   Array of per-type counters, breaking down the "accounted" total.
//   Written only by accounting code and read by metrics.
//
// Relations:
//   Used memory is always greater than or equal to accounted memory
//   Sum of memory usage by types is equal to accounted memory
//
// Lifecycle of a scope:
//   At the start, calculate baseline = used - accounted.
//   This is the memory not accounted for yet, whose changes come from allocations and frees during
//   the operation.
//
//   At the end, calculate used - accounted again as baseline'.
//   delta = baseline' - baseline is the change in unaccounted memory due to the scope's type.
//   Attribute this delta to the scope's type in accounting.by_type[category].
//
//   Crucially, we also add this delta to the accounted total: accounted += delta.
//   This step is critical for nested scope handling. All outer scopes can use change in this
//   accounted total, to excloude changes from inner scopes if any. The nested scopes don't need to
//   know about each other, they just refer to these two values: used and accounted.
//
// Nested scopes:
//   Outer scope A (STRING) starts: used=10, accounted=5, baseline=10-5=5.
//     A allocates 1: used=11.
//     Inner scope B (HASH) starts: used=11, accounted=5, baseline=11-5=6.
//       B allocates 1: used=12.
//       B ends: baseline' = used - accounted = 12 - 5 = 7.
//         delta = baseline' - baseline = 7 - 6 = 1.
//         usage for HASH += 1.
//         accounted += 1, so accounted=6.
//     A ends: baseline' = used - accounted = 12 - 6 = 6.
//       delta = baseline' - baseline = 6 - 5 = 1.
//       usage for STRING += 1.
//       accounted += 1, so accounted=7.
//
// Nested scopes update "accounted" so enclosing scopes can compute the difference.

// Returns used - accounted
int64_t UnattributedMemory();

// updates accounted total, and by type. delta is added to both
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
// Memory tracking is turned off for the durations when fiber is suspended.
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
