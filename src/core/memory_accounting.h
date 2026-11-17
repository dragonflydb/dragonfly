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

// Excluded from enclosing scopes, but never stored in or reported as a per-type total.
inline constexpr int kUntrackedMemory = OBJ_TYPE_MAX;

void InitMemoryAccounting(MiMemoryResource& resource);
void ShutdownMemoryAccounting();

bool MemoryAccountingEnabled();

const TypeAttributedMemory& GetTypeAttributedMemory();

// Current memory usage minus whatever has been accounted for already
int64_t UnattributedMemory();

// Record allocator delta, exclude it from any enclosing scopes
void ChargeMemory(int category, int64_t delta);

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

// Owns the thread's single transaction scope and its fiber hook, even while suspended
class TxMemoryScope {
 public:
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
