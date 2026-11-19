// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/memory_accounting.h"

#include "base/logging.h"
#include "core/mi_memory_resource.h"

namespace dfly {
using namespace util;

namespace {

struct MemoryAccountingState {
  // A live resource enables accounting for this thread
  MiMemoryResource* resource = nullptr;

  // Cumulative signed charges from ChargeMemory form A in U - A accounting
  int64_t charged_total = 0;
  TypeAttributedMemory by_type{};
};

thread_local MemoryAccountingState accounting;

// The transaction retains ownership while its fiber is suspended.
thread_local TxMemoryScope* tl_tx_scope = nullptr;

}  // namespace

void InitMemoryAccounting(MiMemoryResource& resource) {
  DCHECK_EQ(accounting.resource, nullptr);
  accounting.resource = &resource;
}

void ShutdownMemoryAccounting() {
  accounting = {};
}

bool MemoryAccountingEnabled() {
  return accounting.resource != nullptr;
}

const TypeAttributedMemory& GetTypeAttributedMemory() {
  return accounting.by_type;
}

int64_t UnattributedMemory() {
  if (!MemoryAccountingEnabled())
    return 0;

  const int64_t used = UsedMemoryThreadLocal(*accounting.resource);
  return used - accounting.charged_total;
}

void ChargeMemory(int category, int64_t delta) {
  DCHECK_GE(category, 0);
  DCHECK_LE(category, kUntrackedMemory);
  if (!MemoryAccountingEnabled())
    return;

  accounting.charged_total += delta;
  accounting.by_type[category] += delta;
}

AtomicMemoryScope::AtomicMemoryScope(int category)
    : category_(category), baseline_(UnattributedMemory()) {
  DCHECK_GE(category, 0);
  DCHECK_LE(category, kUntrackedMemory);
}

AtomicMemoryScope::~AtomicMemoryScope() {
  ChargeMemory(category_, UnattributedMemory() - baseline_);
}

TxMemoryScope::TxMemoryScope(int obj_type)
    : obj_type_(obj_type), enabled_(MemoryAccountingEnabled()) {
  if (!enabled_)
    return;

  CHECK_EQ(tl_tx_scope, nullptr) << "tx scope created while another tx scope exists";
  DCHECK_GE(obj_type_, 0);
  DCHECK_LT(obj_type_, OBJ_TYPE_MAX);
  mem_baseline_ = UnattributedMemory();
  tl_tx_scope = this;
  prev_hook_ = ThisFiber::SetSwitchHook({MemTrackerHook});
}

TxMemoryScope::~TxMemoryScope() {
  if (!enabled_)
    return;

  CHECK_EQ(tl_tx_scope, this) << "tx scope destroyed without owning the tracking pointer";
  DCHECK(!suspended_);
  ThisFiber::SetSwitchHook(prev_hook_);
  tl_tx_scope = nullptr;

  ChargeMemory(obj_type_, UnattributedMemory() - mem_baseline_);
}

void TxMemoryScope::MemTrackerHook(fb2::FiberSwitchHookEvent event) noexcept {
  DCHECK_NE(tl_tx_scope, nullptr);
  auto& scope = *tl_tx_scope;

  using enum fb2::FiberSwitchHookEvent;
  switch (event) {
    case SUSPEND:
      DCHECK(!scope.suspended_);
      ChargeMemory(scope.obj_type_, UnattributedMemory() - scope.mem_baseline_);
      scope.suspended_ = true;
      break;
    case RESUME:
      DCHECK(scope.suspended_);

      // Start a fresh interval after other fibers may have changed memory usage.
      scope.mem_baseline_ = UnattributedMemory();
      scope.suspended_ = false;
      break;
  }
}

}  // namespace dfly
