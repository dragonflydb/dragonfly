// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// Tagged-dispatch for PageUsage::IsPageForObjectUnderUtilized.
//
// The base class declares IsPageForObjectUnderUtilized as a non-virtual
// method (page_usage_stats.h) but does not define it — definition lives
// here as an inline switch on PageUsage::kind_, forwarding to the concrete
// subclass's non-virtual *Impl method. The compiler turns the switch into
// a jump table and inlines the called impl, recovering ~0.65 ns/call vs
// the original virtual + cross-TU dispatch.
//
// LAYERING: this header includes core/page_usage/page_usage_visitors.h to
// see the concrete subclass definitions (Evacuator, CensusTaker), and
// mimalloc/internal.h for `_mi_ptr_page` used inside Evacuator::*Impl.
// mimalloc/internal.h is NOT pulled into page_usage_visitors.h — including
// it there leaks (via server/defrag.h → engine_shard.h → main_service.h
// → dfly_main.cc) and conflicts with dfly_main.cc's `extern "C"`
// redeclarations. Keeping it scoped to this header (and the TUs that
// include it) avoids that.
//
// Any TU that calls PageUsage::IsPageForObjectUnderUtilized must include
// this header (not just page_usage_stats.h) — otherwise the link fails
// since the method is declared but not defined elsewhere.
//
// All symbols referenced by the inline *Impl bodies (TargetPlan::FindMut,
// EvacDecide, PageCensus::Observe, SetDefragSkip*) live in dfly_page_usage,
// so this header can be safely included from core lib TUs without pulling
// in dragonfly_lib (server-side) symbols.

#define MI_BUILD_RELEASE 1
#include <mimalloc/internal.h>

#include "core/page_usage/page_usage_stats.h"
#include "core/page_usage/page_usage_visitors.h"

namespace dfly {

inline bool CensusTaker::IsPageForObjectUnderUtilizedImpl(void* object) {
  mi_page_usage_stats_t stat = mi_heap_page_is_underutilized(
      static_cast<mi_heap_t*>(zmalloc_heap), object, threshold_, /*collect_stats=*/true);
  census_->Observe(stat, current_cursor_);
  return false;
}

inline bool CensusTaker::IsPageForObjectUnderUtilizedImpl(mi_heap_t* heap, void* object) {
  mi_page_usage_stats_t stat =
      mi_heap_page_is_underutilized(heap, object, threshold_, /*collect_stats=*/true);
  census_->Observe(stat, current_cursor_);
  return false;
}

inline bool Evacuator::IsPageForObjectUnderUtilizedImpl(void* object) {
  const uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(object));
  TargetPage* target = plan_->FindMut(addr);
  if (target == nullptr) {
    ++evac_stats_->blocks_skipped_not_target;
    return false;
  }
  const mi_page_usage_stats_t stat = mi_heap_page_is_underutilized(
      static_cast<mi_heap_t*>(zmalloc_heap), object, threshold_, /*collect_stats=*/true);
  return EvacDecide(*plan_, target, stat, *evac_stats_) == EvacOutcome::kCommitMove;
}

inline bool Evacuator::IsPageForObjectUnderUtilizedImpl(mi_heap_t* heap, void* object) {
  const uintptr_t addr = reinterpret_cast<uintptr_t>(_mi_ptr_page(object));
  TargetPage* target = plan_->FindMut(addr);
  if (target == nullptr) {
    ++evac_stats_->blocks_skipped_not_target;
    return false;
  }
  const mi_page_usage_stats_t stat =
      mi_heap_page_is_underutilized(heap, object, threshold_, /*collect_stats=*/true);
  return EvacDecide(*plan_, target, stat, *evac_stats_) == EvacOutcome::kCommitMove;
}

inline bool PageUsage::IsPageForObjectUnderUtilized(void* object) {
  switch (kind_) {
    case Kind::kBase:
      return BaseIsPageForObjectUnderUtilized(object);
    case Kind::kEvacuator:
      return static_cast<Evacuator*>(this)->IsPageForObjectUnderUtilizedImpl(object);
    case Kind::kCensus:
      return static_cast<CensusTaker*>(this)->IsPageForObjectUnderUtilizedImpl(object);
    case Kind::kCustom:
      return IsPageForObjectUnderUtilizedHook(object);
  }
  __builtin_unreachable();
}

inline bool PageUsage::IsPageForObjectUnderUtilized(mi_heap_t* heap, void* object) {
  switch (kind_) {
    case Kind::kBase:
      return BaseIsPageForObjectUnderUtilized(heap, object);
    case Kind::kEvacuator:
      return static_cast<Evacuator*>(this)->IsPageForObjectUnderUtilizedImpl(heap, object);
    case Kind::kCensus:
      return static_cast<CensusTaker*>(this)->IsPageForObjectUnderUtilizedImpl(heap, object);
    case Kind::kCustom:
      return IsPageForObjectUnderUtilizedHook(heap, object);
  }
  __builtin_unreachable();
}

}  // namespace dfly
