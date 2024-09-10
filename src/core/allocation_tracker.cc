// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/allocation_tracker.h"

#include "absl/random/random.h"
#include "base/logging.h"
#include "util/fibers/stacktrace.h"

namespace dfly {
namespace {
thread_local AllocationTracker g_tracker;
thread_local absl::InsecureBitGen g_bitgen;
}  // namespace

AllocationTracker& AllocationTracker::Get() {
  return g_tracker;
}

bool AllocationTracker::Add(const TrackingInfo& info) {
  if (tracking_.size() >= tracking_.max_size()) {
    return false;
  }

  tracking_.push_back(info);
  return true;
}

bool AllocationTracker::Remove(size_t lower_bound, size_t upper_bound) {
  size_t before_size = tracking_.size();

  tracking_.erase(std::remove_if(tracking_.begin(), tracking_.end(),
                                 [&](const TrackingInfo& info) {
                                   return info.lower_bound == lower_bound &&
                                          info.upper_bound == upper_bound;
                                 }),
                  tracking_.end());

  return before_size != tracking_.size();
}

void AllocationTracker::Clear() {
  tracking_.clear();
}

absl::Span<const AllocationTracker::TrackingInfo> AllocationTracker::GetRanges() const {
  return absl::MakeConstSpan(tracking_);
}

void AllocationTracker::ProcessNew(void* ptr, size_t size) {
  if (tracking_.empty()) {
    return;
  }

  if (inside_tracker_) {
    return;
  }

  // Prevent endless recursion, in case logging allocates memory
  inside_tracker_ = true;
  double random = absl::Uniform(g_bitgen, 0.0, 1.0);
  for (const auto& band : tracking_) {
    if (random >= band.sample_odds || size > band.upper_bound || size < band.lower_bound) {
      continue;
    }

    size_t usable = mi_usable_size(ptr);
    DCHECK_GE(usable, size);
    LOG(INFO) << "Allocating " << usable << " bytes (" << ptr
              << "). Stack: " << util::fb2::GetStacktrace();
    break;
  }
  inside_tracker_ = false;
}

void AllocationTracker::ProcessDelete(void* ptr) {
  if (inside_tracker_) {
    return;
  }

  inside_tracker_ = true;
  // we partially handle deletes, specifically when specifying a single range with
  // 100% sampling rate.
  if (tracking_.size() == 1 && tracking_.front().sample_odds == 1) {
    size_t usable = mi_usable_size(ptr);
    if (usable <= tracking_.front().upper_bound && usable >= tracking_.front().lower_bound) {
      LOG(INFO) << "Deallocating " << usable << " bytes (" << ptr << ")\n"
                << util::fb2::GetStacktrace();
    }
  }
  inside_tracker_ = false;
}

}  // namespace dfly
