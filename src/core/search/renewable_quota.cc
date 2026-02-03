// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/renewable_quota.h"

#include "base/cycle_clock.h"
#include "util/fibers/fibers.h"

namespace dfly::search {

RenewableQuota RenewableQuota::Unlimited() {
  return RenewableQuota{std::numeric_limits<size_t>::max()};
}

// Quota that yields if the fiber is running for too long
void RenewableQuota::Check() const {
  size_t cycles = util::ThisFiber::GetRunningTimeCycles();
  size_t usec = base::CycleClock::ToUsec(cycles);
  if (usec >= max_usec)
    util::ThisFiber::Yield();
}

}  // namespace dfly::search
