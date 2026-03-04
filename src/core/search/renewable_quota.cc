// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/renewable_quota.h"

#include "base/cycle_clock.h"
#include "base/logging.h"
#include "util/fibers/fibers.h"

namespace dfly::search {

RenewableQuota RenewableQuota::Unlimited() {
  return RenewableQuota{std::numeric_limits<size_t>::max()};
}

// Quota that yields if the fiber is running for too long
void RenewableQuota::Check(std::source_location location) const {
  size_t cycles = util::ThisFiber::GetRunningTimeCycles();
  size_t usec = base::CycleClock::ToUsec(cycles);
  if (usec >= max_usec) {
    size_t ms = usec / 1'000;
    VLOG_IF(1, ms >= 50) << "Grabbed " << ms << "ms for " << location.file_name() << ":"
                         << location.line();

    util::ThisFiber::Yield();
  }
}
}  // namespace dfly::search
