// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

#include "base/cycle_clock.h"

namespace dfly {

inline uint64_t CyclesToUsec(uint64_t cycles) {
  const uint64_t frequency = base::CycleClock::Frequency();
  return ((cycles / frequency) * 1'000'000ULL) +
         (((cycles % frequency) * 1'000'000ULL) / frequency);
}

}  // namespace dfly
