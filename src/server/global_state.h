// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <iosfwd>

namespace dfly {

enum class GlobalState : uint8_t {
  ACTIVE,
  LOADING,
  SHUTTING_DOWN,
  TAKEN_OVER,
};

std::ostream& operator<<(std::ostream& os, const GlobalState& state);

const char* GlobalStateName(GlobalState gs);

class Namespaces;

// Globally used atomics for memory readings
inline std::atomic_uint64_t used_mem_current{0};
inline std::atomic_uint64_t rss_mem_current{0};
// Current value of --maxmemory flag
inline std::atomic_uint64_t max_memory_limit{0};

inline Namespaces* namespaces = nullptr;

// version 5.11 maps to 511 etc.
// set upon server start.
inline unsigned kernel_version = 0;

}  // namespace dfly
