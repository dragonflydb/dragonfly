// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

enum class Protocol : uint8_t {
  MEMCACHE = 1,
  REDIS = 2
};

}  // namespace dfly
