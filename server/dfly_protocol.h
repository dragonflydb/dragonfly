// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <cstdint>

namespace dfly {

enum class Protocol : uint8_t {
  MEMCACHE = 1,
  REDIS = 2
};

}  // namespace dfly
