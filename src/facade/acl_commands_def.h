// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace dfly::acl {
// Special flag/mask for all
constexpr uint32_t NONE = 0;
constexpr uint32_t ALL = std::numeric_limits<uint32_t>::max();

enum class KeyOp : int8_t { READ, WRITE, READ_WRITE };

using GlobType = std::pair<std::string, KeyOp>;

struct AclKeys {
  std::vector<GlobType> key_globs;
  bool all_keys = false;
};

}  // namespace dfly::acl
