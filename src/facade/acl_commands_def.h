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
  // The user is allowed to "touch" any key. No glob matching required.
  // Alias for ~*
  bool all_keys = false;
};

// The second bool denotes if the pattern contains an asterisk and it's
// used to pattern match PSUBSCRIBE that requires exact literals
using GlobTypePubSub = std::pair<std::string, bool>;

struct AclPubSub {
  std::vector<GlobTypePubSub> globs;
  // The user can execute any variant of pub/sub/psub. No glob matching required.
  // Alias for &* just like all_keys for AclKeys above.
  bool all_channels = false;
};

struct UserCredentials {
  uint32_t acl_categories{0};
  std::vector<uint64_t> acl_commands;
  AclKeys keys;
  AclPubSub pub_sub;
  std::string ns;
};

}  // namespace dfly::acl
