// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "base/logging.h"

namespace dfly::acl {

/* There are 21 ACL categories as of redis 7
 *
 */

enum AclCat {
  KEYSPACE = 1ULL << 0,
  READ = 1ULL << 1,
  WRITE = 1ULL << 2,
  SET = 1ULL << 3,
  SORTEDSET = 1ULL << 4,
  LIST = 1ULL << 5,
  HASH = 1ULL << 6,
  STRING = 1ULL << 7,
  BITMAP = 1ULL << 8,
  HYPERLOGLOG = 1ULL << 9,
  GEO = 1ULL << 10,
  STREAM = 1ULL << 11,
  PUBSUB = 1ULL << 12,
  ADMIN = 1ULL << 13,
  FAST = 1ULL << 14,
  SLOW = 1ULL << 15,
  BLOCKING = 1ULL << 16,
  DANGEROUS = 1ULL << 17,
  CONNECTION = 1ULL << 18,
  TRANSACTION = 1ULL << 19,
  SCRIPTING = 1ULL << 20,

  // Extensions
  BLOOM = 1ULL << 28,
  FT_SEARCH = 1ULL << 29,
  THROTTLE = 1ULL << 30,
  JSON = 1ULL << 31
};

constexpr uint64_t ALL_COMMANDS = std::numeric_limits<uint64_t>::max();
constexpr uint64_t NONE_COMMANDS = std::numeric_limits<uint64_t>::min();

inline size_t NumberOfFamilies(size_t number = 0) {
  static size_t number_of_families = number;
  return number_of_families;
}

using CategoryIndexTable = absl::flat_hash_map<std::string_view, uint32_t>;
using ReverseCategoryIndexTable = std::vector<std::string>;
// bit index to index in the REVERSE_CATEGORY_INDEX_TABLE
using CategoryToIdxStore = absl::flat_hash_map<uint32_t, uint32_t>;

using RevCommandField = std::vector<std::string>;
using RevCommandsIndexStore = std::vector<RevCommandField>;
using CategoryToCommandsIndexStore = absl::flat_hash_map<std::string, std::vector<uint64_t>>;

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
  size_t db{0};
};

}  // namespace dfly::acl
