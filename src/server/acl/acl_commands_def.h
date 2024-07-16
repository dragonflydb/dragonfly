// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "base/logging.h"
#include "facade/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"

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

}  // namespace dfly::acl
