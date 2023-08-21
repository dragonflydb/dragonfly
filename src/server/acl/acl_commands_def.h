// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "absl/container/flat_hash_map.h"

namespace dfly::acl {
/* There are 21 ACL categories as of redis 7
 *
 * bit 0: keyspace
 * bit 1: read
 * bit 2: write
 * bit 3: set
 * bit 4: sortedset
 * bit 5: list
 * bit 6: hash
 * bit 7: string
 * bit 8: bitmap
 * bit 9: hyperloglog
 * bit 10: geo
 * bit 11: stream
 * bit 12: pubsub
 * bit 13: admin
 * bit 14: fast
 * bit 15: slow
 * bit 16: blocking
 * bit 17: dangerous
 * bit 18: connection
 * bit 19: transaction
 * bit 20: scripting
 * bits 21..28: tba
 * Dragonfly extensions:
 * bit 29: ft_search
 * bit 30: throttle
 * bit 31: json
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
  FT_SEARCH = 1ULL << 29,
  THROTTLE = 1ULL << 30,
  JSON = 1ULL << 31
};

// Special flag/mask for all
constexpr uint32_t NONE = 0;
constexpr uint32_t ALL = std::numeric_limits<uint32_t>::max();

inline const absl::flat_hash_map<std::string_view, uint32_t> CATEGORY_INDEX_TABLE{
    {"KEYSPACE", KEYSPACE},
    {"READ", READ},
    {"WRITE", WRITE},
    {"SET", SET},
    {"SORTED_SET", SORTEDSET},
    {"LIST", LIST},
    {"HASH", HASH},
    {"STRING", STRING},
    {"BITMAP", BITMAP},
    {"HYPERLOG", HYPERLOGLOG},
    {"GEO", GEO},
    {"STREAM", STREAM},
    {"PUBSUB", PUBSUB},
    {"ADMIN", ADMIN},
    {"FAST", FAST},
    {"SLOW", SLOW},
    {"BLOCKING", BLOCKING},
    {"DANGEROUS", DANGEROUS},
    {"CONNECTION", CONNECTION},
    {"TRANSACTION", TRANSACTION},
    {"SCRIPTING", SCRIPTING}};
}  // namespace dfly::acl
