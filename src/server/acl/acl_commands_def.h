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

// See definitions for NONE and ALL in facade/acl_commands_def.h

inline const absl::flat_hash_map<std::string_view, uint32_t> CATEGORY_INDEX_TABLE{
    {"KEYSPACE", KEYSPACE},
    {"READ", READ},
    {"WRITE", WRITE},
    {"SET", SET},
    {"SORTEDSET", SORTEDSET},
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
    {"SCRIPTING", SCRIPTING},
    {"BLOOM", BLOOM},
    {"FT_SEARCH", FT_SEARCH},
    {"THROTTLE", THROTTLE},
    {"JSON", JSON},
    {"ALL", ALL},
    {"NONE", NONE}};

// bit 0 at index 0
// bit 1 at index 1
// bit n at index n
inline const std::vector<std::string> REVERSE_CATEGORY_INDEX_TABLE{
    "KEYSPACE",  "READ",      "WRITE",     "SET",       "SORTEDSET",  "LIST",        "HASH",
    "STRING",    "BITMAP",    "HYPERLOG",  "GEO",       "STREAM",     "PUBSUB",      "ADMIN",
    "FAST",      "SLOW",      "BLOCKING",  "DANGEROUS", "CONNECTION", "TRANSACTION", "SCRIPTING",
    "_RESERVED", "_RESERVED", "_RESERVED", "_RESERVED", "_RESERVED",  "_RESERVED",   "_RESERVED",
    "BLOOM",     "FT_SEARCH", "THROTTLE",  "JSON"};

// bit index to index in the REVERSE_CATEGORY_INDEX_TABLE
using CategoryToIdxStore = absl::flat_hash_map<uint32_t, uint32_t>;

// inline const CategoryToIdxStore& CategoryToIdx(CategoryToIdxStore store = {}) {
inline const CategoryToIdxStore& CategoryToIdx(CategoryToIdxStore store = {}) {
  static CategoryToIdxStore cat_idx = std::move(store);
  return cat_idx;
}

using RevCommandField = std::vector<std::string>;
using RevCommandsIndexStore = std::vector<RevCommandField>;

constexpr uint64_t ALL_COMMANDS = std::numeric_limits<uint64_t>::max();
constexpr uint64_t NONE_COMMANDS = std::numeric_limits<uint64_t>::min();

// A variation of meyers singleton
// This is initialized when the constructor of Service is called.
// Basically, it calls this functions within the AclFamily::Register
// functions which has the number of all the acl families registered
inline size_t NumberOfFamilies(size_t number = 0) {
  static size_t number_of_families = number;
  return number_of_families;
}

inline const RevCommandsIndexStore& CommandsRevIndexer(RevCommandsIndexStore store = {}) {
  static RevCommandsIndexStore rev_index_store = std::move(store);
  return rev_index_store;
}

using CategoryToCommandsIndexStore = absl::flat_hash_map<std::string, std::vector<uint64_t>>;

inline const CategoryToCommandsIndexStore& CategoryToCommandsIndex(
    CategoryToCommandsIndexStore store = {}) {
  static CategoryToCommandsIndexStore index = std::move(store);
  return index;
}

inline void BuildIndexers(RevCommandsIndexStore families, CommandRegistry* cmd_registry) {
  acl::NumberOfFamilies(families.size());
  acl::CommandsRevIndexer(std::move(families));
  CategoryToCommandsIndexStore index;
  cmd_registry->Traverse([&](std::string_view name, auto& cid) {
    auto cat = cid.acl_categories();
    for (size_t i = 0; i < 32; ++i) {
      if (cat & (1 << i)) {
        std::string_view cat_name = REVERSE_CATEGORY_INDEX_TABLE[i];
        if (index[cat_name].empty()) {
          index[cat_name].resize(CommandsRevIndexer().size());
        }
        auto family = cid.GetFamily();
        auto bit_index = cid.GetBitIndex();
        index[cat_name][family] = index[cat_name][family] | bit_index;
      }
    }
  });

  CategoryToCommandsIndex(std::move(index));
  CategoryToIdxStore idx_store;
  for (size_t i = 0; i < 32; ++i) {
    idx_store[1 << i] = i;
  }
  CategoryToIdx(std::move(idx_store));
}

}  // namespace dfly::acl
