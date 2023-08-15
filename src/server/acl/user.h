// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"

namespace dfly {

class CommandId;

// TODO implement these
//#bool CheckIfCommandAllowed(uint64_t command_id, const CommandId& command);
//#bool CheckIfAclCategoryAllowed(uint64_t command_id, const CommandId& command);

namespace AclCat {
/* There are 21 ACL categories as of redis 7
 *
 * bit 0: admin
 * bit 1: bitmap
 * bit 2: blocking
 * bit 3: connection
 * bit 4: dangerous
 * bit 5: geo
 * bit 6: hash
 * bit 7: list
 * bit 8: set
 * bit 9: string
 * bit 10: sorttedset
 * bit 11: hyperloglog
 * bit 12: streams
 * bit 13: fast
 * bit 14: slow
 * bit 15: key-space
 * bit 16: pubsub
 * bit 17: read
 * bit 18: write
 * bit 19: scripting
 * bit 20: transaction
 *
 * The rest of the bitfield, will contain special values like @all
 *
 * bits 21..31: tba
 */
enum AclCat {
  ACL_CATEGORY_KEYSPACE = 1ULL << 0,
  ACL_CATEGORY_READ = 1ULL << 1,
  ACL_CATEGORY_WRITE = 1ULL << 2,
  ACL_CATEGORY_SET = 1ULL << 3,
  ACL_CATEGORY_SORTEDSET = 1ULL << 4,
  ACL_CATEGORY_LIST = 1ULL << 5,
  ACL_CATEGORY_HASH = 1ULL << 6,
  ACL_CATEGORY_STRING = 1ULL << 7,
  ACL_CATEGORY_BITMAP = 1ULL << 8,
  ACL_CATEGORY_HYPERLOGLOG = 1ULL << 9,
  ACL_CATEGORY_GEO = 1ULL << 10,
  ACL_CATEGORY_STREAM = 1ULL << 11,
  ACL_CATEGORY_PUBSUB = 1ULL << 12,
  ACL_CATEGORY_ADMIN = 1ULL << 13,
  ACL_CATEGORY_FAST = 1ULL << 14,
  ACL_CATEGORY_SLOW = 1ULL << 15,
  ACL_CATEGORY_BLOCKING = 1ULL << 16,
  ACL_CATEGORY_DANGEROUS = 1ULL << 17,
  ACL_CATEGORY_CONNECTION = 1ULL << 18,
  ACL_CATEGORY_TRANSACTION = 1ULL << 19,
  ACL_CATEGORY_SCRIPTING = 1ULL << 20
};

// Special flag/mask for all
inline constexpr uint32_t ACL_CATEGORY_NONE = 0;
inline constexpr uint32_t ACL_CATEGORY_ALL = std::numeric_limits<uint32_t>::max();
}  // namespace AclCat

inline const absl::flat_hash_map<std::string_view, uint32_t> CATEGORY_INDEX_TABLE{
    {"KEYSPACE", AclCat::ACL_CATEGORY_KEYSPACE},
    {"READ", AclCat::ACL_CATEGORY_READ},
    {"WRITE", AclCat::ACL_CATEGORY_WRITE},
    {"SET", AclCat::ACL_CATEGORY_SET},
    {"SORTED_SET", AclCat::ACL_CATEGORY_SORTEDSET},
    {"LIST", AclCat::ACL_CATEGORY_LIST},
    {"HASH", AclCat::ACL_CATEGORY_HASH},
    {"STRING", AclCat::ACL_CATEGORY_STRING},
    {"BITMAP", AclCat::ACL_CATEGORY_BITMAP},
    {"HYPERLOG", AclCat::ACL_CATEGORY_HYPERLOGLOG},
    {"GEO", AclCat::ACL_CATEGORY_GEO},
    {"STREAM", AclCat::ACL_CATEGORY_STREAM},
    {"PUBSUB", AclCat::ACL_CATEGORY_PUBSUB},
    {"ADMIN", AclCat::ACL_CATEGORY_ADMIN},
    {"FAST", AclCat::ACL_CATEGORY_FAST},
    {"SLOW", AclCat::ACL_CATEGORY_SLOW},
    {"BLOCKING", AclCat::ACL_CATEGORY_BLOCKING},
    {"DANGEROUS", AclCat::ACL_CATEGORY_DANGEROUS},
    {"CONNECTION", AclCat::ACL_CATEGORY_CONNECTION},
    {"TRANSACTION", AclCat::ACL_CATEGORY_TRANSACTION},
    {"SCRIPTING", AclCat::ACL_CATEGORY_SCRIPTING}};

class User final {
 public:
  struct UpdateRequest {
    std::optional<std::string> password{};

    std::optional<uint32_t> plus_acl_categories{};
    std::optional<uint32_t> minus_acl_categories{};

    // DATATYPE_BITSET commands;

    std::optional<bool> is_active{};
  };

  /* Used for default user
   * password = nopass
   * acl_categories = +@all
   * is_active = true;
   */
  User();

  User(const User&) = delete;
  User(User&&) = default;

  // For single step updates
  void Update(UpdateRequest&& req);

  bool HasPassword(std::string_view password) const;

  uint32_t AclCategory() const;

  // TODO
  // For ACL commands
  // void SetAclCommand()
  // void AclCommand() const;

  bool IsActive() const;

 private:
  // For ACL categories
  void SetAclCategories(uint64_t cat);
  void UnsetAclCategories(uint64_t cat);

  // For is_active flag
  void SetIsActive(bool is_active);

  // Helper function for hashing passwords
  uint32_t HashPassword(std::string_view password) const;

  // For passwords
  void SetPassword(std::string_view password);

  // when optional is empty, the special `nopass` password is implied
  // password hashed with xx64
  std::optional<uint64_t> password_;
  uint32_t acl_categories_{AclCat::ACL_CATEGORY_NONE};

  // we have at least 221 commands including a bunch of subcommands
  //  LARGE_BITFIELD_DATATYPE acl_commands_;

  // if the user is on/off
  bool is_active_{false};
};

}  // namespace dfly
