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
#include "server/acl/acl_commands_def.h"

namespace dfly {

class CommandId;

// TODO implement these
//#bool CheckIfCommandAllowed(uint64_t command_id, const CommandId& command);
//#bool CheckIfAclCategoryAllowed(uint64_t command_id, const CommandId& command);

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

  // For passwords
  void SetPasswordHash(std::string_view password);

  // when optional is empty, the special `nopass` password is implied
  // password hashed with xx64
  std::optional<std::string> password_hash_;
  uint32_t acl_categories_{AclCategory::NONE};

  // we have at least 221 commands including a bunch of subcommands
  //  LARGE_BITFIELD_DATATYPE acl_commands_;

  // if the user is on/off
  bool is_active_{false};
};

}  // namespace dfly
