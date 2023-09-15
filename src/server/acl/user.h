// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "server/acl/acl_commands_def.h"

namespace dfly::acl {

class User final {
 public:
  enum class Sign : int8_t { PLUS, MINUS };

  struct UpdateRequest {
    std::optional<std::string> password{};

    std::vector<std::pair<Sign, uint32_t>> categories;

    std::optional<bool> is_active{};

    bool is_hashed{false};

    // If index s numberic_limits::max() then it's a +all flag
    using CommandsValueType = std::tuple<Sign, size_t /*index*/, uint64_t /*bit*/>;
    using CommandsUpdateType = std::vector<CommandsValueType>;
    CommandsUpdateType commands;
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

  std::vector<uint64_t> AclCommands() const;
  const std::vector<uint64_t>& AclCommandsRef() const;

  bool IsActive() const;

  std::string_view Password() const;

  // Selector maps a command string (like HSET, SET etc) to
  // its respective ID within the commands vector.
  static size_t Selector(std::string_view);

 private:
  // For ACL categories
  void SetAclCategories(uint32_t cat);
  void UnsetAclCategories(uint32_t cat);

  // For ACL commands
  void SetAclCommands(size_t index, uint64_t bit_index);
  void UnsetAclCommands(size_t index, uint64_t bit_index);

  // For is_active flag
  void SetIsActive(bool is_active);

  // For passwords
  void SetPasswordHash(std::string_view password, bool is_hashed);

  // when optional is empty, the special `nopass` password is implied
  // password hashed with xx64
  std::optional<std::string> password_hash_;
  uint32_t acl_categories_{NONE};
  // Each element index in the vector corresponds to a familly of commands
  // Each bit in the uin64_t field at index id, corresponds to a specific
  // command of that family. Look on TableCommandBuilder and on Service::Register
  // on how this mapping is built during the startup/registration of commands
  std::vector<uint64_t> commands_;

  // we have at least 221 commands including a bunch of subcommands
  //  LARGE_BITFIELD_DATATYPE acl_commands_;

  // if the user is on/off
  bool is_active_{false};
};

}  // namespace dfly::acl
