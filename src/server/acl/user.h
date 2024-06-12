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

  struct UpdateKey {
    std::string key;
    KeyOp op;
    bool all_keys = false;
    bool reset_keys = false;
  };

  struct UpdateRequest {
    std::optional<std::string> password{};

    std::optional<bool> is_active{};

    bool is_hashed{false};

    // Categories and commands
    using CategoryValueType = std::pair<Sign, uint32_t>;
    // If index s numberic_limits::max() then it's a +all flag
    using CommandsValueType = std::tuple<Sign, size_t /*index*/, uint64_t /*bit*/>;
    using UpdateType = std::vector<std::variant<CategoryValueType, CommandsValueType>>;
    UpdateType updates;

    // keys
    std::vector<UpdateKey> keys;
    bool reset_all_keys{false};
    bool allow_all_keys{false};
  };

  using CategoryChange = uint32_t;
  using CommandChange = std::pair<size_t, uint64_t>;

  struct ChangeMetadata {
    Sign sign;
    size_t seq_no;
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

  const AclKeys& Keys() const;

  using CategoryChanges = absl::flat_hash_map<CategoryChange, ChangeMetadata>;
  using CommandChanges = absl::flat_hash_map<CommandChange, ChangeMetadata>;

  const CategoryChanges& CatChanges() const;

  const CommandChanges& CmdChanges() const;

 private:
  void SetAclCategoriesAndIncrSeq(uint32_t cat);
  void UnsetAclCategoriesAndIncrSeq(uint32_t cat);

  // For ACL commands
  void SetAclCommands(size_t index, uint64_t bit_index);
  void UnsetAclCommands(size_t index, uint64_t bit_index);

  void SetAclCommandsAndIncrSeq(size_t index, uint64_t bit_index);
  void UnsetAclCommandsAndIncrSeq(size_t index, uint64_t bit_index);

  // For is_active flag
  void SetIsActive(bool is_active);

  // For passwords
  void SetPasswordHash(std::string_view password, bool is_hashed);

  // For ACL key globs
  void SetKeyGlobs(std::vector<UpdateKey> keys);

  // when optional is empty, the special `nopass` password is implied
  // password hashed with xx64
  std::optional<std::string> password_hash_;
  uint32_t acl_categories_{NONE};
  // Each element index in the vector corresponds to a familly of commands
  // Each bit in the uin64_t field at index id, corresponds to a specific
  // command of that family. Look on TableCommandBuilder and on Service::Register
  // on how this mapping is built during the startup/registration of commands
  std::vector<uint64_t> commands_;

  // We also need to track all the explicit changes (ACL SETUSER) of acl's in-order.
  // To speed up insertion we use the flat_hash_map and a seq_ variable which is a
  // strictly monotonically increasing number that is used for ordering. Both of these
  // indexers are merged and then sorted by the seq_ number when for example we print
  // the ACL rules of each user via ACL LIST.
  CategoryChanges cat_changes_;
  CommandChanges cmd_changes_;
  // Global modification order for changes in rules for acl commands and categories
  size_t seq_ = 0;

  // Glob patterns for the keys that a user is allowed to read/write
  AclKeys keys_;

  // if the user is on/off
  bool is_active_{false};
};

}  // namespace dfly::acl
