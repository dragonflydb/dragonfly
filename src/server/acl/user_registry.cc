// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user_registry.h"

#include <limits>
#include <mutex>

#include "base/flags.h"
#include "facade/acl_commands_def.h"
#include "facade/facade_types.h"
#include "server/acl/acl_commands_def.h"

ABSL_DECLARE_FLAG(std::string, requirepass);

using namespace util;

namespace dfly::acl {

void UserRegistry::MaybeAddAndUpdate(std::string_view username, User::UpdateRequest req) {
  std::unique_lock<fb2::SharedMutex> lock(mu_);
  auto& user = registry_[username];
  user.Update(std::move(req), *cat_to_id_table_, *reverse_cat_table_, *cat_to_commands_table_);
}

bool UserRegistry::RemoveUser(std::string_view username) {
  std::unique_lock<fb2::SharedMutex> lock(mu_);
  return registry_.erase(username);
}

UserCredentials UserRegistry::GetCredentials(std::string_view username) const {
  std::shared_lock<fb2::SharedMutex> lock(mu_);
  auto it = registry_.find(username);
  if (it == registry_.end()) {
    return {};
  }
  auto& user = it->second;
  return {user.AclCategory(), user.AclCommands(), user.Keys(), user.PubSub(), user.Namespace()};
}

bool UserRegistry::IsUserActive(std::string_view username) const {
  std::shared_lock<fb2::SharedMutex> lock(mu_);
  auto it = registry_.find(username);
  if (it == registry_.end()) {
    return false;
  }
  return it->second.IsActive();
}

bool UserRegistry::AuthUser(std::string_view username, std::string_view password) const {
  std::shared_lock<fb2::SharedMutex> lock(mu_);
  const auto& user = registry_.find(username);
  if (user == registry_.end()) {
    return false;
  }

  return user->second.IsActive() && user->second.HasPassword(password);
}

UserRegistry::RegistryViewWithLock UserRegistry::GetRegistryWithLock() const {
  std::shared_lock<fb2::SharedMutex> lock(mu_);
  return {std::move(lock), registry_};
}

UserRegistry::RegistryWithWriteLock UserRegistry::GetRegistryWithWriteLock() {
  std::unique_lock<fb2::SharedMutex> lock(mu_);
  return {std::move(lock), registry_};
}

UserRegistry::UserWithWriteLock::UserWithWriteLock(std::unique_lock<fb2::SharedMutex> lk,
                                                   const User& user, bool exists)
    : user(user), exists(exists), registry_lk_(std::move(lk)) {
}

User::UpdateRequest UserRegistry::DefaultUserUpdateRequest() const {
  // Assign field by field to supress an annoying compiler warning
  User::UpdateRequest req;
  req.passwords = std::vector<User::UpdatePass>{{"", false, true}};
  req.is_active = true;
  req.updates = {std::pair<User::Sign, uint32_t>{User::Sign::PLUS, acl::ALL}};
  req.keys = {User::UpdateKey{"~*", KeyOp::READ_WRITE, true, false}};
  req.pub_sub = {User::UpdatePubSub{"", false, true, false}};
  return req;
}

void UserRegistry::Init(const CategoryToIdxStore* cat_to_id_table,
                        const ReverseCategoryIndexTable* reverse_cat_table,
                        const CategoryToCommandsIndexStore* cat_to_commands_table) {
  // if there exists an acl file to load from, requirepass
  // will not overwrite the default's user password loaded from
  // that file. Loading the default's user password from a file
  // has higher priority than the deprecated flag
  cat_to_id_table_ = cat_to_id_table;
  reverse_cat_table_ = reverse_cat_table;
  cat_to_commands_table_ = cat_to_commands_table;
  auto default_user = DefaultUserUpdateRequest();
  auto maybe_password = absl::GetFlag(FLAGS_requirepass);
  if (!maybe_password.empty()) {
    default_user.passwords.front().password = std::move(maybe_password);
    default_user.passwords.front().nopass = false;
  } else if (const char* env_var = getenv("DFLY_PASSWORD"); env_var) {
    default_user.passwords.front().password = env_var;
    default_user.passwords.front().nopass = false;
  } else if (const char* env_var = getenv("DFLY_requirepass"); env_var) {
    default_user.passwords.front().password = env_var;
    default_user.passwords.front().nopass = false;
  }
  MaybeAddAndUpdate("default", std::move(default_user));
}

}  // namespace dfly::acl
