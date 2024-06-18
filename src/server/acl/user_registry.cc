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
  user.Update(std::move(req));
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
  return {it->second.AclCategory(), it->second.AclCommands(), it->second.Keys()};
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
  std::pair<User::Sign, uint32_t> acl{User::Sign::PLUS, acl::ALL};
  auto key = User::UpdateKey{"~*", KeyOp::READ_WRITE, true, false};
  auto pass = std::vector<User::UpdatePass>{{"", false, true}};
  return {std::move(pass), true, false, {std::move(acl)}, {std::move(key)}};
}

void UserRegistry::Init() {
  // if there exists an acl file to load from, requirepass
  // will not overwrite the default's user password loaded from
  // that file. Loading the default's user password from a file
  // has higher priority than the deprecated flag
  auto default_user = DefaultUserUpdateRequest();
  auto maybe_password = absl::GetFlag(FLAGS_requirepass);
  if (!maybe_password.empty()) {
    default_user.passwords.front().nopass = false;
  } else if (const char* env_var = getenv("DFLY_PASSWORD"); env_var) {
    default_user.passwords.front().password = env_var;
  } else if (const char* env_var = getenv("DFLY_requirepass"); env_var) {
    default_user.passwords.front().password = env_var;
  }
  MaybeAddAndUpdate("default", std::move(default_user));
}

}  // namespace dfly::acl
