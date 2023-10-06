// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user_registry.h"

#include <limits>
#include <mutex>

#include "absl/flags/internal/flag.h"
#include "core/fibers.h"
#include "facade/facade_types.h"
#include "server/acl/acl_commands_def.h"

ABSL_DECLARE_FLAG(std::string, requirepass);

namespace dfly::acl {

void UserRegistry::MaybeAddAndUpdate(std::string_view username, User::UpdateRequest req) {
  std::unique_lock<util::SharedMutex> lock(mu_);
  auto& user = registry_[username];
  user.Update(std::move(req));
}

bool UserRegistry::RemoveUser(std::string_view username) {
  std::unique_lock<util::SharedMutex> lock(mu_);
  return registry_.erase(username);
}

UserRegistry::UserCredentials UserRegistry::GetCredentials(std::string_view username) const {
  std::shared_lock<util::SharedMutex> lock(mu_);
  auto it = registry_.find(username);
  if (it == registry_.end()) {
    return {};
  }
  return {it->second.AclCategory(), it->second.AclCommands()};
}

bool UserRegistry::IsUserActive(std::string_view username) const {
  std::shared_lock<util::SharedMutex> lock(mu_);
  auto it = registry_.find(username);
  if (it == registry_.end()) {
    return false;
  }
  return it->second.IsActive();
}

bool UserRegistry::AuthUser(std::string_view username, std::string_view password) const {
  std::shared_lock<util::SharedMutex> lock(mu_);
  const auto& user = registry_.find(username);
  if (user == registry_.end()) {
    return false;
  }

  return user->second.IsActive() && user->second.HasPassword(password);
}

UserRegistry::RegistryViewWithLock UserRegistry::GetRegistryWithLock() const {
  std::shared_lock<util::SharedMutex> lock(mu_);
  return {std::move(lock), registry_};
}

UserRegistry::RegistryWithWriteLock UserRegistry::GetRegistryWithWriteLock() {
  std::unique_lock<util::SharedMutex> lock(mu_);
  return {std::move(lock), registry_};
}

UserRegistry::UserWithWriteLock::UserWithWriteLock(std::unique_lock<util::SharedMutex> lk,
                                                   const User& user, bool exists)
    : user(user), exists(exists), registry_lk_(std::move(lk)) {
}

UserRegistry::UserWithWriteLock UserRegistry::MaybeAddAndUpdateWithLock(std::string_view username,
                                                                        User::UpdateRequest req) {
  std::unique_lock<util::SharedMutex> lock(mu_);
  const bool exists = registry_.contains(username);
  auto& user = registry_[username];
  user.Update(std::move(req));
  return {std::move(lock), user, exists};
}

User::UpdateRequest UserRegistry::DefaultUserUpdateRequest() const {
  User::UpdateRequest::CommandsUpdateType tmp(NumberOfFamilies());
  size_t id = 0;
  for (auto& elem : tmp) {
    elem = {User::Sign::PLUS, id++, acl::ALL_COMMANDS};
  }
  std::pair<User::Sign, uint32_t> acl{User::Sign::PLUS, acl::ALL};
  return {{}, {acl}, true, false, std::move(tmp)};
}

void UserRegistry::Init() {
  // Add default user
  User::UpdateRequest::CommandsUpdateType tmp(NumberOfFamilies());
  // if there exists an acl file to load from, requirepass
  // will not overwrite the default's user password loaded from
  // that file. Loading the default's user password from a file
  // has higher priority than the deprecated flag
  auto default_user = DefaultUserUpdateRequest();
  auto maybe_password = absl::GetFlag(FLAGS_requirepass);
  if (!maybe_password.empty()) {
    default_user.password = std::move(maybe_password);
  }
  MaybeAddAndUpdate("default", std::move(default_user));
}

}  // namespace dfly::acl
