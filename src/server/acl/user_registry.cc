// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user_registry.h"

#include <shared_mutex>

#include "core/fibers.h"

namespace dfly {

void UserRegistry::MaybeAddAndUpdate(std::string_view username, User::UpdateRequest req) {
  std::unique_lock<util::SharedMutex> lock(mu_);
  auto& user = registry_[username];
  user.Update(std::move(req));
}

void UserRegistry::RemoveUser(std::string_view username) {
  std::unique_lock<util::SharedMutex> lock(mu_);
  registry_.erase(username);
  // TODO evict authed connections from user
}

UserRegistry::UserCredentials UserRegistry::GetCredentials(std::string_view username) const {
  std::shared_lock<util::SharedMutex> lock(mu_);
  auto it = registry_.find(username);
  if (it == registry_.end()) {
    return {};
  }
  return {it->second.AclCategory()};
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

  return user->second.HasPassword(password);
}

}  // namespace dfly
