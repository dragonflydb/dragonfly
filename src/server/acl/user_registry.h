// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "core/fibers.h"
#include "server/acl/user.h"

namespace dfly::acl {

class UserRegistry {
 public:
  UserRegistry();

  UserRegistry(const UserRegistry&) = delete;
  UserRegistry(UserRegistry&&) = delete;

  using RegistryType = absl::flat_hash_map<std::string, User>;

  // Acquires a write lock of mu_
  // If the user with name `username` does not exist, it's added in the store with
  // the exact fields found in req
  // If the user exists, the bitfields are updated with a `logical and` operation
  void MaybeAddAndUpdate(std::string_view username, User::UpdateRequest req);

  // Acquires a write lock on mu_
  // Removes user from the store
  // kills already existing connections from the removed user
  // TODO change return time to communicate back results to acl commands
  void RemoveUser(std::string_view username);

  struct UserCredentials {
    uint32_t acl_categories{0};
  };

  // Acquires a read lock
  UserCredentials GetCredentials(std::string_view username) const;

  // Acquires a read lock
  bool IsUserActive(std::string_view username) const;

  // Acquires a read lock
  bool AuthUser(std::string_view username, std::string_view password) const;

  // Helper class for accessing the registry with a ReadLock outside the scope of UserRegistry
  class RegistryViewWithLock {
   public:
    RegistryViewWithLock(std::shared_lock<util::SharedMutex> lk, const RegistryType& registry);
    const RegistryType& registry;

   private:
    std::shared_lock<util::SharedMutex> registry_lk_;
  };

  // Helper function used for printing users via ACL LIST
  RegistryViewWithLock GetRegistryWithLock() const;

  // Helper class for accessing a user with a ReadLock outside the scope of UserRegistry
  class UserWithWriteLock {
   public:
    UserWithWriteLock(std::unique_lock<util::SharedMutex> lk, const User& user, bool exists);
    const User& user;
    const bool exists;

   private:
    std::unique_lock<util::SharedMutex> registry_lk_;
  };

  UserWithWriteLock MaybeAddAndUpdateWithLock(std::string_view username, User::UpdateRequest req);

 private:
  RegistryType registry_;
  // TODO add abseil mutex attributes
  mutable util::SharedMutex mu_;
};

}  // namespace dfly::acl
