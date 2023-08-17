// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <string>

#include "core/fibers.h"
#include "server/acl/user.h"

namespace dfly {

class UserRegistry {
 public:
  UserRegistry() = default;

  UserRegistry(const UserRegistry&) = delete;
  UserRegistry(UserRegistry&&) = delete;

  // Acquires a write lock of mu_
  // If the user with name `username` does not exist, it's added in the store with
  // the exact fields found in req
  // If the user exists, the bitfields are updated with a `logical and` operation
  // TODO change return time to communicate back results to acl commands
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
  // Used by Auth
  bool AuthUser(std::string_view username, std::string_view password) const;

 private:
  absl::flat_hash_map<std::string, User> registry_;
  // TODO add abseil mutex attributes
  mutable util::SharedMutex mu_;
};

}  // namespace dfly
