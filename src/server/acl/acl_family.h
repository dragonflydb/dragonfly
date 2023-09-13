// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "facade/dragonfly_listener.h"
#include "facade/facade_types.h"
#include "helio/util/proactor_pool.h"
#include "server/acl/user_registry.h"
#include "server/common.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

namespace acl {

class AclFamily final {
 public:
  explicit AclFamily(UserRegistry* registry);

  void Register(CommandRegistry* registry);
  void Init(facade::Listener* listener, UserRegistry* registry);

 private:
  void Acl(CmdArgList args, ConnectionContext* cntx);
  void List(CmdArgList args, ConnectionContext* cntx);
  void SetUser(CmdArgList args, ConnectionContext* cntx);
  void DelUser(CmdArgList args, ConnectionContext* cntx);
  void WhoAmI(CmdArgList args, ConnectionContext* cntx);
  void Save(CmdArgList args, ConnectionContext* cntx);
  void Load(CmdArgList args, ConnectionContext* cntx);
  bool Load();

  // Helper function that updates all open connections and their
  // respective ACL fields on all the available proactor threads
  using NestedVector = std::vector<std::vector<uint64_t>>;
  void StreamUpdatesToAllProactorConnections(const std::vector<std::string>& user,
                                             const std::vector<uint32_t>& update_cat,
                                             const NestedVector& update_commands);

  // Helper function that closes all open connection from the deleted user
  void EvictOpenConnectionsOnAllProactors(std::string_view user);

  // Helper function that loads the acl state of an acl file into the user registry
  std::optional<facade::ErrorReply> LoadToRegistryFromFile(std::string_view full_path, bool init);

  std::string RegistryToString() const;

  facade::Listener* main_listener_{nullptr};
  UserRegistry* registry_;
};

}  // namespace acl
}  // namespace dfly
