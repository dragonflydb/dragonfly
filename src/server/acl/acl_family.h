// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "facade/dragonfly_listener.h"
#include "helio/util/proactor_pool.h"
#include "server/common.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

namespace acl {

class AclFamily final {
 public:
  explicit AclFamily(util::ProactorPool& pp);

  void Register(CommandRegistry* registry);
  void Init(std::vector<facade::Listener*> listeners);

 private:
  void Acl(CmdArgList args, ConnectionContext* cntx);
  void List(CmdArgList args, ConnectionContext* cntx);
  void SetUser(CmdArgList args, ConnectionContext* cntx);
  void DelUser(CmdArgList args, ConnectionContext* cntx);

  // Helper function that updates all open connections and their
  // respective ACL fields on all the available proactor threads
  void StreamUpdatesToAllProactorConnections(std::string_view user, uint32_t update_cat);

  // Helper function that closes all open connection from the deleted user
  void EvictOpenConnectionsOnAllProactors(std::string_view user);

  std::vector<facade::Listener*> listeners_;
  util::ProactorPool& pp_;
};

}  // namespace acl
}  // namespace dfly
