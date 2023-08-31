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
  AclFamily() = default;

  void Register(CommandRegistry* registry);
  void Init(facade::Listener* listener);

 private:
  void Acl(CmdArgList args, ConnectionContext* cntx);
  void List(CmdArgList args, ConnectionContext* cntx);
  void SetUser(CmdArgList args, ConnectionContext* cntx);
  void DelUser(CmdArgList args, ConnectionContext* cntx);

  void StreamUpdatesToAllProactorConnections(std::string_view user, uint32_t update_cat);

  // Helper function that closes all open connection from the deleted user
  void EvictOpenConnectionsOnAllProactors(std::string_view user);

  facade::Listener* main_listener_{nullptr};
};

}  // namespace acl
}  // namespace dfly
