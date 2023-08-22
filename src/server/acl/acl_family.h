// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {
class ConnectionContext;
class CommandRegistry;

namespace acl {

class AclFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void List(CmdArgList args, ConnectionContext* cntx);
  static void SetUser(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace acl
}  // namespace dfly
