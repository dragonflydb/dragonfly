// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
class ConnectionContext;

class BloomFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Reserve(CmdArgList args, ConnectionContext* cntx);
  static void Add(CmdArgList args, ConnectionContext* cntx);
  static void MAdd(CmdArgList args, ConnectionContext* cntx);
  static void Exists(CmdArgList args, ConnectionContext* cntx);
  static void MExists(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
