// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
class ConnectionContext;

class StreamFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void XAdd(CmdArgList args, ConnectionContext* cntx);
  static void XRange(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
