// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common_types.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

class HSetFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void HDel(CmdArgList args,  ConnectionContext* cntx);
  static void HLen(CmdArgList args,  ConnectionContext* cntx);
  static void HExists(CmdArgList args,  ConnectionContext* cntx);
  static void HGet(CmdArgList args,  ConnectionContext* cntx);
  static void HIncrBy(CmdArgList args,  ConnectionContext* cntx);
  static void HSet(CmdArgList args,  ConnectionContext* cntx);
  static void HSetNx(CmdArgList args,  ConnectionContext* cntx);
  static void HStrLen(CmdArgList args,  ConnectionContext* cntx);
};

}  // namespace dfly
