// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "base/mutex.h"
#include "server/common.h"

namespace dfly {
class CommandRegistry;
class ConnectionContext;

class SearchFamily {
  static void FtCreate(CmdArgList args, ConnectionContext* cntx);
  static void FtDropIndex(CmdArgList args, ConnectionContext* cntx);
  static void FtInfo(CmdArgList args, ConnectionContext* cntx);
  static void FtList(CmdArgList args, ConnectionContext* cntx);
  static void FtSearch(CmdArgList args, ConnectionContext* cntx);

 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
