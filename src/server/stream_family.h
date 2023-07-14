// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"
#include "server/common.h"

namespace dfly {

class CommandRegistry;
class ConnectionContext;

class StreamFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void XAdd(CmdArgList args, ConnectionContext* cntx);
  static void XDel(CmdArgList args, ConnectionContext* cntx);
  static void XGroup(CmdArgList args, ConnectionContext* cntx);
  static void XInfo(CmdArgList args, ConnectionContext* cntx);
  static void XLen(CmdArgList args, ConnectionContext* cntx);
  static void XRevRange(CmdArgList args, ConnectionContext* cntx);
  static void XRange(CmdArgList args, ConnectionContext* cntx);
  static void XRead(CmdArgList args, ConnectionContext* cntx);
  static void XReadGroup(CmdArgList args, ConnectionContext* cntx);
  static void XSetId(CmdArgList args, ConnectionContext* cntx);
  static void XTrim(CmdArgList args, ConnectionContext* cntx);
  static void XRangeGeneric(CmdArgList args, bool is_rev, ConnectionContext* cntx);
};

}  // namespace dfly
