// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class CommandRegistry;
class ConnectionContext;

class StreamFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void XAdd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XClaim(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XDel(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XGroup(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XInfo(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                    ConnectionContext* cntx);
  static void XLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XPending(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XRevRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XRead(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                    ConnectionContext* cntx);
  static void XReadGroup(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                         ConnectionContext* cntx);
  static void XSetId(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XTrim(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XAck(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void XAutoClaim(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
