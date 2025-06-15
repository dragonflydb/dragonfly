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
struct CommandContext;

class CompactObj;
using PrimeValue = CompactObj;

class StreamMemTracker {
 public:
  StreamMemTracker();

  void UpdateStreamSize(PrimeValue& pv) const;

 private:
  size_t start_size_{0};
};

class StreamFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void XAdd(CmdArgList args, const CommandContext& cmd_cntx);
  static void XClaim(CmdArgList args, const CommandContext& cmd_cntx);
  static void XDel(CmdArgList args, const CommandContext& cmd_cntx);
  static void XGroup(CmdArgList args, const CommandContext& cmd_cntx);
  static void XInfo(CmdArgList args, const CommandContext& cmd_cntx);
  static void XLen(CmdArgList args, const CommandContext& cmd_cntx);
  static void XPending(CmdArgList args, const CommandContext& cmd_cntx);
  static void XRevRange(CmdArgList args, const CommandContext& cmd_cntx);
  static void XRange(CmdArgList args, const CommandContext& cmd_cntx);
  static void XRead(CmdArgList args, const CommandContext& cmd_cntx);
  static void XReadGroup(CmdArgList args, const CommandContext& cmd_cntx);
  static void XSetId(CmdArgList args, const CommandContext& cmd_cntx);
  static void XTrim(CmdArgList args, const CommandContext& cmd_cntx);
  static void XAck(CmdArgList args, const CommandContext& cmd_cntx);
  static void XAutoClaim(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
