// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "base/mutex.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {
class ServerFamily;
class CommandRegistry;
struct CommandContext;

class SearchFamily {
 public:
  explicit SearchFamily(ServerFamily* server_family);

  void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  void FtCreate(CmdArgList args, const CommandContext& cmd_cntx);
  void FtAlter(CmdArgList args, const CommandContext& cmd_cntx);
  void FtDropIndex(CmdArgList args, const CommandContext& cmd_cntx);
  void FtInfo(CmdArgList args, const CommandContext& cmd_cntx);
  void FtList(CmdArgList args, const CommandContext& cmd_cntx);
  void FtSearch(CmdArgList args, const CommandContext& cmd_cntx);
  void FtProfile(CmdArgList args, const CommandContext& cmd_cntx);
  void FtConfig(CmdArgList args, const CommandContext& cmd_cntx);
  void FtAggregate(CmdArgList args, const CommandContext& cmd_cntx);
  void FtTagVals(CmdArgList args, const CommandContext& cmd_cntx);

  // Uses server_family_ to synchronize config changes
  // Should not be registered
  void FtConfigSet(CmdArgList args, const CommandContext& cmd_cntx);

  ServerFamily* server_family_;
};

}  // namespace dfly
