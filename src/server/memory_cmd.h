// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/cmd_arg_parser.h"
#include "server/conn_context.h"

namespace dfly {

class ServerFamily;

class MemoryCmd {
 public:
  MemoryCmd(ServerFamily* owner, CommandContext* cmd_cntx);

  void Run(facade::CmdArgParser parser);

 private:
  void Stats();
  void MallocStats();
  void ArenaStats(facade::CmdArgParser parser);
  void Usage(std::string_view key, bool account_key_memory_usage);
  void Track(facade::CmdArgParser parser);

  CommandContext* cmd_cntx_;
  ServerFamily* owner_;
};

}  // namespace dfly
