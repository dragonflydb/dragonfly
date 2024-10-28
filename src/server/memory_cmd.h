// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/conn_context.h"

namespace dfly {

class ServerFamily;

class MemoryCmd {
 public:
  MemoryCmd(ServerFamily* owner, facade::SinkReplyBuilder* builder, ConnectionContext* cntx);

  void Run(CmdArgList args);

 private:
  void Stats();
  void MallocStats();
  void ArenaStats(CmdArgList args);
  void Usage(std::string_view key);
  void Track(CmdArgList args);

  ConnectionContext* cntx_;
  ServerFamily* owner_;
  facade::SinkReplyBuilder* builder_;
};

}  // namespace dfly
