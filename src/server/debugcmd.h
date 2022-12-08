// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/conn_context.h"

namespace dfly {

class EngineShardSet;
class ServerFamily;

class DebugCmd {
 public:
  DebugCmd(ServerFamily* owner, ConnectionContext* cntx);

  void Run(CmdArgList args);

 private:
  void Populate(CmdArgList args);
  void PopulateRangeFiber(uint64_t from, uint64_t len, std::string_view prefix, unsigned value_len,
                          bool populate_random_values);
  void Reload(CmdArgList args);
  void Replica(CmdArgList args);
  void Load(std::string_view filename);
  void Inspect(std::string_view key);
  void Watched();

  ServerFamily& sf_;
  ConnectionContext* cntx_;
};

}  // namespace dfly
