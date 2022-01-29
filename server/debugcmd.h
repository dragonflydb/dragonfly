// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/conn_context.h"

namespace dfly {

class EngineShardSet;

class DebugCmd {
 public:
  DebugCmd(EngineShardSet* ess, ConnectionContext* cntx);

  void Run(CmdArgList args);

 private:
  void Populate(CmdArgList args);
  void PopulateRangeFiber(uint64_t from, uint64_t len, std::string_view prefix, unsigned value_len);
  void Reload(CmdArgList args);

  EngineShardSet* ess_;
  ConnectionContext* cntx_;
};

}  // namespace dfly
