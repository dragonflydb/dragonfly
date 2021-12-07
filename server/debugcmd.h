// Copyright 2021, Roman Gershman.  All rights reserved.
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

  EngineShardSet* ess_;
  ConnectionContext* cntx_;
};

}  // namespace dfly
