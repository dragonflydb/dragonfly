// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"
#include "server/engine_shard_set.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;
using facade::OpResult;
using facade::OpStatus;

class JsonFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Get(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
