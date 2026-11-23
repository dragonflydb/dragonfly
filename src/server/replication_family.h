// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

#include "facade/cmd_arg_parser.h"
#include "facade/facade_types.h"
#include "server/replica_manager.h"

namespace dfly {

class CommandContext;
class CommandRegistry;
class DflyCmd;

// Replication commands. ReplicaManager (replica side) and DflyCmd (master side) stay
// command-free.
class ReplicationFamily {
 public:
  ReplicationFamily(ReplicaManager* replica_manager, DflyCmd* dfly_cmd);

  // Registers into the current command family, does not start a new one.
  void Register(CommandRegistry* registry);

  // Sets the server to replicate another instance. Does not flush the database beforehand!
  void Replicate(std::string_view host, std::string_view port);

 private:
  void ReplicaOf(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void AddReplicaOf(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void ReplTakeOver(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void ReplConf(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Wait(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Role(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  void Dfly(facade::CmdArgParser parser, CommandContext* cmd_cntx);

  void ReplicaOfInternal(facade::ParsedArgs args, CommandContext* cmd_cntx,
                         ReplicaManager::ActionOnConnectionFail on_error);

  ReplicaManager* replica_manager_;
  DflyCmd* dfly_cmd_;
};

}  // namespace dfly
