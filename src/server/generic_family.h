// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/flags.h"
#include "facade/cmd_arg_parser.h"
#include "facade/facade_types.h"
#include "server/tx_base.h"

ABSL_DECLARE_FLAG(uint32_t, dbnum);

namespace dfly {

using facade::CmdArgList;
using facade::OpResult;

class GenericFamily {
 public:
  static void Register(CommandRegistry* registry);

  // Accessed by Service::Exec and Service::Watch as an utility.
  static OpResult<uint32_t> OpExists(const OpArgs& op_args, const ShardArgs& keys);
  static OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys, bool async);

 private:
  static void Delex(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Ping(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Exists(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Expire(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void ExpireAt(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Persist(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Keys(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void PexpireAt(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Pexpire(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Stick(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Sort(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Sort_RO(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Move(facade::CmdArgParser parser, CommandContext* cmd_cntx);

  static void Rename(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void RenameNx(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Copy(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void ExpireTime(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void PExpireTime(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Ttl(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Pttl(facade::CmdArgParser parser, CommandContext* cmd_cntx);

  static void Echo(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Select(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Scan(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Rm(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Time(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Type(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Dump(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void Restore(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void RandomKey(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void FieldTtl(facade::CmdArgParser parser, CommandContext* cmd_cntx);
  static void FieldExpire(facade::CmdArgParser parser, CommandContext* cmd_cntx);
};

}  // namespace dfly
