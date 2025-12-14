// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/flags.h"
#include "facade/facade_types.h"
#include "server/tx_base.h"

ABSL_DECLARE_FLAG(uint32_t, dbnum);

namespace dfly {

using facade::CmdArgList;
using facade::OpResult;

class CommandRegistry;
struct CommandContext;

class GenericFamily {
 public:
  static void Register(CommandRegistry* registry);

  // Accessed by Service::Exec and Service::Watch as an utility.
  static OpResult<uint32_t> OpExists(const OpArgs& op_args, const ShardArgs& keys);
  static OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys, bool async);

 private:
  static void Del(CmdArgList args, CommandContext* cmd_cntx);
  static void Unlink(CmdArgList args, CommandContext* cmd_cntx);
  static void Ping(CmdArgList args, CommandContext* cmd_cntx);
  static void Exists(CmdArgList args, CommandContext* cmd_cntx);
  static void Expire(CmdArgList args, CommandContext* cmd_cntx);
  static void ExpireAt(CmdArgList args, CommandContext* cmd_cntx);
  static void Persist(CmdArgList args, CommandContext* cmd_cntx);
  static void Keys(CmdArgList args, CommandContext* cmd_cntx);
  static void PexpireAt(CmdArgList args, CommandContext* cmd_cntx);
  static void Pexpire(CmdArgList args, CommandContext* cmd_cntx);
  static void Stick(CmdArgList args, CommandContext* cmd_cntx);
  static void Sort(CmdArgList args, CommandContext* cmd_cntx);
  static void Sort_RO(CmdArgList args, CommandContext* cmd_cntx);
  static void Move(CmdArgList args, CommandContext* cmd_cntx);

  static void Rename(CmdArgList args, CommandContext* cmd_cntx);
  static void RenameNx(CmdArgList args, CommandContext* cmd_cntx);
  static void Copy(CmdArgList args, CommandContext* cmd_cntx);
  static void ExpireTime(CmdArgList args, CommandContext* cmd_cntx);
  static void PExpireTime(CmdArgList args, CommandContext* cmd_cntx);
  static void Ttl(CmdArgList args, CommandContext* cmd_cntx);
  static void Pttl(CmdArgList args, CommandContext* cmd_cntx);

  static void Echo(CmdArgList args, CommandContext* cmd_cntx);
  static void Select(CmdArgList args, CommandContext* cmd_cntx);
  static void Scan(CmdArgList args, CommandContext* cmd_cntx);
  static void Time(CmdArgList args, CommandContext* cmd_cntx);
  static void Type(CmdArgList args, CommandContext* cmd_cntx);
  static void Dump(CmdArgList args, CommandContext* cmd_cntx);
  static void Restore(CmdArgList args, CommandContext* cmd_cntx);
  static void RandomKey(CmdArgList args, CommandContext* cmd_cntx);
  static void FieldTtl(CmdArgList args, CommandContext* cmd_cntx);
  static void FieldExpire(CmdArgList args, CommandContext* cmd_cntx);
};

}  // namespace dfly
