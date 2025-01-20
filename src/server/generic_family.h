// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/flags.h"
#include "facade/facade_types.h"
#include "server/tx_base.h"

ABSL_DECLARE_FLAG(uint32_t, dbnum);

namespace facade {
class SinkReplyBuilder;
};

namespace dfly {

using facade::CmdArgList;
using facade::OpResult;

class CommandRegistry;
class Transaction;
struct CommandContext;

class GenericFamily {
 public:
  static void Register(CommandRegistry* registry);

  // Accessed by Service::Exec and Service::Watch as an utility.
  static OpResult<uint32_t> OpExists(const OpArgs& op_args, const ShardArgs& keys);
  static OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Del(CmdArgList args, const CommandContext& cmd_cntx);
  static void Unlink(CmdArgList args, const CommandContext& cmd_cntx);
  static void Ping(CmdArgList args, const CommandContext& cmd_cntx);
  static void Exists(CmdArgList args, const CommandContext& cmd_cntx);
  static void Expire(CmdArgList args, const CommandContext& cmd_cntx);
  static void ExpireAt(CmdArgList args, const CommandContext& cmd_cntx);
  static void Persist(CmdArgList args, const CommandContext& cmd_cntx);
  static void Keys(CmdArgList args, const CommandContext& cmd_cntx);
  static void PexpireAt(CmdArgList args, const CommandContext& cmd_cntx);
  static void Pexpire(CmdArgList args, const CommandContext& cmd_cntx);
  static void Stick(CmdArgList args, const CommandContext& cmd_cntx);
  static void Sort(CmdArgList args, const CommandContext& cmd_cntx);
  static void Move(CmdArgList args, const CommandContext& cmd_cntx);

  static void Rename(CmdArgList args, const CommandContext& cmd_cntx);
  static void RenameNx(CmdArgList args, const CommandContext& cmd_cntx);
  static void ExpireTime(CmdArgList args, const CommandContext& cmd_cntx);
  static void PExpireTime(CmdArgList args, const CommandContext& cmd_cntx);
  static void Ttl(CmdArgList args, const CommandContext& cmd_cntx);
  static void Pttl(CmdArgList args, const CommandContext& cmd_cntx);

  static void Echo(CmdArgList args, const CommandContext& cmd_cntx);
  static void Select(CmdArgList args, const CommandContext& cmd_cntx);
  static void Scan(CmdArgList args, const CommandContext& cmd_cntx);
  static void Time(CmdArgList args, const CommandContext& cmd_cntx);
  static void Type(CmdArgList args, const CommandContext& cmd_cntx);
  static void Dump(CmdArgList args, const CommandContext& cmd_cntx);
  static void Restore(CmdArgList args, const CommandContext& cmd_cntx);
  static void RandomKey(CmdArgList args, const CommandContext& cmd_cntx);
  static void FieldTtl(CmdArgList args, const CommandContext& cmd_cntx);
  static void FieldExpire(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
