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

class ConnectionContext;
class CommandRegistry;
class Transaction;

class GenericFamily {
 public:
  static void Register(CommandRegistry* registry);

  // Accessed by Service::Exec and Service::Watch as an utility.
  static OpResult<uint32_t> OpExists(const OpArgs& op_args, const ShardArgs& keys);
  static OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Del(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Ping(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                   ConnectionContext* cntx);
  static void Exists(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Expire(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ExpireAt(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Persist(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Keys(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                   ConnectionContext* cntx);
  static void PexpireAt(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Pexpire(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Stick(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Sort(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Move(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Copy(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);

  static void Rename(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void RenameNx(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ExpireTime(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void PExpireTime(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Ttl(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Pttl(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);

  static void Echo(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Select(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                     ConnectionContext* cntx);
  static void Scan(CmdArgList args, Transaction*, SinkReplyBuilder* builder,
                   ConnectionContext* cntx);
  static void Time(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Type(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Dump(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Restore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void RandomKey(CmdArgList args, Transaction*, SinkReplyBuilder* builder,
                        ConnectionContext* cntx);
  static void FieldTtl(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FieldExpire(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
