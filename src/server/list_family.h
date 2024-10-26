// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

using facade::OpResult;

class ConnectionContext;
class CommandRegistry;
class Transaction;

class ListFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void LPush(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LPushX(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void RPush(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void RPushX(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LPop(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void RPop(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void BLPop(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                    ConnectionContext* cntx);
  static void BRPop(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                    ConnectionContext* cntx);
  static void LLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LPos(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LIndex(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LInsert(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LTrim(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LRem(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LSet(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void LMove(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
