// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class ConnectionContext;
class CommandRegistry;

class JsonFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Get(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void MGet(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Type(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void StrLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ObjLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ArrLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Toggle(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void NumIncrBy(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void NumMultBy(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Del(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ObjKeys(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void StrAppend(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Clear(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ArrPop(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ArrTrim(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ArrInsert(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ArrAppend(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ArrIndex(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Debug(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Resp(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Set(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void MSet(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Merge(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
