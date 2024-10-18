// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class ConnectionContext;
class CommandRegistry;
class Transaction;

using facade::CmdArgList;

class StringFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Append(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void Decr(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void DecrBy(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void Get(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void GetDel(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                     ConnectionContext* cntx);
  static void GetRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void GetSet(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                     ConnectionContext* cntx);
  static void GetEx(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                    ConnectionContext* cntx);
  static void Incr(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void IncrBy(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void IncrByFloat(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void MGet(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb, ConnectionContext* cntx);
  static void MSet(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb, ConnectionContext* cntx);
  static void MSetNx(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                     ConnectionContext* cntx);

  static void Set(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb, ConnectionContext* cntx);
  static void SetEx(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                    ConnectionContext* cntx);
  static void SetNx(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                    ConnectionContext* cntx);
  static void SetRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void StrLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void Prepend(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
  static void PSetEx(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb,
                     ConnectionContext* cntx);

  static void ClThrottle(CmdArgList args, Transaction* tx, SinkReplyBuilder* rb);
};

}  // namespace dfly
