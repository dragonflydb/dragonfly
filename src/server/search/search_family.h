// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "base/mutex.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {
class CommandRegistry;
class ConnectionContext;

class SearchFamily {
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void FtCreate(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                       ConnectionContext* cntx);
  static void FtAlter(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtDropIndex(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtInfo(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtList(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtSearch(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtProfile(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtAggregate(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void FtTagVals(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);

 public:
  static void Register(CommandRegistry* registry);
};

}  // namespace dfly
