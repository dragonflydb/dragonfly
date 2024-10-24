// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class CommandRegistry;
class ConnectionContext;

class BloomFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void Reserve(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Add(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void MAdd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void Exists(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void MExists(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
