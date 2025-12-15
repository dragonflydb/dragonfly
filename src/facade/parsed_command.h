// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "common/backed_args.h"
#include "facade/memcache_parser.h"

namespace facade {

class ConnectionContext;
class SinkReplyBuilder;

class ParsedCommand : public cmn::BackedArguments {
 protected:
  SinkReplyBuilder* rb_ = nullptr;  // either RedisReplyBuilder or MCReplyBuilder
  ConnectionContext* conn_cntx_ = nullptr;

  std::unique_ptr<MemcacheParser::Command> mc_cmd;  // only for memcache protocol

 public:
  ParsedCommand() = default;
  ParsedCommand(SinkReplyBuilder* rb, ConnectionContext* conn_cntx)
      : rb_{rb}, conn_cntx_{conn_cntx} {
  }

  void CreateMemcacheCommand() {
    mc_cmd = std::make_unique<MemcacheParser::Command>();
    mc_cmd->backed_args = this;
  }

  void Init(SinkReplyBuilder* rb, ConnectionContext* conn_cntx) {
    rb_ = rb;
    conn_cntx_ = conn_cntx;
  }

  SinkReplyBuilder* rb() const {
    return rb_;
  }

  MemcacheParser::Command* mc_command() const {
    return mc_cmd.get();
  }
};

static_assert(sizeof(ParsedCommand) == 152);

}  // namespace facade
