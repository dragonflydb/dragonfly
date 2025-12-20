// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "facade/facade_types.h"
#include "facade/parsed_command.h"
#include "util/fiber_socket_base.h"

namespace util {
class HttpListenerBase;
}  // namespace util

namespace facade {

class ConnectionContext;
class Connection;
class SinkReplyBuilder;
class MCReplyBuilder;

enum class DispatchResult : uint8_t { OK, OOM, ERROR };

struct DispatchManyResult {
  uint32_t processed;  // how many commands out of passed were actually processed

  // whether to account the processed commands in stats. This is needed to consistently
  // account commands that were included based on squash_stats_latency_lower_limit filter.
  bool account_in_stats;
};

class ServiceInterface {
 public:
  virtual ~ServiceInterface() {
  }

  virtual DispatchResult DispatchCommand(ParsedArgs args, SinkReplyBuilder* builder,
                                         ConnectionContext* cntx) = 0;

  virtual DispatchManyResult DispatchManyCommands(std::function<ParsedArgs()> arg_gen,
                                                  unsigned count, SinkReplyBuilder* builder,
                                                  ConnectionContext* cntx) = 0;

  virtual void DispatchMC(ParsedCommand* cmd) = 0;

  virtual ConnectionContext* CreateContext(Connection* owner) = 0;

  virtual ParsedCommand* AllocateParsedCommand() {
    return new ParsedCommand();
  }

  virtual void ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) {
  }

  virtual void OnConnectionClose(ConnectionContext* cntx) {
  }

  struct ContextInfo {
    std::string Format() const;

    unsigned db_index;
    bool async_dispatch, conn_closing, subscribers, blocked;
  };

  virtual ContextInfo GetContextInfo(ConnectionContext* cntx) const {
    return {};
  }
};

}  // namespace facade
