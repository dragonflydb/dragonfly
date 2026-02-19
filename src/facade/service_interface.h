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

// Controls asynchronicity of command dispatch
enum class AsyncPreference : uint8_t {
  ONLY_SYNC,     // Caller supports only synchronous dispatch
  PREFER_ASYNC,  // Prefer async if available
  ONLY_ASYNC,    // Only async execution is possible (command is dispatched in pipeline)
};

enum class DispatchResult : uint8_t {
  OK,
  OOM,
  ERROR,
  WOULD_BLOCK  // Returned if ONLY_ASYNC was set, but only synchronous execution is possible
};

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

  virtual DispatchResult DispatchCommand(ParsedArgs args, ParsedCommand* cmd, AsyncPreference) = 0;

  virtual DispatchManyResult DispatchManyCommands(
      std::function<std::pair<ParsedArgs, bool*>()> arg_gen, unsigned count,
      SinkReplyBuilder* builder, ConnectionContext* cntx) = 0;

  virtual DispatchResult DispatchMC(ParsedCommand* cmd, AsyncPreference) = 0;

  virtual ConnectionContext* CreateContext(Connection* owner) = 0;

  virtual ParsedCommand* AllocateParsedCommand() = 0;

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
