// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

#include "facade/facade_types.h"
#include "facade/memcache_parser.h"
#include "util/fiber_socket_base.h"

namespace util {
class HttpListenerBase;
}  // namespace util

namespace facade {

class ConnectionContext;
class Connection;
struct ConnectionStats;

class ServiceInterface {
 public:
  virtual ~ServiceInterface() {
  }

  virtual void DispatchCommand(CmdArgList args, ConnectionContext* cntx) = 0;

  virtual void DispatchManyCommands(absl::Span<CmdArgList> args_list, ConnectionContext* cntx) = 0;

  virtual void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                          ConnectionContext* cntx) = 0;

  virtual ConnectionContext* CreateContext(util::FiberSocketBase* peer, Connection* owner) = 0;

  virtual ConnectionStats* GetThreadLocalConnectionStats() = 0;

  virtual void ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) {
  }

  virtual void OnClose(ConnectionContext* cntx) {
  }

  virtual std::string GetContextInfo(ConnectionContext* cntx) {
    return {};
  }
};

}  // namespace facade
