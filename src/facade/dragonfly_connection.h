// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>

#include <deque>
#include <variant>

#include "base/io_buf.h"
#include "facade/facade_types.h"
#include "facade/resp_expr.h"
#include "util/connection.h"
#include "util/fibers/event_count.h"
#include "util/http/http_handler.h"

typedef struct ssl_ctx_st SSL_CTX;
typedef struct mi_heap_s mi_heap_t;

namespace facade {

class ConnectionContext;
class RedisParser;
class ServiceInterface;
class MemcacheParser;

class Connection : public util::Connection {
 public:
  Connection(Protocol protocol, util::HttpListenerBase* http_listener,
             SSL_CTX* ctx, ServiceInterface* service);
  ~Connection();

  using error_code = std::error_code;
  using ShutdownCb = std::function<void()>;
  using ShutdownHandle = unsigned;

  ShutdownHandle RegisterShutdownHook(ShutdownCb cb);
  void UnregisterShutdownHook(ShutdownHandle id);

  Protocol protocol() const {
    return protocol_;
  }

  using BreakerCb = std::function<void(uint32_t)>;
  void RegisterOnBreak(BreakerCb breaker_cb);

 protected:
  void OnShutdown() override;

 private:
  enum ParserStatus { OK, NEED_MORE, ERROR };

  void HandleRequests() final;

  //
  io::Result<bool> CheckForHttpProto(util::FiberSocketBase* peer);

  void ConnectionFlow(util::FiberSocketBase* peer);
  std::variant<std::error_code, ParserStatus> IoLoop(util::FiberSocketBase* peer);

  void DispatchFiber(util::FiberSocketBase* peer);

  ParserStatus ParseRedis();
  ParserStatus ParseMemcache();

  base::IoBuf io_buf_;
  std::unique_ptr<RedisParser> redis_parser_;
  std::unique_ptr<MemcacheParser> memcache_parser_;
  util::HttpListenerBase* http_listener_;
  SSL_CTX* ctx_;
  ServiceInterface* service_;

  std::unique_ptr<ConnectionContext> cc_;

  struct Request;

  static Request* FromArgs(RespVec args, mi_heap_t* heap);

  std::deque<Request*> dispatch_q_;  // coordinated via evc_.
  util::fibers_ext::EventCount evc_;
  unsigned parser_error_ = 0;
  Protocol protocol_;
  struct Shutdown;
  std::unique_ptr<Shutdown> shutdown_;
  BreakerCb breaker_cb_;
};

}  // namespace facade
