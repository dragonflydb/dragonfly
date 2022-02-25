// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>

#include <deque>
#include <variant>

#include "base/io_buf.h"
#include "core/resp_expr.h"
#include "server/common_types.h"
#include "util/connection.h"
#include "util/fibers/event_count.h"

typedef struct ssl_ctx_st SSL_CTX;
typedef struct mi_heap_s mi_heap_t;

namespace dfly {

class ConnectionContext;
class RedisParser;
class Service;
class MemcacheParser;

class Connection : public util::Connection {
 public:
  Connection(Protocol protocol, Service* service, SSL_CTX* ctx);
  ~Connection();

  using error_code = std::error_code;
  using ShutdownCb = std::function<void()>;
  using ShutdownHandle = unsigned;

  ShutdownHandle RegisterShutdownHook(ShutdownCb cb);
  void UnregisterShutdownHook(ShutdownHandle id);

  Protocol protocol() const {
    return protocol_;
  }

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
  Service* service_;
  SSL_CTX* ctx_;
  std::unique_ptr<ConnectionContext> cc_;

  struct Request;

  static Request* FromArgs(RespVec args, mi_heap_t* heap);

  std::deque<Request*> dispatch_q_;  // coordinated via evc_.
  util::fibers_ext::EventCount evc_;
  unsigned parser_error_ = 0;
  Protocol protocol_;
  struct Shutdown;
  std::unique_ptr<Shutdown> shutdown_;
};

}  // namespace dfly
