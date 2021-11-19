// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/connection.h"

#include "base/io_buf.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace dfly {

class ConnectionContext;
class RedisParser;
class Service;

class Connection : public util::Connection {
 public:
  Connection(Service* service, SSL_CTX* ctx);
  ~Connection();

  using error_code = std::error_code;
  using ShutdownCb = std::function<void()>;
  using ShutdownHandle = unsigned;

  ShutdownHandle RegisterShutdownHook(ShutdownCb cb);
  void UnregisterShutdownHook(ShutdownHandle id);

 protected:
  void OnShutdown() override;

 private:
  enum ParserStatus { OK, NEED_MORE, ERROR };

  void HandleRequests() final;

  void InputLoop(util::FiberSocketBase* peer);

  ParserStatus ParseRedis(base::IoBuf* buf);

  std::unique_ptr<RedisParser> redis_parser_;
  Service* service_;
  SSL_CTX* ctx_;
  std::unique_ptr<ConnectionContext> cc_;

  unsigned parser_error_ = 0;

  struct Shutdown;
  std::unique_ptr<Shutdown> shutdown_;
};

}  // namespace dfly
