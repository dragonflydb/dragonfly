// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/inlined_vector.h>

#include <boost/fiber/barrier.hpp>
#include <queue>
#include <variant>

#include "base/io_buf.h"
#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/version.h"
#include "util/fiber_socket_base.h"

namespace facade {
class ReqSerializer;
};  // namespace facade

namespace dfly {

class Service;
class ConnectionContext;
class JournalExecutor;
struct JournalReader;

class ProtocolClient {
 public:
  ProtocolClient(std::string master_host, uint16_t port);
  ~ProtocolClient();

  void CloseSocket();  // Close replica sockets.

  uint64_t LastIoTime() const;
  void TouchIoTime();

 protected:
  struct ServerContext {
    std::string host;
    uint16_t port;
    boost::asio::ip::tcp::endpoint endpoint;

    std::string Description() const;
  };
  explicit ProtocolClient(ServerContext context) : server_context_(std::move(context)) {
  }

  std::error_code ResolveMasterDns();  // Resolve master dns
  // Connect to master and authenticate if needed.
  std::error_code ConnectAndAuth(std::chrono::milliseconds connect_timeout_ms, Context* cntx);

  void DefaultErrorHandler(const GenericError& err);

  // This function uses parser_ and cmd_args_ in order to consume a single response
  // from the sock_. The output will reside in resp_args_.
  // For error reporting purposes, the parsed command would be in last_resp_ if copy_msg is true.
  // If io_buf is not given, a temporary buffer will be used.
  io::Result<size_t> ReadRespReply(base::IoBuf* buffer = nullptr, bool copy_msg = true);

  std::error_code ReadLine(base::IoBuf* io_buf, std::string_view* line);

  // Check if reps_args contains a simple reply.
  bool CheckRespIsSimpleReply(std::string_view reply) const;

  // Check resp_args contains the following types at front.
  bool CheckRespFirstTypes(std::initializer_list<facade::RespExpr::Type> types) const;

  // Send command, update last_io_time, return error.
  std::error_code SendCommand(std::string_view command);
  // Send command, read response into resp_args_.
  std::error_code SendCommandAndReadResponse(std::string_view command,
                                             base::IoBuf* buffer = nullptr);

  const ServerContext& server() const {
    return server_context_;
  }

  void ResetParser(bool server_mode);

  auto& LastResponseArgs() {
    return resp_args_;
  }

  auto* Proactor() const {
    return sock_->proactor();
  }

  util::LinuxSocketBase* Sock() const {
    return sock_.get();
  }

 private:
  ServerContext server_context_;

  std::unique_ptr<facade::ReqSerializer> serializer_;
  std::unique_ptr<facade::RedisParser> parser_;
  facade::RespVec resp_args_;
  base::IoBuf resp_buf_;

  std::unique_ptr<util::LinuxSocketBase> sock_;
  Mutex sock_mu_;

 protected:
  Context cntx_;  // context for tasks in replica.

  std::string last_cmd_;
  std::string last_resp_;

  uint64_t last_io_time_ = 0;  // in ns, monotonic clock.
};

}  // namespace dfly
