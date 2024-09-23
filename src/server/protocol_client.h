// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/strings/escaping.h>

#include <queue>
#include <variant>

#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "io/io_buf.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/version.h"
#include "util/fiber_socket_base.h"

#ifdef DFLY_USE_SSL
#include <openssl/ssl.h>
#endif

namespace dfly {

class Service;
class ConnectionContext;
class JournalExecutor;
struct JournalReader;

void ValidateClientTlsFlags();

// A helper class for implementing a Redis client that talks to a redis server.
// This class should be inherited from.
class ProtocolClient {
 public:
#ifdef DFLY_USE_SSL
  using SSL_CTX = struct ssl_ctx_st;
#endif

  ProtocolClient(std::string master_host, uint16_t port);
  virtual ~ProtocolClient();

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

  // Constructing using a fully initialized ServerContext allows to skip
  // the DNS resolution step.
  explicit ProtocolClient(ServerContext context);

  std::error_code ResolveHostDns();
  // Connect to master and authenticate if needed.
  std::error_code ConnectAndAuth(std::chrono::milliseconds connect_timeout_ms, Context* cntx);

  void DefaultErrorHandler(const GenericError& err);

  struct ReadRespRes {
    uint32_t total_read;
    uint32_t left_in_buffer;
  };

  // This function uses parser_ and cmd_args_ in order to consume a single response
  // from the sock_. The output will reside in resp_args_.
  // For error reporting purposes, the parsed command would be in last_resp_ if copy_msg is true.
  // If io_buf is not given, a internal temporary buffer will be used.
  // It is the responsibility of the caller to call buffer->ConsumeInput(rv.left_in_buffer) when it
  // is done with the result of the call; Calling ConsumeInput may invalidate the data in the result
  // if the buffer relocates.
  io::Result<ReadRespRes> ReadRespReply(base::IoBuf* buffer = nullptr, bool copy_msg = true);
  io::Result<ReadRespRes> ReadRespReply(uint32_t timeout);

  std::error_code ReadLine(base::IoBuf* io_buf, std::string_view* line);

  // Check if reps_args contains a simple reply.
  bool CheckRespIsSimpleReply(std::string_view reply) const;

  // Check resp_args contains the following types at front.
  bool CheckRespFirstTypes(std::initializer_list<facade::RespExpr::Type> types) const;

  // Send command, update last_io_time, return error.
  std::error_code SendCommand(std::string_view command);
  // Send command, read response into resp_args_.
  std::error_code SendCommandAndReadResponse(std::string_view command);

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

  util::FiberSocketBase* Sock() const {
    return sock_.get();
  }

 private:
  ServerContext server_context_;

  std::unique_ptr<facade::RedisParser> parser_;
  facade::RespVec resp_args_;
  base::IoBuf resp_buf_;

  std::unique_ptr<util::FiberSocketBase> sock_;
  util::fb2::Mutex sock_mu_;

 protected:
  Context cntx_;  // context for tasks in replica.

  std::string last_cmd_;
  std::string last_resp_;

  uint64_t last_io_time_ = 0;  // in ns, monotonic clock.

#ifdef DFLY_USE_SSL

  void MaybeInitSslCtx();

  SSL_CTX* ssl_ctx_{nullptr};
#else
  void* ssl_ctx_{nullptr};
#endif
};

}  // namespace dfly

/**
 * A convenience macro to use with ProtocolClient instances for protocol input validation.
 */
#define PC_RETURN_ON_BAD_RESPONSE_T(T, x)                                                      \
  do {                                                                                         \
    if (!(x)) {                                                                                \
      LOG(ERROR) << "Bad response to \"" << last_cmd_ << "\": \"" << absl::CEscape(last_resp_) \
                 << "\"";                                                                      \
      return (T)(std::make_error_code(errc::bad_message));                                     \
    }                                                                                          \
  } while (false)

#define PC_RETURN_ON_BAD_RESPONSE(x) PC_RETURN_ON_BAD_RESPONSE_T(std::error_code, x)
