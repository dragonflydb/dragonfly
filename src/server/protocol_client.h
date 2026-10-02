// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/strings/escaping.h>
#include <time.h>

#include <cstdint>
#include <queue>
#include <variant>

#include "facade/facade_types.h"
#include "facade/resp_parser.h"
#include "io/io_buf.h"
#include "server/execution_state.h"
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

bool ValidateClientTlsFlags();

// A helper class for implementing a Redis client that talks to a redis server.
// This class should be inherited from.
class ProtocolClient {
 public:
#ifdef DFLY_USE_SSL
  using SSL_CTX = struct ssl_ctx_st;
#endif

  ProtocolClient(std::string master_host, uint16_t port);
  virtual ~ProtocolClient();

  // First Shutdown() the socket and immediately Close() it.
  // Any attempt for IO in the socket after Close() will crash with CHECK fail.
  void CloseSocket();

  // Shutdown the underline socket but do not Close() it. By decoupling this, api
  // callers can shutdown the socket, wait for the relevant flows to gracefully exit
  // (by observing during an IO operation that the socket was shut down) and then finally
  // Close() the socket.
  void ShutdownSocket();

  uint64_t LastIoTime() const;
  void TouchIoTime();

  const std::string& GetHost() const {
    return server().host;
  };

  uint16_t GetPort() const {
    return server().port;
  };

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
  std::error_code ConnectAndAuth(std::chrono::milliseconds connect_timeout_ms,
                                 ExecutionState* cntx);

  void DefaultErrorHandler(const GenericError& err);

  struct ReadCommandRes {
    uint32_t total_read = 0;     // wire bytes this command occupied
    bool has_more_data = false;  // parser still holds buffered input
  };

  // Reads one flat RESP array into dest. buffer is only a staging area - the parser copies
  // everything it is fed, so this drains buffer completely and the caller must not consume it.
  io::Result<ReadCommandRes> ReadRespCommand(base::IoBuf* buffer, cmn::BackedArguments* dest);

  // Reads one reply. Bytes received after it stay buffered in the parser (see UnparsedInput) and
  // are returned by the next call. The bytes read while waiting are saved in last_resp_ for
  // diagnostics.
  io::Result<facade::RESPObj> TakeRespReply(uint32_t timeout);

  // Bytes received after the last reply that the parser has not consumed yet, e.g. the start of a
  // binary stream that follows it. Valid until the next read or reset.
  std::string_view UnparsedInput() const {
    return resp_parser_.BufferedInput();
  }

  std::error_code ReadLine(base::IoBuf* io_buf, std::string_view* line);

  // Returns the payload of a simple (+) or bulk ($) string reply, nullopt for any other reply.
  static std::optional<std::string_view> ReplyString(const facade::RESPObj& reply);

  // Send command, update last_io_time, return error.
  std::error_code SendCommand(std::string_view command);
  // Send command and return an owning reply parsed with RESPParser.
  io::Result<facade::RESPObj> SendCommandAndTakeReply(std::string_view command);

  const ServerContext& server() const {
    return server_context_;
  }

  // Start reading replies with RESPParser, discarding any previous parser state and input.
  void ResetReplyParser();
  void ResetCommandParser();

  auto* Proactor() const {
    return sock_->proactor();
  }

  util::FiberSocketBase* Sock() const {
    return sock_.get();
  }

  // Socket diagnostics string for error logs. Evaluates the socket exactly once and tolerates a
  // socket that was never created (e.g. ConnectAndAuth() refusing on a dead context).
  std::string SockInfo() const;

  // Bytes currently sitting unread in the socket's kernel receive buffer, or -1 if unavailable
  // (no socket yet, or the ioctl failed). Locks sock_mu_ so it can't race with ConnectAndAuth()/
  // ShutdownSocketImpl() replacing or destroying sock_ concurrently.
  int GetSocketUnreadBytes();

 private:
  std::error_code Recv(util::FiberSocketBase* input, base::IoBuf* dest);

  void ShutdownSocketImpl(bool should_close);

  ServerContext server_context_;

  base::IoBuf resp_buf_;

  facade::RESPParser resp_parser_;

  std::unique_ptr<util::FiberSocketBase> sock_;
  util::fb2::Mutex sock_mu_;

 protected:
  static uint64_t TimeSec() {
    return time(nullptr);
  }

  std::string last_cmd_;
  std::string last_resp_;

  // Seconds (CLOCK_MONOTONIC_COARSE) — consumed only by master_last_io_sec.
  std::atomic<uint64_t> last_io_time_ = 0;

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
