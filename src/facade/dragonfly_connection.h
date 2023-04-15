// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <sys/socket.h>

#include <deque>
#include <variant>

#include "base/io_buf.h"
#include "util/connection.h"
#include "util/http/http_handler.h"

//
#include "core/fibers.h"
#include "facade/facade_types.h"
#include "facade/resp_expr.h"

typedef struct ssl_ctx_st SSL_CTX;
typedef struct mi_heap_s mi_heap_t;

// need to declare for older linux distributions like CentOS 7
#ifndef SO_INCOMING_CPU
#define SO_INCOMING_CPU 49
#endif

#ifndef SO_INCOMING_NAPI_ID
#define SO_INCOMING_NAPI_ID 56
#endif

namespace facade {

class ConnectionContext;
class RedisParser;
class ServiceInterface;
class MemcacheParser;

// Connection represents an active connection for a client.
//
// It directly dispatches regular commands from the io-loop.
// For pipelined requests, monitor and pubsub messages it uses
// a separate dispatch queue that is processed on a separate fiber.
class Connection : public util::Connection {
 public:
  Connection(Protocol protocol, util::HttpListenerBase* http_listener, SSL_CTX* ctx,
             ServiceInterface* service);
  ~Connection();

  using BreakerCb = std::function<void(uint32_t)>;
  using ShutdownCb = std::function<void()>;
  using ShutdownHandle = unsigned;

  // PubSub message, either incoming message for active subscription or reply for new subscription.
  struct PubMessage {
    enum Type { kSubscribe, kUnsubscribe, kPublish } type;

    std::string pattern{};  // non-empty for pattern subscriber
    std::shared_ptr<std::string> channel{};
    std::shared_ptr<std::string> message{};

    uint32_t channel_cnt = 0;

    PubMessage(bool add, std::shared_ptr<std::string> channel, uint32_t channel_cnt);
    PubMessage(std::string pattern, std::shared_ptr<std::string> channel,
               std::shared_ptr<std::string> message);

    PubMessage(const PubMessage&) = delete;
    PubMessage& operator=(const PubMessage&) = delete;
    PubMessage(PubMessage&&) = default;
  };

  enum Phase { READ_SOCKET, PROCESS };

 public:
  // Add PubMessage to dispatch queue.
  // Virtual because behaviour is overwritten in test_utils.
  virtual void SendPubMessageAsync(PubMessage pub_msg);

  // Add monitor message to dispatch queue.
  void SendMonitorMessageAsync(std::string monitor_msg);

  // Register hook that is executed on connection shutdown.
  ShutdownHandle RegisterShutdownHook(ShutdownCb cb);

  void UnregisterShutdownHook(ShutdownHandle id);

  // Register hook that is executen when the connection breaks.
  void RegisterBreakHook(BreakerCb breaker_cb);

  // Manually shutdown self.
  void ShutdownSelf();

  static void ShutdownThreadLocal();

  std::string GetClientInfo(unsigned thread_id) const;
  std::string RemoteEndpointStr() const;
  std::string RemoteEndpointAddress() const;
  std::string LocalBindAddress() const;
  uint32_t GetClientId() const;

  Protocol protocol() const {
    return protocol_;
  }

  void SetName(std::string name) {
    name_ = name;
  }

  std::string_view GetName() const {
    return name_;
  }

 protected:
  void OnShutdown() override;
  void OnPreMigrateThread() override;
  void OnPostMigrateThread() override;

 private:
  enum ParserStatus { OK, NEED_MORE, ERROR };

  class Request;
  struct DispatchOperations;
  struct DispatchCleanup;
  struct RequestDeleter;
  struct Shutdown;

  // Requests are allocated on the mimalloc heap and thus require a custom deleter.
  using RequestPtr = std::unique_ptr<Request, RequestDeleter>;

 private:
  // Check protocol and handle connection.
  void HandleRequests() final;

  // Start dispatch fiber and run IoLoop.
  void ConnectionFlow(util::FiberSocketBase* peer);

  // Main loop reading client messages and passing requests to dispatch queue.
  std::variant<std::error_code, ParserStatus> IoLoop(util::FiberSocketBase* peer);

  // Returns true if HTTP header is detected.
  io::Result<bool> CheckForHttpProto(util::FiberSocketBase* peer);

  // Handles events from dispatch queue.
  void DispatchFiber(util::FiberSocketBase* peer);

  // Create new pipeline request, re-use from pool when possible.
  RequestPtr FromArgs(RespVec args, mi_heap_t* heap);

  ParserStatus ParseRedis();
  ParserStatus ParseMemcache();

  void OnBreakCb(int32_t mask);

 private:
  std::deque<RequestPtr> dispatch_q_;  // dispatch queue
  dfly::EventCount evc_;               // dispatch queue waker

  base::IoBuf io_buf_;  // used in io loop and parsers
  std::unique_ptr<RedisParser> redis_parser_;
  std::unique_ptr<MemcacheParser> memcache_parser_;

  uint32_t id_;
  Protocol protocol_;
  ConnectionStats* stats_ = nullptr;

  util::HttpListenerBase* http_listener_;
  SSL_CTX* ctx_;

  ServiceInterface* service_;

  time_t creation_time_, last_interaction_;

  Phase phase_;
  std::string name_;

  std::unique_ptr<ConnectionContext> cc_;

  unsigned parser_error_ = 0;
  uint32_t pipeline_msg_cnt_ = 0;

  uint32_t break_poll_id_ = UINT32_MAX;

  BreakerCb breaker_cb_;
  std::unique_ptr<Shutdown> shutdown_cb_;

  RespVec tmp_parse_args_;
  CmdArgVec tmp_cmd_vec_;

  static thread_local std::vector<RequestPtr> free_req_pool_;
};

void RespToArgList(const RespVec& src, CmdArgVec* dest);

}  // namespace facade
