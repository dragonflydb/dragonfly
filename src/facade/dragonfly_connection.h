// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <mimalloc.h>
#include <sys/socket.h>

#include <deque>
#include <memory>
#include <string_view>
#include <utility>
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

#ifdef ABSL_HAVE_ADDRESS_SANITIZER
constexpr size_t kReqStorageSize = 88;
#else
constexpr size_t kReqStorageSize = 120;
#endif

namespace facade {

class ConnectionContext;
class RedisParser;
class ServiceInterface;
class MemcacheParser;
class SinkReplyBuilder;

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
    std::string pattern{};            // non-empty for pattern subscriber
    std::shared_ptr<char[]> buf;      // stores channel name and message
    size_t channel_len, message_len;  // lengths in buf

    std::string_view Channel() const;
    std::string_view Message() const;

    PubMessage(std::string pattern, std::shared_ptr<char[]> buf, size_t channel_len,
               size_t message_len);
  };

  struct MonitorMessage : public std::string {};

  struct AclUpdateMessage {
    std::vector<std::string> username;
    std::vector<uint32_t> categories;
    std::vector<std::vector<uint64_t>> commands;
  };

  struct MigrationRequestMessage {};

  struct PipelineMessage {
    PipelineMessage(size_t nargs, size_t capacity) : args(nargs), storage(capacity) {
    }

    void Reset(size_t nargs, size_t capacity);

    void SetArgs(const RespVec& args);

    size_t StorageCapacity() const;

    // mi_stl_allocator uses mi heap internally.
    // The capacity is chosen so that we allocate a fully utilized (256 bytes) block.
    using StorageType = absl::InlinedVector<char, kReqStorageSize, mi_stl_allocator<char>>;

    absl::InlinedVector<MutableSlice, 6> args;
    StorageType storage;
  };

  struct MessageDeleter {
    void operator()(PipelineMessage* msg) const;
    void operator()(PubMessage* msg) const;
  };

  // Requests are allocated on the mimalloc heap and thus require a custom deleter.
  using PipelineMessagePtr = std::unique_ptr<PipelineMessage, MessageDeleter>;
  using PubMessagePtr = std::unique_ptr<PubMessage, MessageDeleter>;

  struct MessageHandle {
    size_t UsedMemory() const;  // How much bytes this handle takes up in total.

    bool IsPipelineMsg() const;

    std::variant<MonitorMessage, PubMessagePtr, PipelineMessagePtr, AclUpdateMessage,
                 MigrationRequestMessage>
        handle;
  };

  enum Phase { SETUP, READ_SOCKET, PROCESS, NUM_PHASES };

 public:
  // Add PubMessage to dispatch queue.
  // Virtual because behavior is overridden in test_utils.
  virtual void SendPubMessageAsync(PubMessage);

  // Add monitor message to dispatch queue.
  void SendMonitorMessageAsync(std::string);

  // Add acl update to dispatch queue.
  void SendAclUpdateAsync(AclUpdateMessage msg);

  // Must be called before SendAsync to ensure the connection dispatch queue is not overfilled.
  // Blocks until free space is available.
  void EnsureAsyncMemoryBudget();

  // Register hook that is executed on connection shutdown.
  ShutdownHandle RegisterShutdownHook(ShutdownCb cb);

  void UnregisterShutdownHook(ShutdownHandle id);

  // Register hook that is executen when the connection breaks.
  void RegisterBreakHook(BreakerCb breaker_cb);

  // Manually shutdown self.
  void ShutdownSelf();

  // Migrate this connecton to a different thread.
  void Migrate(util::fb2::ProactorBase* dest);

  static void ShutdownThreadLocal();

  bool IsCurrentlyDispatching() const;

  std::string GetClientInfo(unsigned thread_id) const;
  std::string GetClientInfo() const;

  virtual std::string RemoteEndpointStr() const;  // virtual because overwritten in test_utils
  std::string RemoteEndpointAddress() const;

  std::string LocalBindStr() const;
  std::string LocalBindAddress() const;

  uint32_t GetClientId() const;

  virtual bool IsPrivileged() const;  // virtual because overwritten in test_utils

  bool IsMain() const;

  Protocol protocol() const {
    return protocol_;
  }

  void SetName(std::string name) {
    util::ThisFiber::SetName(absl::StrCat("DflyConnection_", name));
    name_ = std::move(name);
  }

  std::string_view GetName() const {
    return name_;
  }

  ConnectionContext* cntx();

  // Requests that at some point, this connection will be migrated to `dest` thread.
  // Connections will migrate at most once, and only when the flag --migrate_connections is true.
  void RequestAsyncMigration(util::fb2::ProactorBase* dest);

 protected:
  void OnShutdown() override;
  void OnPreMigrateThread() override;
  void OnPostMigrateThread() override;

 private:
  enum ParserStatus { OK, NEED_MORE, ERROR };

  struct DispatchOperations;
  struct DispatchCleanup;
  struct Shutdown;

  // Keeps track of total per-thread sizes of dispatch queues to
  // limit memory taken up by pipelined / pubsub commands and slow down clients
  // producing them to quickly via EnsureAsyncMemoryBudget.
  struct QueueBackpressure {
    // Block until memory usage is below limit, can be called from any thread
    void EnsureBelowLimit();

    dfly::EventCount ec;
    std::atomic_size_t bytes = 0;
    size_t limit = 0;
  };

 private:
  // Check protocol and handle connection.
  void HandleRequests() final;

  // Start dispatch fiber and run IoLoop.
  void ConnectionFlow(util::FiberSocketBase* peer);

  // Main loop reading client messages and passing requests to dispatch queue.
  std::variant<std::error_code, ParserStatus> IoLoop(util::FiberSocketBase* peer,
                                                     SinkReplyBuilder* orig_builder);

  // Returns true if HTTP header is detected.
  io::Result<bool> CheckForHttpProto(util::FiberSocketBase* peer);

  // Dispatch last command parsed by ParseRedis
  void DispatchCommand(uint32_t consumed, mi_heap_t* heap);

  // Handles events from dispatch queue.
  void DispatchFiber(util::FiberSocketBase* peer);

  void SendAsync(MessageHandle msg);

  // Create new pipeline request, re-use from pool when possible.
  PipelineMessagePtr FromArgs(RespVec args, mi_heap_t* heap);

  ParserStatus ParseRedis(SinkReplyBuilder* orig_builder);
  ParserStatus ParseMemcache();

  void OnBreakCb(int32_t mask);

  // Shrink pipeline pool by a little while handling regular commands.
  void ShrinkPipelinePool();

  // Returns non-null request ptr if pool has vacant entries.
  PipelineMessagePtr GetFromPipelinePool();

  void HandleMigrateRequest();
  bool ShouldEndDispatchFiber(const MessageHandle& msg);

  void LaunchDispatchFiberIfNeeded();

 private:
  std::pair<std::string, std::string> GetClientInfoBeforeAfterTid() const;
  std::deque<MessageHandle> dispatch_q_;  // dispatch queue
  dfly::EventCount evc_;                  // dispatch queue waker
  util::fb2::Fiber dispatch_fb_;          // dispatch fiber (if started)

  size_t dispatch_q_cmds_count_;  // how many queued async commands

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

  Phase phase_ = SETUP;
  std::string name_;

  // A pointer to the ConnectionContext object if it exists. Some connections (like http
  // requests) don't have it.
  std::unique_ptr<ConnectionContext> cc_;

  unsigned parser_error_ = 0;
  bool break_cb_engaged_ = false;

  BreakerCb breaker_cb_;
  std::unique_ptr<Shutdown> shutdown_cb_;

  RespVec tmp_parse_args_;
  CmdArgVec tmp_cmd_vec_;

  // Pointer to corresponding queue backpressure struct.
  // Needed for access from different threads by EnsureAsyncMemoryBudget().
  QueueBackpressure* queue_backpressure_;

  // Connection migration vars, see RequestAsyncMigration() above.
  bool migration_enabled_;
  util::fb2::ProactorBase* migration_request_ = nullptr;

  // Pooled pipeline messages per-thread
  // Aggregated while handling pipelines, gradually released while handling regular commands.
  static thread_local std::vector<PipelineMessagePtr> pipeline_req_pool_;

  // Per-thread queue backpressure structs.
  static thread_local QueueBackpressure tl_queue_backpressure_;
};

}  // namespace facade
