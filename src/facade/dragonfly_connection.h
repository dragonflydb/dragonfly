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

#include "facade/acl_commands_def.h"
#include "facade/facade_types.h"
#include "facade/memcache_parser.h"
#include "facade/resp_expr.h"
#include "io/io_buf.h"
#include "util/connection.h"
#include "util/fibers/fibers.h"
#include "util/http/http_handler.h"

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
class SinkReplyBuilder;

// Connection represents an active connection for a client.
//
// It directly dispatches regular commands from the io-loop.
// For pipelined requests, monitor and pubsub messages it uses
// a separate dispatch queue that is processed on a separate fiber.
class Connection : public util::Connection {
 public:
  static void Init(unsigned io_threads);
  static void Shutdown();
  static void ShutdownThreadLocal();

  Connection(Protocol protocol, util::HttpListenerBase* http_listener, SSL_CTX* ctx,
             ServiceInterface* service);
  ~Connection();

  // A callback called by Listener::OnConnectionStart in the same thread where
  // HandleRequests will run.
  void OnConnectionStart();

  using BreakerCb = std::function<void(uint32_t)>;
  using ShutdownCb = std::function<void()>;

  // PubSub message, either incoming message for active subscription or reply for new subscription.
  struct PubMessage {
    std::string pattern{};              // non-empty for pattern subscriber
    std::shared_ptr<char[]> buf;        // stores channel name and message
    std::string_view channel, message;  // channel and message parts from buf
  };

  // Pipeline message, accumulated Redis command to be executed.
  struct PipelineMessage {
    PipelineMessage(size_t nargs, size_t capacity) : args(nargs), storage(capacity) {
    }

    void Reset(size_t nargs, size_t capacity);

    void SetArgs(const RespVec& args);

    size_t StorageCapacity() const;

    // mi_stl_allocator uses mi heap internally.
    // The capacity is chosen so that we allocate a fully utilized (256 bytes) block.
    using StorageType = absl::InlinedVector<char, kReqStorageSize, mi_stl_allocator<char>>;

    absl::InlinedVector<std::string_view, 6> args;
    StorageType storage;
  };

  // Pipeline message, accumulated Memcached command to be executed.
  struct MCPipelineMessage {
    MCPipelineMessage(MemcacheParser::Command cmd, std::string_view value);

    MemcacheParser::Command cmd;
    std::string_view value;
    size_t backing_size;
    std::unique_ptr<char[]> backing;  // backing for cmd and value
  };

  // Monitor message, carries a simple payload with the registered event to be sent.
  struct MonitorMessage : public std::string {};

  // ACL Update message, contains ACL updates to be applied to the connection.
  struct AclUpdateMessage {
    std::string username;
    std::vector<uint64_t> commands;
    dfly::acl::AclKeys keys;
    dfly::acl::AclPubSub pub_sub;
  };

  // Migration request message, the async fiber stops to give way for thread migration.
  struct MigrationRequestMessage {};

  // Checkpoint message, used to track when the connection finishes executing the current command.
  struct CheckpointMessage {
    util::fb2::BlockingCounter bc;  // Decremented counter when processed
  };

  struct InvalidationMessage {
    std::string key;
    bool invalidate_due_to_flush = false;
  };

  struct MessageDeleter {
    void operator()(PipelineMessage* msg) const;
    void operator()(PubMessage* msg) const;
  };

  // Requests are allocated on the mimalloc heap and thus require a custom deleter.
  using PipelineMessagePtr = std::unique_ptr<PipelineMessage, MessageDeleter>;
  using PubMessagePtr = std::unique_ptr<PubMessage, MessageDeleter>;

  using MCPipelineMessagePtr = std::unique_ptr<MCPipelineMessage>;
  using AclUpdateMessagePtr = std::unique_ptr<AclUpdateMessage>;

  // Variant wrapper around different message types
  struct MessageHandle {
    size_t UsedMemory() const;  // How much bytes this handle takes up in total.

    // Control messages put themselves at the front of the queue, but only after all other
    // control ones. Used for management messages.
    bool IsControl() const {
      return std::holds_alternative<AclUpdateMessagePtr>(handle) ||
             std::holds_alternative<CheckpointMessage>(handle);
    }

    bool IsPipelineMsg() const {
      return std::holds_alternative<PipelineMessagePtr>(handle) ||
             std::holds_alternative<MCPipelineMessagePtr>(handle);
    }

    bool IsPubMsg() const {
      return std::holds_alternative<PubMessagePtr>(handle);
    }

    bool IsReplying() const;  // control messges don't reply, messages carrying data do

    std::variant<MonitorMessage, PubMessagePtr, PipelineMessagePtr, MCPipelineMessagePtr,
                 AclUpdateMessagePtr, MigrationRequestMessage, CheckpointMessage,
                 InvalidationMessage>
        handle;

    // time when the message was dispatched to the dispatch queue as reported by
    // ProactorBase::GetMonotonicTimeNs()
    uint64_t dispatch_ts = 0;
  };

  static_assert(sizeof(MessageHandle) <= 80,
                "Big structs should use indirection to avoid wasting deque space!");

  enum Phase { SETUP, READ_SOCKET, PROCESS, SHUTTING_DOWN, PRECLOSE, NUM_PHASES };

  // Weak reference to a connection, invalidated upon connection close.
  // Used to dispatch async operations for the connection without worrying about pointer lifetime.
  struct WeakRef {
   public:
    // Get residing thread of connection. Thread-safe.
    unsigned Thread() const;

    // Get pointer to connection if still valid, nullptr if expired.
    // Can only be called from connection's thread. Validity is guaranteed
    // only until the next suspension point.
    Connection* Get() const;

    // Returns thue if the reference expired. Thread-safe.
    bool IsExpired() const;

    // Returns client id.Thread-safe.
    uint32_t GetClientId() const;

    bool operator<(const WeakRef& other);
    bool operator==(const WeakRef& other) const;

   private:
    friend class Connection;

    WeakRef(std::shared_ptr<Connection> ptr, unsigned thread_id, uint32_t client_id);

    std::weak_ptr<Connection> ptr_;
    unsigned thread_id_;
    uint32_t client_id_;
  };

  // Add PubMessage to dispatch queue.
  // Virtual because behavior is overridden in test_utils.
  virtual void SendPubMessageAsync(PubMessage);

  // Add monitor message to dispatch queue.
  void SendMonitorMessageAsync(std::string);

  // Add acl update to dispatch queue.
  void SendAclUpdateAsync(AclUpdateMessage msg);

  // If any dispatch is currently in progress, increment counter and send checkpoint message to
  // decrement it once finished.
  void SendCheckpoint(util::fb2::BlockingCounter bc, bool ignore_paused = false,
                      bool ignore_blocked = false);

  // Add InvalidationMessage to dispatch queue.
  virtual void SendInvalidationMessageAsync(InvalidationMessage);

  // Register hook that is executen when the connection breaks.
  void RegisterBreakHook(BreakerCb breaker_cb);

  // Manually shutdown self.
  void ShutdownSelf();

  // Migrate this connecton to a different thread.
  // Return true if Migrate succeeded
  // Return false if dispatch_fb_ is active
  bool Migrate(util::fb2::ProactorBase* dest);

  // Borrow weak reference to connection. Can be called from any thread.
  WeakRef Borrow();

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

  void SetName(std::string name);

  void SetLibName(std::string name);
  void SetLibVersion(std::string version);

  // Returns a map of 'libname:libver'->count, thread local data
  static const absl::flat_hash_map<std::string, uint64_t>& GetLibStatsTL();

  std::string_view GetName() const {
    return name_;
  }

  struct MemoryUsage {
    size_t mem = 0;
    io::IoBuf::MemoryUsage buf_mem;
  };
  MemoryUsage GetMemoryUsage() const;

  ConnectionContext* cntx();

  // Requests that at some point, this connection will be migrated to `dest` thread.
  // Connections will migrate at most once, and only when the flag --migrate_connections is true.
  void RequestAsyncMigration(util::fb2::ProactorBase* dest);

  // Starts traffic logging in the calling thread. Must be a proactor thread.
  // Each thread creates its own log file combining requests from all the connections in
  // that thread. A noop if the thread is already logging.
  static void StartTrafficLogging(std::string_view base_path);

  // Stops traffic logging in this thread. A noop if the thread is not logging.
  static void StopTrafficLogging();

  // Get quick debug info for logs
  std::string DebugInfo() const;

  bool IsHttp() const;

  // Sets max queue length locally in the calling thread.
  static void SetMaxQueueLenThreadLocal(unsigned tid, uint32_t val);
  static void SetPipelineBufferLimit(unsigned tid, size_t val);
  static void GetRequestSizeHistogramThreadLocal(std::string* hist);
  static void TrackRequestSize(bool enable);
  static void EnsureMemoryBudget(unsigned tid);

  unsigned idle_time() const {
    return time(nullptr) - last_interaction_;
  }

  Phase phase() const {
    return phase_;
  }

  bool IsSending() const;

 protected:
  void OnShutdown() override;
  void OnPreMigrateThread() override;
  void OnPostMigrateThread() override;

  std::unique_ptr<ConnectionContext> cc_;  // Null for http connections

 private:
  enum ParserStatus { OK, NEED_MORE, ERROR };

  struct AsyncOperations;

  // Check protocol and handle connection.
  void HandleRequests() final;

  // Start dispatch fiber and run IoLoop.
  void ConnectionFlow();

  // Main loop reading client messages and passing requests to dispatch queue.
  std::variant<std::error_code, ParserStatus> IoLoop();

  // Returns true if HTTP header is detected.
  io::Result<bool> CheckForHttpProto();

  // Dispatches a single (Redis or MC) command.
  // `has_more` should indicate whether the io buffer has more commands
  // (pipelining in progress). Performs async dispatch if forced (already in async mode) or if
  // has_more is true, otherwise uses synchronous dispatch.
  void DispatchSingle(bool has_more, absl::FunctionRef<void()> invoke_cb,
                      absl::FunctionRef<MessageHandle()> cmd_msg_cb);

  // Handles events from the dispatch queue.
  void AsyncFiber();

  void SendAsync(MessageHandle msg);

  // Updates memory stats and pooling, must be called for all used messages
  void RecycleMessage(MessageHandle msg);

  // Create new pipeline request, re-use from pool when possible.
  PipelineMessagePtr FromArgs(RespVec args, mi_heap_t* heap);

  ParserStatus ParseRedis();
  ParserStatus ParseMemcache();

  void OnBreakCb(int32_t mask);

  // Shrink pipeline pool by a little while handling regular commands.
  void ShrinkPipelinePool();

  // Returns non-null request ptr if pool has vacant entries.
  PipelineMessagePtr GetFromPipelinePool();

  void HandleMigrateRequest();
  std::error_code HandleRecvSocket();

  bool ShouldEndAsyncFiber(const MessageHandle& msg);

  void LaunchAsyncFiberIfNeeded();  // Async fiber is started lazily

  // Squashes pipelined commands from the dispatch queue to spread load over all threads
  void SquashPipeline();

  // Clear pipelined messages, disaptching only intrusive ones.
  void ClearPipelinedMessages();

  std::pair<std::string, std::string> GetClientInfoBeforeAfterTid() const;

  void DecreaseStatsOnClose();
  void BreakOnce(uint32_t ev_mask);

  std::deque<MessageHandle> dispatch_q_;  // dispatch queue
  util::fb2::CondVarAny cnd_;             // dispatch queue waker
  util::fb2::Fiber async_fb_;             // async fiber (if started)

  uint64_t pending_pipeline_cmd_cnt_ = 0;  // how many queued Redis async commands in dispatch_q

  // how many bytes of the current request have been consumed
  size_t request_consumed_bytes_ = 0;

  io::IoBuf io_buf_;  // used in io loop and parsers
  std::unique_ptr<RedisParser> redis_parser_;
  std::unique_ptr<MemcacheParser> memcache_parser_;

  uint32_t id_;
  Protocol protocol_;
  ConnectionStats* stats_ = nullptr;

  std::unique_ptr<SinkReplyBuilder> reply_builder_;
  util::HttpListenerBase* http_listener_;
  SSL_CTX* ssl_ctx_;

  ServiceInterface* service_;

  time_t creation_time_, last_interaction_;
  Phase phase_ = SETUP;
  std::string name_;

  std::string lib_name_;
  std::string lib_ver_;

  unsigned parser_error_ = 0;

  // amount of times we enqued requests asynchronously during the same async_fiber_epoch_.
  unsigned async_streak_len_ = 0;
  uint64_t async_fiber_epoch_ = 0;

  BreakerCb breaker_cb_;

  // Used by redis parser to avoid allocations
  RespVec tmp_parse_args_;
  CmdArgVec tmp_cmd_vec_;

  // Used to keep track of borrowed references. Does not really own itself
  std::shared_ptr<Connection> self_;

  util::fb2::ProactorBase* migration_request_ = nullptr;

  // Pooled pipeline messages per-thread
  // Aggregated while handling pipelines, gradually released while handling regular commands.
  static thread_local std::vector<PipelineMessagePtr> pipeline_req_pool_;

  union {
    uint16_t flags_;
    struct {
      // a flag indicating whether the client has turned on client tracking.
      bool tracking_enabled_ : 1;
      bool skip_next_squashing_ : 1;  // Forcefully skip next squashing

      // Connection migration vars, see RequestAsyncMigration() above.
      bool migration_enabled_ : 1;
      bool migration_in_process_ : 1;
      bool is_http_ : 1;
      bool is_tls_ : 1;
    };
  };
};

}  // namespace facade
