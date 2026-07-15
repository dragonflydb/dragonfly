// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <sys/socket.h>

#include <deque>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>

#include "facade/connection_ref.h"
#include "facade/facade_types.h"
#include "facade/parsed_command.h"
#include "io/io_buf.h"
#include "util/connection.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

typedef struct ssl_ctx_st SSL_CTX;

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

namespace util {
class HttpListenerBase;
}  // namespace util

namespace facade {

struct ConnectionStats;
class ConnectionContext;
class ServiceInterface;
class SinkReplyBuilder;
class RespSrvParser;

// Snapshot of fields rendered into one CLIENT LIST line. Both real
// Connection rows and synthetic rows (outbound replication links) fill
// this and call FormatClientInfo, ensuring the wire format stays in sync.
struct ClientInfo {
  uint32_t id = 0;
  std::string addr;
  std::string laddr;
  int fd = -1;
  bool is_http = false;
  std::string name;
  std::string tls;
  std::optional<int> send_wait_time;
  uint32_t tid = 0;
  bool irqmatch = false;
  std::optional<unsigned> pipeline;
  std::optional<size_t> pbuf;
  time_t age = 0;
  time_t idle = 0;
  uint64_t tot_cmds = 0;
  uint64_t tot_net_in = 0;
  uint64_t tot_read_calls = 0;
  uint64_t tot_dispatches = 0;
  std::optional<unsigned> db;
  std::string flags;
  std::string phase;       // connection lifecycle: setup/readsock/process/...
  std::string repl_phase;  // replication state for outbound master link
  std::string lib_name;
  std::string lib_ver;
};

std::string FormatClientInfo(const ClientInfo& ci);

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

  using ShutdownCb = std::function<void()>;

  // PubSub message, either incoming message for active subscription or reply for new subscription.
  struct PubMessage {
    std::string pattern;                // non-empty for pattern subscriber
    std::shared_ptr<char[]> buf;        // stores channel name and message
    std::string_view channel, message;  // channel and message parts from buf
    bool is_sharded = false;

    // Unsubscribe simultaneously when sending unsubscribe message. Used for cluster migrations
    bool force_unsubscribe = false;
  };

  // Monitor message, carries a simple payload with the registered event to be sent.
  struct MonitorMessage : public std::string {};

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

  // Pipeline message, accumulated Redis command to be executed.
  using PipelineMessagePtr = std::unique_ptr<ParsedCommand>;
  using PubMessagePtr = std::unique_ptr<PubMessage>;

  // Variant wrapper around different message types
  struct MessageHandle {
    size_t UsedMemory() const;  // How much bytes this handle takes up in total.

    // Checkpoint messages put themselves at the front of the queue, but only in relative
    // order to the rest of the messages in the queue.
    bool IsCheckPoint() const {
      return std::holds_alternative<CheckpointMessage>(handle);
    }

    bool IsPubMsg() const {
      return std::holds_alternative<PubMessagePtr>(handle);
    }

    bool IsMonitor() const {
      return std::holds_alternative<MonitorMessage>(handle);
    }

    bool IsReplying() const;  // control messages don't reply, messages carrying data do

    std::variant<MonitorMessage, PubMessagePtr, MigrationRequestMessage, CheckpointMessage,
                 InvalidationMessage>
        handle;

    // time when the message was dispatched to the dispatch queue as reported by
    // CycleClock::Now()
    uint64_t dispatch_cycle = 0;
  };

  static_assert(sizeof(MessageHandle) <= 80,
                "Big structs should use indirection to avoid wasting deque space!");

  enum Phase : uint8_t { SETUP, READ_SOCKET, PROCESS, SHUTTING_DOWN, PRECLOSE, NUM_PHASES };

  using WeakRef = ConnectionRef;

  // Add PubMessage to dispatch queue.
  // Virtual because behavior is overridden in test_utils.
  virtual void SendPubMessageAsync(PubMessage);

  // Add monitor message to dispatch queue.
  // Virtual because behavior is overridden in test_utils.
  virtual void SendMonitorMessageAsync(std::string);

  // If any dispatch is currently in progress, increment counter and send checkpoint message to
  // decrement it once finished.
  void SendCheckpoint(util::fb2::BlockingCounter bc, bool ignore_paused = false,
                      bool ignore_blocked = false);

  // Add InvalidationMessage to dispatch queue.
  virtual void SendInvalidationMessageAsync(InvalidationMessage);

  // Flushes pending replies and returns the reply builder's error code (empty on success).
  std::error_code FlushReplies();

  // Manually shutdown self.
  void ShutdownSelfBlocking();

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

  // Reserves an id from the same monotonic pool Connection instances use.
  static uint32_t NextClientId();

  virtual bool IsPrivileged() const;  // virtual because overwritten in test_utils

  bool IsMain() const;

  // In addition to the listener role being main, also returns true if the protocol is Memcached.
  // This method returns true for customer facing listeners.
  bool IsMainOrMemcache() const;

  // Classification of the traffic source for the DEBUG TRAFFIC recorder.
  // Persisted as the second byte of the file header in the on-disk format;
  // the numeric values are part of that format — do not change them.
  // MAIN_RESP / ADMIN_RESP / REPLICA_RESP all carry RESP-format commands;
  // MAIN vs ADMIN differ by the port they were accepted on, while REPLICA
  // covers commands that arrived on a replica via the replication stream
  // (not from a client-facing listener).
  enum class ListenerType : uint8_t {
    MAIN_RESP = 1,     // main RESP listener (TCP and unix-socket)
    MEMCACHE = 2,      // memcached protocol listener
    ADMIN_RESP = 3,    // privileged / admin listener (RESP protocol on admin port)
    REPLICA_RESP = 4,  // commands arriving on a replica from its master
  };

  ListenerType GetListenerType() const {
    return listener_type_;
  }

  void SetName(std::string name);

  void SetLibName(std::string name);
  void SetLibVersion(std::string version);

  // Returns a map of 'libname:libver'->count, thread local data
  static const absl::flat_hash_map<std::string, uint64_t>& GetLibStatsTL();

  std::string_view GetName() const {
    return name_;
  }

  // Returns protocol type of this connection
  Protocol GetProtocol() const {
    return protocol_;
  }

  // Returns memory usage of this connection's auxiliary members in bytes.
  size_t GetMemoryUsage() const;

  ConnectionContext* cntx();

  // For non replication connections refresh memory usage field as well as update tl stats if conn.
  // is still live.
  void RefreshConnectionMemoryUsage();

  // Sets to false on switching to replication to remove accounting for direct bytes
  void SetConnectionMemoryAccounting(bool enabled);

  // Requests that at some point, this connection will be migrated to `dest` thread.
  // If force is false, the connection will migrate at most once,
  // and only when the flag --migrate_connections is true.
  void RequestAsyncMigration(util::fb2::ProactorBase* dest, bool force);

  // Outcome of a StartTrafficLogging call on a single thread.
  enum class StartTrafficResult : uint8_t {
    kStarted,         // new recording started successfully on this thread
    kAlreadyLogging,  // this thread already had an active recording (noop)
    kOpenFailed,      // failed to open the log file (or unsupported platform)
  };

  // Starts traffic logging in the calling thread. Must be a proactor thread.
  // Each thread creates its own log file containing requests from connections on
  // that thread whose listener type equals `listener_type`. Exactly one listener
  // kind per recording — mixing protocols in a single file is not supported.
  static StartTrafficResult StartTrafficLogging(std::string_view base_path,
                                                ListenerType listener_type);

  // Stops traffic logging in this thread. A noop if the thread is not logging.
  static void StopTrafficLogging();

  // Writes a single command to the per-thread traffic log if (and only if) the
  // logger on this thread is currently recording the REPLICA_RESP source.
  // Used by the replication read path on replicas to capture commands that
  // arrived from the master — they do not travel through a Connection, so the
  // regular per-connection hot path does not see them.
  // `db_index` is the database the command should be applied to; it is stored
  // in the record so replay tools can issue SELECT before dispatch.
  static void LogReplicaCommand(const cmn::BackedArguments& args, uint32_t db_index);

  // Get quick debug info for logs
  std::string DebugInfo() const;

  bool IsHttp() const;

  static void UpdateFromFlags();                          // Set values from flags
  static std::vector<std::string> GetMutableFlagNames();  // Triggers UpdateFromFlags

  static void TrackRequestSize(bool enable);
  static void EnsureMemoryBudget(unsigned tid);
  static void GetRequestSizeHistogramThreadLocal(std::string* hist);

  unsigned idle_time() const {
    return time(nullptr) - last_interaction_;
  }

  unsigned GetSendWaitTimeSec() const;

  Phase phase() const {
    return phase_;
  }

  bool IsSending() const;

  void Notify() {
    io_event_.notify();
  }

  void MarkForClose();

  // Adjusts the pipeline queue byte counter by `delta`.
  // Used when command dispatch mutates a ParsedCommand's backing storage
  // (e.g. SwapArgs during MULTI/EXEC collection).
  void AdjustParsedCmdBytes(ssize_t delta);

 protected:
  void OnShutdown() override;
  void OnPreMigrateThread() override;
  void OnPostMigrateThread() override;

  std::unique_ptr<ConnectionContext> cc_;  // Null for http connections

 private:
  enum ParserStatus : uint8_t { OK, NEED_MORE, ERROR };

  struct AsyncOperations;

  // Check protocol and handle connection.
  void HandleRequests() final;

  // Start dispatch fiber and run IoLoop.
  void ConnectionFlow();

  // Main loop reading client messages and passing requests to dispatch queue.
  std::variant<std::error_code, ParserStatus> IoLoop();

  // Ingests a single proactor recv notification, updating connection I/O state:
  // - io_ec_ on error/abort, pending_input_ for multishot completions
  // - io_buf_ for provided buffers.
  // May return early on error.
  // The caller (OnRecvNotification) wakes the fiber regardless so the loop can observe io_ec_ and
  // close.
  void ProcessRecvNotification(const util::FiberSocketBase::RecvNotification& n);

  // Callback: registered as the proactor OnRecv hook for V2 connections.
  // Processes the notification, eagerly drains the socket into io_buf_, and wakes the connection
  // fiber. Only called from the proactor event loop while the connection fiber is suspended.
  void OnRecvNotification(const util::FiberSocketBase::RecvNotification& n);

  // Enables io_uring multishot receives for the connection if the current thread supports it.
  // This is required during initial setup or after migrating to a new thread/proactor,
  // provided the buffer ring is configured and the connection is not using TLS.
  void MaybeEnableRecvMultishot();

  // Drains currently available bytes from socket into io_buf_ using non-blocking reads.
  void ReadPendingInput();

  void CheckIoBufCapacity(bool reached_capacity, base::IoBuf* buf);

  // Main loop reading client messages and passing requests to dispatch queue.
  std::variant<std::error_code, ParserStatus> IoLoopV2();

  // Returns true if HTTP header is detected.
  io::Result<bool> CheckForHttpProto();

  // Dispatches a single (Redis or MC) command.
  // `has_more` should indicate whether the io buffer has more commands
  // (pipelining in progress). Performs async dispatch if forced (already in async mode) or if
  // has_more is true, otherwise uses synchronous dispatch.
  void DispatchSingle(bool has_more, absl::FunctionRef<void()> invoke_cb,
                      absl::FunctionRef<void()> enqueue_cmd_cb);

  // Handles events from the dispatch queue.
  void AsyncFiber();

  // Processes a single Admin/Control message from dispatch_q_.
  // Returns true if the fiber should terminate (e.g. Migration).
  bool ProcessAdminMessage(MessageHandle* msg, AsyncOperations* async_op);

  // Processes the next Pipeline command from parsed_head_.
  void ProcessPipelineCommandV1();

  void SendAsync(MessageHandle msg);

  // Updates Control Path statistics and backpressure counters for administrative
  // events, monitor messages, and PubSub notifications.
  // If add is true, stats are incremented, otherwise decremented.
  void UpdateDispatchStats(const MessageHandle& msg, bool add);

  // Parse RESP commands from buf until the buffer is exhausted, a protocol error is hit, or
  // max_busy_cycles CPU time elapses (triggering a yield to avoid starving other fibers).
  //
  // When max_busy_cycles==0: would not trigger a yield. This can be used if parsing is done in
  // proactor context.
  // When enqueue_only=true (V2): all parsed commands are enqueued without inline
  // dispatch.
  ParserStatus ParseRedis(base::IoBuf& buf, uint32_t max_busy_cycles, bool enqueue_only);

  void OnBreakCb(int32_t mask);

  // Shrink pipeline pool by a little while handling regular commands.
  void ShrinkPipelinePool();

  // Returns non-null request ptr if pool has vacant entries.
  PipelineMessagePtr GetFromPoolOrCreate();

  void HandleMigrateRequest();
  io::Result<size_t> HandleRecvSocket();

  // Drains dispatch_q_ (control path) up to the given quota.
  // Returns true if the quota was reached.
  bool ProcessControlMessages(uint32_t quota);

  bool ShouldEndAsyncFiber(const MessageHandle& msg);

  void LaunchAsyncFiberIfNeeded();  // Async fiber is started lazily

  // Squashes pipelined commands from the dispatch queue to spread load over all threads
  void SquashPipeline();

  // Drain the control message queue (dispatch_q_) and the parsed command queue (parsed_head_) at
  // close time, processing checkpoints and waiting on in-flight commands, then release deferred
  // checkpoints.
  void DrainConnectionQueues();

  // Dec()s every BlockingCounter held in deferred_checkpoints_, then clear the vector.
  // These belong to the checkpoint commands the V2 loop deferred in
  // AsyncOperations(CheckpointMessage)while async commands were still in flight. Each counter
  // belongs to a DispatchTracker (CLIENT PAUSE / REPLTAKEOVER / cluster migration / shutdown) that
  // is blocked in Wait(). Decrementing it here signals that this connection's in-flight commands
  // have landed.
  void ReleaseDeferredCheckpoints();

  ClientInfo BuildClientInfo(unsigned thread_id) const;

  void IncreaseConnStats();
  void DecreaseConnStats();
  void BreakOnce(uint32_t ev_mask);

  // The read buffer with read data that needs to be parsed and processed.
  // For io_uring bundles we may have available_bytes larger than slice.size()
  // which means that there are more buffers available to read.
  struct ReadBuffer {
    size_t available_bytes;
    io::Bytes slice;

    void Consume(size_t len) {
      available_bytes -= len;
      slice.remove_prefix(len);
    }
  };

  bool IsReplySizeOverLimit() const;

  // Parse a batch of commands from the read buffer, enqueueing each complete command.
  // Returns:
  //   OK        - parsing stopped at a clean command boundary; more may follow (keep going).
  //   NEED_MORE - the buffer ends mid-command (or parsing was throttled by backpressure);
  //               any complete commands seen so far are still enqueued.
  //   ERROR     - a protocol error was hit; the caller must report it and close.
  // Note: a non-OK result does NOT mean "no commands were parsed" - complete commands ahead
  // of the boundary/error are enqueued regardless.
  ParserStatus ParseMCBatch(base::IoBuf& buf);

  ParserStatus ParseRedisBatch(base::IoBuf& buf);

  // Call the appropriate ParseMCBatch or ParseRedisBatch based on the protocol.
  // Only CPU-bound work; must not perform I/O or fiber suspension.
  void ParseFromBuffer(base::IoBuf& buf);

  // Call appropriate ParseBatch function, proceed with Execute and Reply all why input is remaining
  ParserStatus ParseLoop();

  // Loop over enqueued async commands and enqueue them for async execution.
  // If async execution is not possible, handle them in synchronous mode one by one.
  // Returns true on successful execution, false on reply builder error.
  bool ExecuteBatch();

  // V2 vectorized squash phase: dispatch the run starting at parsed_to_execute_ through
  // DispatchSquashedBatch when squashing is enabled. Returns true if a squashed batch was
  // dispatched (and parsed_to_execute_ advanced).
  bool SquashPipelineV2();

  // Loop over finished async commands and let them reply.
  // Returns true on successful execution, false on reply builder error.
  bool ReplyBatch();

  // True if this connection is actively contributing to the pipeline queue and that queue is
  // over the per-thread backpressure limit. The single source of truth for parse throttling.
  bool IsOverPipelineLimit() const;

  // Notifies other connections that pipeline memory may have been freed, by comparing the
  // current thread pipeline byte count against `bytes_before`.
  void NotifyIfMemReleased(size_t bytes_before);

  // True if a thread migration was requested and is actionable now (no active subscriptions).
  bool IsReadyToMigrate() const;

  // Control events that warrant leaving any park (idle or backpressure), independent of new input
  // or pipeline memory: a reply became ready, control messages are queued, the socket errored, or
  // a migration is pending.
  bool HasControlEvent() const;

  // Wake condition for the idle park: HasControlEvent() plus incoming data and a head command
  // ready to run.
  bool ShouldWakeIdle() const;

  // IoLoopV2 control path: drains dispatch_q_, processing up to `quota` control messages.
  // Expects dispatch_q_ to be non-empty. Returns true if dispatch_q_ was depleted or it had to
  // stop draining, false if the quota was reached.
  bool DrainControlPath(uint32_t quota);

  // IoLoopV2 data path when input is available and we are under the pipeline limit: parse, execute
  // and reply. Returns the parser status.
  ParserStatus RunParsePath();

  // IoLoopV2 data path with no new input to parse: execute ready commands and send completed
  // replies, freeing pipeline memory without growing the queue.
  void DrainQueuedCommands();

  // IoLoopV2 backpressure park: if still over the pipeline limit (draining may have relieved it),
  // subscribe to global memory-relief notifications, flush, and park until pressure clears.
  void ParkOnBackpressure(util::fb2::detail::Waiter* backpressure_waiter);

  // Guard of the current subscription to a parsed commands async task blocker
  struct WaitEvent {
    explicit WaitEvent(ParsedCommand* cmd, util::fb2::detail::Waiter* w);

    std::optional<util::fb2::EventCount::SubKey> key;
  };

  ParsedCommand* CreateParsedCommand();
  void EnqueueParsedCommand(ParsedCommand* cmd);

  // Releases the command memory back to the pool, updating pipeline queue accounting
  // (queue length/bytes) but NOT per-command latency/throughput stats.
  // Used both for cleanup/dropped commands and for executed commands where do not wish to
  // account for pipeline stats.
  void ReleaseParsedCommand(ParsedCommand* cmd);

  // Records per-command pipeline latency/throughput stats for a successfully executed command,
  // then delegates to ReleaseParsedCommand for the memory release and queue accounting.
  void ReleasePipelinedCommand(ParsedCommand* cmd);

  void DestroyParsedQueue();
  void AdvanceParsedHead(ParsedCommand* new_head) {
    parsed_head_ = new_head;
    if (!parsed_head_) {
      parsed_tail_ = nullptr;
    }
  }

  // Advance parsed_to_execute_ past the command it currently points at (now dispatched), keeping
  // dispatch_waiting_count_ in sync. Returns the new parsed_to_execute_.
  ParsedCommand* AdvanceToExecute();

  // Dispatch Queue - Queue for the Control Path.
  // Handles asynchronous administrative tasks, events, and high-priority control
  // messages (e.g., PubSub, Monitor, Migration requests, Checkpoints) processed
  // by the AsyncFiber.
  std::deque<MessageHandle> dispatch_q_;    // dispatch queue
  util::fb2::CondVarAny cnd_;               // dispatch queue waker
  util::fb2::Fiber async_fb_;               // async fiber (if started)
  size_t dispatch_q_bytes_ = 0;             // total bytes in dispatch queue
  size_t dispatch_q_subscriber_bytes_ = 0;  // total bytes from subscribers in dispatch queue

  std::error_code io_ec_;
  util::fb2::EventCount io_event_;
  std::optional<WaitEvent> current_wait_;

  // - V2 only: used to store checkpoints (holding a BlockingCounter) whose bc->Dec() was deferred
  // because async commands were still in-flight when the checkpoint was processed.
  // - Pushing them in the back of the dispatch_q_ may create complexities, so holding them
  // separately simplifies the solution.
  // - They are released (bc->Dec()) once HasInFlightCommands() drops to false, or on connection
  // close.
  // - Assumption: usually this vectors holds 0-1 entries, and at any given (extreme) point of time,
  // the number of deferred checkpoints is minimal (<10, no inlined vector required).
  std::vector<util::fb2::BlockingCounter> deferred_checkpoints_;

  // how many bytes of the current request have been consumed
  size_t request_consumed_bytes_ = 0;

  util::FiberSocketBase::ProvidedBuffer recv_buf_;
  io::IoBuf io_buf_;  // used in io loop and parsers
  std::unique_ptr<RespSrvParser> redis_parser_;
  std::unique_ptr<MemcacheParser> memcache_parser_;
  ParsedCommand* parsed_cmd_ = nullptr;

  // Parsed Commands Queue - Queue for the Data Path.
  //
  // Commands move through the following stages in a single linked list:
  //   1) parsed but not yet dispatched        : [parsed_to_execute_, ..., parsed_tail_]
  //   2) dispatched but not yet completed     : between parsed_head_ and parsed_to_execute_
  //   3) completed (replies ready to send)    : a prefix of [parsed_head_, ..., parsed_to_execute_)
  //   4) replied and removed                  : before parsed_head_ (no longer in the list)
  //
  // Logical order diagram:
  //   head -> ... -> (dispatched, waiting for completion) -> ... -> parsed_to_execute_ -> ... ->
  //   tail
  //
  // parsed_to_execute_ is advanced as commands are dispatched for execution.
  // Executed (completed) commands are kept in the queue until their replies are sent,
  // in order to preserve reply ordering.
  // ReplyMCBatch walks from parsed_head_ up to (but not including) parsed_to_execute_,
  // replies commands that have completed, and removes only those replied commands from
  // the queue, advancing parsed_head_ accordingly.
  ParsedCommand* parsed_head_ = nullptr;
  ParsedCommand* parsed_tail_ = nullptr;
  ParsedCommand* parsed_to_execute_ = nullptr;

  // Total number of commands in parsed command queue
  size_t parsed_cmd_q_len_ = 0;

  // Number of parsed commands not yet dispatched: the run [parsed_to_execute_, ..., parsed_tail_].
  // Maintained incrementally (incremented on enqueue, decremented as parsed_to_execute_ advances)
  // so the V2 squasher can pass an exact count starting at parsed_to_execute_ even when earlier
  // commands are still in flight. Always <= parsed_cmd_q_len_.
  size_t dispatch_waiting_count_ = 0;

  // Total bytes used by commands in parsed command queue
  size_t parsed_cmd_q_bytes_ = 0;

  // Returns true if there are dispatched commands that haven't been replied yet.
  bool HasInFlightCommands() const {
    return parsed_head_ != parsed_to_execute_;
  }

  // Returns true if the head command is ready to execute (nothing in-flight ahead of it).
  bool HasCommandToExecute() const {
    return parsed_head_ && !HasInFlightCommands();
  }

  // Returns true if there are any commands pending in the parsed command queue or dispatch queue.
  bool HasPendingMessages() const {
    return parsed_head_ || !dispatch_q_.empty();
  }

  // Returns total count of commands pending in the parsed command queue and dispatch queue.
  size_t GetPendingMessageCount() const {
    return parsed_cmd_q_len_ + dispatch_q_.size();
  }

  uint32_t id_;
  Protocol protocol_;
  Phase phase_ = SETUP;

  // Where the V2 fiber is currently parked (suspended). Used as a safety gate, for example:
  // - parse-in-proactor only fires when parked at kSquashHop (ensuring the parser is idle).
  // - kNone = fiber is running or was just created.
  enum class FiberParkSpot : uint8_t { kNone, kIdleAwait, kSquashHop, kParseYield };
  FiberParkSpot fiber_park_spot_ = FiberParkSpot::kNone;

  // True after IncreaseConnStats registers this connection in the current thread's stats.
  // False before registration and after DecreaseConnStats unregisters it during close/migration.
  bool conn_stats_registered_ = false;

  // True while this connection contributes to connection_memory_bytes. Set false for
  // replication-flow connections, whose direct memory is excluded from the client connection
  // metric.
  bool account_connection_memory_ = true;

  // Last value this connection contributed to connection_memory_bytes. Refreshes compare the
  // current memory usage with this baseline and apply only the delta to the thread-local total.
  size_t accounted_connection_memory_bytes_ = 0;

  struct {
    size_t read_cnt = 0;                // total number of read calls
    size_t net_bytes_in = 0;            // total number of bytes read
    size_t dispatch_entries_added = 0;  // total number of dispatch queue entries
    size_t cmds = 0;                    // total number of commands executed
  } local_stats_;

  std::unique_ptr<SinkReplyBuilder> reply_builder_;
  util::HttpListenerBase* http_listener_;
  SSL_CTX* ssl_ctx_;

  ServiceInterface* service_;

  time_t creation_time_, last_interaction_;
  std::string name_;

  std::string lib_name_;
  std::string lib_ver_;

  unsigned parser_error_ = 0;

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

      // whether the connection is TLS. We can be sure our socket is TlsSocket
      // if the flag is set.
      bool is_tls_ : 1;
      bool is_main_ : 1;
      bool ioloop_v2_ : 1;              // whether this connection is running on ioloop v2
      bool pipeline_squashing_v2_ : 1;  // whether V2 pipeline squashing is enabled

      // If post migration is allowed to call RegisterRecv
      bool migration_allowed_to_register_ : 1;
      bool pending_input_ : 1;

      // Set by parse-in-proactor (OnRecvNotification) when its ParseRedis hits a protocol error.
      // The recv callback cannot return a status, so IoLoopV2 observes this flag and surfaces
      // ParserStatus::ERROR to close the connection and send the protocol-error reply.
      bool proactor_parse_error_ : 1;
    };
  };

  ListenerType listener_type_ = ListenerType::MAIN_RESP;

  bool request_shutdown_ = false;
};

}  // namespace facade
