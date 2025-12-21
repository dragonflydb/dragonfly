// Copyright 2022, DragonflyDB authors.  All rights reserved.
//
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_connection.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include <numeric>
#include <variant>

#include "base/cycle_clock.h"
#include "base/flag_utils.h"
#include "base/flags.h"
#include "base/histogram.h"
#include "base/io_buf.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "common/heap_size.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/memcache_parser.h"
#include "facade/redis_parser.h"
#include "facade/reply_builder.h"
#include "facade/service_interface.h"
#include "facade/socket_utils.h"
#include "io/file.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"

#ifdef DFLY_USE_SSL
#include "util/tls/tls_socket.h"
#endif

#ifdef __linux__
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"
#include "util/fibers/uring_socket.h"
#endif

using namespace std;
using facade::operator""_MB;

ABSL_FLAG(bool, tcp_nodelay, true,
          "Configures dragonfly connections with socket option TCP_NODELAY");
ABSL_FLAG(bool, primary_port_http_enabled, true,
          "If true allows accessing http console on main TCP port");

ABSL_FLAG(uint16_t, admin_port, 0,
          "If set, would enable admin access to console on the assigned port. "
          "This supports both HTTP and RESP protocols");

ABSL_FLAG(string, admin_bind, "",
          "If set, the admin consol TCP connection would be bind the given address. "
          "This supports both HTTP and RESP protocols");

ABSL_FLAG(facade::MemoryBytesFlag, request_cache_limit, 64_MB,
          "Amount of memory to use for request cache in bytes - per IO thread.");

ABSL_FLAG(facade::MemoryBytesFlag, pipeline_buffer_limit, 128_MB,
          "Amount of memory to use for storing pipeline requests - per IO thread."
          "Please note that clients that send excecissively huge pipelines, "
          "may deadlock themselves. See https://github.com/dragonflydb/dragonfly/discussions/3997"
          "for details.");

ABSL_FLAG(uint32_t, pipeline_queue_limit, 10000,
          "Pipeline queue max length, the server will stop reading from the client socket"
          " once its pipeline queue crosses this limit, and will resume once it processes "
          "excessive requests. This is to prevent OOM states. Users of huge pipelines sizes "
          "may require increasing this limit to prevent the risk of deadlocking."
          "See https://github.com/dragonflydb/dragonfly/discussions/3997 for details");

ABSL_FLAG(facade::MemoryBytesFlag, publish_buffer_limit, 128_MB,
          "Amount of memory to use for storing pub commands in bytes - per IO thread");

ABSL_FLAG(uint32_t, pipeline_squash, 1,
          "Number of queued pipelined commands above which squashing is enabled, 0 means disabled");

// When changing this constant, also update `test_large_cmd` test in connection_test.py.
ABSL_FLAG(uint32_t, max_multi_bulk_len, 1u << 16,
          "Maximum multi-bulk (array) length that is "
          "allowed to be accepted when parsing RESP protocol");

ABSL_FLAG(uint64_t, max_bulk_len, 2u << 30,
          "Maximum bulk length that is "
          "allowed to be accepted when parsing RESP protocol");

ABSL_FLAG(facade::MemoryBytesFlag, max_client_iobuf_len, 1u << 16,
          "Maximum io buffer length that is used to read client requests.");

ABSL_FLAG(bool, migrate_connections, true,
          "When enabled, Dragonfly will try to migrate connections to the target thread on which "
          "they operate. Currently this is only supported for Lua script invocations, and can "
          "happen at most once per connection.");

ABSL_FLAG(uint32_t, max_busy_read_usec, 200,
          "Maximum time we read and parse from "
          "a socket without yielding. In microseconds.");

ABSL_FLAG(size_t, squashed_reply_size_limit, 0,
          "Max bytes allowed for squashing_current_reply_size. If this limit is reached, "
          "connections dispatching pipelines won't squash them.");

ABSL_FLAG(bool, always_flush_pipeline, false,
          "if true will flush pipeline response after each pipeline squashing");

ABSL_FLAG(uint32_t, pipeline_squash_limit, 1 << 30, "Limit on the size of a squashed pipeline. ");
ABSL_FLAG(uint32_t, pipeline_wait_batch_usec, 0,
          "If non-zero, waits for this time for more I/O "
          " events to come for the connection in case there is only one command in the pipeline. ");

ABSL_FLAG(bool, experimental_io_loop_v2, false, "new io loop");

using namespace util;
using namespace std;
using absl::GetFlag;
using base::CycleClock;
using nonstd::make_unexpected;

namespace facade {

namespace {

void SendProtocolError(RedisParser::Result pres, SinkReplyBuilder* builder) {
  constexpr string_view res = "-ERR Protocol error: "sv;
  if (pres == RedisParser::BAD_BULKLEN) {
    builder->SendProtocolError(absl::StrCat(res, "invalid bulk length"));
  } else if (pres == RedisParser::BAD_ARRAYLEN) {
    builder->SendProtocolError(absl::StrCat(res, "invalid multibulk length"));
  } else {
    builder->SendProtocolError(absl::StrCat(res, "parse error"));
  }
}

// TODO: to implement correct matcher according to HTTP spec
// https://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html
// One place to find a good implementation would be https://github.com/h2o/picohttpparser
bool MatchHttp11Line(string_view line) {
  return (absl::StartsWith(line, "GET ") || absl::StartsWith(line, "POST ")) &&
         absl::EndsWith(line, "HTTP/1.1");
}

void UpdateIoBufCapacity(const io::IoBuf& io_buf, ConnectionStats* stats,
                         absl::FunctionRef<void()> f) {
  const size_t prev_capacity = io_buf.Capacity();
  f();
  const size_t capacity = io_buf.Capacity();
  if (stats != nullptr && prev_capacity != capacity) {
    VLOG(2) << "Grown io_buf to " << capacity;
    stats->read_buf_capacity += capacity - prev_capacity;
  }
}

size_t UsedMemoryInternal(const Connection::PipelineMessage& msg) {
  return sizeof(msg) + msg.HeapMemory();
}

struct TrafficLogger {
  // protects agains closing the file while writing or data races when opening the file.
  // Also, makes sure that LogTraffic are executed atomically.
  fb2::Mutex mutex;
  unique_ptr<io::WriteFile> log_file;

  void ResetLocked();
  // Returns true if Write succeeded, false if it failed and the recording should be aborted.
  bool Write(string_view blob);
  bool Write(iovec* blobs, size_t len);
};

void TrafficLogger::ResetLocked() {
  if (log_file) {
    std::ignore = log_file->Close();
    log_file.reset();
  }
}

// Returns true if Write succeeded, false if it failed and the recording should be aborted.
bool TrafficLogger::Write(string_view blob) {
  auto ec = log_file->Write(io::Buffer(blob));
  if (ec) {
    LOG(ERROR) << "Error writing to traffic log: " << ec;
    ResetLocked();
    return false;
  }
  return true;
}

bool TrafficLogger::Write(iovec* blobs, size_t len) {
  auto ec = log_file->Write(blobs, len);
  if (ec) {
    LOG(ERROR) << "Error writing to traffic log: " << ec;
    ResetLocked();
    return false;
  }
  return true;
}

thread_local TrafficLogger tl_traffic_logger{};
thread_local base::Histogram* io_req_size_hist = nullptr;

thread_local const size_t reply_size_limit = absl::GetFlag(FLAGS_squashed_reply_size_limit);
thread_local uint32 pipeline_wait_batch_usec = absl::GetFlag(FLAGS_pipeline_wait_batch_usec);

void OpenTrafficLogger(string_view base_path) {
  unique_lock lk{tl_traffic_logger.mutex};
  if (tl_traffic_logger.log_file)
    return;

#ifdef __linux__
  // Open file with append mode, without it concurrent fiber writes seem to conflict
  string path = absl::StrCat(
      base_path, "-", absl::Dec(ProactorBase::me()->GetPoolIndex(), absl::kZeroPad3), ".bin");
  auto file = util::fb2::OpenWrite(path, io::WriteFile::Options{/*.append = */ false});
  if (!file) {
    LOG(ERROR) << "Error opening a file " << path << " for traffic logging: " << file.error();
    return;
  }
  tl_traffic_logger.log_file = unique_ptr<io::WriteFile>{file.value()};
#else
  LOG(WARNING) << "Traffic logger is only supported on Linux";
#endif

  // Write version, incremental numbering :)
  uint8_t version[1] = {2};
  std::ignore = tl_traffic_logger.log_file->Write(version);
}

void LogTraffic(uint32_t id, bool has_more, absl::Span<RespExpr> resp,
                ServiceInterface::ContextInfo ci) {
  string_view cmd = resp.front().GetView();
  if (absl::EqualsIgnoreCase(cmd, "debug"sv))
    return;

  DVLOG(2) << "Recording " << cmd;

  char stack_buf[1024];
  char* next = stack_buf;

  // We write id, timestamp, db_index, has_more, num_parts, part_len, part_len, part_len, ...
  // And then all the part blobs concatenated together.
  auto write_u32 = [&next](uint32_t i) {
    absl::little_endian::Store32(next, i);
    next += 4;
  };

  // id
  write_u32(id);

  // timestamp
  absl::little_endian::Store64(next, absl::GetCurrentTimeNanos());
  next += 8;

  // db_index
  write_u32(ci.db_index);

  // has_more, num_parts
  write_u32(has_more ? 1 : 0);
  write_u32(uint32_t(resp.size()));

  // Grab the lock and check if the file is still open.
  lock_guard lk{tl_traffic_logger.mutex};
  if (!tl_traffic_logger.log_file)
    return;

  // part_len, ...
  for (auto part : resp) {
    if (size_t(next - stack_buf + 4) > sizeof(stack_buf)) {
      if (!tl_traffic_logger.Write(string_view{stack_buf, size_t(next - stack_buf)})) {
        return;
      }
      next = stack_buf;
    }
    write_u32(part.GetView().size());
  }

  // Write the data itself.
  array<iovec, 16> blobs;
  unsigned index = 0;
  if (next != stack_buf) {
    blobs[index++] = iovec{.iov_base = stack_buf, .iov_len = size_t(next - stack_buf)};
  }

  for (auto part : resp) {
    if (auto blob_len = part.GetView().size(); blob_len > 0) {
      blobs[index++] =
          iovec{.iov_base = const_cast<char*>(part.GetView().data()), .iov_len = blob_len};

      if (index >= blobs.size()) {
        if (!tl_traffic_logger.Write(blobs.data(), blobs.size())) {
          return;
        }
        index = 0;
      }
    }
  }

  if (index) {
    tl_traffic_logger.Write(blobs.data(), index);
  }
}

constexpr size_t kMinReadSize = 256;

const char* kPhaseName[Connection::NUM_PHASES] = {"SETUP", "READ", "PROCESS", "SHUTTING_DOWN",
                                                  "PRECLOSE"};

// Keeps track of total per-thread sizes of dispatch queues to limit memory taken up by messages
// in these queues.
struct QueueBackpressure {
  QueueBackpressure() {
  }

  // Block until subscriber memory usage is below limit, can be called from any thread.
  void EnsureBelowLimit();

  bool IsPipelineBufferOverLimit(size_t size, uint32_t q_len) const {
    return size >= pipeline_buffer_limit || q_len > pipeline_queue_max_len;
  }

  // Used by publisher/subscriber actors to make sure we do not publish too many messages
  // into the queue. Thread-safe to allow safe access in EnsureBelowLimit.
  util::fb2::EventCount pubsub_ec;
  atomic_size_t subscriber_bytes = 0;

  // Used by pipelining/execution fiber to throttle the incoming pipeline messages.
  // Used together with pipeline_buffer_limit to limit the pipeline usage per thread.
  util::fb2::CondVarAny pipeline_cnd;

  size_t publish_buffer_limit = 0;        // cached flag publish_buffer_limit
  size_t pipeline_cache_limit = 0;        // cached flag pipeline_cache_limit
  size_t pipeline_buffer_limit = 0;       // cached flag for buffer size in bytes
  uint32_t pipeline_queue_max_len = 256;  // cached flag for pipeline queue max length.
};

void QueueBackpressure::EnsureBelowLimit() {
  pubsub_ec.await(
      [this] { return subscriber_bytes.load(memory_order_relaxed) <= publish_buffer_limit; });
}

// Global array for each io thread to keep track of the total memory usage of the dispatch queues.
QueueBackpressure* thread_queue_backpressure = nullptr;

QueueBackpressure& GetQueueBackpressure() {
  DCHECK(thread_queue_backpressure != nullptr);

  return thread_queue_backpressure[ProactorBase::me()->GetPoolIndex()];
}

thread_local uint64_t max_busy_read_cycles_cached = 1ULL << 32;
thread_local bool always_flush_pipeline_cached = absl::GetFlag(FLAGS_always_flush_pipeline);
thread_local uint32_t pipeline_squash_limit_cached = absl::GetFlag(FLAGS_pipeline_squash_limit);

}  // namespace

thread_local vector<Connection::PipelineMessagePtr> Connection::pipeline_req_pool_;

class PipelineCacheSizeTracker {
 public:
  bool CheckAndUpdateWatermark(size_t pipeline_sz) {
    const auto now = absl::Now();
    const auto elapsed = now - last_check_;
    min_ = std::min(min_, pipeline_sz);
    if (elapsed < absl::Milliseconds(10)) {
      return false;
    }

    const bool watermark_reached = (min_ > 0);
    min_ = Limits::max();
    last_check_ = absl::Now();

    return watermark_reached;
  }

 private:
  using Limits = std::numeric_limits<size_t>;

  absl::Time last_check_ = absl::Now();
  size_t min_ = Limits::max();
};

thread_local PipelineCacheSizeTracker tl_pipe_cache_sz_tracker;

size_t Connection::MessageHandle::UsedMemory() const {
  struct MessageSize {
    size_t operator()(const PubMessagePtr& msg) {
      return sizeof(PubMessage) + (msg->channel.size() + msg->message.size());
    }
    size_t operator()(const PipelineMessagePtr& msg) {
      return UsedMemoryInternal(*msg);
    }
    size_t operator()(const MonitorMessage& msg) {
      return msg.capacity();
    }
    size_t operator()(const AclUpdateMessagePtr& msg) {
      size_t key_cap = std::accumulate(
          msg->keys.key_globs.begin(), msg->keys.key_globs.end(), 0, [](auto acc, auto& str) {
            return acc + (str.first.capacity() * sizeof(char)) + sizeof(str.second);
          });
      return sizeof(AclUpdateMessage) + msg->username.capacity() * sizeof(char) +
             msg->commands.capacity() * sizeof(uint64_t) + key_cap;
    }
    size_t operator()(const MigrationRequestMessage& msg) {
      return 0;
    }
    size_t operator()(const CheckpointMessage& msg) {
      return 0;  // no access to internal type, memory usage negligible
    }
    size_t operator()(const InvalidationMessage& msg) {
      return 0;
    }
  };

  return sizeof(MessageHandle) + visit(MessageSize{}, this->handle);
}

bool Connection::MessageHandle::IsReplying() const {
  return IsPubMsg() || holds_alternative<MonitorMessage>(handle) ||
         holds_alternative<PipelineMessagePtr>(handle);
}

struct Connection::AsyncOperations {
  AsyncOperations(SinkReplyBuilder* b, Connection* me)
      : stats{&tl_facade_stats->conn_stats}, builder{b}, self(me) {
  }

  void operator()(const PubMessage& msg);
  void operator()(PipelineMessage& msg);
  void operator()(const MonitorMessage& msg);
  void operator()(const AclUpdateMessage& msg);
  void operator()(const MigrationRequestMessage& msg);
  void operator()(CheckpointMessage msg);
  void operator()(const InvalidationMessage& msg);

  template <typename T, typename D> void operator()(unique_ptr<T, D>& ptr) {
    operator()(*ptr.get());
  }

  ConnectionStats* stats = nullptr;
  SinkReplyBuilder* builder = nullptr;
  Connection* self = nullptr;
};

void Connection::AsyncOperations::operator()(const MonitorMessage& msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  rbuilder->SendSimpleString(msg);
}

void Connection::AsyncOperations::operator()(const AclUpdateMessage& msg) {
  if (self->cntx()) {
    if (msg.username == self->cntx()->authed_username) {
      self->cntx()->acl_commands = msg.commands;
      self->cntx()->keys = msg.keys;
      self->cntx()->pub_sub = msg.pub_sub;
      self->cntx()->acl_db_idx = msg.db_indx;
    }
  }
}

void Connection::AsyncOperations::operator()(const PubMessage& pub_msg) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(builder);

  // Discard stale messages to not break the protocol after exiting "pubsub" mode.
  // Even after removing all subscriptions, we still can receive messages delayed
  // by inter-thread dispatches or backpressure.
  // TODO: filter messages from channels the client unsubscribed from
  if (self->cntx()->subscriptions == 0 &&
      !base::_in(pub_msg.channel, {"unsubscribe", "punsubscribe"}))
    return;

  if (pub_msg.force_unsubscribe) {
    rb->StartCollection(3, CollectionType::PUSH);
    rb->SendBulkString("sunsubscribe");
    rb->SendBulkString(pub_msg.channel);
    rb->SendLong(0);
    self->cntx()->Unsubscribe(pub_msg.channel);
    return;
  }

  unsigned i = 0;
  array<string_view, 4> arr;
  if (pub_msg.pattern.empty()) {
    arr[i++] = pub_msg.is_sharded ? "smessage" : "message";
  } else {
    arr[i++] = "pmessage";
    arr[i++] = pub_msg.pattern;
  }

  arr[i++] = pub_msg.channel;
  arr[i++] = pub_msg.message;

  rb->SendBulkStrArr(absl::Span<string_view>{arr.data(), i}, CollectionType::PUSH);
}

void Connection::AsyncOperations::operator()(Connection::PipelineMessage& msg) {
  DVLOG(2) << "Dispatching pipeline: " << msg.Front();

  ++self->local_stats_.cmds;
  self->service_->DispatchCommand(ParsedArgs{msg}, self->reply_builder_.get(), self->cc_.get());

  self->last_interaction_ = time(nullptr);
  self->skip_next_squashing_ = false;
}

void Connection::AsyncOperations::operator()(const MigrationRequestMessage& msg) {
  // no-op
}

void Connection::AsyncOperations::operator()(CheckpointMessage msg) {
  VLOG(2) << "Decremented checkpoint at " << self->DebugInfo();

  msg.bc->Dec();
}

void Connection::AsyncOperations::operator()(const InvalidationMessage& msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  DCHECK(rbuilder->IsResp3());
  rbuilder->StartCollection(2, facade::CollectionType::PUSH);
  rbuilder->SendBulkString("invalidate");
  if (msg.invalidate_due_to_flush) {
    rbuilder->SendNull();
  } else {
    string_view keys[] = {msg.key};
    rbuilder->SendBulkStrArr(keys);
  }
}

namespace {
thread_local absl::flat_hash_map<string, uint64_t> g_libname_ver_map;

void UpdateLibNameVerMap(const string& name, const string& ver, int delta) {
  string key = absl::StrCat(name, ":", ver);
  uint64_t& val = g_libname_ver_map[key];
  val += delta;
  if (val == 0) {
    g_libname_ver_map.erase(key);
  }
}
}  // namespace

void Connection::Init(unsigned io_threads) {
  CHECK(thread_queue_backpressure == nullptr);
  thread_queue_backpressure = new QueueBackpressure[io_threads];

  for (unsigned i = 0; i < io_threads; ++i) {
    auto& qbp = thread_queue_backpressure[i];
    qbp.publish_buffer_limit = GetFlag(FLAGS_publish_buffer_limit);
    qbp.pipeline_cache_limit = GetFlag(FLAGS_request_cache_limit);
    qbp.pipeline_buffer_limit = GetFlag(FLAGS_pipeline_buffer_limit);
    qbp.pipeline_queue_max_len = GetFlag(FLAGS_pipeline_queue_limit);

    if (qbp.publish_buffer_limit == 0 || qbp.pipeline_cache_limit == 0 ||
        qbp.pipeline_buffer_limit == 0 || qbp.pipeline_queue_max_len == 0) {
      LOG(ERROR) << "pipeline flag limit is 0";
      exit(-1);
    }
  }
}

void Connection::Shutdown() {
  delete[] thread_queue_backpressure;
  thread_queue_backpressure = nullptr;
}

Connection::Connection(Protocol protocol, util::HttpListenerBase* http_listener, SSL_CTX* ctx,
                       ServiceInterface* service)
    : io_buf_(kMinReadSize),
      protocol_(protocol),
      http_listener_(http_listener),
      ssl_ctx_(ctx),
      service_(service),
      flags_(0) {
  static atomic_uint32_t next_id{1};

  constexpr size_t kReqSz = sizeof(Connection::PipelineMessage);
  static_assert(kReqSz <= 256);

  // TODO: to move parser initialization to where we initialize the reply builder.
  switch (protocol) {
    case Protocol::REDIS:
      redis_parser_.reset(new RedisParser(RedisParser::Mode::SERVER,
                                          GetFlag(FLAGS_max_multi_bulk_len),
                                          GetFlag(FLAGS_max_bulk_len)));
      break;
    case Protocol::MEMCACHE:
      memcache_parser_ = make_unique<MemcacheParser>();
      break;
  }

  creation_time_ = time(nullptr);
  last_interaction_ = creation_time_;
  id_ = next_id.fetch_add(1, memory_order_relaxed);

  migration_enabled_ = GetFlag(FLAGS_migrate_connections);

  // Create shared_ptr with empty value and associate it with `this` pointer (aliasing constructor).
  // We use it for reference counting and accessing `this` (without managing it).
  self_ = {make_shared<std::monostate>(), this};

#ifdef DFLY_USE_SSL
  // Increment reference counter so Listener won't free the context while we're
  // still using it.
  if (ctx) {
    SSL_CTX_up_ref(ctx);
  }
#endif

  UpdateLibNameVerMap(lib_name_, lib_ver_, +1);
  migration_allowed_to_register_ = false;
}

Connection::~Connection() {
#ifdef DFLY_USE_SSL
  SSL_CTX_free(ssl_ctx_);
#endif
  UpdateLibNameVerMap(lib_name_, lib_ver_, -1);
}

bool Connection::IsSending() const {
  return reply_builder_ && reply_builder_->IsSendActive();
}

// Called from Connection::Shutdown() right after socket_->Shutdown call.
void Connection::OnShutdown() {
  VLOG(1) << "Connection::OnShutdown";

  BreakOnce(POLLHUP);
  io_ec_ = make_error_code(errc::connection_aborted);
  io_event_.notify();
}

void Connection::OnPreMigrateThread() {
  DVLOG(1) << "OnPreMigrateThread " << GetClientId();

  CHECK(!cc_->conn_closing);

  DCHECK(!migration_in_process_);

  // CancelOnErrorCb is a preemption point, so we make sure the Migration start
  // is marked beforehand.
  migration_in_process_ = true;

  // Mark as not owned by any thread as it going through the dark hole
  self_.reset();

  socket_->CancelOnErrorCb();
  DCHECK(!async_fb_.IsJoinable()) << GetClientId();
}

void Connection::OnPostMigrateThread() {
  DVLOG(1) << "[" << id_ << "] OnPostMigrateThread";

  // Once we migrated, we should rearm OnBreakCb callback.
  if (breaker_cb_ && socket()->IsOpen()) {
    socket_->RegisterOnErrorCb([this](int32_t mask) { this->OnBreakCb(mask); });
  }

  if (ioloop_v2_ && socket_ && socket_->IsOpen() && migration_allowed_to_register_) {
    socket_->RegisterOnRecv([this](const FiberSocketBase::RecvNotification& n) {
      DoReadOnRecv(n);
      io_event_.notify();
    });
  }

  migration_in_process_ = false;
  self_ = {make_shared<std::monostate>(), this};  // Recreate shared_ptr to self.
  DCHECK(!async_fb_.IsJoinable());

  // If someone had sent Async during the migration, we must create async_fb_.
  if (!dispatch_q_.empty()) {
    LaunchAsyncFiberIfNeeded();
  }

  stats_ = &tl_facade_stats->conn_stats;
  IncrNumConns();
  stats_->read_buf_capacity += io_buf_.Capacity();
}

void Connection::OnConnectionStart() {
  SetName(absl::StrCat(id_));

  stats_ = &tl_facade_stats->conn_stats;

  if (const Listener* lsnr = static_cast<Listener*>(listener()); lsnr) {
    is_main_ = lsnr->IsMainInterface();
  }
}

void Connection::HandleRequests() {
  VLOG(1) << "[" << id_ << "] HandleRequests";
  DCHECK(stats_);

  if (GetFlag(FLAGS_tcp_nodelay) && !socket_->IsUDS()) {
    int val = 1;
    int res = setsockopt(socket_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    DCHECK_EQ(res, 0);
  }

  auto remote_ep = RemoteEndpointStr();

#ifdef DFLY_USE_SSL
  if (ssl_ctx_) {
    // Must be done atomically before the premption point in Accept so that at any
    // point in time, the socket_ is defined.
    uint8_t buf[2];
    auto read_sz = socket_->Read(io::MutableBytes(buf));
    if (!read_sz || *read_sz < sizeof(buf)) {
      LOG_EVERY_T(INFO, 1) << "Error reading from peer " << remote_ep << " "
                           << read_sz.error().message()
                           << ", socket state: " + dfly::GetSocketInfo(socket_->native_handle());
      stats_->tls_accept_disconnects++;
      return;
    }
    if (buf[0] != 0x16 || buf[1] != 0x03) {
      VLOG(1) << "Bad TLS header "
              << absl::StrCat(absl::Hex(buf[0], absl::kZeroPad2),
                              absl::Hex(buf[1], absl::kZeroPad2));
      std::ignore =
          socket_->Write(io::Buffer("-ERR Bad TLS header, double check "
                                    "if you enabled TLS for your client.\r\n"));
      stats_->tls_accept_disconnects++;
      return;
    }

    {
      FiberAtomicGuard fg;
      unique_ptr<tls::TlsSocket> tls_sock = make_unique<tls::TlsSocket>(std::move(socket_));
      tls_sock->InitSSL(ssl_ctx_, buf);
      SetSocket(tls_sock.release());
    }
    FiberSocketBase::AcceptResult aresult = socket_->Accept();

    if (!aresult) {
      // This can flood the logs -- don't change
      LOG_EVERY_T(INFO, 1) << "Error handshaking " << aresult.error().message()
                           << ", socket state: " + dfly::GetSocketInfo(socket_->native_handle());
      stats_->tls_accept_disconnects++;
      return;
    }
    is_tls_ = 1;
    VLOG(1) << "TLS handshake succeeded";
  }
#endif

  io::Result<bool> http_res{false};

  http_res = CheckForHttpProto();

  // We need to check if the socket is open because the server might be
  // shutting down. During the shutdown process, the server iterates over
  // the connections of each shard and shuts down their socket. Since the
  // main listener dispatches the connection into the next proactor, we
  // allow a schedule order that first shuts down the socket and then calls
  // this function which triggers a DCHECK on the socket while it tries to
  // RegisterOnErrorCb. Furthermore, we can get away with one check here
  // because both Write and Recv internally check if the socket was shut
  // down and return with an error accordingly.
  if (http_res && socket_->IsOpen()) {
    cc_.reset(service_->CreateContext(this));

    if (*http_res) {
      VLOG(1) << "HTTP1.1 identified";
      is_http_ = true;
      HttpConnection http_conn{http_listener_};
      http_conn.SetSocket(socket_.get());
      http_conn.set_user_data(cc_.get());

      // We validate the http request using basic-auth inside HttpConnection::HandleSingleRequest.
      cc_->authenticated = true;
      auto ec = http_conn.ParseFromBuffer(io_buf_.InputBuffer());
      io_buf_.ConsumeInput(io_buf_.InputLen());
      if (!ec) {
        http_conn.HandleRequests();
      }

      // Release the ownership of the socket from http_conn so it would stay with
      // this connection.
      http_conn.ReleaseSocket();
    } else {  // non-http
      // ioloop_v2 not supported for TLS connections yet.
      ioloop_v2_ = GetFlag(FLAGS_experimental_io_loop_v2) && !is_tls_;
      if (breaker_cb_) {
        socket_->RegisterOnErrorCb([this](int32_t mask) { this->OnBreakCb(mask); });
      }
      switch (protocol_) {
        case Protocol::REDIS:
          reply_builder_.reset(new RedisReplyBuilder(socket_.get()));
          break;
        case Protocol::MEMCACHE:
          reply_builder_.reset(new MCReplyBuilder(socket_.get()));
          CreateParsedCommand();
          break;
        default:
          break;
      }
      ConnectionFlow();

      socket_->CancelOnErrorCb();  // noop if nothing is registered.
      VLOG(1) << "Closed connection for peer "
              << GetClientInfo(fb2::ProactorBase::me()->GetPoolIndex());
      reply_builder_.reset();
      DestroyParsedQueue();
    }
    cc_.reset();
  }
}

unsigned Connection::GetSendWaitTimeSec() const {
  if (reply_builder_ && reply_builder_->IsSendActive()) {
    return (util::fb2::ProactorBase::GetMonotonicTimeNs() - reply_builder_->GetLastSendTimeNs()) /
           1'000'000'000;
  }

  return 0;
}

void Connection::RegisterBreakHook(BreakerCb breaker_cb) {
  breaker_cb_ = std::move(breaker_cb);
}

pair<string, string> Connection::GetClientInfoBeforeAfterTid() const {
  if (!socket_) {
    LOG(DFATAL) << "unexpected null socket_ "
                << " phase " << unsigned(phase_) << ", is_http: " << unsigned(is_http_);
    return {};
  }

  CHECK_LT(unsigned(phase_), NUM_PHASES);

  string before;
  auto le = LocalBindStr();
  auto re = RemoteEndpointStr();
  time_t now = time(nullptr);

  int cpu = 0;
  socklen_t len = sizeof(cpu);
  getsockopt(socket_->native_handle(), SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len);

#ifdef __APPLE__
  int my_cpu_id = -1;  // __APPLE__ does not have sched_getcpu()
#else
  int my_cpu_id = sched_getcpu();
#endif

  static constexpr string_view PHASE_NAMES[] = {"setup", "readsock", "process", "shutting_down",
                                                "preclose"};
  static_assert(NUM_PHASES == ABSL_ARRAYSIZE(PHASE_NAMES));
  static_assert(PHASE_NAMES[SHUTTING_DOWN] == "shutting_down");

  absl::StrAppend(&before, "id=", id_, " addr=", re, " laddr=", le);
  absl::StrAppend(&before, " fd=", socket_->native_handle());
  if (is_http_) {
    absl::StrAppend(&before, " http=true");
  } else {
    absl::StrAppend(&before, " name=", name_);
  }
  if (is_tls_) {
    tls::TlsSocket* tls_sock = static_cast<tls::TlsSocket*>(socket_.get());
    string_view proto_version = SSL_get_version(tls_sock->ssl_handle());
    const SSL_CIPHER* cipher = SSL_get_current_cipher(tls_sock->ssl_handle());
    absl::StrAppend(&before, " tls=", proto_version, "|", SSL_CIPHER_get_name(cipher));
  }
  string after;
  absl::StrAppend(&after, " irqmatch=", int(cpu == my_cpu_id));
  if (dispatch_q_.size()) {
    absl::StrAppend(&after, " pipeline=", dispatch_q_.size());
    absl::StrAppend(&after, " pbuf=", pending_pipeline_bytes_);
  }
  absl::StrAppend(&after, " age=", now - creation_time_, " idle=", now - last_interaction_);
  string_view phase_name = PHASE_NAMES[phase_];

  absl::StrAppend(&after, " tot-cmds=", local_stats_.cmds,
                  " tot-net-in=", local_stats_.net_bytes_in,
                  " tot-read-calls=", local_stats_.read_cnt,
                  " tot-dispatches=", local_stats_.dispatch_entries_added);

  if (cc_) {
    string cc_info = service_->GetContextInfo(cc_.get()).Format();

    // reply_builder_ may be null if the connection is in the setup phase, for example.
    if (reply_builder_ && reply_builder_->IsSendActive())
      phase_name = "send";
    absl::StrAppend(&after, " ", cc_info);
  }
  absl::StrAppend(&after, " phase=", phase_name);

  if (IsSending()) {
    absl::StrAppend(&before, " send-wait-time=", GetSendWaitTimeSec());
  }

  return {std::move(before), std::move(after)};
}

string Connection::GetClientInfo(unsigned thread_id) const {
  auto [before, after] = GetClientInfoBeforeAfterTid();
  absl::StrAppend(&before, " tid=", thread_id);
  absl::StrAppend(&before, after);
  absl::StrAppend(&before, " lib-name=", lib_name_, " lib-ver=", lib_ver_);
  return before;
}

string Connection::GetClientInfo() const {
  auto [before, after] = GetClientInfoBeforeAfterTid();
  absl::StrAppend(&before, after);
  // The following are dummy fields and users should not rely on those unless
  // we decide to implement them.
  // This is only done because the redis pyclient parser for the field "client-info"
  // for the command ACL LOG hardcodes the expected values. This behaviour does not
  // conform to the actual expected values, since it's missing half of them.
  // That is, even for redis-server, issuing an ACL LOG command via redis-cli and the pyclient
  // will return different results! For example, the fields:
  // addr=127.0.0.1:57275
  // laddr=127.0.0.1:6379
  // are missing from the pyclient.

  absl::StrAppend(&before, " qbuf=0 ", "qbuf-free=0 ", "obl=0 ", "argv-mem=0 ");
  absl::StrAppend(&before, "oll=0 ", "omem=0 ", "tot-mem=0 ", "multi=0 ");
  absl::StrAppend(&before, "psub=0 ", "sub=0");
  return before;
}

uint32_t Connection::GetClientId() const {
  return id_;
}

bool Connection::IsPrivileged() const {
  return static_cast<Listener*>(listener())->IsPrivilegedInterface();
}

bool Connection::IsMain() const {
  return is_main_;
}

bool Connection::IsMainOrMemcache() const {
  if (is_main_) {
    return true;
  }

  const Listener* lsnr = static_cast<Listener*>(listener());
  return lsnr && lsnr->protocol() == Protocol::MEMCACHE;
}

void Connection::SetName(string name) {
  util::ThisFiber::SetName(absl::StrCat("DflyConn_", name));
  name_ = std::move(name);
}

void Connection::SetLibName(string name) {
  UpdateLibNameVerMap(lib_name_, lib_ver_, -1);
  lib_name_ = std::move(name);
  UpdateLibNameVerMap(lib_name_, lib_ver_, +1);
}

void Connection::SetLibVersion(string version) {
  UpdateLibNameVerMap(lib_name_, lib_ver_, -1);
  lib_ver_ = std::move(version);
  UpdateLibNameVerMap(lib_name_, lib_ver_, +1);
}

const absl::flat_hash_map<string, uint64_t>& Connection::GetLibStatsTL() {
  return g_libname_ver_map;
}

io::Result<bool> Connection::CheckForHttpProto() {
  if (!IsPrivileged() && !IsMain()) {
    return false;
  }

  const bool primary_port_enabled = GetFlag(FLAGS_primary_port_http_enabled);
  if (!primary_port_enabled && !IsPrivileged()) {
    return false;
  }

  size_t last_len = 0;
  auto* peer = socket_.get();
  do {
    auto buf = io_buf_.AppendBuffer();
    DCHECK(!buf.empty());

    ::io::Result<size_t> recv_sz = peer->Recv(buf);
    if (!recv_sz) {
      return make_unexpected(recv_sz.error());
    }
    if (recv_sz == 0) {
      // Peer closed connection.
      return false;
    }

    io_buf_.CommitWrite(*recv_sz);
    string_view ib = ToSV(io_buf_.InputBuffer());
    if (ib.size() >= 2 && ib[0] == 22 && ib[1] == 3) {
      // We matched the TLS handshake raw data, which means "peer" is a TCP socket.
      // Reject the connection.
      return make_unexpected(make_error_code(errc::protocol_not_supported));
    }

    ib = ib.substr(last_len);
    size_t pos = ib.find('\n');
    if (pos != string_view::npos) {
      ib = ToSV(io_buf_.InputBuffer().first(last_len + pos));
      if (ib.size() < 10 || ib.back() != '\r')
        return false;

      ib.remove_suffix(1);
      return MatchHttp11Line(ib);
    }
    last_len = io_buf_.InputLen();
    UpdateIoBufCapacity(io_buf_, stats_, [&]() { io_buf_.EnsureCapacity(128); });
  } while (last_len < 1024);

  return false;
}

void Connection::ConnectionFlow() {
  DCHECK(reply_builder_);

  IncrNumConns();
  ++stats_->conn_received_cnt;
  stats_->read_buf_capacity += io_buf_.Capacity();

  ++local_stats_.read_cnt;
  local_stats_.net_bytes_in += io_buf_.InputLen();

  ParserStatus parse_status = OK;

  // At the start we read from the socket to determine the HTTP/Memstore protocol.
  // Therefore we may already have some data in the buffer.
  if (io_buf_.InputLen() > 0) {
    phase_ = PROCESS;
    if (redis_parser_) {
      parse_status = ParseRedis(10000);
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }
  }

  error_code ec = reply_builder_->GetError();

  // Main loop.
  if (parse_status != ERROR && !ec) {
    UpdateIoBufCapacity(io_buf_, stats_, [&]() { io_buf_.EnsureCapacity(64); });
    variant<error_code, Connection::ParserStatus> res;
    if (ioloop_v2_) {
      // Everything above the IoLoopV2 is fiber blocking. A connection can migrate before
      // it reaches here and will cause a double RegisterOnRecv check fail. To avoid this,
      // a migration shall only call RegisterOnRev if it reached the main IoLoopV2 below.
      migration_allowed_to_register_ = true;
      // Breaks with TLS. RegisterOnRecv is unimplemented.
      res = IoLoopV2();
    } else {
      res = IoLoop();
    }

    if (holds_alternative<error_code>(res)) {
      ec = get<error_code>(res);
    } else {
      parse_status = get<ParserStatus>(res);
    }
  }

  // After the client disconnected.
  cc_->conn_closing = true;  // Signal dispatch to close.
  cnd_.notify_one();
  phase_ = SHUTTING_DOWN;
  VLOG(2) << "Before dispatch_fb.join()";
  async_fb_.JoinIfNeeded();
  VLOG(2) << "After dispatch_fb.join()";

  phase_ = PRECLOSE;

  ClearPipelinedMessages();
  DCHECK(dispatch_q_.empty());

  service_->OnConnectionClose(cc_.get());
  DecreaseStatsOnClose();

  if (ioloop_v2_) {
    socket_->ResetOnRecvHook();
  }

  // We wait for dispatch_fb to finish writing the previous replies before replying to the last
  // offending request.
  if (parse_status == ERROR) {
    VLOG(1) << "Error parser status " << parser_error_;

    if (redis_parser_) {
      SendProtocolError(RedisParser::Result(parser_error_), reply_builder_.get());
    } else {
      DCHECK(memcache_parser_);
      reply_builder_->SendProtocolError("bad command line format");
    }

    // Shut down the servers side of the socket to send a FIN to the client
    // then keep draining the socket (discarding any received data) until
    // the client closes the connection.
    //
    // Otherwise the clients write could fail (or block), so they would never
    // read the above protocol error (see issue #1327).
    // TODO: we have a bug that can potentially deadlock the code below.
    // If the socket does not close the socket on the other side, the while loop will never finish.
    // to reproduce: nc localhost 6379  and then run invalid sequence: *1 <enter> *1 <enter>
    error_code ec2 = socket_->Shutdown(SHUT_WR);
    LOG_IF(WARNING, ec2) << "Could not shutdown socket " << ec2;
    while (!ec2) {
      // Discard any received data.
      io_buf_.Clear();
      auto recv_sz = socket_->Recv(io_buf_.AppendBuffer());
      if (!recv_sz || *recv_sz == 0) {
        break;  // Peer closed connection.
      }
    }
  }

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    string conn_info = service_->GetContextInfo(cc_.get()).Format();
    LOG_EVERY_T(WARNING, 1) << "Socket error for connection " << conn_info << " " << GetName()
                            << " during phase " << kPhaseName[phase_] << " : " << ec << " "
                            << ec.message();
  }
}

void Connection::DispatchSingle(bool has_more, absl::FunctionRef<void()> invoke_cb,
                                absl::FunctionRef<MessageHandle()> cmd_msg_cb) {
  bool optimize_for_async = has_more;
  QueueBackpressure& qbp = GetQueueBackpressure();
  if (optimize_for_async &&
      qbp.IsPipelineBufferOverLimit(stats_->dispatch_queue_bytes, dispatch_q_.size())) {
    stats_->pipeline_throttle_count++;
    LOG_EVERY_T(WARNING, 10) << "Pipeline buffer over limit: pipeline_bytes "
                             << stats_->dispatch_queue_bytes << " queue_size " << dispatch_q_.size()
                             << ", consider increasing pipeline_buffer_limit/pipeline_queue_limit";
    fb2::NoOpLock noop;
    qbp.pipeline_cnd.wait(noop, [this, &qbp] {
      bool over_limits =
          qbp.IsPipelineBufferOverLimit(stats_->dispatch_queue_bytes, dispatch_q_.size());
      return !over_limits || (dispatch_q_.empty() && !cc_->async_dispatch) || cc_->conn_closing;
    });

    if (cc_->conn_closing) {
      if (request_shutdown_) {
        ShutdownSelfBlocking();
      }
      return;
    }

    // prefer synchronous dispatching to save memory.
    optimize_for_async = false;
    last_interaction_ = time(nullptr);
  }

  // Avoid sync dispatch if we can interleave with an ongoing async dispatch.
  bool can_dispatch_sync = !cc_->async_dispatch && dispatch_q_.empty() && cc_->subscriptions == 0;

  // Dispatch async if we're handling a pipeline or if we can't dispatch sync.
  if (optimize_for_async || !can_dispatch_sync) {
    SendAsync(cmd_msg_cb());
  } else {
    ShrinkPipelinePool();  // Gradually release pipeline request pool.
    {
      ++local_stats_.cmds;
      cc_->sync_dispatch = true;
      invoke_cb();
      cc_->sync_dispatch = false;
    }
    last_interaction_ = time(nullptr);

    // We might have blocked the dispatch queue from processing, wake it up.
    if (!dispatch_q_.empty())
      cnd_.notify_one();
  }
}

Connection::ParserStatus Connection::ParseRedis(unsigned max_busy_cycles) {
  uint32_t consumed = 0;
  RedisParser::Result result = RedisParser::OK;

  auto dispatch_sync = [this] {
    RespExpr::VecToArgList(tmp_parse_args_, &tmp_cmd_vec_);
    service_->DispatchCommand(ParsedArgs{tmp_cmd_vec_}, reply_builder_.get(), cc_.get());
  };

  auto dispatch_async = [this]() -> MessageHandle { return {FromArgs(tmp_parse_args_)}; };

  io::Bytes read_buffer = io_buf_.InputBuffer();
  // Keep track of total bytes consumed/parsed. The do/while{} loop below preempts,
  // and InputBuffer() size might change between preemption points. There is a corner case,
  // that ConsumeInput() will strip a portion of the request which makes the test_publish_stuck
  // test fail.
  // TODO(kostas): follow up on this
  size_t total_consumed = 0;
  do {
    result = redis_parser_->Parse(read_buffer, &consumed, &tmp_parse_args_);
    request_consumed_bytes_ += consumed;
    total_consumed += consumed;
    if (result == RedisParser::OK && !tmp_parse_args_.empty()) {
      // If we get a non-STRING type (e.g., NIL, ARRAY), it's a protocol error.
      bool valid_input = std::all_of(tmp_parse_args_.begin(), tmp_parse_args_.end(),
                                     [](const auto& arg) { return arg.type == RespExpr::STRING; });
      if (!valid_input) {
        LOG(WARNING) << "Invalid argument - expected all STRING types";
        result = RedisParser::BAD_STRING;
        break;
      }

      DVLOG(2) << "Got Args with first token " << ToSV(tmp_parse_args_.front().GetBuf());

      if (io_req_size_hist)
        io_req_size_hist->Add(request_consumed_bytes_);
      request_consumed_bytes_ = 0;
      bool has_more = consumed < read_buffer.size();

      if (tl_traffic_logger.log_file && IsMain() /* log only on the main interface */) {
        LogTraffic(id_, has_more, absl::MakeSpan(tmp_parse_args_),
                   service_->GetContextInfo(cc_.get()));
      }

      DispatchSingle(has_more, dispatch_sync, dispatch_async);
    }
    if (result != RedisParser::OK && result != RedisParser::INPUT_PENDING) {
      // We do not expect that a replica sends an invalid command so we log if it happens.
      LOG_IF(WARNING, cntx()->replica_conn)
          << "Redis parser error: " << result << " during parse: " << ToSV(read_buffer);
    }
    read_buffer.remove_prefix(consumed);

    // We must yield from time to time to allow other fibers to run.
    // Specifically, if a client sends a huge chunk of data resulting in a very long pipeline,
    // we want to yield to allow AsyncFiber to actually execute on the pending pipeline.
    if (ThisFiber::GetRunningTimeCycles() > max_busy_cycles) {
      stats_->num_read_yields++;
      ThisFiber::Yield();
    }
  } while (RedisParser::OK == result && read_buffer.size() > 0 && !reply_builder_->GetError());

  io_buf_.ConsumeInput(total_consumed);

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING) {
    DCHECK_EQ(read_buffer.size(), 0u);

    return NEED_MORE;
  }

  VLOG(1) << "Parser error " << result;

  return ERROR;
}

auto Connection::ParseMemcache() -> ParserStatus {
  bool commands_parsed = false;
  do {
    commands_parsed = ParseMCBatch();

    if (!ExecuteMCBatch())
      return ERROR;

    if (!ReplyMCBatch())
      return ERROR;
  } while (commands_parsed && io_buf_.InputLen() > 0);

  return commands_parsed ? OK : NEED_MORE;
}

void Connection::OnBreakCb(int32_t mask) {
  if (mask <= 0)
    return;  // we cancelled the poller, which means we do not need to break from anything.

  if (!cc_) {
    LOG(ERROR) << "Unexpected event " << mask;
    return;
  }

  DCHECK(reply_builder_) << "[" << id_ << "] " << phase_ << " " << migration_in_process_;

  VLOG(1) << "[" << id_ << "] Got event " << mask << " " << phase_ << " "
          << reply_builder_->IsSendActive() << " " << reply_builder_->GetError();

  cc_->conn_closing = true;
  BreakOnce(mask);
  cnd_.notify_one();  // Notify dispatch fiber.
}

void Connection::HandleMigrateRequest() {
  if (cc_->conn_closing || !migration_request_) {
    return;
  }

  ProactorBase* dest = migration_request_;

  if (async_fb_.IsJoinable()) {
    SendAsync({MigrationRequestMessage{}});
    async_fb_.Join();
  }

  // We don't support migrating with subscriptions as it would require moving thread local
  // handles. We can't check above, as the queue might have contained a subscribe request.

  if (cc_->subscriptions == 0) {
    // RegisterOnErrorCb might be called on POLLHUP and the join above is a preemption point.
    // So, it could be the case that after this fiber wakes up the connection might be closing.
    if (cc_->conn_closing) {
      return;
    }

    stats_->num_migrations++;
    migration_request_ = nullptr;

    DecreaseStatsOnClose();

    // We need to return early as the socket is closing and IoLoop will clean up.
    // The reason that this is true is because of the following DCHECK
    DCHECK(!async_fb_.IsJoinable());

    // which can never trigger since we Joined on the async_fb_ above and we are
    // atomic in respect to our proactor meaning that no other fiber will
    // launch the DispatchFiber.
    std::ignore = !this->Migrate(dest);
  }
}

io::Result<size_t> Connection::HandleRecvSocket() {
  phase_ = READ_SOCKET;

  io::MutableBytes append_buf = io_buf_.AppendBuffer();
  DCHECK(!append_buf.empty());
  ::io::Result<size_t> recv_sz = socket_->Recv(append_buf);
  last_interaction_ = time(nullptr);

  // In case the socket was closed orderly, we get 0 bytes read.
  if (recv_sz && *recv_sz) {
    size_t commit_sz = *recv_sz;
    io_buf_.CommitWrite(commit_sz);

    stats_->io_read_bytes += commit_sz;
    local_stats_.net_bytes_in += commit_sz;

    ++stats_->io_read_cnt;
    ++local_stats_.read_cnt;
  }
  return recv_sz;
}

variant<error_code, Connection::ParserStatus> Connection::IoLoop() {
  error_code ec;
  ParserStatus parse_status = OK;

  size_t max_iobfuf_len = GetFlag(FLAGS_max_client_iobuf_len);

  auto* peer = socket_.get();
  recv_buf_.res_len = 0;

  do {
    HandleMigrateRequest();
    auto recv_sz = HandleRecvSocket();
    if (!recv_sz) {
      LOG_IF(WARNING, cntx()->replica_conn) << "HandleRecvSocket() error: " << recv_sz.error();
      return recv_sz.error();
    }
    if (*recv_sz == 0) {
      break;
    }

    phase_ = PROCESS;
    bool is_iobuf_full = io_buf_.AppendLen() == 0;

    if (redis_parser_) {
      parse_status = ParseRedis(max_busy_read_cycles_cached);
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }

    if (reply_builder_->GetError()) {
      return reply_builder_->GetError();
    }

    if (parse_status == NEED_MORE) {
      parse_status = OK;

      size_t capacity = io_buf_.Capacity();
      if (capacity < max_iobfuf_len) {
        size_t parser_hint = 0;
        if (redis_parser_)
          parser_hint = redis_parser_->parselen_hint();  // Could be done for MC as well.

        // If we got a partial request and we managed to parse its
        // length, make sure we have space to store it instead of
        // increasing space incrementally.
        // (Note: The buffer object is only working in power-of-2 sizes,
        // so there's no danger of accidental O(n^2) behavior.)
        if (parser_hint > capacity) {
          UpdateIoBufCapacity(io_buf_, stats_,
                              [&]() { io_buf_.Reserve(std::min(max_iobfuf_len, parser_hint)); });
        }

        // If we got a partial request because iobuf was full, grow it up to
        // a reasonable limit to save on Recv() calls.
        if (is_iobuf_full && capacity < max_iobfuf_len / 2) {
          // Last io used most of the io_buf to the end.
          UpdateIoBufCapacity(io_buf_, stats_, [&]() {
            io_buf_.Reserve(capacity * 2);  // Valid growth range.
          });
        }

        if (io_buf_.AppendLen() == 0U) {
          // it can happen with memcached but not for RedisParser, because RedisParser fully
          // consumes the passed buffer
          LOG_EVERY_T(WARNING, 10)
              << "Maximum io_buf length reached, consider to increase max_client_iobuf_len flag";
        }
      }
    } else if (parse_status != OK) {
      break;
    }
  } while (peer->IsOpen());

  return parse_status;
}

bool Connection::ShouldEndAsyncFiber(const MessageHandle& msg) {
  if (!holds_alternative<MigrationRequestMessage>(msg.handle)) {
    return false;
  }

  if (dispatch_q_.empty()) {
    // Migration requests means we should terminate this function (and allow the fiber to
    // join), so that we can re-launch the fiber in the new thread.
    // We intentionally return and not break in order to keep the connection open.
    return true;
  }

  // There shouldn't be any other migration requests in the queue, but it's worth checking
  // as otherwise it would lead to an endless loop.
  bool has_migration_req =
      any_of(dispatch_q_.begin(), dispatch_q_.end(), [](const MessageHandle& msg) {
        return holds_alternative<MigrationRequestMessage>(msg.handle);
      });
  if (!has_migration_req) {
    SendAsync({MigrationRequestMessage{}});
  }

  return false;
}

void Connection::SquashPipeline() {
  DCHECK_EQ(dispatch_q_.size(), pending_pipeline_cmd_cnt_);
  DCHECK_EQ(reply_builder_->GetProtocol(), Protocol::REDIS);  // Only Redis is supported.

  unsigned pipeline_count = std::min<uint32_t>(dispatch_q_.size(), pipeline_squash_limit_cached);

  uint64_t start = CycleClock::Now();

  // We use indexes as iterators are invalidated when pushing into the queue.
  auto get_next_fn = [i = 0, this]() mutable -> ParsedArgs {
    const auto& elem = dispatch_q_[i++];
    CHECK(holds_alternative<PipelineMessagePtr>(elem.handle));
    const auto& pmsg = get<PipelineMessagePtr>(elem.handle);

    return *pmsg;
  };

  // async_dispatch is a guard to prevent concurrent writes into reply_builder_, hence
  // it must guard the Flush() as well.
  cc_->async_dispatch = true;

  DispatchManyResult result =
      service_->DispatchManyCommands(get_next_fn, pipeline_count, reply_builder_.get(), cc_.get());

  uint32_t dispatched = result.processed;
  uint64_t before_flush = CycleClock::Now();
  //
  // TODO: to investigate if always flushing will improve P99 latency because otherwise we
  // wait for the next batch to finish before fully flushing the current response.
  if (pending_pipeline_cmd_cnt_ == pipeline_count ||
      always_flush_pipeline_cached) {  // Flush if no new commands appeared
    reply_builder_->Flush();
    reply_builder_->SetBatchMode(false);  // in case the next dispatch is sync
  } else {
    stats_->skip_pipeline_flushing++;
  }

  cc_->async_dispatch = false;

  if (result.account_in_stats) {
    stats_->pipeline_dispatch_calls++;
    stats_->pipeline_dispatch_commands += dispatched;
    stats_->pipeline_dispatch_flush_usec += CycleClock::ToUsec(CycleClock::Now() - before_flush);
  }
  auto it = dispatch_q_.begin();
  while (it->IsControl())  // Skip all newly received intrusive messages
    ++it;

  for (auto rit = it; rit != it + dispatched; ++rit) {
    if (result.account_in_stats) {
      // measure the time spent in the pipeline  queue waiting for dispatch
      stats_->pipelined_wait_latency += CycleClock::ToUsec(start - rit->dispatch_cycle);
    } else {
      rit->dispatch_cycle = 0;  // Reset dispatch cycle to avoid accounting inside RecycleMessage
    }
    RecycleMessage(std::move(*rit));
  }

  dispatch_q_.erase(it, it + dispatched);

  // If interrupted due to pause, fall back to regular dispatch
  skip_next_squashing_ = dispatched != pipeline_count;
}

void Connection::ClearPipelinedMessages() {
  AsyncOperations async_op{reply_builder_.get(), this};

  // Recycle messages even from disconnecting client to keep properly track of memory stats
  // As well as to avoid pubsub backpressure leakege.
  for (auto& msg : dispatch_q_) {
    FiberAtomicGuard guard;  // don't suspend when concluding to avoid getting new messages
    if (msg.IsControl())
      visit(async_op, msg.handle);  // to not miss checkpoints
    RecycleMessage(std::move(msg));
  }

  dispatch_q_.clear();
  QueueBackpressure& qbp = GetQueueBackpressure();
  qbp.pipeline_cnd.notify_all();
  qbp.pubsub_ec.notifyAll();
}

string Connection::DebugInfo() const {
  string info = "{";

  absl::StrAppend(&info, "id=", id_, ", ");
  absl::StrAppend(&info, "phase=", phase_, ", ");
  if (cc_) {
    // In some rare cases cc_ can be null, see https://github.com/dragonflydb/dragonfly/pull/3873
    absl::StrAppend(&info, "dispatch(s/a)=", cc_->sync_dispatch, " ", cc_->async_dispatch, ", ");
    absl::StrAppend(&info, "closing=", cc_->conn_closing, ", ");
  }
  absl::StrAppend(&info, "df:joinable=", async_fb_.IsJoinable(), ", ");

  bool intrusive_front = !dispatch_q_.empty() && dispatch_q_.front().IsControl();
  absl::StrAppend(&info, "dq:size=", dispatch_q_.size(), ", ");
  absl::StrAppend(&info, "dq:pipelined=", pending_pipeline_cmd_cnt_, ", ");
  absl::StrAppend(&info, "dq:intrusive=", intrusive_front, ", ");

  if (cc_) {
    absl::StrAppend(&info, "state=");
    if (cc_->paused)
      absl::StrAppend(&info, "p");
    if (cc_->blocked)
      absl::StrAppend(&info, "b");
  }
  time_t now = time(nullptr);
  absl::StrAppend(&info, " age=", now - creation_time_, " idle=", now - last_interaction_, "}");

  return info;
}

// DispatchFiber handles commands coming from the InputLoop.
// Thus, InputLoop can quickly read data from the input buffer, parse it and push
// into the dispatch queue and DispatchFiber will run those commands asynchronously with
// InputLoop. Note: in some cases, InputLoop may decide to dispatch directly and bypass the
// DispatchFiber.
void Connection::AsyncFiber() {
  ThisFiber::SetName("AsyncFiber");

  AsyncOperations async_op{reply_builder_.get(), this};

  size_t squashing_threshold = GetFlag(FLAGS_pipeline_squash);

  uint64_t prev_epoch = fb2::FiberSwitchEpoch();
  fb2::NoOpLock noop_lk;
  QueueBackpressure& qbp = GetQueueBackpressure();
  while (!reply_builder_->GetError()) {
    DCHECK_EQ(socket()->proactor(), ProactorBase::me());
    cnd_.wait(noop_lk, [this] {
      return cc_->conn_closing || (!dispatch_q_.empty() && !cc_->sync_dispatch);
    });
    if (cc_->conn_closing)
      break;

    // We really want to have batching in the builder if possible. This is especially
    // critical in situations where Nagle's algorithm can introduce unwanted high
    // latencies. However we can only batch if we're sure that there are more commands
    // on the way that will trigger a flush. To know if there are, we sometimes yield before
    // executing the last command in the queue and let the producer fiber push more commands if it
    // wants to.
    // As an optimization, we only yield if the fiber was not suspended since the last dispatch.
    uint64_t cur_epoch = fb2::FiberSwitchEpoch();
    if (dispatch_q_.size() == 1 && cur_epoch == prev_epoch) {
      if (pipeline_wait_batch_usec > 0) {
        ThisFiber::SleepFor(chrono::microseconds(pipeline_wait_batch_usec));
      } else {
        ThisFiber::Yield();
      }
      DVLOG(2) << "After yielding to producer, dispatch_q_.size()=" << dispatch_q_.size();
      if (cc_->conn_closing)
        break;
    }
    prev_epoch = cur_epoch;

    reply_builder_->SetBatchMode(dispatch_q_.size() > 1);

    bool subscriber_over_limit =
        stats_->dispatch_queue_subscriber_bytes >= qbp.publish_buffer_limit;

    // Special case: if the dispatch queue accumulated a big number of commands,
    // we can try to squash them
    // It is only enabled if the threshold is reached and the whole dispatch queue
    // consists only of commands (no pubsub or monitor messages)
    bool squashing_enabled = squashing_threshold > 0;
    bool threshold_reached = pending_pipeline_cmd_cnt_ > squashing_threshold;
    bool are_all_plain_cmds = pending_pipeline_cmd_cnt_ == dispatch_q_.size();
    if (squashing_enabled && threshold_reached && are_all_plain_cmds && !skip_next_squashing_ &&
        !IsReplySizeOverLimit()) {
      SquashPipeline();
    } else {
      MessageHandle msg = std::move(dispatch_q_.front());
      dispatch_q_.pop_front();

      stats_->pipelined_wait_latency += CycleClock::ToUsec(CycleClock::Now() - msg.dispatch_cycle);

      // We keep the batch mode enabled as long as the dispatch queue is not empty, relying on the
      // last command to reply and flush. If it doesn't reply (i.e. is a control message like
      // migrate), we have to flush manually.
      bool is_replying = msg.IsReplying();
      if (dispatch_q_.empty() && !is_replying) {
        reply_builder_->Flush();
      }

      if (ShouldEndAsyncFiber(msg)) {
        RecycleMessage(std::move(msg));
        CHECK(dispatch_q_.empty()) << DebugInfo();
        qbp.pipeline_cnd.notify_all();
        return;  // don't set conn closing flag
      }

      auto replies_recorded_before = reply_builder_->RepliesRecorded();
      cc_->async_dispatch = true;
      std::visit(async_op, msg.handle);
      cc_->async_dispatch = false;
      // If last msg in queue was replying but nothing was replied during dispatch
      // (i.e. pubsub message was discarded) we have to manually flush now.
      if (dispatch_q_.empty() && is_replying &&
          (replies_recorded_before == reply_builder_->RepliesRecorded())) {
        reply_builder_->Flush();
      }
      RecycleMessage(std::move(msg));
    }

    if (!qbp.IsPipelineBufferOverLimit(stats_->dispatch_queue_bytes, dispatch_q_.size()) ||
        dispatch_q_.empty()) {
      qbp.pipeline_cnd.notify_all();  // very cheap if noone is waiting on it.
    }

    if (subscriber_over_limit && stats_->dispatch_queue_subscriber_bytes < qbp.publish_buffer_limit)
      qbp.pubsub_ec.notify();
  }

  DCHECK(cc_->conn_closing || reply_builder_->GetError());
  cc_->conn_closing = true;
  qbp.pipeline_cnd.notify_all();
}

Connection::PipelineMessagePtr Connection::FromArgs(const RespVec& args) {
  PipelineMessagePtr ptr;
  if (ptr = GetFromPipelinePool(); !ptr) {
    // We must construct in place here, since there is a slice that uses memory locations
    ptr = make_unique<PipelineMessage>();
  }

  auto map = [](const RespExpr& expr) { return expr.GetView(); };
  auto range = base::it::Transform(map, base::it::Range(args.begin(), args.end()));
  ptr->Assign(range.begin(), range.end(), args.size());
  return ptr;
}

void Connection::ShrinkPipelinePool() {
  if (pipeline_req_pool_.empty())
    return;

  if (tl_pipe_cache_sz_tracker.CheckAndUpdateWatermark(pipeline_req_pool_.size())) {
    stats_->pipeline_cmd_cache_bytes -= UsedMemoryInternal(*pipeline_req_pool_.back());
    pipeline_req_pool_.pop_back();
  }
}

Connection::PipelineMessagePtr Connection::GetFromPipelinePool() {
  if (pipeline_req_pool_.empty())
    return nullptr;

  auto ptr = std::move(pipeline_req_pool_.back());
  stats_->pipeline_cmd_cache_bytes -= UsedMemoryInternal(*ptr);
  pipeline_req_pool_.pop_back();
  return ptr;
}

void Connection::ShutdownSelfBlocking() {
  util::Connection::Shutdown();
}

bool Connection::Migrate(util::fb2::ProactorBase* dest) {
  // Migrate is used only by replication, so it doesn't have properties of full-fledged
  // connections
  CHECK(!cc_->async_dispatch);
  CHECK_EQ(cc_->subscriptions, 0);  // are bound to thread local caches
  CHECK_EQ(self_.use_count(), 1u);  // references cache our thread and backpressure
                                    //
  if (ioloop_v2_ && socket_ && socket_->IsOpen()) {
    socket_->ResetOnRecvHook();
  }

  // Migrate is only used by DFLY Thread and Flow command which both check against
  // the result of Migration and handle it explicitly in their flows so this can act
  // as a weak if condition instead of a crash prone CHECK.
  if (async_fb_.IsJoinable() || cc_->conn_closing) {
    return false;
  }

  listener()->Migrate(this, dest);

  // After we migrate, it could be the case the connection was shut down. We should
  // act accordingly.
  if (!socket()->IsOpen()) {
    return false;
  }

  return true;
}

Connection::WeakRef Connection::Borrow() {
  DCHECK(self_);

  return {self_, unsigned(socket_->proactor()->GetPoolIndex()), id_};
}

void Connection::ShutdownThreadLocal() {
  pipeline_req_pool_.clear();
}

bool Connection::IsCurrentlyDispatching() const {
  if (!cc_)
    return false;

  return cc_->async_dispatch || cc_->sync_dispatch;
}

void Connection::SendPubMessageAsync(PubMessage msg) {
  SendAsync({make_unique<PubMessage>(std::move(msg))});
}

void Connection::SendMonitorMessageAsync(string msg) {
  SendAsync({MonitorMessage{std::move(msg)}});
}

void Connection::SendAclUpdateAsync(AclUpdateMessage msg) {
  SendAsync({make_unique<AclUpdateMessage>(std::move(msg))});
}

void Connection::SendCheckpoint(fb2::BlockingCounter bc, bool ignore_paused, bool ignore_blocked) {
  if (!IsCurrentlyDispatching())
    return;

  if (cc_->paused && ignore_paused)
    return;

  if (cc_->blocked && ignore_blocked)
    return;

  VLOG(2) << "Sent checkpoint to " << DebugInfo();

  bc->Add(1);
  SendAsync({CheckpointMessage{bc}});
}

void Connection::SendInvalidationMessageAsync(InvalidationMessage msg) {
  SendAsync({std::move(msg)});
}

void Connection::LaunchAsyncFiberIfNeeded() {
  if (!async_fb_.IsJoinable() && !migration_in_process_) {
    VLOG(1) << "[" << id_ << "] LaunchAsyncFiberIfNeeded ";
    async_fb_ = fb2::Fiber(fb2::Launch::post, "connection_dispatch", [this]() { AsyncFiber(); });
  }
}

// Should never block - the callers may run in as a a brief callback.
void Connection::SendAsync(MessageHandle msg) {
  DCHECK(cc_);
  DCHECK(listener());
  DCHECK_EQ(ProactorBase::me(), socket_->proactor());

  // "Closing" connections might be still processing commands, as we don't interrupt them.
  // So we still want to deliver control messages to them (like checkpoints) if
  // async_fb_ is running (joinable).
  if (cc_->conn_closing && (!msg.IsControl() || !async_fb_.IsJoinable()))
    return;

  // If we launch while closing, it won't be awaited. Control messages will be processed on cleanup.
  if (!cc_->conn_closing) {
    LaunchAsyncFiberIfNeeded();
  }

  DCHECK_NE(phase_, PRECLOSE);  // No more messages are processed after this point

  QueueBackpressure& qbp = GetQueueBackpressure();

  // Close MONITOR connection if we overflow pipeline limits
  if (msg.IsMonitor() &&
      qbp.IsPipelineBufferOverLimit(stats_->dispatch_queue_bytes, dispatch_q_.size())) {
    cc_->conn_closing = true;
    request_shutdown_ = true;
    // We don't shutdown here. The reason is that TLS socket is preemptive
    // and SendAsync is atomic.
    cnd_.notify_one();
    return;
  }

  size_t used_mem = msg.UsedMemory();
  ++local_stats_.dispatch_entries_added;
  stats_->dispatch_queue_entries++;
  stats_->dispatch_queue_bytes += used_mem;

  msg.dispatch_cycle = CycleClock::Now();
  if (msg.IsPubMsg()) {
    qbp.subscriber_bytes.fetch_add(used_mem, memory_order_relaxed);
    stats_->dispatch_queue_subscriber_bytes += used_mem;
  }

  // Squashing is only applied to redis commands
  if (std::holds_alternative<PipelineMessagePtr>(msg.handle)) {
    pending_pipeline_cmd_cnt_++;
    pending_pipeline_bytes_ += used_mem;
  }

  if (msg.IsControl()) {
    auto it = dispatch_q_.begin();
    while (it < dispatch_q_.end() && it->IsControl())
      ++it;
    dispatch_q_.insert(it, std::move(msg));
  } else {
    dispatch_q_.push_back(std::move(msg));
  }

  // Don't notify if a sync dispatch is in progress, it will wake after finishing.
  if (dispatch_q_.size() == 1 && !cc_->sync_dispatch) {
    cnd_.notify_one();
  }
}

void Connection::RecycleMessage(MessageHandle msg) {
  size_t used_mem = msg.UsedMemory();

  stats_->dispatch_queue_bytes -= used_mem;
  stats_->dispatch_queue_entries--;

  QueueBackpressure& qbp = GetQueueBackpressure();
  if (msg.IsPubMsg()) {
    qbp.subscriber_bytes.fetch_sub(used_mem, memory_order_relaxed);
    stats_->dispatch_queue_subscriber_bytes -= used_mem;
  }

  if (msg.IsPipelineMsg() && msg.dispatch_cycle) {
    ++stats_->pipelined_cmd_cnt;
    stats_->pipelined_cmd_latency += CycleClock::ToUsec(CycleClock::Now() - msg.dispatch_cycle);
  }

  // Retain pipeline message in pool.
  if (auto* pipe = get_if<PipelineMessagePtr>(&msg.handle); pipe) {
    DCHECK_GE(pending_pipeline_bytes_, used_mem);
    DCHECK_GE(pending_pipeline_cmd_cnt_, 1u);
    pending_pipeline_cmd_cnt_--;
    pending_pipeline_bytes_ -= used_mem;
    if (stats_->pipeline_cmd_cache_bytes < qbp.pipeline_cache_limit) {
      stats_->pipeline_cmd_cache_bytes += UsedMemoryInternal(*(*pipe));
      pipeline_req_pool_.push_back(std::move(*pipe));
    }
  }
}

std::string Connection::LocalBindStr() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto le = socket_->LocalEndpoint();
  return absl::StrCat(le.address().to_string(), ":", le.port());
}

std::string Connection::LocalBindAddress() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto le = socket_->LocalEndpoint();
  return le.address().to_string();
}

std::string Connection::RemoteEndpointStr() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto re = socket_->RemoteEndpoint();
  return absl::StrCat(re.address().to_string(), ":", re.port());
}

std::string Connection::RemoteEndpointAddress() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto re = socket_->RemoteEndpoint();
  return re.address().to_string();
}

facade::ConnectionContext* Connection::cntx() {
  return cc_.get();
}

void Connection::RequestAsyncMigration(util::fb2::ProactorBase* dest, bool force) {
  if ((!force && !migration_enabled_) || cc_ == nullptr) {
    return;
  }

  // Connections can migrate at most once.
  migration_enabled_ = false;
  migration_request_ = dest;
}

void Connection::StartTrafficLogging(string_view path) {
  OpenTrafficLogger(path);
}

void Connection::StopTrafficLogging() {
  lock_guard lk(tl_traffic_logger.mutex);
  tl_traffic_logger.ResetLocked();
}

bool Connection::IsHttp() const {
  return is_http_;
}

Connection::MemoryUsage Connection::GetMemoryUsage() const {
  size_t mem = sizeof(*this) + cmn::HeapSize(dispatch_q_) + cmn::HeapSize(name_) +
               cmn::HeapSize(tmp_parse_args_) + cmn::HeapSize(tmp_cmd_vec_) +
               cmn::HeapSize(memcache_parser_) + cmn::HeapSize(redis_parser_) + cmn::HeapSize(cc_) +
               cmn::HeapSize(reply_builder_);

  // We add a hardcoded 9k value to accomodate for the part of the Fiber stack that is in use.
  // The allocated stack is actually larger (~130k), but only a small fraction of that (9k
  // according to our checks) is actually part of the RSS.
  mem += 9'000;

  return {
      .mem = mem,
      .buf_mem = io_buf_.GetMemoryUsage(),
  };
}

void Connection::DecreaseStatsOnClose() {
  stats_->read_buf_capacity -= io_buf_.Capacity();
  DecrNumConns();
}

void Connection::BreakOnce(uint32_t ev_mask) {
  if (breaker_cb_) {
    DVLOG(1) << "[" << id_ << "] Connection::breaker_cb_ " << ev_mask;
    auto fun = std::move(breaker_cb_);
    DCHECK(!breaker_cb_);
    fun(ev_mask);
  }
}

void Connection::IncrNumConns() {
  if (IsMainOrMemcache())
    ++stats_->num_conns_main;
  else
    ++stats_->num_conns_other;
}

void Connection::DecrNumConns() {
  if (IsMainOrMemcache())
    --stats_->num_conns_main;
  else
    --stats_->num_conns_other;
}

bool Connection::IsReplySizeOverLimit() const {
  std::atomic<size_t>& reply_sz = tl_facade_stats->reply_stats.squashing_current_reply_size;
  size_t current = reply_sz.load(std::memory_order_acquire);
  const bool over_limit = reply_size_limit != 0 && current > 0 && current > reply_size_limit;
  if (over_limit) {
    LOG_EVERY_T(INFO, 10) << "Commands squashing current reply size is overlimit: " << current
                          << "/" << reply_size_limit
                          << ". Falling back to single command dispatch (instead of squashing)";
    // Used by testing. Should not be used in production, therefore debug log level 5.
    DVLOG(5) << "Commands squashing current reply size is overlimit: " << current << "/"
             << reply_size_limit
             << ". Falling back to single command dispatch (instead of squashing)";
  }
  return over_limit;
}

bool Connection::ParseMCBatch() {
  CHECK(io_buf_.InputLen() > 0);

  do {
    if (parsed_cmd_ == nullptr) {
      CreateParsedCommand();
    }
    uint32_t consumed = 0;

    MemcacheParser::Result result = memcache_parser_->Parse(io::View(io_buf_.InputBuffer()),
                                                            &consumed, parsed_cmd_->mc_command());

    io_buf_.ConsumeInput(consumed);

    DVLOG(2) << "mc_result " << result << " consumed: " << consumed << " type "
             << unsigned(parsed_cmd_->mc_command()->type);
    if (result == MemcacheParser::INPUT_PENDING)
      return false;

    EnqueueParsedCommand();

    if (result != MemcacheParser::OK) {
      // We can not just reply directly to parse error, as we may have pipelined commands before.
      // Fill the reply_payload with the error and continue parsing.
      memcache_parser_->Reset();
      auto client_error = [](string_view msg) { return absl::StrCat("CLIENT_ERROR ", msg); };

      parsed_tail_->SetDeferredReply();
      switch (result) {
        case MemcacheParser::UNKNOWN_CMD:
          parsed_tail_->SendSimpleString("ERROR");
          break;
        case MemcacheParser::PARSE_ERROR:
          parsed_tail_->SendSimpleString(client_error("bad data chunk"));
          break;
        case MemcacheParser::BAD_DELTA:
          parsed_tail_->SendSimpleString(client_error("invalid numeric delta argument"));
          break;
        default:
          parsed_tail_->SendSimpleString(client_error("bad command line format"));
          break;
      }
    }
  } while (parsed_cmd_q_len_ < 128 && io_buf_.InputLen() > 0);
  return true;
}

bool Connection::ExecuteMCBatch() {
  // Execute sequentially all parsed commands.
  while (parsed_to_execute_) {
    auto* cmd = parsed_to_execute_;
    bool is_head = (cmd == parsed_head_);

    bool has_replied = false;

    if (is_head) {
      // Protocol parse errors create commands with already cached replies.
      // Try sending payload now in case it's already set.
      has_replied = cmd->SendPayload();
      DVLOG(2) << "Maybe replying head: " << has_replied;
    } else {
      // We are not the head command, so we can not reply directly.
      if (cmd->IsDeferredReply()) {
        has_replied = true;  // The error reply is filled by the parser.
      } else {
        cmd->SetDeferredReply();
      }
    }

    if (!has_replied) {
      service_->DispatchMC(cmd);

      // If the reply was not deferred, then DispatchMC has surely replied.
      has_replied = !cmd->IsDeferredReply();
      DVLOG(2) << "Executed command, has_replied: " << has_replied;
    }
    parsed_to_execute_ = cmd->next;

    // Only if commands have deferred replies we need to keep them in the parsed queue
    // until they complete.
    if (is_head && has_replied) {
      // This is head and it replied to the client socket, so we can remove it from the parsed
      // queue right away.
      // This optimization makes the ReplyMCBatch call a no-op unless we actually run asynchronous
      // commands with deferred replies.
      parsed_head_ = parsed_to_execute_;
      ReleaseParsedCommand(cmd, parsed_head_ != nullptr /* is_pipelined */);
    }

    if (reply_builder_->GetError()) {
      return false;
    }
  }
  if (parsed_head_ == nullptr)
    parsed_tail_ = nullptr;
  return true;
}

bool Connection::ReplyMCBatch() {
  if (protocol_ != Protocol::MEMCACHE) {
    // We do not support async replies for RESP protocol yet.
    return true;
  }

  while (parsed_head_ != parsed_to_execute_) {
    auto* cmd = parsed_head_;
    if (!cmd->PollHeadForCompletion())
      break;

    // This command finished processing and can be replied.
    auto* next = cmd->next;

    cmd->SendPayload();
    ReleaseParsedCommand(cmd, next != parsed_to_execute_ /* is_pipelined */);
    parsed_head_ = next;
    if (reply_builder_->GetError()) {
      return false;
    }
  }

  if (parsed_head_ == nullptr)
    parsed_tail_ = nullptr;

  // Flush any remaining data in the reply builder.
  reply_builder_->Flush();
  return !reply_builder_->GetError();
}

void Connection::CreateParsedCommand() {
  parsed_cmd_ = service_->AllocateParsedCommand();
  parsed_cmd_->Init(reply_builder_.get(), cc_.get());
  if (protocol_ == Protocol::MEMCACHE)
    parsed_cmd_->CreateMemcacheCommand();
}

void Connection::EnqueueParsedCommand() {
  parsed_cmd_->next = nullptr;
  parsed_cmd_->parsed_cycle = base::CycleClock::Now();
  if (parsed_head_ == nullptr) {
    parsed_head_ = parsed_cmd_;
    parsed_to_execute_ = parsed_cmd_;
  } else {
    parsed_tail_->next = parsed_cmd_;
    if (parsed_to_execute_ == nullptr) {
      // we've executed all the parsed commands so far.
      parsed_to_execute_ = parsed_cmd_;
    }
    // We have a pipelined command
    local_stats_.dispatch_entries_added++;
  }
  parsed_tail_ = parsed_cmd_;
  stats_->dispatch_queue_bytes += parsed_cmd_->UsedMemory();

  parsed_cmd_ = nullptr;  // ownership transferred
  parsed_cmd_q_len_++;
}

void Connection::ReleaseParsedCommand(ParsedCommand* cmd, bool is_pipelined) {
  size_t used_mem = cmd->UsedMemory();
  DCHECK_GE(stats_->dispatch_queue_bytes, used_mem);
  DCHECK_GT(parsed_cmd_q_len_, 0u);
  stats_->dispatch_queue_bytes -= used_mem;
  --parsed_cmd_q_len_;
  if (is_pipelined) {
    stats_->pipelined_cmd_cnt++;
    stats_->pipelined_cmd_latency += CycleClock::ToUsec(CycleClock::Now() - cmd->parsed_cycle);
  }

  // Cache a single command for immediate reuse, otherwise free it.
  // TODO: we can cache parsed commands similarly to pipeline_req_pool_.
  // In fact we should unify both approaches.
  if (parsed_cmd_ == nullptr) {
    parsed_cmd_ = cmd;
    parsed_cmd_->ResetForReuse();
  } else {
    delete cmd;
  }
}

void Connection::DestroyParsedQueue() {
  while (parsed_head_ != nullptr) {
    auto* cmd = parsed_head_;
    stats_->dispatch_queue_bytes -= cmd->UsedMemory();
    parsed_head_ = cmd->next;

    if (cmd->MarkForDestruction()) {  // whether async operation finished or not started
      DVLOG(2) << "Deleting parsed command " << cmd;
      delete cmd;
    }
  }
  parsed_tail_ = nullptr;
  parsed_cmd_q_len_ = 0;
  delete parsed_cmd_;
  parsed_cmd_ = nullptr;
}

void Connection::UpdateFromFlags() {
  unsigned tid = fb2::ProactorBase::me()->GetPoolIndex();
  thread_queue_backpressure[tid].pipeline_queue_max_len = GetFlag(FLAGS_pipeline_queue_limit);
  thread_queue_backpressure[tid].pipeline_buffer_limit = GetFlag(FLAGS_pipeline_buffer_limit);
  thread_queue_backpressure[tid].pipeline_cnd.notify_all();

  max_busy_read_cycles_cached = base::CycleClock::FromUsec(GetFlag(FLAGS_max_busy_read_usec));
  always_flush_pipeline_cached = GetFlag(FLAGS_always_flush_pipeline);
  pipeline_squash_limit_cached = GetFlag(FLAGS_pipeline_squash_limit);
  pipeline_wait_batch_usec = GetFlag(FLAGS_pipeline_wait_batch_usec);
}

std::vector<std::string> Connection::GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_pipeline_queue_limit, FLAGS_pipeline_buffer_limit,
                            FLAGS_max_busy_read_usec, FLAGS_always_flush_pipeline,
                            FLAGS_pipeline_squash_limit, FLAGS_pipeline_wait_batch_usec);
}

void Connection::GetRequestSizeHistogramThreadLocal(std::string* hist) {
  if (io_req_size_hist)
    *hist = io_req_size_hist->ToString();
}

void Connection::TrackRequestSize(bool enable) {
  if (enable && !io_req_size_hist) {
    io_req_size_hist = new base::Histogram;
  } else if (!enable && io_req_size_hist) {
    delete io_req_size_hist;
    io_req_size_hist = nullptr;
  }
}

void Connection::EnsureMemoryBudget(unsigned tid) {
  thread_queue_backpressure[tid].EnsureBelowLimit();
}

ConnectionRef::ConnectionRef(const std::shared_ptr<Connection>& ptr, unsigned thread_id,
                             uint32_t client_id)
    : ptr_{ptr}, last_known_thread_id_{thread_id}, client_id_{client_id} {
}

Connection* ConnectionRef::Get() const {
  auto sptr = ptr_.lock();

  //  The connection can only be deleted on this thread, so
  //  this pointer is valid until the next suspension.
  //  Note: keeping a shared_ptr doesn't prolong the lifetime because
  //  it doesn't manage the underlying connection. See definition of `self_`.
  return sptr.get();
}

bool Connection::WeakRef::IsExpired() const {
  return ptr_.expired();
}

uint32_t Connection::WeakRef::GetClientId() const {
  return client_id_;
}

bool ConnectionRef::operator<(const ConnectionRef& other) const {
  return client_id_ < other.client_id_;
}

bool ConnectionRef::operator==(const ConnectionRef& other) const {
  return client_id_ == other.client_id_;
}

void Connection::DoReadOnRecv(const util::FiberSocketBase::RecvNotification& n) {
  if (std::holds_alternative<std::error_code>(n.read_result)) {
    io_ec_ = std::get<std::error_code>(n.read_result);
    return;
  }

  // TODO non epoll API via EnableRecvMultishot
  // if (std::holds_alternative<io::MutableBytes>(n.read_result))
  using RecvNoti = util::FiberSocketBase::RecvNotification::RecvCompletion;
  if (std::holds_alternative<RecvNoti>(n.read_result)) {
    if (!std::get<RecvNoti>(n.read_result)) {
      io_ec_ = make_error_code(errc::connection_aborted);
      return;
    }

    if (io_buf_.AppendLen() == 0) {
      // We will regrow in IoLoopV2
      return;
    }

    io::MutableBytes buf = io_buf_.AppendBuffer();
    io::Result<size_t> res = socket_->TryRecv(buf);

    if (res) {
      if (*res > 0) {
        // A recv call can return fewer bytes than requested even if the
        // socket buffer actually contains enough data to satisfy the full request.
        // TODO maybe worth looping here and try another recv call until it fails
        // with EAGAIN or EWOULDBLOCK. The problem there is that we need to handle
        // resizing if AppendBuffer is zero.
        io_buf_.CommitWrite(*res);
        return;
      }
      // *res == 0
      io_ec_ = make_error_code(errc::connection_aborted);
      return;
    }

    // error path (!res)
    auto ec = res.error();
    // EAGAIN and EWOULDBLOCK
    if (ec == errc::resource_unavailable_try_again || ec == errc::operation_would_block) {
      return;
    }

    io_ec_ = ec;
    return;
  }

  DCHECK(false) << "Should not reach here";
}

variant<error_code, Connection::ParserStatus> Connection::IoLoopV2() {
  size_t max_io_buf_len = GetFlag(FLAGS_max_client_iobuf_len);

  auto* peer = socket_.get();
  recv_buf_.res_len = 0;

  // Return early because RegisterOnRecv() should not be called if the socket
  // is not open. Both migrations and replication hit this flow upon cancellations.
  if (!peer->IsOpen()) {
    return ParserStatus::OK;
  }

  peer->RegisterOnRecv([this](const FiberSocketBase::RecvNotification& n) {
    DoReadOnRecv(n);
    io_event_.notify();
  });

  ParserStatus parse_status = OK;

  do {
    HandleMigrateRequest();

    // Poll again for readiness. The event handler registered above is edge triggered
    // (called once per socket readiness event). So, for example, it could be that the
    // cb read less data than it is available because of io_buf_ capacity. If after
    // an iteration the fiber does not poll the socket for more data it might deadlock.
    // TODO maybe use a flag instead of a poll
    DoReadOnRecv(FiberSocketBase::RecvNotification{true});
    io_event_.await([this]() {
      return io_buf_.InputLen() > 0 || (parsed_head_ && parsed_head_->PollHeadForCompletion()) ||
             io_ec_;
    });

    if (io_ec_) {
      LOG_IF(WARNING, cntx()->replica_conn) << "async io error: " << io_ec_;
      return std::exchange(io_ec_, {});
    }

    phase_ = PROCESS;
    bool is_iobuf_full = io_buf_.AppendLen() == 0;

    if (io_buf_.InputLen() > 0) {
      if (redis_parser_) {
        parse_status = ParseRedis(max_busy_read_cycles_cached);
      } else {
        DCHECK(memcache_parser_);
        parse_status = ParseMemcache();
      }
    } else {
      parse_status = NEED_MORE;
      if (parsed_head_) {
        ReplyMCBatch();
      }
    }

    if (reply_builder_->GetError()) {
      return reply_builder_->GetError();
    }

    if (parse_status == NEED_MORE) {
      parse_status = OK;

      size_t capacity = io_buf_.Capacity();
      if (capacity < max_io_buf_len) {
        size_t parser_hint = 0;
        if (redis_parser_)
          parser_hint = redis_parser_->parselen_hint();  // Could be done for MC as well.

        // If we got a partial request and we managed to parse its
        // length, make sure we have space to store it instead of
        // increasing space incrementally.
        // (Note: The buffer object is only working in power-of-2 sizes,
        // so there's no danger of accidental O(n^2) behavior.)
        if (parser_hint > capacity) {
          UpdateIoBufCapacity(io_buf_, stats_,
                              [&]() { io_buf_.Reserve(std::min(max_io_buf_len, parser_hint)); });
        }

        // If we got a partial request because iobuf was full, grow it up to
        // a reasonable limit to save on Recv() calls.
        if (is_iobuf_full && capacity < max_io_buf_len / 2) {
          // Last io used most of the io_buf to the end.
          UpdateIoBufCapacity(io_buf_, stats_, [&]() {
            io_buf_.Reserve(capacity * 2);  // Valid growth range.
          });
        }

        if (io_buf_.AppendLen() == 0U) {
          // it can happen with memcached but not for RedisParser, because RedisParser fully
          // consumes the passed buffer
          LOG_EVERY_T(WARNING, 10)
              << "Maximum io_buf length reached, consider to increase max_client_iobuf_len flag";
        }
      }
    } else if (parse_status != OK) {
      break;
    }
  } while (peer->IsOpen());

  return parse_status;
}

void ResetStats() {
  auto& cstats = tl_facade_stats->conn_stats;
  cstats.pipelined_cmd_cnt = 0;
  cstats.conn_received_cnt = 0;
  cstats.command_cnt_main = 0;
  cstats.command_cnt_other = 0;
  cstats.io_read_cnt = 0;
  cstats.io_read_bytes = 0;

  tl_facade_stats->reply_stats = {};
  if (io_req_size_hist)
    io_req_size_hist->Clear();
}

}  // namespace facade
