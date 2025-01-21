// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_connection.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <numeric>
#include <variant>

#include "base/flags.h"
#include "base/histogram.h"
#include "base/io_buf.h"
#include "base/logging.h"
#include "core/heap_size.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/memcache_parser.h"
#include "facade/redis_parser.h"
#include "facade/service_interface.h"
#include "io/file.h"
#include "util/fibers/proactor_base.h"

#ifdef DFLY_USE_SSL
#include "util/tls/tls_socket.h"
#endif

#ifdef __linux__
#include "util/fibers/uring_file.h"
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

ABSL_FLAG(uint64_t, request_cache_limit, 64_MB,
          "Amount of memory to use for request cache in bytes - per IO thread.");

ABSL_FLAG(uint64_t, pipeline_buffer_limit, 128_MB,
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

ABSL_FLAG(uint64_t, publish_buffer_limit, 128_MB,
          "Amount of memory to use for storing pub commands in bytes - per IO thread");

ABSL_FLAG(bool, no_tls_on_admin_port, false, "Allow non-tls connections on admin port");

ABSL_FLAG(uint32_t, pipeline_squash, 10,
          "Number of queued pipelined commands above which squashing is enabled, 0 means disabled");

// When changing this constant, also update `test_large_cmd` test in connection_test.py.
ABSL_FLAG(uint32_t, max_multi_bulk_len, 1u << 16,
          "Maximum multi-bulk (array) length that is "
          "allowed to be accepted when parsing RESP protocol");

ABSL_FLAG(size_t, max_client_iobuf_len, 1u << 16,
          "Maximum io buffer length that is used to read client requests.");

ABSL_FLAG(bool, migrate_connections, true,
          "When enabled, Dragonfly will try to migrate connections to the target thread on which "
          "they operate. Currently this is only supported for Lua script invocations, and can "
          "happen at most once per connection.");

using namespace util;
using namespace std;
using absl::GetFlag;
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
    log_file->Close();
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
  tl_traffic_logger.log_file->Write(version);
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
    blobs[index++] = iovec{.iov_base = const_cast<char*>(part.GetView().data()),
                           .iov_len = part.GetView().size()};
    if (index >= blobs.size()) {
      if (!tl_traffic_logger.Write(blobs.data(), blobs.size())) {
        return;
      }
      index = 0;
    }
  }

  if (index) {
    tl_traffic_logger.Write(blobs.data(), index);
  }
}

constexpr size_t kMinReadSize = 256;

thread_local uint32_t free_req_release_weight = 0;

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
  return *thread_queue_backpressure;
}

}  // namespace

thread_local vector<Connection::PipelineMessagePtr> Connection::pipeline_req_pool_;

void Connection::PipelineMessage::SetArgs(const RespVec& args) {
  auto* next = storage.data();
  for (size_t i = 0; i < args.size(); ++i) {
    RespExpr::Buffer buf = args[i].GetBuf();
    size_t s = buf.size();
    if (s)
      memcpy(next, buf.data(), s);
    next[s] = '\0';
    this->args[i] = MutableSlice(next, s);
    next += (s + 1);
  }
}

Connection::MCPipelineMessage::MCPipelineMessage(MemcacheParser::Command cmd_in,
                                                 std::string_view value_in)
    : cmd{std::move(cmd_in)}, value{value_in}, backing_size{0} {
  // Note: The process of laundering string_views should be placed in an utility function,
  // but there are no other uses like this so far.

  // Compute total size and create backing
  backing_size = cmd.key.size() + value.size();
  for (const auto& ext_key : cmd.keys_ext)
    backing_size += ext_key.size();

  backing = make_unique<char[]>(backing_size);

  // Copy everything into backing
  if (!cmd.key.empty())
    memcpy(backing.get(), cmd.key.data(), cmd.key.size());
  if (!value.empty())
    memcpy(backing.get() + cmd.key.size(), value.data(), value.size());
  size_t offset = cmd.key.size() + value.size();
  for (const auto& ext_key : cmd.keys_ext) {
    if (!ext_key.empty())
      memcpy(backing.get() + offset, ext_key.data(), ext_key.size());
    offset += ext_key.size();
  }

  // Update string_views
  cmd.key = string_view{backing.get(), cmd.key.size()};
  value = string_view{backing.get() + cmd.key.size(), value.size()};
  offset = cmd.key.size() + value.size();
  for (auto& key : cmd.keys_ext) {
    key = {backing.get() + offset, key.size()};
    offset += key.size();
  }
}

void Connection::MessageDeleter::operator()(PipelineMessage* msg) const {
  msg->~PipelineMessage();
  mi_free(msg);
}

void Connection::MessageDeleter::operator()(PubMessage* msg) const {
  msg->~PubMessage();
  mi_free(msg);
}

void Connection::PipelineMessage::Reset(size_t nargs, size_t capacity) {
  storage.resize(capacity);
  args.resize(nargs);
}

size_t Connection::PipelineMessage::StorageCapacity() const {
  return storage.capacity() + args.capacity();
}

size_t Connection::MessageHandle::UsedMemory() const {
  struct MessageSize {
    size_t operator()(const PubMessagePtr& msg) {
      return sizeof(PubMessage) + (msg->channel.size() + msg->message.size());
    }
    size_t operator()(const PipelineMessagePtr& msg) {
      return sizeof(PipelineMessage) + msg->args.capacity() * sizeof(MutableSlice) +
             msg->storage.capacity();
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
    size_t operator()(const MCPipelineMessagePtr& msg) {
      return sizeof(MCPipelineMessage) + msg->backing_size +
             msg->cmd.keys_ext.size() * sizeof(string_view);
    }
  };

  return sizeof(MessageHandle) + visit(MessageSize{}, this->handle);
}

bool Connection::MessageHandle::IsReplying() const {
  return IsPubMsg() || holds_alternative<MonitorMessage>(handle) ||
         holds_alternative<PipelineMessagePtr>(handle) ||
         (holds_alternative<MCPipelineMessagePtr>(handle) &&
          !get<MCPipelineMessagePtr>(handle)->cmd.no_reply);
}

struct Connection::AsyncOperations {
  AsyncOperations(SinkReplyBuilder* b, Connection* me)
      : stats{&tl_facade_stats->conn_stats}, builder{b}, self(me) {
  }

  void operator()(const PubMessage& msg);
  void operator()(PipelineMessage& msg);
  void operator()(const MCPipelineMessage& msg);
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
    }
  }
}

void Connection::AsyncOperations::operator()(const PubMessage& pub_msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  unsigned i = 0;
  array<string_view, 4> arr;
  if (pub_msg.pattern.empty()) {
    arr[i++] = "message";
  } else {
    arr[i++] = "pmessage";
    arr[i++] = pub_msg.pattern;
  }
  arr[i++] = pub_msg.channel;
  arr[i++] = pub_msg.message;
  rbuilder->SendBulkStrArr(absl::Span<string_view>{arr.data(), i},
                           RedisReplyBuilder::CollectionType::PUSH);
}

void Connection::AsyncOperations::operator()(Connection::PipelineMessage& msg) {
  DVLOG(2) << "Dispatching pipeline: " << ToSV(msg.args.front());

  self->service_->DispatchCommand(CmdArgList{msg.args.data(), msg.args.size()},
                                  self->reply_builder_.get(), self->cc_.get());

  self->last_interaction_ = time(nullptr);
  self->skip_next_squashing_ = false;
}

void Connection::AsyncOperations::operator()(const MCPipelineMessage& msg) {
  self->service_->DispatchMC(msg.cmd, msg.value,
                             static_cast<MCReplyBuilder*>(self->reply_builder_.get()),
                             self->cc_.get());
  self->last_interaction_ = time(nullptr);
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
  rbuilder->StartCollection(2, facade::RedisReplyBuilder::CollectionType::PUSH);
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
  static_assert(kReqSz <= 256 && kReqSz >= 200);

  switch (protocol) {
    case Protocol::REDIS:
      redis_parser_.reset(new RedisParser(GetFlag(FLAGS_max_multi_bulk_len)));
      break;
    case Protocol::MEMCACHE:
      memcache_parser_.reset(new MemcacheParser);
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
}

void Connection::OnPreMigrateThread() {
  DVLOG(1) << "OnPreMigrateThread " << GetClientId();

  CHECK(!cc_->conn_closing);

  DCHECK(!migration_in_process_);

  // CancelOnErrorCb is a preemption point, so we make sure the Migration start
  // is marked beforehand.
  migration_in_process_ = true;

  socket_->CancelOnErrorCb();
  DCHECK(!async_fb_.IsJoinable()) << GetClientId();
}

void Connection::OnPostMigrateThread() {
  DVLOG(1) << "[" << id_ << "] OnPostMigrateThread";

  // Once we migrated, we should rearm OnBreakCb callback.
  if (breaker_cb_ && socket()->IsOpen()) {
    socket_->RegisterOnErrorCb([this](int32_t mask) { this->OnBreakCb(mask); });
  }
  migration_in_process_ = false;
  DCHECK(!async_fb_.IsJoinable());

  // If someone had sent Async during the migration, we must create async_fb_.
  if (!dispatch_q_.empty()) {
    LaunchAsyncFiberIfNeeded();
  }

  stats_ = &tl_facade_stats->conn_stats;
  ++stats_->num_conns;
  stats_->read_buf_capacity += io_buf_.Capacity();
}

void Connection::OnConnectionStart() {
  ThisFiber::SetName("DflyConnection");

  stats_ = &tl_facade_stats->conn_stats;
}

void Connection::HandleRequests() {
  VLOG(1) << "[" << id_ << "] HandleRequests";

  if (GetFlag(FLAGS_tcp_nodelay) && !socket_->IsUDS()) {
    int val = 1;
    int res = setsockopt(socket_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    DCHECK_EQ(res, 0);
  }

  auto remote_ep = RemoteEndpointStr();

#ifdef DFLY_USE_SSL
  if (ssl_ctx_) {
    const bool no_tls_on_admin_port = GetFlag(FLAGS_no_tls_on_admin_port);
    if (!(IsPrivileged() && no_tls_on_admin_port)) {
      // Must be done atomically before the premption point in Accept so that at any
      // point in time, the socket_ is defined.
      uint8_t buf[2];
      auto read_sz = socket_->Read(io::MutableBytes(buf));
      if (!read_sz || *read_sz < sizeof(buf)) {
        VLOG(1) << "Error reading from peer " << remote_ep << " " << read_sz.error().message();
        return;
      }
      if (buf[0] != 0x16 || buf[1] != 0x03) {
        VLOG(1) << "Bad TLS header "
                << absl::StrCat(absl::Hex(buf[0], absl::kZeroPad2),
                                absl::Hex(buf[1], absl::kZeroPad2));
        socket_->Write(
            io::Buffer("-ERR Bad TLS header, double check "
                       "if you enabled TLS for your client.\r\n"));
      }

      {
        FiberAtomicGuard fg;
        unique_ptr<tls::TlsSocket> tls_sock = make_unique<tls::TlsSocket>(std::move(socket_));
        tls_sock->InitSSL(ssl_ctx_, buf);
        SetSocket(tls_sock.release());
      }
      FiberSocketBase::AcceptResult aresult = socket_->Accept();

      if (!aresult) {
        LOG(INFO) << "Error handshaking " << aresult.error().message();
        return;
      }
      is_tls_ = 1;
      VLOG(1) << "TLS handshake succeeded";
    }
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
      if (breaker_cb_) {
        socket_->RegisterOnErrorCb([this](int32_t mask) { this->OnBreakCb(mask); });
      }
      switch (protocol_) {
        case Protocol::REDIS:
          reply_builder_.reset(new RedisReplyBuilder(socket_.get()));
          break;
        case Protocol::MEMCACHE:
          reply_builder_.reset(new MCReplyBuilder(socket_.get()));
          break;
        default:
          break;
      }
      ConnectionFlow();

      socket_->CancelOnErrorCb();  // noop if nothing is registered.
      VLOG(1) << "Closed connection for peer "
              << GetClientInfo(fb2::ProactorBase::me()->GetPoolIndex());
      reply_builder_.reset();
    }
    cc_.reset();
  }
}

void Connection::RegisterBreakHook(BreakerCb breaker_cb) {
  breaker_cb_ = breaker_cb;
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

  string after;
  absl::StrAppend(&after, " irqmatch=", int(cpu == my_cpu_id));
  if (dispatch_q_.size()) {
    absl::StrAppend(&after, " pipeline=", dispatch_q_.size());
  }
  absl::StrAppend(&after, " age=", now - creation_time_, " idle=", now - last_interaction_);
  string_view phase_name = PHASE_NAMES[phase_];

  if (cc_) {
    string cc_info = service_->GetContextInfo(cc_.get()).Format();

    // reply_builder_ may be null if the connection is in the setup phase, for example.
    if (reply_builder_ && reply_builder_->IsSendActive())
      phase_name = "send";
    absl::StrAppend(&after, " ", cc_info);
  }
  absl::StrAppend(&after, " phase=", phase_name);

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
  return static_cast<Listener*>(listener())->IsMainInterface();
}

void Connection::SetName(string name) {
  util::ThisFiber::SetName(absl::StrCat("DflyConnection_", name));
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

  ++stats_->num_conns;
  ++stats_->conn_received_cnt;
  stats_->read_buf_capacity += io_buf_.Capacity();

  ParserStatus parse_status = OK;

  // At the start we read from the socket to determine the HTTP/Memstore protocol.
  // Therefore we may already have some data in the buffer.
  if (io_buf_.InputLen() > 0) {
    phase_ = PROCESS;
    if (redis_parser_) {
      parse_status = ParseRedis();
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }
  }

  error_code ec = reply_builder_->GetError();

  // Main loop.
  if (parse_status != ERROR && !ec) {
    UpdateIoBufCapacity(io_buf_, stats_, [&]() { io_buf_.EnsureCapacity(64); });
    auto res = IoLoop();

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
    if (!ec2) {
      while (true) {
        // Discard any received data.
        io_buf_.Clear();
        if (!socket_->Recv(io_buf_.AppendBuffer())) {
          break;
        }
      }
    }
  }

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    string conn_info = service_->GetContextInfo(cc_.get()).Format();
    LOG(WARNING) << "Socket error for connection " << conn_info << " " << GetName()
                 << " during phase " << kPhaseName[phase_] << " : " << ec << " " << ec.message();
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
    if (cc_->conn_closing)
      return;

    // prefer synchronous dispatching to save memory.
    optimize_for_async = false;
    last_interaction_ = time(nullptr);
  }

  // Avoid sync dispatch if we can interleave with an ongoing async dispatch.
  bool can_dispatch_sync = !cc_->async_dispatch && dispatch_q_.empty() && cc_->subscriptions == 0;

  // Dispatch async if we're handling a pipeline or if we can't dispatch sync.
  if (optimize_for_async || !can_dispatch_sync) {
    SendAsync(cmd_msg_cb());

    auto epoch = fb2::FiberSwitchEpoch();

    if (async_fiber_epoch_ == epoch) {
      // If we pushed too many items without context switching - yield
      if (++async_streak_len_ >= 10 && !cc_->async_dispatch) {
        async_streak_len_ = 0;
        ThisFiber::Yield();
      }
    } else {
      async_streak_len_ = 0;
      async_fiber_epoch_ = epoch;
    }
  } else {
    ShrinkPipelinePool();  // Gradually release pipeline request pool.
    {
      cc_->sync_dispatch = true;
      invoke_cb();
      cc_->sync_dispatch = false;
    }
    last_interaction_ = time(nullptr);

    // We might have blocked the dispatch queue from processing, wake it up.
    if (dispatch_q_.size() > 0)
      cnd_.notify_one();
  }
}

Connection::ParserStatus Connection::ParseRedis() {
  uint32_t consumed = 0;
  RedisParser::Result result = RedisParser::OK;

  // Re-use connection local resources to reduce allocations
  RespVec& parse_args = tmp_parse_args_;
  CmdArgVec& cmd_vec = tmp_cmd_vec_;

  auto dispatch_sync = [this, &parse_args, &cmd_vec] {
    RespExpr::VecToArgList(parse_args, &cmd_vec);
    service_->DispatchCommand(absl::MakeSpan(cmd_vec), reply_builder_.get(), cc_.get());
  };
  auto dispatch_async = [this, &parse_args, tlh = mi_heap_get_backing()]() -> MessageHandle {
    return {FromArgs(std::move(parse_args), tlh)};
  };

  do {
    result = redis_parser_->Parse(io_buf_.InputBuffer(), &consumed, &parse_args);
    request_consumed_bytes_ += consumed;
    if (result == RedisParser::OK && !parse_args.empty()) {
      if (RespExpr& first = parse_args.front(); first.type == RespExpr::STRING)
        DVLOG(2) << "Got Args with first token " << ToSV(first.GetBuf());

      if (io_req_size_hist)
        io_req_size_hist->Add(request_consumed_bytes_);
      request_consumed_bytes_ = 0;
      bool has_more = consumed < io_buf_.InputLen();

      if (tl_traffic_logger.log_file && IsMain() /* log only on the main interface */) {
        LogTraffic(id_, has_more, absl::MakeSpan(parse_args), service_->GetContextInfo(cc_.get()));
      }

      DispatchSingle(has_more, dispatch_sync, dispatch_async);
    }
    io_buf_.ConsumeInput(consumed);
  } while (RedisParser::OK == result && io_buf_.InputLen() > 0 && !reply_builder_->GetError());

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}

auto Connection::ParseMemcache() -> ParserStatus {
  uint32_t consumed = 0;
  MemcacheParser::Result result = MemcacheParser::OK;

  MemcacheParser::Command cmd;
  string_view value;

  auto dispatch_sync = [this, &cmd, &value] {
    service_->DispatchMC(cmd, value, static_cast<MCReplyBuilder*>(reply_builder_.get()), cc_.get());
  };

  auto dispatch_async = [&cmd, &value]() -> MessageHandle {
    return {make_unique<MCPipelineMessage>(std::move(cmd), value)};
  };

  MCReplyBuilder* builder = static_cast<MCReplyBuilder*>(reply_builder_.get());

  do {
    string_view str = ToSV(io_buf_.InputBuffer());

    if (str.empty()) {
      return OK;
    }

    result = memcache_parser_->Parse(str, &consumed, &cmd);

    DVLOG(2) << "mc_result " << result << " consumed: " << consumed << " type " << cmd.type;
    if (result != MemcacheParser::OK) {
      io_buf_.ConsumeInput(consumed);
      break;
    }

    size_t total_len = consumed;
    if (MemcacheParser::IsStoreCmd(cmd.type)) {
      total_len += cmd.bytes_len + 2;
      if (io_buf_.InputLen() >= total_len) {
        string_view parsed_value = str.substr(consumed, cmd.bytes_len + 2);
        if (parsed_value[cmd.bytes_len] != '\r' && parsed_value[cmd.bytes_len + 1] != '\n') {
          builder->SendClientError("bad data chunk");
          // We consume the whole buffer because we don't really know where it ends
          // since the value length exceeds the cmd.bytes_len.
          io_buf_.ConsumeInput(io_buf_.InputLen());
          return OK;
        }

        value = parsed_value.substr(0, cmd.bytes_len);
      } else {
        return NEED_MORE;
      }
    }
    DispatchSingle(total_len < io_buf_.InputLen(), dispatch_sync, dispatch_async);
    io_buf_.ConsumeInput(total_len);
  } while (!builder->GetError());

  parser_error_ = result;

  if (result == MemcacheParser::INPUT_PENDING) {
    return NEED_MORE;
  }

  if (result == MemcacheParser::PARSE_ERROR || result == MemcacheParser::UNKNOWN_CMD) {
    builder->SendSimpleString("ERROR");
  } else if (result == MemcacheParser::BAD_DELTA) {
    builder->SendClientError("invalid numeric delta argument");
  } else if (result != MemcacheParser::OK) {
    builder->SendClientError("bad command line format");
  }

  return OK;
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
    stats_->num_migrations++;
    migration_request_ = nullptr;

    DecreaseStatsOnClose();

    // We need to return early as the socket is closing and IoLoop will clean up.
    // The reason that this is true is because of the following DCHECK
    DCHECK(!async_fb_.IsJoinable());

    // which can never trigger since we Joined on the async_fb_ above and we are
    // atomic in respect to our proactor meaning that no other fiber will
    // launch the DispatchFiber.
    if (!this->Migrate(dest)) {
      return;
    }
  }
}

error_code Connection::HandleRecvSocket() {
  io::MutableBytes append_buf = io_buf_.AppendBuffer();
  DCHECK(!append_buf.empty());

  phase_ = READ_SOCKET;
  error_code ec;

  ::io::Result<size_t> recv_sz = socket_->Recv(append_buf);
  last_interaction_ = time(nullptr);

  if (!recv_sz) {
    return recv_sz.error();
  }

  size_t commit_sz = *recv_sz;

  io_buf_.CommitWrite(commit_sz);
  stats_->io_read_bytes += commit_sz;
  ++stats_->io_read_cnt;

  return ec;
}

auto Connection::IoLoop() -> variant<error_code, ParserStatus> {
  error_code ec;
  ParserStatus parse_status = OK;

  size_t max_iobfuf_len = GetFlag(FLAGS_max_client_iobuf_len);
  auto* peer = socket_.get();

  do {
    HandleMigrateRequest();
    ec = HandleRecvSocket();
    if (ec) {
      return ec;
    }

    phase_ = PROCESS;
    bool is_iobuf_full = io_buf_.AppendLen() == 0;

    if (redis_parser_) {
      parse_status = ParseRedis();
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
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

        // If we got a partial request and we couldn't parse the length, just
        // double the capacity.
        // If we got a partial request because iobuf was full, grow it up to
        // a reasonable limit to save on Recv() calls.
        if (io_buf_.AppendLen() < 64u || (is_iobuf_full && capacity < 4096)) {
          // Last io used most of the io_buf to the end.
          UpdateIoBufCapacity(io_buf_, stats_, [&]() {
            io_buf_.Reserve(capacity * 2);  // Valid growth range.
          });
        }

        DCHECK_GT(io_buf_.AppendLen(), 0U);
      } else if (io_buf_.AppendLen() == 0) {
        // We have a full buffer and we can not progress with parsing.
        // This means that we have request too large.
        LOG(ERROR) << "Request is too large, closing connection";
        parse_status = ERROR;
        break;
      }
    } else if (parse_status != OK) {
      break;
    }
    ec = reply_builder_->GetError();
  } while (peer->IsOpen() && !ec);

  if (ec)
    return ec;

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

  vector<ArgSlice> squash_cmds;
  squash_cmds.reserve(dispatch_q_.size());

  for (auto& msg : dispatch_q_) {
    CHECK(holds_alternative<PipelineMessagePtr>(msg.handle))
        << msg.handle.index() << " on " << DebugInfo();

    auto& pmsg = get<PipelineMessagePtr>(msg.handle);
    squash_cmds.push_back(absl::MakeSpan(pmsg->args));
  }

  cc_->async_dispatch = true;

  size_t dispatched =
      service_->DispatchManyCommands(absl::MakeSpan(squash_cmds), reply_builder_.get(), cc_.get());

  if (pending_pipeline_cmd_cnt_ == squash_cmds.size()) {  // Flush if no new commands appeared
    reply_builder_->Flush();
    reply_builder_->SetBatchMode(false);  // in case the next dispatch is sync
  }

  cc_->async_dispatch = false;

  auto it = dispatch_q_.begin();
  while (it->IsControl())  // Skip all newly received intrusive messages
    ++it;

  for (auto rit = it; rit != it + dispatched; ++rit)
    RecycleMessage(std::move(*rit));

  dispatch_q_.erase(it, it + dispatched);

  // If interrupted due to pause, fall back to regular dispatch
  skip_next_squashing_ = dispatched != squash_cmds.size();
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

  absl::StrAppend(&info, "address=", uint64_t(this), ", ");
  absl::StrAppend(&info, "phase=", phase_, ", ");
  if (cc_) {
    // In some rare cases cc_ can be null, see https://github.com/dragonflydb/dragonfly/pull/3873
    absl::StrAppend(&info, "dispatch(s/a)=", cc_->sync_dispatch, " ", cc_->async_dispatch, ", ");
    absl::StrAppend(&info, "closing=", cc_->conn_closing, ", ");
  }
  absl::StrAppend(&info, "dispatch_fiber:joinable=", async_fb_.IsJoinable(), ", ");

  bool intrusive_front = dispatch_q_.size() > 0 && dispatch_q_.front().IsControl();
  absl::StrAppend(&info, "dispatch_queue:size=", dispatch_q_.size(), ", ");
  absl::StrAppend(&info, "dispatch_queue:pipelined=", pending_pipeline_cmd_cnt_, ", ");
  absl::StrAppend(&info, "dispatch_queue:intrusive=", intrusive_front, ", ");

  if (cc_) {
    absl::StrAppend(&info, "state=");
    if (cc_->paused)
      absl::StrAppend(&info, "p");
    if (cc_->blocked)
      absl::StrAppend(&info, "b");
  }

  absl::StrAppend(&info, "}");
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
      ThisFiber::Yield();
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
    if (squashing_enabled && threshold_reached && are_all_plain_cmds && !skip_next_squashing_) {
      SquashPipeline();
    } else {
      MessageHandle msg = std::move(dispatch_q_.front());
      dispatch_q_.pop_front();

      // We keep the batch mode enabled as long as the dispatch queue is not empty, relying on the
      // last command to reply and flush. If it doesn't reply (i.e. is a control message like
      // migrate), we have to flush manually.
      if (dispatch_q_.empty() && !msg.IsReplying()) {
        reply_builder_->Flush();
      }

      if (ShouldEndAsyncFiber(msg)) {
        RecycleMessage(std::move(msg));
        CHECK(dispatch_q_.empty()) << DebugInfo();
        qbp.pipeline_cnd.notify_all();
        return;  // don't set conn closing flag
      }

      cc_->async_dispatch = true;
      std::visit(async_op, msg.handle);
      cc_->async_dispatch = false;
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

Connection::PipelineMessagePtr Connection::FromArgs(RespVec args, mi_heap_t* heap) {
  DCHECK(!args.empty());
  size_t backed_sz = 0;
  for (const auto& arg : args) {
    CHECK_EQ(RespExpr::STRING, arg.type);
    backed_sz += arg.GetBuf().size() + 1;  // for '\0'
  }
  DCHECK(backed_sz);

  constexpr auto kReqSz = sizeof(PipelineMessage);
  static_assert(kReqSz < MI_SMALL_SIZE_MAX);
  static_assert(alignof(PipelineMessage) == 8);

  PipelineMessagePtr ptr;
  if (ptr = GetFromPipelinePool(); ptr) {
    ptr->Reset(args.size(), backed_sz);
  } else {
    void* heap_ptr = mi_heap_malloc_small(heap, sizeof(PipelineMessage));
    // We must construct in place here, since there is a slice that uses memory locations
    ptr.reset(new (heap_ptr) PipelineMessage(args.size(), backed_sz));
  }

  ptr->SetArgs(args);
  return ptr;
}

void Connection::ShrinkPipelinePool() {
  if (pipeline_req_pool_.empty())
    return;

  // The request pool is shared by all the connections in the thread so we do not want
  // to release it aggressively just because some connection is running in
  // non-pipelined mode. So by using free_req_release_weight we wait at least N times,
  // where N is the number of connections in the thread.
  ++free_req_release_weight;

  if (free_req_release_weight > stats_->num_conns) {
    free_req_release_weight = 0;
    stats_->pipeline_cmd_cache_bytes -= pipeline_req_pool_.back()->StorageCapacity();
    pipeline_req_pool_.pop_back();
  }
}

Connection::PipelineMessagePtr Connection::GetFromPipelinePool() {
  if (pipeline_req_pool_.empty())
    return nullptr;

  free_req_release_weight = 0;  // Reset the release weight.
  auto ptr = std::move(pipeline_req_pool_.back());
  stats_->pipeline_cmd_cache_bytes -= ptr->StorageCapacity();
  pipeline_req_pool_.pop_back();
  return ptr;
}

void Connection::ShutdownSelf() {
  util::Connection::Shutdown();
}

bool Connection::Migrate(util::fb2::ProactorBase* dest) {
  // Migrate is used only by replication, so it doesn't have properties of full-fledged
  // connections
  CHECK(!cc_->async_dispatch);
  CHECK_EQ(cc_->subscriptions, 0);  // are bound to thread local caches
  CHECK_EQ(self_.use_count(), 1u);  // references cache our thread and backpressure
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

  return WeakRef(self_, socket_->proactor()->GetPoolIndex(), id_);
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
  void* ptr = mi_malloc(sizeof(PubMessage));
  SendAsync({PubMessagePtr{new (ptr) PubMessage{std::move(msg)}, MessageDeleter{}}});
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

void Connection::SendAsync(MessageHandle msg) {
  DCHECK(cc_);
  DCHECK(listener());
  DCHECK_EQ(ProactorBase::me(), socket_->proactor());

  // "Closing" connections might be still processing commands, as we don't interrupt them.
  // So we still want to deliver control messages to them (like checkpoints).
  if (cc_->conn_closing && !msg.IsControl())
    return;

  // If we launch while closing, it won't be awaited. Control messages will be processed on cleanup.
  if (!cc_->conn_closing) {
    LaunchAsyncFiberIfNeeded();
  }

  DCHECK_NE(phase_, PRECLOSE);  // No more messages are processed after this point

  size_t used_mem = msg.UsedMemory();
  stats_->dispatch_queue_entries++;
  stats_->dispatch_queue_bytes += used_mem;

  msg.dispatch_ts = ProactorBase::GetMonotonicTimeNs();
  if (msg.IsPubMsg()) {
    QueueBackpressure& qbp = GetQueueBackpressure();
    qbp.subscriber_bytes.fetch_add(used_mem, memory_order_relaxed);
    stats_->dispatch_queue_subscriber_bytes += used_mem;
  }

  // Squashing is only applied to redis commands
  if (std::holds_alternative<PipelineMessagePtr>(msg.handle)) {
    pending_pipeline_cmd_cnt_++;
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

  if (msg.IsPipelineMsg()) {
    ++stats_->pipelined_cmd_cnt;
    stats_->pipelined_cmd_latency += (ProactorBase::GetMonotonicTimeNs() - msg.dispatch_ts) / 1000;
  }

  // Retain pipeline message in pool.
  if (auto* pipe = get_if<PipelineMessagePtr>(&msg.handle); pipe) {
    pending_pipeline_cmd_cnt_--;
    if (stats_->pipeline_cmd_cache_bytes < qbp.pipeline_cache_limit) {
      stats_->pipeline_cmd_cache_bytes += (*pipe)->StorageCapacity();
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

void Connection::RequestAsyncMigration(util::fb2::ProactorBase* dest) {
  if (!migration_enabled_ || cc_ == nullptr) {
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
  size_t mem = sizeof(*this) + dfly::HeapSize(dispatch_q_) + dfly::HeapSize(name_) +
               dfly::HeapSize(tmp_parse_args_) + dfly::HeapSize(tmp_cmd_vec_) +
               dfly::HeapSize(memcache_parser_) + dfly::HeapSize(redis_parser_) +
               dfly::HeapSize(cc_) + dfly::HeapSize(reply_builder_);

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

  --stats_->num_conns;
}

void Connection::BreakOnce(uint32_t ev_mask) {
  if (breaker_cb_) {
    DVLOG(1) << "[" << id_ << "] Connection::breaker_cb_ " << ev_mask;
    auto fun = std::move(breaker_cb_);
    DCHECK(!breaker_cb_);
    fun(ev_mask);
  }
}

void Connection::SetMaxQueueLenThreadLocal(unsigned tid, uint32_t val) {
  thread_queue_backpressure[tid].pipeline_queue_max_len = val;
  thread_queue_backpressure[tid].pipeline_cnd.notify_all();
}

void Connection::SetPipelineBufferLimit(unsigned tid, size_t val) {
  thread_queue_backpressure[tid].pipeline_buffer_limit = val;
  thread_queue_backpressure[tid].pipeline_cnd.notify_all();
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

Connection::WeakRef::WeakRef(std::shared_ptr<Connection> ptr, unsigned thread_id,
                             uint32_t client_id)
    : ptr_{ptr}, thread_id_{thread_id}, client_id_{client_id} {
}

unsigned Connection::WeakRef::Thread() const {
  return thread_id_;
}

Connection* Connection::WeakRef::Get() const {
  // We should never access the connection object from other threads.
  DCHECK_EQ(ProactorBase::me()->GetPoolIndex(), int(thread_id_));

  //  The connection can only be deleted on this thread, so
  //  this pointer is valid until the next suspension.
  //  Note: keeping a shared_ptr doesn't prolong the lifetime because
  //  it doesn't manage the underlying connection. See definition of `self_`.
  return ptr_.lock().get();
}

bool Connection::WeakRef::IsExpired() const {
  return ptr_.expired();
}

uint32_t Connection::WeakRef::GetClientId() const {
  return client_id_;
}

bool Connection::WeakRef::operator<(const WeakRef& other) {
  return client_id_ < other.client_id_;
}

bool Connection::WeakRef::operator==(const WeakRef& other) const {
  return client_id_ == other.client_id_;
}

void ResetStats() {
  auto& cstats = tl_facade_stats->conn_stats;
  cstats.command_cnt = 0;
  cstats.pipelined_cmd_cnt = 0;
  cstats.conn_received_cnt = 0;
  cstats.pipelined_cmd_cnt = 0;
  cstats.command_cnt = 0;
  cstats.io_read_cnt = 0;
  cstats.io_read_bytes = 0;

  tl_facade_stats->reply_stats = {};
  if (io_req_size_hist)
    io_req_size_hist->Clear();
}

}  // namespace facade
