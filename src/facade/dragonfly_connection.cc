// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_connection.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/memcache_parser.h"
#include "facade/redis_parser.h"
#include "facade/service_interface.h"
#include "server/conn_context.h"
#include "util/fibers/proactor_base.h"

#ifdef DFLY_USE_SSL
#include "util/tls/tls_socket.h"
#endif

using namespace std;

ABSL_FLAG(bool, tcp_nodelay, false,
          "Configures dragonfly connections with socket option TCP_NODELAY");
ABSL_FLAG(bool, primary_port_http_enabled, true,
          "If true allows accessing http console on main TCP port");

ABSL_FLAG(uint16_t, admin_port, 0,
          "If set, would enable admin access to console on the assigned port. "
          "This supports both HTTP and RESP protocols");

ABSL_FLAG(string, admin_bind, "",
          "If set, the admin consol TCP connection would be bind the given address. "
          "This supports both HTTP and RESP protocols");

ABSL_FLAG(uint64_t, request_cache_limit, 1ULL << 26,  // 64MB
          "Amount of memory to use for request cache in bytes - per IO thread.");

ABSL_FLAG(bool, no_tls_on_admin_port, false, "Allow non-tls connections on admin port");

ABSL_FLAG(uint64_t, pipeline_squash, 0,
          "Number of queued pipelined commands above which squashing is enabled, 0 means disabled");

// When changing this constant, also update `test_large_cmd` test in connection_test.py.
ABSL_FLAG(uint32_t, max_multi_bulk_len, 1u << 16,
          "Maximum multi-bulk (array) length that is "
          "allowed to be accepted when parsing RESP protocol");

ABSL_FLAG(size_t, max_client_iobuf_len, 1u << 16,
          "Maximum io buffer length that is used to read client requests.");

using namespace util;
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

void FetchBuilderStats(ConnectionStats* stats, SinkReplyBuilder* builder) {
  stats->io_write_cnt += builder->io_write_cnt();
  stats->io_write_bytes += builder->io_write_bytes();

  for (const auto& k_v : builder->err_count()) {
    stats->err_count_map[k_v.first] += k_v.second;
  }
  builder->reset_io_stats();
}

// TODO: to implement correct matcher according to HTTP spec
// https://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html
// One place to find a good implementation would be https://github.com/h2o/picohttpparser
bool MatchHttp11Line(string_view line) {
  return absl::StartsWith(line, "GET ") && absl::EndsWith(line, "HTTP/1.1");
}

constexpr size_t kMinReadSize = 256;
constexpr size_t kMaxDispatchQMemory = 5_MB;

thread_local uint32_t free_req_release_weight = 0;

}  // namespace

thread_local vector<Connection::PipelineMessagePtr> Connection::pipeline_req_pool_;

struct Connection::Shutdown {
  absl::flat_hash_map<ShutdownHandle, ShutdownCb> map;
  ShutdownHandle next_handle = 1;

  ShutdownHandle Add(ShutdownCb cb) {
    map[next_handle] = move(cb);
    return next_handle++;
  }

  void Remove(ShutdownHandle sh) {
    map.erase(sh);
  }
};

Connection::PubMessage::PubMessage(string pattern, shared_ptr<char[]> buf, size_t channel_len,
                                   size_t message_len)
    : pattern{move(pattern)}, buf{move(buf)}, channel_len{channel_len}, message_len{message_len} {
}

string_view Connection::PubMessage::Channel() const {
  return {buf.get(), channel_len};
}

string_view Connection::PubMessage::Message() const {
  return {buf.get() + channel_len, message_len};
}

struct Connection::DispatchOperations {
  DispatchOperations(SinkReplyBuilder* b, Connection* me)
      : stats{me->service_->GetThreadLocalConnectionStats()}, builder{b}, self(me) {
  }

  void operator()(const PubMessage& msg);
  void operator()(Connection::PipelineMessage& msg);
  void operator()(const MonitorMessage& msg);
  void operator()(const AclUpdateMessage& msg);

  template <typename T, typename D> void operator()(unique_ptr<T, D>& ptr) {
    operator()(*ptr.get());
  }

  ConnectionStats* stats = nullptr;
  SinkReplyBuilder* builder = nullptr;
  Connection* self = nullptr;
};

void Connection::PipelineMessage::SetArgs(const RespVec& args) {
  auto* next = storage.data();
  for (size_t i = 0; i < args.size(); ++i) {
    RespExpr::Buffer buf = args[i].GetBuf();
    size_t s = buf.size();
    if (s)
      memcpy(next, buf.data(), s);
    this->args[i] = MutableSlice(next, s);
    next += s;
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

template <class... Ts> struct Overloaded : Ts... {
  using Ts::operator()...;

  template <typename T, typename D> size_t operator()(const unique_ptr<T, D>& ptr) {
    return operator()(*ptr.get());
  }
};

template <class... Ts> Overloaded(Ts...) -> Overloaded<Ts...>;

size_t Connection::MessageHandle::UsedMemory() const {
  // TODO: don't count inline size
  auto pub_size = [](const PubMessage& msg) -> size_t {
    return sizeof(PubMessage) + (msg.channel_len + msg.message_len);
  };
  auto msg_size = [](const PipelineMessage& arg) -> size_t {
    return sizeof(PipelineMessage) + arg.args.capacity() * sizeof(MutableSlice) +
           arg.storage.capacity();
  };
  auto monitor_size = [](const MonitorMessage& arg) -> size_t { return arg.capacity(); };
  auto acl_update_size = [](const AclUpdateMessage& msg) -> size_t {
    return sizeof(AclUpdateMessage);
  };
  return sizeof(MessageHandle) +
         visit(Overloaded{pub_size, msg_size, monitor_size, acl_update_size}, this->handle);
}

bool Connection::MessageHandle::IsPipelineMsg() const {
  return get_if<PipelineMessagePtr>(&this->handle) != nullptr;
}

void Connection::DispatchOperations::operator()(const MonitorMessage& msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  rbuilder->SendSimpleString(msg);
}

void Connection::DispatchOperations::operator()(const AclUpdateMessage& msg) {
  auto* ctx = static_cast<dfly::ConnectionContext*>(self->cntx());
  if (ctx && msg.username == ctx->authed_username) {
    ctx->acl_categories = msg.categories;
  }
}

void Connection::DispatchOperations::operator()(const PubMessage& pub_msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  ++stats->async_writes_cnt;
  unsigned i = 0;
  array<string_view, 4> arr;
  if (pub_msg.pattern.empty()) {
    arr[i++] = "message";
  } else {
    arr[i++] = "pmessage";
    arr[i++] = pub_msg.pattern;
  }
  arr[i++] = pub_msg.Channel();
  arr[i++] = pub_msg.Message();
  rbuilder->SendStringArr(absl::Span<string_view>{arr.data(), i},
                          RedisReplyBuilder::CollectionType::PUSH);
}

void Connection::DispatchOperations::operator()(Connection::PipelineMessage& msg) {
  ++stats->pipelined_cmd_cnt;

  DVLOG(2) << "Dispatching pipeline: " << ToSV(msg.args.front());

  self->service_->DispatchCommand(CmdArgList{msg.args.data(), msg.args.size()}, self->cc_.get());
  self->last_interaction_ = time(nullptr);
}

Connection::Connection(Protocol protocol, util::HttpListenerBase* http_listener, SSL_CTX* ctx,
                       ServiceInterface* service)
    : io_buf_(kMinReadSize), http_listener_(http_listener), ctx_(ctx), service_(service), name_{} {
  static atomic_uint32_t next_id{1};

  protocol_ = protocol;

  constexpr size_t kReqSz = sizeof(Connection::PipelineMessage);
  static_assert(kReqSz <= 256 && kReqSz >= 232);

  switch (protocol) {
    case Protocol::REDIS:
      redis_parser_.reset(new RedisParser(absl::GetFlag(FLAGS_max_multi_bulk_len)));
      break;
    case Protocol::MEMCACHE:
      memcache_parser_.reset(new MemcacheParser);
      break;
  }

  creation_time_ = time(nullptr);
  last_interaction_ = creation_time_;
  id_ = next_id.fetch_add(1, memory_order_relaxed);
}

Connection::~Connection() {
}

// Called from Connection::Shutdown() right after socket_->Shutdown call.
void Connection::OnShutdown() {
  VLOG(1) << "Connection::OnShutdown";

  if (shutdown_cb_) {
    for (const auto& k_v : shutdown_cb_->map) {
      k_v.second();
    }
  }
}

void Connection::OnPreMigrateThread() {
  // If we migrating to another io_uring we should cancel any pending requests we have.
  if (break_poll_id_ != UINT32_MAX) {
    socket_->CancelPoll(break_poll_id_);
    break_poll_id_ = UINT32_MAX;
  }
}

void Connection::OnPostMigrateThread() {
  // Once we migrated, we should rearm OnBreakCb callback.
  if (breaker_cb_) {
    DCHECK_EQ(UINT32_MAX, break_poll_id_);

    break_poll_id_ =
        socket_->PollEvent(POLLERR | POLLHUP, [this](int32_t mask) { this->OnBreakCb(mask); });
  }
}

auto Connection::RegisterShutdownHook(ShutdownCb cb) -> ShutdownHandle {
  if (!shutdown_cb_) {
    shutdown_cb_ = make_unique<Shutdown>();
  }
  return shutdown_cb_->Add(std::move(cb));
}

void Connection::UnregisterShutdownHook(ShutdownHandle id) {
  if (shutdown_cb_) {
    shutdown_cb_->Remove(id);
    if (shutdown_cb_->map.empty())
      shutdown_cb_.reset();
  }
}

void Connection::HandleRequests() {
  ThisFiber::SetName("DflyConnection");

  if (absl::GetFlag(FLAGS_tcp_nodelay)) {
    int val = 1;
    CHECK_EQ(0, setsockopt(socket_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)));
  }

  auto remote_ep = socket_->RemoteEndpoint();

  FiberSocketBase* peer = socket_.get();
#ifdef DFLY_USE_SSL
  if (ctx_) {
    const bool no_tls_on_admin_port = absl::GetFlag(FLAGS_no_tls_on_admin_port);
    if (!(IsAdmin() && no_tls_on_admin_port)) {
      unique_ptr<tls::TlsSocket> tls_sock = make_unique<tls::TlsSocket>(std::move(socket_));
      tls_sock->InitSSL(ctx_);
      FiberSocketBase::AcceptResult aresult = tls_sock->Accept();
      SetSocket(tls_sock.release());

      if (!aresult) {
        LOG(WARNING) << "Error handshaking " << aresult.error().message();
        return;
      }
      peer = socket_.get();
      VLOG(1) << "TLS handshake succeeded";
    }
  }
#endif

  io::Result<bool> http_res{false};

  http_res = CheckForHttpProto(peer);

  if (http_res) {
    if (*http_res) {
      VLOG(1) << "HTTP1.1 identified";
      HttpConnection http_conn{http_listener_};
      http_conn.SetSocket(peer);
      auto ec = http_conn.ParseFromBuffer(io_buf_.InputBuffer());
      io_buf_.ConsumeInput(io_buf_.InputLen());
      if (!ec) {
        http_conn.HandleRequests();
      }
      http_conn.ReleaseSocket();
    } else {
      cc_.reset(service_->CreateContext(peer, this));
      if (breaker_cb_) {
        break_poll_id_ =
            socket_->PollEvent(POLLERR | POLLHUP, [this](int32_t mask) { this->OnBreakCb(mask); });
      }

      ConnectionFlow(peer);

      if (break_poll_id_ != UINT32_MAX) {
        socket_->CancelPoll(break_poll_id_);
      }

      cc_.reset();
    }
  }

  VLOG(1) << "Closed connection for peer " << remote_ep;
}

void Connection::RegisterBreakHook(BreakerCb breaker_cb) {
  breaker_cb_ = breaker_cb;
}

std::string Connection::LocalBindAddress() const {
  auto le = socket_->LocalEndpoint();
  return le.address().to_string();
}

string Connection::GetClientInfo(unsigned thread_id) const {
  CHECK(service_ && socket_);
  CHECK_LT(unsigned(phase_), NUM_PHASES);

  string res;
  auto le = socket_->LocalEndpoint();
  auto re = socket_->RemoteEndpoint();
  time_t now = time(nullptr);

  int cpu = 0;
  socklen_t len = sizeof(cpu);
  getsockopt(socket_->native_handle(), SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len);

#ifdef __APPLE__
  int my_cpu_id = -1;  // __APPLE__ does not have sched_getcpu()
#else
  int my_cpu_id = sched_getcpu();
#endif

  static constexpr string_view PHASE_NAMES[] = {"setup", "readsock", "process"};
  static_assert(PHASE_NAMES[PROCESS] == "process");

  absl::StrAppend(&res, "id=", id_, " addr=", re.address().to_string(), ":", re.port());
  absl::StrAppend(&res, " laddr=", le.address().to_string(), ":", le.port());
  absl::StrAppend(&res, " fd=", socket_->native_handle(), " name=", name_);
  absl::StrAppend(&res, " tid=", thread_id, " irqmatch=", int(cpu == my_cpu_id));
  absl::StrAppend(&res, " age=", now - creation_time_, " idle=", now - last_interaction_);
  absl::StrAppend(&res, " phase=", PHASE_NAMES[phase_]);

  if (cc_) {
    string cc_info = service_->GetContextInfo(cc_.get());
    absl::StrAppend(&res, " ", cc_info);
  }

  return res;
}

uint32_t Connection::GetClientId() const {
  return id_;
}

bool Connection::IsAdmin() const {
  return static_cast<Listener*>(owner())->IsAdminInterface();
}

io::Result<bool> Connection::CheckForHttpProto(FiberSocketBase* peer) {
  bool primary_port_enabled = absl::GetFlag(FLAGS_primary_port_http_enabled);
  bool admin = IsAdmin();
  if (!primary_port_enabled && !admin) {
    return false;
  }

  size_t last_len = 0;
  do {
    auto buf = io_buf_.AppendBuffer();
    DCHECK(!buf.empty());

    ::io::Result<size_t> recv_sz = peer->Recv(buf);
    if (!recv_sz) {
      return make_unexpected(recv_sz.error());
    }
    io_buf_.CommitWrite(*recv_sz);
    string_view ib = ToSV(io_buf_.InputBuffer().subspan(last_len));
    size_t pos = ib.find('\n');
    if (pos != string_view::npos) {
      ib = ToSV(io_buf_.InputBuffer().first(last_len + pos));
      if (ib.size() < 10 || ib.back() != '\r')
        return false;

      ib.remove_suffix(1);
      return MatchHttp11Line(ib);
    }
    last_len = io_buf_.InputLen();
    io_buf_.EnsureCapacity(io_buf_.Capacity());
  } while (last_len < 1024);

  return false;
}

void Connection::ConnectionFlow(FiberSocketBase* peer) {
  stats_ = service_->GetThreadLocalConnectionStats();

  ++stats_->num_conns;
  ++stats_->conn_received_cnt;
  stats_->read_buf_capacity += io_buf_.Capacity();

  ParserStatus parse_status = OK;
  SinkReplyBuilder* orig_builder = cc_->reply_builder();

  // At the start we read from the socket to determine the HTTP/Memstore protocol.
  // Therefore we may already have some data in the buffer.
  if (io_buf_.InputLen() > 0) {
    phase_ = PROCESS;
    if (redis_parser_) {
      parse_status = ParseRedis(orig_builder);
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }
  }

  error_code ec = orig_builder->GetError();

  // Main loop.
  if (parse_status != ERROR && !ec) {
    if (io_buf_.AppendLen() < 64) {
      io_buf_.EnsureCapacity(io_buf_.Capacity() * 2);
    }
    auto res = IoLoop(peer, orig_builder);

    if (holds_alternative<error_code>(res)) {
      ec = get<error_code>(res);
    } else {
      parse_status = get<ParserStatus>(res);
    }
  }

  // After the client disconnected.
  cc_->conn_closing = true;  // Signal dispatch to close.
  evc_.notify();
  VLOG(1) << "Before dispatch_fb.join()";
  if (dispatch_fb_.IsJoinable())
    dispatch_fb_.Join();
  VLOG(1) << "After dispatch_fb.join()";
  service_->OnClose(cc_.get());

  stats_->read_buf_capacity -= io_buf_.Capacity();

  // Update num_replicas if this was a replica connection.
  if (cc_->replica_conn) {
    --stats_->num_replicas;
  }

  // We wait for dispatch_fb to finish writing the previous replies before replying to the last
  // offending request.
  if (parse_status == ERROR) {
    VLOG(1) << "Error parser status " << parser_error_;

    if (redis_parser_) {
      SendProtocolError(RedisParser::Result(parser_error_), orig_builder);
    } else {
      DCHECK(memcache_parser_);
      orig_builder->SendProtocolError("bad command line format");
    }

    // Shut down the servers side of the socket to send a FIN to the client
    // then keep draining the socket (discarding any received data) until
    // the client closes the connection.
    //
    // Otherwise the clients write could fail (or block), so they would never
    // read the above protocol error (see issue #1327).
    error_code ec2 = peer->Shutdown(SHUT_WR);
    LOG_IF(WARNING, ec2) << "Could not shutdown socket " << ec2;
    if (!ec2) {
      while (true) {
        // Discard any received data.
        io_buf_.Clear();
        if (!peer->Recv(io_buf_.AppendBuffer())) {
          break;
        }
      }
    }

    FetchBuilderStats(stats_, orig_builder);
  }

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    string conn_info = service_->GetContextInfo(cc_.get());
    LOG(WARNING) << "Socket error for connection " << conn_info << " " << GetName() << ": " << ec
                 << " " << ec.message();
  }

  --stats_->num_conns;
}

void Connection::DispatchCommand(uint32_t consumed, mi_heap_t* heap) {
  bool can_dispatch_sync = (consumed >= io_buf_.InputLen());

  // Avoid sync dispatch if an async dispatch is already in progress, or else they'll interleave.
  if (cc_->async_dispatch)
    can_dispatch_sync = false;

  // Avoid sync dispatch if we already have pending async messages or
  // can potentially receive some (subscriptions > 0). Otherwise the dispatch
  // fiber might be constantly blocked by sync_dispatch.
  if (dispatch_q_.size() > 0 || cc_->subscriptions > 0)
    can_dispatch_sync = false;

  if (can_dispatch_sync) {
    ShrinkPipelinePool();  // Gradually release pipeline request pool.

    RespExpr::VecToArgList(tmp_parse_args_, &tmp_cmd_vec_);

    {
      cc_->sync_dispatch = true;
      service_->DispatchCommand(absl::MakeSpan(tmp_cmd_vec_), cc_.get());
      cc_->sync_dispatch = false;
    }

    last_interaction_ = time(nullptr);

    // We might have blocked the dispatch queue from processing, wake it up.
    if (dispatch_q_.size() > 0)
      evc_.notify();

  } else {
    dispatch_q_cmds_count_++;
    SendAsync(MessageHandle{FromArgs(move(tmp_parse_args_), heap)});
    if (dispatch_q_.size() > 10)
      ThisFiber::Yield();
  }
}

Connection::ParserStatus Connection::ParseRedis(SinkReplyBuilder* orig_builder) {
  uint32_t consumed = 0;

  RedisParser::Result result = RedisParser::OK;
  mi_heap_t* tlh = mi_heap_get_backing();

  do {
    result = redis_parser_->Parse(io_buf_.InputBuffer(), &consumed, &tmp_parse_args_);

    if (result == RedisParser::OK && !tmp_parse_args_.empty()) {
      RespExpr& first = tmp_parse_args_.front();
      if (first.type == RespExpr::STRING) {
        DVLOG(2) << "Got Args with first token " << ToSV(first.GetBuf());
      }

      DispatchCommand(consumed, tlh);
    }
    io_buf_.ConsumeInput(consumed);
  } while (RedisParser::OK == result && !orig_builder->GetError());

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}

auto Connection::ParseMemcache() -> ParserStatus {
  MemcacheParser::Result result = MemcacheParser::OK;
  uint32_t consumed = 0;
  MemcacheParser::Command cmd;
  string_view value;
  MCReplyBuilder* builder = static_cast<MCReplyBuilder*>(cc_->reply_builder());

  do {
    string_view str = ToSV(io_buf_.InputBuffer());
    result = memcache_parser_->Parse(str, &consumed, &cmd);

    if (result != MemcacheParser::OK) {
      io_buf_.ConsumeInput(consumed);
      break;
    }

    size_t total_len = consumed;
    if (MemcacheParser::IsStoreCmd(cmd.type)) {
      total_len += cmd.bytes_len + 2;
      if (io_buf_.InputLen() >= total_len) {
        value = str.substr(consumed, cmd.bytes_len);
        // TODO: dispatch.
      } else {
        return NEED_MORE;
      }
    }

    // An optimization to skip dispatch_q_ if no pipelining is identified.
    // We use ASYNC_DISPATCH as a lock to avoid out-of-order replies when the
    // dispatch fiber pulls the last record but is still processing the command and then this
    // fiber enters the condition below and executes out of order.
    bool is_sync_dispatch = !cc_->async_dispatch;
    if (dispatch_q_.empty() && is_sync_dispatch) {
      service_->DispatchMC(cmd, value, cc_.get());
    }
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

  VLOG(1) << "Got event " << mask;
  CHECK(cc_);
  cc_->conn_closing = true;
  break_poll_id_ = UINT32_MAX;  // do not attempt to cancel it.

  breaker_cb_(mask);
  evc_.notify();  // Notify dispatch fiber.
}

auto Connection::IoLoop(util::FiberSocketBase* peer, SinkReplyBuilder* orig_builder)
    -> variant<error_code, ParserStatus> {
  error_code ec;
  ParserStatus parse_status = OK;

  size_t max_iobfuf_len = absl::GetFlag(FLAGS_max_client_iobuf_len);

  do {
    FetchBuilderStats(stats_, orig_builder);

    io::MutableBytes append_buf = io_buf_.AppendBuffer();
    DCHECK(!append_buf.empty());

    phase_ = READ_SOCKET;

    ::io::Result<size_t> recv_sz = peer->Recv(append_buf);
    last_interaction_ = time(nullptr);

    if (!recv_sz) {
      ec = recv_sz.error();
      parse_status = OK;
      break;
    }

    io_buf_.CommitWrite(*recv_sz);
    stats_->io_read_bytes += *recv_sz;
    ++stats_->io_read_cnt;
    phase_ = PROCESS;
    bool is_iobuf_full = io_buf_.AppendLen() == 0;

    if (redis_parser_) {
      parse_status = ParseRedis(orig_builder);
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
          io_buf_.Reserve(std::min(max_iobfuf_len, parser_hint));
        }

        // If we got a partial request and we couldn't parse the length, just
        // double the capacity.
        // If we got a partial request because iobuf was full, grow it up to
        // a reasonable limit to save on Recv() calls.
        if (io_buf_.AppendLen() < 64u || (is_iobuf_full && capacity < 4096)) {
          // Last io used most of the io_buf to the end.
          io_buf_.Reserve(capacity * 2);  // Valid growth range.
        }

        if (capacity < io_buf_.Capacity()) {
          VLOG(1) << "Growing io_buf to " << io_buf_.Capacity();
          stats_->read_buf_capacity += (io_buf_.Capacity() - capacity);
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
    ec = orig_builder->GetError();
  } while (peer->IsOpen() && !ec);

  FetchBuilderStats(stats_, orig_builder);

  if (ec)
    return ec;

  return parse_status;
}

// DispatchFiber handles commands coming from the InputLoop.
// Thus, InputLoop can quickly read data from the input buffer, parse it and push
// into the dispatch queue and DispatchFiber will run those commands asynchronously with
// InputLoop. Note: in some cases, InputLoop may decide to dispatch directly and bypass the
// DispatchFiber.
void Connection::DispatchFiber(util::FiberSocketBase* peer) {
  ThisFiber::SetName("DispatchFiber");

  SinkReplyBuilder* builder = cc_->reply_builder();
  DispatchOperations dispatch_op{builder, this};
  uint64_t request_cache_limit = absl::GetFlag(FLAGS_request_cache_limit);

  size_t squashing_threshold = absl::GetFlag(FLAGS_pipeline_squash);

  uint64_t prev_epoch = fb2::FiberSwitchEpoch();
  while (!builder->GetError()) {
    evc_.await(
        [this] { return cc_->conn_closing || (!dispatch_q_.empty() && !cc_->sync_dispatch); });
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
      DVLOG(1) << "After yielding to producer, dispatch_q_.size()=" << dispatch_q_.size();
    }
    prev_epoch = cur_epoch;
    builder->SetBatchMode(dispatch_q_.size() > 1);

    auto recycle = [this, request_cache_limit](MessageHandle msg) {
      dispatch_q_bytes_.fetch_sub(msg.UsedMemory(), memory_order_relaxed);

      // Retain pipeline message in pool.
      if (auto* pipe = get_if<PipelineMessagePtr>(&msg.handle); pipe) {
        dispatch_q_cmds_count_--;
        if (stats_->pipeline_cache_capacity < request_cache_limit) {
          stats_->pipeline_cache_capacity += (*pipe)->StorageCapacity();
          pipeline_req_pool_.push_back(move(*pipe));
        }
      }
    };

    // Special case: if the dispatch queue accumulated a big number of commands,
    // we can try to squash them
    // It is only enabled if the threshold is reached and the whole dispatch queue
    // consists only of commands (no pubsub or monitor messages)
    bool squashing_enabled = squashing_threshold > 0;
    bool threshold_reached = dispatch_q_cmds_count_ > squashing_threshold;
    bool are_all_plain_cmds = dispatch_q_cmds_count_ == dispatch_q_.size();
    if (squashing_enabled && threshold_reached && are_all_plain_cmds) {
      vector<CmdArgList> args;
      args.reserve(dispatch_q_.size());
      for (auto& msg : dispatch_q_) {
        CHECK(holds_alternative<PipelineMessagePtr>(msg.handle));
        auto& pipe_msg = get<PipelineMessagePtr>(msg.handle);
        args.push_back(absl::MakeSpan(pipe_msg->args));
      }

      cc_->async_dispatch = true;
      service_->DispatchManyCommands(absl::MakeSpan(args), cc_.get());
      cc_->async_dispatch = false;

      // Dispatch queue could have grown, so handle strictly as many as we executed
      for (size_t i = 0; i < args.size(); i++) {
        recycle(move(dispatch_q_.front()));
        dispatch_q_.pop_front();
      }

      if (dispatch_q_.empty()) {
        builder->FlushBatch();
        builder->SetBatchMode(false);  // in case the next dispatch is sync
      }
    } else {
      MessageHandle msg = move(dispatch_q_.front());
      dispatch_q_.pop_front();

      cc_->async_dispatch = true;
      std::visit(dispatch_op, msg.handle);
      cc_->async_dispatch = false;

      recycle(move(msg));
    }

    evc_bp_.notify();
  }

  cc_->conn_closing = true;

  // make sure that we don't have any leftovers!
  dispatch_q_.clear();
}

Connection::PipelineMessagePtr Connection::FromArgs(RespVec args, mi_heap_t* heap) {
  DCHECK(!args.empty());
  size_t backed_sz = 0;
  for (const auto& arg : args) {
    CHECK_EQ(RespExpr::STRING, arg.type);
    backed_sz += arg.GetBuf().size();
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
    stats_->pipeline_cache_capacity -= pipeline_req_pool_.back()->StorageCapacity();
    pipeline_req_pool_.pop_back();
  }
}

Connection::PipelineMessagePtr Connection::GetFromPipelinePool() {
  if (pipeline_req_pool_.empty())
    return nullptr;

  free_req_release_weight = 0;  // Reset the release weight.
  auto ptr = move(pipeline_req_pool_.back());
  stats_->pipeline_cache_capacity -= ptr->StorageCapacity();
  pipeline_req_pool_.pop_back();
  return ptr;
}

void Connection::ShutdownSelf() {
  util::Connection::Shutdown();
}

void Connection::Migrate(util::fb2::ProactorBase* dest) {
  // Migrate is used only by replication, so it doesn't have properties of full-fledged
  // connections
  CHECK(!cc_->async_dispatch);
  CHECK_EQ(cc_->subscriptions, 0);    // are bound to thread local caches
  CHECK(!dispatch_fb_.IsJoinable());  // can't move once it started

  owner()->Migrate(this, dest);
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
  SendAsync({PubMessagePtr{new (ptr) PubMessage{move(msg)}, MessageDeleter{}}});
}

void Connection::SendMonitorMessageAsync(string msg) {
  SendAsync({MonitorMessage{move(msg)}});
}

void Connection::SendAclUpdateAsync(AclUpdateMessage msg) {
  SendAsync({msg});
}

void Connection::SendAsync(MessageHandle msg) {
  DCHECK(cc_);
  DCHECK(owner());
  DCHECK_EQ(ProactorBase::me(), socket_->proactor());

  if (cc_->conn_closing)
    return;

  if (!dispatch_fb_.IsJoinable()) {
    DCHECK_EQ(dispatch_q_.size(), 0u);
    auto* peer = socket_.get();
    dispatch_fb_ =
        fb2::Fiber(dfly::Launch::post, "connection_dispatch", [&] { DispatchFiber(peer); });
  }

  dispatch_q_bytes_.fetch_add(msg.UsedMemory(), memory_order_relaxed);
  if (std::holds_alternative<AclUpdateMessage>(msg.handle)) {
    dispatch_q_.push_front(std::move(msg));
  } else {
    dispatch_q_.push_back(std::move(msg));
  }

  // Don't notify if a sync dispatch is in progress, it will wake after finishing.
  // This might only happen if we started receving messages while `SUBSCRIBE`
  // is still updating thread local data (see channel_store). We need to make sure its
  // ack is sent before all other messages.
  if (dispatch_q_.size() == 1 && !cc_->sync_dispatch) {
    evc_.notify();
  }
}

void Connection::EnsureAsyncMemoryBudget() {
  evc_bp_.await(
      [this] { return dispatch_q_bytes_.load(memory_order_relaxed) <= kMaxDispatchQMemory; });
}

std::string Connection::RemoteEndpointStr() const {
  const bool unix_socket = socket_->IsUDS();
  std::string connection_str = unix_socket ? "unix:" : std::string{};

  auto re = socket_->RemoteEndpoint();
  absl::StrAppend(&connection_str, re.address().to_string(), ":", re.port());
  return connection_str;
}

std::string Connection::RemoteEndpointAddress() const {
  auto re = socket_->RemoteEndpoint();
  return re.address().to_string();
}

facade::ConnectionContext* Connection::cntx() {
  return cc_.get();
}

}  // namespace facade
