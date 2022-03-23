// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_connection.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>
#include <mimalloc.h>

#include <boost/fiber/operations.hpp>

#include "base/logging.h"

#include "facade/conn_context.h"
#include "facade/memcache_parser.h"
#include "facade/redis_parser.h"
#include "facade/service_interface.h"
#include "util/fiber_sched_algo.h"
#include "util/tls/tls_socket.h"
#include "util/uring/uring_socket.h"

DEFINE_bool(tcp_nodelay, false, "Configures dragonfly connections with socket option TCP_NODELAY");

using namespace util;
using namespace std;
using nonstd::make_unexpected;
namespace this_fiber = boost::this_fiber;
namespace fibers = boost::fibers;

namespace facade {
namespace {

void SendProtocolError(RedisParser::Result pres, FiberSocketBase* peer) {
  string res("-ERR Protocol error: ");
  if (pres == RedisParser::BAD_BULKLEN) {
    res.append("invalid bulk length\r\n");
  } else {
    CHECK_EQ(RedisParser::BAD_ARRAYLEN, pres);
    res.append("invalid multibulk length\r\n");
  }

  auto size_res = peer->Send(::io::Buffer(res));
  if (!size_res) {
    LOG(WARNING) << "Error " << size_res.error();
  }
}

void RespToArgList(const RespVec& src, CmdArgVec* dest) {
  dest->resize(src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    (*dest)[i] = ToMSS(src[i].GetBuf());
  }
}

void FetchBuilderStats(ConnectionStats* stats, SinkReplyBuilder* builder) {
  stats->io_write_cnt += builder->io_write_cnt();
  stats->io_write_bytes += builder->io_write_bytes();

  for (const auto& k_v : builder->err_count()) {
    stats->err_count[k_v.first] += k_v.second;
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
constexpr size_t kMaxReadSize = 32_KB;

}  // namespace

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

struct Connection::Request {
  absl::FixedArray<MutableSlice> args;

  // I do not use mi_heap_t explicitly but mi_stl_allocator at the end does the same job
  // of using the thread's heap.
  // The capacity is chosen so that we allocate a fully utilized (512 bytes) block.
  absl::FixedArray<char, 190, mi_stl_allocator<char>> storage;

  Request(size_t nargs, size_t capacity) : args(nargs), storage(capacity) {
  }

  Request(const Request&) = delete;
};

Connection::Connection(Protocol protocol, util::HttpListenerBase* http_listener, SSL_CTX* ctx,
                       ServiceInterface* service)
    : io_buf_(kMinReadSize), http_listener_(http_listener), ctx_(ctx), service_(service) {
  protocol_ = protocol;

  constexpr size_t kReqSz = sizeof(Connection::Request);
  (void)kReqSz;

  switch (protocol) {
    case Protocol::REDIS:
      redis_parser_.reset(new RedisParser);
      break;
    case Protocol::MEMCACHE:
      memcache_parser_.reset(new MemcacheParser);
      break;
  }
}

Connection::~Connection() {
}

void Connection::OnShutdown() {
  VLOG(1) << "Connection::OnShutdown";

  if (shutdown_) {
    for (const auto& k_v : shutdown_->map) {
      k_v.second();
    }
  }
}

auto Connection::RegisterShutdownHook(ShutdownCb cb) -> ShutdownHandle {
  if (!shutdown_) {
    shutdown_ = make_unique<Shutdown>();
  }
  return shutdown_->Add(std::move(cb));
}

void Connection::UnregisterShutdownHook(ShutdownHandle id) {
  if (shutdown_) {
    shutdown_->Remove(id);
    if (shutdown_->map.empty())
      shutdown_.reset();
  }
}

void Connection::HandleRequests() {
  this_fiber::properties<FiberProps>().set_name("DflyConnection");

  LinuxSocketBase* lsb = static_cast<LinuxSocketBase*>(socket_.get());

  if (FLAGS_tcp_nodelay) {
    int val = 1;
    CHECK_EQ(0, setsockopt(lsb->native_handle(), SOL_TCP, TCP_NODELAY, &val, sizeof(val)));
  }

  auto remote_ep = lsb->RemoteEndpoint();

  unique_ptr<tls::TlsSocket> tls_sock;
  if (ctx_) {
    tls_sock.reset(new tls::TlsSocket(socket_.get()));
    tls_sock->InitSSL(ctx_);

    FiberSocketBase::AcceptResult aresult = tls_sock->Accept();
    if (!aresult) {
      LOG(WARNING) << "Error handshaking " << aresult.error().message();
      return;
    }
    VLOG(1) << "TLS handshake succeeded";
  }

  FiberSocketBase* peer = tls_sock ? (FiberSocketBase*)tls_sock.get() : socket_.get();
  io::Result<bool> http_res = CheckForHttpProto(peer);

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

      bool should_disarm_poller = false;
      // TODO: to move this interface to LinuxSocketBase so we won't need to cast.
      uring::UringSocket* us = static_cast<uring::UringSocket*>(socket_.get());
      uint32_t poll_id = 0;
      if (breaker_cb_) {
        should_disarm_poller = true;

        poll_id = us->PollEvent(POLLERR | POLLHUP, [&](uint32_t mask) {
          VLOG(1) << "Got event " << mask;
          cc_->conn_closing = true;
          breaker_cb_(mask);
          evc_.notify();  // Notify dispatch fiber.
          should_disarm_poller = false;
        });
      }

      ConnectionFlow(peer);

      if (should_disarm_poller) {
        us->CancelPoll(poll_id);
      }
      cc_.reset();
    }
  }

  VLOG(1) << "Closed connection for peer " << remote_ep;
}

void Connection::RegisterOnBreak(BreakerCb breaker_cb) {
  breaker_cb_ = breaker_cb;
}

io::Result<bool> Connection::CheckForHttpProto(util::FiberSocketBase* peer) {
  size_t last_len = 0;
  do {
    auto buf = io_buf_.AppendBuffer();
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
  } while (last_len < 1024);

  return false;
}

void Connection::ConnectionFlow(FiberSocketBase* peer) {
  auto dispatch_fb = fibers::fiber(fibers::launch::dispatch, [&] { DispatchFiber(peer); });
  ConnectionStats* stats = service_->GetThreadLocalConnectionStats();
  stats->num_conns++;
  stats->read_buf_capacity += io_buf_.Capacity();

  ParserStatus parse_status = OK;

  // At the start we read from the socket to determine the HTTP/Memstore protocol.
  // Therefore we may already have some data in the buffer.
  if (io_buf_.InputLen() > 0) {
    if (redis_parser_) {
      parse_status = ParseRedis();
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }
  }

  error_code ec;

  // Main loop.
  if (parse_status != ERROR) {
    auto res = IoLoop(peer);

    if (holds_alternative<error_code>(res)) {
      ec = get<error_code>(res);
    } else {
      parse_status = get<ParserStatus>(res);
    }
  }

  // After the client disconnected.
  cc_->conn_closing = true;  // Signal dispatch to close.
  evc_.notify();
  dispatch_fb.join();

  stats->read_buf_capacity -= io_buf_.Capacity();

  // Update num_replicas if this was a replica connection.
  if (cc_->replica_conn) {
    --stats->num_replicas;
  }

  // We wait for dispatch_fb to finish writing the previous replies before replying to the last
  // offending request.
  if (parse_status == ERROR) {
    VLOG(1) << "Error stats " << parse_status;
    if (redis_parser_) {
      SendProtocolError(RedisParser::Result(parser_error_), peer);
    } else {
      string_view sv{"CLIENT_ERROR bad command line format\r\n"};
      auto size_res = peer->Send(::io::Buffer(sv));
      if (!size_res) {
        LOG(WARNING) << "Error " << size_res.error();
        ec = size_res.error();
      }
    }
  }

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    LOG(WARNING) << "Socket error " << ec;
  }

  --stats->num_conns;
}

auto Connection::ParseRedis() -> ParserStatus {
  RespVec args;
  CmdArgVec arg_vec;
  uint32_t consumed = 0;

  RedisParser::Result result = RedisParser::OK;
  SinkReplyBuilder* builder = cc_->reply_builder();
  mi_heap_t* tlh = mi_heap_get_backing();

  do {
    result = redis_parser_->Parse(io_buf_.InputBuffer(), &consumed, &args);

    if (result == RedisParser::OK && !args.empty()) {
      RespExpr& first = args.front();
      if (first.type == RespExpr::STRING) {
        DVLOG(2) << "Got Args with first token " << ToSV(first.GetBuf());
      }

      // An optimization to skip dispatch_q_ if no pipelining is identified.
      // We use ASYNC_DISPATCH as a lock to avoid out-of-order replies when the
      // dispatch fiber pulls the last record but is still processing the command and then this
      // fiber enters the condition below and executes out of order.
      bool is_sync_dispatch = !cc_->async_dispatch;
      if (dispatch_q_.empty() && is_sync_dispatch && consumed >= io_buf_.InputLen()) {
        RespToArgList(args, &arg_vec);
        service_->DispatchCommand(CmdArgList{arg_vec.data(), arg_vec.size()}, cc_.get());
      } else {
        // Dispatch via queue to speedup input reading
        // We could use
        Request* req = FromArgs(std::move(args), tlh);

        dispatch_q_.emplace_back(req);
        if (dispatch_q_.size() == 1) {
          evc_.notify();
        } else if (dispatch_q_.size() > 10) {
          this_fiber::yield();
        }
      }
    }
    io_buf_.ConsumeInput(consumed);
  } while (RedisParser::OK == result && !builder->GetError());

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

  if (result == MemcacheParser::PARSE_ERROR) {
    builder->SendError("");  // ERROR.
  } else if (result == MemcacheParser::BAD_DELTA) {
    builder->SendClientError("invalid numeric delta argument");
  } else if (result != MemcacheParser::OK) {
    builder->SendClientError("bad command line format");
  }

  return OK;
}

auto Connection::IoLoop(util::FiberSocketBase* peer) -> variant<error_code, ParserStatus> {
  SinkReplyBuilder* builder = cc_->reply_builder();
  ConnectionStats* stats = service_->GetThreadLocalConnectionStats();
  error_code ec;
  ParserStatus parse_status = OK;

  do {
    FetchBuilderStats(stats, builder);

    io::MutableBytes append_buf = io_buf_.AppendBuffer();
    ::io::Result<size_t> recv_sz = peer->Recv(append_buf);

    if (!recv_sz) {
      ec = recv_sz.error();
      parse_status = OK;
      break;
    }

    io_buf_.CommitWrite(*recv_sz);
    stats->io_read_bytes += *recv_sz;
    ++stats->io_read_cnt;

    if (redis_parser_)
      parse_status = ParseRedis();
    else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }

    if (parse_status == NEED_MORE) {
      parse_status = OK;

      size_t capacity = io_buf_.Capacity();
      if (capacity < kMaxReadSize) {
        size_t parser_hint = 0;
        if (redis_parser_)
          parser_hint = redis_parser_->parselen_hint();  // Could be done for MC as well.

        if (parser_hint > capacity) {
          io_buf_.Reserve(std::min(kMaxReadSize, parser_hint));
        } else if (append_buf.size() == *recv_sz && append_buf.size() > capacity / 2) {
          // Last io used most of the io_buf to the end.
          io_buf_.Reserve(capacity * 2);  // Valid growth range.
        }

        if (capacity < io_buf_.Capacity()) {
          VLOG(1) << "Growing io_buf to " << io_buf_.Capacity();
          stats->read_buf_capacity += (io_buf_.Capacity() - capacity);
        }
      }
    } else if (parse_status != OK) {
      break;
    }
    ec = builder->GetError();
  } while (peer->IsOpen() && !ec);

  FetchBuilderStats(stats, builder);

  if (ec)
    return ec;

  return parse_status;
}

// DispatchFiber handles commands coming from the InputLoop.
// Thus, InputLoop can quickly read data from the input buffer, parse it and push
// into the dispatch queue and DispatchFiber will run those commands asynchronously with InputLoop.
// Note: in some cases, InputLoop may decide to dispatch directly and bypass the DispatchFiber.
void Connection::DispatchFiber(util::FiberSocketBase* peer) {
  this_fiber::properties<FiberProps>().set_name("DispatchFiber");

  ConnectionStats* stats = service_->GetThreadLocalConnectionStats();
  SinkReplyBuilder* builder = cc_->reply_builder();

  while (!builder->GetError()) {
    evc_.await([this] { return cc_->conn_closing || !dispatch_q_.empty(); });
    if (cc_->conn_closing)
      break;

    Request* req = dispatch_q_.front();
    dispatch_q_.pop_front();

    ++stats->pipelined_cmd_cnt;

    builder->SetBatchMode(!dispatch_q_.empty());
    cc_->async_dispatch = true;
    service_->DispatchCommand(CmdArgList{req->args.data(), req->args.size()}, cc_.get());
    cc_->async_dispatch = false;
    req->~Request();
    mi_free(req);
  }

  cc_->conn_closing = true;

  // Clean up leftovers.
  while (!dispatch_q_.empty()) {
    Request* req = dispatch_q_.front();
    dispatch_q_.pop_front();
    req->~Request();
    mi_free(req);
  }
}

auto Connection::FromArgs(RespVec args, mi_heap_t* heap) -> Request* {
  DCHECK(!args.empty());
  size_t backed_sz = 0;
  for (const auto& arg : args) {
    CHECK_EQ(RespExpr::STRING, arg.type);
    backed_sz += arg.GetBuf().size();
  }
  DCHECK(backed_sz);

  constexpr auto kReqSz = sizeof(Request);
  static_assert(kReqSz < MI_SMALL_SIZE_MAX);
  static_assert(alignof(Request) == 8);
  void* ptr = mi_heap_malloc_small(heap, kReqSz);

  Request* req = new (ptr) Request{args.size(), backed_sz};

  auto* next = req->storage.data();
  for (size_t i = 0; i < args.size(); ++i) {
    auto buf = args[i].GetBuf();
    size_t s = buf.size();
    memcpy(next, buf.data(), s);
    req->args[i] = MutableSlice(next, s);
    next += s;
  }

  return req;
}

}  // namespace facade
