// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/dragonfly_connection.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/memcache_parser.h"
#include "server/redis_parser.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fiber_sched_algo.h"
#include "util/tls/tls_socket.h"
#include "util/uring/uring_socket.h"

using namespace util;
using namespace std;
using nonstd::make_unexpected;
namespace this_fiber = boost::this_fiber;
namespace fibers = boost::fibers;

namespace dfly {
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

Connection::Connection(Protocol protocol, Service* service, SSL_CTX* ctx)
    : io_buf_{kMinReadSize}, service_(service), ctx_(ctx) {
  protocol_ = protocol;

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

  int val = 1;
  LinuxSocketBase* lsb = static_cast<LinuxSocketBase*>(socket_.get());
  CHECK_EQ(0, setsockopt(lsb->native_handle(), SOL_TCP, TCP_NODELAY, &val, sizeof(val)));

  auto remote_ep = lsb->RemoteEndpoint();

  std::unique_ptr<tls::TlsSocket> tls_sock;
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
      HttpConnection http_conn{service_->http_listener()};
      http_conn.SetSocket(peer);
      auto ec = http_conn.ParseFromBuffer(io_buf_.InputBuffer());
      io_buf_.ConsumeInput(io_buf_.InputLen());
      if (!ec) {
        http_conn.HandleRequests();
      }
      http_conn.ReleaseSocket();
    } else {
      cc_.reset(new ConnectionContext(peer, this));
      cc_->shard_set = &service_->shard_set();

      // TODO: to move this interface to LinuxSocketBase so we won't need to cast.
      uring::UringSocket* us = static_cast<uring::UringSocket*>(socket_.get());

      bool poll_armed = true;
      uint32_t poll_id = us->PollEvent(POLLERR | POLLHUP, [&](uint32_t mask) {
        VLOG(1) << "Got event " << mask;
        cc_->conn_state.mask |= ConnectionState::CONN_CLOSING;
        if (cc_->transaction) {
          cc_->transaction->BreakOnClose();
        }

        evc_.notify();  // Notify dispatch fiber.
        poll_armed = false;
      });

      InputLoop(peer);

      if (poll_armed) {
        us->CancelPoll(poll_id);
      }
    }
  }

  VLOG(1) << "Closed connection for peer " << remote_ep;
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

void Connection::InputLoop(FiberSocketBase* peer) {
  auto dispatch_fb = fibers::fiber(fibers::launch::dispatch, [&] { DispatchFiber(peer); });
  ConnectionStats* stats = ServerState::tl_connection_stats();
  stats->num_conns++;
  stats->read_buf_capacity += io_buf_.Capacity();

  ParserStatus parse_status = OK;
  std::error_code ec;
  ReplyBuilderInterface* builder = cc_->reply_builder();

  if (io_buf_.InputLen() > 0) {
    if (redis_parser_) {
      parse_status = ParseRedis();
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseMemcache();
    }
    if (parse_status == ERROR)
      goto finish;
  }

  do {
    io::MutableBytes append_buf = io_buf_.AppendBuffer();
    ::io::Result<size_t> recv_sz = peer->Recv(append_buf);
    ++stats->io_reads_cnt;

    if (!recv_sz) {
      ec = recv_sz.error();
      parse_status = OK;
      break;
    }
    io_buf_.CommitWrite(*recv_sz);

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
        size_t parser_hint = redis_parser_->parselen_hint();
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
  } while (peer->IsOpen() && !builder->GetError());

finish:
  cc_->conn_state.mask |= ConnectionState::CONN_CLOSING;  // Signal dispatch to close.
  evc_.notify();
  dispatch_fb.join();

  stats->read_buf_capacity -= io_buf_.Capacity();

  // Update num_replicas if this was a replica connection.
  if (cc_->conn_state.mask & ConnectionState::REPL_CONNECTION) {
    --stats->num_replicas;
  }

  if (builder->GetError()) {
    ec = builder->GetError();
  } else {
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
  ReplyBuilderInterface* builder = cc_->reply_builder();

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
      bool is_sync_dispatch = !cc_->conn_state.IsRunViaDispatch();
      if (dispatch_q_.empty() && is_sync_dispatch && consumed >= io_buf_.InputLen()) {
        RespToArgList(args, &arg_vec);
        service_->DispatchCommand(CmdArgList{arg_vec.data(), arg_vec.size()}, cc_.get());
      } else {
        // Dispatch via queue to speedup input reading,
        Request* req = FromArgs(std::move(args));
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
  ReplyBuilderInterface* builder = cc_->reply_builder();

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
    bool is_sync_dispatch = (cc_->conn_state.mask & ConnectionState::ASYNC_DISPATCH) == 0;
    if (dispatch_q_.empty() && is_sync_dispatch && consumed >= io_buf_.InputLen()) {
      service_->DispatchMC(cmd, value, cc_.get());
    }
    io_buf_.ConsumeInput(consumed);
  } while (!builder->GetError());

  parser_error_ = result;

  if (result == MemcacheParser::OK)
    return OK;

  if (result == MemcacheParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}

// DispatchFiber handles commands coming from the InputLoop.
// Thus, InputLoop can quickly read data from the input buffer, parse it and push
// into the dispatch queue and DispatchFiber will run those commands asynchronously with InputLoop.
// Note: in some cases, InputLoop may decide to dispatch directly and bypass the DispatchFiber.
void Connection::DispatchFiber(util::FiberSocketBase* peer) {
  this_fiber::properties<FiberProps>().set_name("DispatchFiber");

  ConnectionStats* stats = ServerState::tl_connection_stats();
  SinkReplyBuilder* builder = static_cast<SinkReplyBuilder*>(cc_->reply_builder());

  while (!builder->GetError()) {
    evc_.await([this] { return cc_->conn_state.IsClosing() || !dispatch_q_.empty(); });
    if (cc_->conn_state.IsClosing())
      break;

    std::unique_ptr<Request> req{dispatch_q_.front()};
    dispatch_q_.pop_front();

    ++stats->pipelined_cmd_cnt;

    builder->SetBatchMode(!dispatch_q_.empty());
    cc_->conn_state.mask |= ConnectionState::ASYNC_DISPATCH;
    service_->DispatchCommand(CmdArgList{req->args.data(), req->args.size()}, cc_.get());
    cc_->conn_state.mask &= ~ConnectionState::ASYNC_DISPATCH;
  }

  cc_->conn_state.mask |= ConnectionState::CONN_CLOSING;
}

auto Connection::FromArgs(RespVec args) -> Request* {
  DCHECK(!args.empty());
  size_t backed_sz = 0;
  for (const auto& arg : args) {
    CHECK_EQ(RespExpr::STRING, arg.type);
    backed_sz += arg.GetBuf().size();
  }
  DCHECK(backed_sz);

  Request* req = new Request{args.size(), backed_sz};

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

}  // namespace dfly
