// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/dragonfly_connection.h"

#include <boost/fiber/operations.hpp>

#include "base/io_buf.h"
#include "base/logging.h"
#include "server/main_service.h"
#include "server/redis_parser.h"
#include "util/fiber_sched_algo.h"

using namespace util;
using namespace std;
namespace this_fiber = boost::this_fiber;
namespace fibers = boost::fibers;

namespace dfly {
namespace {



constexpr size_t kMinReadSize = 256;

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

Connection::Connection(Service* service)
    : service_(service) {
  redis_parser_.reset(new RedisParser);
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
  CHECK_EQ(0, setsockopt(socket_->native_handle(), SOL_TCP, TCP_NODELAY, &val, sizeof(val)));

  FiberSocketBase* peer = socket_.get();
  InputLoop(peer);

  VLOG(1) << "Closed connection for peer " << socket_->RemoteEndpoint();
}

void Connection::InputLoop(FiberSocketBase* peer) {
  base::IoBuf io_buf{kMinReadSize};
  ParserStatus status = OK;
  std::error_code ec;

  do {
    auto buf = io_buf.AppendBuffer();
    ::io::Result<size_t> recv_sz = peer->Recv(buf);

    if (!recv_sz) {
      ec = recv_sz.error();
      status = OK;
      break;
    }

    io_buf.CommitWrite(*recv_sz);
    status = ParseRedis(&io_buf, peer);
     if (status == NEED_MORE) {
      status = OK;
    } else if (status != OK) {
      break;
    }
  } while (peer->IsOpen());

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    LOG(WARNING) << "Socket error " << ec;
  }
}

auto Connection::ParseRedis(base::IoBuf* io_buf, util::FiberSocketBase* peer) -> ParserStatus {
  RespVec args;
  uint32_t consumed = 0;

  RedisParser::Result result = RedisParser::OK;
  error_code ec;
  do {
    result = redis_parser_->Parse(io_buf->InputBuffer(), &consumed, &args);

    if (result == RedisParser::OK && !args.empty()) {
      RespExpr& first = args.front();
      if (first.type == RespExpr::STRING) {
        DVLOG(2) << "Got Args with first token " << ToSV(first.GetBuf());
      }

      CHECK_EQ(RespExpr::STRING, first.type);  // TODO
      string_view sv = ToSV(first.GetBuf());
      if (sv == "PING") {
        ec = peer->Write(io::Buffer("PONG\r\n"));
      } else if (sv == "SET") {
        CHECK_EQ(3u, args.size());
        service_->Set(ToSV(args[1].GetBuf()), ToSV(args[2].GetBuf()));
        ec = peer->Write(io::Buffer("OK\r\n"));
      }
    }
    io_buf->ConsumeInput(consumed);
  } while (RedisParser::OK == result && !ec);

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}
}  // namespace dfly
