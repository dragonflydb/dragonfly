// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/dragonfly_connection.h"

#include <boost/fiber/operations.hpp>

#include "base/io_buf.h"
#include "base/logging.h"
#include "server/main_service.h"
#include "server/redis_parser.h"
#include "server/conn_context.h"
#include "server/command_registry.h"
#include "util/fiber_sched_algo.h"

using namespace util;
using namespace std;
namespace this_fiber = boost::this_fiber;
namespace fibers = boost::fibers;

namespace dfly {
namespace {

using CmdArgVec = std::vector<MutableStrSpan>;

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

inline MutableStrSpan ToMSS(absl::Span<uint8_t> span) {
  return MutableStrSpan{reinterpret_cast<char*>(span.data()), span.size()};
}

void RespToArgList(const RespVec& src, CmdArgVec* dest) {
  dest->resize(src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    (*dest)[i] = ToMSS(src[i].GetBuf());
  }
}

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
  cc_.reset(new ConnectionContext(peer, this));
  cc_->shard_set = &service_->shard_set();

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
    status = ParseRedis(&io_buf);
     if (status == NEED_MORE) {
      status = OK;
    } else if (status != OK) {
      break;
    }
  } while (peer->IsOpen() && !cc_->ec());

  if (cc_->ec()) {
    ec = cc_->ec();
  } else {
    if (status == ERROR) {
      VLOG(1) << "Error stats " << status;
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
}

auto Connection::ParseRedis(base::IoBuf* io_buf) -> ParserStatus {
  RespVec args;
  CmdArgVec arg_vec;
  uint32_t consumed = 0;

  RedisParser::Result result = RedisParser::OK;

  do {
    result = redis_parser_->Parse(io_buf->InputBuffer(), &consumed, &args);

    if (result == RedisParser::OK && !args.empty()) {
      RespExpr& first = args.front();
      if (first.type == RespExpr::STRING) {
        DVLOG(2) << "Got Args with first token " << ToSV(first.GetBuf());
      }

      RespToArgList(args, &arg_vec);
      service_->DispatchCommand(CmdArgList{arg_vec.data(), arg_vec.size()}, cc_.get());
    }
    io_buf->ConsumeInput(consumed);
  } while (RedisParser::OK == result && !cc_->ec());

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}

}  // namespace dfly
