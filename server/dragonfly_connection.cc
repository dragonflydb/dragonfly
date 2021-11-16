// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/dragonfly_connection.h"

#include <boost/fiber/operations.hpp>

#include "base/io_buf.h"
#include "base/logging.h"
#include "server/main_service.h"
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

  std::error_code ec;

  do {
    auto buf = io_buf.AppendBuffer();
    ::io::Result<size_t> recv_sz = peer->Recv(buf);

    if (!recv_sz) {
      ec = recv_sz.error();
      break;
    }

    io_buf.CommitWrite(*recv_sz);
    ec = peer->Write(io_buf.InputBuffer());
    if (ec)
      break;
  } while (peer->IsOpen());

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    LOG(WARNING) << "Socket error " << ec;
  }
}

}  // namespace dfly
