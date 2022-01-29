// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/replica.h"

extern "C" {
#include "redis/rdb.h"
}

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/asio/ip/tcp.hpp>

#include "base/logging.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "server/redis_parser.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace boost::asio;

namespace {

// TODO: 2. Use time-out on socket-reads so that we would not deadlock on unresponsive master.
//       3. Support ipv6 at some point.
int ResolveDns(std::string_view host, char* dest) {
  struct addrinfo hints, *servinfo;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_ALL;

  int res = getaddrinfo(host.data(), NULL, &hints, &servinfo);
  if (res != 0)
    return res;

  static_assert(INET_ADDRSTRLEN < INET6_ADDRSTRLEN, "");

  res = EAI_FAMILY;
  for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
    if (p->ai_family == AF_INET) {
      struct sockaddr_in* ipv4 = (struct sockaddr_in*)p->ai_addr;
      const char* inet_res = inet_ntop(p->ai_family, &ipv4->sin_addr, dest, INET6_ADDRSTRLEN);
      CHECK_NOTNULL(inet_res);
      res = 0;
      break;
    }
    LOG(WARNING) << "Only IPv4 is supported";
  }

  freeaddrinfo(servinfo);

  return res;
}

}  // namespace

Replica::Replica(string host, uint16_t port, Service* se)
    : service_(*se), host_(std::move(host)), port_(port) {
}

Replica::~Replica() {
  if (sync_fb_.joinable())
    sync_fb_.join();

  if (sock_) {
    auto ec = sock_->Close();
    LOG_IF(ERROR, ec) << "Error closing replica socket " << ec;
  }
}

static const char kConnErr[] = "could not connect to master: ";

bool Replica::Run(ConnectionContext* cntx) {
  CHECK(!sock_ && !sock_thread_);

  sock_thread_ = ProactorBase::me();
  CHECK(sock_thread_);

  error_code ec = ConnectSocket();
  if (ec) {
    cntx->SendError(absl::StrCat(kConnErr, ec.message()));
    return false;
  }

  state_mask_ = R_ENABLED | R_TCP_CONNECTED;
  last_io_time_ = sock_thread_->GetMonotonicTimeNs();
  cntx->SendOk();

  return true;
}

std::error_code Replica::ConnectSocket() {
  sock_.reset(sock_thread_->CreateSocket());

  char ip_addr[INET6_ADDRSTRLEN];
  int resolve_res = ResolveDns(host_, ip_addr);
  if (resolve_res != 0) {
    LOG(ERROR) << "Dns error " << gai_strerror(resolve_res);
    return make_error_code(errc::host_unreachable);
  }
  auto address = ip::make_address(ip_addr);
  ip::tcp::endpoint ep{address, port_};

  /* These may help but require additional field testing to learn.

 int yes = 1;

 CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));
 CHECK_EQ(0, setsockopt(sock_->native_handle(), SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)));

 int intv = 15;
 CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPIDLE, &intv, sizeof(intv)));

 intv /= 3;
 CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPINTVL, &intv, sizeof(intv)));

 intv = 3;
 CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPCNT, &intv, sizeof(intv)));
*/

  return sock_->Connect(ep);
}

void Replica::Stop() {
  if (sock_thread_) {
    sock_thread_->Await([this] {
      state_mask_ = 0;  // Specifically ~R_ENABLED.
      auto ec = sock_->Shutdown(SHUT_RDWR);
      LOG_IF(ERROR, ec) << "Could not shutdown socket " << ec;
    });
  }
  if (sync_fb_.joinable())
    sync_fb_.join();
}

// Threadsafe, fiber blocking.
auto Replica::GetInfo() const -> Info {
  CHECK(sock_thread_);
  return sock_thread_->AwaitBrief([this] {
    Info res;
    res.host = host_;
    res.port = port_;
    res.master_link_established = (state_mask_ & R_TCP_CONNECTED);
    res.sync_in_progress = (state_mask_ & R_SYNCING);
    res.master_last_io_sec = (ProactorBase::GetMonotonicTimeNs() - last_io_time_) / 1000000000UL;
    return res;
  });
}

}  // namespace dfly
