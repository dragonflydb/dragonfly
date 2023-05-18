// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <queue>

#include "base/histogram.h"
#include "base/init.h"
#include "base/io_buf.h"
#include "facade/redis_parser.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"
#include "util/uring/uring_socket.h"

using namespace std;

// A load-test for DragonflyDB that fixes coordinated omission problem.

ABSL_FLAG(uint16_t, p, 6379, "Server port");
ABSL_FLAG(uint32_t, c, 20, "Number of connections per thread");
ABSL_FLAG(uint32_t, qps, 20, "QPS schedule at which the generator sends requests to the server");
ABSL_FLAG(uint32_t, n, 1000, "Number of requests to send per connection");
ABSL_FLAG(string, h, "localhost", "server hostname/ip");

using absl::GetFlag;

using namespace util;
using tcp = ::boost::asio::ip::tcp;
using facade::RedisParser;
using facade::RespVec;

// Per connection driver.
class Driver {
 public:
  explicit Driver(ProactorBase* p) {
    socket_.reset(p->CreateSocket());
  }

  Driver(const Driver&) = delete;

  void Connect(unsigned index, const tcp::endpoint& ep);
  void Run(base::Histogram* dest);

 private:
  void ReceiveFb();

  struct Req {
    uint64_t start;
  };

  unique_ptr<FiberSocketBase> socket_;
  queue<Req> reqs_;
};

// Per thread client.
class TLocalClient {
 public:
  explicit TLocalClient(ProactorBase* p) : p_(p) {
    drivers_.reserve(absl::GetFlag(FLAGS_c));
    for (size_t i = 0; i < drivers_.size(); ++i) {
      drivers_.push_back(make_unique<Driver>(p_));
    }
  }

  TLocalClient(const TLocalClient&) = delete;

  void Connect(tcp::endpoint ep);
  void Run();

 private:
  ProactorBase* p_;
  vector<unique_ptr<Driver>> drivers_;
};

void Driver::Connect(unsigned index, const tcp::endpoint& ep) {
  error_code ec = socket_->Connect(ep);
  CHECK(!ec) << "Could not connect to " << ep << " " << ec;
}

void Driver::Run(base::Histogram* dest) {
  string cmd;
  bool conn_close = false;
  uint32_t qps = absl::GetFlag(FLAGS_qps);
  double interval = 1000000000.0 / qps;

  // TODO: to measure latencies and add them into dest.
  auto receive_fb = MakeFiber([this] { ReceiveFb(); });

  uint32_t now = absl::GetCurrentTimeNanos();
  uint32_t n = absl::GetFlag(FLAGS_n);
  VLOG(1) << "Sending " << n << " requests";

  for (unsigned i = 0; i < n; ++i) {
    cmd.clear();
    absl::StrAppend(&cmd, "set ", "key", i, " val\r\n");
    Req req;
    req.start = absl::GetCurrentTimeNanos();
    reqs_.push(req);

    // TODO: to schedule constant QPS.
    error_code ec = socket_->Write(io::Buffer(cmd));
    if (ec && FiberSocketBase::IsConnClosed(ec)) {
      conn_close = true;
      VLOG(1) << "Connection closed";
      break;
    }
    CHECK(!ec) << ec.message();
  }

  // TODO: to change to a condvar or something.
  while (!reqs_.empty()) {
    ThisFiber::SleepFor(1ms);
  }

  socket_->Shutdown(SHUT_RDWR);  // breaks the receive fiber.
  receive_fb.Join();
}

void Driver::ReceiveFb() {
  facade::RedisParser parser{false};
  base::IoBuf io_buf{512};
  unsigned num_resp = 0;
  while (true) {
    auto buf = io_buf.AppendBuffer();
    VLOG(2) << "Socket read: " << reqs_.size() << " " << num_resp;

    ::io::Result<size_t> recv_sz = socket_->Recv(buf);
    if (!recv_sz && FiberSocketBase::IsConnClosed(recv_sz.error())) {
      break;
    }
    CHECK(recv_sz) << recv_sz.error().message();
    io_buf.CommitWrite(*recv_sz);

    uint32_t consumed = 0;
    RedisParser::Result result = RedisParser::OK;
    RespVec parse_args;

    do {
      result = parser.Parse(io_buf.InputBuffer(), &consumed, &parse_args);
      if (result == RedisParser::OK && !parse_args.empty()) {
        reqs_.pop();
        parse_args.clear();
        ++num_resp;
      }
      io_buf.ConsumeInput(consumed);
    } while (result == RedisParser::OK);
  }
  VLOG(1) << "ReceiveFb done";
}

void TLocalClient::Connect(tcp::endpoint ep) {
  vector<fb2::Fiber> fbs(drivers_.size());

  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] = MakeFiber([&, i] {
      ThisFiber::SetName(absl::StrCat("connect/", i));
      drivers_[i]->Connect(i, ep);
    });
  }

  for (auto& fb : fbs)
    fb.Join();
}

void TLocalClient::Run() {
  vector<fb2::Fiber> fbs(drivers_.size());
  base::Histogram hist;

  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] = MakeFiber([&, i] {
      ThisFiber::SetName(absl::StrCat("run/", i));
      drivers_[i]->Run(&hist);
    });
  }

  for (auto& fb : fbs)
    fb.Join();
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  unique_ptr<ProactorPool> pp;
  pp.reset(fb2::Pool::IOUring(256));
  pp->Run();

  auto* proactor = pp->GetNextProactor();
  char ip_addr[128];

  error_code ec =
      proactor->Await([&] { return fb2::DnsResolve(GetFlag(FLAGS_h), 2000, ip_addr, proactor); });
  CHECK(!ec) << "Could not resolve " << GetFlag(FLAGS_h) << " " << ec;

  auto address = ::boost::asio::ip::make_address(ip_addr);
  tcp::endpoint ep{address, GetFlag(FLAGS_p)};

  thread_local unique_ptr<TLocalClient> client;

  pp->AwaitFiberOnAll([&](auto* p) {
    client = make_unique<TLocalClient>(p);
    client->Connect(ep);
  });

  pp->AwaitFiberOnAll([&](auto* p) { client->Run(); });

  pp->AwaitFiberOnAll([&](auto* p) { client.reset(); });

  pp->Stop();

  return 0;
}
