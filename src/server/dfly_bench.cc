// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>

#include <queue>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "base/histogram.h"
#include "base/init.h"
#include "base/io_buf.h"
#include "core/fibers.h"
#include "facade/redis_parser.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"
#include "util/uring/uring_socket.h"

// A load-test for DragonflyDB that fixes coordinated omission problem.

ABSL_FLAG(uint16_t, p, 6379, "Server port");
ABSL_FLAG(uint32_t, c, 20, "Number of connections per thread");
ABSL_FLAG(uint32_t, qps, 20, "QPS schedule at which the generator sends requests to the server");
ABSL_FLAG(uint32_t, n, 1000, "Number of requests to send per connection");
ABSL_FLAG(std::string, h, "localhost", "server hostname/ip");
ABSL_FLAG(uint32_t, key_minimum, 0, "Min value for keys used");
ABSL_FLAG(uint32_t, key_maximum, 1'000, "Max value for keys used");
ABSL_FLAG(std::string, ratio, "1:10", "Set:Get ratio");

using namespace std;
using namespace util;
using absl::GetFlag;
using facade::RedisParser;
using facade::RespVec;
using tcp = ::boost::asio::ip::tcp;

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
  void ReceiveFb(base::Histogram* dest);

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
    drivers_.resize(GetFlag(FLAGS_c));
    for (auto& driver : drivers_) {
      driver = make_unique<Driver>(p_);
    }
  }

  TLocalClient(const TLocalClient&) = delete;

  void Connect(tcp::endpoint ep);
  void Run();

  base::Histogram hist;

 private:
  ProactorBase* p_;
  vector<unique_ptr<Driver>> drivers_;
};

void Driver::Connect(unsigned index, const tcp::endpoint& ep) {
  VLOG(2) << "Connecting " << index;
  error_code ec = socket_->Connect(ep);
  CHECK(!ec) << "Could not connect to " << ep << " " << ec;
}

void Driver::Run(base::Histogram* dest) {
  // TODO: move flag parsing to a central place and copy it to each thread
  string cmd;
  const uint32_t qps = GetFlag(FLAGS_qps);
  const int64_t interval = 1000000000LL / qps;

  auto receive_fb = MakeFiber([this, dest] { ReceiveFb(dest); });

  int64_t next_invocation = absl::GetCurrentTimeNanos();

  uint32_t n = GetFlag(FLAGS_n);
  const absl::Time start = absl::Now();
  LOG(INFO) << "Sending " << n << " requests at a rate of " << GetFlag(FLAGS_qps)
            << "qps, i.e. request every " << interval << "ns";

  thread_local absl::InsecureBitGen bit_gen;
  const uint32_t key_minimum = GetFlag(FLAGS_key_minimum);
  const uint32_t key_maximum = GetFlag(FLAGS_key_maximum);

  pair<string, string> ratio_str = absl::StrSplit(GetFlag(FLAGS_ratio), ':');
  uint32_t ratio_set, ratio_get;
  CHECK(absl::SimpleAtoi(ratio_str.first, &ratio_set));
  CHECK(absl::SimpleAtoi(ratio_str.second, &ratio_get));

  for (unsigned i = 0; i < n; ++i) {
    int64_t now = absl::GetCurrentTimeNanos();

    int64_t sleep_ns = next_invocation - now;
    if (sleep_ns > 0) {
      VLOG(5) << "Sleeping for " << sleep_ns << "ns";
      ThisFiber::SleepFor(chrono::nanoseconds(sleep_ns));
    } else {
      VLOG(5) << "Behind QPS schedule";
    }
    next_invocation += interval;

    cmd.clear();

    uint32_t key_suffix = absl::Uniform(bit_gen, key_minimum, key_maximum);

    if (absl::Uniform(bit_gen, 0U, ratio_get + ratio_set) < ratio_set) {
      // TODO: value size
      absl::StrAppend(&cmd, "set ", "key", key_suffix, " val\r\n");
    } else {
      absl::StrAppend(&cmd, "get ", "key", key_suffix, "\r\n");
    }

    Req req;
    req.start = absl::GetCurrentTimeNanos();
    reqs_.push(req);
    // TODO: add type (get/set)

    error_code ec = socket_->Write(io::Buffer(cmd));
    if (ec && FiberSocketBase::IsConnClosed(ec)) {
      // TODO: report failure
      VLOG(1) << "Connection closed";
      break;
    }
    CHECK(!ec) << ec.message();
  }

  const absl::Time finish = absl::Now();
  LOG(INFO) << "Done queuing " << n << " requests, which took " << finish - start
            << ". Waiting for server processing";

  // TODO: to change to a condvar or something.
  while (!reqs_.empty()) {
    ThisFiber::SleepFor(1ms);
  }

  socket_->Shutdown(SHUT_RDWR);  // breaks the receive fiber.
  receive_fb.Join();
}

void Driver::ReceiveFb(base::Histogram* dest) {
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
        dest->Add(absl::GetCurrentTimeNanos() - reqs_.front().start);
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
  VLOG(2) << "Connecting client...";
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

  LOG(INFO) << "Connecting threads";
  pp->AwaitFiberOnAll([&](auto* p) {
    client = make_unique<TLocalClient>(p);
    client->Connect(ep);
  });

  LOG(INFO) << "Running all threads";
  const absl::Time start_time = absl::Now();
  pp->AwaitFiberOnAll([&](auto* p) { client->Run(); });
  absl::Duration duration = absl::Now() - start_time;
  LOG(INFO) << "Finished. Total time: " << duration;

  dfly::Mutex mutex;
  base::Histogram hist;
  LOG(INFO) << "Resetting all threads";
  pp->AwaitFiberOnAll([&](auto* p) {
    lock_guard gu(mutex);
    hist.Merge(client->hist);
    client.reset();
  });

  LOG(INFO) << "Summary:\n" << hist.ToString();

  pp->Stop();

  return 0;
}
