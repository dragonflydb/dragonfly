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
#include "base/zipf_gen.h"
#include "facade/redis_parser.h"
#include "io/io_buf.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"
#include "util/fibers/uring_socket.h"

// A load-test for DragonflyDB that fixes coordinated omission problem.

using std::string;

ABSL_FLAG(uint16_t, p, 6379, "Server port");
ABSL_FLAG(uint32_t, c, 20, "Number of connections per thread");
ABSL_FLAG(uint32_t, qps, 20, "QPS schedule at which the generator sends requests to the server");
ABSL_FLAG(uint32_t, n, 1000, "Number of requests to send per connection");
ABSL_FLAG(string, h, "localhost", "server hostname/ip");
ABSL_FLAG(uint64_t, key_minimum, 0, "Min value for keys used");
ABSL_FLAG(uint64_t, key_maximum, 10'000, "Max value for keys used");
ABSL_FLAG(string, key_prefix, "key:", "keys prefix");
ABSL_FLAG(string, key_dist, "U", "U for uniform, N for normal, Z for zipfian");
ABSL_FLAG(double, zipf_alpha, 0.99, "zipfian alpha parameter");
ABSL_FLAG(uint64_t, key_stddev, 0,
          "Standard deviation for non-uniform distribution, 0 chooses"
          " a default value of (max-min)/6");
ABSL_FLAG(string, ratio, "1:10", "Set:Get ratio");
ABSL_FLAG(string, command, "", "custom command with __key__ placeholder for keys");

using namespace std;
using namespace util;
using absl::GetFlag;
using facade::RedisParser;
using facade::RespVec;
using tcp = ::boost::asio::ip::tcp;

constexpr string_view kKeyPlaceholder = "__key__"sv;

thread_local absl::InsecureBitGen bit_gen;

class KeyGenerator {
 public:
  KeyGenerator(uint32_t min, uint32_t max);

  string operator()();

 private:
  string prefix_;
  uint64_t min_, max_, range_;
  double stddev_ = 1.0 / 6;
  optional<base::ZipfianGenerator> zipf_;
  enum DistType { UNIFORM, NORMAL, ZIPFIAN } dist_type_;
};

class CommandGenerator {
 public:
  CommandGenerator(KeyGenerator* keygen);

  string operator()();

 private:
  KeyGenerator* keygen_;
  uint32_t ratio_set_ = 0, ratio_get_ = 0;
  string command_;
  string cmd_;
  std::vector<size_t> key_indices_;
};

CommandGenerator::CommandGenerator(KeyGenerator* keygen) : keygen_(keygen) {
  command_ = GetFlag(FLAGS_command);
  if (command_.empty()) {
    pair<string, string> ratio_str = absl::StrSplit(GetFlag(FLAGS_ratio), ':');
    CHECK(absl::SimpleAtoi(ratio_str.first, &ratio_set_));
    CHECK(absl::SimpleAtoi(ratio_str.second, &ratio_get_));
  } else {
    for (size_t pos = 0; (pos = command_.find(kKeyPlaceholder, pos)) != string::npos;
         pos += kKeyPlaceholder.size()) {
      key_indices_.push_back(pos);
    }
  }
}

string CommandGenerator::operator()() {
  cmd_.clear();
  string key;
  if (command_.empty()) {
    key = (*keygen_)();

    if (absl::Uniform(bit_gen, 0U, ratio_get_ + ratio_set_) < ratio_set_) {
      // TODO: value size
      absl::StrAppend(&cmd_, "set ", key, " val\r\n");
    } else {
      absl::StrAppend(&cmd_, "get ", key, "\r\n");
    }
  } else {
    size_t last_pos = 0;
    for (size_t pos : key_indices_) {
      key = (*keygen_)();
      absl::StrAppend(&cmd_, command_.substr(last_pos, pos - last_pos), key);
      last_pos = pos + kKeyPlaceholder.size();
    }
    absl::StrAppend(&cmd_, command_.substr(last_pos), "\r\n");
  }
  return cmd_;
}

// Per connection driver.
class Driver {
 public:
  explicit Driver(ProactorBase* p = nullptr) {
    if (p)
      socket_.reset(p->CreateSocket());
  }

  Driver(const Driver&) = delete;
  Driver(Driver&&) = default;
  Driver& operator=(Driver&&) = default;

  void Connect(unsigned index, const tcp::endpoint& ep);
  void Run(uint32_t num_reqs, uint64_t cycle_ns, base::Histogram* dest);

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
      driver = Driver{p_};
    }
  }

  TLocalClient(const TLocalClient&) = delete;

  void Connect(tcp::endpoint ep);
  void Run(uint64_t cycle_ns);

  base::Histogram hist;

 private:
  ProactorBase* p_;
  vector<Driver> drivers_;
};

KeyGenerator::KeyGenerator(uint32_t min, uint32_t max)
    : min_(min), max_(max), range_(max - min + 1) {
  prefix_ = GetFlag(FLAGS_key_prefix);
  string dist = GetFlag(FLAGS_key_dist);
  CHECK_GT(range_, 0u);

  if (dist == "U") {
    dist_type_ = UNIFORM;
  } else if (dist == "N") {
    dist_type_ = NORMAL;
    uint64_t stddev = GetFlag(FLAGS_key_stddev);
    if (stddev != 0) {
      stddev_ = double(stddev) / double(range_);
    }
  } else if (dist == "Z") {
    dist_type_ = ZIPFIAN;
    zipf_.emplace(min, max, GetFlag(FLAGS_zipf_alpha));
  } else {
    LOG(FATAL) << "Unknown distribution type: " << dist;
  }
}

string KeyGenerator::operator()() {
  uint64_t key_suffix{0};
  switch (dist_type_) {
    case UNIFORM:
      key_suffix = absl::Uniform(bit_gen, min_, max_);
      break;
    case NORMAL: {
      double val = absl::Gaussian(bit_gen, 0.5, stddev_);
      key_suffix = min_ + uint64_t(val * range_);
      break;
    }
    case ZIPFIAN:
      key_suffix = zipf_->Next(bit_gen);
      break;
  }

  return absl::StrCat(prefix_, key_suffix);
}

void Driver::Connect(unsigned index, const tcp::endpoint& ep) {
  VLOG(2) << "Connecting " << index;
  error_code ec = socket_->Connect(ep);
  CHECK(!ec) << "Could not connect to " << ep << " " << ec;
}

void Driver::Run(uint32_t num_reqs, uint64_t cycle_ns, base::Histogram* dest) {
  auto receive_fb = MakeFiber([this, dest] { ReceiveFb(dest); });

  int64_t next_invocation = absl::GetCurrentTimeNanos();

  const absl::Time start = absl::Now();

  const uint32_t key_minimum = GetFlag(FLAGS_key_minimum);
  const uint32_t key_maximum = GetFlag(FLAGS_key_maximum);

  KeyGenerator key_gen(key_minimum, key_maximum);
  CommandGenerator cmd_gen(&key_gen);
  for (unsigned i = 0; i < num_reqs; ++i) {
    int64_t now = absl::GetCurrentTimeNanos();

    int64_t sleep_ns = next_invocation - now;
    if (sleep_ns > 0) {
      VLOG(5) << "Sleeping for " << sleep_ns << "ns";
      ThisFiber::SleepFor(chrono::nanoseconds(sleep_ns));
    } else {
      VLOG(5) << "Behind QPS schedule";
    }
    next_invocation += cycle_ns;

    string cmd = cmd_gen();

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
  VLOG(1) << "Done queuing " << num_reqs << " requests, which took " << finish - start
          << ". Waiting for server processing";

  // TODO: to change to a condvar or something.
  while (!reqs_.empty()) {
    ThisFiber::SleepFor(1ms);
  }

  socket_->Shutdown(SHUT_RDWR);  // breaks the receive fiber.
  receive_fb.Join();
  std::ignore = socket_->Close();
}

void Driver::ReceiveFb(base::Histogram* dest) {
  facade::RedisParser parser{1 << 16, false};
  io::IoBuf io_buf{512};
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
        uint64_t now = absl::GetCurrentTimeNanos();
        uint64_t usec = (now - reqs_.front().start) / 1000;
        dest->Add(usec);
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
      drivers_[i].Connect(i, ep);
    });
  }

  for (auto& fb : fbs)
    fb.Join();
}

void TLocalClient::Run(uint64_t cycle_ns) {
  vector<fb2::Fiber> fbs(drivers_.size());
  uint32_t num_reqs = GetFlag(FLAGS_n);

  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] =
        fb2::Fiber(absl::StrCat("run/", i), [&, i] { drivers_[i].Run(num_reqs, cycle_ns, &hist); });
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

  const uint32_t qps = GetFlag(FLAGS_qps);
  const int64_t interval = 1000000000LL / qps;
  uint32_t num_reqs = GetFlag(FLAGS_n);

  CONSOLE_INFO << "Running all threads, sending " << num_reqs << " requests at a rate of "
               << GetFlag(FLAGS_qps) << "qps, i.e. request every " << interval / 1000 << "us";

  const absl::Time start_time = absl::Now();
  pp->AwaitFiberOnAll([&](auto* p) { client->Run(interval); });
  absl::Duration duration = absl::Now() - start_time;
  LOG(INFO) << "Finished. Total time: " << duration;

  fb2::Mutex mutex;
  base::Histogram hist;
  LOG(INFO) << "Resetting all threads";
  pp->AwaitFiberOnAll([&](auto* p) {
    lock_guard gu(mutex);
    hist.Merge(client->hist);
    client.reset();
  });

  CONSOLE_INFO << "Summary, all times are in usec:\n" << hist.ToString();

  pp->Stop();

  return 0;
}
