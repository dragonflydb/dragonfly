// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

extern "C" {
#include "redis/crc16.h"
}

#include <absl/container/flat_hash_set.h>
#include <absl/random/random.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>

#include <boost/icl/interval_set.hpp>
#include <queue>
#include <tuple>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "base/histogram.h"
#include "base/init.h"
#include "base/random.h"
#include "base/zipf_gen.h"
#include "facade/redis_parser.h"
#include "io/io.h"
#include "io/io_buf.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/uring_socket.h"

// A load-test for DragonflyDB that fixes coordinated omission problem.

using std::string;

ABSL_FLAG(uint16_t, p, 6379, "Server port");
ABSL_FLAG(uint32_t, c, 20, "Number of connections per thread");
ABSL_FLAG(uint32_t, qps, 20, "QPS schedule at which the generator sends requests to the server");
ABSL_FLAG(uint32_t, n, 1000, "Number of requests to send per connection");
ABSL_FLAG(uint32_t, test_time, 0, "Testing time in seconds");
ABSL_FLAG(uint32_t, d, 16, "Value size in bytes ");
ABSL_FLAG(string, h, "localhost", "server hostname/ip");
ABSL_FLAG(uint64_t, key_minimum, 0, "Min value for keys used");
ABSL_FLAG(uint64_t, key_maximum, 50'000'000, "Max value for keys used");
ABSL_FLAG(string, key_prefix, "key:", "keys prefix");
ABSL_FLAG(string, key_dist, "U", "U for uniform, N for normal, Z for zipfian, S for sequential");
ABSL_FLAG(double, zipf_alpha, 0.99, "zipfian alpha parameter");
ABSL_FLAG(uint64_t, seed, 42, "A seed for random data generation");
ABSL_FLAG(uint64_t, key_stddev, 0,
          "Standard deviation for non-uniform distribution, 0 chooses"
          " a default value of (max-min)/6");
ABSL_FLAG(uint32_t, pipeline, 1, "maximum number of pending requests per connection");
ABSL_FLAG(string, ratio, "1:10", "Set:Get ratio");
ABSL_FLAG(string, command, "",
          "custom command with __key__ placeholder for keys, "
          "__data__ for values, __score__ for doubles");
ABSL_FLAG(string, P, "", "protocol can be empty (for RESP) or memcache_text");

ABSL_FLAG(bool, tcp_nodelay, false, "If true, set nodelay option on tcp socket");
ABSL_FLAG(bool, noreply, false, "If true, does not wait for replies. Relevant only for memcached.");
ABSL_FLAG(bool, greet, true,
          "If true, sends a greeting command on each connection, "
          "to make sure the connection succeeded");
ABSL_FLAG(bool, cluster_skip_tags, true,
          "If true, skips tags (compatible with memtier benchmark) in cluster mode, "
          "othewise adds hash tags to keys");
ABSL_FLAG(bool, ascii, true, "If true, use ascii characters for values");

using namespace std;
using namespace util;
using absl::GetFlag;
using absl::StrFormat;
using facade::RedisParser;
using facade::RespExpr;
using facade::RespVec;
using tcp = ::boost::asio::ip::tcp;
using absl::StrCat;

thread_local base::Xoroshiro128p bit_gen;

#if __INTELLISENSE__
#pragma diag_suppress 144
#endif

enum Protocol { RESP, MC_TEXT } protocol;
enum DistType { UNIFORM, NORMAL, ZIPFIAN, SEQUENTIAL } dist_type{UNIFORM};
constexpr uint16_t kNumSlots = 16384;

static string GetRandomHex(size_t len, bool ascii) {
  std::string res(len, '\0');
  size_t indx = 0;

  for (; indx + 16 <= len; indx += 16) {  // 2 chars per byte
    absl::numbers_internal::FastHexToBufferZeroPad16(bit_gen(), res.data() + indx);
  }

  DCHECK_LE(indx, len);

  if (indx < len) {
    char buf[24];
    absl::numbers_internal::FastHexToBufferZeroPad16(bit_gen(), buf);

    for (unsigned j = 0; indx < len;) {
      res[indx++] = buf[j++];
    }
  }

  if (!ascii) {
    for (size_t i = 0; i < len; i++) {
      res[i] += 80;
    }
  }
  return res;
}

uint16_t SlotId(string_view str) {
  return crc16(str.data(), str.size()) % kNumSlots;
}

using SlotRange = pair<uint16_t, uint16_t>;

struct ShardInfo {
  vector<SlotRange> slots;  // list of [start, end] pairs. inclusive.
  tcp::endpoint endpoint;
};

using ClusterShards = vector<ShardInfo>;

class ShardSlots {
 private:
  using IntervalSet = boost::icl::interval_set<uint16_t>;
  using Interval = boost::icl::interval<uint16_t>;

 public:
  void SetClusterSlotRanges(const ClusterShards& cluster_shards) {
    for (auto shard : cluster_shards) {
      IntervalSet shard_slots_;
      for (auto& slot : shard.slots) {
        shard_slots_.insert(Interval::closed(slot.first, slot.second));
      }
      shards_slots_.emplace(shard.endpoint, shard_slots_);
    }
  }

  SlotRange NextSlotRange(const tcp::endpoint& ep, size_t i) {
    shared_lock<fb2::SharedMutex> lock(mu_);
    const auto& shard_slot_interval = shards_slots_[ep];
    unsigned index = i % shard_slot_interval.iterative_size();
    const auto& interval = next(shard_slot_interval.begin(), index);
    return SlotRange{boost::icl::first(*interval), boost::icl::last(*interval)};
  }

  bool Empty() const {
    return shards_slots_.empty();
  }

  size_t Size() const {
    return shards_slots_.size();
  }

  vector<tcp::endpoint> Endpoints() const {
    vector<tcp::endpoint> endpoints;
    for (const auto& shard : shards_slots_) {
      endpoints.push_back(shard.first);
    }
    return endpoints;
  }

  void MoveSlot(const tcp::endpoint& src_ep, const tcp::endpoint& dst_ep, uint16_t slot_id) {
    unique_lock<fb2::SharedMutex> lock(mu_);
    // Remove slot from source ep
    auto& src_shard_slots = shards_slots_[src_ep];
    // If slot id doesn't exists on source ep we have moved this slot before
    if (src_shard_slots.find(slot_id) == src_shard_slots.end()) {
      return;
    }
    src_shard_slots.subtract(slot_id);
    // Add slot to dest ep
    auto& dst_shard_slots = shards_slots_[dst_ep];
    dst_shard_slots.insert(slot_id);
  }

 private:
  struct Hasher {
    using is_transparent = void;
    size_t operator()(const tcp::endpoint& ep) const {
      std::size_t hash1 = std::hash<string>()(ep.address().to_string());
      std::size_t hash2 = std::hash<unsigned short>()(ep.port());
      return hash1 ^ (hash2 + 0x9e3779b9 + (hash1 << 6) + (hash1 >> 2));
    }
  };

  struct Eq {
    using is_transparent = void;
    bool operator()(const tcp::endpoint& left, const tcp::endpoint& right) const {
      return left == right;
    }
  };

 private:
  fb2::SharedMutex mu_;
  absl::flat_hash_map<tcp::endpoint, IntervalSet, Hasher, Eq> shards_slots_;
};

class KeyGenerator {
 public:
  KeyGenerator(uint32_t min, uint32_t max);

  string operator()(uint16_t from, uint16_t to) const;
  void EnableClusterMode();

  bool IsClusterEnabled() const {
    return !hash_slots_.empty();
  }

 private:
  string prefix_;
  uint64_t min_, max_, range_;
  mutable uint64_t seq_cursor_;
  double stddev_ = 1.0 / 6;
  mutable optional<base::ZipfianGenerator> zipf_;
  vector<string> hash_slots_;
};

class CommandGenerator {
 public:
  CommandGenerator(KeyGenerator* keygen);

  string Next(SlotRange range);

  bool might_hit() const {
    return might_hit_;
  }

  bool noreply() const {
    return noreply_;
  }

 private:
  enum TemplateType { KEY, VALUE, SCORE };

  string FillSet(string_view key);
  string FillGet(string_view key);

  KeyGenerator* keygen_;
  uint32_t ratio_set_ = 0, ratio_get_ = 0;
  string command_;

  using CmdPart = variant<string_view, TemplateType>;
  vector<CmdPart> cmd_parts_;

  string value_;
  bool might_hit_ = false;
  bool noreply_ = false;
  bool is_ascii_ = true;
};

CommandGenerator::CommandGenerator(KeyGenerator* keygen) : keygen_(keygen) {
  command_ = GetFlag(FLAGS_command);
  is_ascii_ = GetFlag(FLAGS_ascii);
  value_ = string(GetFlag(FLAGS_d), is_ascii_ ? 'a' : char(130));

  if (command_.empty()) {
    pair<string, string> ratio_str = absl::StrSplit(GetFlag(FLAGS_ratio), ':');
    CHECK(absl::SimpleAtoi(ratio_str.first, &ratio_set_));
    CHECK(absl::SimpleAtoi(ratio_str.second, &ratio_get_));
    return;
  }

  vector<string_view> parts = absl::StrSplit(command_, ' ', absl::SkipEmpty());
  for (string_view p : parts) {
    if (p == "__key__"sv) {
      cmd_parts_.emplace_back(KEY);
    } else if (p == "__data__"sv) {
      cmd_parts_.emplace_back(VALUE);
    } else if (p == "__score__"sv) {
      cmd_parts_.emplace_back(SCORE);
    } else {
      cmd_parts_.emplace_back(p);
    }
  }

  if (!cmd_parts_.empty()) {
    const string_view* cmd = get_if<string_view>(&cmd_parts_.front());
    if (cmd) {
      might_hit_ = absl::EqualsIgnoreCase(*cmd, "get") || absl::StartsWithIgnoreCase(*cmd, "mget");
    }
  }
}

string CommandGenerator::Next(SlotRange range) {
  noreply_ = false;

  if (command_.empty()) {
    string key = (*keygen_)(range.first, range.second);

    if (absl::Uniform(bit_gen, 0U, ratio_get_ + ratio_set_) < ratio_set_) {
      might_hit_ = false;
      return FillSet(key);
    }
    might_hit_ = true;
    return FillGet(key);
  }

  // For custom commands, we select a random slot and then use it for key generation.
  uint16_t slot_id = 0;

  if (keygen_->IsClusterEnabled()) {
    slot_id = absl::Uniform(absl::IntervalClosedClosed, bit_gen, range.first, range.second);
  }

  string str, gen_cmd;
  absl::StrAppend(&gen_cmd, "*", cmd_parts_.size(), "\r\n");
  for (const CmdPart& part : cmd_parts_) {
    if (auto p = get_if<string_view>(&part)) {
      absl::StrAppend(&gen_cmd, "$", p->size(), "\r\n", *p, "\r\n");
    } else {
      switch (get<TemplateType>(part)) {
        case KEY:
          str = (*keygen_)(slot_id, slot_id);
          break;
        case VALUE:
          str = GetRandomHex(value_.size(), is_ascii_);
          break;
        case SCORE: {
          uniform_real_distribution<double> uniform(0, 1);
          str = absl::StrCat(uniform(bit_gen));
        }
      }
      absl::StrAppend(&gen_cmd, "$", str.size(), "\r\n", str, "\r\n");
    }
  }

  return gen_cmd;
}

string CommandGenerator::FillSet(string_view key) {
  string res;

  if (protocol == RESP) {
    absl::StrAppend(&res, "*3\r\n$3\r\nset\r\n$", key.size(), "\r\n", key);
    absl::StrAppend(&res, "\r\n$", value_.size(), "\r\n", value_, "\r\n");
  } else {
    DCHECK_EQ(protocol, MC_TEXT);
    absl::StrAppend(&res, "set ", key, " 0 0 ", value_.size());
    if (GetFlag(FLAGS_noreply)) {
      absl::StrAppend(&res, " noreply");
      noreply_ = true;
    }

    absl::StrAppend(&res, "\r\n", value_, "\r\n");
  }
  return res;
}

string CommandGenerator::FillGet(string_view key) {
  return absl::StrCat("get ", key, "\r\n");
}

struct ClientStats {
  base::Histogram hist;

  uint64_t num_responses = 0;
  uint64_t hit_count = 0;
  uint64_t hit_opportunities = 0;
  uint64_t num_errors = 0;
  unsigned num_clients = 0;

  ClientStats& operator+=(const ClientStats& o) {
    hist.Merge(o.hist);
    num_responses += o.num_responses;
    hit_count += o.hit_count;
    hit_opportunities += o.hit_opportunities;
    num_errors += o.num_errors;
    num_clients += o.num_clients;
    return *this;
  }
};

// Per connection driver.
class Driver {
 public:
  explicit Driver(uint32_t num_reqs, uint32_t time_limit, ClientStats* stats, ProactorBase* p,
                  ShardSlots* ss)
      : num_reqs_(num_reqs), time_limit_(time_limit), shard_slots_(*ss), stats_(*stats) {
    socket_.reset(p->CreateSocket());
    if (time_limit_ > 0)
      num_reqs_ = UINT32_MAX;
  }

  Driver(const Driver&) = delete;
  Driver(Driver&&) = delete;
  Driver& operator=(Driver&&) = delete;

  void Connect(unsigned index, const tcp::endpoint& ep);
  void Run(uint64_t* cycle_ns, CommandGenerator* cmd_gen);

  float done() const {
    if (time_limit_ > 0)
      return double(absl::GetCurrentTimeNanos() - start_ns_) / (time_limit_ * 1e9);
    return double(received_) / num_reqs_;
  }

  unsigned pending_length() const {
    return reqs_.size();
  }

 private:
  void PopRequest();
  void ReceiveFb();
  void ParseRESP();
  void ParseMC();

  struct Req {
    uint64_t start;
    bool might_hit;
  };

  uint32_t num_reqs_, time_limit_, received_ = 0;
  int64_t start_ns_ = 0;

  tcp::endpoint ep_;
  ShardSlots& shard_slots_;
  ClientStats& stats_;
  unique_ptr<FiberSocketBase> socket_;
  fb2::Fiber receive_fb_;
  queue<Req> reqs_;
  fb2::CondVarAny cnd_;

  facade::RedisParser parser_{RedisParser::Mode::CLIENT, 1 << 16};
  io::IoBuf io_buf_{512};
};

// Per thread client.
class TLocalClient {
 public:
  explicit TLocalClient(ProactorBase* p, ShardSlots* ss) : p_(p), shard_slots_(ss) {
  }

  TLocalClient(const TLocalClient&) = delete;

  void Connect(tcp::endpoint ep, const vector<tcp::endpoint>& shard_endpoints);
  void Start(uint32_t key_min, uint32_t key_max, uint64_t cycle_ns);
  void Join();

  ClientStats stats;

  tuple<float, float> GetMinMaxDone() const {
    float min = 1, max = 0;

    for (unsigned i = 0; i < drivers_.size(); ++i) {
      float done = drivers_[i]->done();
      max = std::max(done, max);
      min = std::min(done, min);
    }

    return {min, max};
  }

  unsigned MaxPending() const {
    unsigned max = 0;
    for (unsigned i = 0; i < drivers_.size(); ++i) {
      if (drivers_[i]->pending_length() > max) {
        max = drivers_[i]->pending_length();
      }
    }
    return max;
  }

  unsigned num_conns() const {
    return drivers_.size();
  }

  void AdjustCycle();

 private:
  ProactorBase* p_;
  ShardSlots* shard_slots_;
  vector<unique_ptr<Driver>> drivers_;
  optional<KeyGenerator> key_gen_;
  optional<CommandGenerator> cmd_gen_;

  vector<fb2::Fiber> driver_fbs_;
  uint64_t cur_cycle_ns_;
  uint64_t target_cycle_;
  int64_t start_time_;
};

KeyGenerator::KeyGenerator(uint32_t min, uint32_t max)
    : min_(min), max_(max), range_(max - min + 1) {
  prefix_ = GetFlag(FLAGS_key_prefix);
  CHECK_GT(range_, 0u);

  seq_cursor_ = min_;
  switch (dist_type) {
    case NORMAL: {
      uint64_t stddev = GetFlag(FLAGS_key_stddev);
      if (stddev != 0) {
        stddev_ = double(stddev) / double(range_);
      }
      break;
    }
    case ZIPFIAN:
      zipf_.emplace(min, max, GetFlag(FLAGS_zipf_alpha));
      break;
    default:;
  }
}

string KeyGenerator::operator()(uint16_t from, uint16_t to) const {
  uint64_t key_suffix = 0;
  uint16_t slot_id = from;
  bool skip_tags = IsClusterEnabled() && GetFlag(FLAGS_cluster_skip_tags);
  string res;

  do {
    switch (dist_type) {
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
      case SEQUENTIAL:
        key_suffix = seq_cursor_++;
        if (seq_cursor_ > max_)
          seq_cursor_ = min_;
        break;
    }

    if (!skip_tags)
      break;

    // If we skip tags, we must make sure that the key fits the slot range.
    res = absl::StrCat(prefix_, key_suffix);
    slot_id = SlotId(res);
  } while (slot_id < from || slot_id > to);

  // If we are in cluster mode we add the hash slot to the key to make sure it lands in the correct
  // range.
  if (IsClusterEnabled()) {
    if (!skip_tags) {
      if (to > from)
        slot_id = absl::Uniform(absl::IntervalClosedClosed, bit_gen, from, to);
      absl::StrAppend(&res, prefix_, "{", hash_slots_[slot_id], "}", key_suffix);
    }
  } else {
    absl::StrAppend(&res, prefix_, key_suffix);
  }

  return res;
}

void KeyGenerator::EnableClusterMode() {
  hash_slots_.resize(kNumSlots);
  uint32_t i = 0;
  uint32_t num_slots_filled = 0;

  // Precompute the hash slots for each of the slot ids so given the slot id
  // we could generate a key that belongs to that slot.
  while (num_slots_filled < kNumSlots) {
    string key = absl::StrCat(i);
    uint16_t id = SlotId(key);
    if (hash_slots_[id].empty()) {
      hash_slots_[id] = std::move(key);
      num_slots_filled++;
    }
    ++i;
  }
}

void Driver::Connect(unsigned index, const tcp::endpoint& ep) {
  VLOG(2) << "Connecting " << index << " to " << ep;
  error_code ec = socket_->Connect(ep);
  CHECK(!ec) << "Could not connect to " << ep << " " << ec;
  if (GetFlag(FLAGS_tcp_nodelay)) {
    int yes = 1;
    CHECK_EQ(0, setsockopt(socket_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));
  }

  if (absl::GetFlag(FLAGS_greet)) {
    // TCP Connect does not ensure that the connection was indeed accepted by the server.
    // if server backlog is too short the connection will get stuck in the accept queue.
    // Therefore, we send a ping command to ensure that every connection got connected.
    ec = socket_->Write(io::Buffer("ping\r\n"));
    CHECK(!ec);

    uint8_t buf[128];
    auto res_sz = socket_->Recv(io::MutableBytes(buf));
    CHECK(res_sz) << res_sz.error().message();
    string_view resp = io::View(io::Bytes(buf, *res_sz));
    CHECK(absl::EndsWith(resp, "\r\n")) << resp;
  }
  ep_ = ep;
  receive_fb_ = MakeFiber(fb2::Launch::dispatch, [this] { ReceiveFb(); });
}

void Driver::Run(uint64_t* cycle_ns, CommandGenerator* cmd_gen) {
  start_ns_ = absl::GetCurrentTimeNanos();
  unsigned pipeline = GetFlag(FLAGS_pipeline);

  stats_.num_clients++;
  int64_t time_limit_ns =
      time_limit_ > 0 ? int64_t(time_limit_) * 1'000'000'000 + start_ns_ : INT64_MAX;

  SlotRange slot_range{0, kNumSlots - 1};

  for (unsigned i = 0; i < num_reqs_; ++i) {
    int64_t now = absl::GetCurrentTimeNanos();

    if (now > time_limit_ns) {
      break;
    }
    if (cycle_ns) {
      int64_t target_ts = start_ns_ + i * (*cycle_ns);
      int64_t sleep_ns = target_ts - now;
      if (reqs_.size() > 10 && sleep_ns <= 0) {
        sleep_ns = 10'000;
      }

      if (sleep_ns > 0) {
        VLOG(5) << "Sleeping for " << sleep_ns << "ns";
        // There is no point in sending more requests if they are piled up in the server.
        do {
          ThisFiber::SleepFor(chrono::nanoseconds(sleep_ns));
        } while (reqs_.size() > 10);
      } else if (i % 256 == 255) {
        ThisFiber::Yield();
        VLOG(5) << "Behind QPS schedule";
      }
    } else {
      // Coordinated omission.

      fb2::NoOpLock lk;
      cnd_.wait(lk, [this, pipeline] { return reqs_.size() < pipeline; });
    }

    // TODO: this skews the distribution if slot ranges are uneven.
    // Ideally we would like to pick randomly a single slot from all the ranges we have
    // and pass it to cmd_gen->Next below.
    if (!shard_slots_.Empty()) {
      slot_range = shard_slots_.NextSlotRange(ep_, i);
    }

    string cmd = cmd_gen->Next(slot_range);

    Req req;
    req.start = absl::GetCurrentTimeNanos();
    req.might_hit = cmd_gen->might_hit();

    reqs_.push(req);

    error_code ec = socket_->Write(io::Buffer(cmd));
    if (ec && FiberSocketBase::IsConnClosed(ec)) {
      // TODO: report failure
      VLOG(1) << "Connection closed";
      break;
    }
    CHECK(!ec) << ec.message();
    if (cmd_gen->noreply()) {
      PopRequest();
    }
  }

  int64_t finish = absl::GetCurrentTimeNanos();
  VLOG(1) << "Done queuing " << num_reqs_ << " requests, which took "
          << StrFormat("%.1fs", double(finish - start_ns_) / 1000'000'000)
          << ". Waiting for server processing";

  // TODO: to change to a condvar or something.
  while (!reqs_.empty()) {
    ThisFiber::SleepFor(1ms);
  }

  std::ignore = socket_->Shutdown(SHUT_RDWR);  // breaks the receive fiber.
  receive_fb_.Join();
  std::ignore = socket_->Close();
  stats_.num_clients--;
}

static string_view FindLine(io::Bytes buf) {
  if (buf.size() < 2)
    return {};
  for (unsigned i = 0; i < buf.size() - 1; ++i) {
    if (buf[i] == '\r' && buf[i + 1] == '\n') {
      return io::View(buf.subspan(0, i + 2));
    }
  }
  return {};
};

void Driver::PopRequest() {
  uint64_t now = absl::GetCurrentTimeNanos();
  uint64_t usec = (now - reqs_.front().start) / 1000;
  stats_.hist.Add(usec);
  stats_.hit_opportunities += reqs_.front().might_hit;
  ++received_;
  reqs_.pop();
  if (reqs_.empty()) {
    cnd_.notify_one();
  }
  ++stats_.num_responses;
}

void Driver::ReceiveFb() {
  while (true) {
    io_buf_.EnsureCapacity(256);
    auto buf = io_buf_.AppendBuffer();
    VLOG(3) << "Socket read: " << reqs_.size();

    ::io::Result<size_t> recv_sz = socket_->Recv(buf);
    if (!recv_sz && FiberSocketBase::IsConnClosed(recv_sz.error())) {
      LOG_IF(DFATAL, !reqs_.empty())
          << "Broke with " << reqs_.size() << " requests,  received: " << received_;
      // clear reqs - to prevent Driver::Run block on them indefinitely.
      decltype(reqs_)().swap(reqs_);
      break;
    }

    CHECK(recv_sz) << recv_sz.error().message();
    io_buf_.CommitWrite(*recv_sz);

    if (protocol == RESP) {
      ParseRESP();
    } else {
      // MC_TEXT
      ParseMC();
    }
  }
  VLOG(1) << "ReceiveFb done";
}

void Driver::ParseRESP() {
  uint32_t consumed = 0;
  RedisParser::Result result = RedisParser::OK;
  RespVec parse_args;
  constexpr string_view kMovedErrorKey = "MOVED"sv;

  do {
    result = parser_.Parse(io_buf_.InputBuffer(), &consumed, &parse_args);
    if (result == RedisParser::OK && !parse_args.empty()) {
      if (parse_args[0].type == RespExpr::ERROR) {
        string_view error = parse_args[0].GetView();
        VLOG(2) << "Error " << error;
        if (absl::StartsWith(error, kMovedErrorKey)) {
          error = error.substr(kMovedErrorKey.length());
          vector<string_view> parts =
              absl::StrSplit(absl::StripTrailingAsciiWhitespace(error), ' ', absl::SkipEmpty());

          CHECK_EQ(parts.size(), 2u);
          uint32_t slot_id;
          CHECK(absl::SimpleAtoi(parts[0], &slot_id));

          vector<string_view> addr_parts = absl::StrSplit(parts[1], ':');
          CHECK_EQ(2u, addr_parts.size());

          auto host = ::boost::asio::ip::make_address(addr_parts[0]);

          uint32_t port;
          CHECK(absl::SimpleAtoi(addr_parts[1], &port));
          CHECK_LT(port, 65536u);

          shard_slots_.MoveSlot(ep_, tcp::endpoint(host, port), slot_id);
        }
        ++stats_.num_errors;
      } else if (reqs_.front().might_hit && parse_args[0].type != RespExpr::NIL) {
        ++stats_.hit_count;
      }
      parse_args.clear();
      PopRequest();
    }
    io_buf_.ConsumeInput(consumed);
  } while (result == RedisParser::OK && io_buf_.InputLen() > 0);
}

void Driver::ParseMC() {
  unsigned blob_len = 0;

  while (true) {
    string_view line = FindLine(io_buf_.InputBuffer());
    if (line.empty())
      break;

    CHECK_EQ(line.back(), '\n');
    if (line == "STORED\r\n" || line == "END\r\n") {
      PopRequest();
      blob_len = 0;
    } else if (absl::StartsWith(line, "VALUE")) {
      // last token is a blob length.
      auto it = line.rbegin();
      while (it != line.rend() && *it != ' ')
        ++it;
      size_t len = it - line.rbegin() - 2;
      const char* start = &(*it) + 1;
      if (!absl::SimpleAtoi(string(start, len), &blob_len)) {
        LOG(ERROR) << "Invalid blob len " << line;
        return;
      }
      ++stats_.hit_count;
    } else if (absl::StartsWith(line, "SERVER_ERROR")) {
      ++stats_.num_errors;
      PopRequest();
      blob_len = 0;
    } else {
      auto handle = socket_->native_handle();
      CHECK_EQ(blob_len + 2, line.size()) << line;
      blob_len = 0;
      VLOG(2) << "Got line " << handle << ": " << line;
    }
    io_buf_.ConsumeInput(line.size());
  }
}

void TLocalClient::Connect(tcp::endpoint ep, const vector<tcp::endpoint>& endpoints) {
  VLOG(2) << "Connecting client...";

  unsigned conn_per_shard = GetFlag(FLAGS_c);
  if (shard_slots_->Empty()) {
    drivers_.resize(conn_per_shard);
  } else {
    drivers_.resize(shard_slots_->Size() * conn_per_shard);
  }

  for (auto& driver : drivers_) {
    driver.reset(new Driver{GetFlag(FLAGS_n), GetFlag(FLAGS_test_time), &stats, p_, shard_slots_});
  }
  vector<fb2::Fiber> fbs(drivers_.size());

  for (size_t i = 0; i < fbs.size(); ++i) {
    vector<SlotRange> slots;
    tcp::endpoint shard_ep = ep;
    if (!shard_slots_->Empty()) {
      size_t shard = i / conn_per_shard;
      shard_ep = endpoints[shard];
    }
    fbs[i] = MakeFiber([&, shard_ep, i] {
      ThisFiber::SetName(StrCat("connect/", i));
      drivers_[i]->Connect(i, shard_ep);
    });
  }

  for (auto& fb : fbs)
    fb.Join();
}

void TLocalClient::Start(uint32_t key_min, uint32_t key_max, uint64_t cycle_ns) {
  key_gen_.emplace(key_min, key_max);
  cmd_gen_.emplace(&key_gen_.value());

  driver_fbs_.resize(drivers_.size());
  if (!shard_slots_->Empty()) {
    key_gen_->EnableClusterMode();
  }
  cur_cycle_ns_ = cycle_ns;
  target_cycle_ = cycle_ns;
  start_time_ = absl::GetCurrentTimeNanos();

  for (size_t i = 0; i < driver_fbs_.size(); ++i) {
    driver_fbs_[i] = fb2::Fiber(StrCat("run/", i), [&, i] {
      drivers_[i]->Run(cur_cycle_ns_ ? &cur_cycle_ns_ : nullptr, &cmd_gen_.value());
    });
  }
}

void TLocalClient::Join() {
  for (auto& fb : driver_fbs_)
    fb.Join();

  VLOG(1) << "Total hits: " << stats.hit_count;
}

void TLocalClient::AdjustCycle() {
  if (cur_cycle_ns_ == 0 || stats.num_responses == 0)
    return;

  // We adjust sleeping cycle per thread, and it's the same for all connection in this thread.
  // We compute the aggregated cycle so far based on responses, and if it
  // is greater than current we increase the current cycle. Otherwise,
  // we try slowly reducing the cycle back to the nominal one.

  int64_t running_time = absl::GetCurrentTimeNanos() - start_time_;
  int64_t real_cycle = running_time * drivers_.size() / stats.num_responses;
  if (real_cycle > cur_cycle_ns_ * 1.05) {
    cur_cycle_ns_ = (cur_cycle_ns_ + real_cycle) / 2;
    VLOG(1) << "Increasing cycle to " << cur_cycle_ns_;
  } else if (cur_cycle_ns_ > target_cycle_) {
    cur_cycle_ns_ -= (cur_cycle_ns_ - target_cycle_) * 0.2;
  }
}

thread_local unique_ptr<TLocalClient> client;

void WatchFiber(size_t num_shards, atomic_bool* finish_signal, ProactorPool* pp) {
  fb2::Mutex mutex;

  int64_t start_time = absl::GetCurrentTimeNanos();
  LOG(INFO) << "Started watching";

  int64_t last_print = start_time;
  uint64_t num_last_resp_cnt = 0;
  num_shards = max<size_t>(num_shards, 1u);
  uint64_t resp_goal = GetFlag(FLAGS_c) * pp->size() * GetFlag(FLAGS_n) * num_shards;
  uint32_t time_limit = GetFlag(FLAGS_test_time);

  while (*finish_signal == false) {
    // we sleep with resolution of 1s but print with lower frequency to be more responsive
    // when benchmark finishes.
    ThisFiber::SleepFor(1s);
    pp->AwaitBrief([](auto, auto*) { client->AdjustCycle(); });

    int64_t now = absl::GetCurrentTimeNanos();
    if (now - last_print < 5000'000'000LL)  // 5s
      continue;

    ClientStats stats;
    float done_max = 0;
    float done_min = 1;
    unsigned max_pending = 0;

    pp->AwaitFiberOnAll([&](auto* p) {
      auto [mind, maxd] = client->GetMinMaxDone();
      unsigned max_pend = client->MaxPending();

      unique_lock lk(mutex);
      stats += client->stats;
      done_max = max(done_max, maxd);
      done_min = min(done_min, mind);
      max_pending = max(max_pending, max_pend);
    });

    uint64_t total_ms = (now - start_time) / 1'000'000;
    uint64_t period_ms = (now - last_print) / 1'000'000;
    uint64_t period_resp_cnt = stats.num_responses - num_last_resp_cnt;
    double done_perc = time_limit > 0 ? double(total_ms) / (10 * time_limit)
                                      : double(stats.num_responses) * 100 / resp_goal;
    double hitrate = stats.hit_opportunities > 0
                         ? 100 * double(stats.hit_count) / double(stats.hit_opportunities)
                         : 0;
    unsigned latency = stats.hist.Percentile(99);

    CONSOLE_INFO << total_ms / 1000 << "s: " << StrFormat("%.1f", done_perc)
                 << "% done, RPS(now/agg): " << period_resp_cnt * 1000 / period_ms << "/"
                 << stats.num_responses * 1000 / total_ms << ", errs: " << stats.num_errors
                 << ", hitrate: " << StrFormat("%.1f%%", hitrate)
                 << ", clients: " << stats.num_clients << "\n"
                 << "done_min: " << StrFormat("%.2f%%", done_min * 100)
                 << ", done_max: " << StrFormat("%.2f%%", done_max * 100)
                 << ", p99_lat(us): " << latency << ", max_pending: " << max_pending;

    last_print = now;
    num_last_resp_cnt = stats.num_responses;
  }
}

ClusterShards FetchClusterInfo(const tcp::endpoint& ep, ProactorBase* proactor) {
  unique_ptr<FiberSocketBase> socket(proactor->CreateSocket());
  error_code ec = socket->Connect(ep);
  CHECK(!ec) << "Could not connect to " << ep << " " << ec;
  ec = socket->Write(io::Buffer("cluster nodes\r\n"));
  CHECK(!ec);
  facade::RedisParser parser{RedisParser::CLIENT, 1024};
  uint8_t buf[1024];
  RespVec resp_vec;
  while (true) {
    io::Result<size_t> res = socket->Recv(buf);
    CHECK(res) << res.error().message();
    RespExpr::Buffer bytes(buf, *res);
    uint32_t consumed = 0;
    facade::RedisParser::Result result = parser.Parse(bytes, &consumed, &resp_vec);
    if (result == facade::RedisParser::OK) {
      break;
    }
    CHECK_EQ(result, facade::RedisParser::INPUT_PENDING);
  }
  CHECK_EQ(1u, resp_vec.size());
  std::ignore = socket->Close();
  if (resp_vec.front().type == RespExpr::ERROR) {
    LOG(INFO) << "Cluster command failed " << resp_vec.front().GetString();
    return {};
  }
  string cluster_spec = resp_vec.front().GetString();
  LOG(INFO) << "Cluster spec: " << cluster_spec;
  vector<string_view> lines = absl::StrSplit(cluster_spec, '\n', absl::SkipEmpty());
  ClusterShards res;
  for (string_view line : lines) {
    vector<string_view> parts = absl::StrSplit(line, ' ');
    // <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv>
    // <config-epoch> <link-state> <slot> <slot> ... <slot>
    if (parts.size() < 9) {
      LOG(WARNING) << "Skipping line: " << line;
      continue;
    }
    ShardInfo shard;
    vector<string_view> addr_parts = absl::StrSplit(parts[1], ':');
    CHECK_EQ(2u, addr_parts.size());
    string host(addr_parts[0]);
    char ip_addr[INET6_ADDRSTRLEN];
    std::error_code ec = fb2::DnsResolve(host, ip_addr);
    CHECK(!ec) << "Could not resolve " << host << " " << ec;
    auto address = ::boost::asio::ip::make_address(ip_addr);

    uint32_t val;
    vector<string_view> port_parts = absl::StrSplit(addr_parts[1], '@');
    CHECK_EQ(2u, port_parts.size());
    CHECK(absl::SimpleAtoi(port_parts[0], &val));
    CHECK_LT(val, 65536u);

    shard.endpoint = tcp::endpoint(address, val);

    string_view flags = parts[2];
    absl::flat_hash_set<string_view> flags_set(absl::StrSplit(flags, ','));
    if (!flags_set.contains("master")) {
      LOG(INFO) << "Skipping non-master node " << shard.endpoint << " " << flags;
      continue;
    }

    for (size_t i = 8; i < parts.size(); ++i) {
      vector<string_view> slots = absl::StrSplit(parts[i], '-');
      if (!absl::SimpleAtoi(slots[0], &val) || val >= kNumSlots) {
        LOG(ERROR) << "Invalid slot definition " << parts[i];
        continue;
      }
      SlotRange slot_range{uint16_t(val), uint16_t(val)};
      if (slots.size() > 1) {
        CHECK(absl::SimpleAtoi(slots[1], &val));
        slot_range.second = val;
      }
      shard.slots.push_back(slot_range);
    }
    res.push_back(shard);
  }

  return res;
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  unique_ptr<ProactorPool> pp;
  pp.reset(fb2::Pool::IOUring(256));
  pp->Run();
  fb2::InitDnsResolver(2000);

  string proto_str = GetFlag(FLAGS_P);
  if (proto_str == "memcache_text") {
    protocol = MC_TEXT;
  } else {
    CHECK(proto_str.empty());
    protocol = RESP;
  }

  string dist = GetFlag(FLAGS_key_dist);

  if (dist == "U") {
    dist_type = UNIFORM;
  } else if (dist == "N") {
    dist_type = NORMAL;
  } else if (dist == "Z") {
    dist_type = ZIPFIAN;
  } else if (dist == "S") {
    dist_type = SEQUENTIAL;
  } else {
    LOG(FATAL) << "Unknown distribution type: " << dist;
  }

  auto* proactor = pp->GetNextProactor();
  char ip_addr[128];

  error_code ec =
      proactor->Await([&] { return fb2::DnsResolve(GetFlag(FLAGS_h), 2000, ip_addr, proactor); });
  CHECK(!ec) << "Could not resolve " << GetFlag(FLAGS_h) << " " << ec;

  auto address = ::boost::asio::ip::make_address(ip_addr);
  tcp::endpoint ep{address, GetFlag(FLAGS_p)};

  ClusterShards shards;
  if (protocol == RESP) {
    shards = proactor->Await([&] { return FetchClusterInfo(ep, proactor); });
  }
  LOG(INFO) << "Connecting threads to "
            << (shards.empty() ? string("single node ")
                               : absl::StrCat(shards.size(), " shard cluster"));

  if (!shards.empty() && !GetFlag(FLAGS_command).empty() && GetFlag(FLAGS_cluster_skip_tags)) {
    // For custom commands we may need to use the same hashtag for multiple keys.
    LOG(WARNING) << "Enforcing hash tags for custom commands";
    absl::SetFlag(&FLAGS_cluster_skip_tags, false);
  }

  ShardSlots shard_slots;
  shard_slots.SetClusterSlotRanges(shards);
  std::vector<tcp::endpoint> shard_endpoints = shard_slots.Endpoints();

  pp->AwaitFiberOnAll([&](unsigned index, auto* p) {
    base::SplitMix64 seed_mix(GetFlag(FLAGS_seed) + index * 0x6a45554a264d72bULL);
    auto seed = seed_mix();
    VLOG(1) << "Seeding bitgen with seed " << seed;
    bit_gen.seed(seed);
    client = make_unique<TLocalClient>(p, &shard_slots);
    client->Connect(ep, shard_endpoints);
  });

  const uint32_t key_minimum = GetFlag(FLAGS_key_minimum);
  const uint32_t key_maximum = GetFlag(FLAGS_key_maximum);
  CHECK_LE(key_minimum, key_maximum);

  uint32_t thread_key_step = 0;
  const uint32_t qps = GetFlag(FLAGS_qps);
  const int64_t interval = qps ? 1'000'000'000LL / qps : 0;
  uint64_t num_reqs = GetFlag(FLAGS_n);
  uint64_t total_conn_num = GetFlag(FLAGS_c) * pp->size();
  uint64_t total_requests = num_reqs * total_conn_num;
  uint32_t time_limit = GetFlag(FLAGS_test_time);

  if (dist_type == SEQUENTIAL) {
    thread_key_step = std::max(1UL, (key_maximum - key_minimum + 1) / pp->size());
    if (total_requests > (key_maximum - key_minimum)) {
      CONSOLE_INFO << "Warning: only " << key_maximum - key_minimum
                   << " unique entries will be accessed with " << total_requests
                   << " total requests";
    }
  }

  if (!time_limit) {
    CONSOLE_INFO << "Running " << pp->size() << " threads, sending " << num_reqs
                 << " requests per each connection, or " << total_requests << " requests overall";
  }
  if (interval) {
    CONSOLE_INFO << "At a rate of " << GetFlag(FLAGS_qps)
                 << " rps per connection, i.e. request every " << interval / 1000 << "us";
    CONSOLE_INFO << "Overall scheduled RPS: " << qps * total_conn_num;
  } else {
    CONSOLE_INFO << "Coordinated omission mode - the rate is determined by the server";
  }

  atomic_bool finish{false};
  pp->AwaitBrief([&](unsigned index, auto* p) {
    uint32_t key_max = (thread_key_step > 0 && index + 1 < pp->size())
                           ? key_minimum + (index + 1) * thread_key_step - 1
                           : key_maximum;
    client->Start(key_minimum + index * thread_key_step, key_max, interval);
  });

  auto watch_fb =
      pp->GetNextProactor()->LaunchFiber([&] { WatchFiber(shards.size(), &finish, pp.get()); });
  const absl::Time start_time = absl::Now();

  // The actual run.
  pp->AwaitFiberOnAll([&](unsigned index, auto* p) { client->Join(); });

  absl::Duration duration = absl::Now() - start_time;
  finish.store(true);
  watch_fb.Join();

  fb2::Mutex mutex;

  LOG(INFO) << "Resetting all threads";

  ClientStats summary;
  pp->AwaitFiberOnAll([&](auto* p) {
    unique_lock lk(mutex);
    summary += client->stats;
    lk.unlock();
    client.reset();
  });

  unsigned dur_sec = duration / absl::Seconds(1);

  CONSOLE_INFO << "\nTotal time: " << duration
               << ". Overall number of requests: " << summary.num_responses
               << ", QPS: " << (dur_sec ? StrCat(summary.num_responses / dur_sec) : "nan")
               << ", P99 lat: " << summary.hist.Percentile(99) << "us";

  if (summary.num_errors) {
    CONSOLE_INFO << "Got " << summary.num_errors << " error responses!";
  }

  CONSOLE_INFO << "Latency summary, all times are in usec:\n" << summary.hist.ToString();
  if (summary.hit_opportunities) {
    CONSOLE_INFO << "----------------------------------\nHit rate: "
                 << 100 * double(summary.hit_count) / double(summary.hit_opportunities) << "%\n";
  }
  pp->Stop();

  return 0;
}
