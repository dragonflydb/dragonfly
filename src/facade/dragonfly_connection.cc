// Copyright 2026, DragonflyDB authors.  All rights reserved.
//
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_connection.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include <algorithm>
#include <numeric>
#include <variant>

#include "base/cycle_clock.h"
#include "base/flag_utils.h"
#include "base/flags.h"
#include "base/histogram.h"
#include "base/io_buf.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "common/heap_size.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_types.h"
#include "facade/memcache_parser.h"
#include "facade/op_status.h"
#include "facade/redis_parser.h"
#include "facade/reply_builder.h"
#include "facade/resp_srv_parser.h"
#include "facade/service_interface.h"
#include "facade/socket_utils.h"
#include "io/file.h"
#include "strings/human_readable.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"

#ifdef DFLY_USE_SSL
#include "util/tls/tls_socket.h"
#endif

#ifdef __linux__
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"
#include "util/fibers/uring_socket.h"
#endif

using namespace std;
using facade::operator""_MB;

ABSL_FLAG(bool, tcp_nodelay, true,
          "Configures dragonfly connections with socket option TCP_NODELAY");
ABSL_FLAG(bool, primary_port_http_enabled, true,
          "If true allows accessing http console on main TCP port");

ABSL_FLAG(uint16_t, admin_port, 0,
          "If set, would enable admin access to console on the assigned port. "
          "This supports both HTTP and RESP protocols");

ABSL_FLAG(string, admin_bind, "",
          "If set, the admin consol TCP connection would be bind the given address. "
          "This supports both HTTP and RESP protocols");

ABSL_FLAG(strings::MemoryBytesFlag, request_cache_limit, 64_MB,
          "Amount of memory to use for request cache in bytes - per IO thread.");

ABSL_FLAG(strings::MemoryBytesFlag, pipeline_buffer_limit, 128_MB,
          "Amount of memory to use for storing pipeline requests - per IO thread."
          "Please note that clients that send excecissively huge pipelines, "
          "may deadlock themselves. See https://github.com/dragonflydb/dragonfly/discussions/3997"
          "for details.");

ABSL_FLAG(uint32_t, pipeline_queue_limit, 10000,
          "Pipeline queue max length, the server will stop reading from the client socket"
          " once its pipeline queue crosses this limit, and will resume once it processes "
          "excessive requests. This is to prevent OOM states. Users of huge pipelines sizes "
          "may require increasing this limit to prevent the risk of deadlocking."
          "See https://github.com/dragonflydb/dragonfly/discussions/3997 for details");

ABSL_FLAG(strings::MemoryBytesFlag, publish_buffer_limit, 196_MB,
          "Amount of memory to use for storing pub commands in bytes - per IO thread. This is the "
          "soft Pub/Sub back-pressure limit; publishers are only parked once the per-thread "
          "subscriber memory reaches this value times the internal hard-limit multiplier.");

ABSL_FLAG(uint32_t, pubsub_slow_subscriber_timeout_ms, 0,
          "If a subscriber connection keeps a Pub/Sub socket write blocked for at least this many "
          "milliseconds while the per-IO-thread subscriber memory is above the soft "
          "publish_buffer_limit, Dragonfly closes that subscriber to release the back-pressure. "
          "0 disables this slow-subscriber protection. Startup-only, like publish_buffer_limit.");

ABSL_FLAG(uint32_t, pipeline_squash, 1,
          "Number of queued pipelined commands above which squashing is enabled, 0 means disabled");

// When changing this constant, also update `test_large_cmd` test in connection_test.py.
ABSL_FLAG(uint32_t, max_multi_bulk_len, 1u << 16,
          "Maximum multi-bulk (array) length that is "
          "allowed to be accepted when parsing RESP protocol");

ABSL_FLAG(uint64_t, max_bulk_len, 2u << 30,
          "Maximum bulk length that is "
          "allowed to be accepted when parsing RESP protocol");

ABSL_FLAG(strings::MemoryBytesFlag, max_client_iobuf_len, 1u << 16,
          "Maximum io buffer length that is used to read client requests.");

ABSL_FLAG(bool, migrate_connections, true,
          "When enabled, Dragonfly will try to migrate connections to the target thread on which "
          "they operate. Currently this is only supported for Lua script invocations, and can "
          "happen at most once per connection.");

ABSL_FLAG(uint32_t, max_busy_read_usec, 200,
          "Maximum time we read and parse from "
          "a socket without yielding. In microseconds.");

ABSL_FLAG(size_t, squashed_reply_size_limit, 0,
          "Max bytes allowed for squashing_current_reply_size. If this limit is reached, "
          "connections dispatching pipelines won't squash them.");

ABSL_FLAG(bool, always_flush_pipeline, false,
          "if true will flush pipeline response after each pipeline squashing");

ABSL_FLAG(uint32_t, async_dispatch_quota, 100,
          "Maximum number of consecutive async dispatch messages to process before either "
          "yielding to I/O when the pipeline appears empty or forcibly processing a queued "
          "pipelined command to prevent starvation. Set to 0 to disable this mechanism.");

ABSL_FLAG(uint32_t, pipeline_squash_limit, 1 << 30, "Limit on the size of a squashed pipeline. ");
ABSL_FLAG(uint32_t, pipeline_wait_batch_usec, 0,
          "If non-zero, waits for this time for more I/O "
          " events to come for the connection in case there is only one command in the pipeline. ");

ABSL_FLAG(
    bool, pipeline_prioritize_large_batches, true,
    "V2 only: in ParseLoop, defer executing a parsed pipeline batch and return to the read loop "
    "to accumulate more already-available input, so the squasher sees one large batch instead of "
    "many small ones.");

ABSL_FLAG(bool, pipeline_parse_in_proactor, true,
          "V2 only: parse newly-arrived bytes from the proactor OnRecv callback while the fiber "
          "is blocked in a squash hop, so the next batch is already grown when execution resumes.");

ABSL_FLAG(bool, enable_memcache_io_loop_v2, true,
          "Enable the event-driven IoLoopV2 for non-TLS Memcache connections.");
ABSL_FLAG(bool, enable_resp_io_loop_v2, false,
          "Enable the event-driven IoLoopV2 for non-TLS RESP connections.");
ABSL_FLAG(bool, enable_pipeline_squashing_v2, true,
          "Enable vectorized pipeline squashing for the V2 dispatch loop. Groups consecutive "
          "single-shard pipeline commands by shard and executes them in parallel.");
ABSL_RETIRED_FLAG(bool, experimental_io_loop_v2, true, "retired.");

using namespace util;
using namespace std;
using absl::GetFlag;
using base::CycleClock;
using nonstd::make_unexpected;

#define CONN_ID "[" << id_ << "] "

namespace facade {

namespace {

// Multiplier applied to the soft publish_buffer_limit to derive the hard Pub/Sub back-pressure
// limit. Publishers are only parked in EnsureMemoryBudget once the per-thread subscriber memory
// reaches soft_limit * kPubSubHardMultiplier.
constexpr uint32_t kPubSubHardMultiplier = 4;

void SendProtocolError(RespSrvParser::Result pres, SinkReplyBuilder* builder) {
  constexpr string_view res = "-ERR Protocol error: "sv;
  if (pres == RespSrvParser::BAD_BULKLEN) {
    builder->SendProtocolError(absl::StrCat(res, "invalid bulk length"));
  } else if (pres == RespSrvParser::BAD_ARRAYLEN) {
    builder->SendProtocolError(absl::StrCat(res, "invalid multibulk length"));
  } else {
    builder->SendProtocolError(absl::StrCat(res, "parse error"));
  }
}

// TODO: to implement correct matcher according to HTTP spec
// https://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html
// One place to find a good implementation would be https://github.com/h2o/picohttpparser
bool MatchHttp11Line(string_view line) {
  return (absl::StartsWith(line, "GET ") || absl::StartsWith(line, "POST ")) &&
         absl::EndsWith(line, "HTTP/1.1");
}

struct ReadBufTracker {
  explicit ReadBufTracker(const io::IoBuf& io_buf, uint32_t conn_id)
      : io_buf_(io_buf), last_capacity_(io_buf.Capacity()), id_(conn_id) {
  }

  ~ReadBufTracker() {
    size_t capacity = io_buf_.Capacity();
    if (last_capacity_ != capacity) {
      VLOG(2) << CONN_ID << "Grown io_buf to " << capacity;
      tl_facade_stats->conn_stats.read_buf_capacity += capacity - last_capacity_;
    }
  }

 private:
  const io::IoBuf& io_buf_;
  size_t last_capacity_;
  uint32_t id_;
};

struct ConnectionMemoryTracker {
  explicit ConnectionMemoryTracker(Connection* conn) : conn_(conn) {
  }

  ConnectionMemoryTracker(const ConnectionMemoryTracker&) = delete;
  ConnectionMemoryTracker& operator=(const ConnectionMemoryTracker&) = delete;

  ~ConnectionMemoryTracker() {
    conn_->RefreshConnectionMemoryUsage();
  }

 private:
  Connection* conn_;
};

size_t UsedMemoryInternal(const ParsedCommand& msg) {
  return msg.GetSize() + msg.HeapMemory();
}

struct TrafficLogger {
  // protects agains closing the file while writing or data races when opening the file.
  // Also, makes sure that LogTraffic are executed atomically.
  fb2::Mutex mutex;
  unique_ptr<io::WriteFile> log_file;
  // Listener type that this thread's file is recording. Only connections with a
  // matching `listener_type_` produce records; others are skipped on the hot path.
  // Set once when the file is opened, cleared in ResetLocked().
  Connection::ListenerType listener_type = Connection::ListenerType::MAIN_RESP;

  void ResetLocked();
  // Returns true if Write succeeded, false if it failed and the recording should be aborted.
  bool Write(string_view blob);
  bool Write(iovec* blobs, size_t len);
};

void TrafficLogger::ResetLocked() {
  if (log_file) {
    std::ignore = log_file->Close();
    log_file.reset();
  }
  listener_type = Connection::ListenerType::MAIN_RESP;
}

// Returns true if Write succeeded, false if it failed and the recording should be aborted.
bool TrafficLogger::Write(string_view blob) {
  auto ec = log_file->Write(io::Buffer(blob));
  if (ec) {
    LOG(ERROR) << "Error writing to traffic log: " << ec;
    ResetLocked();
    return false;
  }
  return true;
}

bool TrafficLogger::Write(iovec* blobs, size_t len) {
  auto ec = log_file->Write(blobs, len);
  if (ec) {
    LOG(ERROR) << "Error writing to traffic log: " << ec;
    ResetLocked();
    return false;
  }
  return true;
}

thread_local TrafficLogger tl_traffic_logger{};
thread_local base::Histogram* io_req_size_hist = nullptr;

thread_local const size_t reply_size_limit = absl::GetFlag(FLAGS_squashed_reply_size_limit);
thread_local uint32 pipeline_wait_batch_usec = absl::GetFlag(FLAGS_pipeline_wait_batch_usec);

// Opens the per-thread traffic log file. Distinguishes three outcomes so the caller
// can report an accurate error to the user (was the logger already running, or did
// we fail to open a file). `listener_type` is only committed after the file is
// successfully opened so the logger's state stays consistent on failure.
Connection::StartTrafficResult OpenTrafficLogger(string_view base_path,
                                                 Connection::ListenerType listener_type) {
  using Res = Connection::StartTrafficResult;
  unique_lock lk{tl_traffic_logger.mutex};
  if (tl_traffic_logger.log_file)
    return Res::kAlreadyLogging;

#ifdef __linux__
  // Open file with append mode, without it concurrent fiber writes seem to conflict
  string path = absl::StrCat(
      base_path, "-", absl::Dec(ProactorBase::me()->GetPoolIndex(), absl::kZeroPad3), ".bin");
  auto file = util::fb2::OpenWrite(path, io::WriteFile::Options{/*.append = */ false});
  if (!file) {
    LOG(ERROR) << "Error opening a file " << path << " for traffic logging: " << file.error();
    return Res::kOpenFailed;
  }
  tl_traffic_logger.log_file = unique_ptr<io::WriteFile>{file.value()};
  tl_traffic_logger.listener_type = listener_type;
#else
  LOG(WARNING) << "Traffic logger is only supported on Linux";
  return Res::kOpenFailed;
#endif

  // File header: version byte (v3), followed by a single byte carrying the listener
  // type for the whole file. Every record in the file belongs to this listener.
  uint8_t header[2] = {3, static_cast<uint8_t>(listener_type)};
  std::ignore = tl_traffic_logger.log_file->Write(header);
  return Res::kStarted;
}

// Writes a single record. `parts[0]` is the command name, following entries are its arguments.
// Callers must guarantee a non-empty span (both LogTraffic and LogMemcacheTraffic push
// the command name as the first element before invoking this function).
void LogTrafficParts(uint32_t id, bool has_more, uint32_t db_index,
                     absl::Span<const string_view> parts) {
  DCHECK(!parts.empty());

  string_view cmd = parts.front();
  if (absl::EqualsIgnoreCase(cmd, "debug"sv))
    return;

  DVLOG(2) << "Recording " << cmd;

  char stack_buf[1024];
  char* next = stack_buf;

  // Record header: id, timestamp, db_index, has_more, num_parts, followed by
  // part_len, part_len, ... and finally the concatenated part blobs.
  // The listener type is stored once in the file header; it is not repeated per record.
  auto write_u32 = [&next](uint32_t i) {
    absl::little_endian::Store32(next, i);
    next += 4;
  };

  write_u32(id);

  absl::little_endian::Store64(next, absl::GetCurrentTimeNanos());
  next += 8;

  write_u32(db_index);
  write_u32(has_more ? 1u : 0u);
  write_u32(uint32_t(parts.size()));

  // Grab the lock and check if the file is still open.
  lock_guard lk{tl_traffic_logger.mutex};
  if (!tl_traffic_logger.log_file)
    return;

  // part_len, ...
  for (string_view part : parts) {
    if (size_t(next - stack_buf + 4) > sizeof(stack_buf)) {
      if (!tl_traffic_logger.Write(string_view{stack_buf, size_t(next - stack_buf)})) {
        return;
      }
      next = stack_buf;
    }
    write_u32(part.size());
  }

  // Write the data itself.
  array<iovec, 16> blobs;
  unsigned index = 0;
  if (next != stack_buf) {
    blobs[index++] = iovec{.iov_base = stack_buf, .iov_len = size_t(next - stack_buf)};
  }

  for (string_view part : parts) {
    if (auto blob_len = part.size(); blob_len > 0) {
      blobs[index++] = iovec{.iov_base = const_cast<char*>(part.data()), .iov_len = blob_len};

      if (index >= blobs.size()) {
        if (!tl_traffic_logger.Write(blobs.data(), blobs.size())) {
          return;
        }
        index = 0;
      }
    }
  }

  if (index) {
    tl_traffic_logger.Write(blobs.data(), index);
  }
}

void LogTraffic(uint32_t id, bool has_more, const cmn::BackedArguments& args,
                ServiceInterface::ContextInfo ci) {
  absl::InlinedVector<string_view, 16> parts;
  parts.reserve(args.size());
  for (auto v : args.view())
    parts.push_back(v);
  LogTrafficParts(id, has_more, ci.db_index, absl::MakeSpan(parts));
}

// Variant used by the Memcache protocol path.
//
// The memcache parser keeps fields that are NOT arguments in scalar Command members
// (flags, expire_ts, delta, cas_unique) rather than in `backed_args`. We serialize
// them into the record so that tools/replay has enough context to reproduce the
// command faithfully. Record layout per command type:
//
//   SET/ADD/REPLACE/APPEND/PREPEND : [cmd, key, value, flags, expire_ts]
//   CAS                            : [cas, key, value, flags, expire_ts, cas_unique]
//   INCR/DECR                      : [cmd, key, delta]
//   GAT/GATS                       : [cmd, expire_ts, key+]  (expire BEFORE keys, matches wire)
//   all others (GET/GETS/DELETE/
//               FLUSHALL/STATS/
//               QUIT/VERSION)      : [cmd, *backed_args]
void LogMemcacheTraffic(uint32_t id, bool has_more, const MemcacheParser::Command& mc,
                        unsigned db_index) {
  using MP = MemcacheParser;
  string_view cmd_name = MP::CmdName(mc.type);
  if (cmd_name.empty())
    return;

  // owned backs stringified numeric fields. We use a fixed-size std::array
  // rather than a resizable vector so that string_views inserted into `parts`
  // remain stable even if more fields are appended in the future: std::array
  // never reallocates. kMaxOwned must be >= the largest per-type push count
  // (currently 3, for CAS: flags + expire_ts + cas_unique).
  constexpr size_t kMaxOwned = 4;
  std::array<string, kMaxOwned> owned;
  size_t owned_n = 0;

  absl::InlinedVector<string_view, 16> parts;
  parts.reserve(mc.backed_args->size() + kMaxOwned + 1);
  parts.push_back(cmd_name);

  auto push_num = [&](uint64_t n) {
    DCHECK_LT(owned_n, kMaxOwned);
    owned[owned_n] = absl::StrCat(n);
    parts.push_back(owned[owned_n]);
    ++owned_n;
  };

  // For GAT/GATS we want expire_ts to precede the key list because the parser can
  // push multiple keys into backed_args; placing expire at the end would make the
  // expire index depend on the number of keys.
  if (mc.type == MP::GAT || mc.type == MP::GATS)
    push_num(mc.raw_expire_ts);

  for (string_view a : mc.backed_args->view())
    parts.push_back(a);

  switch (mc.type) {
    case MP::SET:
    case MP::ADD:
    case MP::REPLACE:
    case MP::APPEND:
    case MP::PREPEND:
      push_num(mc.flags);
      push_num(mc.raw_expire_ts);
      break;
    case MP::CAS:
      push_num(mc.flags);
      push_num(mc.raw_expire_ts);
      push_num(mc.cas_unique);
      break;
    case MP::INCR:
    case MP::DECR:
      push_num(mc.delta);
      break;
    default:
      break;
  }

  LogTrafficParts(id, has_more, db_index, absl::MakeSpan(parts));
}

constexpr size_t kMinReadSize = 256;

const char* kPhaseName[Connection::NUM_PHASES] = {"SETUP", "READ", "PROCESS", "SHUTTING_DOWN",
                                                  "PRECLOSE"};

// Keeps track of total per-thread sizes of dispatch queues to limit memory taken up by messages
// in these queues.
struct QueueBackpressure {
  QueueBackpressure() {
  }

  // Block until subscriber memory usage is below the hard limit, can be called from any thread.
  void EnsureBelowLimit();

  // Hard Pub/Sub back-pressure limit (soft publish_buffer_limit times kPubSubHardMultiplier).
  // EnsureBelowLimit only parks publishers once subscriber memory reaches this value.
  size_t PubSubHardLimit() const {
    return publish_buffer_limit * kPubSubHardMultiplier;
  }

  // True while the per-thread subscriber memory is above the soft publish_buffer_limit, i.e. a
  // Pub/Sub back-pressure episode is in progress. Read on the owning I/O thread.
  bool IsSubscriberSoftLimited() const {
    return subscriber_bytes.load(memory_order_relaxed) > publish_buffer_limit;
  }

  // Accounts `mem` subscriber bytes on this thread and, on an at/below-soft -> above-soft
  // transition, counts one soft_limit_crossing event. Called only on the owning I/O thread.
  void AddSubscriberBytes(size_t mem);

  // Releases `mem` subscriber bytes on this thread. Called only on the owning I/O thread.
  void SubSubscriberBytes(size_t mem);

  // Checks if backpressure should be applied.
  // 'size' should be the total bytes currently consumed by all connections on this thread.
  // 'q_len' should be the length of the pipeline queue for the current connection.
  //
  // Returns true if EITHER:
  // 1. Thread-local: memory limit (on all thread's connections) is exceeded (protects server from
  // OOM).
  // 2. Per-Connection queue length limit is exceeded (protects against single-client abuse).
  bool IsPipelineBufferOverLimit(size_t size, uint32_t q_len) const {
    return size >= (pipeline_buffer_limit) || (q_len > pipeline_queue_max_len);
  }

  // Checks if usage has dropped below the limit in at least one criteria.
  // Used to determine if we should notify waiters.
  // 'size' should be the total bytes currently consumed by all connections on this thread.
  // 'q_len' should be the length of the pipeline queue for the current connection.
  //
  // Returns true if EITHER:
  // 1. Thread-Global memory is now under the limit (allows neighbors to wake up).
  // 2. Per-Connection queue length is now within the limit (allows self to wake up).
  bool IsPipelineBufferUnderLimit(size_t size, uint32_t q_len) const {
    return (size < pipeline_buffer_limit) || (q_len <= pipeline_queue_max_len);
  }

  // Used by publisher/subscriber actors to make sure we do not publish too many messages
  // into the queue. Thread-safe to allow safe access in EnsureBelowLimit.
  util::fb2::EventCount pubsub_ec;
  atomic_size_t subscriber_bytes = 0;

  // Used by pipelining/execution fiber to throttle the incoming pipeline messages.
  // Used together with pipeline_buffer_limit to limit the pipeline usage per thread.
  util::fb2::CondVarAny pipeline_cnd;

  // V2 connections subscribe to this EventCount when parked on backpressure.
  // When global memory is freed, notifyAll() calls for each subscriber's callback,
  // which in turn calls io_event_.notify() to wake the V2 fiber.
  // Registration and unregistration are O(1) via intrusive linked list.
  util::fb2::EventCount v2_pipeline_backpressure_ec;

  // Notifies both V1 waiters (pipeline_cnd) and V2 subscribers (v2_pipeline_backpressure_ec).
  void NotifyPipelineWaiters() {
    pipeline_cnd.notify_all();
    v2_pipeline_backpressure_ec.notifyAll();
  }

  size_t publish_buffer_limit = 0;        // cached flag publish_buffer_limit
  size_t pipeline_cache_limit = 0;        // cached flag pipeline_cache_limit
  size_t pipeline_buffer_limit = 0;       // cached flag for buffer size in bytes
  uint32_t pipeline_queue_max_len = 256;  // cached flag for pipeline queue max length.
};

void QueueBackpressure::EnsureBelowLimit() {
  const size_t hard_limit = PubSubHardLimit();
  // Fast path: below the hard red line, publishers proceed without parking (they only get
  // throttled by the soft limit via the slow-subscriber protection policy, not here).
  if (subscriber_bytes.load(memory_order_relaxed) <= hard_limit)
    return;

  // We are about to park a publisher - count one hard-limit throttle episode. Incremented on the
  // publisher's own thread-local stats, so it is safe even though this qbp belongs to another
  // thread.
  ++tl_facade_stats->conn_stats.pubsub_backpressure.hard_limit_throttled;
  pubsub_ec.await([&] { return subscriber_bytes.load(memory_order_relaxed) <= hard_limit; });
}

void QueueBackpressure::AddSubscriberBytes(size_t mem) {
  // subscriber_bytes is only written by the owning I/O thread, so the fetch_add result is the true
  // previous value. Count one soft_limit_crossing on the at/below-soft -> above-soft transition.
  size_t before = subscriber_bytes.fetch_add(mem, memory_order_relaxed);
  if (before <= publish_buffer_limit && before + mem > publish_buffer_limit)
    ++tl_facade_stats->conn_stats.pubsub_backpressure.soft_limit_crossing;
}

void QueueBackpressure::SubSubscriberBytes(size_t mem) {
  DCHECK_GE(subscriber_bytes.load(memory_order_relaxed), mem);
  subscriber_bytes.fetch_sub(mem, memory_order_relaxed);
}

// Global array for each io thread to keep track of the total memory usage of the dispatch queues.
QueueBackpressure* thread_queue_backpressure = nullptr;

QueueBackpressure& GetQueueBackpressure() {
  DCHECK(thread_queue_backpressure != nullptr);

  return thread_queue_backpressure[ProactorBase::me()->GetPoolIndex()];
}

// A special accessor for accessing thread local ConnectionStats that is robust to fiber-thread
// migrations. Compiler optimizations can cache a stale thread local pointer, and not refresh it
// after HandleMigrateRequest() is called. This function should be used to force loading
// the variable from memory every time, preventing such bugs.
ConnectionStats& __attribute__((noinline)) GetLocalConnStats() {
  // https://stackoverflow.com/a/75622732
  asm volatile("");

  return tl_facade_stats->conn_stats;
}

thread_local uint32_t max_busy_read_cycles_cached = UINT32_MAX;
thread_local bool always_flush_pipeline_cached = absl::GetFlag(FLAGS_always_flush_pipeline);
thread_local uint32_t pipeline_squash_limit_cached = absl::GetFlag(FLAGS_pipeline_squash_limit);
thread_local bool pipeline_prioritize_large_batches_cached =
    absl::GetFlag(FLAGS_pipeline_prioritize_large_batches);
thread_local bool pipeline_parse_in_proactor_cached =
    absl::GetFlag(FLAGS_pipeline_parse_in_proactor);

// Cached deadline (in CycleClock cycles) after which a continuously blocked Pub/Sub send makes a
// subscriber eligible for the slow-subscriber protection close. 0 means the protection is disabled.
// Derived from the startup-only --pubsub_slow_subscriber_timeout_ms flag.
thread_local uint64_t pubsub_slow_subscriber_timeout_cycles_cached = base::CycleClock::FromUsec(
    uint64_t(absl::GetFlag(FLAGS_pubsub_slow_subscriber_timeout_ms)) * 1000);

}  // namespace

thread_local vector<Connection::PipelineMessagePtr> Connection::pipeline_req_pool_;

class PipelineCacheSizeTracker {
 public:
  bool CheckAndUpdateWatermark(size_t pipeline_sz) {
    const auto now = absl::Now();
    const auto elapsed = now - last_check_;
    min_ = std::min(min_, pipeline_sz);
    if (elapsed < absl::Milliseconds(10)) {
      return false;
    }

    const bool watermark_reached = (min_ > 0);
    min_ = Limits::max();
    last_check_ = absl::Now();

    return watermark_reached;
  }

 private:
  using Limits = std::numeric_limits<size_t>;

  absl::Time last_check_ = absl::Now();
  size_t min_ = Limits::max();
};

thread_local PipelineCacheSizeTracker tl_pipe_cache_sz_tracker;

size_t Connection::MessageHandle::UsedMemory() const {
  struct MessageSize {
    size_t operator()(const PubMessagePtr& msg) {
      return sizeof(PubMessage) + (msg->channel.size() + msg->message.size());
    }
    size_t operator()(const MonitorMessage& msg) {
      return msg.capacity();
    }
    size_t operator()(const MigrationRequestMessage& msg) {
      return 0;
    }
    size_t operator()(const CheckpointMessage& msg) {
      return 0;  // no access to internal type, memory usage negligible
    }
    size_t operator()(const InvalidationMessage& msg) {
      return 0;
    }
  };

  return sizeof(MessageHandle) + visit(MessageSize{}, this->handle);
}

bool Connection::MessageHandle::IsReplying() const {
  return IsPubMsg() || holds_alternative<MonitorMessage>(handle);
}

struct Connection::AsyncOperations {
  AsyncOperations(SinkReplyBuilder* b, Connection* me) : builder{b}, self(me) {
  }

  void operator()(const PubMessage& msg);
  void operator()(ParsedCommand& msg);
  void operator()(const MonitorMessage& msg);
  void operator()(const MigrationRequestMessage& msg);
  void operator()(CheckpointMessage msg);
  void operator()(const InvalidationMessage& msg);

  template <typename T, typename D> void operator()(unique_ptr<T, D>& ptr) {
    operator()(*ptr.get());
  }

  SinkReplyBuilder* builder = nullptr;
  Connection* self = nullptr;
};

void Connection::AsyncOperations::operator()(const MonitorMessage& msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  rbuilder->SendSimpleString(msg);
}

void Connection::AsyncOperations::operator()(const PubMessage& pub_msg) {
  RedisReplyBuilder* rb = static_cast<RedisReplyBuilder*>(builder);

  // Discard stale messages to not break the protocol after exiting "pubsub" mode.
  // Even after removing all subscriptions, we still can receive messages delayed
  // by inter-thread dispatches or backpressure.
  // TODO: filter messages from channels the client unsubscribed from
  if (self->cntx()->subscriptions == 0 &&
      !base::_in(pub_msg.channel, {"unsubscribe", "punsubscribe"}))
    return;

  if (pub_msg.force_unsubscribe) {
    rb->StartCollection(3, CollectionType::PUSH);
    rb->SendBulkString("sunsubscribe");
    rb->SendBulkString(pub_msg.channel);
    rb->SendLong(0);
    self->cntx()->Unsubscribe(pub_msg.channel);
    return;
  }

  unsigned i = 0;
  array<string_view, 4> arr;
  if (pub_msg.pattern.empty()) {
    arr[i++] = pub_msg.is_sharded ? "smessage" : "message";
  } else {
    arr[i++] = "pmessage";
    arr[i++] = pub_msg.pattern;
  }

  arr[i++] = pub_msg.channel;
  arr[i++] = pub_msg.message;

  rb->SendBulkStrArr(absl::Span<string_view>{arr.data(), i}, CollectionType::PUSH);
}

void Connection::AsyncOperations::operator()(ParsedCommand& cmd) {
  DVLOG(2) << "[" << self->id_ << "] "
           << "Dispatching pipeline: " << cmd.Front();

  ++self->local_stats_.cmds;
  self->service_->DispatchCommand(ParsedArgs{cmd}, &cmd, facade::AsyncPreference::ONLY_SYNC);

  self->last_interaction_ = time(nullptr);
  self->skip_next_squashing_ = false;
}

void Connection::AsyncOperations::operator()(const MigrationRequestMessage& msg) {
  // no-op
}

void Connection::AsyncOperations::operator()(CheckpointMessage msg) {
  // V2 only:
  // - ExecuteBatch() -> DispatchCommandSimple() returns while the command may still be in flight.
  // - A checkpoint that lands in that window must NOT Dec() yet - otherwise a CLIENT PAUSE /
  // DispatchTracker would return before the in-flight write actually happens.
  // - That's why here we defer the blocking counter by holding it. We will release it once
  // HasInFlightCommands() is false, by calling ReleaseDeferredCheckpoints().
  // - A blocked connection is excluded: a blocked command isn't an in-flight write and may never
  // complete, so deferring its checkpoint could hold the DispatchTracker forever. Instead, we Dec()
  // it immediately.
  if (self->ioloop_v2_ && self->HasInFlightCommands() && !self->cc_->blocked) {
    DVLOG(2) << "[" << self->id_ << "] "
             << "Deferring checkpoint (in-flight commands) at " << self->DebugInfo();
    self->deferred_checkpoints_.push_back(std::move(msg.bc));
    return;
  }

  msg.bc->Dec();
}

void Connection::AsyncOperations::operator()(const InvalidationMessage& msg) {
  RedisReplyBuilder* rbuilder = (RedisReplyBuilder*)builder;
  // Invalidations are client-side-caching pushes that only exist under RESP3 with tracking on.
  // A message can still be queued when the connection leaves that state - most notably RESET, which
  // switches the connection back to RESP2 and disables tracking. Emitting a PUSH now would break
  // the RESP2 protocol, so drop stale invalidations instead (mirrors the stale PubMessage
  // handling).
  if (!rbuilder->IsResp3())
    return;
  rbuilder->StartCollection(2, facade::CollectionType::PUSH);
  rbuilder->SendBulkString("invalidate");
  if (msg.invalidate_due_to_flush) {
    rbuilder->SendNull();
  } else {
    string_view keys[] = {msg.key};
    rbuilder->SendBulkStrArr(keys);
  }
}

namespace {
thread_local absl::flat_hash_map<string, uint64_t> g_libname_ver_map;

void UpdateLibNameVerMap(const string& name, const string& ver, int delta) {
  string key = absl::StrCat(name, ":", ver);
  uint64_t& val = g_libname_ver_map[key];
  val += delta;
  if (val == 0) {
    g_libname_ver_map.erase(key);
  }
}
}  // namespace

void Connection::Init(unsigned io_threads) {
  CHECK(thread_queue_backpressure == nullptr);
  thread_queue_backpressure = new QueueBackpressure[io_threads];

  for (unsigned i = 0; i < io_threads; ++i) {
    auto& qbp = thread_queue_backpressure[i];
    qbp.publish_buffer_limit = GetFlag(FLAGS_publish_buffer_limit);
    qbp.pipeline_cache_limit = GetFlag(FLAGS_request_cache_limit);
    qbp.pipeline_buffer_limit = GetFlag(FLAGS_pipeline_buffer_limit);
    qbp.pipeline_queue_max_len = GetFlag(FLAGS_pipeline_queue_limit);

    if (qbp.publish_buffer_limit == 0 || qbp.pipeline_cache_limit == 0 ||
        qbp.pipeline_buffer_limit == 0 || qbp.pipeline_queue_max_len == 0) {
      LOG(ERROR) << "pipeline flag limit is 0";
      exit(-1);
    }
  }
}

void Connection::Shutdown() {
  delete[] thread_queue_backpressure;
  thread_queue_backpressure = nullptr;
}

Connection::Connection(Protocol protocol, util::HttpListenerBase* http_listener, SSL_CTX* ctx,
                       ServiceInterface* service)
    : io_buf_(kMinReadSize),
      protocol_(protocol),
      http_listener_(http_listener),
      ssl_ctx_(ctx),
      service_(service),
      flags_(0) {
  // TODO: to move parser initialization to where we initialize the reply builder.
  switch (protocol) {
    case Protocol::REDIS:
      redis_parser_.reset(
          new RespSrvParser(GetFlag(FLAGS_max_multi_bulk_len), GetFlag(FLAGS_max_bulk_len)));
      break;
    case Protocol::MEMCACHE:
      memcache_parser_ =
          make_unique<MemcacheParser>(std::min<uint64_t>(GetFlag(FLAGS_max_bulk_len), UINT32_MAX));
      break;
  }

  creation_time_ = time(nullptr);
  last_interaction_ = creation_time_;
  id_ = NextClientId();

  migration_enabled_ = GetFlag(FLAGS_migrate_connections);

  // Create shared_ptr with empty value and associate it with `this` pointer (aliasing constructor).
  // We use it for reference counting and accessing `this` (without managing it).
  self_ = {make_shared<std::monostate>(), this};

#ifdef DFLY_USE_SSL
  // Increment reference counter so Listener won't free the context while we're
  // still using it.
  if (ctx) {
    SSL_CTX_up_ref(ctx);
  }
#endif

  UpdateLibNameVerMap(lib_name_, lib_ver_, +1);
  migration_allowed_to_register_ = false;
}

Connection::~Connection() {
#ifdef DFLY_USE_SSL
  SSL_CTX_free(ssl_ctx_);
#endif
  UpdateLibNameVerMap(lib_name_, lib_ver_, -1);
}

bool Connection::IsSending() const {
  return reply_builder_ && reply_builder_->IsSendActive();
}

void Connection::MarkForClose() {
  if (reply_builder_) {
    reply_builder_->CloseConnection();
  }
  request_shutdown_ = true;
}

// Called from Connection::Shutdown() right after socket_->Shutdown call.
void Connection::OnShutdown() {
  VLOG(1) << CONN_ID << "Connection::OnShutdown";

  BreakOnce(POLLHUP);
  io_ec_ = make_error_code(errc::connection_aborted);
  io_event_.notify();
}

void Connection::OnPreMigrateThread() {
  DVLOG(1) << CONN_ID << "OnPreMigrateThread " << GetClientId();

  CHECK(!cc_->conn_closing);

  DCHECK(!migration_in_process_);

  // CancelOnErrorCb is a preemption point, so we make sure the Migration start
  // is marked beforehand.
  migration_in_process_ = true;

  // Mark as not owned by any thread as it going through the dark hole
  self_.reset();

  socket_->CancelOnErrorCb();
  DCHECK(!async_fb_.IsJoinable()) << GetClientId();

  DecreaseConnStats();
}

void Connection::OnPostMigrateThread() {
  DVLOG(1) << CONN_ID << "OnPostMigrateThread";

  // Once we migrated, we should rearm OnBreakCb callback.
  if (socket()->IsOpen()) {
    socket_->RegisterOnErrorCb([this](int32_t mask) { this->OnBreakCb(mask); });
  }

  if (ioloop_v2_ && socket_ && socket_->IsOpen() && migration_allowed_to_register_) {
    MaybeEnableRecvMultishot();
    socket_->RegisterOnRecv(
        [this](const FiberSocketBase::RecvNotification& n) { OnRecvNotification(n); });
  }

  migration_in_process_ = false;
  self_ = {make_shared<std::monostate>(), this};  // Recreate shared_ptr to self.
  DCHECK(!async_fb_.IsJoinable());

  // If someone had sent Async during the migration, we must create async_fb_.
  if (HasPendingMessages() && !ioloop_v2_) {
    LaunchAsyncFiberIfNeeded();
  }

  IncreaseConnStats();
}

void Connection::OnConnectionStart() {
  SetName(absl::StrCat(id_));

  // is null in unit-tests.
  if (const Listener* lsnr = static_cast<Listener*>(listener()); lsnr) {
    is_main_ = lsnr->IsMainInterface();
    if (lsnr->IsPrivilegedInterface()) {
      listener_type_ = ListenerType::ADMIN_RESP;
    } else if (protocol_ == Protocol::MEMCACHE) {
      listener_type_ = ListenerType::MEMCACHE;
    } else {
      // MAIN_RESP covers TCP main listener as well as unix-socket RESP listeners.
      listener_type_ = ListenerType::MAIN_RESP;
    }
  }

  if (GetFlag(FLAGS_tcp_nodelay) && !socket_->IsUDS()) {
    int val = 1;
    int res = setsockopt(socket_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    DCHECK_EQ(res, 0);
  }
}

void Connection::HandleRequests() {
  VLOG(1) << CONN_ID << "HandleRequests";
  DCHECK(tl_facade_stats);
  auto& conn_stats = tl_facade_stats->conn_stats;

  auto remote_ep = RemoteEndpointStr();

#ifdef DFLY_USE_SSL
  if (ssl_ctx_) {
    // Early TLS connection filter
    //
    // Before entering the expensive OpenSSL handshake we pre-read the 5-byte TLS Record Layer
    // header on the raw TCP socket. This serves two purposes:
    //
    //  1. Wrong-client detection:
    //     Clients that forgot to enable TLS (e.g. a plaintext Redis client connecting to the TLS
    //     port) will not send a valid TLS Record Layer header.  We detect this immediately and
    //     reply with a human-readable "-ERR" message before disconnecting, instead of letting
    //     OpenSSL produce a cryptic handshake failure.
    //
    //  2. Zombie-connection rejection:
    //     Zombie connections —— open a TCP socket but never send any data.  By demanding at least
    //     the 5-byte header before allocating any SSL state, we drop these cheaply on the raw
    //     socket instead of tying up an OpenSSL context and handshake state machine that will never
    //     complete.
    //
    // The pre-read header bytes are injected into the TlsSocket via InitSSL(), which writes them
    // into OpenSSL's internal BIO so that Accept() can drive the normal handshake from there.
    //
    // Reminder: TLS Record Layer header structure (universal across TLS 1.0 – 1.3):
    // - Byte 0: ContentType (0x16 = Handshake)
    // - Bytes 1–2: ProtocolVersion. While the minor version varies (0x01 for TLS 1.0,
    //   0x03 for TLS 1.2/1.3), the major version is consistently 0x03 for all
    //   modern TLS versions.
    // - Bytes 3–4: Length (uint16 BE) — payload length, max 2^14 = 16384
    uint8_t buf[5];  // universal TLS Record Header size is 5 bytes
    auto read_sz = socket_->Read(io::MutableBytes(buf));
    if (!read_sz || *read_sz < sizeof(buf)) {
      auto msg = read_sz ? absl::StrCat(*read_sz, " < ", sizeof(buf)) : read_sz.error().message();
      LOG_EVERY_T(INFO, 1) << CONN_ID << "Error reading from peer " << remote_ep << " " << msg
                           << ", socket state: " + dfly::GetSocketInfo(socket_->native_handle());
      conn_stats.tls_accept_disconnects++;
      return;
    }

    // Byte 0: ContentType must be 0x16 (Handshake).
    // Byte 1: major ProtocolVersion — always 0x03 for TLS 1.0 through TLS 1.3.
    // Byte 2: minor ProtocolVersion — 0x01 (TLS 1.0), 0x02 (TLS 1.1), 0x03 (TLS 1.2/1.3).
    //         SSL 3.0 (0x00) is deprecated (RFC 7568) and rejected.
    if ((buf[0] != 0x16) || (buf[1] != 0x03) || (buf[2] < 0x01) || (buf[2] > 0x03)) {
      VLOG(1) << CONN_ID << "Bad TLS header "
              << absl::StrCat(absl::Hex(buf[0], absl::kZeroPad2),
                              absl::Hex(buf[1], absl::kZeroPad2),
                              absl::Hex(buf[2], absl::kZeroPad2));
      std::ignore =
          socket_->Write(io::Buffer("-ERR Bad TLS header, double check "
                                    "if you enabled TLS for your client.\r\n"));
      conn_stats.tls_accept_disconnects++;
      return;
    }

    // Must be done atomically before the preemption point in Accept so that at any
    // point in time, the socket_ is defined.
    {
      FiberAtomicGuard fg;
      unique_ptr<tls::TlsSocket> tls_sock = make_unique<tls::TlsSocket>(std::move(socket_));
      tls_sock->InitSSL(ssl_ctx_, buf);
      SetSocket(tls_sock.release());
    }
    FiberSocketBase::AcceptResult aresult = socket_->Accept();

    if (!aresult) {
      // This can flood the logs -- don't change
      LOG_EVERY_T(INFO, 1) << CONN_ID << "Error handshaking " << aresult.error().message()
                           << ", socket state: " + dfly::GetSocketInfo(socket_->native_handle());
      conn_stats.tls_accept_disconnects++;
      return;
    }
    is_tls_ = 1;
    VLOG(1) << CONN_ID << "TLS handshake succeeded";
  }
#endif

  io::Result<bool> http_res{false};

  http_res = CheckForHttpProto();

  // We need to check if the socket is open because the server might be
  // shutting down. During the shutdown process, the server iterates over
  // the connections of each shard and shuts down their socket. Since the
  // main listener dispatches the connection into the next proactor, we
  // allow a schedule order that first shuts down the socket and then calls
  // this function which triggers a DCHECK on the socket while it tries to
  // RegisterOnErrorCb. Furthermore, we can get away with one check here
  // because both Write and Recv internally check if the socket was shut
  // down and return with an error accordingly.
  if (http_res && socket_->IsOpen()) {
    cc_.reset(service_->CreateContext(this));

    if (*http_res) {
      VLOG(1) << CONN_ID << "HTTP1.1 identified";
      is_http_ = true;
      HttpConnection http_conn{http_listener_};
      http_conn.SetSocket(socket_.get());
      http_conn.set_user_data(cc_.get());

      // We validate the http request using basic-auth inside HttpConnection::HandleSingleRequest.
      cc_->authenticated = true;
      auto ec = http_conn.ParseFromBuffer(io_buf_.InputBuffer());
      io_buf_.ConsumeInput(io_buf_.InputLen());
      if (!ec) {
        http_conn.HandleRequests();
      }

      // Release the ownership of the socket from http_conn so it would stay with
      // this connection.
      http_conn.ReleaseSocket();
    } else {  // non-http
      ioloop_v2_ =
          !is_tls_ &&
          ((protocol_ == Protocol::MEMCACHE && GetFlag(FLAGS_enable_memcache_io_loop_v2)) ||
           (protocol_ == Protocol::REDIS && GetFlag(FLAGS_enable_resp_io_loop_v2)));
      pipeline_squashing_v2_ =
          ioloop_v2_ && GetFlag(FLAGS_enable_pipeline_squashing_v2) && protocol_ == Protocol::REDIS;

      socket_->RegisterOnErrorCb([this](int32_t mask) { this->OnBreakCb(mask); });
      switch (protocol_) {
        case Protocol::REDIS:
          reply_builder_.reset(new RedisReplyBuilder(socket_.get()));
          break;
        case Protocol::MEMCACHE:
          reply_builder_.reset(new MCReplyBuilder(socket_.get()));
          break;
        default:
          break;
      }
      parsed_cmd_ = CreateParsedCommand();
      ConnectionFlow();

      socket_->CancelOnErrorCb();  // noop if nothing is registered.
      VLOG(1) << CONN_ID << "Closed connection for peer "
              << GetClientInfo(fb2::ProactorBase::me()->GetPoolIndex());
      reply_builder_.reset();
      DestroyParsedQueue();
    }
    cc_.reset();
  }
}

unsigned Connection::GetSendWaitTimeSec() const {
  if (reply_builder_ && reply_builder_->IsSendActive()) {
    return base::CycleClock::ToUsec(base::CycleClock::Now() -
                                    reply_builder_->GetLastSendTimeCycles()) /
           1'000'000;
  }

  return 0;
}

std::error_code Connection::FlushReplies() {  // NOLINT must not be const due to flush side effect
  DCHECK(reply_builder_);
  reply_builder_->Flush();
  return reply_builder_->GetError();
}

std::string FormatClientInfo(const ClientInfo& ci) {
  string out;
  absl::StrAppend(&out, "id=", ci.id, " addr=", ci.addr, " laddr=", ci.laddr, " fd=", ci.fd);
  if (ci.is_http) {
    absl::StrAppend(&out, " http=true");
  } else {
    absl::StrAppend(&out, " name=", ci.name);
  }
  if (!ci.tls.empty())
    absl::StrAppend(&out, " tls=", ci.tls);
  if (ci.send_wait_time.has_value())
    absl::StrAppend(&out, " send-wait-time=", *ci.send_wait_time);
  absl::StrAppend(&out, " tid=", ci.tid, " irqmatch=", int(ci.irqmatch));
  if (ci.pipeline.has_value())
    absl::StrAppend(&out, " pipeline=", *ci.pipeline, " pbuf=", ci.pbuf.value_or(0));
  absl::StrAppend(&out, " age=", ci.age, " idle=", ci.idle, " tot-cmds=", ci.tot_cmds,
                  " tot-net-in=", ci.tot_net_in, " tot-read-calls=", ci.tot_read_calls,
                  " tot-dispatches=", ci.tot_dispatches);
  if (ci.db.has_value())
    absl::StrAppend(&out, " db=", *ci.db);
  if (!ci.flags.empty())
    absl::StrAppend(&out, " flags=", ci.flags);
  if (!ci.phase.empty())
    absl::StrAppend(&out, " phase=", ci.phase);
  if (!ci.repl_phase.empty())
    absl::StrAppend(&out, " repl-phase=", ci.repl_phase);
  absl::StrAppend(&out, " lib-name=", ci.lib_name, " lib-ver=", ci.lib_ver);
  return out;
}

ClientInfo Connection::BuildClientInfo(unsigned thread_id) const {
  ClientInfo ci;
  if (!socket_) {
    LOG(DFATAL) << CONN_ID << "unexpected null socket_ "
                << " phase " << unsigned(phase_) << ", is_http: " << unsigned(is_http_);
    return ci;
  }
  CHECK_LT(unsigned(phase_), NUM_PHASES);

  static constexpr string_view PHASE_NAMES[] = {"setup", "readsock", "process", "shutting_down",
                                                "preclose"};
  static_assert(NUM_PHASES == ABSL_ARRAYSIZE(PHASE_NAMES));
  static_assert(PHASE_NAMES[SHUTTING_DOWN] == "shutting_down");

  int cpu = 0;
  socklen_t len = sizeof(cpu);
  getsockopt(socket_->native_handle(), SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len);
#ifdef __APPLE__
  int my_cpu_id = -1;  // __APPLE__ does not have sched_getcpu()
#else
  int my_cpu_id = sched_getcpu();
#endif

  time_t now = time(nullptr);
  ci.id = id_;
  ci.addr = RemoteEndpointStr();
  ci.laddr = LocalBindStr();
  ci.fd = socket_->native_handle();
  ci.is_http = is_http_;
  if (!is_http_)
    ci.name = name_;
#ifdef DFLY_USE_SSL
  if (is_tls_) {
    tls::TlsSocket* tls_sock = static_cast<tls::TlsSocket*>(socket_.get());
    string_view proto_version = SSL_get_version(tls_sock->ssl_handle());
    const SSL_CIPHER* cipher = SSL_get_current_cipher(tls_sock->ssl_handle());
    ci.tls = absl::StrCat(proto_version, "|", SSL_CIPHER_get_name(cipher));
  }
#endif
  if (IsSending())
    ci.send_wait_time = GetSendWaitTimeSec();
  ci.tid = thread_id;
  ci.irqmatch = (cpu == my_cpu_id);
  if (parsed_cmd_q_len_ > 0) {
    ci.pipeline = parsed_cmd_q_len_;
    ci.pbuf = parsed_cmd_q_bytes_;
  }
  ci.age = now - creation_time_;
  ci.idle = now - last_interaction_;
  ci.tot_cmds = local_stats_.cmds;
  ci.tot_net_in = local_stats_.net_bytes_in;
  ci.tot_read_calls = local_stats_.read_cnt;
  ci.tot_dispatches = local_stats_.dispatch_entries_added;

  string_view phase_name = PHASE_NAMES[phase_];
  if (cc_) {
    auto ctx_info = service_->GetContextInfo(cc_.get());
    if (reply_builder_ && reply_builder_->IsSendActive())
      phase_name = "send";
    else if (ctx_info.is_scheduled)
      phase_name = "scheduled";
    ci.db = ctx_info.db_index;
    // ctx_info.Format() returns "db=N" or "db=N flags=X"; extract flags only.
    size_t flags_pos = ctx_info.Format().find(" flags=");
    if (flags_pos != string::npos)
      ci.flags = ctx_info.Format().substr(flags_pos + sizeof(" flags=") - 1);
  }
  ci.phase = string(phase_name);
  ci.lib_name = lib_name_;
  ci.lib_ver = lib_ver_;
  return ci;
}

string Connection::GetClientInfo(unsigned thread_id) const {
  return FormatClientInfo(BuildClientInfo(thread_id));
}

string Connection::GetClientInfo() const {
  // CLIENT INFO appends dummy counters that redis-py's ACL LOG parser hard-codes.
  // Real values for these fields are not tracked by Dragonfly today.
  string info = FormatClientInfo(BuildClientInfo(0));
  absl::StrAppend(&info, " qbuf=0 qbuf-free=0 obl=0 argv-mem=0 oll=0 omem=0 tot-mem=0 multi=0",
                  " psub=0 sub=0");
  return info;
}

uint32_t Connection::GetClientId() const {
  return id_;
}

uint32_t Connection::NextClientId() {
  static std::atomic_uint32_t next_id{1};
  return next_id.fetch_add(1, std::memory_order_relaxed);
}

bool Connection::IsPrivileged() const {
  return static_cast<Listener*>(listener())->IsPrivilegedInterface();
}

bool Connection::IsMain() const {
  return is_main_;
}

bool Connection::IsMainOrMemcache() const {
  return is_main_ || protocol_ == Protocol::MEMCACHE;
}

void Connection::SetName(string name) {
  util::ThisFiber::SetName(absl::StrCat("DflyConn_", name));
  name_ = std::move(name);
}

void Connection::SetLibName(string name) {
  UpdateLibNameVerMap(lib_name_, lib_ver_, -1);
  lib_name_ = std::move(name);
  UpdateLibNameVerMap(lib_name_, lib_ver_, +1);
}

void Connection::SetLibVersion(string version) {
  UpdateLibNameVerMap(lib_name_, lib_ver_, -1);
  lib_ver_ = std::move(version);
  UpdateLibNameVerMap(lib_name_, lib_ver_, +1);
}

const absl::flat_hash_map<string, uint64_t>& Connection::GetLibStatsTL() {
  return g_libname_ver_map;
}

io::Result<bool> Connection::CheckForHttpProto() {
  if (!IsPrivileged() && !IsMain()) {
    return false;
  }

  const bool primary_port_enabled = GetFlag(FLAGS_primary_port_http_enabled);
  if (!primary_port_enabled && !IsPrivileged()) {
    return false;
  }

  size_t last_len = 0;
  auto* peer = socket_.get();
  do {
    auto buf = io_buf_.AppendBuffer();
    DCHECK(!buf.empty());

    ::io::Result<size_t> recv_sz = peer->Recv(buf);
    if (!recv_sz) {
      return make_unexpected(recv_sz.error());
    }
    if (recv_sz == 0) {
      // Peer closed connection.
      return false;
    }

    io_buf_.CommitWrite(*recv_sz);
    string_view ib = io::View(io_buf_.InputBuffer());
    if (ib.size() >= 2 && ib[0] == 22 && ib[1] == 3) {
      // We matched the TLS handshake raw data, which means "peer" is a TCP socket.
      // Reject the connection.
      return make_unexpected(make_error_code(errc::protocol_not_supported));
    }

    ib = ib.substr(last_len);
    size_t pos = ib.find('\n');
    if (pos != string_view::npos) {
      ib = io::View(io_buf_.InputBuffer().first(last_len + pos));
      if (ib.size() < 10 || ib.back() != '\r')
        return false;

      ib.remove_suffix(1);
      return MatchHttp11Line(ib);
    }
    last_len = io_buf_.InputLen();
    {
      ReadBufTracker tracker(io_buf_, id_);
      io_buf_.EnsureCapacity(128);
    }
  } while (last_len < 1024);

  return false;
}

void Connection::ConnectionFlow() {
  DCHECK(reply_builder_);
  auto& conn_stats = tl_facade_stats->conn_stats;

  // Register the new connection with the thread-local statistics.
  // At this point (connection birth), local queue stats/luggage are 0,
  // so only connection counts and buffer capacities are incremented.
  IncreaseConnStats();
  ++conn_stats.conn_received_cnt;

  ++local_stats_.read_cnt;
  local_stats_.net_bytes_in += io_buf_.InputLen();

  ParserStatus parse_status = OK;

  // At the start we read from the socket to determine the HTTP/Memstore protocol.
  // Therefore we may already have some data in the buffer.
  if (io_buf_.InputLen() > 0) {
    phase_ = PROCESS;
    if (redis_parser_ && !ioloop_v2_) {
      parse_status = ParseRedis(io_buf_, 10000, /*enqueue_only=*/false);
    } else {
      parse_status = ParseLoop();
    }
  }

  error_code ec = reply_builder_->GetError();

  // Main loop.
  if (parse_status != ERROR && !ec) {
    {
      ReadBufTracker tracker(io_buf_, id_);
      io_buf_.EnsureCapacity(64);
    }
    variant<error_code, Connection::ParserStatus> res;
    if (ioloop_v2_) {
      res = IoLoopV2();
    } else {
      res = IoLoop();
    }

    if (holds_alternative<error_code>(res)) {
      ec = get<error_code>(res);
    } else {
      parse_status = get<ParserStatus>(res);
    }
  }

  // After the client disconnected.
  cc_->conn_closing = true;  // Signal dispatch to close.
  cnd_.notify_one();
  phase_ = SHUTTING_DOWN;
  VLOG(2) << CONN_ID << "Before dispatch_fb.join()";
  async_fb_.JoinIfNeeded();
  VLOG(2) << CONN_ID << "After dispatch_fb.join()";

  phase_ = PRECLOSE;

  DrainConnectionQueues();
  DCHECK(!HasPendingMessages());

  service_->OnConnectionClose(cc_.get());

  // We have already cleared the queues above (DrainConnectionQueues), so local queue stats
  // (dispatch_q_bytes_, etc.) represent 0 usage. DecreaseConnStats will safely subtract 0 for those
  // stats, while correctly removing this connection from the global connection counts and buffer
  // capacity tracking.
  DecreaseConnStats();

  if (ioloop_v2_) {
    socket_->ResetOnRecvHook();
  }

  // We wait for dispatch_fb to finish writing the previous replies before replying to the last
  // offending request.
  if (parse_status == ERROR) {
    VLOG(1) << CONN_ID << "Error parser status " << parser_error_;

    if (redis_parser_) {
      SendProtocolError(RespSrvParser::Result(parser_error_), reply_builder_.get());
    } else {
      DCHECK(memcache_parser_);
      reply_builder_->SendProtocolError("bad command line format");
    }

    // Shut down the servers side of the socket to send a FIN to the client
    // then keep draining the socket (discarding any received data) until
    // the client closes the connection.
    //
    // Otherwise the clients write could fail (or block), so they would never
    // read the above protocol error (see issue #1327).
    // TODO: we have a bug that can potentially deadlock the code below.
    // If the socket does not close the socket on the other side, the while loop will never finish.
    // to reproduce: nc localhost 6379  and then run invalid sequence: *1 <enter> *1 <enter>
    error_code ec2 = socket_->Shutdown(SHUT_WR);
    LOG_IF(WARNING, ec2) << CONN_ID << "Could not shutdown socket " << ec2;
    while (!ec2) {
      // Discard any received data.
      io_buf_.Clear();
      auto recv_sz = socket_->Recv(io_buf_.AppendBuffer());
      if (!recv_sz || *recv_sz == 0) {
        break;  // Peer closed connection.
      }
    }
  }

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    string conn_info = service_->GetContextInfo(cc_.get()).Format();
    LOG_EVERY_T(WARNING, 1) << CONN_ID << "Socket error for connection " << conn_info << " "
                            << GetName() << " during phase " << kPhaseName[phase_] << " : " << ec
                            << " " << ec.message();
  }
}

void Connection::DispatchSingle(bool has_more, absl::FunctionRef<void()> invoke_cb,
                                absl::FunctionRef<void()> enqueue_cmd_cb) {
  // Unconditional return when closing:
  // else, non-throttled connections skip the check below and enqueue data even if they are closing.
  // No one will read that data anyway.
  if (cc_->conn_closing)
    return;
  auto can_dispatch_sync_fn = [this]() {
    return !cc_->async_dispatch && !HasPendingMessages() && (cc_->subscriptions == 0);
  };
  bool optimize_for_async = has_more;
  bool can_dispatch_sync = can_dispatch_sync_fn();
  QueueBackpressure& qbp = GetQueueBackpressure();
  ConnectionStats* conn_stats = &tl_facade_stats->conn_stats;
  if ((optimize_for_async || !can_dispatch_sync) &&
      qbp.IsPipelineBufferOverLimit(conn_stats->pipeline_queue_bytes, parsed_cmd_q_len_)) {
    conn_stats->pipeline_throttle_count++;
    LOG_EVERY_T(WARNING, 10) << CONN_ID << "Pipeline buffer over limit (V1)."
                             << ", Thread pipeline_queue_bytes: "
                             << conn_stats->pipeline_queue_bytes
                             << ", Thread pipeline_queue_entries: "
                             << conn_stats->pipeline_queue_entries
                             << ", Connection parsed_cmd_q_bytes_: " << parsed_cmd_q_bytes_
                             << ", Connection parsed commands queue size: " << parsed_cmd_q_len_
                             << ", consider increasing pipeline_buffer_limit/pipeline_queue_limit";
    fb2::NoOpLock noop;
    qbp.pipeline_cnd.wait(noop, [this, &qbp, &can_dispatch_sync_fn] {
      // Wait until at least one is true:
      // 1) Connection is closing.
      // 2) Can dispatch synchronously.
      // 3) Not over limits (for an async dispatch).
      bool can_dispatch_sync = can_dispatch_sync_fn();
      if (can_dispatch_sync)
        return true;
      bool over_limits = qbp.IsPipelineBufferOverLimit(
          tl_facade_stats->conn_stats.pipeline_queue_bytes, parsed_cmd_q_len_);
      return !over_limits || cc_->conn_closing;
    });

    // prefer synchronous dispatching to save memory.
    optimize_for_async = false;
    last_interaction_ = time(nullptr);
  }

  // Avoid sync dispatch if we can interleave with an ongoing async dispatch.
  can_dispatch_sync = can_dispatch_sync_fn();

  // Dispatch async if we're handling a pipeline or if we can't dispatch sync.
  if (optimize_for_async || !can_dispatch_sync) {
    LaunchAsyncFiberIfNeeded();
    enqueue_cmd_cb();
  } else {
    ShrinkPipelinePool();  // Gradually release pipeline request pool.
    {
      ++local_stats_.cmds;
      cc_->sync_dispatch = true;
      invoke_cb();
      cc_->sync_dispatch = false;
    }
    last_interaction_ = time(nullptr);

    // We might have blocked the dispatch queue from processing, wake it up.
    if (HasPendingMessages())
      cnd_.notify_one();
  }
}

Connection::ParserStatus Connection::ParseRedis(base::IoBuf& io_buf, uint32_t max_busy_cycles,
                                                bool enqueue_only) {
  uint32_t consumed = 0;
  RespSrvParser::Result result = RespSrvParser::OK;

  auto dispatch_sync = [this] {
    service_->DispatchCommand(ParsedArgs{*parsed_cmd_}, parsed_cmd_,
                              facade::AsyncPreference::ONLY_SYNC);
  };
  auto dispatch_async = [this]() -> void {
    PipelineMessagePtr ptr = GetFromPoolOrCreate();
    // parsed_cmd_ holds the parsed arguments. Move it to 'cmd' to be enqueued and set it with a new
    // empty ParsedCommand for the next parse.
    auto* cmd = std::exchange(parsed_cmd_, ptr.release());
    EnqueueParsedCommand(cmd);
  };
  io::Bytes read_buffer = io_buf.InputBuffer();
  // Keep track of total bytes consumed/parsed. The do/while{} loop below preempts,
  // and InputBuffer() size might change between preemption points. There is a corner case,
  // that ConsumeInput() will strip a portion of the request which makes the test_publish_stuck
  // test fail.
  // TODO(kostas): follow up on this
  size_t total_consumed = 0;
  do {
    bool stop_parsing = false;
    DCHECK(parsed_cmd_);
    result = redis_parser_->Parse(read_buffer, &consumed, parsed_cmd_);
    request_consumed_bytes_ += consumed;
    total_consumed += consumed;
    if (result == RespSrvParser::OK) {
      DCHECK(!parsed_cmd_->empty());
      DVLOG(2) << CONN_ID << "Got Args with first token " << parsed_cmd_->Front();

      if (io_req_size_hist)
        io_req_size_hist->Add(request_consumed_bytes_);
      request_consumed_bytes_ = 0;
      bool has_more = consumed < read_buffer.size();

      if (tl_traffic_logger.log_file && tl_traffic_logger.listener_type == listener_type_) {
        LogTraffic(id_, has_more, *parsed_cmd_, service_->GetContextInfo(cc_.get()));
      }

      if (enqueue_only) {
        dispatch_async();

        // Stop parsing the current buffer if we crossed the limit.
        // Unparsed bytes remain in io_buf_ for the next ParseLoop iteration.
        if (GetQueueBackpressure().IsPipelineBufferOverLimit(
                GetLocalConnStats().pipeline_queue_bytes, parsed_cmd_q_len_)) {
          DVLOG(2) << CONN_ID << "Pipeline buffer over limit, breaking from parsing loop.";
          stop_parsing = true;
        }
      } else {
        DispatchSingle(has_more, dispatch_sync, dispatch_async);
      }
    }
    if (result != RespSrvParser::OK && result != RespSrvParser::INPUT_PENDING) {
      // We do not expect that a replica sends an invalid command so we log if it happens.
      LOG_IF(WARNING, cntx()->replica_conn)
          << CONN_ID << "Redis parser error: " << static_cast<unsigned int>(result)
          << " during parse: " << io::View(read_buffer);
    }
    RefreshConnectionMemoryUsage();
    if (stop_parsing)
      break;

    read_buffer.remove_prefix(consumed);

    // We must yield from time to time to allow other fibers to run.
    // Specifically, if a client sends a huge chunk of data resulting in a very long pipeline,
    // we want to yield to allow AsyncFiber to actually execute on the pending pipeline.
    //
    // If max_busy_cycles == 0, never yield. We rely on io_buf_ to bound the work.
    if ((max_busy_cycles > 0) && ThisFiber::GetRunningTimeCycles() > max_busy_cycles) {
      GetLocalConnStats().num_read_yields++;

      fiber_park_spot_ = FiberParkSpot::kParseYield;
      ThisFiber::Yield();
      fiber_park_spot_ = FiberParkSpot::kNone;

      // Note:
      // - read_buffer stays valid across this yield since proactor only calls ReadPendingInput ->
      // AppendBuffer + CommitWrite. which writes the free tail and bumps size_ without touching
      // buf_/offs_.
      // - If provided buffers are ever enabled, we should revisit this code again when
      // parse-in-proactor enabled.
    }
  } while (RespSrvParser::OK == result && !read_buffer.empty() && !reply_builder_->GetError());

  io_buf.ConsumeInput(total_consumed);

  parser_error_ = result;
  if (result == RespSrvParser::OK)
    return OK;

  if (result == RespSrvParser::INPUT_PENDING) {
    DCHECK_EQ(read_buffer.size(), 0u);

    return NEED_MORE;
  }

  VLOG(1) << CONN_ID << "Parser error " << result;

  return ERROR;
}

auto Connection::ParseLoop() -> ParserStatus {
  auto parse_func =
      protocol_ == Protocol::MEMCACHE ? &Connection::ParseMCBatch : &Connection::ParseRedisBatch;

  ParserStatus parse_status = NEED_MORE;

  do {
    DCHECK_GT(io_buf_.InputLen(), 0u);
    parse_status = (this->*parse_func)(io_buf_);

    // V2 large-batch prioritization (pipeline_prioritize_large_batches):
    // - When a real pipeline is forming (>1 queued) and more socket data is expected
    // (pending_input_), return without executing so IoLoopV2's read path fill the buffer and
    // re-enters ParseLoop.
    // - This helps accumulating commands into the same queue for one larger squash batch instead of
    // many small ones.
    if (ioloop_v2_ && pipeline_prioritize_large_batches_cached && (parse_status != ERROR) &&
        pending_input_ && (parsed_cmd_q_len_ > 1) && !IsOverPipelineLimit())
      return parse_status;

    // Execute/reply the commands parsed so far first, so a trailing protocol error still flushes
    // earlier replies in order before we report it.
    if (!ExecuteBatch())
      return ERROR;

    if (!ReplyBatch())
      return ERROR;

    // Surface a protocol error to the caller (HandleRequests/IoLoopV2) so it can send the
    // protocol error reply (using parser_error_) and close the connection.
    if (parse_status == ERROR)
      return ERROR;
  } while (parse_status == OK && io_buf_.InputLen() > 0);

  return parse_status;  // OK or NEED_MORE
}

void Connection::OnBreakCb(int32_t mask) {
  if (mask <= 0)
    return;  // we cancelled the poller, which means we do not need to break from anything.

  if (!cc_) {
    LOG(ERROR) << CONN_ID << "Unexpected event " << mask;
    return;
  }

  DCHECK(reply_builder_) << CONN_ID << unsigned(phase_) << " " << migration_in_process_;

  VLOG(1) << CONN_ID << "Got event " << mask << " " << unsigned(phase_) << " "
          << reply_builder_->IsSendActive() << " " << reply_builder_->GetError();

  cc_->conn_closing = true;
  BreakOnce(mask);
  cnd_.notify_one();  // Notify dispatch fiber.
}

void Connection::HandleMigrateRequest() {
  if (!migration_request_)
    return;

  if (cc_->conn_closing) {
    migration_request_ = nullptr;
    return;
  }

  ProactorBase* dest = migration_request_;

  // V1 loop: we must coordinate with and shutdown the AsyncFiber before migration.
  // V2 loop: is single-fiber and handles this by breaking the dispatch_q loop.
  if (!ioloop_v2_ && async_fb_.IsJoinable()) {
    SendAsync({MigrationRequestMessage{}});
    async_fb_.Join();
  }

  // We don't support migrating with subscriptions as it would require moving thread-local handles.
  if (cc_->subscriptions == 0) {
    // RegisterOnErrorCb might be called on POLLHUP and the join above is a preemption point.
    // So, it could be the case that after this fiber wakes up the connection might be closing.
    // Re-check closing status after the potential V1 Join() or any preemption.
    if (cc_->conn_closing) {
      return;
    }

    tl_facade_stats->conn_stats.num_migrations++;
    migration_request_ = nullptr;

    // Verify that no background command processing is active.
    // V1 loop: Join() ensures this.
    // V2 loop: it is guaranteed by the single-fiber loop.
    DCHECK(ioloop_v2_ || !async_fb_.IsJoinable());

    std::ignore = !this->Migrate(dest);
  }

  // Note: If cc_->subscriptions > 0, we skip the hop but leave migration_request_
  // set. This defers the migration, retrying at the start of every subsequent
  // loop iteration until all subscriptions are cleared.
}

bool Connection::ProcessControlMessages(uint32_t quota) {
  // Invariant: batched_ must be false on entry.
  // PubSub replies flush immediately via FinishScope() only when batched_ is false.
  // ReplyBatch() and ExecuteBatch() both reset it via absl::Cleanup guards on all return paths.
  DCHECK(!reply_builder_->IsBatchMode());

  // Batch control-message replies when multiple are queued,
  // avoiding per-message sendmsg syscalls while preserving latency for single-message wakeups.
  reply_builder_->SetBatchMode(dispatch_q_.size() > 1);
  absl::Cleanup batch_guard = [this] { reply_builder_->SetBatchMode(false); };

  uint32_t dispatched = 0;

  while (!dispatch_q_.empty()) {
    // If Quota reached: stop draining the dispatch queue and fall through to the data path.
    if ((quota > 0) && (dispatched >= quota)) {
      LOG_EVERY_T(INFO, 1) << CONN_ID << "V2 dispatch_q_ quota reached (" << dispatched << "/"
                           << quota << "), falling through to data path";
      return true;
    }

    auto msg = std::move(dispatch_q_.front());
    dispatch_q_.pop_front();
    UpdateDispatchStats(msg, false /* subtract */);
    dispatched++;

    // If a MigrationRequestMessage arrives via the dispatch queue, stop processing
    // and let the loop iterate back to HandleMigrateRequest() at the top.
    if (std::holds_alternative<MigrationRequestMessage>(msg.handle)) {
      break;
    }

    async_op_start_cycle_ = base::CycleClock::Now();
    std::visit(AsyncOperations{reply_builder_.get(), this}, msg.handle);
    async_op_start_cycle_ = 0;
  }

  return false;
}

io::Result<size_t> Connection::HandleRecvSocket() {
  phase_ = READ_SOCKET;
  auto& conn_stats = tl_facade_stats->conn_stats;

  io::MutableBytes append_buf = io_buf_.AppendBuffer();
  DCHECK(!append_buf.empty());
  ::io::Result<size_t> recv_sz = socket_->Recv(append_buf);
  last_interaction_ = time(nullptr);

  // In case the socket was closed orderly, we get 0 bytes read.
  if (recv_sz && *recv_sz) {
    size_t commit_sz = *recv_sz;
    DVLOG(2) << CONN_ID << "Received " << commit_sz << " bytes from socket";

    io_buf_.CommitWrite(commit_sz);

    conn_stats.io_read_bytes += commit_sz;
    local_stats_.net_bytes_in += commit_sz;

    ++conn_stats.io_read_cnt;
    ++local_stats_.read_cnt;
  }
  return recv_sz;
}

variant<error_code, Connection::ParserStatus> Connection::IoLoop() {
  error_code ec;
  ParserStatus parse_status = OK;
  size_t max_iobfuf_len = GetFlag(FLAGS_max_client_iobuf_len);

  auto* peer = socket_.get();
  recv_buf_.res_len = 0;

  do {
    HandleMigrateRequest();
    auto recv_sz = HandleRecvSocket();
    if (!recv_sz) {
      LOG_IF(WARNING, cntx()->replica_conn)
          << CONN_ID << "HandleRecvSocket() error: " << recv_sz.error();
      return recv_sz.error();
    }
    if (*recv_sz == 0) {
      break;
    }

    phase_ = PROCESS;
    bool reached_capacity = io_buf_.AppendLen() == 0;

    if (redis_parser_) {
      parse_status = ParseRedis(io_buf_, max_busy_read_cycles_cached, /*enqueue_only=*/false);
    } else {
      DCHECK(memcache_parser_);
      parse_status = ParseLoop();
    }

    if (reply_builder_->GetError()) {
      return reply_builder_->GetError();
    }

    if (parse_status == NEED_MORE) {
      parse_status = OK;

      size_t capacity = io_buf_.Capacity();
      if (capacity < max_iobfuf_len) {
        size_t parser_hint = 0;
        if (redis_parser_)
          parser_hint = redis_parser_->parselen_hint();  // Could be done for MC as well.

        // If we got a partial request and we managed to parse its
        // length, make sure we have space to store it instead of
        // increasing space incrementally.
        // (Note: The buffer object is only working in power-of-2 sizes,
        // so there's no danger of accidental O(n^2) behavior.)
        if (parser_hint > capacity) {
          ReadBufTracker tracker(io_buf_, id_);
          io_buf_.Reserve(std::min(max_iobfuf_len, parser_hint));
        }

        // If we got a partial request because iobuf was full, grow it up to
        // a reasonable limit to save on Recv() calls.
        if (reached_capacity && capacity < max_iobfuf_len / 2) {
          // Last io used most of the io_buf to the end.
          ReadBufTracker tracker(io_buf_, id_);
          io_buf_.Reserve(capacity * 2);  // Valid growth range.
        }

        if (io_buf_.AppendLen() == 0U) {
          // it can happen with memcached but not for RedisParser, because RedisParser fully
          // consumes the passed buffer
          LOG_EVERY_T(WARNING, 10)
              << CONN_ID
              << "Maximum io_buf length reached, consider to increase max_client_iobuf_len flag";
        }
      }
    } else if (parse_status != OK) {
      break;
    }
  } while (peer->IsOpen());

  return parse_status;
}

bool Connection::ShouldEndAsyncFiber(const MessageHandle& msg) {
  if (!holds_alternative<MigrationRequestMessage>(msg.handle)) {
    return false;
  }

  if (!HasPendingMessages()) {
    // Migration requests means we should terminate this function (and allow the fiber to
    // join), so that we can re-launch the fiber in the new thread.
    // We intentionally return and not break in order to keep the connection open.
    return true;
  }

  // There shouldn't be any other migration requests in the queue, but it's worth checking
  // as otherwise it would lead to an endless loop.
  bool has_migration_req =
      any_of(dispatch_q_.begin(), dispatch_q_.end(), [](const MessageHandle& msg) {
        return holds_alternative<MigrationRequestMessage>(msg.handle);
      });
  if (!has_migration_req) {
    SendAsync({MigrationRequestMessage{}});
  }

  return false;
}

void Connection::SquashPipeline() {
  DCHECK_EQ(GetPendingMessageCount(), parsed_cmd_q_len_);
  DCHECK_EQ(reply_builder_->GetProtocol(), Protocol::REDIS);  // Only Redis is supported.
  ConnectionMemoryTracker memory_tracker(this);
  unsigned pipeline_count = std::min<uint32_t>(parsed_cmd_q_len_, pipeline_squash_limit_cached);
  auto& conn_stats = tl_facade_stats->conn_stats;

  uint64_t start = CycleClock::Now();

  // async_dispatch guards the whole batch: it gates sync dispatch and reply_builder_ writes
  // (incl. Flush), and marks the connection as dispatching for DispatchTracker checkpoints.
  // It must be set before DispatchSquashedBatch, which can preempt on shard hops.
  cc_->async_dispatch = true;

  uint32_t squashed =
      service_->DispatchSquashedBatch(parsed_to_execute_, pipeline_count, cc_.get());

  // Nothing was squashed (the head command can't join a batch, e.g. MULTI/EXEC, EVAL,
  // subscribe, blocking, or unknown). Hand it off to regular dispatch without flushing or
  // resetting batch mode here — doing so would break reply aggregation for the rest of the
  // pipeline (e.g. a pipelined MULTI/EXEC would flush once per command).
  if (squashed == 0) {
    cc_->async_dispatch = false;
    skip_next_squashing_ = true;
    return;
  }

  // Send all replies under a ReplyScope before releasing the commands.
  // This allows the reply builder to flush without copies
  {
    SinkReplyBuilder::ReplyScope scope(reply_builder_.get());
    auto* cmd = parsed_head_;
    for (unsigned i = 0; i < squashed && cmd; i++, cmd = cmd->next) {
      DCHECK(cmd->CanReply());
      // TODO: Coroutine replies don't preserve lifetimes after running, so pause the scope
      std::optional<SinkReplyBuilder::ScopePause> pause;
      if (cmd->IsSuspendedReply())
        pause.emplace(reply_builder_.get());
      cmd->SendReply();
      conn_stats.pipelined_wait_latency += CycleClock::ToUsec(start - cmd->parsed_cycle);
    }
  }

  for (unsigned i = 0; i < squashed && parsed_head_; ++i) {
    auto* current = parsed_head_;
    auto* next = current->next;
    ReleasePipelinedCommand(current);
    AdvanceParsedHead(next);
  }
  DCHECK_GE(dispatch_waiting_count_, squashed);
  dispatch_waiting_count_ -= squashed;  // the squashed run was waiting; it is now fully handled
  parsed_to_execute_ = parsed_head_;

  local_stats_.cmds += squashed;
  last_interaction_ = time(nullptr);

  // Flush if no new commands appeared while we dispatched. The released commands were already
  // subtracted from parsed_cmd_q_len_, so add them back for the comparison.
  if (parsed_cmd_q_len_ + squashed == pipeline_count || always_flush_pipeline_cached) {
    uint64_t flush_start_cycle = CycleClock::Now();
    reply_builder_->Flush();
    conn_stats.pipeline_dispatch_flush_count++;
    reply_builder_->SetBatchMode(false);  // in case the next dispatch is sync
    conn_stats.pipeline_dispatch_flush_usec +=
        CycleClock::ToUsec(CycleClock::Now() - flush_start_cycle);
  }

  cc_->async_dispatch = false;

  conn_stats.pipeline_dispatch_calls++;
  conn_stats.pipeline_dispatch_commands += squashed;

  // If interrupted due to pause or a non-squashable command, fall back to regular dispatch.
  skip_next_squashing_ = (squashed != pipeline_count);
}

void Connection::DrainConnectionQueues() {
  AsyncOperations async_op{reply_builder_.get(), this};

  // First, clear dispatch queue
  // Recycle messages even from disconnecting client to keep properly track of memory stats
  // As well as to avoid pubsub backpressure leakage.
  for (auto& msg : dispatch_q_) {
    FiberAtomicGuard guard;  // don't suspend when concluding to avoid getting new messages
    if (msg.IsCheckPoint())
      visit(async_op, msg.handle);  // to not miss checkpoints
    UpdateDispatchStats(msg, false /* subtract */);
  }

  dispatch_q_.clear();

  // Second, drain the pending pipeline queue: release memory and update stats without executing
  // commands.
  while (parsed_head_) {
    auto* curr{parsed_head_};
    parsed_head_ = parsed_head_->next;

    // Wait for the in-flight async commands processing by consumer to finish before recycling.
    if (curr->IsDeferredReply() && !curr->CanReply()) {
      curr->Blocker()->Wait();
    }

    ReleaseParsedCommand(curr);
  }

  DCHECK_EQ(parsed_cmd_q_len_, 0u);
  DCHECK_EQ(parsed_cmd_q_bytes_, 0u);
  parsed_tail_ = nullptr;
  parsed_to_execute_ = nullptr;
  dispatch_waiting_count_ = 0;

  ReleaseDeferredCheckpoints();

  QueueBackpressure& qbp = GetQueueBackpressure();
  qbp.NotifyPipelineWaiters();
  qbp.pubsub_ec.notifyAll();
}

void Connection::ReleaseDeferredCheckpoints() {
  // Invariant: only the V2 loop should defer a checkpoint, else vector must be empty.
  DCHECK(ioloop_v2_ || deferred_checkpoints_.empty());
  // Invariant: we never release while an async command is still in flight - doing so would let a
  // CLIENT PAUSE / DispatchTracker return before the write lands. Callers must hold this rule.
  DCHECK(!HasInFlightCommands());

  if (deferred_checkpoints_.empty())
    return;

  DVLOG(1) << CONN_ID << " Releasing " << deferred_checkpoints_.size() << " deferred checkpoint(s) "
           << DebugInfo();
  for (auto& bc : deferred_checkpoints_) {
    bc->Dec();
  }
  deferred_checkpoints_.clear();
}

string Connection::DebugInfo() const {
  string info = "{";

  absl::StrAppend(&info, "id=", id_, ", ");
  absl::StrAppend(&info, "phase=", phase_, ", ");
  if (cc_) {
    // In some rare cases cc_ can be null, see https://github.com/dragonflydb/dragonfly/pull/3873
    absl::StrAppend(&info, "dispatch(s/a)=", cc_->sync_dispatch, " ", cc_->async_dispatch, ", ");
    absl::StrAppend(&info, "closing=", cc_->conn_closing, ", ");
  }
  absl::StrAppend(&info, "df:joinable=", async_fb_.IsJoinable(), ", ");

  absl::StrAppend(&info, "dq:size=", dispatch_q_.size(), ", ");
  absl::StrAppend(&info, "pq:parsed_cmd_q_len=", parsed_cmd_q_len_, ", ");
  absl::StrAppend(&info, "pq:is_empty=", (parsed_head_ == nullptr), ", ");

  if (cc_) {
    absl::StrAppend(&info, "state=");
    if (cc_->paused)
      absl::StrAppend(&info, "p");
    if (cc_->blocked)
      absl::StrAppend(&info, "b");
  }
  time_t now = time(nullptr);
  absl::StrAppend(&info, " age=", now - creation_time_, " idle=", now - last_interaction_, "}");

  return info;
}

bool Connection::ProcessAdminMessage(MessageHandle* msg, AsyncOperations* async_op) {
  // Guard: Automatically subtract stats when this scope exits (via return or exception).
  absl::Cleanup stats_guard = [this, msg] { UpdateDispatchStats(*msg, false /* subtract */); };
  bool is_replying = msg->IsReplying();

  // Pre-execution Flush
  // If this is a non-replying control message (e.g. Migration) and it's the last item,
  // we MUST flush the buffer now. Otherwise, previous pipelined replies might wait
  // indefinitely or be lost if the fiber terminates.
  if (!HasPendingMessages() && !is_replying) {
    reply_builder_->Flush();
  }

  // Fiber Termination Check
  if (ShouldEndAsyncFiber(*msg)) {
    CHECK(!HasPendingMessages()) << DebugInfo();
    GetQueueBackpressure().NotifyPipelineWaiters();
    return true;  // Signal to terminate AsyncFiber
  }

  // Execution
  auto replies_recorded_before = reply_builder_->RepliesRecorded();
  cc_->async_dispatch = true;
  // Track the control-path async operation so the slow-subscriber protection policy can observe how
  // long the current Pub/Sub send has been blocked (see SendPubMessageAsync).
  async_op_start_cycle_ = base::CycleClock::Now();
  std::visit(*async_op, msg->handle);
  async_op_start_cycle_ = 0;
  cc_->async_dispatch = false;

  // A slow-subscriber close may have been requested from SendPubMessageAsync while we were parked
  // in the send above. Stop normal processing; MarkForClose() already set the reply-builder error,
  // so the AsyncFiber loop will observe it and run the connection's shutdown path.
  if (request_shutdown_)
    return false;

  // Post-execution Flush
  // We force a flush If the message is supposed to reply (e.g. PubSub) but didn't write to the
  // buffer (e.g. subscription filter), and the queues are empty.
  if (!HasPendingMessages() && is_replying &&
      (replies_recorded_before == reply_builder_->RepliesRecorded())) {
    reply_builder_->Flush();
  }
  return false;
}

void Connection::ProcessPipelineCommandV1() {
  DCHECK(parsed_head_ && parsed_to_execute_) << DebugInfo();
  ConnectionMemoryTracker memory_tracker(this);
  auto* cmd = parsed_to_execute_;
  AdvanceParsedHead(AdvanceToExecute());

  tl_facade_stats->conn_stats.pipelined_wait_latency +=
      CycleClock::ToUsec(CycleClock::Now() - cmd->parsed_cycle);

  cc_->async_dispatch = true;
  local_stats_.cmds++;
  service_->DispatchCommand(ParsedArgs{*cmd}, cmd, facade::AsyncPreference::ONLY_SYNC);
  last_interaction_ = time(nullptr);
  skip_next_squashing_ = false;
  cc_->async_dispatch = false;

  ReleasePipelinedCommand(cmd);

  // If we drained the pipeline and no admin messages are waiting, flush.
  if (!HasPendingMessages()) {
    reply_builder_->Flush();
  }
}

// AsyncFiber acts as the consumer for all asynchronous connection tasks.
//
// It operates on a producer-consumer model where the InputLoop parses socket data
// and routes it into two distinct streams:
// 1. Data Path: Pipelined commands are queued in a Parsed Commands linked list
// 2. Control Path: Admin events (Migrations, Checkpoints, PubSub) use a deque (dispatch_q_)
//
// AsyncFiber drains these queues according to system prioritization, ensuring
// high-priority events are handled promptly while preventing priority inversion
// during thread migrations. For simple requests, the InputLoop may bypass this
// fiber and dispatch synchronously to minimize latency.
void Connection::AsyncFiber() {
  ThisFiber::SetName("AsyncFiber");

  AsyncOperations async_op{reply_builder_.get(), this};
  size_t squashing_threshold = GetFlag(FLAGS_pipeline_squash);
  uint64_t prev_epoch = fb2::FiberSwitchEpoch();
  fb2::NoOpLock noop_lk;
  QueueBackpressure& qbp = GetQueueBackpressure();
  auto& conn_stats = tl_facade_stats->conn_stats;
  uint32_t dispatch_q_cmd_processed = 0;
  uint32_t async_dispatch_quota = GetFlag(FLAGS_async_dispatch_quota);

  while (!reply_builder_->GetError()) {
    DCHECK_EQ(socket()->proactor(), ProactorBase::me());
    cnd_.wait(noop_lk, [this] {
      if (cc_->conn_closing)
        return true;

      // If we are currently executing a synchronous dispatch (e.g. inside IoLoop),
      // we must wait until it finishes to avoid race conditions.
      if (cc_->sync_dispatch)
        return false;

      // For Memcache, we ONLY wake up for Admin messages (dispatch_q_) as we process
      // parsed_head_  in the connection fiber. For RESP, we wake up for both queues.
      if (protocol_ == Protocol::MEMCACHE) {
        return !dispatch_q_.empty();
      }
      return HasPendingMessages();
    });

    if (cc_->conn_closing)
      break;

    // We really want to have batching in the builder if possible. This is especially
    // critical in situations where Nagle's algorithm can introduce unwanted high
    // latencies. However we can only batch if we're sure that there are more commands
    // on the way that will trigger a flush. To know if there are, we sometimes yield before
    // executing the last command in the queue and let the producer fiber push more commands if it
    // wants to.
    // As an optimization, we only yield if the fiber was not suspended since the last dispatch.
    uint64_t cur_epoch = fb2::FiberSwitchEpoch();
    if ((GetPendingMessageCount() == 1) && (cur_epoch == prev_epoch)) {
      if (pipeline_wait_batch_usec > 0) {
        ThisFiber::SleepFor(chrono::microseconds(pipeline_wait_batch_usec));
      } else {
        ThisFiber::Yield();
      }
      DVLOG(2) << CONN_ID << "After yielding to producer, parsed_cmd_q_len_=" << parsed_cmd_q_len_
               << " dispatch_q size=" << dispatch_q_.size();
      if (cc_->conn_closing)
        break;
    }
    prev_epoch = cur_epoch;

    reply_builder_->SetBatchMode(GetPendingMessageCount() > 1);

    // Publishers park at the hard limit, so we only need to wake them when the subscriber memory
    // transitions back below it.
    bool subscriber_over_limit =
        conn_stats.dispatch_queue_subscriber_bytes >= qbp.PubSubHardLimit();

    // The below if/else conditionally choose between 3 message processing policies:
    // 1. Pipeline squashing
    // 2. Process pipeline queue
    // 3. Process admin queue
    //
    // Special case: if the dispatch queue accumulated a big number of commands,
    // we can try to squash them
    // It is only enabled if the threshold is reached and the whole dispatch queue
    // consists only of commands (no pubsub or monitor messages)
    bool squashing_enabled = squashing_threshold > 0;
    bool threshold_reached = parsed_cmd_q_len_ > squashing_threshold;
    if (squashing_enabled && threshold_reached && dispatch_q_.empty() && !skip_next_squashing_ &&
        !IsReplySizeOverLimit()) {  // 1. Pipeline squashing
      SquashPipeline();
      dispatch_q_cmd_processed = 0;
    } else {
      MessageHandle msg;

      // If the front message is a Migration Request, but we still have pipeline data
      // (parsed_head_), we must block the migration and process the pipeline messages first.
      bool is_migration_req =
          !dispatch_q_.empty() &&
          std::holds_alternative<MigrationRequestMessage>(dispatch_q_.front().handle);

      // If the quota is reached but the pipeline appears empty, we must yield to the IoLoop
      // (producer). This allows the discovery and parsing of commands potentially sitting in the
      // TCP buffer. Without this yield, AsyncFiber would monopolize the CPU, starving the IoLoop
      // and remaining blind to pending pipeline data.
      bool quota_reached =
          (async_dispatch_quota > 0) && (dispatch_q_cmd_processed >= async_dispatch_quota);
      if (quota_reached && (parsed_head_ == nullptr)) {
        ThisFiber::Yield();

        // If it is STILL empty after IoLoop got a chance to run, the client hasn't sent anything.
        // Reset the counter so we don't yield on every single loop.
        if (parsed_head_ == nullptr) {
          dispatch_q_cmd_processed = 0;
        }
      }

      // We prioritize pipeline execution over the admin queue in two distinct cases (Pipeline queue
      // must be non-empty for both cases):
      // 1. A migration is requested (Redis only), but we must drain the existing
      // pipeline first.
      // 2.  The dispatch quota was reached, forcing a pipeline execution to prevent
      // starvation.
      bool prefer_pipeline_execution = false;
      if (parsed_head_ != nullptr) {
        prefer_pipeline_execution =
            quota_reached || (is_migration_req && (protocol_ == Protocol::REDIS));
      }
      if (dispatch_q_.empty() || prefer_pipeline_execution) {  // 2. Process pipeline Queue
        VLOG_IF(1, prefer_pipeline_execution)
            << CONN_ID << "Preferring pipeline execution over admin queue. "
            << "Migration requested: " << is_migration_req
            << ", dispatch quota reached: " << quota_reached
            << ", async_dispatch_quota: " << async_dispatch_quota
            << ", dispatch_q_cmd_processed: " << dispatch_q_cmd_processed;
        ProcessPipelineCommandV1();
        dispatch_q_cmd_processed = 0;
      } else {  // 3. Process admin Queue
        msg = std::move(dispatch_q_.front());
        dispatch_q_.pop_front();
        dispatch_q_cmd_processed++;

        // Execute and check if we need to terminate the fiber
        if (ProcessAdminMessage(&msg, &async_op)) {
          return;  // don't set conn closing flag
        }
      }
    }

    // Notify waiters if backpressure constraints are relieved.
    // 1. Global memory (bytes) is under limit -> Wakes up neighbors on this thread.
    // 2. Local queue (length) is under limit -> Wakes up this connection's producer.
    if (qbp.IsPipelineBufferUnderLimit(conn_stats.pipeline_queue_bytes, parsed_cmd_q_len_) ||
        !HasPendingMessages()) {
      qbp.NotifyPipelineWaiters();
    }

    if (subscriber_over_limit &&
        conn_stats.dispatch_queue_subscriber_bytes <= qbp.PubSubHardLimit())
      qbp.pubsub_ec.notify();
  }

  DCHECK(cc_->conn_closing || reply_builder_->GetError());

  cc_->conn_closing = true;
  qbp.NotifyPipelineWaiters();

  // If shutdown was requested, we need to break the receive call in case the i/o fiber
  // is blocked there. With io loop v2, we can have a different mechanism to break from recv flow.
  if (request_shutdown_) {
    ShutdownSelfBlocking();
  }
}

void Connection::ShrinkPipelinePool() {
  if (pipeline_req_pool_.empty())
    return;
  auto& conn_stats = tl_facade_stats->conn_stats;

  if (tl_pipe_cache_sz_tracker.CheckAndUpdateWatermark(pipeline_req_pool_.size())) {
    conn_stats.pipeline_cmd_cache_bytes -= UsedMemoryInternal(*pipeline_req_pool_.back());
    pipeline_req_pool_.pop_back();
  }
}

Connection::PipelineMessagePtr Connection::GetFromPoolOrCreate() {
  if (pipeline_req_pool_.empty())
    return PipelineMessagePtr{CreateParsedCommand()};
  auto& conn_stats = tl_facade_stats->conn_stats;

  auto ptr = std::move(pipeline_req_pool_.back());
  pipeline_req_pool_.pop_back();

  conn_stats.pipeline_cmd_cache_bytes -= UsedMemoryInternal(*ptr);
  ptr->ResetForReuse();

  ptr->Init(reply_builder_.get(), cc_.get());
  ptr->ConfigureMCExtension(protocol_ == Protocol::MEMCACHE);

  return ptr;
}

void Connection::ShutdownSelfBlocking() {
  util::Connection::Shutdown();
}

bool Connection::Migrate(util::fb2::ProactorBase* dest) {
  // Migrate() runs synchronously and only supports connections without subscriptions and with no
  // background command processing, as enforced by the CHECKs below.
  //
  // V2: Skip the !async_dispatch assertion:
  // - Uses a single-fiber, so the same fiber that set the flag is the one doing the migration.
  // - ExecuteBatch may set this flag around DispatchCommandSimple while flushing a pipeline;
  //   this guard is defensive in case future callers invoke Migrate() while a dispatch is active.
  // V1: keeps the assertion: there, a true async_dispatch means the AsyncFiber consumer is active.
  CHECK(ioloop_v2_ || !cc_->async_dispatch);
  CHECK_EQ(cc_->subscriptions, 0);  // are bound to thread local caches
  CHECK_EQ(self_.use_count(), 1u);  // references cache our thread and backpressure
                                    //
  if (ioloop_v2_ && socket_ && socket_->IsOpen()) {
    socket_->ResetOnRecvHook();
  }

  // Migrate is only used by DFLY Thread and Flow command which both check against
  // the result of Migration and handle it explicitly in their flows so this can act
  // as a weak if condition instead of a crash prone CHECK.
  if ((!ioloop_v2_ && async_fb_.IsJoinable()) || cc_->conn_closing) {
    return false;
  }

  listener()->Migrate(this, dest);

  // After we migrate, it could be the case the connection was shut down. We should
  // act accordingly.
  if (!socket()->IsOpen()) {
    return false;
  }

  return true;
}

Connection::WeakRef Connection::Borrow() {
  DCHECK(self_);

  return {self_, unsigned(socket_->proactor()->GetPoolIndex()), id_};
}

void Connection::ShutdownThreadLocal() {
  pipeline_req_pool_.clear();
}

bool Connection::IsCurrentlyDispatching() const {
  if (!cc_)
    return false;

  return cc_->async_dispatch || cc_->sync_dispatch;
}

bool Connection::IsAsyncOpOverdue(uint64_t timeout_cycles) const {
  if (async_op_start_cycle_ == 0)
    return false;
  return base::CycleClock::Now() - async_op_start_cycle_ >= timeout_cycles;
}

void Connection::RequestPubsubClose() {
  QueueBackpressure& qbp = GetQueueBackpressure();

  // Evict already-queued PubMessage items, releasing their per-thread subscriber accounting
  // immediately. Other control messages (checkpoints, migration, ...) are left for the normal
  // connection cleanup so their waiters/statistics stay consistent.
  size_t discarded_msgs = 0, discarded_bytes = 0;
  for (auto it = dispatch_q_.begin(); it != dispatch_q_.end();) {
    if (it->IsPubMsg()) {
      ++discarded_msgs;
      discarded_bytes += it->UsedMemory();
      UpdateDispatchStats(*it, false /* subtract */);
      it = dispatch_q_.erase(it);
    } else {
      ++it;
    }
  }

  // The close is non-blocking but guaranteed: MarkForClose() sets the reply-builder error, so the
  // AsyncFiber loop tears the connection down once the parked send returns. RequestPubsubClose runs
  // at most once per connection (further pub messages are dropped via request_shutdown_), so the
  // disconnect is counted exactly once here.
  auto& bp = tl_facade_stats->conn_stats.pubsub_backpressure;
  ++bp.forced_disconnect;
  bp.messages_discarded += discarded_msgs;

  // Rate-limited structured diagnostic. Bounded fields only - the connection id goes to the log,
  // not to a metric label.
  LOG_EVERY_T(WARNING, 1) << "Closing slow Pub/Sub subscriber " << DebugInfo()
                          << " tid=" << ProactorBase::me()->GetPoolIndex()
                          << " publish_buffer_limit=" << qbp.publish_buffer_limit
                          << " thread_subscriber_bytes="
                          << qbp.subscriber_bytes.load(memory_order_relaxed)
                          << " blocked_send_usec="
                          << base::CycleClock::ToUsec(base::CycleClock::Now() -
                                                      async_op_start_cycle_)
                          << " discarded_entries=" << discarded_msgs
                          << " discarded_bytes=" << discarded_bytes;

  // Non-blocking: only mark the reply builder error and request_shutdown_.
  MarkForClose();

  // The subscriber budget just dropped - wake every parked publisher so they can re-check.
  qbp.pubsub_ec.notifyAll();
}

void Connection::SendPubMessageAsync(PubMessage msg) {
  // Slow-subscriber protection (RESP V1): once a close was requested, drop further pub messages
  // instead of repopulating the queue. Cleanup releases accounting and wakes publishers.
  if (request_shutdown_)
    return;

  // Two conjunctive conditions: the thread is in a soft-limit back-pressure episode AND this
  // connection's active Pub/Sub send has been blocked continuously past the configured deadline.
  // A slow send below the budget, or a full budget with a still-progressing send, is left alone.
  uint64_t timeout_cycles = pubsub_slow_subscriber_timeout_cycles_cached;
  if (timeout_cycles > 0 && GetQueueBackpressure().IsSubscriberSoftLimited() &&
      IsAsyncOpOverdue(timeout_cycles)) {
    RequestPubsubClose();
    return;  // discard the message that triggered the close
  }

  SendAsync({make_unique<PubMessage>(std::move(msg))});
}

void Connection::SendMonitorMessageAsync(string msg) {
  SendAsync({MonitorMessage{std::move(msg)}});
}

void Connection::SendCheckpoint(fb2::BlockingCounter bc, bool ignore_paused, bool ignore_blocked) {
  // Only send a checkpoint if the connection has an "active" command the tracker must wait for:
  // 1) It is dispatching now, OR
  // 2) V2 only - an async command is still in flight after DispatchCommandSimple already returned.
  // In that V2 use case: IsCurrentlyDispatching() is already false, so without checking
  // HasInFlightCommands() the checkpoint would be skipped and CLIENT PAUSE / DispatchTracker
  // could return before the write is done.
  bool has_active_command = IsCurrentlyDispatching() || (ioloop_v2_ && HasInFlightCommands());
  if (!has_active_command)
    return;

  if (cc_->paused && ignore_paused)
    return;

  if (cc_->blocked && ignore_blocked)
    return;

  VLOG(2) << CONN_ID << "Sent checkpoint to " << DebugInfo();

  bc->Add(1);
  SendAsync({CheckpointMessage{bc}});
}

void Connection::SendInvalidationMessageAsync(InvalidationMessage msg) {
  SendAsync({std::move(msg)});
}

void Connection::LaunchAsyncFiberIfNeeded() {
  DCHECK(!ioloop_v2_);
  if (!async_fb_.IsJoinable() && !migration_in_process_) {
    VLOG(1) << CONN_ID << "LaunchAsyncFiberIfNeeded ";
    async_fb_ = fb2::Fiber(fb2::Launch::post, "connection_dispatch", [this]() { AsyncFiber(); });
  }
}

// SendAsync is now strictly for the Control Path (Admin/Events).
// Pipeline commands are handled separately via EnqueueParsedCommand to maintain
// clean separation between Data and Control paths.
// Note: Should never block - the callers may run in as a brief callback.
void Connection::SendAsync(MessageHandle msg) {
  DCHECK(cc_);
  DCHECK(listener());
  DCHECK_EQ(ProactorBase::me(), socket_->proactor());
  auto& conn_stats = tl_facade_stats->conn_stats;

  // A closing connection drops control messages - nothing consumes them. The one exception is a
  // checkpoint with a live V1 async fiber (V2 never qualifies, so we don't read async_fb_ there).
  auto* checkpoint = std::get_if<CheckpointMessage>(&msg.handle);
  if (cc_->conn_closing) {
    const bool checkpoint_on_live_async_fiber = checkpoint && !ioloop_v2_ && async_fb_.IsJoinable();
    if (!checkpoint_on_live_async_fiber) {
      // Dec() the dropped checkpoint's blocking counter (SendCheckpoint already Add()-edit) or the
      // DispatchTracker leaks and the takeover / pause / migration / shutdown waiting on it hangs.
      if (checkpoint) {
        DVLOG(1) << CONN_ID << "Dropping checkpoint on closing conn " << DebugInfo();
        checkpoint->bc->Dec();
      }
      return;
    }
  }

  // A checkpoint on a closing V2 connection is always dropped-and-healed above, never enqueued
  // here.
  DCHECK(!(checkpoint && cc_->conn_closing && ioloop_v2_));

  // If we launch while closing, it won't be awaited. Control messages will be processed on cleanup.
  if (!cc_->conn_closing && !ioloop_v2_) {
    LaunchAsyncFiberIfNeeded();
  }
  DCHECK_NE(phase_, PRECLOSE);  // No more messages are processed after this point

  // Close MONITOR connection if we overflow limits.
  // We must check the Thread-Global memory usage of BOTH:
  // 1. The Control Path (dispatch_queue_bytes)
  // 2. The Data Path (pipeline_queue_bytes)
  if (msg.IsMonitor()) {
    if (GetQueueBackpressure().IsPipelineBufferOverLimit(
            conn_stats.dispatch_queue_bytes + conn_stats.pipeline_queue_bytes,
            GetPendingMessageCount())) {
      cc_->conn_closing = true;
      request_shutdown_ = true;
      // We don't shutdown here. The reason is that TLS socket is preemptive
      // and SendAsync is atomic.
      cnd_.notify_one();
      return;
    }
  }

  local_stats_.dispatch_entries_added++;
  UpdateDispatchStats(msg, true /* add */);
  msg.dispatch_cycle = CycleClock::Now();

  // Admin Queueing Rules:
  // Checkpoints go to the front (after existing checkpoints), while all others to the back.
  bool had_pending_messages = HasPendingMessages();  // check the queues before enqueuing
  if (msg.IsCheckPoint()) {
    auto it = dispatch_q_.begin();
    while (it < dispatch_q_.end() && it->IsCheckPoint())
      ++it;
    dispatch_q_.insert(it, std::move(msg));
  } else {
    dispatch_q_.push_back(std::move(msg));
  }

  // Control Path Notification:

  // TODO: Poissbily optimize wakeups
  if (ioloop_v2_) {
    io_event_.notify();
    return;
  }

  // We need to wake up the AsyncFiber only if it is currently sleeping.
  // 1. Memcache: Sleeps if dispatch_q_ is empty. Must notify on 0->1 transition.
  // 2. Redis: Sleeps if BOTH queues are empty. If pipeline has items, it's already awake.
  bool should_notify = false;
  if (protocol_ == Protocol::REDIS) {
    if (!had_pending_messages) {
      should_notify = true;
    }
  } else {  // MEMCACHE
    should_notify = (dispatch_q_.size() == 1);
  }

  if (should_notify && !cc_->sync_dispatch) {
    cnd_.notify_one();
  }
}

void Connection::UpdateDispatchStats(const MessageHandle& msg, bool add) {
  size_t mem = msg.UsedMemory();
  auto& qbp = GetQueueBackpressure();
  auto& conn_stats = tl_facade_stats->conn_stats;
  if (add) {
    conn_stats.dispatch_queue_entries++;
    conn_stats.dispatch_queue_bytes += mem;
    dispatch_q_bytes_ += mem;
    if (msg.IsPubMsg()) {
      qbp.AddSubscriberBytes(mem);
      conn_stats.dispatch_queue_subscriber_bytes += mem;
      dispatch_q_subscriber_bytes_ += mem;
    }
  } else {
    DCHECK_GT(conn_stats.dispatch_queue_entries, 0u);
    DCHECK_GE(conn_stats.dispatch_queue_bytes, mem);
    conn_stats.dispatch_queue_entries--;
    conn_stats.dispatch_queue_bytes -= mem;
    dispatch_q_bytes_ -= mem;
    if (msg.IsPubMsg()) {
      DCHECK_GE(conn_stats.dispatch_queue_subscriber_bytes, mem);
      DCHECK_GE(qbp.subscriber_bytes.load(std::memory_order_relaxed), mem);
      qbp.SubSubscriberBytes(mem);
      conn_stats.dispatch_queue_subscriber_bytes -= mem;
      dispatch_q_subscriber_bytes_ -= mem;
    }
  }
}

std::string Connection::LocalBindStr() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto le = socket_->LocalEndpoint();
  return absl::StrCat(le.address().to_string(), ":", le.port());
}

std::string Connection::LocalBindAddress() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto le = socket_->LocalEndpoint();
  return le.address().to_string();
}

std::string Connection::RemoteEndpointStr() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto re = socket_->RemoteEndpoint();
  return absl::StrCat(re.address().to_string(), ":", re.port());
}

std::string Connection::RemoteEndpointAddress() const {
  if (socket_->IsUDS())
    return "unix-domain-socket";

  auto re = socket_->RemoteEndpoint();
  return re.address().to_string();
}

facade::ConnectionContext* Connection::cntx() {
  return cc_.get();
}

void Connection::RequestAsyncMigration(util::fb2::ProactorBase* dest, bool force) {
  if ((!force && !migration_enabled_) || cc_ == nullptr) {
    return;
  }

  // Connections can migrate at most once.
  migration_enabled_ = false;
  migration_request_ = dest;

  // Wake up the V2 loop so it can immediately process the migration request.
  if (ioloop_v2_) {
    io_event_.notify();
  }
}

Connection::StartTrafficResult Connection::StartTrafficLogging(string_view path,
                                                               ListenerType listener_type) {
  return OpenTrafficLogger(path, listener_type);
}

void Connection::StopTrafficLogging() {
  lock_guard lk(tl_traffic_logger.mutex);
  tl_traffic_logger.ResetLocked();
}

void Connection::LogReplicaCommand(const cmn::BackedArguments& args, uint32_t db_index) {
  // Contract: LSN/PING opcodes are filtered before ExecuteTx, and
  // COMMAND/EXPIRED journal entries always carry at least a command name.
  DCHECK(!args.empty());
  // Fast-path gate: cheap thread-local reads without the mutex. If the logger
  // was swapped out concurrently, LogTrafficParts re-checks `log_file` inside
  // the lock so at worst we do a bit of wasted work (building `parts`).
  // id=0 is a synthetic client id — replication has no connection/client of its
  // own, and callers on the same fiber serialise naturally.
  if (!tl_traffic_logger.log_file ||
      tl_traffic_logger.listener_type != ListenerType::REPLICA_RESP) {
    return;
  }
  absl::InlinedVector<string_view, 16> parts;
  parts.reserve(args.size());
  for (auto v : args.view())
    parts.push_back(v);
  LogTrafficParts(/*id=*/0, /*has_more=*/false, db_index, absl::MakeSpan(parts));
}

bool Connection::IsHttp() const {
  return is_http_;
}

size_t Connection::GetMemoryUsage() const {
  size_t mem = sizeof(*this) + cmn::HeapSize(name_) + cmn::HeapSize(memcache_parser_) +
               cmn::HeapSize(redis_parser_) + cmn::HeapSize(cc_) + cmn::HeapSize(reply_builder_);

  // parsed_cmd_ can be null when dispatching a command, or for http connections.
  if (parsed_cmd_) {
    mem += UsedMemoryInternal(*parsed_cmd_);
  }

  // We add a hardcoded 9k value to accommodate for the part of the Fiber stack that is in use.
  // The allocated stack is actually larger (~130k), but only a small fraction of that (9k
  // according to our checks) is actually part of the RSS.
  mem += 9'000;

  return mem;
}

void Connection::RefreshConnectionMemoryUsage() {
  if (!conn_stats_registered_)
    return;

  DCHECK(socket());
  DCHECK_EQ(socket()->proactor(), ProactorBase::me());

  size_t current = account_connection_memory_ ? GetMemoryUsage() : 0;
  ConnectionStats& conn_stats = GetLocalConnStats();

  if (current >= accounted_connection_memory_bytes_) {
    conn_stats.connection_memory_bytes += current - accounted_connection_memory_bytes_;
  } else {
    const size_t delta = accounted_connection_memory_bytes_ - current;
    if (ABSL_PREDICT_FALSE(delta > conn_stats.connection_memory_bytes)) {
      LOG(DFATAL) << CONN_ID << "Connection memory accounting underflow: total="
                  << conn_stats.connection_memory_bytes << " delta=" << delta;
      conn_stats.connection_memory_bytes = 0;
    } else {
      conn_stats.connection_memory_bytes -= delta;
    }
  }

  accounted_connection_memory_bytes_ = current;
}

void Connection::SetConnectionMemoryAccounting(bool enabled) {
  if (account_connection_memory_ == enabled)
    return;

  account_connection_memory_ = enabled;
  // reduce memory to 0 and tl stat contribution to 0 on enabled=false
  RefreshConnectionMemoryUsage();
}

void Connection::IncreaseConnStats() {
  DCHECK(tl_facade_stats);
  DCHECK(!conn_stats_registered_);
  DCHECK(socket());
  DCHECK_EQ(socket()->proactor(), ProactorBase::me());
  auto& conn_stats = tl_facade_stats->conn_stats;
  if (IsMainOrMemcache())
    ++conn_stats.num_conns_main;
  else
    ++conn_stats.num_conns_other;
  conn_stats.read_buf_capacity += io_buf_.Capacity();

  conn_stats.dispatch_queue_entries += dispatch_q_.size();
  conn_stats.dispatch_queue_bytes += dispatch_q_bytes_;
  conn_stats.pipeline_queue_entries += parsed_cmd_q_len_;
  conn_stats.pipeline_queue_bytes += parsed_cmd_q_bytes_;
  if (dispatch_q_subscriber_bytes_ > 0) {
    auto& qbp = GetQueueBackpressure();
    conn_stats.dispatch_queue_subscriber_bytes += dispatch_q_subscriber_bytes_;
    qbp.AddSubscriberBytes(dispatch_q_subscriber_bytes_);
  }

  conn_stats_registered_ = true;
  accounted_connection_memory_bytes_ = account_connection_memory_ ? GetMemoryUsage() : 0;
  conn_stats.connection_memory_bytes += accounted_connection_memory_bytes_;
}

void Connection::DecreaseConnStats() {
  DCHECK(tl_facade_stats);
  DCHECK(conn_stats_registered_);
  auto& conn_stats = tl_facade_stats->conn_stats;
  RefreshConnectionMemoryUsage();
  if (ABSL_PREDICT_FALSE(accounted_connection_memory_bytes_ > conn_stats.connection_memory_bytes)) {
    LOG(DFATAL) << CONN_ID << "Connection memory accounting underflow on unregister: total="
                << conn_stats.connection_memory_bytes
                << " delta=" << accounted_connection_memory_bytes_;
    conn_stats.connection_memory_bytes = 0;
  } else {
    conn_stats.connection_memory_bytes -= accounted_connection_memory_bytes_;
  }
  accounted_connection_memory_bytes_ = 0;
  conn_stats_registered_ = false;

  if (IsMainOrMemcache()) {
    DCHECK_GT(conn_stats.num_conns_main, 0u);
    --conn_stats.num_conns_main;
  } else {
    DCHECK_GT(conn_stats.num_conns_other, 0u);
    --conn_stats.num_conns_other;
  }
  DCHECK_GE(conn_stats.read_buf_capacity, io_buf_.Capacity());
  conn_stats.read_buf_capacity -= io_buf_.Capacity();

  DCHECK_GE(conn_stats.dispatch_queue_entries, dispatch_q_.size());
  conn_stats.dispatch_queue_entries -= dispatch_q_.size();
  DCHECK_GE(conn_stats.dispatch_queue_bytes, dispatch_q_bytes_);
  conn_stats.dispatch_queue_bytes -= dispatch_q_bytes_;
  if (dispatch_q_subscriber_bytes_ > 0) {
    auto& qbp = GetQueueBackpressure();
    DCHECK_GE(conn_stats.dispatch_queue_subscriber_bytes, dispatch_q_subscriber_bytes_);
    conn_stats.dispatch_queue_subscriber_bytes -= dispatch_q_subscriber_bytes_;
    DCHECK_GE(qbp.subscriber_bytes.load(std::memory_order_relaxed), dispatch_q_subscriber_bytes_);
    qbp.SubSubscriberBytes(dispatch_q_subscriber_bytes_);
  }
  DCHECK_GE(conn_stats.pipeline_queue_entries, parsed_cmd_q_len_);
  conn_stats.pipeline_queue_entries -= parsed_cmd_q_len_;
  DCHECK_GE(conn_stats.pipeline_queue_bytes, parsed_cmd_q_bytes_);
  conn_stats.pipeline_queue_bytes -= parsed_cmd_q_bytes_;
}

void Connection::BreakOnce(uint32_t ev_mask) {
  if (cc_) {
    cc_->OnSocketError(ev_mask);
  }
}

bool Connection::IsReplySizeOverLimit() const {
  std::atomic<size_t>& reply_sz = tl_facade_stats->reply_stats.squashing_current_reply_size;
  size_t current = reply_sz.load(std::memory_order_acquire);
  const bool over_limit = reply_size_limit != 0 && current > 0 && current > reply_size_limit;
  if (over_limit) {
    LOG_EVERY_T(INFO, 10) << CONN_ID
                          << "Commands squashing current reply size is overlimit: " << current
                          << "/" << reply_size_limit
                          << ". Falling back to single command dispatch (instead of squashing)";
    // Used by testing. Should not be used in production, therefore debug log level 5.
    DVLOG(5) << CONN_ID << "Commands squashing current reply size is overlimit: " << current << "/"
             << reply_size_limit
             << ". Falling back to single command dispatch (instead of squashing)";
  }
  return over_limit;
}

Connection::ParserStatus Connection::ParseRedisBatch(base::IoBuf& buf) {
  if (IsOverPipelineLimit()) {
    // Signal ParseLoop to stop (NEED_MORE, not an error). IoLoopV2 will drain before resuming.
    DVLOG(2) << CONN_ID << "Pipeline buffer over limit. Avoid parsing Redis batch.";
    GetLocalConnStats().pipeline_throttle_count++;
    return NEED_MORE;
  }
  // Forward ParseRedis's status verbatim (OK / NEED_MORE / ERROR) so a protocol error
  // propagates to ParseLoop instead of being flattened into "no commands parsed".
  return ParseRedis(buf, max_busy_read_cycles_cached, /*enqueue_only=*/true);
}

Connection::ParserStatus Connection::ParseMCBatch(base::IoBuf& io_buf) {
  CHECK(io_buf.InputLen() > 0);

  do {
    if (parsed_cmd_ == nullptr) {
      // Happens with pipelined commands after the first one.
      PipelineMessagePtr ptr = GetFromPoolOrCreate();
      parsed_cmd_ = ptr.release();
    }
    uint32_t consumed = 0;
    memcache_parser_->set_last_unix_time(time(nullptr));
    MemcacheParser::Result result = memcache_parser_->Parse(io::View(io_buf.InputBuffer()),
                                                            &consumed, parsed_cmd_->mc_command());
    io_buf.ConsumeInput(consumed);

    DVLOG(2) << CONN_ID << "mc_result " << unsigned(result) << " consumed: " << consumed << " type "
             << unsigned(parsed_cmd_->mc_command()->type);
    if (result == MemcacheParser::INPUT_PENDING) {
      RefreshConnectionMemoryUsage();
      return NEED_MORE;
    }

    if (result == MemcacheParser::OK && tl_traffic_logger.log_file &&
        tl_traffic_logger.listener_type == listener_type_) {
      bool has_more = io_buf_.InputLen() > 0;
      unsigned db_index = service_->GetContextInfo(cc_.get()).db_index;
      LogMemcacheTraffic(id_, has_more, *parsed_cmd_->mc_command(), db_index);
    }

    // We push the command to the parsed queue even in case of parse errors,
    // so that we can reply in order.
    EnqueueParsedCommand(parsed_cmd_);
    parsed_cmd_ = nullptr;  // ownership transferred.

    if (result != MemcacheParser::OK) {
      // We can not just reply directly to parse error, as we may have pipelined commands before.
      // Fill the reply_payload into parsed_tail_ with the error and continue parsing.
      memcache_parser_->Reset();
      // TODO(vlad): Use Proper SendError calls instead of SendSimpleString and error building
      auto client_error = [](string_view msg) { return absl::StrCat("CLIENT_ERROR ", msg); };

      parsed_tail_->SetDeferredReply();
      switch (result) {
        case MemcacheParser::UNKNOWN_CMD:
          parsed_tail_->SendSimpleString("ERROR");
          break;
        case MemcacheParser::PARSE_ERROR:
          parsed_tail_->SendSimpleString(client_error("bad data chunk"));
          break;
        case MemcacheParser::BAD_DELTA:
          parsed_tail_->SendSimpleString(client_error("invalid numeric delta argument"));
          break;
        default:
          parsed_tail_->SendSimpleString(client_error("bad command line format"));
          break;
      }
    }
    RefreshConnectionMemoryUsage();
  } while (parsed_cmd_q_len_ < 128 && io_buf.InputLen() > 0);
  // Memcache parse errors are turned into deferred replies above, so we never return ERROR here.
  return OK;
}

bool Connection::SquashPipelineV2() {
  // vectorized squash phase: pack multiple commands and dispatch at once.
  // dispatch_waiting_count_ is the exact length of the run starting at parsed_to_execute_, so the
  // squash works even when earlier commands are still in flight.
  auto& conn_stats = tl_facade_stats->conn_stats;

  uint64_t dispatch_start = CycleClock::Now();
  fiber_park_spot_ = FiberParkSpot::kSquashHop;
  // DispatchSquashedBatch is synchronous - raise sync_dispatch so DispatchTracker sees this
  // connection as dispatching and waits for these in-flight commands to finish.
  // Invariant: clear on entry - only reached from the V2 loop between per-command dispatches.
  DCHECK(!cc_->sync_dispatch);
  cc_->sync_dispatch = true;
  unsigned squashed =
      service_->DispatchSquashedBatch(parsed_to_execute_, dispatch_waiting_count_, cc_.get());
  cc_->sync_dispatch = false;
  fiber_park_spot_ = FiberParkSpot::kNone;

  if (squashed == 0)
    return false;

  // Like V1's SquashPipeline, sample once before the blocking squash and attribute it to every
  // squashed command's parse->dispatch wait.
  for (unsigned i = 0; i < squashed; i++) {
    auto usec = CycleClock::ToUsec(dispatch_start - parsed_to_execute_->parsed_cycle);
    conn_stats.pipelined_wait_latency += usec;
    AdvanceToExecute();
  }

  conn_stats.pipeline_dispatch_calls++;
  conn_stats.pipeline_dispatch_commands += squashed;
  local_stats_.cmds += squashed;
  last_interaction_ = time(nullptr);
  return true;
}

bool Connection::ExecuteBatch() {
  // Invariant: batched_ must be false on entry.
  // Both ReplyBatch() and ExecuteBatch() reset it via absl::Cleanup guards on all return paths.
  DCHECK(!reply_builder_->IsBatchMode());

  if (parsed_to_execute_ == nullptr) {
    return true;  // no errors.
  }

  ConnectionMemoryTracker memory_tracker(this);
  absl::Cleanup batch_guard = [this] { reply_builder_->SetBatchMode(false); };
  auto& conn_stats = tl_facade_stats->conn_stats;

  bool is_true_pipeline = (parsed_to_execute_->next) != nullptr;

  // Retires the head command once its reply has been handled: removes it from the queue and
  // releases it. Only reached when the head is also the command we just processed
  // (parsed_head_ == parsed_to_execute_), so both pointers advance together to head->next.
  auto retire_head = [&] {
    DCHECK_EQ(parsed_head_, parsed_to_execute_);
    ParsedCommand* cmd = parsed_head_;
    // Advance both pointers to cmd->next in lockstep: AdvanceToExecute() moves parsed_to_execute_
    // (keeping dispatch_waiting_count_ in sync) and returns the new value for parsed_head_.
    AdvanceParsedHead(AdvanceToExecute());
    DCHECK_EQ(parsed_head_, parsed_to_execute_);
    if (is_true_pipeline)
      ReleasePipelinedCommand(cmd);
    else
      ReleaseParsedCommand(cmd);
  };

  // Execute sequentially all parsed commands. parsed_to_execute_ points to the next command to
  // dispatch; it advances as commands are dispatched, and parsed_head_ advances with it whenever a
  // command retires (executes synchronously or replies immediately).
  DVLOG(2) << CONN_ID << "ExecuteBatch: " << dispatch_waiting_count_ << " commands ";

  while (parsed_to_execute_ != nullptr) {
    if (reply_builder_->GetError())
      return false;

    if (pipeline_squashing_v2_ && dispatch_waiting_count_ > 1) {
      // if we squashed any commands, continue the loop to check if there are
      // non-squashable commands to process.
      DVLOG(2) << CONN_ID << "Squashing pipeline " << dispatch_waiting_count_ << " commands "
               << pending_input_ << " " << io_buf_.InputLen();

      if (SquashPipelineV2()) {
        // - This helps with throughput. Explanation:
        //   when we suspend the thread calls io-callbacks that fill up the input buffer.
        //   By breaking now we give the io-loop a chance to add more commands to the pipeline.
        // - Skip the break when parse-in-proactor is on: the proactor already parsed those bytes
        //   into the queue during the squash wait, so keep squashing in place instead.
        if (!pipeline_parse_in_proactor_cached && (pending_input_ || io_buf_.InputLen() > 0))
          break;
        continue;
      }
    }

    ParsedCommand* cmd = parsed_to_execute_;
    bool is_head = cmd == parsed_head_;

    // parser errors are stored as deferred replies
    if (cmd->IsDeferredReply() && cmd->CanReply()) {
      if (is_head) {
        cmd->SendReply();
        retire_head();
      } else {
        AdvanceToExecute();
      }
      continue;
    }

    // We must continue with async execution if we already have executing commands
    auto mode = is_head ? AsyncPreference::PREFER_ASYNC : AsyncPreference::ONLY_ASYNC;

    if (!ioloop_v2_)  // only v2 loop supports any async commands so far
      mode = AsyncPreference::ONLY_SYNC;

    // V2: Batch the head command's reply when more commands are queued behind it.
    // This prevents sync-only commands from triggering immediate flushes, keeping
    // sendmsg syscalls to a minimum. IoLoopV2's idle-await block handles the final flush.
    reply_builder_->SetBatchMode(ioloop_v2_ && is_head && (cmd->next != nullptr));

    // We set TWO flags because they feed two different, independent consumers (see conn_context.h);
    // one flag cannot serve both:
    // - sync_dispatch (every command): inflight-dispatch tracking (conn_context.h #1).
    //   IsCurrentlyDispatching() must be true so a CLIENT PAUSE / REPLTAKEOVER / migration started
    //   mid-flight waits for this command. A lone V2 command sets no other flag, so without it the
    //   write can slip past CLIENT PAUSE.
    // - async_dispatch (flush-before-block): makes MainService::DispatchCommand flush the buffered
    //   pipeline replies before a blocking command (e.g. BLPOP) parks the fiber. Gating it avoids a
    //   bogus flush / blocking-in-pipeline stat bump for a lone blocking command.
    // V1 sets these flags elsewhere (DispatchSingle / the AsyncFiber paths), hence the ioloop_v2_
    // guard.
    //
    // Note: an async command may still be in flight after this returns, yet sync_dispatch is
    // cleared right below - so a CLIENT PAUSE / DispatchTracker sampling that window would see the
    // connection idle. That gap is closed in AsyncOperations::operator()(CheckpointMessage) and
    // SendCheckpoint() by deferring the checkpoint's bc->Dec().
    if (ioloop_v2_) {
      // Invariant: both are clear on entry - the single V2 fiber never dispatches while already
      // dispatching, and every path above resets them before the next command is reached.
      DCHECK(!cc_->sync_dispatch);
      DCHECK(!cc_->async_dispatch);
      cc_->sync_dispatch = true;
      cc_->async_dispatch = is_true_pipeline;
    }
    uint64_t dispatch_start = CycleClock::Now();
    auto dispatch_res = service_->DispatchCommandSimple(cmd, mode);
    if (ioloop_v2_) {
      cc_->sync_dispatch = false;
      cc_->async_dispatch = false;
    }
    // Enforce the pipeline reply-ordering invariant: replies must reach the socket in parse order.
    // V1 (ONLY_SYNC): all commands run serially, so parsed_to_execute_ always equals parsed_head_
    //    (is_head is always true). The invariant holds trivially.
    // V2 (async dispatch): the head may be an in-flight async command while later commands are
    //    dispatched behind it. For those non-head commands the state must satisfy ONE of:
    //    - dispatch_res != WOULD_BLOCK: ran async, must have buffered the reply locally
    //      (is_deferred == true) to avoid corrupting the in-order socket stream.
    //    - dispatch_res == WOULD_BLOCK: requires blocking, must NOT have buffered a reply
    //      (is_deferred == false) - we break and wait for it to become head first.
    bool is_deferred = cmd->IsDeferredReply();
    DCHECK(is_head || (is_deferred == (dispatch_res != DispatchResult::WOULD_BLOCK)))
        << "Pipeline contract breach! Invalid state for non-head command. "
        << "DispatchResult: " << static_cast<int>(dispatch_res) << ", IsDeferred: " << is_deferred;

    if (dispatch_res == DispatchResult::WOULD_BLOCK)
      break;  // Sync command. Wait for current async commands to finish

    conn_stats.pipelined_wait_latency += CycleClock::ToUsec(dispatch_start - cmd->parsed_cycle);
    conn_stats.pipeline_dispatch_commands++;
    if (is_head)
      conn_stats.pipeline_dispatch_calls++;
    local_stats_.cmds++;
    last_interaction_ = time(nullptr);

    if (cmd->IsDeferredReply()) {
      AdvanceToExecute();
    } else {
      DCHECK(is_head);  // only head can execute sync
      retire_head();
    }
  }

  return true;
}

bool Connection::ReplyBatch() {
  ConnectionMemoryTracker memory_tracker(this);
  reply_builder_->SetBatchMode(true);
  absl::Cleanup batch_guard = [this] { reply_builder_->SetBatchMode(false); };

  // Use single ReplyScope for all commands. We can release commands only after the scope finishes
  // as references become invalidate otherwise
  ParsedCommand* release_head = parsed_head_;
  unsigned replied = 0;
  {
    SinkReplyBuilder::ReplyScope scope(reply_builder_.get());
    while (HasInFlightCommands() && parsed_head_->CanReply()) {
      current_wait_.reset();  // Clear the subscription before moving to the next command
      auto* cmd = parsed_head_;

      // Pure coroutine replies don't preserve lifetimes
      const bool suspended = cmd->IsSuspendedReply();
      std::optional<SinkReplyBuilder::ScopePause> pause;
      if (suspended)
        pause.emplace(reply_builder_.get());

      // V2 in-flight tracking:
      // - A command isn't "done" until its reply is written. A suspended (coroutine) reply can do
      //   real work on resume (e.g. a tiered value's disk read) and fiber-block inside SendReply().
      // - We keep parsed_head_ pointing at this command across SendReply() and only advance it
      //   AFTERWARDS. That way HasInFlightCommands() stays true for the whole reply-write, so a
      //   CLIENT PAUSE / DispatchTracker sampling mid-resume still sees the connection busy (via
      //   SendCheckpoint's HasInFlightCommands() check) and waits for the write to land.
      // - A non-suspended reply just copies an already-built payload and can't preempt.
      cmd->SendReply();
      AdvanceParsedHead(cmd->next);
      replied++;

      if (reply_builder_->GetError())
        break;
    }
  }

  // Release all the commands that replied
  for (unsigned i = 0; i < replied; ++i) {
    auto* next = release_head->next;
    ReleasePipelinedCommand(release_head);
    release_head = next;
  }

  if (reply_builder_->GetError())
    return false;

  // V1: handles its pipeline batching inside AsyncFiber, so it flushes unconditionally here.
  //
  // V2: operates as a single-fiber event loop where reading, parsing, and executing happen
  // sequentially. Because ParseLoop processes pipelines in chunks, flushing here would trigger a
  // sendmsg syscall for every single chunk. Instead, V2 delegates flushing to IoLoopV2, which
  // safely flushes the coalesced buffer right before the fiber yields (await) or when memory limits
  // are reached.
  if (!ioloop_v2_) {
    reply_builder_->Flush();
  }

  return !reply_builder_->GetError();
}

ParsedCommand* Connection::CreateParsedCommand() {
  auto* res = service_->AllocateParsedCommand();
  res->Init(reply_builder_.get(), cc_.get());
  res->ConfigureMCExtension(protocol_ == Protocol::MEMCACHE);
  return res;
}

void Connection::EnqueueParsedCommand(ParsedCommand* cmd) {
  DCHECK(cmd);
  cmd->next = nullptr;
  auto& conn_stats = tl_facade_stats->conn_stats;

  cmd->FinalizeParsing();

  if (parsed_head_ == nullptr) {
    parsed_head_ = cmd;
    parsed_to_execute_ = cmd;
  } else {
    parsed_tail_->next = cmd;
    if (parsed_to_execute_ == nullptr) {
      // we've executed all the parsed commands so far.
      parsed_to_execute_ = cmd;
    }
  }
  parsed_tail_ = cmd;

  size_t used_mem = cmd->UsedMemory();
  parsed_cmd_q_len_++;
  dispatch_waiting_count_++;  // the newly appended tail command is not yet dispatched
  parsed_cmd_q_bytes_ += used_mem;
  local_stats_.dispatch_entries_added++;
  conn_stats.pipeline_queue_entries++;
  conn_stats.pipeline_queue_bytes += used_mem;

  // AsyncFiber for Memcache only wakes up on dispatch_q_, notify only redis as this is the parse
  // commands queue.
  if ((!cc_->sync_dispatch) && (protocol_ == Protocol::REDIS)) {
    cnd_.notify_one();
  }
}

void Connection::ReleasePipelinedCommand(ParsedCommand* cmd) {
  auto& conn_stats = tl_facade_stats->conn_stats;
  conn_stats.pipelined_cmd_cnt++;
  uint64_t latency_usec = CycleClock::ToUsec(CycleClock::Now() - cmd->parsed_cycle);
  conn_stats.pipelined_cmd_latency += latency_usec;
  conn_stats.pipelined_latency_hist.Add(latency_usec);
  // Decay the histogram every kPipelineLatencyDecayPeriod samples to
  // approximate a moving-window distribution; older observations contribute
  // half as much after each decay period.
  constexpr uint64_t kPipelineLatencyDecayPeriod = 1 << 14;  // 16384
  if ((conn_stats.pipelined_latency_hist.count() & (kPipelineLatencyDecayPeriod - 1)) == 0) {
    conn_stats.pipelined_latency_hist.Decay();
  }

  ReleaseParsedCommand(cmd);
}

void Connection::ReleaseParsedCommand(ParsedCommand* cmd) {
  size_t used_mem = cmd->UsedMemory();
  auto& conn_stats = tl_facade_stats->conn_stats;

  DCHECK_GT(parsed_cmd_q_len_, 0u);
  DCHECK_GE(parsed_cmd_q_bytes_, used_mem);
  DCHECK_GT(conn_stats.pipeline_queue_entries, 0u);
  DCHECK_GE(conn_stats.pipeline_queue_bytes, used_mem);
  parsed_cmd_q_len_--;
  parsed_cmd_q_bytes_ -= used_mem;

  conn_stats.pipeline_queue_entries--;
  conn_stats.pipeline_queue_bytes -= used_mem;

  if (parsed_cmd_ == nullptr) {
    parsed_cmd_ = cmd;
    parsed_cmd_->ResetForReuse();
  } else {
    // If we are over the limit, destroy the command instead of caching it.
    size_t cmd_mem = UsedMemoryInternal(*cmd);
    QueueBackpressure& qbp = GetQueueBackpressure();
    if (conn_stats.pipeline_cmd_cache_bytes + cmd_mem <= qbp.pipeline_cache_limit) {
      conn_stats.pipeline_cmd_cache_bytes += cmd_mem;
      pipeline_req_pool_.emplace_back(cmd);
    } else {
      delete cmd;
    }
  }
}

void Connection::AdjustParsedCmdBytes(ssize_t delta) {
  if (parsed_cmd_q_bytes_ == 0)
    return;  // command dispatched synchronously, not in pipeline queue
  auto& conn_stats = tl_facade_stats->conn_stats;
  DCHECK_GE(static_cast<ssize_t>(parsed_cmd_q_bytes_) + delta, 0);
  DCHECK_GE(static_cast<ssize_t>(conn_stats.pipeline_queue_bytes) + delta, 0);
  parsed_cmd_q_bytes_ += delta;
  conn_stats.pipeline_queue_bytes += delta;
}

void Connection::DestroyParsedQueue() {
  ConnectionMemoryTracker memory_tracker(this);
  while (parsed_head_ != nullptr) {
    auto* cmd = parsed_head_;
    parsed_head_ = cmd->next;

    // Being able to drop an in-flight transaction would require it keeping no pointers
    // at all to any context data - too costly for now! (maybe let it own the arguments?)
    if (cmd->IsDeferredReply() && !cmd->CanReply())
      cmd->Blocker()->Wait();  // explicitly wait for it to finish
    ReleaseParsedCommand(cmd);
  }

  parsed_tail_ = nullptr;
  parsed_to_execute_ = nullptr;
  dispatch_waiting_count_ = 0;
  CHECK_EQ(parsed_cmd_q_len_, 0u);
  CHECK_EQ(parsed_cmd_q_bytes_, 0u);
  delete parsed_cmd_;
  parsed_cmd_ = nullptr;
}

ParsedCommand* Connection::AdvanceToExecute() {
  DCHECK(parsed_to_execute_ != nullptr);
  DCHECK_GT(dispatch_waiting_count_, 0u);

  --dispatch_waiting_count_;
  parsed_to_execute_ = parsed_to_execute_->next;
  return parsed_to_execute_;
}

void Connection::UpdateFromFlags() {
  unsigned tid = fb2::ProactorBase::me()->GetPoolIndex();
  thread_queue_backpressure[tid].pipeline_queue_max_len = GetFlag(FLAGS_pipeline_queue_limit);
  thread_queue_backpressure[tid].pipeline_buffer_limit = GetFlag(FLAGS_pipeline_buffer_limit);
  thread_queue_backpressure[tid].NotifyPipelineWaiters();

  max_busy_read_cycles_cached = base::CycleClock::FromUsec(GetFlag(FLAGS_max_busy_read_usec));
  always_flush_pipeline_cached = GetFlag(FLAGS_always_flush_pipeline);
  pipeline_squash_limit_cached = GetFlag(FLAGS_pipeline_squash_limit);
  pipeline_wait_batch_usec = GetFlag(FLAGS_pipeline_wait_batch_usec);
}

std::vector<std::string> Connection::GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_pipeline_queue_limit, FLAGS_pipeline_buffer_limit,
                            FLAGS_max_busy_read_usec, FLAGS_always_flush_pipeline,
                            FLAGS_pipeline_squash_limit, FLAGS_pipeline_wait_batch_usec);
}

void Connection::GetRequestSizeHistogramThreadLocal(std::string* hist) {
  if (io_req_size_hist)
    *hist = io_req_size_hist->ToString();
}

void Connection::TrackRequestSize(bool enable) {
  if (enable && !io_req_size_hist) {
    io_req_size_hist = new base::Histogram;
  } else if (!enable && io_req_size_hist) {
    delete io_req_size_hist;
    io_req_size_hist = nullptr;
  }
}

void Connection::EnsureMemoryBudget(unsigned tid) {
  thread_queue_backpressure[tid].EnsureBelowLimit();
}

ConnectionRef::ConnectionRef(const std::shared_ptr<Connection>& ptr, unsigned thread_id,
                             uint32_t client_id)
    : ptr_{ptr}, last_known_thread_id_{thread_id}, client_id_{client_id} {
}

Connection* ConnectionRef::Get() const {
  auto sptr = ptr_.lock();

  //  The connection can only be deleted on this thread, so
  //  this pointer is valid until the next suspension.
  //  Note: keeping a shared_ptr doesn't prolong the lifetime because
  //  it doesn't manage the underlying connection. See definition of `self_`.
  return sptr.get();
}

bool Connection::WeakRef::IsExpired() const {
  return ptr_.expired();
}

uint32_t Connection::WeakRef::GetClientId() const {
  return client_id_;
}

bool ConnectionRef::operator<(const ConnectionRef& other) const {
  return client_id_ < other.client_id_;
}

bool ConnectionRef::operator==(const ConnectionRef& other) const {
  return client_id_ == other.client_id_;
}

void Connection::OnRecvNotification(const util::FiberSocketBase::RecvNotification& n) {
  DVLOG(2) << CONN_ID << "OnRecvNotification: io_buf_ input_len=" << io_buf_.InputLen()
           << " pending_input=" << pending_input_;
  ProcessRecvNotification(n);

  if (!IsOverPipelineLimit()) {
    // Drain the socket while the fiber is suspended (no-op when io_buf_ is full / no append room).
    const size_t before = io_buf_.InputLen();
    ReadPendingInput();
    if (io_buf_.InputLen() > before)
      ++GetLocalConnStats().proactor_reads;

    // Parse In Proactor: parse newly-read bytes while the fiber is parked in a squash hop, so the
    // next batch is already larger on resume.
    // - This is safe because the parser is idle at kSquashHop.
    // - Calling ParseRedis() with max_busy_cycles==0: proactor's callbacks must not suspend.
    if (pipeline_parse_in_proactor_cached && (fiber_park_spot_ == FiberParkSpot::kSquashHop) &&
        redis_parser_ && (io_buf_.InputLen() > 0)) {
      size_t cmds_before = parsed_cmd_q_len_;
      ParserStatus st = ParseRedis(io_buf_, 0, /*enqueue_only=*/true);
      if (parsed_cmd_q_len_ > cmds_before)
        ++GetLocalConnStats().proactor_parse;
      // The recv callback cannot return a status. If parsing hit a protocol error, flag it so
      // IoLoopV2 surfaces ParserStatus::ERROR and sends the protocol-error reply.
      if (st == ERROR)
        proactor_parse_error_ = true;
      DVLOG(1) << CONN_ID << "Parse-in-proactor added " << (parsed_cmd_q_len_ - cmds_before)
               << " commands, pq_len=" << parsed_cmd_q_len_;
    }
  }

  io_event_.notify();
}

void Connection::ProcessRecvNotification(const util::FiberSocketBase::RecvNotification& n) {
  if (std::holds_alternative<std::error_code>(n.read_result)) {
    io_ec_ = std::get<std::error_code>(n.read_result);
    return;
  }

  using RecvNoti = util::FiberSocketBase::RecvNotification::RecvCompletion;
  if (std::holds_alternative<RecvNoti>(n.read_result)) {
    if (!std::get<RecvNoti>(n.read_result)) {  // false - connection aborted
      io_ec_ = make_error_code(errc::connection_aborted);
      return;
    }

    pending_input_ = true;
  } else if (std::holds_alternative<io::MutableBytes>(n.read_result)) {  // provided buffer.
    io::MutableBytes buf = std::get<io::MutableBytes>(n.read_result);
    {
      ReadBufTracker tracker(io_buf_, id_);
      io_buf_.WriteAndCommit(buf.data(), buf.size());
    }
    last_interaction_ = time(nullptr);

    DCHECK(tl_facade_stats);
    auto& conn_stats = tl_facade_stats->conn_stats;
    conn_stats.io_read_bytes += buf.size();
    local_stats_.net_bytes_in += buf.size();
    ++conn_stats.io_read_cnt;
    ++local_stats_.read_cnt;
    ++conn_stats.num_recv_provided_calls;
  } else {
    LOG(FATAL) << CONN_ID << "Should not reach here";
  }
}

void Connection::ReadPendingInput() {
  if (!pending_input_)
    return;

  // Drain available socket data into io_buf_.
  io::MutableBytes buf = io_buf_.AppendBuffer();
  // A recv call can return fewer bytes than requested even if the
  // socket buffer actually contains enough data to satisfy the full request.
  while (!buf.empty()) {
    io::Result<size_t> res = socket_->TryRecv(buf);
    if (!res) {
      auto ec = res.error();
      // TryRecv is non-blocking: it returns EAGAIN/EWOULDBLOCK when nothing is ready.
      if (ec == errc::resource_unavailable_try_again || ec == errc::operation_would_block)
        pending_input_ = false;
      else
        io_ec_ = ec;
      break;
    }

    if (*res == 0) {
      io_ec_ = make_error_code(errc::connection_aborted);  // *res == 0, clean EOF
      pending_input_ = false;
      break;
    }

    DVLOG(1) << CONN_ID << "Read " << *res << " bytes from socket";

    auto& conn_stats = tl_facade_stats->conn_stats;
    size_t commit_sz = *res;
    conn_stats.io_read_bytes += commit_sz;
    local_stats_.net_bytes_in += commit_sz;

    ++conn_stats.io_read_cnt;
    ++local_stats_.read_cnt;

    last_interaction_ = time(nullptr);
    io_buf_.CommitWrite(commit_sz);
    buf = io_buf_.AppendBuffer();
  }
}

void Connection::CheckIoBufCapacity(bool reached_capacity, base::IoBuf* io_buf) {
  size_t max_io_buf_len = GetFlag(FLAGS_max_client_iobuf_len);

  size_t capacity = io_buf->Capacity();
  if (capacity < max_io_buf_len) {
    size_t parser_hint = 0;
    if (redis_parser_)
      parser_hint = redis_parser_->parselen_hint();  // Could be done for MC as well.

    // If we got a partial request and we managed to parse its
    // length, make sure we have space to store it instead of
    // increasing space incrementally.
    // (Note: The buffer object is only working in power-of-2 sizes,
    // so there's no danger of accidental O(n^2) behavior.)
    if (parser_hint > capacity) {
      ReadBufTracker tracker(*io_buf, id_);
      io_buf->Reserve(std::min(max_io_buf_len, parser_hint));
    }

    // If we got a partial request because iobuf was full, grow it up to
    // a reasonable limit to save on Recv() calls.
    if (reached_capacity && capacity < max_io_buf_len / 2) {
      // Last io used most of the io_buf to the end.
      ReadBufTracker tracker(*io_buf, id_);
      io_buf->Reserve(capacity * 2);  // Valid growth range.
    }

    if (io_buf->AppendLen() == 0U) {
      // it can happen with memcached but not for RedisParser, because RedisParser fully
      // consumes the passed buffer
      LOG_EVERY_T(WARNING, 10) << CONN_ID << "Maximum io_buf length reached " << io_buf->Capacity()
                               << ", consider to increase max_client_iobuf_len flag";
    }
  }
}

void Connection::MaybeEnableRecvMultishot() {
#ifdef __linux__
  if (fb2::ProactorBase::me()->GetKind() == fb2::ProactorBase::Kind::IOURING) {
    auto* up = static_cast<fb2::UringProactor*>(fb2::ProactorBase::me());
    // Only enable if the buffer ring is configured and we aren't using TLS
    if (up->BufRingEntrySize(kRecvSockGid) > 0 && !is_tls_) {
      static_cast<fb2::UringSocket*>(socket_.get())->EnableRecvMultishot();
      pending_input_ = false;
    }
  }
#endif
}

bool Connection::IsOverPipelineLimit() const {
  // Connections with parsed_cmd_q_len_ == 0 must always be allowed to parse so that
  // administrative commands (CONFIG SET, etc.) can execute and relieve backpressure.
  return parsed_cmd_q_len_ > 0 && GetQueueBackpressure().IsPipelineBufferOverLimit(
                                      GetLocalConnStats().pipeline_queue_bytes, parsed_cmd_q_len_);
}

void Connection::NotifyIfMemReleased(size_t bytes_before) {
  // Executing and replying to commands frees up memory. Because those internal functions only
  // wake up this specific connection, we manually notify other connections on this thread that
  // there is now room to resume.
  if (GetLocalConnStats().pipeline_queue_bytes < bytes_before)
    GetQueueBackpressure().NotifyPipelineWaiters();
}

bool Connection::IsReadyToMigrate() const {
  return migration_request_ && (cc_->subscriptions == 0);
}

bool Connection::HasControlEvent() const {
  // Control events warrant leaving any park, independent of new socket input or pipeline memory:
  // a reply became ready, control-plane messages are queued (dispatch_q_), the socket errored or
  // closed (io_ec_), or a thread migration is pending and actionable.
  return (parsed_head_ && parsed_head_->CanReply()) || !dispatch_q_.empty() || io_ec_ ||
         IsReadyToMigrate();
}

bool Connection::ShouldWakeIdle() const {
  // TODO: optimize CanReply with looking up waiter key
  // io_buf_.InputLen() > 0 is still needed for multishot flow.

  // On top of the control events, the idle park also wakes for incoming data and for a head command
  // that is now ready to run.
  return io_buf_.InputLen() > 0 || pending_input_ || HasCommandToExecute() || HasControlEvent();
}

bool Connection::DrainControlPath(uint32_t quota) {
  DCHECK(!dispatch_q_.empty());

  // TODO: ProcessControlMessages should be fused into here.

  // Bounded quota prevents the control path from starving the data path: under a PubSub flood
  // dispatch_q_ can accumulate thousands of messages, and without a quota the fiber would drain
  // them all before parsing any socket data. Mirrors V1's async_dispatch_quota mechanism.
  bool quota_reached = ProcessControlMessages(quota);
  GetQueueBackpressure().pubsub_ec.notifyAll();

  // Restart the loop unless the quota was hit. Restarting forces ReadPendingInput() to pull newly
  // arrived TCP data (so the data path doesn't miss a parse cycle) and acts as a fast path to flush
  // the accumulated PubSub replies via the idle-await block. Falling through only when the quota is
  // reached guarantees a continuous flood cannot indefinitely starve pipelined commands.
  return !quota_reached;
}

Connection::ParserStatus Connection::RunParsePath() {
  // We have input data AND memory budget - parse new commands, execute, reply.
  size_t mem_before = GetLocalConnStats().pipeline_queue_bytes;
  ParserStatus parse_status = ParseLoop();
  NotifyIfMemReleased(mem_before);
  return parse_status;
}

void Connection::DrainQueuedCommands() {
  // No new input to parse (or parsing is held off by backpressure). Drain already-queued commands
  // - execute ready ones and send completed replies - to free pipeline memory without growing the
  // queue.
  size_t mem_before = GetLocalConnStats().pipeline_queue_bytes;

  if (parsed_head_) {
    if (HasCommandToExecute())
      ExecuteBatch();
    ReplyBatch();
  }

  NotifyIfMemReleased(mem_before);
}

void Connection::ParkOnBackpressure(util::fb2::detail::Waiter* backpressure_waiter) {
  // Draining (by the caller) may have freed enough memory; only park if still over the limit, to
  // prevent a busy-spin.
  if (!IsOverPipelineLimit())
    return;

  auto& conn_stats = GetLocalConnStats();
  conn_stats.pipeline_throttle_count++;
  LOG_EVERY_T(WARNING, 10) << CONN_ID << "Pipeline buffer over limit (V2)."
                           << ", Thread pipeline_queue_bytes: " << conn_stats.pipeline_queue_bytes
                           << ", Thread pipeline_queue_entries: "
                           << conn_stats.pipeline_queue_entries
                           << ", Connection parsed_cmd_q_bytes_: " << parsed_cmd_q_bytes_
                           << ", Connection parsed commands queue size: " << parsed_cmd_q_len_
                           << ", consider increasing pipeline_buffer_limit/pipeline_queue_limit";

  // Subscribe persistently to the global backpressure EventCount so that when another connection
  // frees memory (or CONFIG SET raises limits), our waiter callback fires io_event_.notify(),
  // waking this fiber. Must be persistent because io_event_.await()'s internal loop may re-sleep
  // if the predicate is still false after the first notification. A one-shot subscription would be
  // consumed on the first wake, leaving us "deaf" to future memory relief.
  auto sub_key =
      GetQueueBackpressure().v2_pipeline_backpressure_ec.subscribe_persistent(backpressure_waiter);

  // Exit on error, the caller will propagate the reply_builder error further.
  if (auto ec = FlushReplies(); ec)
    return;

  io_event_.await([this]() {
    // Leave the backpressure wait once our own pipeline pressure clears, or on any control event
    // (the latter lets a terminating/migrating connection escape the park).
    bool under_limit = !GetQueueBackpressure().IsPipelineBufferOverLimit(
        GetLocalConnStats().pipeline_queue_bytes, parsed_cmd_q_len_);
    return under_limit || HasControlEvent();
  });
}

variant<error_code, Connection::ParserStatus> Connection::IoLoopV2() {
  auto* peer = socket_.get();
  recv_buf_.res_len = 0;

  // Don't proceed with RegisterOnRecv() if socket is closed (possible cancellation)
  if (!peer->IsOpen())
    return ParserStatus::OK;

  // Everything above the IoLoopV2 is fiber blocking. A connection can migrate before
  // it reaches here and will cause a double RegisterOnRecv check fail. To avoid this,
  // a migration shall only call RegisterOnRecv if it reached the main IoLoopV2 below.
  migration_allowed_to_register_ = true;
  pending_input_ = true;

  MaybeEnableRecvMultishot();

  peer->RegisterOnRecv(
      [this](const FiberSocketBase::RecvNotification& n) { OnRecvNotification(n); });

  ParserStatus parse_status = OK;

  // Callback that wakes the currrent V2 fiber by bumping the io_event_ epoch.
  // Multiple waiters (e.g command completion, backpressure relief) can use the same callback since
  // they all wake the same fiber.
  auto ioevent_cb = [this]() { io_event_.notify(); };

  // Waiter used to establish a mandatory subscription to the head command's blocker,
  // ensuring the fiber wakes immediately upon async command completion.
  util::fb2::detail::Waiter cmd_completion_waiter{ioevent_cb};
  absl::Cleanup waiter_cleanup = [this] { current_wait_.reset(); };

  // Waiter used for transient, conditional subscriptions (via check_or_subscribe)
  // to global pipeline-backpressure relief notifications.
  util::fb2::detail::Waiter backpressure_waiter{ioevent_cb};

  const uint32_t async_dispatch_quota = GetFlag(FLAGS_async_dispatch_quota);

  do {
    HandleMigrateRequest();

    // Register completion for current head if its pending and we don't wait on current_wait_.
    if (HasInFlightCommands() && !current_wait_.has_value()) {
      current_wait_.emplace(parsed_head_, &cmd_completion_waiter);
    }

    ReadPendingInput();

    // Idle park: flush and sleep only when the fiber is truly idle (ShouldWakeIdle() is false).
    // When synchronous commands (e.g. PUBLISH) are pipelined, ExecuteBatch processes them
    // and loops back here with HasCommandToExecute() == true; skipping the flush then lets the
    // whole pipeline execute in-memory before a single sendmsg at the end.
    if (!ShouldWakeIdle()) {
      phase_ = READ_SOCKET;

      // Flush replies deferred by ReplyBatch before sleeping - ensures the client gets its response
      // even when no more data arrives (single commands, end of pipeline).
      if (auto ec = FlushReplies(); ec) {
        return ec;
      }

      fiber_park_spot_ = FiberParkSpot::kIdleAwait;
      io_event_.await([this] { return ShouldWakeIdle(); });
      fiber_park_spot_ = FiberParkSpot::kNone;
    }

    phase_ = PROCESS;
    bool reached_capacity = io_buf_.AppendLen() == 0;

    // Control path: drain dispatch_q_. Restart the loop (so fresh socket data is read first)
    // unless we hit the quota, in which case fall through to the data path to avoid starving it.
    if (!dispatch_q_.empty() && DrainControlPath(async_dispatch_quota)) {
      continue;
    }

    bool over_limit = IsOverPipelineLimit();
    if (io_buf_.InputLen() == 0 || over_limit) {
      // Drain-only path: either there is no new input to parse, or we are over the pipeline memory
      // limit (parsing would only grow the queue further). Either way, drain queued commands to
      // free memory instead of parsing. When over the limit, also park until another connection
      // relieves the pressure. (Empty queues are never over the limit, so admin commands can still
      // parse.)
      DrainQueuedCommands();
      if (over_limit)
        ParkOnBackpressure(&backpressure_waiter);
      parse_status = NEED_MORE;
    } else {
      // Input available and under budget: parse, execute, reply.
      parse_status = RunParsePath();
    }

    // Release any checkpoints deferred while async commands were in flight, now that the in-flight
    // run has drained.
    if (!HasInFlightCommands()) {
      ReleaseDeferredCheckpoints();
    }

    // A protocol error detected by parse-in-proactor (which runs in the recv callback and cannot
    // return a status) is surfaced here by setting parse_status = ERROR.
    if (proactor_parse_error_) {
      proactor_parse_error_ = false;
      parse_status = ERROR;
    }

    if (reply_builder_->GetError()) {
      return reply_builder_->GetError();
    }

    // Check io_ec_ after parsing and flushing replies, so that half-closed
    // connections get their responses before we close.
    if (io_ec_) {
      if (auto ec = FlushReplies(); ec) {
        return ec;
      }
      LOG_IF(WARNING, cntx()->replica_conn) << CONN_ID << "async io error: " << io_ec_;
      return std::exchange(io_ec_, {});
    }

    if (parse_status == ERROR) {
      break;
    }

    // Migration requested and actionable: skip buffer bookkeeping, jump to HandleMigrateRequest().
    if (IsReadyToMigrate()) {
      // Flush before migrating: handing off unflushed thread-local buffers to a
      // new thread will cause data corruption or a hard crash.
      if (auto ec = FlushReplies(); ec) {
        return ec;  // Connection is dead, no point migrating it cross-thread.
      }
      continue;
    }

    if (parse_status == NEED_MORE) {
      parse_status = OK;
      CheckIoBufCapacity(reached_capacity, &io_buf_);
    }
  } while (peer->IsOpen());

  return parse_status;
}

Connection::WaitEvent::WaitEvent(ParsedCommand* cmd, util::fb2::detail::Waiter* w)
    : key(cmd->Blocker()->OnCompletion(w)) {
}

void ResetStats() {
  auto& cstats = tl_facade_stats->conn_stats;
  cstats.pipelined_cmd_cnt = 0;
  cstats.conn_received_cnt = 0;
  cstats.command_cnt_main = 0;
  cstats.command_cnt_other = 0;
  cstats.io_read_cnt = 0;
  cstats.io_read_bytes = 0;
  cstats.proactor_reads = 0;
  cstats.proactor_parse = 0;

  tl_facade_stats->reply_stats = {};
  if (io_req_size_hist)
    io_req_size_hist->Clear();
}

}  // namespace facade
