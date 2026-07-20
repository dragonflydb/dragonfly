// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstdint>

#include "base/histogram.h"
namespace facade {

// Counters for the Pub/Sub back-pressure / slow-subscriber protection policy. Exported as the
// prometheus counter `pubsub_backpressure_events_total` with a bounded `event` label.
struct PubsubBackpressureStats {
  // Transitions where a thread's queued Pub/Sub bytes crossed from at/below the soft limit to
  // above.
  uint64_t soft_limit_crossing = 0;
  // Publisher wait episodes caused by reaching the hard limit.
  uint64_t hard_limit_throttled = 0;

  // Subscriber connections closed by the policy after the soft-limit and stuck-send conditions
  // were met.
  uint64_t forced_disconnect = 0;

  // Queued Pub/Sub messages discarded during policy-driven disconnects.
  uint64_t messages_discarded = 0;

  PubsubBackpressureStats& operator+=(const PubsubBackpressureStats& o) {
    soft_limit_crossing += o.soft_limit_crossing;
    hard_limit_throttled += o.hard_limit_throttled;
    forced_disconnect += o.forced_disconnect;
    messages_discarded += o.messages_discarded;
    return *this;
  }
};

struct ConnectionStats {
  size_t read_buf_capacity = 0;  // total capacity of input buffers
  size_t connection_memory_bytes = 0;
  // Count of pending messages in dispatch queue
  uint64_t dispatch_queue_entries = 0;
  // Memory used by pending messages in dispatch queue
  size_t dispatch_queue_bytes = 0;
  // Count of pending parsed commands in the pipeline queue (Data Path)
  uint64_t pipeline_queue_entries = 0;
  // Memory used by pending parsed commands in the pipeline queue (Data Path)
  size_t pipeline_queue_bytes = 0;
  // total size of all publish messages (subset of dispatch_queue_bytes)
  size_t dispatch_queue_subscriber_bytes = 0;

  size_t pipeline_cmd_cache_bytes = 0;

  uint64_t io_read_cnt = 0;
  size_t io_read_bytes = 0;

  uint64_t command_cnt_main = 0;
  uint64_t command_cnt_other = 0;
  uint64_t pipelined_cmd_cnt = 0;
  uint64_t pipelined_cmd_latency = 0;      // in microseconds
  base::Histogram pipelined_latency_hist;  // distribution of per-command latencies (usec)

  // in microseconds, time spent waiting for the pipelined commands to start executing
  uint64_t pipelined_wait_latency = 0;
  uint64_t conn_received_cnt = 0;

  uint32_t num_conns_main = 0;
  uint32_t num_conns_other = 0;
  uint32_t num_blocked_clients = 0;

  // number of times the connection yielded due to max_busy_read_usec limit
  uint32_t num_read_yields = 0;
  uint64_t num_migrations = 0;
  uint64_t num_recv_provided_calls = 0;

  // Number of times the tls connection was closed by the time we started reading from it.
  uint64_t tls_accept_disconnects = 0;  // number of TLS socket disconnects during the handshake
                                        //
  uint64_t handshakes_started = 0;
  uint64_t handshakes_completed = 0;

  // Number of events when the pipeline queue was over the limit and was throttled.
  uint64_t pipeline_throttle_count = 0;
  uint64_t pipeline_dispatch_calls = 0;
  uint64_t pipeline_dispatch_commands = 0;
  uint64_t pipeline_dispatch_flush_usec = 0;

  // number of times we flushed when dispatching the pipeline.
  uint64_t pipeline_dispatch_flush_count = 0;

  // V2 Only: Number of times the proactor OnRecv callback actually drained bytes into io_buf_.
  uint64_t proactor_reads = 0;

  // V2 Only: Number of times parse-in-proactor enqueued at least one command from the OnRecv
  // callback.
  uint64_t proactor_parse = 0;

  // Pub/Sub back-pressure / slow-subscriber protection counters.
  PubsubBackpressureStats pubsub_backpressure;

  ConnectionStats& operator+=(const ConnectionStats& o);
};

struct ReplyStats {
  struct SendStats {
    int64_t count = 0;
    // In CycleClock cycles. Convert via base::CycleClock::ToUsec at the reporting boundary.
    int64_t total_duration = 0;

    SendStats& operator+=(const SendStats& other) {
      static_assert(sizeof(SendStats) == 16u);

      count += other.count;
      total_duration += other.total_duration;
      return *this;
    }
  };

  // Send() operations that are written to sockets
  SendStats send_stats;

  size_t io_write_cnt = 0;
  size_t io_write_bytes = 0;
  uint64_t borrowed_string_sent_cnt = 0;

  absl::flat_hash_map<std::string, uint64_t> err_count;
  size_t script_error_count = 0;

  // This variable can be updated directly from shard threads when they allocate memory for replies.
  std::atomic<size_t> squashing_current_reply_size{0};

  ReplyStats() = default;
  ReplyStats(ReplyStats&& other) noexcept;
  ReplyStats& operator+=(const ReplyStats& other);
  ReplyStats& operator=(const ReplyStats& other);
};

struct FacadeStats {
  ConnectionStats conn_stats;
  ReplyStats reply_stats;

  FacadeStats& operator+=(const FacadeStats& other) {
    conn_stats += other.conn_stats;
    reply_stats += other.reply_stats;
    return *this;
  }
};

inline thread_local FacadeStats* tl_facade_stats = nullptr;

}  // namespace facade
