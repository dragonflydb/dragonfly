// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstdint>

#include "base/histogram.h"
namespace facade {

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
  // Number of times the V2 proactor OnRecv callback drained socket bytes
  // into io_buf_
  uint64_t proactor_reads = 0;
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

  // (squash_cmd_cnt / squash_call_cnt) is the average squash batch size.
  //
  // squash dispatches that packed >= 1 command.
  uint64_t squash_call_cnt = 0;
  // total commands packed across those calls
  uint64_t squash_cmd_cnt = 0;
  // distribution of commands per squash call
  base::Histogram squash_batch_size_hist;

  // (parse_iteration_cmd_cnt / parse_iteration_cnt) is the average number of commands a single
  // fiber pass parses before it must execute (bounded by max_client_iobuf_len at large value
  // sizes).
  //
  // number of fiber parse passes over the read buffer.
  uint64_t parse_iteration_cnt = 0;
  // commands enqueued across those passes.
  uint64_t parse_iteration_cmd_cnt = 0;

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
