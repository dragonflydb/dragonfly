// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/metrics.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>

#include "base/flags.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/reply_builder.h"
#include "io/proc_reader.h"
#include "server/cluster_support.h"
#include "server/command_registry.h"
#include "server/dflycmd.h"
#include "server/journal/journal.h"
#include "server/namespaces.h"
#include "server/search/doc_index.h"
#include "server/server_family.h"
#include "server/snapshot.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"

ABSL_DECLARE_FLAG(uint32_t, maxclients);

namespace dfly {

using namespace facade;
using namespace util;
using absl::GetFlag;
using absl::StrCat;
using http::StringResponse;

namespace {

enum class MetricType : uint8_t { COUNTER, GAUGE, SUMMARY, HISTOGRAM };

const char* MetricTypeName(MetricType type) {
  switch (type) {
    case MetricType::COUNTER:
      return "counter";
    case MetricType::GAUGE:
      return "gauge";
    case MetricType::SUMMARY:
      return "summary";
    case MetricType::HISTOGRAM:
      return "histogram";
  }
  return "unknown";
}

inline string GetMetricFullName(string_view metric_name) {
  return StrCat("dragonfly_", metric_name);
}

void AppendMetricHeader(string_view metric_name, string_view metric_help, MetricType type,
                        string* dest) {
  const auto full_metric_name = GetMetricFullName(metric_name);
  absl::StrAppend(dest, "# HELP ", full_metric_name, " ", metric_help, "\n");
  absl::StrAppend(dest, "# TYPE ", full_metric_name, " ", MetricTypeName(type), "\n");
}

void AppendLabelTupple(absl::Span<const string_view> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  if (label_names.empty())
    return;

  absl::StrAppend(dest, "{");
  for (size_t i = 0; i < label_names.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(dest, ", ");
    }
    absl::StrAppend(dest, label_names[i], "=\"", label_values[i], "\"");
  }

  absl::StrAppend(dest, "}");
}

void AppendMetricValue(string_view metric_name, const absl::AlphaNum& value,
                       absl::Span<const string_view> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  absl::StrAppend(dest, GetMetricFullName(metric_name));
  AppendLabelTupple(label_names, label_values, dest);
  absl::StrAppend(dest, " ", value, "\n");
}

void AppendMetricWithoutLabels(string_view name, string_view help, const absl::AlphaNum& value,
                               MetricType type, string* dest) {
  AppendMetricHeader(name, help, type, dest);
  AppendMetricValue(name, value, {}, {}, dest);
}

void AppendPipelineLatencySummary(string_view name, string_view help, const base::Histogram& hist,
                                  uint64_t total_count, double total_sum_usec, string* dest) {
  AppendMetricHeader(name, help, MetricType::SUMMARY, dest);
  const string full_name = GetMetricFullName(name);
  if (hist.count() > 0) {
    auto [p95, p99] = hist.Percentiles(95, 99);
    AppendMetricValue(name, p95 * 1e-6, {"quantile"}, {"0.95"}, dest);
    AppendMetricValue(name, p99 * 1e-6, {"quantile"}, {"0.99"}, dest);
  }
  // Use monotonically increasing counters for _sum/_count so that Prometheus
  // rate()/irate() functions work correctly even though the histogram is decayed.
  absl::StrAppend(dest, full_name, "_sum ", total_sum_usec * 1e-6, "\n");
  absl::StrAppend(dest, full_name, "_count ", total_count, "\n");
}

}  // namespace

void Metrics::Print(uint64_t uptime, const CommandRegistry* registry, DflyCmd* dfly_cmd,
                    util::http::StringResponse* resp, bool legacy) {
  bool is_master = ServerState::tlocal()->is_master;
  const Metrics& m = *this;

  // Server metrics
  AppendMetricHeader("version", "", MetricType::GAUGE, &resp->body());
  AppendMetricValue("version", 1, {"version"}, {GetVersion()}, &resp->body());

  AppendMetricWithoutLabels("master", "1 if master 0 if replica", is_master ? 1 : 0,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("uptime_in_seconds", "", uptime, MetricType::COUNTER, &resp->body());

  // Clients metrics
  const auto& conn_stats = m.facade_stats.conn_stats;
  AppendMetricWithoutLabels("max_clients", "Maximal number of clients", GetFlag(FLAGS_maxclients),
                            MetricType::GAUGE, &resp->body());
  AppendMetricHeader("connected_clients", "", MetricType::GAUGE, &resp->body());
  AppendMetricValue("connected_clients", conn_stats.num_conns_main, {"listener"}, {"main"},
                    &resp->body());
  AppendMetricValue("connected_clients", conn_stats.num_conns_other, {"listener"}, {"other"},
                    &resp->body());
  AppendMetricHeader("tls_handshakes_total", "Total TLS handshakes by status", MetricType::COUNTER,
                     &resp->body());
  AppendMetricValue("tls_handshakes_total", conn_stats.handshakes_started, {"status"}, {"started"},
                    &resp->body());
  AppendMetricValue("tls_handshakes_total", conn_stats.handshakes_completed, {"status"},
                    {"completed"}, &resp->body());

  AppendMetricWithoutLabels("blocked_clients", "", conn_stats.num_blocked_clients,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("pipeline_queue_length", "", conn_stats.pipeline_queue_entries,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("send_delay_seconds", "",
                            double(GetDelayMs(m.oldest_pending_send_ts)) / 1000.0,
                            MetricType::GAUGE, &resp->body());

  AppendMetricWithoutLabels("pipeline_throttle_total", "", conn_stats.pipeline_throttle_count,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("pipeline_commands_total", "", conn_stats.pipelined_cmd_cnt,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("pipeline_dispatch_calls_total", "", conn_stats.pipeline_dispatch_calls,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("pipeline_dispatch_commands_total", "",
                            conn_stats.pipeline_dispatch_commands, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("pipeline_dispatch_flush_total", "",
                            conn_stats.pipeline_dispatch_flush_count, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("pipeline_dispatch_flush_duration_seconds", "",
                            conn_stats.pipeline_dispatch_flush_usec * 1e-6, MetricType::COUNTER,
                            &resp->body());

  AppendMetricWithoutLabels("pipeline_commands_duration_seconds", "",
                            conn_stats.pipelined_cmd_latency * 1e-6, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("pipeline_queue_wait_duration_seconds", "",
                            conn_stats.pipelined_wait_latency * 1e-6, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("pipeline_blocking_commands_total", "",
                            m.coordinator_stats.blocking_commands_in_pipelines, MetricType::COUNTER,
                            &resp->body());

  // pipelined_cmd_cnt/pipelined_cmd_latency are monotonically increasing counters used for
  // Prometheus _count/_sum; the histogram is decayed and therefore not monotonic.
  AppendPipelineLatencySummary("pipeline_latency_seconds", "Pipeline command latency distribution",
                               conn_stats.pipelined_latency_hist, conn_stats.pipelined_cmd_cnt,
                               conn_stats.pipelined_cmd_latency, &resp->body());

  AppendMetricWithoutLabels("cmd_squash_hop_total", "", m.coordinator_stats.multi_squash_hops,
                            MetricType::COUNTER, &resp->body());

  AppendMetricWithoutLabels("cmd_squash_commands_total", "", m.coordinator_stats.squashed_commands,
                            MetricType::COUNTER, &resp->body());

  AppendMetricWithoutLabels("cmd_squash_hop_duration_seconds", "",
                            m.coordinator_stats.multi_squash_exec_hop_usec * 1e-6,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("cmd_squash_hop_reply_seconds", "",
                            m.coordinator_stats.multi_squash_exec_reply_usec * 1e-6,
                            MetricType::COUNTER, &resp->body());

  string connections_libs;
  AppendMetricHeader("connections_libs", "Total number of connections by libname:ver",
                     MetricType::GAUGE, &connections_libs);
  for (const auto& [lib, count] : m.connections_lib_name_ver_map) {
    AppendMetricValue("connections_libs", count, {"lib"}, {lib}, &connections_libs);
  }
  absl::StrAppend(&resp->body(), connections_libs);

  // Memory metrics
  io::StatusData sdata;
  bool success = ReadProcStats(&sdata);
  AppendMetricWithoutLabels("memory_used_bytes", "", m.heap_used_bytes, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("memory_used_peak_bytes", "", m.used_mem_peak, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels(
      "connection_memory_bytes",
      "Approximate direct memory used by active non-replication-flow connections, excluding "
      "separately tracked read buffers and queues.",
      conn_stats.connection_memory_bytes, MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("fibers_count", "", m.worker_fiber_count, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("blocked_tasks", "", m.blocked_tasks, MetricType::GAUGE, &resp->body());

  AppendMetricWithoutLabels("memory_max_bytes", "", max_memory_limit.load(memory_order_relaxed),
                            MetricType::GAUGE, &resp->body());

  if (m.events.insertion_rejections | m.coordinator_stats.oom_error_cmd_cnt) {
    AppendMetricHeader("oom_errors_total", "Rejected requests due to out of memory errors",
                       MetricType::COUNTER, &resp->body());
    AppendMetricValue("oom_errors_total", m.events.insertion_rejections, {"type"}, {"insert"},
                      &resp->body());
    AppendMetricValue("oom_errors_total", m.coordinator_stats.oom_error_cmd_cnt, {"type"}, {"cmd"},
                      &resp->body());
  }
  if (success) {
    size_t rss = sdata.hugetlb_pages + sdata.vm_rss;
    AppendMetricWithoutLabels("used_memory_rss_bytes", "", rss, MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("swap_memory_bytes", "", sdata.vm_swap, MetricType::GAUGE,
                              &resp->body());
  }

  DbStats total;
  for (const auto& db_stats : m.db_stats) {
    total += db_stats;
  }

  {
    string type_used_memory_metric;
    bool added = false;
    AppendMetricHeader("type_used_memory", "Memory used per type", MetricType::GAUGE,
                       &type_used_memory_metric);

    for (unsigned type = 0; type < total.memory_usage_by_type.size(); type++) {
      size_t mem = total.memory_usage_by_type[type];
      if (mem > 0) {
        AppendMetricValue("type_used_memory", mem, {"type"}, {ObjTypeToString(type)},
                          &type_used_memory_metric);
        added = true;
      }
    }
    if (added)
      absl::StrAppend(&resp->body(), type_used_memory_metric);
  }

  // Stats metrics
  AppendMetricWithoutLabels("connections_received_total", "", conn_stats.conn_received_cnt,
                            MetricType::COUNTER, &resp->body());

  AppendMetricHeader("commands_processed_total", "", MetricType::COUNTER, &resp->body());
  AppendMetricValue("commands_processed_total", conn_stats.command_cnt_main, {"listener"}, {"main"},
                    &resp->body());
  AppendMetricValue("commands_processed_total", conn_stats.command_cnt_other, {"listener"},
                    {"other"}, &resp->body());
  AppendMetricWithoutLabels("keyspace_hits_total", "", m.events.hits, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("keyspace_misses_total", "", m.events.misses, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("keyspace_mutations_total", "", m.events.mutations, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("lua_interpreter_cnt", "", m.lua_stats.interpreter_cnt,
                            MetricType::GAUGE, &resp->body());

  AppendMetricWithoutLabels("freed_memory_lua", "", m.lua_stats.gc_freed_memory,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("lua_blocked_total", "", m.lua_stats.blocked_cnt, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("lua_gc_interpreter_return", "", m.lua_stats.interpreter_return,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("lua_force_gc_calls", "", m.lua_stats.force_gc_calls,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("lua_gc_duration_total_sec", "", m.lua_stats.gc_duration_ns * 1e-9,
                            MetricType::COUNTER, &resp->body());

  AppendMetricWithoutLabels("backups_total", "", m.loading_stats.backup_count, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("failed_backups_total", "", m.loading_stats.failed_backup_count,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("restores_total", "", m.loading_stats.restore_count,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("failed_restores_total", "", m.loading_stats.failed_restore_count,
                            MetricType::COUNTER, &resp->body());

  // Net metrics
  AppendMetricWithoutLabels("net_input_recv_total", "", conn_stats.io_read_cnt, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("net_read_yields_total", "", conn_stats.num_read_yields,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("proactor_reads_total", "V2 OnRecv reads that actually drained bytes",
                            conn_stats.proactor_reads, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("proactor_parse_total",
                            "V2 OnRecv parses that enqueued at least one command",
                            conn_stats.proactor_parse, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("pipeline_idle_parks_total",
                            "V2 connection fiber idle-await parks (went to sleep)",
                            conn_stats.pipeline_idle_parks, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels(
      "pipeline_reply_wait_parks_total",
      "V2 idle-await parks while a dispatched command reply was still pending (coroutine window)",
      conn_stats.pipeline_reply_wait_parks, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("net_input_bytes_total", "", conn_stats.io_read_bytes,
                            MetricType::COUNTER, &resp->body());

  AppendMetricWithoutLabels("net_output_bytes_total", "", m.facade_stats.reply_stats.io_write_bytes,
                            MetricType::COUNTER, &resp->body());
  {
    AppendMetricWithoutLabels(
        "reply_duration_seconds", "",
        base::CycleClock::ToUsec(m.facade_stats.reply_stats.send_stats.total_duration) * 1e-6,
        MetricType::COUNTER, &resp->body());
    AppendMetricWithoutLabels("reply_total", "", m.facade_stats.reply_stats.send_stats.count,
                              MetricType::COUNTER, &resp->body());
  }

  AppendMetricWithoutLabels("script_error_total", "", m.facade_stats.reply_stats.script_error_count,
                            MetricType::COUNTER, &resp->body());

  AppendMetricHeader("listener_accept_error_total", "Listener accept errors", MetricType::COUNTER,
                     &resp->body());
  AppendMetricValue("listener_accept_error_total", m.refused_conn_max_clients_reached_count,
                    {"reason"}, {"limit_reached"}, &resp->body());
  AppendMetricValue("listener_accept_error_total", m.facade_stats.conn_stats.tls_accept_disconnects,
                    {"reason"}, {"tls_error"}, &resp->body());

  // Per-DB expired/evicted totals
  {
    string exp_str, evict_str;
    for (size_t i = 0; i < m.db_stats.size(); ++i) {
      const auto& s = m.db_stats[i];
      if (s.events.expired_keys > 0)
        AppendMetricValue("expired_keys_total", s.events.expired_keys, {"db"}, {StrCat("db", i)},
                          &exp_str);
      if (s.events.evicted_keys > 0)
        AppendMetricValue("evicted_keys_total", s.events.evicted_keys, {"db"}, {StrCat("db", i)},
                          &evict_str);
    }
    AppendMetricHeader("expired_keys_total", "", MetricType::COUNTER, &resp->body());
    absl::StrAppend(&resp->body(), exp_str);
    AppendMetricHeader("evicted_keys_total", "", MetricType::COUNTER, &resp->body());
    absl::StrAppend(&resp->body(), evict_str);
  }

  // Memory stats
  if (legacy) {
    AppendMetricWithoutLabels("memory_fiberstack_vms_bytes",
                              "virtual memory size used by all the fibers",
                              m.worker_fiber_stack_size, MetricType::GAUGE, &resp->body());

    AppendMetricWithoutLabels(
        "commands_squashing_replies_bytes", "",
        m.facade_stats.reply_stats.squashing_current_reply_size.load(memory_order_relaxed),
        MetricType::GAUGE, &resp->body());

    AppendMetricWithoutLabels("tls_bytes", "", m.tls_bytes, MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("snapshot_serialization_bytes", "", m.serialization_bytes,
                              MetricType::GAUGE, &resp->body());

    AppendMetricWithoutLabels("used_memory_lua", "", m.lua_stats.used_bytes, MetricType::GAUGE,
                              &resp->body());

    AppendMetricWithoutLabels("client_read_buffer_bytes", "", conn_stats.read_buf_capacity,
                              MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("dispatch_queue_bytes", "", conn_stats.dispatch_queue_bytes,
                              MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("pipeline_queue_bytes", "", conn_stats.pipeline_queue_bytes,
                              MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("pipeline_cmd_cache_bytes", "", conn_stats.pipeline_cmd_cache_bytes,
                              MetricType::GAUGE, &resp->body());
  }

  string memory_by_class_bytes;
  AppendMetricHeader("memory_by_class_bytes", "Memory metrics", MetricType::GAUGE,
                     &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.lua_stats.used_bytes, {"class"}, {"used_lua"},
                    &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.worker_fiber_stack_size, {"class"},
                    {"fiberstack_vms"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.tls_bytes, {"class"}, {"tls"},
                    &memory_by_class_bytes);

  const size_t squashed =
      m.facade_stats.reply_stats.squashing_current_reply_size.load(memory_order_relaxed);

  AppendMetricValue("memory_by_class_bytes", squashed, {"class"}, {"commands_squashing_replies"},
                    &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", conn_stats.pipeline_cmd_cache_bytes, {"class"},
                    {"pipeline_cmd_cache"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", conn_stats.pipeline_queue_bytes, {"class"},
                    {"pipeline_queue"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", conn_stats.dispatch_queue_bytes, {"class"},
                    {"dispatch_queue"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", conn_stats.read_buf_capacity, {"class"},
                    {"client_read_buffer"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", conn_stats.connection_memory_bytes, {"class"},
                    {"connection"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", total.table_mem_usage, {"class"}, {"table_used"},
                    &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", total.obj_memory_usage, {"class"}, {"object_used"},
                    &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.coordinator_stats.stored_cmd_bytes, {"class"},
                    {"conn_stored_commands"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.search_stats.used_memory, {"class"}, {"search_used"},
                    &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.interned_string_stats.pool_bytes, {"class"},
                    {"interned_string_pool"}, &memory_by_class_bytes);

  AppendMetricValue("memory_by_class_bytes", m.interned_string_stats.pool_table_bytes, {"class"},
                    {"interned_string_table"}, &memory_by_class_bytes);
  AppendMetricValue("memory_by_class_bytes", m.lsn_buffer_bytes, {"class"}, {"repl_backlog_buffer"},
                    &memory_by_class_bytes);

  // Interned string stats
  AppendMetricWithoutLabels("interned_string_entries", "Number of unique interned strings",
                            m.interned_string_stats.pool_entries, MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("interned_string_hits_total", "Interned string pool hits",
                            m.interned_string_stats.hits, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("interned_string_misses_total", "Interned string pool misses",
                            m.interned_string_stats.misses, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("interned_string_entries_dedup_factor",
                            "Deduplication achieved by interned strings",
                            m.interned_string_stats.pool_entries == 0
                                ? 0.0
                                : static_cast<double>(m.interned_string_stats.live_references) /
                                      static_cast<double>(m.interned_string_stats.pool_entries),
                            MetricType::GAUGE, &resp->body());

  // Command stats
  auto cmd_stats = registry->NamedCallStats(m.cmd_call_stats);
  if (!cmd_stats.empty()) {
    string command_metrics;

    AppendMetricHeader("commands_total", "Total number of commands executed", MetricType::COUNTER,
                       &command_metrics);
    for (const auto& [name, stat] : cmd_stats) {
      const auto calls = stat.first;
      AppendMetricValue("commands_total", calls, {"cmd"}, {name}, &command_metrics);
    }

    AppendMetricHeader("commands_duration_seconds", "Duration of commands in seconds",
                       MetricType::COUNTER, &command_metrics);
    for (const auto& [name, stat] : cmd_stats) {
      const double duration_seconds = stat.second * 1e-6;
      AppendMetricValue("commands_duration_seconds", duration_seconds, {"cmd"}, {name},
                        &command_metrics);
    }

    absl::StrAppend(&resp->body(), command_metrics);
  }

  if (m.replica_side_info) {  // replica side
    const auto reconnect_count = m.replica_side_info->summary.reconnect_count;
    AppendMetricWithoutLabels("replica_reconnect_count", "Number of replica reconnects",
                              reconnect_count, MetricType::COUNTER, &resp->body());
  } else {  // Master side
    string replication_lag_metrics;
    vector<ReplicaRoleInfo> replicas_info = dfly_cmd->GetReplicasRoleInfo();
    if (legacy) {
      AppendMetricWithoutLabels(
          "replication_streaming_bytes", "Stable sync replication memory usage",
          m.replication_stats.streamer_buf_capacity_bytes, MetricType::GAUGE, &resp->body());
      AppendMetricWithoutLabels("replication_full_sync_bytes", "Full sync memory usage",
                                m.replication_stats.full_sync_buf_bytes, MetricType::GAUGE,
                                &resp->body());
    }
    AppendMetricValue("memory_by_class_bytes", m.replication_stats.streamer_buf_capacity_bytes,
                      {"class"}, {"replication_streaming"}, &memory_by_class_bytes);
    AppendMetricValue("memory_by_class_bytes", m.replication_stats.full_sync_buf_bytes, {"class"},
                      {"replication_full_sync"}, &memory_by_class_bytes);

    AppendMetricWithoutLabels("replication_psync_count", "Pync count",
                              m.coordinator_stats.psync_requests_total, MetricType::COUNTER,
                              &resp->body());
    AppendMetricHeader("connected_replica_lag_records", "Lag in records of a connected replica.",
                       MetricType::GAUGE, &replication_lag_metrics);
    for (const auto& replica : replicas_info) {
      AppendMetricValue("connected_replica_lag_records", replica.lsn_lag,
                        {"replica_ip", "replica_port", "replica_state"},
                        {replica.address, absl::StrCat(replica.listening_port), replica.state},
                        &replication_lag_metrics);
    }
    absl::StrAppend(&resp->body(), replication_lag_metrics);
  }

  AppendMetricWithoutLabels("fiber_switch_total", "", m.fiber_switch_cnt, MetricType::COUNTER,
                            &resp->body());
  double delay_seconds = m.fiber_switch_delay_usec * 1e-6;
  AppendMetricWithoutLabels("fiber_switch_delay_seconds_total", "", delay_seconds,
                            MetricType::COUNTER, &resp->body());

  // Average fraction of time IO-loop proactor threads are idle (parked in wait_for_cqe with no
  // ready fiber). Near 0 under saturation; higher when the server has spare capacity.
  AppendMetricWithoutLabels("proactor_idle_ratio", "", m.proactor_idle_ratio, MetricType::GAUGE,
                            &resp->body());

  AppendMetricWithoutLabels("fiber_longrun_total", "", m.fiber_longrun_cnt, MetricType::COUNTER,
                            &resp->body());
  double longrun_seconds = m.fiber_longrun_usec * 1e-6;
  AppendMetricWithoutLabels("fiber_longrun_seconds", "", longrun_seconds, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("tx_queue_len", "", m.tx_queue_len, MetricType::GAUGE, &resp->body());

  {
    bool added = false;
    string str;
    AppendMetricHeader("transaction_widths_total", "Transaction counts by their widths",
                       MetricType::COUNTER, &str);

    for (unsigned width = 0; width < shard_set->size(); ++width) {
      uint64_t count = m.coordinator_stats.tx_width_freq_arr[width];

      if (count > 0) {
        AppendMetricValue("transaction_widths_total", count, {"width"}, {StrCat("w", width + 1)},
                          &str);
        added = true;
      }
    }
    if (added)
      absl::StrAppend(&resp->body(), str);
  }

  if (IsClusterEnabled()) {
    string migration_errors_str;
    AppendMetricHeader("migration_errors_total", "Total error numbers of current migrations",
                       MetricType::GAUGE, &migration_errors_str);
    AppendMetricValue("migration_errors_total", m.migration_errors_total, {"num"},
                      {"migration errors"}, &migration_errors_str);
    absl::StrAppend(&resp->body(), migration_errors_str);

    string moved_errors_str;
    uint64_t moved_total_errors = 0;
    if (m.facade_stats.reply_stats.err_count.contains("MOVED")) {
      moved_total_errors = m.facade_stats.reply_stats.err_count.at("MOVED");
    }
    AppendMetricHeader("moved_errors_total", "Total number of moved slot errors",
                       MetricType::COUNTER, &moved_errors_str);
    AppendMetricValue("moved_errors_total", moved_total_errors, {"num"}, {"moved errors"},
                      &moved_errors_str);
    absl::StrAppend(&resp->body(), moved_errors_str);
  }

  string db_key_metrics, db_key_expire_metrics, db_capacity_metrics;

  AppendMetricHeader("db_keys", "Total number of keys by DB", MetricType::GAUGE, &db_key_metrics);
  AppendMetricHeader("db_capacity", "Table capacity by DB", MetricType::GAUGE,
                     &db_capacity_metrics);

  AppendMetricHeader("db_keys_expiring", "Total number of expiring keys by DB", MetricType::GAUGE,
                     &db_key_expire_metrics);

  for (size_t i = 0; i < m.db_stats.size(); ++i) {
    AppendMetricValue("db_keys", m.db_stats[i].key_count, {"db"}, {StrCat("db", i)},
                      &db_key_metrics);
    AppendMetricValue("db_capacity", m.db_stats[i].prime_capacity, {"db"}, {StrCat("db", i)},
                      &db_capacity_metrics);

    AppendMetricValue("db_keys_expiring", m.db_stats[i].expire_count, {"db"}, {StrCat("db", i)},
                      &db_key_expire_metrics);

    AppendMetricValue("keyspace_hits_total", m.db_stats[i].events.hits, {"db"}, {StrCat("db", i)},
                      &resp->body());
    AppendMetricValue("keyspace_misses_total", m.db_stats[i].events.misses, {"db"},
                      {StrCat("db", i)}, &resp->body());
  }

  absl::StrAppend(&resp->body(), db_key_metrics, db_key_expire_metrics, db_capacity_metrics,
                  memory_by_class_bytes);

  AppendMetricWithoutLabels("defrag_invocations", "Defrag invocations",
                            m.shard_stats.defrag_task_invocation_total, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("defrag_attempts", "Objects examined",
                            m.shard_stats.defrag_attempt_total, MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("defrag_objects_moved", "Objects moved",
                            m.shard_stats.defrag_realloc_total, MetricType::COUNTER, &resp->body());

  AppendMetricHeader("defrag_skipped_total", "Defrag tasks skipped", MetricType::COUNTER,
                     &resp->body());
  AppendMetricValue("defrag_skipped_total", m.shard_stats.defrag_skipped_mem_under_threshold,
                    {"reason"}, {"mem_under_threshold"}, &resp->body());
  AppendMetricValue("defrag_skipped_total", m.shard_stats.defrag_skipped_within_check_interval,
                    {"reason"}, {"within_check_interval"}, &resp->body());
  AppendMetricValue("defrag_skipped_total", m.shard_stats.defrag_skipped_not_enough_fragmentation,
                    {"reason"}, {"not_enough_fragmentation"}, &resp->body());

  AppendMetricWithoutLabels("huffman_tables_built", "Huffman tables built",
                            m.shard_stats.huffman_tables_built, MetricType::COUNTER, &resp->body());

  AppendMetricHeader("list_reads", "List Reads Patterns", MetricType::COUNTER, &resp->body());
  AppendMetricValue("list_reads", m.qlist_stats.total_node_reads, {"type"}, {"total"},
                    &resp->body());
  AppendMetricValue("list_reads", m.qlist_stats.interior_node_reads, {"type"}, {"interior"},
                    &resp->body());

  AppendMetricHeader("list_compression_attempts", "List compression attempts", MetricType::COUNTER,
                     &resp->body());
  AppendMetricValue("list_compression_attempts", m.qlist_stats.compression_attempts, {"type"},
                    {"total"}, &resp->body());
  AppendMetricValue("list_compression_attempts", m.qlist_stats.bad_compression_attempts, {"type"},
                    {"fail"}, &resp->body());

  AppendMetricHeader("list_compressed_bytes", "List compressed bytes", MetricType::GAUGE,
                     &resp->body());
  AppendMetricValue("list_compressed_bytes", m.qlist_stats.compressed_bytes, {"type"},
                    {"compressed"}, &resp->body());
  AppendMetricValue("list_compressed_bytes", m.qlist_stats.raw_compressed_bytes, {"type"}, {"raw"},
                    &resp->body());

  // Tiered metrics
  {
    AppendMetricWithoutLabels("tiered_entries", "Tiered entries", total.tiered_entries,
                              MetricType::GAUGE, &resp->body());

    // Bytes: used, allocated, capacity
    AppendMetricHeader("tiered_bytes", "Tiered bytes", MetricType::GAUGE, &resp->body());
    AppendMetricValue("tiered_bytes", total.tiered_used_bytes, {"type"}, {"used"}, &resp->body());
    AppendMetricValue("tiered_bytes", m.tiered_stats.cold_storage_bytes, {"type"}, {"cold"},
                      &resp->body());
    AppendMetricValue("tiered_bytes", m.tiered_stats.allocated_bytes, {"type"}, {"allocated"},
                      &resp->body());
    AppendMetricValue("tiered_bytes", m.tiered_stats.capacity_bytes, {"type"}, {"capacity"},
                      &resp->body());

    // Events: stash, fetch, upload, cancel
    AppendMetricHeader("tiered_events", "Tiered events", MetricType::COUNTER, &resp->body());
    AppendMetricValue("tiered_events", m.tiered_stats.total_stashes, {"type"}, {"stash"},
                      &resp->body());
    AppendMetricValue("tiered_events", m.tiered_stats.total_fetches, {"type"}, {"fetch"},
                      &resp->body());
    AppendMetricValue("tiered_events", m.tiered_stats.total_uploads, {"type"}, {"upload"},
                      &resp->body());
    AppendMetricValue("tiered_events", m.tiered_stats.total_cancels, {"type"}, {"cancel"},
                      &resp->body());
    AppendMetricValue("tiered_events", m.tiered_stats.total_deletes, {"type"}, {"delete"},
                      &resp->body());

    // Hits: ram, cool, missed
    AppendMetricHeader("tiered_hits", "Tiered hits", MetricType::COUNTER, &resp->body());
    AppendMetricValue("tiered_hits", m.events.ram_hits, {"type"}, {"ram"}, &resp->body());
    AppendMetricValue("tiered_hits", m.events.ram_cool_hits, {"type"}, {"cool"}, &resp->body());
    AppendMetricValue("tiered_hits", m.events.ram_misses, {"type"}, {"disk"}, &resp->body());

    // Potential problems due to overloading system
    AppendMetricHeader("tiered_overload", "Potential problems due to overloading",
                       MetricType::COUNTER, &resp->body());
    AppendMetricValue("tiered_overload", m.tiered_stats.total_clients_throttled, {"type"},
                      {"client throttling"}, &resp->body());
    AppendMetricValue("tiered_overload", m.tiered_stats.total_stash_overflows, {"type"},
                      {"stash overflows"}, &resp->body());

    AppendMetricHeader("tiered_list_events", "Tiered List Events", MetricType::COUNTER,
                       &resp->body());
    AppendMetricValue("tiered_list_events", m.qlist_stats.offload_requests, {"type"}, {"offload"},
                      &resp->body());
    AppendMetricValue("tiered_list_events", m.qlist_stats.onload_requests, {"type"}, {"onload"},
                      &resp->body());
  }

  // Replication Info
  if (m.replica_side_info) {
    const ReplicaSummary& rsummary = m.replica_side_info->summary;
    AppendMetricWithoutLabels("master_link_status", "1 if up 0 if down",
                              rsummary.master_link_established ? 1 : 0, MetricType::GAUGE,
                              &resp->body());
    AppendMetricWithoutLabels("master_last_io_seconds_ago", "Last Master IO Seconds Ago",
                              rsummary.master_last_io_sec, MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("master_sync_in_progress", "1 if true 0 if false",
                              rsummary.full_sync_in_progress ? 1 : 0, MetricType::GAUGE,
                              &resp->body());
    // Print last known offset either during stable sync (online) or during disconnects when
    // the full sync phase did not start yet.
    if (rsummary.full_sync_done || (rsummary.passed_full_sync && !rsummary.master_link_established))
      AppendMetricWithoutLabels("slave_repl_offset", "Slave Replication Offset",
                                rsummary.repl_offset_sum, MetricType::GAUGE, &resp->body());
  }

  // Stream access pattern metrics
  if (m.shard_stats.stream_sequential_accesses || m.shard_stats.stream_random_accesses ||
      m.shard_stats.stream_fetch_all_accesses) {
    AppendMetricHeader("stream_accesses_total", "Total stream accesses by type",
                       MetricType::COUNTER, &resp->body());
    AppendMetricValue("stream_accesses_total", m.shard_stats.stream_sequential_accesses,
                      {"access_type"}, {"sequential"}, &resp->body());
    AppendMetricValue("stream_accesses_total", m.shard_stats.stream_random_accesses,
                      {"access_type"}, {"random"}, &resp->body());
    AppendMetricValue("stream_accesses_total", m.shard_stats.stream_fetch_all_accesses,
                      {"access_type"}, {"fetch_all"}, &resp->body());
  }
}

void Metrics::Merge(const Metrics& src) {
  // Sum of all member sizes plus alignment padding (4 bytes after blocked_tasks).
  // Expressed via sizeof so it adapts to different STL/Abseil implementations.
  // If this fires, a field was added/removed - update Merge() and InitFromThread().
  static_assert(
      sizeof(Metrics) == sizeof(SliceEvents) + sizeof(std::vector<DbStats>) +
                             sizeof(EngineShard::Stats) + sizeof(facade::FacadeStats) +
                             sizeof(TieredStats) + sizeof(SearchStats) +
                             sizeof(ServerState::Stats) + sizeof(PeakStats) + sizeof(QList::Stats) +
                             sizeof(ReplicationMemoryStats) + sizeof(InterpreterManager::Stats) +
                             sizeof(std::vector<std::pair<uint64_t, uint64_t>>) +
                             sizeof(absl::flat_hash_map<std::string, uint64_t>) +
                             sizeof(std::optional<Metrics::ReplicaInfo>) + sizeof(LoadingStats) +
                             sizeof(absl::flat_hash_map<std::string, hdr_histogram*>) +
                             sizeof(InternedStringStats) + sizeof(acl::UserRegistry::AclStats) +
                             184,  // scalar fields + proactor_idle_ratio + 4-byte align padding
      "Metrics size changed - update Merge() and InitFromThread()");

  // Per-db stats / events / small_string_bytes are merged element-wise.
  if (src.db_stats.size() > db_stats.size())
    db_stats.resize(src.db_stats.size());
  for (size_t i = 0; i < src.db_stats.size(); ++i)
    db_stats[i] += src.db_stats[i];
  events += src.events;
  small_string_bytes += src.small_string_bytes;

  // Aggregate sub-structs.
  shard_stats += src.shard_stats;
  facade_stats += src.facade_stats;
  tiered_stats += src.tiered_stats;
  search_stats += src.search_stats;
  coordinator_stats.Add(src.coordinator_stats);
  qlist_stats += src.qlist_stats;
  replication_stats += src.replication_stats;
  lua_stats += src.lua_stats;
  interned_string_stats += src.interned_string_stats;

  // Scalar sums.
  qps += src.qps;
  heap_used_bytes += src.heap_used_bytes;
  serialization_bytes += src.serialization_bytes;
  proactor_idle_ratio += src.proactor_idle_ratio;  // summed here; averaged in GetMetrics
  traverse_ttl_per_sec += src.traverse_ttl_per_sec;
  delete_ttl_per_sec += src.delete_ttl_per_sec;
  fiber_switch_cnt += src.fiber_switch_cnt;
  fiber_switch_delay_usec += src.fiber_switch_delay_usec;
  fiber_longrun_cnt += src.fiber_longrun_cnt;
  fiber_longrun_usec += src.fiber_longrun_usec;
  worker_fiber_stack_size += src.worker_fiber_stack_size;
  worker_fiber_count += src.worker_fiber_count;
  blocked_tasks += src.blocked_tasks;
  tls_bytes += src.tls_bytes;
  refused_conn_max_clients_reached_count += src.refused_conn_max_clients_reached_count;
  lsn_buffer_size += src.lsn_buffer_size;
  lsn_buffer_bytes += src.lsn_buffer_bytes;

  // Non-sum reductions.
  tx_queue_len = std::max(tx_queue_len, src.tx_queue_len);
  oldest_pending_send_ts = std::min(oldest_pending_send_ts, src.oldest_pending_send_ts);

  // Map merges.
  for (const auto& [k, v] : src.connections_lib_name_ver_map)
    connections_lib_name_ver_map[k] += v;

  if (src.cmd_call_stats.size() > cmd_call_stats.size())
    cmd_call_stats.resize(src.cmd_call_stats.size());
  for (size_t i = 0; i < src.cmd_call_stats.size(); ++i) {
    cmd_call_stats[i].first += src.cmd_call_stats[i].first;
    cmd_call_stats[i].second += src.cmd_call_stats[i].second;
  }
}

void Metrics::InitFromThread(Namespace* ns, const CommandRegistry* registry,
                             unsigned proactor_index, const MetricsCollectOpts& opts,
                             DflyCmd* dfly_cmd) {
  static_assert(
      sizeof(Metrics) == sizeof(SliceEvents) + sizeof(std::vector<DbStats>) +
                             sizeof(EngineShard::Stats) + sizeof(facade::FacadeStats) +
                             sizeof(TieredStats) + sizeof(SearchStats) +
                             sizeof(ServerState::Stats) + sizeof(PeakStats) + sizeof(QList::Stats) +
                             sizeof(ReplicationMemoryStats) + sizeof(InterpreterManager::Stats) +
                             sizeof(std::vector<std::pair<uint64_t, uint64_t>>) +
                             sizeof(absl::flat_hash_map<std::string, uint64_t>) +
                             sizeof(std::optional<Metrics::ReplicaInfo>) + sizeof(LoadingStats) +
                             sizeof(absl::flat_hash_map<std::string, hdr_histogram*>) +
                             sizeof(InternedStringStats) + sizeof(acl::UserRegistry::AclStats) +
                             184,  // scalar fields + proactor_idle_ratio + 4-byte align padding
      "Metrics size changed - update Merge() and InitFromThread()");
  EngineShard* shard = EngineShard::tlocal();
  ServerState* ss = ServerState::tlocal();

  fiber_switch_cnt = fb2::FiberSwitchEpoch();
  fiber_switch_delay_usec = fb2::FiberSwitchDelayUsec();
  fiber_longrun_cnt = fb2::FiberLongRunCnt();
  fiber_longrun_usec = fb2::FiberLongRunSumUsec();
  worker_fiber_stack_size = fb2::WorkerFibersStackSize();
  worker_fiber_count = fb2::WorkerFibersCount();
  blocked_tasks = TaskQueue::blocked_submitters();

  coordinator_stats.Add(ss->stats);

  qps = uint64_t(ss->MovingSum6());
  facade_stats = *tl_facade_stats;
  serialization_bytes = SliceSnapshot::GetThreadLocalMemoryUsage();

  // Per-proactor idle ratio (0..1). ProactorBase::me() is valid inside the per-thread
  // InitFromThread callback dispatched by ProactorPool::AwaitBrief.
  if (fb2::ProactorBase* pb = fb2::ProactorBase::me())
    proactor_idle_ratio = pb->IdleRatio();

  if (shard) {
    heap_used_bytes = shard->UsedMemory();
    const DbSlice::Stats slice_stats = ns->GetDbSlice(shard->shard_id()).GetStats();
    db_stats = slice_stats.db_stats;
    events = slice_stats.events;
    small_string_bytes = slice_stats.small_string_bytes;
    shard_stats = shard->stats();

    if (shard->tiered_storage()) {
      tiered_stats = shard->tiered_storage()->GetStats();
    }

    if (shard->search_indices()) {
      search_stats = shard->search_indices()->GetStats();
    }

    qlist_stats = QList::stats;

    traverse_ttl_per_sec = shard->GetMovingSum6(EngineShard::TTL_TRAVERSE);
    delete_ttl_per_sec = shard->GetMovingSum6(EngineShard::TTL_DELETE);
    tx_queue_len = shard->txq()->size();

    if (shard->journal()) {
      lsn_buffer_size = journal::LsnBufferSize();
      lsn_buffer_bytes = journal::LsnBufferBytes();
    }

    if (opts.replication_memory)
      replication_stats = dfly_cmd->GetReplicationMemoryStats(shard);
  }

  tls_bytes = Listener::TLSUsedMemoryThreadLocal();
  refused_conn_max_clients_reached_count = Listener::RefusedConnectionMaxClientsCount();

  lua_stats = InterpreterManager::tl_stats();

  connections_lib_name_ver_map = facade::Connection::GetLibStatsTL();

  auto& send_list = facade::SinkReplyBuilder::pending_list;
  if (!send_list.empty()) {
    DCHECK(
        std::is_sorted(send_list.begin(), send_list.end(), [](const auto& left, const auto& right) {
          return left.timestamp_cycles < right.timestamp_cycles;
        }));
    oldest_pending_send_ts = send_list.front().timestamp_cycles;
  }

  // Skip when no rendered section needs per-command stats — leaving the vector empty makes the
  // later Merge and NamedCallStats no-ops.
  if (opts.cmd_stats) {
    cmd_call_stats.resize(registry->size());
    registry->CollectThreadCallStats(proactor_index, cmd_call_stats.data());
  }

  interned_string_stats = GetInternedStringStats();
}

}  // namespace dfly
