// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/qlist.h"
#include "facade/facade_stats.h"
#include "server/acl/user_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/replica_types.h"
#include "server/server_state.h"
#include "server/stats.h"
#include "util/http/http_server_utils.h"

struct hdr_histogram;

namespace dfly {

class CommandRegistry;
class DflyCmd;

struct ReplicationMemoryStats {
  size_t streamer_buf_capacity_bytes = 0;  // total capacities of streamer buffers
  size_t full_sync_buf_bytes = 0;          // total bytes used for full sync buffers

  ReplicationMemoryStats& operator+=(const ReplicationMemoryStats& o) {
    streamer_buf_capacity_bytes += o.streamer_buf_capacity_bytes;
    full_sync_buf_bytes += o.full_sync_buf_bytes;
    return *this;
  }
};

struct LoadingStats {
  size_t restore_count = 0;
  size_t failed_restore_count = 0;

  size_t backup_count = 0;
  size_t failed_backup_count = 0;
};

// Global peak stats recorded after aggregating metrics over all shards.
// Note that those values are only updated during GetMetrics calls.
struct PeakStats {
  size_t conn_dispatch_queue_bytes = 0;  // peak value of conn_stats.dispatch_queue_bytes
  size_t conn_read_buf_capacity = 0;     // peak of total read buf capcacities
};

// Aggregated metrics over multiple sources on all shards.
struct Metrics {
  SliceEvents events;              // general keyspace stats
  std::vector<DbStats> db_stats;   // dbsize stats
  EngineShard::Stats shard_stats;  // per-shard stats

  facade::FacadeStats facade_stats;  // client stats and buffer sizes
  TieredStats tiered_stats;

  SearchStats search_stats;
  ServerState::Stats coordinator_stats;  // stats on transaction running
  PeakStats peak_stats;
  QList::Stats qlist_stats;

  size_t qps = 0;

  size_t used_mem_peak = 0;
  size_t used_mem_rss_peak = 0;

  size_t heap_used_bytes = 0;
  size_t small_string_bytes = 0;
  uint32_t traverse_ttl_per_sec = 0;
  uint32_t delete_ttl_per_sec = 0;
  uint64_t hoffman_encode_total = 0, hoffman_encode_success = 0;
  uint64_t fiber_switch_cnt = 0;
  uint64_t fiber_switch_delay_usec = 0;
  uint64_t tls_bytes = 0;
  uint64_t refused_conn_max_clients_reached_count = 0;
  uint64_t serialization_bytes = 0;

  // Statistics about fibers running for a long time (more than 1ms).
  uint64_t fiber_longrun_cnt = 0;
  uint64_t fiber_longrun_usec = 0;

  // Max length of the all the tx shard-queues.
  uint32_t tx_queue_len = 0;
  uint32_t worker_fiber_count = 0;
  uint32_t blocked_tasks = 0;
  size_t worker_fiber_stack_size = 0;

  size_t lsn_buffer_size = 0;
  size_t lsn_buffer_bytes = 0;

  // Meaningful only on a master (zero on replicas / no replicas).
  ReplicationMemoryStats replication_stats;

  // CPU cycles timestamp (CycleClock) of the connection stuck on send for longest time.
  uint64_t oldest_pending_send_ts = uint64_t(-1);

  InterpreterManager::Stats lua_stats;

  // command call frequencies (count, aggregated latency in usec).
  std::map<std::string, std::pair<uint64_t, uint64_t>> cmd_stats_map;

  absl::flat_hash_map<std::string, uint64_t> connections_lib_name_ver_map;

  struct ReplicaInfo {
    ReplicaSummary summary;

    // cluster
    std::vector<ReplicaSummary> cl_repl_summary;
  };

  // Replica reconnect stats on the replica side. Undefined for master
  std::optional<ReplicaInfo> replica_side_info;

  size_t migration_errors_total;

  LoadingStats loading_stats;

  absl::flat_hash_map<std::string, hdr_histogram*> cmd_latency_map;

  InternedStringStats interned_string_stats;

  acl::UserRegistry::AclStats acl_stats;

  // Collects all thread-local / per-shard stats on the current proactor thread.
  void InitFromThread(Namespace* ns, CommandRegistry* registry, unsigned proactor_index,
                      bool collect_replication_memory, DflyCmd* dfly_cmd);

  // Folds `src` into *this. Almost everything sums; tx_queue_len takes the max
  // and oldest_pending_send_ts the min.
  void Merge(const Metrics& src);

  void Print(uint64_t uptime, DflyCmd* dfly_cmd, util::http::StringResponse* resp, bool legacy);
};

}  // namespace dfly
