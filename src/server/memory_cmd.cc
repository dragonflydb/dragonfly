// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/memory_cmd.h"

#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include "base/io_buf.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/snapshot.h"

using namespace std;
using namespace facade;

namespace dfly {

namespace {

void MiStatsCallback(const char* msg, void* arg) {
  string* str = (string*)arg;
  absl::StrAppend(str, msg);
}

// blocksize, reserved, committed, used.
using BlockKey = std::tuple<size_t, size_t, size_t, size_t>;
using BlockMap = absl::flat_hash_map<BlockKey, uint64_t>;

bool MiArenaVisit(const mi_heap_t* heap, const mi_heap_area_t* area, void* block, size_t block_size,
                  void* arg) {
  BlockMap* bmap = (BlockMap*)arg;
  BlockKey bkey{block_size, area->reserved, area->committed, area->used * block_size};
  (*bmap)[bkey]++;

  return true;
};

std::string MallocStats(bool backing, unsigned tid) {
  string str;

  uint64_t start = absl::GetCurrentTimeNanos();
  absl::StrAppend(&str, "___ Begin mimalloc statistics ___\n");
  mi_stats_print_out(MiStatsCallback, &str);

  absl::StrAppend(&str, "\nArena statistics from thread:", tid, "\n");
  absl::StrAppend(&str, "Count BlockSize Reserved Committed Used\n");

  mi_heap_t* data_heap = backing ? mi_heap_get_backing() : ServerState::tlocal()->data_heap();
  BlockMap block_map;

  mi_heap_visit_blocks(data_heap, false /* visit all blocks*/, MiArenaVisit, &block_map);
  uint64_t reserved = 0, committed = 0, used = 0;
  for (const auto& k_v : block_map) {
    uint64_t count = k_v.second;
    absl::StrAppend(&str, count, " ", get<0>(k_v.first), " ", get<1>(k_v.first), " ",
                    get<2>(k_v.first), " ", get<3>(k_v.first), "\n");
    reserved += count * get<1>(k_v.first);
    committed += count * get<2>(k_v.first);
    used += count * get<3>(k_v.first);
  }

  uint64_t delta = (absl::GetCurrentTimeNanos() - start) / 1000;
  absl::StrAppend(&str, "--- End mimalloc statistics, took ", delta, "us ---\n");
  absl::StrAppend(&str, "total reserved: ", reserved, ", comitted: ", committed, ", used: ", used,
                  " fragmentation waste: ",
                  (100.0 * (committed - used)) / std::max<size_t>(1UL, committed), "%\n");

  return str;
}

size_t MemoryUsage(PrimeIterator it) {
  return it->first.MallocUsed() + it->second.MallocUsed();
}

}  // namespace

MemoryCmd::MemoryCmd(ServerFamily* owner, ConnectionContext* cntx) : cntx_(cntx), owner_(owner) {
}

void MemoryCmd::Run(CmdArgList args) {
  string_view sub_cmd = ArgS(args, 0);

  if (sub_cmd == "HELP") {
    string_view help_arr[] = {
        "MEMORY <subcommand> [<arg> ...]. Subcommands are:",
        "STATS",
        "    Shows breakdown of memory.",
        "MALLOC-STATS [BACKING] [thread-id]",
        "    Show malloc stats for a heap residing in specified thread-id. 0 by default.",
        "    If BACKING is specified, show stats for the backing heap.",
        "USAGE <key>",
        "    Show memory usage of a key.",
        "DECOMMIT",
        "    Force decommit the memory freed by the server back to OS.",
    };
    return (*cntx_)->SendSimpleStrArr(help_arr);
  };

  if (sub_cmd == "STATS") {
    return Stats();
  }

  if (sub_cmd == "USAGE" && args.size() > 1) {
    string_view key = ArgS(args, 1);
    return Usage(key);
  }

  if (sub_cmd == "DECOMMIT") {
    shard_set->pool()->Await([](auto* pb) {
      mi_heap_collect(ServerState::tlocal()->data_heap(), true);
      mi_heap_collect(mi_heap_get_backing(), true);
    });
    return (*cntx_)->SendSimpleString("OK");
  }

  if (sub_cmd == "MALLOC-STATS") {
    uint32_t tid = 0;
    bool backing = false;
    if (args.size() >= 2) {
      ToUpper(&args[1]);

      unsigned tid_indx = 1;
      if (ArgS(args, tid_indx) == "BACKING") {
        ++tid_indx;
        backing = true;
      }

      if (args.size() > tid_indx && !absl::SimpleAtoi(ArgS(args, tid_indx), &tid)) {
        return (*cntx_)->SendError(kInvalidIntErr);
      }
    }

    if (backing && tid >= shard_set->pool()->size()) {
      return cntx_->SendError(
          absl::StrCat("Thread id must be less than ", shard_set->pool()->size()));
    }

    if (!backing && tid >= shard_set->size()) {
      return cntx_->SendError(absl::StrCat("Thread id must be less than ", shard_set->size()));
    }

    string res = shard_set->pool()->at(tid)->AwaitBrief([=] { return MallocStats(backing, tid); });

    return (*cntx_)->SendBulkString(res);
  }

  string err = UnknownSubCmd(sub_cmd, "MEMORY");
  return (*cntx_)->SendError(err, kSyntaxErrType);
}

namespace {

struct ConnectionMemoryUsage {
  size_t connection_count = 0;
  size_t pipelined_bytes = 0;
  base::IoBuf::MemoryUsage connections_memory;

  size_t replication_connection_count = 0;
  base::IoBuf::MemoryUsage replication_memory;
};

ConnectionMemoryUsage GetConnectionMemoryUsage(ServerFamily* server) {
  vector<ConnectionMemoryUsage> mems(shard_set->size());

  for (auto* listener : server->GetListeners()) {
    listener->TraverseConnections([&](unsigned thread_index, util::Connection* conn) {
      auto* dfly_conn = static_cast<facade::Connection*>(conn);
      auto* cntx = static_cast<ConnectionContext*>(dfly_conn->cntx());

      if (cntx->replication_flow == nullptr) {
        mems[thread_index].connection_count++;
        mems[thread_index].connections_memory += dfly_conn->GetMemoryUsage();
      } else {
        mems[thread_index].replication_connection_count++;
        mems[thread_index].replication_memory += dfly_conn->GetMemoryUsage();
      }

      if (cntx != nullptr) {
        mems[thread_index].pipelined_bytes +=
            cntx->conn_state.exec_info.body.capacity() * sizeof(StoredCmd);
        for (const auto& pipeline : cntx->conn_state.exec_info.body) {
          mems[thread_index].pipelined_bytes += pipeline.UsedHeapMemory();
        }
      }
    });
  }

  ConnectionMemoryUsage mem;
  for (const auto& m : mems) {
    mem.connection_count += m.connection_count;
    mem.pipelined_bytes += m.pipelined_bytes;
    mem.connections_memory += m.connections_memory;
    mem.replication_connection_count += m.replication_connection_count;
    mem.replication_memory += m.replication_memory;
  }
  return mem;
}

void PushMemoryUsageStats(const base::IoBuf::MemoryUsage& mem, string_view prefix, size_t total,
                          vector<pair<string, size_t>>* stats) {
  stats->push_back({absl::StrCat(prefix, ".total_bytes"), total});
  stats->push_back({absl::StrCat(prefix, ".consumed_bytes"), mem.consumed});
  stats->push_back({absl::StrCat(prefix, ".pending_input_bytes"), mem.input_length});
  stats->push_back({absl::StrCat(prefix, ".pending_output_bytes"), mem.append_length});
}

}  // namespace

void MemoryCmd::Stats() {
  vector<pair<string, size_t>> stats;
  stats.reserve(25);
  auto server_metrics = owner_->GetMetrics();

  // RSS
  stats.push_back({"rss_bytes", rss_mem_current.load(memory_order_relaxed)});
  stats.push_back({"rss_peak_bytes", rss_mem_peak.load(memory_order_relaxed)});

  // Used by DbShards and DashTable
  stats.push_back({"data_bytes", used_mem_current.load(memory_order_relaxed)});
  stats.push_back({"data_peak_bytes", used_mem_peak.load(memory_order_relaxed)});

  ConnectionMemoryUsage connection_memory = GetConnectionMemoryUsage(owner_);

  // Connection stats, excluding replication connections
  stats.push_back({"connections.count", connection_memory.connection_count});
  PushMemoryUsageStats(
      connection_memory.connections_memory, "connections",
      connection_memory.connections_memory.GetTotalSize() + connection_memory.pipelined_bytes,
      &stats);
  stats.push_back({"connections.pipeline_bytes", connection_memory.pipelined_bytes});

  // Replication connection stats
  stats.push_back(
      {"replication.connections_count", connection_memory.replication_connection_count});
  PushMemoryUsageStats(connection_memory.replication_memory, "replication",
                       connection_memory.replication_memory.GetTotalSize(), &stats);

  atomic<size_t> serialization_memory = 0;
  shard_set->pool()->AwaitFiberOnAll(
      [&](auto*) { serialization_memory.fetch_add(SliceSnapshot::GetThreadLocalMemoryUsage()); });

  // Serialization stats, including both replication-related serialization and saving to RDB files.
  stats.push_back({"serialization", serialization_memory.load()});

  (*cntx_)->StartCollection(stats.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : stats) {
    (*cntx_)->SendBulkString(k);
    (*cntx_)->SendLong(v);
  }
}

void MemoryCmd::Usage(std::string_view key) {
  ShardId sid = Shard(key, shard_set->size());
  ssize_t memory_usage = shard_set->pool()->at(sid)->AwaitBrief([key, this]() -> ssize_t {
    auto& db_slice = EngineShard::tlocal()->db_slice();
    auto [pt, exp_t] = db_slice.GetTables(cntx_->db_index());
    PrimeIterator it = pt->Find(key);
    if (IsValid(it)) {
      return MemoryUsage(it);
    } else {
      return -1;
    }
  });

  if (memory_usage < 0)
    return cntx_->SendError(kKeyNotFoundErr);
  (*cntx_)->SendLong(memory_usage);
}

}  // namespace dfly
