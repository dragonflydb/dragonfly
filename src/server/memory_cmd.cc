// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/memory_cmd.h"

#include <absl/strings/str_cat.h>

#ifdef __linux__
#include <malloc.h>
#endif

#include <mimalloc.h>

#include "base/logging.h"
#include "core/allocation_tracker.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/error.h"
#include "io/io_buf.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/snapshot.h"
#include "server/transaction.h"

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

std::string MallocStatsCb(bool backing, unsigned tid) {
  string str;

  uint64_t start = absl::GetCurrentTimeNanos();

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
  absl::StrAppend(&str, "total reserved: ", reserved, ", comitted: ", committed, ", used: ", used,
                  " fragmentation waste: ",
                  (100.0 * (committed - used)) / std::max<size_t>(1UL, committed), "%\n");
  absl::StrAppend(&str, "--- End mimalloc statistics, took ", delta, "us ---\n");

  return str;
}

size_t MemoryUsage(PrimeIterator it) {
  size_t key_size = it->first.MallocUsed();
  return key_size + it->second.MallocUsed(true);
}

}  // namespace

MemoryCmd::MemoryCmd(ServerFamily* owner, facade::SinkReplyBuilder* builder,
                     ConnectionContext* cntx)
    : cntx_(cntx), owner_(owner), builder_(builder) {
}

void MemoryCmd::Run(CmdArgList args) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (sub_cmd == "HELP") {
    string_view help_arr[] = {
        "MEMORY <subcommand> [<arg> ...]. Subcommands are:",
        "STATS",
        "    Shows breakdown of memory.",
        "MALLOC-STATS",
        "    Show global malloc stats as provided by allocator libraries",
        "ARENA BACKING] [thread-id]",
        "    Show mimalloc arena stats for a heap residing in specified thread-id. 0 by default.",
        "    If BACKING is specified, show stats for the backing heap.",
        "ARENA SHOW",
        "    Prints the arena summary report for the entire process.",
        "    Requires MIMALLOC_VERBOSE=1 environment to be set. The output goes to stdout",
        "USAGE <key>",
        "    Show memory usage of a key.",
        "DECOMMIT",
        "    Force decommit the memory freed by the server back to OS.",
        "TRACK",
        "    Allow tracking of memory allocation via `new` and `delete` based on input criteria.",
        "    USE WITH CAUTIOUS! This command is designed for Dragonfly developers.",
        "    ADD <lower-bound> <upper-bound> <sample-odds>",
        "        Sets up tracking memory allocations in the (inclusive) range [lower, upper]",
        "        sample-odds indicates how many of the allocations will be logged, there 0 means "
        "none, 1 means all, and everything in between is linear",
        "        There could be at most 4 tracking placed in parallel",
        "    REMOVE <lower-bound> <upper-bound>",
        "        Removes all memory tracking added which match bounds",
        "        Could remove 0, 1 or more",
        "    CLEAR",
        "        Removes all memory tracking",
        "    GET",
        "        Returns an array with all active tracking",
        "    ADDRESS <address>",
        "        Returns whether <address> is known to be allocated internally by any of the "
        "backing heaps",
    };
    auto* rb = static_cast<RedisReplyBuilder*>(builder_);
    return rb->SendSimpleStrArr(help_arr);
  };

  if (sub_cmd == "STATS") {
    return Stats();
  }

  if (sub_cmd == "USAGE" && args.size() > 1) {
    string_view key = ArgS(args, 1);
    return Usage(key);
  }

  if (sub_cmd == "DECOMMIT") {
    shard_set->pool()->AwaitBrief(
        [](unsigned, auto* pb) { ServerState::tlocal()->DecommitMemory(ServerState::kAllMemory); });
    return builder_->SendSimpleString("OK");
  }

  if (sub_cmd == "MALLOC-STATS") {
    return MallocStats();
  }

  if (sub_cmd == "ARENA") {
    return ArenaStats(args);
  }

  if (sub_cmd == "TRACK") {
    args.remove_prefix(1);
    return Track(args);
  }

  if (sub_cmd == "DEFRAGMENT") {
    shard_set->pool()->DispatchOnAll([](util::ProactorBase*) {
      if (auto* shard = EngineShard::tlocal(); shard)
        shard->ForceDefrag();
    });
    return builder_->SendSimpleString("OK");
  }

  string err = UnknownSubCmd(sub_cmd, "MEMORY");
  return builder_->SendError(err, kSyntaxErrType);
}

namespace {

struct ConnectionMemoryUsage {
  size_t connection_count = 0;
  size_t connection_size = 0;
  size_t pipelined_bytes = 0;
  io::IoBuf::MemoryUsage connections_memory;

  size_t replication_connection_count = 0;
  size_t replication_connection_size = 0;
  io::IoBuf::MemoryUsage replication_memory;
};

ConnectionMemoryUsage GetConnectionMemoryUsage(ServerFamily* server) {
  vector<ConnectionMemoryUsage> mems(shard_set->pool()->size());

  for (auto* listener : server->GetListeners()) {
    listener->TraverseConnections([&](unsigned thread_index, util::Connection* conn) {
      if (conn == nullptr) {
        return;
      }

      auto* dfly_conn = static_cast<facade::Connection*>(conn);
      auto* cntx = static_cast<ConnectionContext*>(dfly_conn->cntx());

      auto usage = dfly_conn->GetMemoryUsage();
      if (cntx == nullptr || cntx->replication_flow == nullptr) {
        mems[thread_index].connection_count++;
        mems[thread_index].connection_size += usage.mem;
        mems[thread_index].connections_memory += usage.buf_mem;
      } else {
        mems[thread_index].replication_connection_count++;
        mems[thread_index].replication_connection_size += usage.mem;
        mems[thread_index].replication_memory += usage.buf_mem;
      }
    });
  }

  shard_set->pool()->AwaitBrief([&](unsigned index, auto*) {
    mems[index].pipelined_bytes += tl_facade_stats->conn_stats.pipeline_cmd_cache_bytes;
    mems[index].pipelined_bytes += tl_facade_stats->conn_stats.dispatch_queue_bytes;
  });

  ConnectionMemoryUsage mem;
  for (const auto& m : mems) {
    mem.connection_count += m.connection_count;
    mem.pipelined_bytes += m.pipelined_bytes;
    mem.connection_size += m.connection_size;
    mem.connections_memory += m.connections_memory;
    mem.replication_connection_count += m.replication_connection_count;
    mem.replication_connection_size += m.replication_connection_size;
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
  ConnectionMemoryUsage connection_memory = GetConnectionMemoryUsage(owner_);

  // Connection stats, excluding replication connections
  stats.push_back({"connections.count", connection_memory.connection_count});
  stats.push_back({"connections.direct_bytes", connection_memory.connection_size});
  PushMemoryUsageStats(
      connection_memory.connections_memory, "connections",
      connection_memory.connections_memory.GetTotalSize() + connection_memory.connection_size,
      &stats);

  // Replication connection stats
  stats.push_back(
      {"replication.connections_count", connection_memory.replication_connection_count});
  stats.push_back({"replication.direct_bytes", connection_memory.replication_connection_size});
  PushMemoryUsageStats(connection_memory.replication_memory, "replication",
                       connection_memory.replication_memory.GetTotalSize() +
                           connection_memory.replication_connection_size,
                       &stats);

  auto* rb = static_cast<RedisReplyBuilder*>(builder_);
  rb->StartCollection(stats.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : stats) {
    rb->SendBulkString(k);
    rb->SendLong(v);
  }
}

void MemoryCmd::MallocStats() {
  string report;

#if __GLIBC__  // MUSL/alpine do not have mallinfo routines.
#if __GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33)
  struct mallinfo2 malloc_info = mallinfo2();
#else
  struct mallinfo malloc_info = mallinfo();  // buggy because 32-bit stats may overflow.
#endif

  absl::StrAppend(&report, "___ Begin malloc stats ___\n");
  absl::StrAppend(&report, "arena: ", malloc_info.arena, ", ordblks: ", malloc_info.ordblks,
                  ", smblks: ", malloc_info.smblks, "\n");
  absl::StrAppend(&report, "hblks: ", malloc_info.hblks, ", hblkhd: ", malloc_info.hblkhd,
                  ", usmblks: ", malloc_info.usmblks, "\n");
  absl::StrAppend(&report, "fsmblks: ", malloc_info.fsmblks, ", uordblks: ", malloc_info.uordblks,
                  ", fordblks: ", malloc_info.fordblks, ", keepcost: ", malloc_info.keepcost, "\n");
  absl::StrAppend(&report, "___ End malloc stats ___\n\n");
#endif

  absl::StrAppend(&report, "___ Begin mimalloc stats ___\n");
  mi_stats_print_out(MiStatsCallback, &report);
  absl::StrAppend(&report, "___ End mimalloc stats ___\n\n");

  auto* rb = static_cast<RedisReplyBuilder*>(builder_);
  return rb->SendVerbatimString(report);
}

void MemoryCmd::ArenaStats(CmdArgList args) {
  uint32_t tid = 0;
  bool backing = false;
  bool show_arenas = false;
  if (args.size() >= 2) {
    string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 1));

    if (sub_cmd == "SHOW") {
      if (args.size() != 2)
        return builder_->SendError(kSyntaxErr, kSyntaxErrType);
      show_arenas = true;
    } else {
      unsigned tid_indx = 1;

      if (sub_cmd == "BACKING") {
        ++tid_indx;
        backing = true;
      }
      if (args.size() > tid_indx && !absl::SimpleAtoi(ArgS(args, tid_indx), &tid)) {
        return builder_->SendError(kInvalidIntErr);
      }
    }
  }

  if (show_arenas) {
    mi_debug_show_arenas(true, true, true);
    return builder_->SendOk();
  }

  if (backing && tid >= shard_set->pool()->size()) {
    return builder_->SendError(
        absl::StrCat("Thread id must be less than ", shard_set->pool()->size()));
  }

  if (!backing && tid >= shard_set->size()) {
    return builder_->SendError(absl::StrCat("Thread id must be less than ", shard_set->size()));
  }

  string mi_malloc_info =
      shard_set->pool()->at(tid)->AwaitBrief([=] { return MallocStatsCb(backing, tid); });

  auto* rb = static_cast<RedisReplyBuilder*>(builder_);
  return rb->SendVerbatimString(mi_malloc_info);
}

void MemoryCmd::Usage(std::string_view key) {
  ShardId sid = Shard(key, shard_set->size());
  ssize_t memory_usage = shard_set->pool()->at(sid)->AwaitBrief([key, this, sid]() -> ssize_t {
    auto& db_slice = cntx_->ns->GetDbSlice(sid);
    auto [pt, exp_t] = db_slice.GetTables(cntx_->db_index());
    PrimeIterator it = pt->Find(key);
    if (IsValid(it)) {
      return MemoryUsage(it);
    } else {
      return -1;
    }
  });

  auto* rb = static_cast<RedisReplyBuilder*>(builder_);
  if (memory_usage < 0)
    return rb->SendNull();
  rb->SendLong(memory_usage);
}

void MemoryCmd::Track(CmdArgList args) {
#ifndef DFLY_ENABLE_MEMORY_TRACKING
  return builder_->SendError("MEMORY TRACK must be enabled at build time.");
#endif

  CmdArgParser parser(args);

  if (parser.Check("ADD")) {
    AllocationTracker::TrackingInfo tracking_info;
    std::tie(tracking_info.lower_bound, tracking_info.upper_bound, tracking_info.sample_odds) =
        parser.Next<size_t, size_t, double>();
    if (parser.HasError()) {
      return builder_->SendError(parser.Error()->MakeReply());
    }

    atomic_bool error{false};
    shard_set->pool()->AwaitBrief([&](unsigned index, auto*) {
      if (!AllocationTracker::Get().Add(tracking_info)) {
        error.store(true);
      }
    });

    if (error.load()) {
      return builder_->SendError("Unable to add tracker");
    } else {
      return builder_->SendOk();
    }
  }

  if (parser.Check("REMOVE")) {
    auto [lower_bound, upper_bound] = parser.Next<size_t, size_t>();
    if (parser.HasError()) {
      return builder_->SendError(parser.Error()->MakeReply());
    }

    atomic_bool error{false};
    shard_set->pool()->AwaitBrief([&, lo = lower_bound, hi = upper_bound](unsigned index, auto*) {
      if (!AllocationTracker::Get().Remove(lo, hi)) {
        error.store(true);
      }
    });

    if (error.load()) {
      return builder_->SendError("Unable to remove tracker");
    } else {
      return builder_->SendOk();
    }
  }

  if (parser.Check("CLEAR")) {
    shard_set->pool()->AwaitBrief([&](unsigned index, auto*) { AllocationTracker::Get().Clear(); });
    return builder_->SendOk();
  }

  if (parser.Check("GET")) {
    auto ranges = AllocationTracker::Get().GetRanges();
    auto* rb = static_cast<facade::RedisReplyBuilder*>(builder_);
    rb->StartArray(ranges.size());
    for (const auto& range : ranges) {
      rb->SendSimpleString(
          absl::StrCat(range.lower_bound, ",", range.upper_bound, ",", range.sample_odds));
    }
    return;
  }

  if (parser.Check("ADDRESS")) {
    string_view ptr_str = parser.Next();
    if (parser.HasError()) {
      return builder_->SendError(parser.Error()->MakeReply());
    }

    size_t ptr = 0;
    if (!absl::SimpleHexAtoi(ptr_str, &ptr)) {
      return builder_->SendError("Address must be hex number");
    }

    atomic_bool found{false};
    shard_set->pool()->AwaitBrief([&](unsigned index, auto*) {
      if (mi_heap_check_owned(mi_heap_get_backing(), (void*)ptr)) {
        found.store(true);
      }
    });

    return builder_->SendSimpleString(found.load() ? "FOUND" : "NOT-FOUND");
  }

  return builder_->SendError(kSyntaxErrType);
}

}  // namespace dfly
