// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/memory_cmd.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#ifdef __linux__
#include <malloc.h>
#endif

#include <mimalloc.h>

#include "base/flags.h"
#include "core/allocation_tracker.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "io/io_buf.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"
#include "server/server_family.h"
#include "server/server_state.h"

using namespace std;
using namespace facade;

ABSL_DECLARE_FLAG(float, mem_defrag_page_utilization_threshold);

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
}

struct BlockSummary {
  size_t reserved = 0;
  size_t committed = 0;
  size_t used = 0;
};

using BlockSummaryMap = absl::flat_hash_map<size_t, BlockSummary>;

bool MiArenaVisitSummary(const mi_heap_t*, const mi_heap_area_t* area, void*, size_t block_size,
                         void* arg) {
  BlockSummaryMap* bsm = static_cast<BlockSummaryMap*>(arg);
  BlockSummary& block_stats = (*bsm)[block_size];
  block_stats.committed += area->committed;
  block_stats.reserved += area->reserved;
  block_stats.used += area->used * block_size;
  return true;
}

BlockSummaryMap CollectSummary(bool backing) {
  BlockSummaryMap summary;
  const mi_heap_t* data_heap = backing ? mi_heap_get_backing() : ServerState::tlocal()->data_heap();
  mi_heap_visit_blocks(data_heap, false, MiArenaVisitSummary, &summary);
  return summary;
}

vector<BlockSummaryMap> CollectSummaries(bool backing) {
  std::vector<BlockSummaryMap> summaries(shard_set->size());
  shard_set->RunBriefInParallel([&summaries, backing](EngineShard* shard) {
    summaries[shard->shard_id()] = CollectSummary(backing);
  });
  return summaries;
}

void FormatSummary(std::string* str, const BlockSummaryMap& summary) {
  absl::StrAppend(str, absl::StrFormat("%10s %10s %10s %10s %10s %8s\n", "BlockSize", "Reserved",
                                       "Committed", "Used", "Wasted", "Waste%"));
  std::vector<std::pair<size_t, BlockSummary>> entries{summary.begin(), summary.end()};
  std::ranges::sort(entries, {}, [](const auto& entry) {
    const BlockSummary& stats = entry.second;
    return stats.committed > stats.used ? stats.committed - stats.used : 0;
  });

  size_t total_reserved = 0;
  size_t total_committed = 0;
  size_t total_used = 0;

  for (const auto& [size, block_summary] : entries) {
    const size_t wasted = block_summary.committed > block_summary.used
                              ? block_summary.committed - block_summary.used
                              : 0;
    const double waste_pct = 100.0 * wasted / std::max<size_t>(1UL, block_summary.committed);
    absl::StrAppend(str, absl::StrFormat("%10zu %10zu %10zu %10zu %10zu %8.2f%%\n", size,
                                         block_summary.reserved, block_summary.committed,
                                         block_summary.used, wasted, waste_pct));
    total_reserved += block_summary.reserved;
    total_committed += block_summary.committed;
    total_used += block_summary.used;
  }

  const size_t wasted = total_committed > total_used ? total_committed - total_used : 0;
  absl::StrAppend(str, absl::StrFormat("%10s %10zu %10zu %10zu %10zu %8.2f%%\n", "Total:",
                                       total_reserved, total_committed, total_used, wasted,
                                       100.0 * wasted / std::max<size_t>(1UL, total_committed)));
}

string FormatSummaries(const vector<BlockSummaryMap>& summaries) {
  string str;
  BlockSummaryMap machine_wide;
  for (size_t i = 0; i < summaries.size(); ++i) {
    absl::StrAppend(&str, "\nArena statistics for thread ", i, ":\n");
    FormatSummary(&str, summaries[i]);
    for (const auto& [size, block_summary] : summaries[i]) {
      BlockSummary& machine_block = machine_wide[size];
      machine_block.reserved += block_summary.reserved;
      machine_block.committed += block_summary.committed;
      machine_block.used += block_summary.used;
    }
  }

  absl::StrAppend(&str, "\nArena statistics for machine:\n");
  FormatSummary(&str, machine_wide);

  return str;
}

std::string MallocStatsCb(bool backing, unsigned tid) {
  string str;

  uint64_t start = absl::GetCurrentTimeNanos();

  absl::StrAppend(&str, "\nArena statistics from thread:", tid, "\n");

  mi_heap_t* data_heap = backing ? mi_heap_get_backing() : ServerState::tlocal()->data_heap();

  BlockMap block_map;

  mi_heap_visit_blocks(data_heap, false /* visit all blocks*/, MiArenaVisit, &block_map);
  uint64_t reserved = 0, committed = 0, used = 0;
  absl::StrAppend(&str, "Count BlockSize Reserved Committed Used\n");
  for (const auto& k_v : block_map) {
    uint64_t count = k_v.second;
    absl::StrAppend(&str, count, " ", get<0>(k_v.first), " ", get<1>(k_v.first), " ",
                    get<2>(k_v.first), " ", get<3>(k_v.first), "\n");
    reserved += count * get<1>(k_v.first);
    committed += count * get<2>(k_v.first);
    used += count * get<3>(k_v.first);
  }

  absl::StrAppend(
      &str, "total reserved: ", reserved, ", committed: ", committed, ", used: ", used,
      " fragmentation waste: ",
      100.0 * (committed > used ? committed - used : 0) / std::max<size_t>(1UL, committed), "%\n");
  const uint64_t delta = (absl::GetCurrentTimeNanos() - start) / 1000;
  absl::StrAppend(&str, "--- End mimalloc statistics, took ", delta, "us ---\n");

  return str;
}

size_t MemoryUsage(PrimeIterator it, bool account_key_memory_usage) {
  size_t key_size = account_key_memory_usage ? it->first.MallocUsed() : 0;
  return key_size + it->second.MallocUsed(true);
}

}  // namespace

MemoryCmd::MemoryCmd(ServerFamily* owner, CommandContext* cmd_cntx)
    : cmd_cntx_(cmd_cntx), owner_(owner) {
}

void MemoryCmd::Run(CmdArgList args) {
  CmdArgParser parser(args);

  if (parser.Check("HELP")) {
    string_view help_arr[] = {
        "MEMORY <subcommand> [<arg> ...]. Subcommands are:",
        "STATS",
        "    Shows breakdown of memory.",
        "MALLOC-STATS",
        "    Show global malloc stats as provided by allocator libraries",
        "ARENA [SUMMARY] [BACKING] [thread-id]",
        "    Show mimalloc arena stats for a heap residing in specified thread-id. 0 by default.",
        "    If SUMMARY is specified, show stats summarized by block size",
        "        per thread summary, followed by machine wide summary",
        "        thread-id is ignored for summary output.",
        "    If BACKING is specified, show stats for the backing heap.",
        "ARENA SHOW",
        "    Prints the arena summary report for the entire process.",
        "    Requires MIMALLOC_VERBOSE=1 environment to be set. The output goes to stdout",
        "USAGE <key> [WITHOUTKEY]",
        "    Show memory usage of a key.",
        "    If WITHOUTKEY is specified, the key itself is not accounted.",
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
        "DEFRAGMENT [threshold]",
        "    Tries to free memory by moving allocations around from sparsely used memory pages.",
        "    If a threshold is supplied, it is used to determine if data will be moved from the "
        "page.",
        "    Pages used less than the threshold percentage (default 0.8) are targeted for moving "
        "out data.",
    };
    auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
    return rb->SendSimpleStrArr(help_arr);
  };

  if (parser.Check("STATS")) {
    return Stats();
  }

  if (parser.Check("USAGE")) {
    if (!parser.HasNext()) {
      return cmd_cntx_->SendError(kSyntaxErr);
    }
    string_view key = parser.Next();
    bool account_key_memory_usage = !parser.Check("WITHOUTKEY");
    return Usage(key, account_key_memory_usage);
  }

  if (parser.Check("DECOMMIT")) {
    shard_set->pool()->AwaitBrief(
        [](unsigned, auto* pb) { ServerState::tlocal()->DecommitMemory(ServerState::kAllMemory); });
    return cmd_cntx_->rb()->SendSimpleString("OK");
  }

  if (parser.Check("MALLOC-STATS")) {
    return MallocStats();
  }

  if (parser.Check("ARENA")) {
    return ArenaStats(args);
  }

  if (parser.Check("TRACK")) {
    args.remove_prefix(1);
    return Track(args);
  }

  if (parser.Check("DEFRAGMENT")) {
    static const float default_threshold =
        absl::GetFlag(FLAGS_mem_defrag_page_utilization_threshold);
    const float threshold = parser.NextOrDefault(default_threshold);

    std::vector<CollectedPageStats> results(shard_set->size());
    shard_set->pool()->AwaitFiberOnAll([threshold, &results](util::ProactorBase*) {
      if (auto* shard = EngineShard::tlocal(); shard) {
        PageUsage page_usage{CollectPageStats::YES, threshold,
                             CycleQuota{CycleQuota::kDefaultDefragQuota}};
        if (auto shard_res = shard->DoDefrag(&page_usage); shard_res.has_value()) {
          results[shard->shard_id()] = std::move(shard_res.value());
        }
      }
    });

    const CollectedPageStats merged = CollectedPageStats::Merge(std::move(results), threshold);
    auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
    return rb->SendVerbatimString(merged.ToString());
  }

  string err = UnknownSubCmd(parser.Next(), "MEMORY");
  return cmd_cntx_->SendError(err, kSyntaxErrType);
}

namespace {

struct ConnectionMemoryUsage {
  size_t connection_size = 0;
  size_t replication_connection_count = 0;
  size_t replication_connection_size = 0;
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

      size_t usage = dfly_conn->GetMemoryUsage();
      if (cntx == nullptr || cntx->master_repl_flow == nullptr) {
        mems[thread_index].connection_size += usage;
      } else {
        mems[thread_index].replication_connection_count++;
        mems[thread_index].replication_connection_size += usage;
      }
    });
  }

  ConnectionMemoryUsage mem;
  for (const auto& m : mems) {
    mem.connection_size += m.connection_size;
    mem.replication_connection_count += m.replication_connection_count;
    mem.replication_connection_size += m.replication_connection_size;
  }
  return mem;
}

}  // namespace

void MemoryCmd::Stats() {
  vector<pair<string, size_t>> stats;
  stats.reserve(25);
  ConnectionMemoryUsage connection_memory = GetConnectionMemoryUsage(owner_);

  // Connection stats, excluding replication connections
  stats.push_back({"connections.direct_bytes", connection_memory.connection_size});

  // Replication connection stats
  stats.push_back(
      {"replication.connections_count", connection_memory.replication_connection_count});
  stats.push_back({"replication.direct_bytes", connection_memory.replication_connection_size});

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
  rb->StartCollection(stats.size(), CollectionType::MAP);
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

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
  return rb->SendVerbatimString(report);
}

void MemoryCmd::ArenaStats(CmdArgList args) {
  uint32_t tid = 0;
  bool backing = false;
  bool show_arenas = false;
  bool summarize = false;

  if (args.size() >= 2) {
    string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 1));

    if (sub_cmd == "SHOW") {
      if (args.size() != 2)
        return cmd_cntx_->SendError(kSyntaxErr, kSyntaxErrType);
      show_arenas = true;
    } else {
      unsigned tid_indx = 1;

      if (sub_cmd == "SUMMARY") {
        ++tid_indx;
        summarize = true;

        if (args.size() > tid_indx) {
          sub_cmd = absl::AsciiStrToUpper(ArgS(args, tid_indx));
        }
      }

      if (sub_cmd == "BACKING") {
        ++tid_indx;
        backing = true;
      }

      if (summarize && args.size() > tid_indx) {
        return cmd_cntx_->SendError(kSyntaxErr, kSyntaxErrType);
      }

      if (args.size() > tid_indx && !absl::SimpleAtoi(ArgS(args, tid_indx), &tid)) {
        return cmd_cntx_->SendError(kInvalidIntErr);
      }
    }
  }

  if (show_arenas) {
    mi_debug_show_arenas();
    return cmd_cntx_->rb()->SendOk();
  }

  if (summarize) {
    const uint64_t start = absl::GetCurrentTimeNanos();
    const auto summaries = CollectSummaries(backing);
    string report = FormatSummaries(summaries);
    const uint64_t delta = (absl::GetCurrentTimeNanos() - start) / 1000;
    absl::StrAppend(&report, "\n--- End mimalloc statistics, took ", delta, "us ---\n");
    auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
    return rb->SendVerbatimString(report);
  }

  if (backing && tid >= shard_set->pool()->size()) {
    return cmd_cntx_->SendError(
        absl::StrCat("Thread id must be less than ", shard_set->pool()->size()));
  }

  if (!backing && tid >= shard_set->size()) {
    return cmd_cntx_->SendError(absl::StrCat("Thread id must be less than ", shard_set->size()));
  }

  const string mi_malloc_info =
      shard_set->pool()->at(tid)->AwaitBrief([=] { return MallocStatsCb(backing, tid); });

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
  return rb->SendVerbatimString(mi_malloc_info);
}

void MemoryCmd::Usage(std::string_view key, bool account_key_memory_usage) {
  ShardId sid = Shard(key, shard_set->size());
  ssize_t memory_usage = shard_set->pool()->at(sid)->AwaitBrief(
      [key, account_key_memory_usage, this, sid]() -> ssize_t {
        auto& db_slice = cmd_cntx_->server_conn_cntx()->ns->GetDbSlice(sid);
        auto [pt, exp_t] = db_slice.GetTables(cmd_cntx_->server_conn_cntx()->db_index());
        PrimeIterator it = pt->Find(key);
        if (IsValid(it)) {
          return MemoryUsage(it, account_key_memory_usage);
        } else {
          return -1;
        }
      });

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx_->rb());
  if (memory_usage < 0)
    return rb->SendNull();
  rb->SendLong(memory_usage);
}

void MemoryCmd::Track(CmdArgList args) {
#ifndef DFLY_ENABLE_MEMORY_TRACKING
  return cmd_cntx_->SendError("MEMORY TRACK must be enabled at build time.");
#endif

  CmdArgParser parser(args);

  if (parser.Check("ADD")) {
    AllocationTracker::TrackingInfo tracking_info;
    std::tie(tracking_info.lower_bound, tracking_info.upper_bound, tracking_info.sample_odds) =
        parser.Next<size_t, size_t, double>();
    if (parser.HasError()) {
      return cmd_cntx_->SendError(parser.TakeError().MakeReply());
    }

    atomic_bool error{false};
    shard_set->pool()->AwaitBrief([&](unsigned index, auto*) {
      if (!AllocationTracker::Get().Add(tracking_info)) {
        error.store(true);
      }
    });

    if (error.load()) {
      return cmd_cntx_->SendError("Unable to add tracker");
    } else {
      return cmd_cntx_->rb()->SendOk();
    }
  }

  if (parser.Check("REMOVE")) {
    auto [lower_bound, upper_bound] = parser.Next<size_t, size_t>();
    if (parser.HasError()) {
      return cmd_cntx_->SendError(parser.TakeError().MakeReply());
    }

    atomic_bool error{false};
    shard_set->pool()->AwaitBrief([&, lo = lower_bound, hi = upper_bound](unsigned index, auto*) {
      if (!AllocationTracker::Get().Remove(lo, hi)) {
        error.store(true);
      }
    });

    if (error.load()) {
      return cmd_cntx_->SendError("Unable to remove tracker");
    } else {
      return cmd_cntx_->rb()->SendOk();
    }
  }

  if (parser.Check("CLEAR")) {
    shard_set->pool()->AwaitBrief([&](unsigned index, auto*) { AllocationTracker::Get().Clear(); });
    return cmd_cntx_->rb()->SendOk();
  }

  if (parser.Check("GET")) {
    auto ranges = AllocationTracker::Get().GetRanges();
    auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx_->rb());
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
      return cmd_cntx_->SendError(parser.TakeError().MakeReply());
    }

    size_t ptr = 0;
    if (!absl::SimpleHexAtoi(ptr_str, &ptr)) {
      return cmd_cntx_->SendError("Address must be hex number");
    }

    atomic_bool found{false};
    shard_set->pool()->AwaitBrief([&](unsigned index, auto*) {
      if (mi_heap_check_owned(mi_heap_get_backing(), (void*)ptr)) {
        found.store(true);
      }
    });

    return cmd_cntx_->rb()->SendSimpleString(found.load() ? "FOUND" : "NOT-FOUND");
  }

  return cmd_cntx_->SendError(kSyntaxErrType);
}

}  // namespace dfly
