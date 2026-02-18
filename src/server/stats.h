// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace dfly {

constexpr unsigned kObjTypeMax = 17U;

struct SlotStats {
  uint64_t key_count = 0;
  uint64_t total_reads = 0;
  uint64_t total_writes = 0;
  uint64_t memory_bytes = 0;
  SlotStats& operator+=(const SlotStats& o);
};

struct DbTableStats {
  // Number of inline keys.
  uint64_t inline_keys = 0;

  // Object memory usage besides hash-table capacity.
  // Applies for any non-inline objects.
  size_t obj_memory_usage = 0;

  size_t tiered_entries = 0;
  size_t tiered_used_bytes = 0;

  struct {
    // Per-database hits/misses on keys
    size_t hits = 0;
    size_t misses = 0;

    // Per-database expired/evicted keys
    size_t expired_keys = 0;
    size_t evicted_keys = 0;
  } events;

  std::array<size_t, kObjTypeMax> memory_usage_by_type = {};

  // Mostly used internally, exposed for tiered storage.
  void AddTypeMemoryUsage(unsigned type, int64_t delta);

  DbTableStats& operator+=(const DbTableStats& o);
};

struct DbStats : public DbTableStats {
  // number of active keys.
  size_t key_count = 0;

  // number of keys that have expiry deadline.
  size_t expire_count = 0;

  // total number of slots in prime dictionary (key capacity).
  size_t prime_capacity = 0;

  // total number of slots in prime dictionary (key capacity).
  size_t expire_capacity = 0;

  // Memory used by dictionaries.
  size_t table_mem_usage = 0;

  // We override additional DbStats fields explicitly in DbSlice::GetStats().
  using DbTableStats::operator=;

  DbStats& operator+=(const DbStats& o);
};

struct SliceEvents {
  // Number of eviction events.
  size_t evicted_keys = 0;

  // evictions that were performed when we have a negative memory budget.
  size_t hard_evictions = 0;
  size_t expired_keys = 0;
  size_t garbage_checked = 0;
  size_t garbage_collected = 0;
  size_t stash_unloaded = 0;
  size_t bumpups = 0;  // how many bump-upds we did.

  // hits/misses on keys
  size_t hits = 0;
  size_t misses = 0;
  size_t mutations = 0;

  // ram hit/miss when tiering is enabled
  size_t ram_hits = 0;
  size_t ram_cool_hits = 0;
  size_t ram_misses = 0;

  // how many insertions were rejected due to OOM.
  size_t insertion_rejections = 0;

  // how many updates and insertions of keys between snapshot intervals
  size_t update = 0;

  uint64_t huff_encode_total = 0, huff_encode_success = 0;

  SliceEvents& operator+=(const SliceEvents& o);
};

struct ShardStats {
  uint64_t defrag_attempt_total = 0;
  uint64_t defrag_realloc_total = 0;
  uint64_t defrag_task_invocation_total = 0;
  uint64_t poll_execution_total = 0;

  // number of optimistic executions - that were run as part of the scheduling.
  uint64_t tx_optimistic_total = 0;
  uint64_t tx_ooo_total = 0;

  // Number of ScheduleBatchInShard calls.
  uint64_t tx_batch_schedule_calls_total = 0;

  // Number of transactions scheduled via ScheduleBatchInShard.
  uint64_t tx_batch_scheduled_items_total = 0;

  uint64_t total_heartbeat_expired_keys = 0;
  uint64_t total_heartbeat_expired_bytes = 0;
  uint64_t total_heartbeat_expired_calls = 0;

  // cluster stats
  uint64_t total_migrated_keys = 0;

  // how many huffman tables were built successfully in the background
  uint32_t huffman_tables_built = 0;

  ShardStats& operator+=(const ShardStats&);
};

struct TieredStats {
  uint64_t total_stashes = 0;
  uint64_t total_fetches = 0;
  uint64_t total_cancels = 0;
  uint64_t total_deletes = 0;
  uint64_t total_defrags = 0;
  uint64_t total_uploads = 0;
  uint64_t total_registered_buf_allocs = 0;
  uint64_t total_heap_buf_allocs = 0;

  // How many times the system did not perform Stash call due to overloaded disk write queue
  // (disjoint with total_stashes).
  uint64_t total_stash_overflows = 0;
  uint64_t total_offloading_steps = 0;
  uint64_t total_offloading_stashes = 0;

  size_t allocated_bytes = 0;
  size_t capacity_bytes = 0;

  uint32_t pending_read_cnt = 0;
  uint32_t pending_stash_cnt = 0;

  uint64_t small_bins_cnt = 0;
  uint64_t small_bins_entries_cnt = 0;
  size_t small_bins_filling_bytes = 0;
  size_t small_bins_filling_entries_cnt = 0;
  size_t cold_storage_bytes = 0;

  uint64_t clients_throttled = 0;        // current number of throttled clients
  uint64_t total_clients_throttled = 0;  // total number of throttles

  TieredStats& operator+=(const TieredStats&);
};

struct SearchStats {
  size_t used_memory = 0;
  size_t num_indices = 0;
  size_t num_entries = 0;

  SearchStats& operator+=(const SearchStats&);
};

}  // namespace dfly
