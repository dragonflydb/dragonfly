// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stats.h"

#include <algorithm>

#include "base/logging.h"

extern "C" {
#include "redis/redis_aux.h"
}

namespace dfly {

static_assert(kObjTypeMax == OBJ_TYPE_MAX, "kObjTypeMax must match OBJ_TYPE_MAX from redis_aux.h");

#define ADD(x) (x) += o.x

SlotStats& SlotStats::operator+=(const SlotStats& o) {
  static_assert(sizeof(SlotStats) == 32);

  ADD(key_count);
  ADD(total_reads);
  ADD(total_writes);
  ADD(memory_bytes);
  return *this;
}

void DbTableStats::AddTypeMemoryUsage(unsigned type, int64_t delta) {
  if (type >= memory_usage_by_type.size()) {
    LOG_FIRST_N(WARNING, 1) << "Encountered unknown type when aggregating per-type memory: "
                            << type;
    DCHECK(false) << "Unsupported type " << type;
    return;
  }
  obj_memory_usage += delta;
  memory_usage_by_type[type] += delta;
}

DbTableStats& DbTableStats::operator+=(const DbTableStats& o) {
  constexpr size_t kDbSz = sizeof(DbTableStats) - sizeof(memory_usage_by_type);
  static_assert(kDbSz == 64);

  ADD(inline_keys);
  ADD(obj_memory_usage);
  ADD(tiered_entries);
  ADD(tiered_used_bytes);
  ADD(events.hits);
  ADD(events.misses);
  ADD(events.expired_keys);
  ADD(events.evicted_keys);

  for (size_t i = 0; i < o.memory_usage_by_type.size(); ++i) {
    memory_usage_by_type[i] += o.memory_usage_by_type[i];
  }

  return *this;
}

DbStats& DbStats::operator+=(const DbStats& o) {
  constexpr size_t kDbSz = sizeof(DbStats) - sizeof(DbTableStats);
  static_assert(kDbSz == 40);

  DbTableStats::operator+=(o);

  ADD(key_count);
  ADD(expire_count);
  ADD(prime_capacity);
  ADD(expire_capacity);
  ADD(table_mem_usage);

  return *this;
}

SliceEvents& SliceEvents::operator+=(const SliceEvents& o) {
  static_assert(sizeof(SliceEvents) == 136, "You should update this function with new fields");

  ADD(evicted_keys);
  ADD(hard_evictions);
  ADD(expired_keys);
  ADD(garbage_collected);
  ADD(stash_unloaded);
  ADD(bumpups);
  ADD(garbage_checked);
  ADD(hits);
  ADD(misses);
  ADD(mutations);
  ADD(insertion_rejections);
  ADD(update);
  ADD(ram_hits);
  ADD(ram_cool_hits);
  ADD(ram_misses);
  ADD(huff_encode_total);
  ADD(huff_encode_success);
  return *this;
}

ShardStats& ShardStats::operator+=(const ShardStats& o) {
  static_assert(sizeof(ShardStats) == 104);

  ADD(defrag_attempt_total);
  ADD(defrag_realloc_total);
  ADD(defrag_task_invocation_total);
  ADD(poll_execution_total);
  ADD(tx_ooo_total);
  ADD(tx_optimistic_total);
  ADD(tx_batch_schedule_calls_total);
  ADD(tx_batch_scheduled_items_total);
  ADD(total_heartbeat_expired_keys);
  ADD(total_heartbeat_expired_bytes);
  ADD(total_heartbeat_expired_calls);
  ADD(total_migrated_keys);
  ADD(huffman_tables_built);

  return *this;
}

TieredStats& TieredStats::operator+=(const TieredStats& o) {
  static_assert(sizeof(TieredStats) == 168);

  ADD(total_stashes);
  ADD(total_fetches);
  ADD(total_cancels);
  ADD(total_deletes);
  ADD(total_defrags);
  ADD(total_uploads);
  ADD(total_heap_buf_allocs);
  ADD(total_registered_buf_allocs);

  ADD(allocated_bytes);
  ADD(capacity_bytes);

  ADD(pending_read_cnt);
  ADD(pending_stash_cnt);

  ADD(small_bins_cnt);
  ADD(small_bins_entries_cnt);
  ADD(small_bins_filling_bytes);
  ADD(small_bins_filling_entries_cnt);

  ADD(total_stash_overflows);
  ADD(cold_storage_bytes);
  ADD(total_offloading_steps);
  ADD(total_offloading_stashes);

  ADD(clients_throttled);
  ADD(total_clients_throttled);
  return *this;
}

SearchStats& SearchStats::operator+=(const SearchStats& o) {
  static_assert(sizeof(SearchStats) == 24);
  ADD(used_memory);
  ADD(num_entries);

  // Different shards could have inconsistent num_indices values during concurrent operations.
  // This can happen on concurrent index creation.
  // We use max to ensure that the total num_indices is the maximum of all shards.
  num_indices = std::max(num_indices, o.num_indices);
  return *this;
}

#undef ADD

}  // namespace dfly
