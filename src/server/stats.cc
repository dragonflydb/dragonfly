// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stats.h"

#include <algorithm>

namespace dfly {

#define ADD(x) (x) += o.x

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
