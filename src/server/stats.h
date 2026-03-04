// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

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
