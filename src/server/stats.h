// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

struct TieredStats {
  // Successful stash operations (values written to disk).
  uint64_t total_stashes = 0;

  // Read operations fetching values back from disk.
  uint64_t total_fetches = 0;

  // Stash operations cancelled before completion (e.g. entry deleted while stash pending).
  uint64_t total_cancels = 0;

  // Deletion operations on offloaded entries.
  uint64_t total_deletes = 0;

  // Defragmentation operations (small bins read and consolidated back to memory).
  uint64_t total_defrags = 0;

  // Values restored from disk to memory (e.g. after modification).
  uint64_t total_uploads = 0;

  // Registered buffer allocations for disk I/O (memory regions registered with io_uring for
  // zero-copy I/O).
  uint64_t total_registered_buf_allocs = 0;

  // Heap buffer allocations for disk I/O (fallback when registered buffers are unavailable).
  uint64_t total_heap_buf_allocs = 0;

  // How many times the system did not perform Stash call due to overloaded disk write queue
  // (disjoint with total_stashes).
  uint64_t total_stash_overflows = 0;

  // Iterations through the primary table during background offloading scans.
  uint64_t total_offloading_steps = 0;

  // Values stashed during background offloading scans (subset of total_stashes).
  uint64_t total_offloading_stashes = 0;

  // Disk space currently in use (tracked by ExternalAllocator).
  size_t allocated_bytes = 0;

  // Total capacity of the disk backing file.
  size_t capacity_bytes = 0;

  // In-flight read operations.
  uint32_t pending_read_cnt = 0;

  // In-flight stash (write) operations.
  uint32_t pending_stash_cnt = 0;

  // Small bins pack multiple small values into a single disk page.
  // Number of fully written bins on disk.
  uint64_t small_bins_cnt = 0;

  // Total entries across all fully written bins on disk.
  uint64_t small_bins_entries_cnt = 0;

  // Bytes accumulated in the in-memory bin currently being filled.
  size_t small_bins_filling_bytes = 0;

  // Entry count in the in-memory bin currently being filled.
  size_t small_bins_filling_entries_cnt = 0;

  // Memory used by the cooling layer (recently stashed values held before full eviction).
  size_t cold_storage_bytes = 0;

  // Current number of throttled clients.
  uint64_t clients_throttled = 0;

  // Total number of client throttle events.
  uint64_t total_clients_throttled = 0;

  TieredStats& operator+=(const TieredStats&);
};

struct SearchStats {
  size_t used_memory = 0;
  size_t num_indices = 0;
  size_t num_entries = 0;

  SearchStats& operator+=(const SearchStats&);
};

}  // namespace dfly
