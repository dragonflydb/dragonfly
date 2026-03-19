// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "base/pmr/memory_resource.h"

namespace dfly {

class TOPKTest;

//
// TOPK: User-Facing API Data Structure
//
// This class implements the data structure required to support the public Redis
// TOPK module API (e.g., TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY).
//
// WHY WE HAVE TWO TOP-K IMPLEMENTATIONS:
// Dragonfly maintains two separate Top-K tracking structures to protect the
// performance of the database's hot path:
// 1. `TopKeys` (src/core/top_keys.h): An internal-only, hyper-optimized O(1)
//    tracker that runs on every single database command to detect hot keys.
//    It intentionally lacks a min-heap and uses standard memory allocation to
//    maximize raw speed and minimize instruction cache pollution.
// 2. `TOPK` (this file): The user-facing implementation. To comply with the Redis
//    API contract, this class MUST support instant eviction reporting (requiring an
//    O(log K) Min-Heap), arbitrary increments, and PMR allocators for strict
//    memory limit tracking and RDB snapshot serialization.
//
// Forcing the internal tracker to support Min-Heaps and PMR would severely
// degrade overall database throughput, hence the strict separation of concerns.
//
// Algorithm Deviation Note:
// While heavily inspired by the HeavyKeeper algorithm, this is NOT a strict
// implementation. The original HeavyKeeper paper requires storing a
// (fingerprint, count) pair in each cell so that decay only penalizes a specific
// item. This implementation uses a bare `uint32_t` counter grid, making it closer
// to a Count-Min Sketch coupled with a Min-Heap and a decay heuristic. This
// design safely overestimates counts (which is acceptable for Top-K bounds)
// while simplifying PMR memory layout and RDB serialization.
//
// TODO: Full PMR Integration for String Ownership
// Currently, min_heap_ and counters_ use the provided memory_resource, ensuring the
// dominant allocations are tracked. However, the std::string keys inside HeapItem
// and the absl::flat_hash_map use the default heap.
// Future optimization: Wrap flat_hash_map with a PMR allocator and upgrade
// HeapItem to use PMR_NS::string with proper uses_allocator construction.
class TOPK {
  friend class TOPKTest;

 public:
  // Initializes a Top-K tracking sketch with the specified dimensions.
  //
  // mr: Pointer to the memory resource used for allocations (MUST NOT be null).
  // k: Maximum number of most frequent items to maintain in the min-heap.
  // width: Number of counter buckets per row in the hash grid (default: 8).
  // depth: Number of independent hash functions (rows) used (default: 7).
  // decay: Probability multiplier for exponential decay (must be 0.0 to 1.0, default: 0.9).
  TOPK(PMR_NS::memory_resource* mr, uint32_t k, uint32_t width = kDefaultWidth,
       uint32_t depth = kDefaultDepth, double decay = kDefaultDecay);

  TOPK(const TOPK&) = delete;
  TOPK& operator=(const TOPK&) = delete;
  TOPK(TOPK&& other) noexcept;
  TOPK& operator=(TOPK&& other) noexcept;
  ~TOPK() = default;

  static constexpr double kDefaultDecay = 0.9;
  static constexpr uint32_t kDefaultWidth = 8;
  static constexpr uint32_t kDefaultDepth = 7;
  static constexpr double kDecayEpsilon = 1e-9;
  // Size is 4097 so that (kDecayLookupSize - 1) equals exactly 4096 (2^12).
  // This allows the C++ compiler to optimize the division and modulo operations
  // in the extrapolation hot-path into very-fast bitwise shifts & ANDs.
  static constexpr size_t kDecayLookupSize = 4097;

  // Represents an item in the Top-K list with its estimated count
  struct TopKItem {
    std::string item;
    uint32_t count;
  };

  // Inserts a single item into the Top-K sketch, incrementing its estimated frequency by 1.
  //
  // Returns: The string of the evicted item if this insertion caused a resident
  //          item to be displaced from the Top-K min-heap, or std::nullopt
  //          if no eviction occurred.
  std::optional<std::string> Add(std::string_view item);

  // Batch equivalent of Add().
  //
  // Input: A vector of items to insert into the sketch.
  // Returns: A vector of results mapping 1:1 to the input items. Each element
  //          contains either the evicted item resulting from that specific
  //          insertion, or std::nullopt.
  std::vector<std::optional<std::string>> AddMultiple(const std::vector<std::string_view>& items);

  // Increments an item's estimated frequency by a specific, arbitrary amount.
  //
  // Precondition: 'increment' must be strictly greater than 0.
  // Returns: The string of the evicted item if this operation caused a resident
  //          item to be displaced from the Top-K min-heap, or std::nullopt.
  std::optional<std::string> IncrBy(std::string_view item, uint32_t increment);

  // Batch equivalent of IncrBy().
  //
  // Input: A vector of pairs containing the item and its respective increment amount.
  // Returns: A vector of results mapping 1:1 to the input pairs. Each element
  //          contains either the evicted item resulting from that specific
  //          increment, or std::nullopt.
  std::vector<std::optional<std::string>> IncrByMultiple(
      const std::vector<std::pair<std::string_view, uint32_t>>& items);

  // Queries whether a batch of items currently resides in the Top-K min-heap.
  //
  // Input: A vector of strings (items) to check against the Top-K list.
  // Output: A vector of integers mapping 1:1 to the input vector.
  //         The i-th element of the output will be 1 if items[i] is
  //         currently a Top-K high-frequency item, and 0 otherwise.
  [[nodiscard]] std::vector<int> Query(const std::vector<std::string_view>& items) const;

  // Estimates the frequency count for a batch of items using the underlying sketch.
  //
  // Input: A vector of strings (items) to query.
  // Output: A vector of unsigned integers mapping 1:1 to the input vector,
  //         representing the lowest uncorrupted counter value for each item.
  [[nodiscard]] std::vector<uint32_t> Count(const std::vector<std::string_view>& items) const;

  // Retrieves the complete list of current Top-K high-frequency items.
  //
  // Returns: A vector of TopKItem structures (containing the key and its count),
  //          sorted in descending order by estimated frequency (highest first).
  [[nodiscard]] std::vector<TopKItem> List() const;

  // --------------------------------------------------------------------------
  // Accessors for Top-K Configuration Parameters
  // --------------------------------------------------------------------------

  // Returns the maximum capacity (K) of the Top-K min-heap.
  [[nodiscard]] uint32_t K() const {
    return k_;
  }

  // Returns the width (number of columns/buckets) of the Count-Min Sketch array.
  [[nodiscard]] uint32_t Width() const {
    return width_;
  }

  // Returns the depth (number of rows/hash functions) of the Count-Min Sketch array.
  [[nodiscard]] uint32_t Depth() const {
    return depth_;
  }

  // Returns the exponential decay probability base used by the HeavyKeeper algorithm.
  [[nodiscard]] double Decay() const {
    return decay_;
  }

  // Calculates the total heap memory dynamically allocated by this Top-K instance,
  // including sketch counters, min-heap allocations, and hash map overhead.
  //
  // Returns: Total memory usage in bytes.
  [[nodiscard]] size_t MallocUsed() const;

  // --------------------------------------------------------------------------
  // Serialization and Persistence
  // --------------------------------------------------------------------------

  // Pod-like structure to hold the exact internal state of the Top-K instance.
  struct SerializedData {
    uint32_t k;
    uint32_t width;
    uint32_t depth;
    double decay;
    std::vector<TopKItem> heap_items;
    std::vector<uint32_t> counters;
  };

  // Extracts the current structural state of the sketch for RDB persistence.
  [[nodiscard]] SerializedData Serialize() const;

  // Reconstructs the internal state of the sketch from a previously serialized dataset.
  void Deserialize(const SerializedData& data);

 private:
  struct HeapItem {
    std::string key;
    uint32_t count;
    size_t hash;  // Pre-computed hash

    // Min heap comparator
    bool operator>(const HeapItem& other) const {
      return count > other.count;
    }
  };

  // Hash function for bucket selection in row
  [[nodiscard]] uint64_t Hash(std::string_view item, uint32_t row) const;

  // Exponential decay logic
  [[nodiscard]] bool ShouldDecay(uint32_t current_count) const;

  // Get the minimum count for an item across all hash table rows
  [[nodiscard]] uint32_t GetMinCount(std::string_view item) const;

  // Updates the min-heap with the new count for the given item.
  // Returns the evicted item's key if the heap is at capacity and a new item displaces an existing
  // one. Otherwise, returns std::nullopt.
  std::optional<std::string> UpdateHeap(std::string_view item, uint32_t new_count);

  // Check if an item is in the Top-K heap
  [[nodiscard]] bool IsInHeap(std::string_view item) const {
    return item_to_hash_.contains(item);
  }

  // Hashes the item for a specific row and calculates its flattened 1D index
  // within the counters_ array. Maps the 2D Count-Min Sketch grid (depth x width)
  // into a single contiguous block of memory for better CPU cache locality.
  size_t GetCounterIndex(std::string_view item, uint32_t row) const;

  // Shared increment logic
  std::optional<std::string> IncrementInternal(std::string_view item, uint32_t increment);

  // Compute decay probability using lookup table or extrapolation
  double ComputeDecayProbability(uint32_t count) const;

  // Heap maintenance functions
  // O(log k) ops
  void HeapifyUp(size_t index);
  void HeapifyDown(size_t index);

  uint32_t k_;      // Number of top items to track
  uint32_t width_;  // Hash table width (buckets per row)
  uint32_t depth_;  // Hash table depth (number of rows)
  double decay_;    // Decay constant (0.0-1.0, typically 0.9)

  // Pointer to the active decay lookup table. For the default decay (0.9), this points to
  // a process-wide shared static table (32KB, allocated once). For custom (non-default) decay
  // values, it points to custom_decay_table_ below. This pattern can help to avoid embedding a 32KB
  // array in every TOPK object.
  // Assumption: >99% of TOPK instances will use the default decay, so
  // this optimization can significantly reduce memory usage and improve startup performance by
  // avoiding the need to build a custom table for each instance.
  const std::array<double, kDecayLookupSize>* decay_lookup_ = nullptr;

  // Heap-allocated table for non-default decay values. Null for the common case (decay=0.9).
  std::unique_ptr<std::array<double, kDecayLookupSize>> custom_decay_table_;

  // HeavyKeeper data structures
  // Hash table: width × depth matrix of counters
  std::vector<uint32_t, PMR_NS::polymorphic_allocator<uint32_t>> counters_;

  // Min heap: vector of top-K items maintained as a min heap
  std::vector<HeapItem, PMR_NS::polymorphic_allocator<HeapItem>> min_heap_;

  // O(1) fast-path membership index for the min-heap.
  // Maps items currently in the Top-K list to their pre-computed 64-bit hashes.
  // This serves two critical performance goals:
  // 1. Prevents O(K) linear scans just to check if an item is currently in the heap.
  // 2. Caches the hash to allow very fast integer comparisons instead of
  //    slow string comparisons when locating the item inside the heap array.
  absl::flat_hash_map<std::string, size_t> item_to_hash_;
};

}  // namespace dfly
