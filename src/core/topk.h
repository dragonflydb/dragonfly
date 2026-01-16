// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "base/pmr/memory_resource.h"

namespace dfly {

/// Top-K implementation using the HeavyKeeper algorithm.
/// Compatible with Redis TOPK commands from RedisBloom module.
class TOPK {
 public:
  // Create a Top-K sketch with specified parameters.
  // k: number of top items to track
  // width: number of counters per row (hash table buckets)
  // depth: number of rows (hash functions)
  // decay: probability decay constant for exponential decay (0.0-1.0)
  TOPK(uint32_t k, uint32_t width, uint32_t depth, double decay,
       PMR_NS::memory_resource* mr = nullptr);

  TOPK(const TOPK&) = delete;
  TOPK& operator=(const TOPK&) = delete;

  TOPK(TOPK&& other) noexcept;
  TOPK& operator=(TOPK&& other) noexcept;

  ~TOPK() = default;

  // Represents an item in the Top-K list with its estimated count
  struct TopKItem {
    std::string item;
    uint32_t count;
  };

  // Add an item to the sketch.
  // Returns the expelled item if one was removed from Top-K, or empty vector.
  std::vector<std::string> Add(std::string_view item);

  // Add multiple items to the sketch.
  // Returns a vector where each element is either an expelled item or empty string.
  std::vector<std::optional<std::string>> AddMultiple(const std::vector<std::string_view>& items);

  // Increment an item's count by the specified amount.
  // Returns the expelled item if one was removed, or empty vector.
  // increment must be > 0.
  std::vector<std::string> IncrBy(std::string_view item, uint32_t increment);

  // Increment multiple items by specified amounts.
  // Returns a vector where each element is either an expelled item or empty string.
  std::vector<std::optional<std::string>> IncrByMultiple(
      const std::vector<std::pair<std::string_view, uint32_t>>& items);

  // Query if items are in the Top-K list.
  // Returns 1 if item is in Top-K, 0 otherwise.
  [[nodiscard]] std::vector<int> Query(const std::vector<std::string_view>& items) const;

  // Get estimated counts for items.
  [[nodiscard]] std::vector<uint32_t> Count(const std::vector<std::string_view>& items) const;

  // Get the Top-K items list, optionally with counts.
  // Items are sorted by estimated frequency (highest first).
  [[nodiscard]] std::vector<TopKItem> List() const;

  // Accessors for Top-K parameters
  [[nodiscard]] uint32_t K() const {
    return k_;
  }

  [[nodiscard]] uint32_t Width() const {
    return width_;
  }

  [[nodiscard]] uint32_t Depth() const {
    return depth_;
  }

  [[nodiscard]] double Decay() const {
    return decay_;
  }

  // Memory usage in bytes
  [[nodiscard]] size_t MallocUsed() const;

  // Serialization support for RDB persistence
  struct SerializedData {
    uint32_t k;
    uint32_t width;
    uint32_t depth;
    double decay;
    std::vector<TopKItem> heap_items;
    std::vector<uint32_t> counters;
  };
  [[nodiscard]] SerializedData Serialize() const;
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

  // Update the min heap after count changes
  void UpdateHeap(std::string_view item, uint32_t new_count);

  // Try to expel the minimum item from heap if it's full
  // Returns the expelled item or empty string
  std::string TryExpelMin();

  // Check if an item is in the Top-K heap
  [[nodiscard]] bool IsInHeap(std::string_view item) const;

  // Shared increment logic
  std::vector<std::string> IncrementInternal(std::string_view item, uint32_t increment);

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

  // Pre-calculated decay lookup table
  static constexpr size_t kDecayLookupSize = 256;
  std::array<double, kDecayLookupSize> decay_lookup_{};

  // HeavyKeeper data structures
  // Hash table: width × depth matrix of counters
  std::vector<uint32_t, PMR_NS::polymorphic_allocator<uint32_t>> counters_;

  // Min heap: vector of top-K items maintained as a min heap
  std::vector<HeapItem, PMR_NS::polymorphic_allocator<HeapItem>> min_heap_;

  // Fast lookup: item name -> hash for O(1) "is in top-k" queries
  absl::flat_hash_map<std::string, size_t> item_to_hash_;
};

}  // namespace dfly
