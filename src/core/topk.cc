// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/topk.h"

#include <xxhash.h>

#include <algorithm>
#include <cmath>
#include <random>

#include "base/logging.h"

namespace dfly {

TOPK::TOPK(const uint32_t k, const uint32_t width, const uint32_t depth, const double decay,
           PMR_NS::memory_resource* mr)
    : k_(k),
      width_(width),
      depth_(depth),
      decay_(decay),
      counters_(static_cast<size_t>(width) * depth, 0, PMR_NS::polymorphic_allocator<uint32_t>(mr)),
      min_heap_(PMR_NS::polymorphic_allocator<HeapItem>(mr)) {
  DCHECK_GT(k_, 0u);
  DCHECK_GT(width_, 0u);
  DCHECK_GT(depth_, 0u);
  DCHECK_GE(decay_, 0.0);
  DCHECK_LE(decay_, 1.0);
  min_heap_.reserve(k + 1);

  // Pre-compute decay lookup table for i = 0 to 255 to avoid repeated std::pow() calls
  for (size_t i = 0; i < kDecayLookupSize; ++i) {
    decay_lookup_[i] = std::pow(decay_, static_cast<double>(i));
  }
}

TOPK::TOPK(TOPK&& other) noexcept
    : k_(other.k_),
      width_(other.width_),
      depth_(other.depth_),
      decay_(other.decay_),
      decay_lookup_(other.decay_lookup_),
      counters_(std::move(other.counters_)),
      min_heap_(std::move(other.min_heap_)),
      item_to_hash_(std::move(other.item_to_hash_)) {
}

TOPK& TOPK::operator=(TOPK&& other) noexcept {
  if (this != &other) {
    k_ = other.k_;
    width_ = other.width_;
    depth_ = other.depth_;
    decay_ = other.decay_;
    decay_lookup_ = other.decay_lookup_;
    counters_ = std::move(other.counters_);
    min_heap_ = std::move(other.min_heap_);
    item_to_hash_ = std::move(other.item_to_hash_);
  }
  return *this;
}

uint64_t TOPK::Hash(std::string_view item, uint32_t row) const {
  return XXH3_64bits_withSeed(item.data(), item.size(), row) % width_;
}

double TOPK::ComputeDecayProbability(uint32_t count) const {
  if (count < kDecayLookupSize) {
    // Direct lookup for small counts
    return decay_lookup_[count];
  }

  // When decay is not in table range [0..N] use extrapolation:
  // decay^count ≈ (decay^(N-1))^(count/(N-1)) × decay^(count%(N-1))
  uint32_t quotient = count / (kDecayLookupSize - 1);
  uint32_t remainder = count % (kDecayLookupSize - 1);

  // Compute (decay^(N-1))^quotient × decay^remainder
  double base = decay_lookup_[kDecayLookupSize - 1];
  double result = std::pow(base, static_cast<double>(quotient)) * decay_lookup_[remainder];

  return result;
}

bool TOPK::ShouldDecay(uint32_t current_count) const {
  if (current_count == 0)
    return false;

  // Exponential decay probability: decay^count
  // Use thread-local random generator for thread safety
  thread_local std::mt19937 gen(std::random_device{}());
  thread_local std::uniform_real_distribution<double> dis(0.0, 1.0);

  double prob = ComputeDecayProbability(current_count);
  return dis(gen) < prob;
}

void TOPK::HeapifyUp(size_t index) {
  // Restore min-heap property upward from index
  // Used when an item's count increases
  while (index > 0) {
    size_t parent = (index - 1) / 2;
    if (min_heap_[parent].count <= min_heap_[index].count) {
      break;  // Heap property satisfied
    }

    // Swap with parent
    std::swap(min_heap_[parent], min_heap_[index]);

    index = parent;
  }
}

void TOPK::HeapifyDown(size_t index) {
  // Restore min-heap property downward from index
  // Used when an item's count decreases or after removal
  size_t size = min_heap_.size();

  while (true) {
    size_t left = 2 * index + 1;
    size_t right = 2 * index + 2;
    size_t smallest = index;

    if (left < size && min_heap_[left].count < min_heap_[smallest].count) {
      smallest = left;
    }
    if (right < size && min_heap_[right].count < min_heap_[smallest].count) {
      smallest = right;
    }

    if (smallest == index) {
      break;  // Heap property satisfied
    }

    // Swap with smallest child
    std::swap(min_heap_[smallest], min_heap_[index]);

    index = smallest;
  }
}

uint32_t TOPK::GetMinCount(std::string_view item) const {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();

  for (uint32_t row = 0; row < depth_; ++row) {
    uint64_t bucket = Hash(item, row);
    size_t idx = static_cast<size_t>(row) * width_ + bucket;
    min_count = std::min(min_count, counters_[idx]);
  }

  return min_count;
}

bool TOPK::IsInHeap(std::string_view item) const {
  return item_to_hash_.contains(std::string(item));
}

std::vector<std::string> TOPK::IncrementInternal(std::string_view item, uint32_t increment) {
  std::vector<std::string> expelled;

  // Update counters using HeavyKeeper logic
  for (uint32_t row = 0; row < depth_; ++row) {
    uint64_t bucket = Hash(item, row);
    size_t idx = static_cast<size_t>(row) * width_ + bucket;

    // Apply exponential decay with probability for existing counts
    if (counters_[idx] > 0 && ShouldDecay(counters_[idx])) {
      counters_[idx] = std::max(1u, counters_[idx] - 1);
    } else {
      // Increment by specified amount
      counters_[idx] = std::min(counters_[idx] + increment, std::numeric_limits<uint32_t>::max());
    }
  }

  // Get the minimum count across all hash functions
  uint32_t min_count = GetMinCount(item);

  // Update heap
  UpdateHeap(item, min_count);

  // Check if we need to expel an item
  if (min_heap_.size() > k_) {
    std::string expelled_item = TryExpelMin();
    if (!expelled_item.empty()) {
      expelled.push_back(std::move(expelled_item));
    }
  }

  return expelled;
}

std::vector<std::string> TOPK::Add(std::string_view item) {
  return IncrementInternal(item, 1);
}

std::vector<std::optional<std::string>> TOPK::AddMultiple(
    const std::vector<std::string_view>& items) {
  std::vector<std::optional<std::string>> result;
  result.reserve(items.size());

  for (const auto& item : items) {
    auto expelled = Add(item);
    if (expelled.empty()) {
      result.emplace_back(std::nullopt);
    } else {
      result.emplace_back(expelled[0]);
    }
  }

  return result;
}

std::vector<std::string> TOPK::IncrBy(std::string_view item, uint32_t increment) {
  if (increment < 1) {
    // Invalid increment, return empty
    return {};
  }
  return IncrementInternal(item, increment);
}

std::vector<std::optional<std::string>> TOPK::IncrByMultiple(
    const std::vector<std::pair<std::string_view, uint32_t>>& items) {
  std::vector<std::optional<std::string>> result;
  result.reserve(items.size());

  for (const auto& [item, incr] : items) {
    auto expelled = IncrBy(item, incr);
    if (expelled.empty()) {
      result.emplace_back(std::nullopt);
    } else {
      result.emplace_back(expelled[0]);
    }
  }

  return result;
}

std::vector<int> TOPK::Query(const std::vector<std::string_view>& items) const {
  std::vector<int> result;
  result.reserve(items.size());

  for (const auto& item : items) {
    result.push_back(IsInHeap(item) ? 1 : 0);
  }

  return result;
}

std::vector<uint32_t> TOPK::Count(const std::vector<std::string_view>& items) const {
  std::vector<uint32_t> result;
  result.reserve(items.size());

  for (const auto& item : items) {
    result.push_back(GetMinCount(item));
  }

  return result;
}

std::vector<TOPK::TopKItem> TOPK::List() const {
  std::vector<TopKItem> result;
  result.reserve(min_heap_.size());

  // Copy heap items
  for (const auto& heap_item : min_heap_) {
    result.push_back({heap_item.key, heap_item.count});
  }

  // Sort by count (descending) for output
  std::sort(result.begin(), result.end(),
            [](const TopKItem& a, const TopKItem& b) { return a.count > b.count; });

  return result;
}

void TOPK::UpdateHeap(std::string_view item, uint32_t new_count) {
  std::string item_str(item);
  size_t item_hash = XXH3_64bits(item.data(), item.size());

  // First check if item is in top-k using O(1) hash lookup
  auto it = item_to_hash_.find(item_str);
  if (it != item_to_hash_.end()) {
    // Item is in top-k, find its position in heap with O(k) linear search
    // This is acceptable since we only do it for items we KNOW are in the heap
    for (size_t i = 0; i < min_heap_.size(); ++i) {
      if (min_heap_[i].key == item_str) {
        // Update count and restore heap property with O(log k) heapify
        uint32_t old_count = min_heap_[i].count;
        min_heap_[i].count = new_count;

        // Restore heap property based on count change
        if (new_count > old_count) {
          HeapifyUp(i);  // Count increased, may need to move up
        } else if (new_count < old_count) {
          HeapifyDown(i);  // Count decreased, may need to move down
        }
        // If counts equal, no heapify needed
        return;
      }
    }
  }

  // Item not in heap - add if heap not full or count > min
  if (min_heap_.size() < k_) {
    // Heap not full, add directly
    size_t new_idx = min_heap_.size();
    min_heap_.push_back({item_str, new_count, item_hash});
    item_to_hash_[item_str] = item_hash;
    HeapifyUp(new_idx);  // Restore heap property
  } else if (new_count > min_heap_.front().count) {
    // Count is higher than minimum in heap, replace minimum
    std::string old_key = min_heap_[0].key;
    item_to_hash_.erase(old_key);

    min_heap_[0] = {item_str, new_count, item_hash};
    item_to_hash_[item_str] = item_hash;
    HeapifyDown(0);  // Restore heap property from root
  }
}

std::string TOPK::TryExpelMin() {
  if (min_heap_.empty() || min_heap_.size() <= k_) {
    return "";
  }

  // Remove minimum item (at root)
  std::string min_key = min_heap_[0].key;
  item_to_hash_.erase(min_key);

  // Move last item to root and heapify down
  if (min_heap_.size() > 1) {
    min_heap_[0] = min_heap_.back();
  }
  min_heap_.pop_back();

  // Restore heap property from root (only if we moved an item)
  if (!min_heap_.empty()) {
    HeapifyDown(0);
  }

  return min_key;
}

size_t TOPK::MallocUsed() const {
  size_t size = 0;

  // Counter array
  size += counters_.capacity() * sizeof(uint32_t);

  // Heap items - calculate actual string sizes
  size += min_heap_.capacity() * sizeof(HeapItem);
  for (const auto& item : min_heap_) {
    size += item.key.capacity();
  }

  // flat_hash_map overhead
  size += item_to_hash_.bucket_count();  // Account for control bytes
  size +=
      item_to_hash_.bucket_count() * sizeof(std::pair<const std::string, size_t>);  // Pair storage
  for (const auto& [key, hash] : item_to_hash_) {
    size += key.capacity();
  }

  return size;
}

TOPK::SerializedData TOPK::Serialize() const {
  SerializedData data;
  data.k = k_;
  data.width = width_;
  data.depth = depth_;
  data.decay = decay_;

  // Serialize heap items
  for (const auto& heap_item : min_heap_) {
    data.heap_items.push_back({heap_item.key, heap_item.count});
  }

  // Serialize counter array
  data.counters.assign(counters_.begin(), counters_.end());

  return data;
}

void TOPK::Deserialize(const SerializedData& data) {
  // Clear existing data
  min_heap_.clear();
  item_to_hash_.clear();

  // Restore counters
  counters_.assign(data.counters.begin(), data.counters.end());

  // Restore heap
  for (const auto& item : data.heap_items) {
    size_t item_hash = XXH3_64bits(item.item.data(), item.item.size());
    min_heap_.push_back({item.item, item.count, item_hash});
    item_to_hash_[item.item] = item_hash;
  }

  // Rebuild heap property
  std::make_heap(min_heap_.begin(), min_heap_.end(), std::greater<HeapItem>());
}

}  // namespace dfly
