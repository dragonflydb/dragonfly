// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/topk.h"

#include <xxhash.h>

#include <algorithm>
#include <cmath>
#include <utility>

#include "absl/random/distributions.h"
#include "base/logging.h"
#include "base/random.h"

namespace dfly {

namespace {

const std::array<double, TOPK::kDecayLookupSize>& GetDefaultDecayTable() {
  static const auto table = [] {
    std::array<double, TOPK::kDecayLookupSize> t{};
    for (size_t i = 0; i < TOPK::kDecayLookupSize; ++i) {
      t[i] = std::pow(TOPK::kDefaultDecay, static_cast<double>(i));
    }
    return t;
  }();
  return table;
}

}  // namespace

TOPK::TOPK(PMR_NS::memory_resource* mr, uint32_t k, uint32_t width, uint32_t depth, double decay)
    : k_(k),
      width_(width),
      depth_(depth),
      decay_(decay),
      counters_(static_cast<size_t>(width) * depth, 0, PMR_NS::polymorphic_allocator<uint32_t>(mr)),
      min_heap_(PMR_NS::polymorphic_allocator<HeapItem>(mr)) {
  DCHECK(mr != nullptr);
  DCHECK_GT(k_, 0u);
  DCHECK_GT(width_, 0u);
  DCHECK_GT(depth_, 0u);
  DCHECK_GE(decay_, 0.0);
  DCHECK_LE(decay_, 1.0);
  min_heap_.reserve(k_);

  if (std::abs(decay_ - TOPK::kDefaultDecay) < TOPK::kDecayEpsilon) {
    // default decay value: use shared static table to save memory and initialization time
    decay_lookup_ = &GetDefaultDecayTable();
  } else {
    // custom decay value: build a dedicated table for this instance
    custom_decay_table_ = std::make_unique<std::array<double, TOPK::kDecayLookupSize>>();
    for (size_t i = 0; i < TOPK::kDecayLookupSize; ++i) {
      (*custom_decay_table_)[i] = std::pow(decay_, static_cast<double>(i));
    }
    decay_lookup_ = custom_decay_table_.get();
  }
}

TOPK::TOPK(TOPK&& other) noexcept
    : k_(std::exchange(other.k_, 0)),
      width_(std::exchange(other.width_, 0)),
      depth_(std::exchange(other.depth_, 0)),
      decay_(std::exchange(other.decay_, 0.0)),
      decay_lookup_(std::exchange(other.decay_lookup_, nullptr)),
      custom_decay_table_(std::move(other.custom_decay_table_)),
      counters_(std::move(other.counters_)),
      min_heap_(std::move(other.min_heap_)),
      item_to_hash_(std::move(other.item_to_hash_)) {
}

TOPK& TOPK::operator=(TOPK&& other) noexcept {
  if (this != &other) {
    k_ = std::exchange(other.k_, 0);
    width_ = std::exchange(other.width_, 0);
    depth_ = std::exchange(other.depth_, 0);
    decay_ = std::exchange(other.decay_, 0.0);
    decay_lookup_ = std::exchange(other.decay_lookup_, nullptr);
    custom_decay_table_ = std::move(other.custom_decay_table_);
    counters_ = std::move(other.counters_);
    min_heap_ = std::move(other.min_heap_);
    item_to_hash_ = std::move(other.item_to_hash_);
  }
  return *this;
}

uint64_t TOPK::Hash(std::string_view item, uint32_t row) const {
  auto full_hash = XXH3_64bits_withSeed(item.data(), item.size(), row);

  // Lemire's Fast Range Reduction avoids the expensive CPU integer division penalty of the modulo
  // (%) operator. The main principle: multiplication is much faster than division, so we multiply
  // a 32-bit slice of the hash by the width, and then shift right by 32 bits to get the bucket
  // index. See: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
  uint32_t hash32 = static_cast<uint32_t>(full_hash);

  uint64_t bucket = (static_cast<uint64_t>(hash32) * width_) >> 32;
  DCHECK_LT(bucket, width_);
  return bucket;
}

double TOPK::ComputeDecayProbability(uint32_t count) const {
  DCHECK(decay_lookup_);
  DCHECK_GT(count, 0u);
  const auto& table = *decay_lookup_;
  if (count < kDecayLookupSize) {
    return table[count];
  }

  // If the probability is already less than kDecayEpsilon, the chance of decay is
  // statistically zero (see ShouldDecay). Skip the expensive std::pow extrapolation entirely.
  if (table[TOPK::kDecayLookupSize - 1] < TOPK::kDecayEpsilon) {
    return 0.0;
  }

  // Extrapolate probabilities for counts that exceed our lookup table's max index.
  // Let M = the maximum table index (kDecayLookupSize - 1)
  // Let Q = the quotient (count / M)
  // Let R = the remainder (count % M)
  //
  // Using the Laws of Exponents, we break down decay^count:
  // decay^count = decay^((Q * M) + R) = (decay^M)^Q * decay^R
  //
  // This translates directly to reusing our cached table:
  // std::pow(table[M], Q) * table[R]
  uint32_t quotient = count / (TOPK::kDecayLookupSize - 1);
  uint32_t remainder = count % (TOPK::kDecayLookupSize - 1);
  double base = table[TOPK::kDecayLookupSize - 1];
  return std::pow(base, static_cast<double>(quotient)) * table[remainder];
}

bool TOPK::ShouldDecay(uint32_t current_count) const {
  if (current_count == 0)
    return false;

  // Exponential decay probability: decay^count
  thread_local base::Xoroshiro128p bitgen;
  double prob = ComputeDecayProbability(current_count);
  return absl::Uniform(bitgen, 0.0, 1.0) < prob;
}

void TOPK::HeapifyUp(size_t index) {
  DCHECK_LT(index, min_heap_.size());
  // Restores the min-heap property by shifting the element at 'index' upward.
  // Triggered in two cases:
  // 1. Initial insertion: A new item is appended to the array and needs to bubble up.
  // 2. Count decrease: An existing item's count drops (becomes smaller), floating higher.
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
  DCHECK_LT(index, min_heap_.size());
  // Restores the min-heap property by shifting the element at 'index' downward.
  // Triggered in two cases:
  // 1. Root replacement/removal: The minimum item is evicted/replaced and the new root must sink.
  // 2. Count increase: An existing item's count grows (becomes heavier), sinking lower.
  size_t size = min_heap_.size();

  while (true) {
    size_t left = (2 * index) + 1;
    size_t right = (2 * index) + 2;
    size_t smallest = index;

    if ((left < size) && (min_heap_[left].count) < (min_heap_[smallest].count)) {
      smallest = left;
    }
    if ((right < size) && (min_heap_[right].count) < (min_heap_[smallest].count)) {
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

size_t TOPK::GetCounterIndex(std::string_view item, uint32_t row) const {
  DCHECK_LT(row, depth_);
  // Note:
  // - bucket is mathematically guaranteed to be in the range [0, width_ - 1]
  // - The max possible idx is depth * width - 1, which is within the bounds of our counters_
  // vector
  uint64_t bucket = Hash(item, row);
  size_t idx = static_cast<size_t>(row) * width_ + bucket;
  DCHECK_LT(idx, counters_.size());
  return idx;
}

uint32_t TOPK::GetMinCount(std::string_view item) const {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();

  for (uint32_t row = 0; row < depth_; ++row) {
    size_t idx = GetCounterIndex(item, row);
    min_count = std::min(min_count, counters_[idx]);
  }

  return min_count;
}

std::optional<std::string> TOPK::IncrementInternal(std::string_view item, uint32_t increment) {
  // Update counters using HeavyKeeper logic
  for (uint32_t row = 0; row < depth_; ++row) {
    size_t idx = GetCounterIndex(item, row);

    // HeavyKeeper: decay and increment are mutually exclusive.
    // - With probability decay^count, the counter is decremented (colliding items suppress each
    // other).
    // - Otherwise, the counter is incremented for the item being added.
    if ((counters_[idx] > 0) && ShouldDecay(counters_[idx])) {
      --counters_[idx];
    } else {
      counters_[idx] = static_cast<uint32_t>(
          std::min(static_cast<uint64_t>(counters_[idx]) + increment,
                   static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())));
    }
  }

  // Count-Min Sketch property: The minimum counter across all rows is the
  // most accurate, as it has suffered the fewest hash collisions.
  uint32_t min_count = GetMinCount(item);

  return UpdateHeap(item, min_count);
}

std::optional<std::string> TOPK::Add(std::string_view item) {
  return IncrementInternal(item, 1);
}

std::vector<std::optional<std::string>> TOPK::AddMultiple(
    const std::vector<std::string_view>& items) {
  std::vector<std::optional<std::string>> result;
  result.reserve(items.size());

  for (const auto& item : items) {
    result.push_back(Add(item));
  }

  return result;
}

std::optional<std::string> TOPK::IncrBy(std::string_view item, uint32_t increment) {
  if (increment < 1) {
    return std::nullopt;
  }
  return IncrementInternal(item, increment);
}

std::vector<std::optional<std::string>> TOPK::IncrByMultiple(
    const std::vector<std::pair<std::string_view, uint32_t>>& items) {
  std::vector<std::optional<std::string>> result;
  result.reserve(items.size());

  for (const auto& [item, incr] : items) {
    result.push_back(IncrBy(item, incr));
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

  for (const auto& heap_item : min_heap_) {
    result.push_back({heap_item.key, heap_item.count});
  }

  // Sort by count (descending) for output
  std::sort(result.begin(), result.end(),
            [](const TopKItem& a, const TopKItem& b) { return a.count > b.count; });

  return result;
}

std::optional<std::string> TOPK::UpdateHeap(std::string_view item, uint32_t new_count) {
  // Fast path: O(1) hash lookup for membership, followed by a cache-friendly
  // O(K) linear scan and O(log K) heap update.
  auto it = item_to_hash_.find(item);
  if (it != item_to_hash_.end()) {
    size_t cached_hash = it->second;
    for (size_t i = 0; i < min_heap_.size(); ++i) {
      if ((min_heap_[i].hash == cached_hash) && (min_heap_[i].key == item)) {
        uint32_t old_count = min_heap_[i].count;
        min_heap_[i].count = new_count;
        if (new_count > old_count) {
          HeapifyDown(i);
        } else if (new_count < old_count) {
          HeapifyUp(i);
        }
        DCHECK_EQ(min_heap_.size(), item_to_hash_.size());
        return std::nullopt;
      }
    }
    LOG(DFATAL) << "TopK invariant broken: item found in map but missing from heap!";
    // Production: self-heal the corruption so the item can be cleanly re-inserted below.
    item_to_hash_.erase(it);
  }

  // Fast reject: item doesn't qualify for the heap. Just exit without any memory allocations or
  // modifications.
  if ((min_heap_.size() >= k_) && (new_count <= min_heap_.front().count)) {
    return std::nullopt;
  }
  DCHECK_LE(min_heap_.size(), k_);

  // Slow path: item will enter the heap. Now allocate.
  std::string item_str(item);
  size_t item_hash = XXH3_64bits(item.data(), item.size());

  if (min_heap_.size() < k_) {
    // Heap not full, add the item, no eviction needed
    size_t new_idx = min_heap_.size();
    min_heap_.push_back({item_str, new_count, item_hash});
    item_to_hash_[item_str] = item_hash;
    HeapifyUp(new_idx);
    DCHECK_EQ(min_heap_.size(), item_to_hash_.size());
    return std::nullopt;
  }

  // Heap is full, evict minimum and add new item
  DCHECK_EQ(min_heap_.size(), k_);
  std::string old_key = std::move(min_heap_[0].key);
  item_to_hash_.erase(old_key);
  min_heap_[0] = {item_str, new_count, item_hash};
  item_to_hash_[item_str] = item_hash;
  HeapifyDown(0);
  DCHECK_EQ(min_heap_.size(), item_to_hash_.size());
  return old_key;
}

size_t TOPK::MallocUsed() const {
  size_t size = 0;

  // Custom decay table (only for non-default decay values)
  if (custom_decay_table_) {
    size += sizeof(std::array<double, kDecayLookupSize>);
  }

  // Counter array
  size += counters_.capacity() * sizeof(uint32_t);

  // Heap items - calculate actual string sizes
  size += min_heap_.capacity() * sizeof(HeapItem);
  for (const auto& item : min_heap_) {
    size += item.key.capacity();
  }

  // flat_hash_map overhead
  size += item_to_hash_.bucket_count() * sizeof(std::pair<const std::string, size_t>);
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
  data.heap_items.reserve(min_heap_.size());
  for (const auto& heap_item : min_heap_) {
    data.heap_items.push_back({heap_item.key, heap_item.count});
  }

  // Serialize counter array
  data.counters.assign(counters_.begin(), counters_.end());

  return data;
}

void TOPK::Deserialize(const SerializedData& data) {
  DCHECK_EQ(data.counters.size(), static_cast<size_t>(width_) * depth_);
  DCHECK_LE(data.heap_items.size(), k_);
  DCHECK_EQ(data.k, k_);
  DCHECK_EQ(data.width, width_);
  DCHECK_EQ(data.depth, depth_);
  DCHECK_EQ(data.decay, decay_);

  // Clear existing data
  min_heap_.clear();
  item_to_hash_.clear();

  // Restore counters
  counters_.assign(data.counters.begin(), data.counters.end());

  // Restore heap
  min_heap_.reserve(data.heap_items.size());
  item_to_hash_.reserve(data.heap_items.size());
  for (const auto& item : data.heap_items) {
    size_t item_hash = XXH3_64bits(item.item.data(), item.item.size());
    min_heap_.push_back({item.item, item.count, item_hash});
    item_to_hash_[item.item] = item_hash;
  }

  // Rebuild heap property
  DCHECK_EQ(item_to_hash_.size(), data.heap_items.size());
  std::make_heap(min_heap_.begin(), min_heap_.end(), std::greater<HeapItem>());
}

}  // namespace dfly
