// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_table.h"

#include "core/oah_entry.h"
#include "core/oah_pair.h"

namespace dfly {

template <typename Entry> OAHTable<Entry>::~OAHTable() {
  FreeAllSlots();
}

template <typename Entry> void OAHTable<Entry>::Shrink(size_t new_size) {
  size_t required = size_ / kOverloadFactor + (size_ % kOverloadFactor != 0);
  size_t target = absl::bit_ceil(std::max({new_size, kMinBucketCount, required}));
  if (target >= BucketCount())
    return;

  size_t prev_size = entries_.size();
  capacity_log_ = absl::bit_width(target) - 1;

  // Process from low to high (opposite of Grow/Rehash).
  for (size_t i = 0; i < prev_size; ++i) {
    ShrinkBucket(i);
  }

  entries_.resize(Capacity());
  entries_.shrink_to_fit();
}

template <typename Entry> void OAHTable<Entry>::Clear() {
  FreeAllSlots();
  capacity_log_ = 0;
  entries_.resize(0);
  size_ = 0;
  obj_alloc_used_ = 0;
  ptr_vectors_alloc_used_ = 0;
  expiration_used_ = false;
}

template <typename Entry> uint32_t OAHTable<Entry>::ClearStep(uint32_t start, uint32_t count) {
  const uint32_t total = entries_.size();
  const uint32_t end = std::min(total, start + count);
  for (uint32_t i = start; i < end; ++i) {
    auto bucket = At(i);
    if (bucket.Empty())
      continue;

    if (bucket.IsVector()) {
      auto vec = bucket.AsVector();
      for (TaggedPtr& cell : vec) {
        Entry entry(cell);
        if (entry) {
          obj_alloc_used_ -= entry.AllocSize();
          --size_;
        }
      }
      ptr_vectors_alloc_used_ -= vec.AllocSize();
    } else {
      obj_alloc_used_ -= bucket[0].AllocSize();
      --size_;
    }
    bucket.Clear();
  }
  // Match Clear() semantics: once incrementally cleared empty, the TTL flag is stale.
  if (size_ == 0)
    expiration_used_ = false;
  return end;
}

template <typename Entry> size_t OAHTable<Entry>::SizeSlow() {
  if (expiration_used_)
    CollectExpired();
  return size_;
}

template <typename Entry> void OAHTable<Entry>::GrowCapacity(size_t bucket_capacity) {
  bucket_capacity = absl::bit_ceil(bucket_capacity);
  if (bucket_capacity > entries_.size()) {
    capacity_log_ = std::max(kMinCapacityLog, uint32_t(absl::bit_width(bucket_capacity) - 1));
    size_t prev_size = entries_.size();
    entries_.resize(Capacity());
    Rehash(prev_size);
  }
  assert(entries_.size() >= kDisplacementSize);
}

template <typename Entry> void OAHTable<Entry>::FreeAllSlots() {
  for (size_t i = 0, n = entries_.size(); i < n; ++i) {
    if (entries_[i])
      At(i).Clear();
  }
}

template <typename Entry> void OAHTable<Entry>::CollectExpired() {
  if (time_now_ == 0)
    return;
  const uint32_t now = time_now_;
  for (size_t b = 0, n = entries_.size(); b < n; ++b) {
    if (!entries_[b])
      continue;
    auto bucket = At(b);
    for (uint32_t i = 0, m = bucket.ElementsNum(); i < m; ++i) {
      auto entry = bucket[i];
      if (entry && entry.HasExpiry() && entry.GetExpiry() <= now)
        entry.ExpireIfNeeded(now, &size_, &obj_alloc_used_);
    }
  }
}

// Keep these explicit instantiations in sync with the OAH table entry types.
template OAHTable<OAHEntry>::~OAHTable();
template void OAHTable<OAHEntry>::Shrink(size_t);
template void OAHTable<OAHEntry>::Clear();
template uint32_t OAHTable<OAHEntry>::ClearStep(uint32_t, uint32_t);
template size_t OAHTable<OAHEntry>::SizeSlow();
template void OAHTable<OAHEntry>::GrowCapacity(size_t);
template void OAHTable<OAHEntry>::FreeAllSlots();
template void OAHTable<OAHEntry>::CollectExpired();

template OAHTable<OAHPair>::~OAHTable();
template void OAHTable<OAHPair>::Shrink(size_t);
template void OAHTable<OAHPair>::Clear();
template uint32_t OAHTable<OAHPair>::ClearStep(uint32_t, uint32_t);
template size_t OAHTable<OAHPair>::SizeSlow();
template void OAHTable<OAHPair>::GrowCapacity(size_t);
template void OAHTable<OAHPair>::FreeAllSlots();
template void OAHTable<OAHPair>::CollectExpired();

}  // namespace dfly
