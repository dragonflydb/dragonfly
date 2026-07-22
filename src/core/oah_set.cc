// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_set.h"

namespace dfly {

void OAHSet::Clear() {
  FreeAllSlots();
  capacity_log_ = 0;
  entries_.resize(0);
  size_ = 0;
  obj_alloc_used_ = 0;
  ptr_vectors_alloc_used_ = 0;
  expiration_used_ = false;
}

uint32_t OAHSet::ClearStep(uint32_t start, uint32_t count) {
  const uint32_t total = entries_.size();
  const uint32_t end = std::min(total, start + count);
  for (uint32_t i = start; i < end; ++i) {
    auto bucket = At(i);
    if (bucket.Empty())
      continue;

    if (bucket.IsVector()) {
      auto vec = bucket.AsVector();
      for (TaggedPtr& cell : vec) {
        OAHEntry entry(cell);
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
  if (size_ == 0)
    expiration_used_ = false;
  return end;
}

void OAHSet::Fill(OAHSet* other) {
  assert(other->entries_.empty());
  other->Reserve(UpperBoundSize());
  other->set_time(time_now());
  for (auto it = begin(), it_end = end(); it != it_end; ++it) {
    other->Add(it->Key(), it.HasExpiry() ? it.ExpiryTime() - time_now() : UINT32_MAX);
  }
}

uint32_t OAHSet::Scan(uint32_t cursor, const ItemCb& cb) {
  if (entries_.empty())
    return 0;

  uint32_t bucket_id = cursor >> (32 - capacity_log_);
  const bool expire = expiration_used_;

  for (; bucket_id < BucketCount(); ++bucket_id) {
    const bool reported =
        expire ? ScanHomeBucket<true>(bucket_id, cb) : ScanHomeBucket<false>(bucket_id, cb);
    if (reported)
      break;
  }

  if (++bucket_id >= BucketCount())
    return 0;

  return bucket_id << (32 - capacity_log_);
}

bool OAHSet::Erase(std::string_view str) {
  if (entries_.empty())
    return false;
  const uint64_t hash = Hash(str);
  const uint32_t bid = BucketId(hash, capacity_log_);
  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  const LaneMasks masks = ProbeWindow(&entries_[bid], ext_hash);

  TaggedPtr* matched = nullptr;
  TaggedPtr* base = entries_.data();
  for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
    TaggedPtr* cell = &base[bid + std::countr_zero(cand_bits)];
    if (OAHEntry(*cell).Key() == str) {
      matched = cell;
      break;
    }
  }
  const uint32_t ext_bid = GetExtensionPoint(bid);
  bool in_vector = false;
  if (!matched && At(ext_bid).IsVector()) {
    matched = ProbeExtensionVector(ext_bid, str, ext_hash);
    in_vector = matched != nullptr;
  }
  if (!matched)
    return false;

  OAHEntry victim(*matched);
  const bool removed = !IsExpired(victim);

  --size_;
  obj_alloc_used_ -= victim.AllocSize();
  OAHEntry::Destroy(victim.Release());

  if (in_vector) {
    OAHPtr bucket = At(ext_bid);
    auto vec = bucket.AsVector();
    if (vec.Empty()) {
      ptr_vectors_alloc_used_ -= vec.AllocSize();
      bucket.Clear();
    }
  }
  return removed;
}

OAHSet::iterator OAHSet::PickFromBucket(uint32_t b) {
  OAHPtr bucket = At(b);
  if (!bucket.IsVector()) {
    OAHEntry e = bucket[0];
    ExpireIfNeeded(e);
    return e.Empty() ? end() : iterator{this, b, 0};
  }
  auto vec = bucket.AsVector();
  for (uint32_t pos = 0, vec_size = vec.Size(); pos < vec_size; ++pos) {
    OAHEntry entry(vec[pos]);
    if (!entry)
      continue;
    ExpireIfNeeded(entry);
    if (entry)
      return iterator{this, b, pos};
  }
  return end();
}

OAHSet::iterator OAHSet::ScanRange(uint32_t lo, uint32_t hi) {
  for (; lo + EntryWide::kLanes <= hi; lo += EntryWide::kLanes) {
    const EntryWide data = EntryWide::Load(&entries_[lo]);
    uint32_t used = (~(data == uint64_t(0))).GetMSBs();
    while (used) {
      const uint32_t b = lo + std::countr_zero(used);
      used &= used - 1;
      if (auto it = PickFromBucket(b); it != end())
        return it;
    }
  }
  for (; lo < hi; ++lo) {
    if (entries_[lo] == 0)
      continue;
    if (auto it = PickFromBucket(lo); it != end())
      return it;
  }
  return end();
}

OAHSet::iterator OAHSet::GetRandomMember() {
  if (entries_.empty() || size_ == 0)
    return end();

  static thread_local absl::InsecureBitGen rng;
  const uint32_t n = entries_.size();
  const uint32_t start = absl::Uniform<uint32_t>(rng, 0u, n);

  if (auto it = ScanRange(start, n); it != end())
    return it;
  return ScanRange(0, start);
}

uint32_t OAHSet::FindEmptyAround(uint32_t bid) {
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
    const EntryWide data = EntryWide::Load(&entries_[bid + off]);
    if (uint32_t empties = (data == uint64_t(0)).GetMSBs())
      return bid + off + std::countr_zero(empties);
  }
  const uint32_t ext = GetExtensionPoint(bid);
  DCHECK_LT(ext, entries_.size());
  return ext;
}

void OAHSet::Rehash(uint32_t prev_size) {
  if (prev_size == 0) {
    return;
  }

  constexpr size_t mix_size = (2 << kShiftLog) - 1;
  std::array<TaggedPtr, mix_size> old_buckets{};
  for (size_t i = 0; i < mix_size; ++i) {
    old_buckets[i] = entries_[i];
    entries_[i] = 0;
  }

  for (size_t bucket_id = prev_size - 1; bucket_id >= mix_size; --bucket_id) {
    TaggedPtr slot = entries_[bucket_id];
    entries_[bucket_id] = 0;
    RedistributeBucket(slot);
  }

  for (size_t bucket_id = 0; bucket_id < mix_size; ++bucket_id)
    RedistributeBucket(old_buckets[bucket_id]);
}

void OAHSet::RedistributeBucket(TaggedPtr& slot) {
  OAHPtr bucket(slot);
  for (uint32_t pos = 0, size = bucket.ElementsNum(); pos < size; ++pos) {
    if (bucket[pos]) {
      uint32_t new_bucket_id = FindEmptyAround(RehashEntry(bucket[pos]));
      ptr_vectors_alloc_used_ += At(new_bucket_id).Insert(bucket.Remove(pos));
    }
  }
  if (bucket.IsVector())
    ptr_vectors_alloc_used_ -= bucket.AsVector().AllocSize();
  bucket.Clear();
}

void OAHSet::Shrink(size_t new_size) {
  assert(absl::has_single_bit(new_size));
  assert(new_size >= (1u << kMinCapacityLog));
  assert(new_size < entries_.size());

  size_t prev_size = entries_.size();
  capacity_log_ = absl::bit_width(new_size) - 1;

  for (size_t i = 0; i < prev_size; ++i) {
    ShrinkBucket(i);
  }

  entries_.resize(Capacity());
  entries_.shrink_to_fit();
}

}  // namespace dfly
