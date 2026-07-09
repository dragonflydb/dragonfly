// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_set.h"

#include <algorithm>
#include <bit>

#include "base/logging.h"

namespace dfly {

template <typename Wide>
OAHSet::LaneMasks OAHSet::ProbeLanes(const TaggedPtr* base, uint64_t shifted_ext_hash) noexcept {
  DCHECK_NE(shifted_ext_hash, 0u);  // never 0 (CalcExtHash remap), so empty lanes can't match
  auto data_v = Wide::Load(base);
  // Mask covers the vector bit so vector slots never match a candidate (window probes then need
  // no per-candidate IsVector test); ext-hash is never 0, so empty lanes can't match the query.
  auto stored = data_v & Wide::Fill(OAHEntry::kExtHashShiftedMask | OAHPtr::kVectorBit);
  auto is_empty = data_v == uint64_t(0);
  auto candidate = stored == shifted_ext_hash;
  return {candidate.GetMSBs(), is_empty.GetMSBs()};
}

// Window may exceed one SIMD register: sweep in EntryWide strides, packing each
// stride's masks (lane i of stride `off` -> bit off+i). uint32_t masks => <= 32 lanes.
// Takes the unshifted fingerprint and shifts it in place. Erase/Find use this so the shift
// stays out of their (larger) inlined bodies -- inlining it there measured a codegen regression.
OAHSet::LaneMasks OAHSet::ProbeWindow(const TaggedPtr* base, uint64_t ext_hash) noexcept {
  const uint64_t shifted_ext_hash = ext_hash << OAHEntry::kExtHashShift;
  LaneMasks w{0, 0};
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
    const LaneMasks m = ProbeLanes<EntryWide>(base + off, shifted_ext_hash);
    w.candidates |= m.candidates << off;
    w.empties |= m.empties << off;
  }
  return w;
}

// Twin of ProbeWindow taking the already-shifted fingerprint (no internal shift). AddImpl uses it
// because it also needs the shifted value for SetShiftedExtHash -- one shift instead of two.
OAHSet::LaneMasks OAHSet::ProbeWindowShifted(const TaggedPtr* base,
                                             uint64_t shifted_ext_hash) noexcept {
  LaneMasks w{0, 0};
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
    const LaneMasks m = ProbeLanes<EntryWide>(base + off, shifted_ext_hash);
    w.candidates |= m.candidates << off;
    w.empties |= m.empties << off;
  }
  return w;
}

uint32_t OAHSet::ScanWindowMask(const TaggedPtr* base, uint64_t target, uint32_t shift,
                                uint32_t* vector_mask_out) noexcept {
  uint32_t cand = 0;
  uint32_t vec = 0;
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
    const EntryWide data = EntryWide::Load(base + off);
    const uint32_t isvec =
        ((data & EntryWide::Fill(OAHPtr::kVectorBit)) == OAHPtr::kVectorBit).GetMSBs();
    const uint32_t matched = ((data >> shift) == target).GetMSBs();
    // target == 0 also matches all-zero empties; drop them here (cheaper than a scalar re-read).
    const uint32_t is_empty = (data == uint64_t(0)).GetMSBs();
    cand |= (matched & ~is_empty & ~isvec) << off;
    vec |= isvec << off;
  }
  *vector_mask_out = vec;
  return cand;
}

template <typename Wide>
uint32_t OAHSet::AffiliationMask(const TaggedPtr* base, uint64_t target, uint32_t shift) noexcept {
  const Wide data = Wide::Load(base);
  // Vector arrays may hold empty slots; exclude them so target == 0 doesn't report holes.
  const uint32_t matched = ((data >> shift) == target).GetMSBs();
  const uint32_t is_empty = (data == uint64_t(0)).GetMSBs();
  return matched & ~is_empty;
}

template <bool Expire> bool OAHSet::ScanHomeBucket(uint32_t bucket_id, const ItemCb& cb) {
  const uint32_t part = std::min(capacity_log_, kShiftLog);
  DCHECK_GT(part, 0u);
  // ScanWindowMask drops empty lanes, so `cand` holds only affiliated non-empty single entries.
  const uint32_t shift = 64 - part;
  const uint64_t target = bucket_id & ((uint64_t{1} << part) - 1);

  const TaggedPtr* base = &entries_[bucket_id];
  // Prefetch every window slot's blob to overlap the loads with the SIMD mask below (an empty
  // slot prefetches null, a no-op).
  for (uint32_t i = 0; i < kDisplacementSize; ++i)
    PREFETCH_READ(reinterpret_cast<const char*>(base[i] & ~OAHEntry::kTagMask));

  uint32_t vec_mask = 0;
  uint32_t cand = ScanWindowMask(base, target, shift, &vec_mask);
  bool reported = false;

  while (cand) {
    const uint32_t i = std::countr_zero(cand);
    cand &= cand - 1;
    OAHEntry e = At(bucket_id + i)[0];
    if constexpr (Expire) {
      ExpireIfNeeded(e);
      if (e.Empty())
        continue;
    }
    cb(e.Key());
    reported = true;
  }

  if (vec_mask) {
    DCHECK_EQ(vec_mask & (vec_mask - 1), 0u);
    const uint32_t vi = std::countr_zero(vec_mask);
    auto vec = At(bucket_id + vi).AsVector();
    TaggedPtr* raw = vec.Raw();
    const size_t vsize = vec.Size();
    for (size_t b = 0; b < vsize; b += VectorWide::kLanes) {
      uint32_t m = AffiliationMask<VectorWide>(&raw[b], target, shift);
      while (m) {
        const uint32_t j = std::countr_zero(m);
        m &= m - 1;
        OAHEntry el(raw[b + j]);
        if constexpr (Expire) {
          ExpireIfNeeded(el);
          if (el.Empty())
            continue;
        }
        cb(el.Key());
        reported = true;
      }
    }
  }

  return reported;
}

// Explicit instantiations so the inline Scan (in the header) can call both specializations.
template bool OAHSet::ScanHomeBucket<true>(uint32_t, const ItemCb&);
template bool OAHSet::ScanHomeBucket<false>(uint32_t, const ItemCb&);

// 2-lane SIMD strides. Vector sizes are always even (PtrVector grows by 2), so the
// stride covers the array with no tail.
TaggedPtr* OAHSet::ProbeExtensionVector(uint32_t ext_bid, std::string_view str, uint64_t ext_hash) {
  auto vec = At(ext_bid).AsVector();
  TaggedPtr* raw_arr = vec.Raw();
  const size_t size = vec.Size();
  DCHECK_GE(size, size_t(kVectorLaneStep));
  DCHECK_EQ(size % kVectorLaneStep, 0u);

  const uint64_t shifted_ext_hash = ext_hash << OAHEntry::kExtHashShift;
  for (size_t base = 0; base < size; base += kVectorLaneStep) {
    auto cand_bits =
        ProbeLanes<VectorWide>(reinterpret_cast<const uint64_t*>(&raw_arr[base]), shifted_ext_hash)
            .candidates;
    while (cand_bits) {
      const uint32_t j = std::countr_zero(cand_bits);
      cand_bits &= cand_bits - 1;
      if (OAHEntry(raw_arr[base + j]).Key() == str)
        return &raw_arr[base + j];
    }
  }
  return nullptr;
}

bool OAHSet::AddImpl(std::string_view str, uint32_t ttl_sec) {
  if (size_ >= entries_.size()) [[unlikely]] {
    Reserve(BucketCount() * 2);
  }
  DCHECK_GE(Capacity(), kDisplacementSize);

  uint64_t hash = Hash(str);
  auto bucket_id = BucketId(hash, capacity_log_);
  PREFETCH_READ(entries_.data() + bucket_id);

  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  const uint64_t shifted_ext_hash = ext_hash << OAHEntry::kExtHashShift;

  const ssize_t mem_before = zmalloc_used_memory_tl;
  TaggedPtr entry_tagged_ptr = OAHEntry::Create(str, EntryTTL(ttl_sec));
  OAHEntry(entry_tagged_ptr).SetShiftedExtHash(shifted_ext_hash);  // reuse the shifted value
  if (ttl_sec != UINT32_MAX)
    expiration_used_ = true;
  const size_t entry_alloc_size = zmalloc_used_memory_tl - mem_before;

  const uint32_t ext_bid = GetExtensionPoint(bucket_id);
  PREFETCH_READ(At(ext_bid).Raw());

  const LaneMasks masks = ProbeWindowShifted(&entries_[bucket_id], shifted_ext_hash);

  // Add/Find/Erase each inline their own probe (measured faster than a shared helper). Add needs
  // only the matched cell, for the duplicate check and slot reuse.
  TaggedPtr* matched = nullptr;
  TaggedPtr* base = entries_.data();
  for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
    TaggedPtr* cell = &base[bucket_id + std::countr_zero(cand_bits)];
    if (OAHEntry(*cell).Key() == str) {
      matched = cell;
      break;
    }
  }
  if (!matched && At(ext_bid).IsVector())
    matched = ProbeExtensionVector(ext_bid, str, ext_hash);

  if (matched) {
    OAHEntry dup(*matched);
    ExpireIfNeeded(dup);  // reap an already-expired duplicate so its cell can be reused
    if (!dup.Empty()) {
      OAHEntry::Destroy(entry_tagged_ptr);  // live duplicate
      return false;
    }
    // Reaped an expired duplicate: its cell is empty, so reuse it in place.
    obj_alloc_used_ += entry_alloc_size;
    ++size_;
    *matched = entry_tagged_ptr;
    return true;
  }

  obj_alloc_used_ += entry_alloc_size;
  ++size_;
  if (masks.empties) {
    At(bucket_id + std::countr_zero(masks.empties)).Assign(entry_tagged_ptr);
  } else {
    ptr_vectors_alloc_used_ += At(ext_bid).InsertNonEmpty(entry_tagged_ptr);  // window full
  }
  return true;
}

bool OAHSet::Add(std::string_view str, uint32_t ttl_sec) {
  return AddImpl(str, ttl_sec);
}

unsigned OAHSet::AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec, bool keepttl) {
  Reserve(span.size());
  unsigned res = 0;
  const bool has_ttl = ttl_sec != UINT32_MAX;
  for (auto& s : span) {
    if (AddImpl(s, ttl_sec)) {
      ++res;
    } else if (has_ttl && !keepttl) {
      auto it = Find(s);
      if (it != end())
        it.SetExpiryTime(ttl_sec);
    }
  }
  return res;
}

template <bool Expire>
OAHSet::iterator OAHSet::FindInternal(uint32_t bid, std::string_view str, uint64_t hash) {
  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  const LaneMasks masks = ProbeWindow(&entries_[bid], ext_hash);

  // Find returns an iterator built straight from the match. With no TTLs a key match is always
  // live, so ExpireIfNeeded and the resulting empty re-check are compiled out.
  TaggedPtr* base = entries_.data();
  for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
    const uint32_t bucket_id = bid + std::countr_zero(cand_bits);
    OAHEntry e(base[bucket_id]);
    if (e.Key() == str) {
      if constexpr (Expire) {
        ExpireIfNeeded(e);
        if (e.Empty())
          return end();
      }
      return iterator{this, bucket_id, 0};
    }
  }
  const uint32_t ext_bid = GetExtensionPoint(bid);
  if (At(ext_bid).IsVector()) {
    if (TaggedPtr* hit = ProbeExtensionVector(ext_bid, str, ext_hash)) {
      if constexpr (Expire) {
        OAHEntry e(*hit);
        ExpireIfNeeded(e);
        if (e.Empty())
          return end();
      }
      return iterator{this, ext_bid, static_cast<uint32_t>(hit - At(ext_bid).AsVector().Raw())};
    }
  }
  return end();
}

OAHSet::iterator OAHSet::Find(std::string_view member) {
  if (entries_.empty())
    return end();
  const uint64_t hash = Hash(member);
  const uint32_t bid = BucketId(hash, capacity_log_);
  return expiration_used_ ? FindInternal<true>(bid, member, hash)
                          : FindInternal<false>(bid, member, hash);
}

bool OAHSet::Erase(std::string_view str) {
  if (entries_.empty())
    return false;
  const uint64_t hash = Hash(str);
  const uint32_t bid = BucketId(hash, capacity_log_);
  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  const LaneMasks masks = ProbeWindow(&entries_[bid], ext_hash);

  // Erase keeps the matched cell (to free it) and whether the hit lives in an extension vector.
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
  const bool removed = !IsExpired(victim);  // already-expired target => not-removed (like Redis)

  --size_;
  obj_alloc_used_ -= victim.AllocSize();
  OAHEntry::Destroy(victim.Release());

  if (in_vector) {  // reclaim the vector if the erase emptied it
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

  // Random-start wrap-around. The first range covers `n - start` buckets out of `n`,
  // so for a non-trivially populated set finding a live entry there is the common
  // case; the wrap-around call to [0, start) is the rare cold path.
  if (auto it = ScanRange(start, n); it != end())
    return it;
  return ScanRange(0, start);
}

uint32_t OAHSet::FindEmptyAround(uint32_t bid) {
  // Strides scanned in order, so the first empty found is the lowest-index one. In
  // bounds thanks to entries_' kDisplacementSize-1 slack past BucketCount.
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
    const EntryWide data = EntryWide::Load(&entries_[bid + off]);
    if (uint32_t empties = (data == uint64_t(0)).GetMSBs())
      return bid + off + std::countr_zero(empties);
  }
  // TODO add expiration logic
  const uint32_t ext = GetExtensionPoint(bid);
  DCHECK_LT(ext, entries_.size());
  return ext;
}

void OAHSet::Rehash(uint32_t prev_size) {
  if (prev_size == 0) {
    return;
  }
  // we should prevent moving elements before current possition to avoid double processing.
  // Detach the first mix_size slots into locals; each `bucket` view is freed explicitly
  // after its entries are redistributed into entries_ (a TaggedPtr slot has no dtor).
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

  // Process from low to high (opposite of Grow/Rehash).
  for (size_t i = 0; i < prev_size; ++i) {
    ShrinkBucket(i);
  }

  entries_.resize(Capacity());
  entries_.shrink_to_fit();
}

}  // namespace dfly
