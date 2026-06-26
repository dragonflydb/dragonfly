// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_set.h"

#include <bit>

#include "base/logging.h"

namespace dfly {

// Definitions below are `inline FORCE_INLINE`: inline makes them COMDAT
// (non-interposable), which lets always_inline apply to an out-of-line member so
// they fold into their in-TU callers.

template <typename Wide>
inline FORCE_INLINE OAHSet::LaneMasks OAHSet::ProbeLanes(const TaggedPtr* base,
                                                         uint64_t ext_hash) noexcept {
  auto data_v = Wide::Load(base);
  auto hash_v = (data_v & Wide::Fill(OAHEntry::kExtHashShiftedMask)) >> OAHEntry::kExtHashShift;
  // ~is_empty excludes empty lanes, whose zero hash would otherwise match a zero query hash.
  auto is_empty = data_v == uint64_t(0);
  auto candidate = (hash_v == ext_hash) & ~is_empty;
  return {candidate.GetMSBs(), is_empty.GetMSBs()};
}

// Window may exceed one SIMD register: sweep in EntryWide strides, packing each
// stride's masks (lane i of stride `off` -> bit off+i). uint32_t masks => <= 32 lanes.
inline FORCE_INLINE OAHSet::LaneMasks OAHSet::ProbeWindow(const TaggedPtr* base,
                                                          uint64_t ext_hash) noexcept {
  LaneMasks w{0, 0};
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
    const LaneMasks m = ProbeLanes<EntryWide>(base + off, ext_hash);
    w.candidates |= m.candidates << off;
    w.empties |= m.empties << off;
  }
  return w;
}

// 2-lane SIMD strides. Vector sizes are always even (PtrVector grows by 2), so the
// stride covers the array with no tail.
inline FORCE_INLINE TaggedPtr* OAHSet::ProbeExtensionVector(uint32_t ext_bid, std::string_view str,
                                                            uint64_t ext_hash) {
  auto vec = At(ext_bid).AsVector();
  TaggedPtr* raw_arr = vec.Raw();
  const size_t size = vec.Size();
  DCHECK_GE(size, size_t(kVectorLaneStep));
  DCHECK_EQ(size % kVectorLaneStep, 0u);

  for (size_t base = 0; base < size; base += kVectorLaneStep) {
    auto cand_bits =
        ProbeLanes<VectorWide>(reinterpret_cast<const uint64_t*>(&raw_arr[base]), ext_hash)
            .candidates;
    while (cand_bits) {
      const uint32_t j = std::countr_zero(cand_bits);
      cand_bits &= cand_bits - 1;
      OAHEntry re(raw_arr[base + j]);
      const bool match = re.Key() == str;
      re.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
      if (match)
        return &raw_arr[base + j];
    }
  }
  return nullptr;
}

// Window read stays in bounds: entries_ has kDisplacementSize-1 slack past BucketCount.
inline FORCE_INLINE OAHSet::MatchResult OAHSet::FindMatch(uint32_t bid, uint32_t ext_bid,
                                                          uint32_t cand_bits, std::string_view str,
                                                          uint64_t ext_hash) {
  while (cand_bits) {
    const uint32_t i = std::countr_zero(cand_bits);
    cand_bits &= cand_bits - 1;
    const uint32_t bucket_id = bid + i;
    OAHPtr p = At(bucket_id);
    if (p.IsVector())  // vectors live only at the extension point
      continue;
    OAHEntry e = p[0];
    const bool match = e.Key() == str;
    e.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
    if (match)
      return {&entries_[bucket_id], bucket_id, 0};
  }
  if (At(ext_bid).IsVector()) {
    if (TaggedPtr* hit = ProbeExtensionVector(ext_bid, str, ext_hash))
      return {hit, ext_bid, static_cast<uint32_t>(hit - At(ext_bid).AsVector().Raw())};
  }
  return {nullptr, 0, 0};
}

inline FORCE_INLINE bool OAHSet::AddImpl(std::string_view str, uint32_t ttl_sec) {
  // Grow at load factor 2; until then overflow lands in the window / extension vectors.
  if (size_ >= entries_.size() * 2) [[unlikely]] {
    Reserve(BucketCount() * 2);
  }
  DCHECK_GE(Capacity(), kDisplacementSize);

  uint64_t hash = Hash(str);
  auto bucket_id = BucketId(hash, capacity_log_);
  PREFETCH_READ(entries_.data() + bucket_id);

  const ssize_t mem_before = zmalloc_used_memory_tl;
  TaggedPtr entry_tagged_ptr = OAHEntry::Create(str, EntryTTL(ttl_sec));
  if (ttl_sec != UINT32_MAX)
    expiration_used_ = true;
  const size_t entry_alloc_size = zmalloc_used_memory_tl - mem_before;

  const uint32_t ext_bid = GetExtensionPoint(bucket_id);
  PREFETCH_READ(At(ext_bid).Raw());

  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  OAHEntry new_entry(entry_tagged_ptr);
  new_entry.SetExtHash(ext_hash);

  const LaneMasks masks = ProbeWindow(&entries_[bucket_id], ext_hash);
  const MatchResult m = FindMatch(bucket_id, ext_bid, masks.candidates, str, ext_hash);
  if (m.matched && *m.matched != 0) {
    OAHEntry::Destroy(entry_tagged_ptr);  // duplicate already present: discard the new blob
    return false;
  }

  obj_alloc_used_ += entry_alloc_size;
  ++size_;
  // Reuse an expired duplicate's slot, else a free window lane, else spill to the vector.
  if (m.matched) {
    *m.matched = entry_tagged_ptr;
  } else if (masks.empties) {
    At(bucket_id + std::countr_zero(masks.empties)).Assign(entry_tagged_ptr);
  } else {
    ptr_vectors_alloc_used_ += At(ext_bid).Insert(entry_tagged_ptr);
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

inline FORCE_INLINE OAHSet::iterator OAHSet::FindInternal(uint32_t bid, std::string_view str,
                                                          uint64_t hash) {
  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  const uint32_t cand_bits = ProbeWindow(&entries_[bid], ext_hash).candidates;
  const MatchResult m = FindMatch(bid, GetExtensionPoint(bid), cand_bits, str, ext_hash);
  if (m.matched && *m.matched != 0)  // empty => matched but just expired, i.e. gone
    return iterator{this, m.bucket_id, m.pos_in_vec};
  return end();
}

OAHSet::iterator OAHSet::Find(std::string_view member) {
  if (entries_.empty())
    return end();
  uint64_t hash = Hash(member);
  return FindInternal(BucketId(hash, capacity_log_), member, hash);
}

bool OAHSet::Erase(std::string_view str) {
  if (entries_.empty())
    return false;
  uint64_t hash = Hash(str);
  auto item = FindInternal(BucketId(hash, capacity_log_), str, hash);
  if (item == end())
    return false;
  --size_;
  OAHEntry victim = *item;
  obj_alloc_used_ -= victim.AllocSize();
  OAHEntry::Destroy(victim.Release());

  OAHPtr bucket = At(item.bucket_id());
  if (bucket.IsVector() && bucket.AsVector().Empty()) {
    ptr_vectors_alloc_used_ -= bucket.AsVector().AllocSize();
    bucket.Clear();
  }
  return true;
}

inline FORCE_INLINE OAHSet::iterator OAHSet::PickFromBucket(uint32_t b) {
  OAHPtr bucket = At(b);
  if (!bucket.IsVector()) {
    OAHEntry e = bucket[0];
    e.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
    return e.Empty() ? end() : iterator{this, b, 0};
  }
  auto vec = bucket.AsVector();
  for (uint32_t pos = 0, vec_size = vec.Size(); pos < vec_size; ++pos) {
    OAHEntry entry(vec[pos]);
    if (!entry)
      continue;
    entry.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
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

inline FORCE_INLINE uint32_t OAHSet::FindEmptyAround(uint32_t bid) {
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
