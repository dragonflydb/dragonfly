// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_set.h"

#include <bit>

#include "base/logging.h"

namespace dfly {

// Several definitions below are `inline FORCE_INLINE`: the inline keyword makes
// them COMDAT (non-interposable), which is what lets always_inline apply to an
// out-of-line member, so they fold into their in-TU callers (notably AddImpl into
// AddMany's bulk-insert loop, and FindMatch into both AddImpl and FindInternal).

template <typename Wide>
inline FORCE_INLINE OAHSet::LaneMasks OAHSet::ProbeLanes(const OAHEntry* base,
                                                         uint64_t ext_hash) noexcept {
  auto data_v = Wide::Load(reinterpret_cast<const uint64_t*>(base));
  auto hash_v = (data_v & Wide::Fill(OAHEntry::kExtHashShiftedMask)) >> OAHEntry::kExtHashShift;
  // ~is_empty stops an empty lane's zero hash from aliasing a hash/lazy-zero match.
  auto is_empty = data_v == uint64_t(0);
  auto candidate = ((hash_v == ext_hash) | (hash_v == uint64_t(0))) & ~is_empty;
  return {candidate.GetMSBs(), is_empty.GetMSBs()};
}

inline FORCE_INLINE void OAHSet::RefreshStaleCandidate(OAHEntry& e, uint64_t ext_hash) {
  if (e.GetHash() != ext_hash)
    e.SetExtHash(CalcExtHash(Hash(e.Key()), capacity_log_));
  e.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
}

// 2-lane SIMD strides; the vector's size is a power of 2, >= 2.
inline FORCE_INLINE OAHEntry* OAHSet::ProbeExtensionVector(uint32_t ext_bid, std::string_view str,
                                                           uint64_t ext_hash) {
  auto& vec = entries_[ext_bid].AsVector();
  auto* raw_arr = vec.Raw();
  const size_t size = vec.Size();
  DCHECK_GE(size, size_t(kVectorLaneStep));
  DCHECK(std::has_single_bit(size));

  for (size_t base = 0; base < size; base += kVectorLaneStep) {
    auto cand_bits = ProbeLanes<VectorWide>(&raw_arr[base], ext_hash).candidates;
    while (cand_bits) {
      const uint32_t j = std::countr_zero(cand_bits);
      cand_bits &= cand_bits - 1;
      OAHEntry& re = raw_arr[base + j];
      if (re.Key() != str) {
        RefreshStaleCandidate(re, ext_hash);
        continue;
      }
      re.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
      return &re;
    }
  }
  return nullptr;
}

// Scans the displacement window then the extension vector for `str`. entries_
// spans (1 << capacity_log_) + kDisplacementSize - 1 with bid < (1 << capacity_log_),
// so the window read stays in bounds.
inline FORCE_INLINE OAHSet::MatchResult OAHSet::FindMatch(uint32_t bid, uint32_t ext_bid,
                                                          uint32_t cand_bits, std::string_view str,
                                                          uint64_t ext_hash) {
  while (cand_bits) {
    const uint32_t i = std::countr_zero(cand_bits);
    cand_bits &= cand_bits - 1;
    const uint32_t bucket_id = bid + i;
    OAHEntry& e = entries_[bucket_id];
    if (e.IsVector())  // vectors live only at the extension point
      continue;
    if (e.Key() != str) {
      RefreshStaleCandidate(e, ext_hash);
      continue;
    }
    e.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
    return {&e, bucket_id, 0};
  }
  if (entries_[ext_bid].IsVector()) {
    if (OAHEntry* hit = ProbeExtensionVector(ext_bid, str, ext_hash))
      return {hit, ext_bid, static_cast<uint32_t>(hit - entries_[ext_bid].AsVector().Raw())};
  }
  return {nullptr, 0, 0};
}

inline FORCE_INLINE bool OAHSet::AddImpl(std::string_view str, uint32_t ttl_sec) {
  if (size_ >= entries_.size()) [[unlikely]] {
    Reserve(BucketCount() * 2);
  }
  DCHECK_GE(Capacity(), kDisplacementSize);

  uint64_t hash = Hash(str);
  auto bucket_id = BucketId(hash, capacity_log_);
  PREFETCH_READ(entries_.data() + bucket_id);

  const ssize_t mem_before = zmalloc_used_memory_tl;
  OAHEntry entry(str, EntryTTL(ttl_sec));
  if (ttl_sec != UINT32_MAX)
    expiration_used_ = true;
  const size_t entry_alloc_size = zmalloc_used_memory_tl - mem_before;

  const uint32_t ext_bid = GetExtensionPoint(bucket_id);
  PREFETCH_READ(entries_[ext_bid].Raw());

  const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
  entry.SetExtHash(ext_hash);

  const LaneMasks masks = ProbeLanes<EntryWide>(&entries_[bucket_id], ext_hash);
  const MatchResult m = FindMatch(bucket_id, ext_bid, masks.candidates, str, ext_hash);
  if (m.matched && !m.matched->Empty())
    return false;

  obj_alloc_used_ += entry_alloc_size;
  ++size_;
  // Place it: reuse an expired duplicate's slot, else a free window lane, else
  // spill into the extension vector.
  if (m.matched) {
    *m.matched = std::move(entry);
  } else if (masks.empties) {
    entries_[bucket_id + std::countr_zero(masks.empties)] = std::move(entry);
  } else {
    ptr_vectors_alloc_used_ += entries_[ext_bid].Insert(std::move(entry));
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
  const uint32_t cand_bits = ProbeLanes<EntryWide>(&entries_[bid], ext_hash).candidates;
  const MatchResult m = FindMatch(bid, GetExtensionPoint(bid), cand_bits, str, ext_hash);
  if (m.matched && !m.matched->Empty())  // empty => matched but just expired, i.e. gone
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
  obj_alloc_used_ -= item->AllocSize();
  *item = OAHEntry();
  uint32_t erase_bucket = item.bucket_id();
  if (entries_[erase_bucket].IsVector() && entries_[erase_bucket].AsVector().Empty()) {
    ptr_vectors_alloc_used_ -= entries_[erase_bucket].AsVector().AllocSize();
    entries_[erase_bucket] = OAHEntry();
  }
  return true;
}

}  // namespace dfly
