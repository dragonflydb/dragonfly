// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/numeric/bits.h>
#include <absl/random/random.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <functional>
#include <string_view>
#include <vector>

#include "common/rapidhash.h"
#include "core/detail/stateless_allocator.h"
#include "core/oah_base.h"
#include "core/oah_ptr.h"
#include "core/simd_op.h"

namespace dfly {

// oah_table.h - an open-addressing hash container of string members (the OAHTable base template).
//
// OAHTable<Entry> stores members in a flat array of TaggedPtr buckets. Each bucket is wrapped by a
// non-owning OAHPtr holding either a single Entry (OAHEntry for a set, OAHPair for a map) or a
// PtrVector collision chain. Lookups probe a small SIMD window around the home bucket and spill
// overflow into an extension-point vector. Buckets own their blobs/vectors and are freed explicitly
// (a TaggedPtr has no destructor).
//
// OAHTable holds all entry-agnostic machinery (probe/find/erase/scan/rehash). The insertion path is
// entry-specific (a set adds a key, a map a key+value with replace/exchange semantics), so it lives
// in the derived OAHSet / OAHMap where each keeps its own tight, separately optimized code path.
template <typename Entry> class OAHTable {  // Open Addressing Hash table
 protected:
  using TaggedPtr = oah::TaggedPtr;

 private:
  using Buckets = std::vector<TaggedPtr, StatelessAllocator<TaggedPtr>>;

 public:
  static constexpr std::uint32_t kShiftLog = 2;                         // TODO make template
  static constexpr std::uint32_t kMinCapacityLog = kShiftLog;           // should be >= ShiftLog
  static constexpr std::uint32_t kDisplacementSize = (1 << kShiftLog);  // TODO check

  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Entry;
    using pointer = Entry;
    using reference = Entry;

    iterator(OAHTable* owner, uint32_t bucket_id, uint32_t pos_in_bucket)
        : owner_(owner), bucket_(bucket_id), pos_(pos_in_bucket) {
    }

    void SetExpiryTime(uint32_t ttl_sec) {
      auto entry = owner_->At(bucket_)[pos_];
      owner_->obj_alloc_used_ -= entry.AllocSize();
      owner_->At(bucket_)[pos_].SetExpiry(owner_->EntryTTL(ttl_sec));
      owner_->obj_alloc_used_ += entry.AllocSize();
      owner_->expiration_used_ = true;
    }

    iterator& operator++() {
      ++pos_;
      SetEntryIt();
      return *this;
    }

    bool operator==(const iterator& r) const {
      if (owner_ == nullptr || r.owner_ == nullptr) {
        return owner_ == r.owner_;
      }
      assert(owner_ == r.owner_);
      return bucket_ == r.bucket_ && pos_ == r.pos_;
    }

    bool operator!=(const iterator& r) const {
      return !operator==(r);
    }

    reference operator*() {
      return owner_->At(bucket_)[pos_];
    }

    reference operator->() {
      return owner_->At(bucket_)[pos_];
    }

    bool HasExpiry() {
      return owner_->At(bucket_)[pos_].HasExpiry();
    }

    uint32_t ExpiryTime() {
      return owner_->At(bucket_)[pos_].GetExpiry();
    }

    uint32_t bucket_id() const {
      return bucket_;
    }

    operator bool() const {
      return owner_;
    }

    // Reallocates fragmented buffers in this bucket (inner entries + array buffer for
    // vectors). Returns true iff anything moved. Idempotent within a defrag pass.
    bool ReallocIfNeeded(PageUsage* page_usage) {
      auto bucket = owner_->At(bucket_);
      bool realloced = false;
      ssize_t delta = bucket.ReallocIfNeeded(page_usage, &realloced);
      // delta can be negative if a realloc lands in a smaller mimalloc usable-size
      // bucket; route the signed update through ssize_t to avoid size_t underflow.
      if (delta >= 0) {
        owner_->obj_alloc_used_ += static_cast<size_t>(delta);
      } else {
        const size_t shrink = static_cast<size_t>(-delta);
        assert(shrink <= owner_->obj_alloc_used_);
        owner_->obj_alloc_used_ -= shrink;
      }
      return realloced;
    }

    // find valid entry_ iterator starting from buckets_it_ and set it
    void SetEntryIt() {
      if (!owner_)
        return;
      // time_now_ == 0 disables expiry (callers set it to 0 around serialization).
      const uint32_t now = owner_->time_now_;
      for (auto num_entries = owner_->entries_.size(); bucket_ < num_entries; ++bucket_) {
        auto bucket = owner_->At(bucket_);
        for (uint32_t bucket_size = bucket.ElementsNum(); pos_ < bucket_size; ++pos_) {
          auto entry = bucket[pos_];
          if (!entry)
            continue;
          if (now != 0 && entry.HasExpiry() && entry.GetExpiry() <= now) {
            entry.ExpireIfNeeded(now, &owner_->size_, &owner_->obj_alloc_used_);
            continue;
          }
          return;
        }
        pos_ = 0;
      }
      owner_ = nullptr;
    }

   private:
    OAHTable* owner_ = nullptr;
    uint32_t bucket_ = 0;
    uint32_t pos_ = 0;
  };

  iterator begin() {
    iterator res(this, 0, 0);
    res.SetEntryIt();
    return res;
  }

  iterator end() {
    return iterator(nullptr, 0, 0);
  }

  static constexpr uint32_t kMaxBatchLen = 32;

  // Buckets hold one TaggedPtr control word per lane, so 4 fill a 32-byte AVX2 register.
  // The window is probed in kEntryLaneStep-lane strides, so kDisplacementSize must be a
  // multiple of the stride and <= 32 (masks fit a uint32_t). OAHEntry stays a thin
  // word-sized accessor over such a slot.
  static_assert(sizeof(Entry) == sizeof(TaggedPtr));
  static_assert(alignof(Entry) == alignof(TaggedPtr));
  static constexpr std::uint32_t kEntryLaneStep = 4;
  using EntryWide = SimdOp<uint64_t, kEntryLaneStep>;
  static_assert(kDisplacementSize % kEntryLaneStep == 0 && kDisplacementSize <= 32);

  // 2-lane SIMD for iterating the extension-point vector. Vector sizes are always
  // even with a minimum of 2 (see PtrVector::Grow), so a 2-lane
  // (16-byte SSE) load always stays within the heap allocation.
  static constexpr std::uint32_t kVectorLaneStep = 2;
  using VectorWide = SimdOp<uint64_t, kVectorLaneStep>;

  OAHTable() = default;

  // Buckets are TaggedPtr control words that own their blobs/vectors (freed by
  // ~OAHTable), so a shallow copy would double-free. Non-copyable, matching DenseSet.
  OAHTable(const OAHTable&) = delete;
  OAHTable& operator=(const OAHTable&) = delete;

  ~OAHTable() {
    FreeAllSlots();
  }

  void Reserve(size_t sz) {
    sz = absl::bit_ceil(sz);
    if (sz > entries_.size()) {
      capacity_log_ = std::max(kMinCapacityLog, uint32_t(absl::bit_width(sz) - 1));
      size_t prev_size = entries_.size();
      entries_.resize(Capacity());
      Rehash(prev_size);
    }
    assert(entries_.size() >= kDisplacementSize);
  }

  // TODO rewrite using extended hash approach
  //
  // Shrinks the table to new_size (power of 2, >= 1 << kMinCapacityLog and >= element count).
  void Shrink(size_t new_size) {
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

  void Clear() {
    FreeAllSlots();
    capacity_log_ = 0;
    entries_.resize(0);
    size_ = 0;
    obj_alloc_used_ = 0;
    ptr_vectors_alloc_used_ = 0;
    expiration_used_ = false;
  }

  // Incrementally clears [start, start+count). Returns the next bucket index; equals
  // Capacity() when the table is empty. Mirrors DenseSet::ClearStep (AsyncDeleter).
  uint32_t ClearStep(uint32_t start, uint32_t count) {
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

  /**
   * stable scanning api. has the same guarantees as redis scan command.
   * we avoid doing bit-reverse by using a different function to derive a bucket id
   * from hash values. By using msb part of hash we make it "stable" with respect to
   * rehashes. For example, with table log size 4 (size 16), entries in bucket id
   * 1110 come from hashes 1110XXXXX.... When a table grows to log size 5,
   * these entries can move either to 11100 or 11101. So if we traversed with our cursor
   * range [0000-1110], it's guaranteed that in grown table we do not need to cover again
   * [00000-11100]. Similarly with shrinkage, if a table is shrunk to log size 3,
   * keys from 1110 and 1111 will move to bucket 111. Again, it's guaranteed that we
   * covered the range [000-111] (all keys in that case).
   * Returns: next cursor or 0 if reached the end of scan.
   * cursor = 0 - initiates a new scan.
   */

  // The view is valid for the duration of the callback.
  using ItemCb = std::function<void(std::string_view)>;

  uint32_t Scan(uint32_t cursor, const ItemCb& cb) {
    if (entries_.empty())
      return 0;

    uint32_t bucket_id = cursor >> (32 - capacity_log_);
    const bool expire = expiration_used_;

    // First find the bucket to scan, skip empty buckets. Dispatch the per-entry expiry handling
    // once via the compile-time-specialized ScanHomeBucket (no-TTL sets skip it entirely).
    for (; bucket_id < BucketCount(); ++bucket_id) {
      const bool reported =
          expire ? ScanHomeBucket<true>(bucket_id, cb) : ScanHomeBucket<false>(bucket_id, cb);
      if (reported)
        break;
    }

    return ++bucket_id >= BucketCount() ? 0 : bucket_id << (32 - capacity_log_);
  }

  bool Erase(std::string_view str) {
    if (entries_.empty())
      return false;
    const ASCIIStr key(str);
    const uint64_t hash = Hash(key.content());
    const uint32_t bid = BucketId(hash, capacity_log_);
    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const LaneMasks masks = ProbeWindowShifted(&entries_[bid], ext_hash << oah::kExtHashShift);

    // Erase keeps the matched cell (to free it) and whether the hit lives in an extension vector.
    TaggedPtr* matched = nullptr;
    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      TaggedPtr* cell = &base[bid + std::countr_zero(cand_bits)];
      if (Entry(*cell).KeyMatches(key.content(), key.len())) {
        matched = cell;
        break;
      }
    }
    const uint32_t ext_bid = GetExtensionPoint(bid);
    bool in_vector = false;
    if (!matched && At(ext_bid).IsVector()) {
      matched = ProbeExtensionVector(ext_bid, key.content(), key.len(), ext_hash);
      in_vector = matched != nullptr;
    }
    if (!matched)
      return false;

    Entry victim(*matched);
    const bool removed = !IsExpired(victim);  // already-expired target => not-removed (like Redis)

    --size_;
    obj_alloc_used_ -= victim.AllocSize();
    Entry::Destroy(victim.Release());

    if (in_vector) {  // reclaim the vector if the erase emptied it
      OAHPtr<Entry> bucket = At(ext_bid);
      auto vec = bucket.AsVector();
      if (vec.Empty()) {
        ptr_vectors_alloc_used_ -= vec.AllocSize();
        bucket.Clear();
      }
    }
    return removed;
  }

  iterator Find(std::string_view member) {
    if (entries_.empty())
      return end();
    const ASCIIStr key(member);
    return Find(key.content(), key.len());
  }

  bool Contains(std::string_view member) {
    return Find(member) != end();
  }

  // Returns an allocation-free logical key: raw content is viewed in place and encoded content is
  // decoded into bounded inline storage.
  static oah::key::Decoded DecodeKey(Entry e) {
    return oah::key::Decode(e.StoredKey());
  }

  // Iterator to a uniformly random non-empty entry, or end() if empty (SPOP/SRANDMEMBER).
  iterator GetRandomMember() {
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

  // Returns the number of elements in the map. Note that it might be that some of these elements
  // have expired and can't be accessed.
  size_t UpperBoundSize() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

  std::uint32_t BucketCount() const {
    return entries_.empty() ? 0 : (1 << capacity_log_);
  }

  std::uint32_t Capacity() const {
    return (1 << capacity_log_) + kDisplacementSize - 1;
  }

  // set an abstract time that allows expiry.
  void set_time(uint32_t val) {
    time_now_ = val;
  }

  uint32_t time_now() const {
    return time_now_;
  }

  size_t ObjMallocUsed() const {
    return obj_alloc_used_;
  }

  size_t SetMallocUsed() const {
    return entries_.capacity() * sizeof(TaggedPtr) + ptr_vectors_alloc_used_;
  }

  bool ExpirationUsed() const {
    return expiration_used_;
  }

  size_t SizeSlow() {
    if (expiration_used_)
      CollectExpired();
    return size_;
  }

 protected:
  static uint64_t Hash(std::string_view str) {
    constexpr uint64_t kHashSeed = 24061983;
    return rapidhashMicro_withSeed(str.data(), str.size(), kHashSeed);
  }

  static uint32_t BucketId(uint64_t hash, uint32_t capacity_log) {
    return hash >> (64 - capacity_log);
  }

  // A non-owning OAHPtr over bucket slot `i`.
  OAHPtr<Entry> At(uint32_t i) {
    return OAHPtr<Entry>(entries_[i]);
  }

  // Frees the blob/vector of every non-empty bucket. Used by ~OAHTable and Clear().
  void FreeAllSlots() {
    for (size_t i = 0, n = entries_.size(); i < n; ++i) {
      if (entries_[i])
        At(i).Clear();
    }
  }

  // Reaps every expired entry so size_ becomes the live count. Nulls slots the same way the
  // iterator and Erase do, which is safe under fixed-window displacement probing.
  void CollectExpired() {
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

  // was Grow in StringSet
  void Rehash(uint32_t prev_size) {
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

  // Rehashes and re-inserts every entry of `slot`'s bucket into entries_, then frees it.
  void RedistributeBucket(TaggedPtr& slot) {
    OAHPtr<Entry> bucket(slot);
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

  // it is inefficient for now,
  // TODO predict new position by current position and extended hash
  void ShrinkBucket(uint32_t bucket_id) {
    // Detach the slot bits into a local; `bucket` views the local and is freed
    // explicitly below (At(new_bucket_id) writes into entries_, never this local).
    TaggedPtr slot = entries_[bucket_id];
    entries_[bucket_id] = 0;
    OAHPtr<Entry> bucket(slot);
    if (bucket.Empty())
      return;

    for (uint32_t pos = 0, size = bucket.ElementsNum(); pos < size; ++pos) {
      Entry entry = bucket[pos];
      if (!entry)
        continue;
      // Drop entries whose TTL has passed instead of rehashing them (no-TTL sets skip the check).
      if (expiration_used_ && IsExpired(entry)) {
        obj_alloc_used_ -= entry.AllocSize();
        --size_;
        continue;
      }
      uint32_t new_bucket_id = FindEmptyAround(RehashEntry(entry));
      ptr_vectors_alloc_used_ += At(new_bucket_id).Insert(bucket.Remove(pos));
    }

    if (bucket.IsVector()) {
      ptr_vectors_alloc_used_ -= bucket.AsVector().AllocSize();
    }
    // Frees the (now drained) collision array and any expired entries left behind.
    bucket.Clear();
  }

  static uint32_t GetExtensionPoint(uint32_t bid) {
    constexpr uint32_t extension_point_shift = kDisplacementSize - 1;
    return bid | extension_point_shift;
  }

  uint32_t EntryTTL(uint32_t ttl_sec) const {
    return ttl_sec == UINT32_MAX ? ttl_sec : time_now_ + ttl_sec;
  }

  // First empty slot in the window [bid, bid+kDisplacementSize), or the extension
  // point if full. SIMD over EntryWide strides.
  uint32_t FindEmptyAround(uint32_t bid) {
    // Strides scanned in order, so the first empty found is the lowest-index one. In
    // bounds thanks to entries_' kDisplacementSize-1 slack past BucketCount.
    for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
      const EntryWide data = EntryWide::Load(&entries_[bid + off]);
      if (uint32_t empties = (data == uint64_t(0)).GetMSBs())
        return bid + off + std::countr_zero(empties);
    }
    // TODO add expiration logic
    const uint32_t ext = GetExtensionPoint(bid);
    assert(ext < entries_.size());
    return ext;
  }

  // Returns an iterator to a live entry in bucket `b` (skipping just-expired ones for
  // single entries and walking vector entries in order), or end() if none remain.
  iterator PickFromBucket(uint32_t b) {
    OAHPtr<Entry> bucket = At(b);
    if (!bucket.IsVector()) {
      Entry e = bucket[0];
      ExpireIfNeeded(e);
      return e.Empty() ? end() : iterator{this, b, 0};
    }
    auto vec = bucket.AsVector();
    for (uint32_t pos = 0, vec_size = vec.Size(); pos < vec_size; ++pos) {
      Entry entry(vec[pos]);
      if (!entry)
        continue;
      ExpireIfNeeded(entry);
      if (entry)
        return iterator{this, b, pos};
    }
    return end();
  }

  // Linear [lo, hi) scan returning the first live entry or end(). SIMD strides over
  // EntryWide::kLanes plus a scalar tail for the < kLanes trailing buckets. Kept
  // out-of-line so GetRandomMember can call it twice (hot range + rare wrap) without
  // duplicating the body in the cold path.
  iterator ScanRange(uint32_t lo, uint32_t hi) {
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

  // Result of a SIMD probe over a run of OAHEntry lanes.
  struct LaneMasks {
    uint32_t candidates;  // non-empty lanes whose stored ext-hash equals the query ext-hash
    uint32_t empties;     // empty lanes (data_ == 0); Add picks a slot from these
  };

  // Vectorized hash probe over Wide::kLanes consecutive lanes from `base`. `shifted_ext_hash` is
  // the query pre-shifted to bits [kExtHashShift, 64) so the stored hash is compared in place.
  template <typename Wide>
  static LaneMasks ProbeLanes(const TaggedPtr* base, uint64_t shifted_ext_hash) noexcept {
    assert(shifted_ext_hash != 0u);  // never 0 (CalcExtHash remap), so empty lanes can't match
    auto data_v = Wide::Load(base);
    // Mask covers the vector bit so vector slots never match a candidate (window probes then need
    // no per-candidate IsVector test); ext-hash is never 0, so empty lanes can't match the query.
    auto stored = data_v & Wide::Fill(oah::kExtHashShiftedMask | oah::kVectorBit);
    auto is_empty = data_v == uint64_t(0);
    auto candidate = stored == shifted_ext_hash;
    return {candidate.GetMSBs(), is_empty.GetMSBs()};
  }

  // Combined candidate/empty masks over the window (lane i -> bit i). Takes the already-shifted
  // fingerprint; callers pass `ext_hash << kExtHashShift` (insertion reuses the shifted value).
  LaneMasks ProbeWindowShifted(const TaggedPtr* base, uint64_t shifted_ext_hash) noexcept {
    LaneMasks w{0, 0};
    for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
      const LaneMasks m = ProbeLanes<EntryWide>(base + off, shifted_ext_hash);
      w.candidates |= m.candidates << off;
      w.empties |= m.empties << off;
    }
    return w;
  }

  // Returns lanes in a scan window holding single entries affiliated with the
  // home bucket, and separately reports vector lanes.
  static uint32_t ScanWindowMask(const TaggedPtr* base, uint64_t target, uint32_t shift,
                                 uint32_t* vector_mask_out) noexcept {
    uint32_t cand = 0;
    uint32_t vec = 0;
    for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes) {
      const EntryWide data = EntryWide::Load(base + off);
      const uint32_t isvec =
          ((data & EntryWide::Fill(oah::kVectorBit)) == oah::kVectorBit).GetMSBs();
      const uint32_t matched = ((data >> shift) == target).GetMSBs();
      // target == 0 also matches all-zero empties; drop them here (cheaper than a scalar re-read).
      const uint32_t is_empty = (data == uint64_t(0)).GetMSBs();
      cand |= (matched & ~is_empty & ~isvec) << off;
      vec |= isvec << off;
    }
    *vector_mask_out = vec;
    return cand;
  }

  // Lanes affiliated with the home bucket (top `part` bits of ext-hash == target) and
  // non-empty. Used for the extension-point vector (no vector-bit lanes to exclude).
  template <typename Wide>
  static uint32_t AffiliationMask(const TaggedPtr* base, uint64_t target, uint32_t shift) noexcept {
    const Wide data = Wide::Load(base);
    // Vector arrays may hold empty slots; exclude them so target == 0 doesn't report holes.
    const uint32_t matched = ((data >> shift) == target).GetMSBs();
    const uint32_t is_empty = (data == uint64_t(0)).GetMSBs();
    return matched & ~is_empty;
  }

  // Scans one stable-SCAN home bucket window and reports live affiliated entries. Templated on
  // Expire: no-TTL sets skip the per-entry lazy expiration and the post-expiry empty re-check.
  template <bool Expire> bool ScanHomeBucket(uint32_t bucket_id, const ItemCb& cb) {
    const uint32_t part = std::min(capacity_log_, kShiftLog);
    assert(part > 0u);
    // ScanWindowMask drops empty lanes, so `cand` holds only affiliated non-empty single entries.
    const uint32_t shift = 64 - part;
    const uint64_t target = bucket_id & ((uint64_t{1} << part) - 1);

    const TaggedPtr* base = &entries_[bucket_id];
    // Prefetch every window slot's blob to overlap the loads with the SIMD mask below (an empty
    // slot prefetches null, a no-op).
    for (uint32_t i = 0; i < kDisplacementSize; ++i)
      oah::PrefetchRead(reinterpret_cast<const char*>(base[i] & ~oah::kTagMask));

    uint32_t vec_mask = 0;
    uint32_t cand = ScanWindowMask(base, target, shift, &vec_mask);
    bool reported = false;

    while (cand) {
      const uint32_t i = std::countr_zero(cand);
      cand &= cand - 1;
      Entry e = At(bucket_id + i)[0];
      if constexpr (Expire) {
        ExpireIfNeeded(e);
        if (e.Empty())
          continue;
      }
      const oah::key::Decoded key = DecodeKey(e);
      cb(key.view());
      reported = true;
    }

    if (vec_mask) {
      assert((vec_mask & (vec_mask - 1)) == 0u);
      const uint32_t vi = std::countr_zero(vec_mask);
      auto vec = At(bucket_id + vi).AsVector();
      TaggedPtr* raw = vec.Raw();
      const size_t vsize = vec.Size();
      for (size_t b = 0; b < vsize; b += VectorWide::kLanes) {
        uint32_t m = AffiliationMask<VectorWide>(&raw[b], target, shift);
        while (m) {
          const uint32_t j = std::countr_zero(m);
          m &= m - 1;
          Entry el(raw[b + j]);
          if constexpr (Expire) {
            ExpireIfNeeded(el);
            if (el.Empty())
              continue;
          }
          const oah::key::Decoded key = DecodeKey(el);
          cb(key.view());
          reported = true;
        }
      }
    }

    return reported;
  }

  // Searches the extension-point vector for the query key. Returns the matched slot, or nullptr.
  // Does not expire, so the returned entry may be live or already-expired.
  TaggedPtr* ProbeExtensionVector(uint32_t ext_bid, std::string_view content, uint32_t len,
                                  uint64_t ext_hash) {
    auto vec = At(ext_bid).AsVector();
    TaggedPtr* raw_arr = vec.Raw();
    const size_t size = vec.Size();
    assert(size >= size_t(kVectorLaneStep));
    assert(size % kVectorLaneStep == 0u);

    const uint64_t shifted_ext_hash = ext_hash << oah::kExtHashShift;
    for (size_t base = 0; base < size; base += kVectorLaneStep) {
      auto cand_bits = ProbeLanes<VectorWide>(reinterpret_cast<const uint64_t*>(&raw_arr[base]),
                                              shifted_ext_hash)
                           .candidates;
      while (cand_bits) {
        const uint32_t j = std::countr_zero(cand_bits);
        cand_bits &= cand_bits - 1;
        if (Entry(raw_arr[base + j]).KeyMatches(content, len))
          return &raw_arr[base + j];
      }
    }
    return nullptr;
  }

  // Probes for an already-encoded query key (content bytes + logical length); assumes entries_ is
  // non-empty. Overload so OAHMap's insert path doesn't re-encode.
  iterator Find(std::string_view content, uint32_t len) {
    const uint64_t hash = Hash(content);
    const uint32_t bid = BucketId(hash, capacity_log_);
    return expiration_used_ ? FindInternal<true>(bid, content, len, hash)
                            : FindInternal<false>(bid, content, len, hash);
  }

  // Probes for the query key; returns an iterator to the live entry or end(). Templated on Expire:
  // with no TTLs a key match is provably live, so the expiry re-check is compiled out.
  template <bool Expire>
  iterator FindInternal(uint32_t bid, std::string_view content, uint32_t len, uint64_t hash) {
    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const LaneMasks masks = ProbeWindowShifted(&entries_[bid], ext_hash << oah::kExtHashShift);

    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      const uint32_t bucket_id = bid + std::countr_zero(cand_bits);
      Entry e(base[bucket_id]);
      if (e.KeyMatches(content, len)) {
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
      if (TaggedPtr* hit = ProbeExtensionVector(ext_bid, content, len, ext_hash)) {
        if constexpr (Expire) {
          Entry e(*hit);
          ExpireIfNeeded(e);
          if (e.Empty())
            return end();
        }
        return iterator{this, ext_bid, static_cast<uint32_t>(hit - At(ext_bid).AsVector().Raw())};
      }
    }
    return end();
  }

  static uint64_t CalcExtHash(uint64_t hash, uint32_t capacity_log) {
    const uint32_t start_hash_bit = capacity_log > kShiftLog ? capacity_log - kShiftLog : 0;
    const uint32_t ext_hash_shift = 64 - start_hash_bit - oah::kExtHashSize;
    const uint64_t h = (hash >> ext_hash_shift) & oah::kExtHashMask;
    // Remap the rare all-zero fingerprint to 1 so empty (all-zero) slots never match a real
    // entry, letting the SIMD probes drop the empty mask. Full entropy otherwise; home-prefix
    // bits stay 0.
    return h ? h : 1;
  }

  // Recomputes the entry's hash, refreshes its stored ext-hash, and returns its new bucket.
  uint32_t RehashEntry(Entry entry) {
    uint64_t hash = Hash(entry.KeyContent());
    entry.SetExtHash(CalcExtHash(hash, capacity_log_));
    return BucketId(hash, capacity_log_);
  }

  // Lazily expires `entry` when it carries a live TTL. Templated on Expire so no-TTL callers
  // select the <false> instantiation, whose body compiles to nothing.
  template <bool Expire> void ExpireIfNeeded(Entry entry) {
    if constexpr (Expire)
      entry.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
  }

  // Runtime dispatch on expiration_used_: no-TTL sets run no expiry code at all.
  void ExpireIfNeeded(Entry entry) {
    expiration_used_ ? ExpireIfNeeded<true>(entry) : ExpireIfNeeded<false>(entry);
  }

  // True when `entry`'s TTL has elapsed (HasExpiry() already covers the no-TTL case).
  bool IsExpired(Entry entry) const {
    return entry.HasExpiry() && entry.GetExpiry() <= time_now_;
  }

  mutable size_t obj_alloc_used_ = 0;
  mutable size_t ptr_vectors_alloc_used_ = 0;

  std::uint32_t capacity_log_ = 0;
  std::uint32_t size_ = 0;  // number of elements in the set.
  std::uint32_t time_now_ = 0;
  bool expiration_used_ = false;
  Buckets entries_;
};

}  // namespace dfly
