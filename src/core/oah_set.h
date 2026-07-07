// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/numeric/bits.h>
#include <absl/random/random.h>
#include <absl/types/span.h>

#include <bit>
#include <concepts>
#include <vector>

#include "common/rapidhash.h"
#include "core/detail/stateless_allocator.h"
#include "core/oah_ptr.h"
#include "core/simd_op.h"
#include "core/string_set.h"

namespace dfly {

// oah_set.h - an open-addressing hash set of string members (the OAHSet container).
//
// OAHSet stores members in a flat array of TaggedPtr buckets. Each bucket is wrapped by a
// non-owning OAHPtr holding either a single OAHEntry or a PtrVector collision chain. Lookups probe
// a small SIMD window around the home bucket and spill overflow into an extension-point vector.
// Buckets own their blobs/vectors and are freed explicitly (a TaggedPtr has no destructor).
//
// TODO add template parameter instead of OAHEntry
class OAHSet {  // Open Addressing Hash Set
  using Buckets = std::vector<TaggedPtr, StatelessAllocator<TaggedPtr>>;

 public:
  static constexpr std::uint32_t kShiftLog = 2;                         // TODO make template
  static constexpr std::uint32_t kMinCapacityLog = kShiftLog;           // should be >= ShiftLog
  static constexpr std::uint32_t kDisplacementSize = (1 << kShiftLog);  // TODO check

  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = OAHEntry;
    using pointer = OAHEntry;
    using reference = OAHEntry;

    iterator(OAHSet* owner, uint32_t bucket_id, uint32_t pos_in_bucket)
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
    OAHSet* owner_ = nullptr;
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
  static_assert(sizeof(OAHEntry) == sizeof(TaggedPtr));
  static_assert(alignof(OAHEntry) == alignof(TaggedPtr));
  static constexpr std::uint32_t kEntryLaneStep = 4;
  using EntryWide = SimdOp<uint64_t, kEntryLaneStep>;
  static_assert(kDisplacementSize % kEntryLaneStep == 0 && kDisplacementSize <= 32);

  // 2-lane SIMD for iterating the extension-point vector. Vector sizes are always
  // even with a minimum of 2 (see OAHEntry::Insert / PtrVector::Grow), so a 2-lane
  // (16-byte SSE) load always stays within the heap allocation.
  static constexpr std::uint32_t kVectorLaneStep = 2;
  using VectorWide = SimdOp<uint64_t, kVectorLaneStep>;

  explicit OAHSet() = default;

  // Buckets are TaggedPtr control words that own their blobs/vectors (freed by
  // ~OAHSet), so a shallow copy would double-free. Non-copyable, matching DenseSet.
  OAHSet(const OAHSet&) = delete;
  OAHSet& operator=(const OAHSet&) = delete;

  ~OAHSet() {
    FreeAllSlots();
  }

  // Inserts `str` (optional TTL); returns false if already present.
  bool Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX);

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
  void Shrink(size_t new_size);

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
    // Match Clear() semantics: once incrementally cleared empty, the TTL flag is stale.
    if (size_ == 0)
      expiration_used_ = false;
    return end;
  }

  // keepttl=true: existing entries are left alone (current/legacy behavior).
  // keepttl=false: when ttl_sec is set, existing entries' expiry is updated to ttl_sec.
  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec = UINT32_MAX,
                   bool keepttl = true);

  // TODO: Consider using chunks for this as in StringSet
  void Fill(OAHSet* other) {
    assert(other->entries_.empty());
    other->Reserve(UpperBoundSize());
    other->set_time(time_now());
    for (auto it = begin(), it_end = end(); it != it_end; ++it) {
      other->Add(it->Key(), it.HasExpiry() ? it.ExpiryTime() - time_now() : UINT32_MAX);
    }
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

  using ItemCb = std::function<void(std::string_view)>;

  uint32_t Scan(uint32_t cursor, const ItemCb& cb) {
    if (entries_.empty())
      return 0;

    uint32_t bucket_id = cursor >> (32 - capacity_log_);

    // First find the bucket to scan, skip empty buckets.
    for (; bucket_id < BucketCount(); ++bucket_id) {
      bool res = false;
      for (uint32_t i = 0; i < kDisplacementSize; i++) {
        const uint32_t shifted_bid = bucket_id + i;
        res |= ScanBucket(At(shifted_bid), cb, bucket_id);
      }
      if (res)
        break;
    }

    if (++bucket_id >= BucketCount()) {
      return 0;
    }

    return bucket_id << (32 - capacity_log_);
  }

  bool Erase(std::string_view str);

  iterator Find(std::string_view member);

  bool Contains(std::string_view member) {
    return Find(member) != end();
  }

  // Iterator to a uniformly random non-empty entry, or end() if empty (SPOP/SRANDMEMBER).
  iterator GetRandomMember();

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
    // TODO
    assert(false);
    // CollectExpired();
    return size_;
  }

 private:
  static uint64_t Hash(std::string_view str) {
    constexpr uint64_t kHashSeed = 24061983;
    return rapidhashMicro_withSeed(str.data(), str.size(), kHashSeed);
  }

  static uint32_t BucketId(uint64_t hash, uint32_t capacity_log) {
    return hash >> (64 - capacity_log);
  }

  // A non-owning OAHPtr over bucket slot `i`.
  OAHPtr At(uint32_t i) {
    return OAHPtr(entries_[i]);
  }

  // Frees the blob/vector of every non-empty bucket. Used by ~OAHSet and Clear().
  void FreeAllSlots() {
    for (size_t i = 0, n = entries_.size(); i < n; ++i) {
      if (entries_[i])
        At(i).Clear();
    }
  }

  // was Grow in StringSet
  void Rehash(uint32_t prev_size);

  // Rehashes and re-inserts every entry of `slot`'s bucket into entries_, then frees it.
  void RedistributeBucket(TaggedPtr& slot);

  // it is inefficient for now,
  // TODO predict new position by current position and extended hash
  void ShrinkBucket(uint32_t bucket_id) {
    // Detach the slot bits into a local; `bucket` views the local and is freed
    // explicitly below (At(new_bucket_id) writes into entries_, never this local).
    TaggedPtr slot = entries_[bucket_id];
    entries_[bucket_id] = 0;
    OAHPtr bucket(slot);
    if (bucket.Empty())
      return;

    for (uint32_t pos = 0, size = bucket.ElementsNum(); pos < size; ++pos) {
      if (bucket[pos]) {
        // Check for TTL expiration during shrink - skip expired elements
        if (bucket[pos].HasExpiry() && bucket[pos].GetExpiry() <= time_now_) {
          obj_alloc_used_ -= bucket[pos].AllocSize();
          --size_;
          continue;
        }

        uint32_t new_bucket_id = FindEmptyAround(RehashEntry(bucket[pos]));
        ptr_vectors_alloc_used_ += At(new_bucket_id).Insert(bucket.Remove(pos));
      }
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

  template <std::invocable<std::string_view> T>
  bool ScanBucket(OAHPtr entry, const T& cb, uint32_t bucket_id) {
    if (!entry.IsVector()) {
      OAHEntry e = entry[0];
      e.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
      if (CheckBucketAffiliation(e, bucket_id)) {
        cb(e.Key());
        return true;
      }
    } else {
      auto arr = entry.AsVector();
      bool result = false;
      for (TaggedPtr& cell : arr) {
        OAHEntry el(cell);
        el.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
        if (CheckBucketAffiliation(el, bucket_id)) {
          cb(el.Key());
          result = true;
        }
      }
      return result;
    }
    return false;
  }

  uint32_t EntryTTL(uint32_t ttl_sec) const {
    return ttl_sec == UINT32_MAX ? ttl_sec : time_now_ + ttl_sec;
  }

  // First empty slot in the window [bid, bid+kDisplacementSize), or the extension
  // point if full. SIMD over EntryWide strides.
  uint32_t FindEmptyAround(uint32_t bid);

  // Returns an iterator to a live entry in bucket `b` (skipping just-expired ones for
  // single entries and walking vector entries in order), or end() if none remain.
  // FORCE_INLINE: it sits inside ScanRange's hot SIMD inner loop.
  iterator PickFromBucket(uint32_t b);

  // Linear [lo, hi) scan returning the first live entry or end(). SIMD strides over
  // EntryWide::kLanes plus a scalar tail for the < kLanes trailing buckets. Kept
  // out-of-line so GetRandomMember can call it twice (hot range + rare wrap) without
  // duplicating the body in the cold path.
  iterator ScanRange(uint32_t lo, uint32_t hi);

  // The body of Add, FORCE_INLINE so it folds into Add and AddMany.
  bool AddImpl(std::string_view str, uint32_t ttl_sec);

  // Result of a SIMD probe over a run of OAHEntry lanes.
  struct LaneMasks {
    uint32_t candidates;  // non-empty lanes whose stored ext-hash equals the query ext-hash
    uint32_t empties;     // empty lanes (data_ == 0); Add picks a slot from these
  };

  // Vectorized hash probe over Wide::kLanes consecutive lanes from `base`. Backs
  // the window (EntryWide) and extension-vector (VectorWide) scans.
  template <typename Wide>
  static LaneMasks ProbeLanes(const TaggedPtr* base, uint64_t ext_hash) noexcept;

  // Combined candidate/empty masks over the whole window (lane i -> bit i).
  LaneMasks ProbeWindow(const TaggedPtr* base, uint64_t ext_hash) noexcept;

  // Searches the extension-point vector for `str`. Returns the matched slot
  // (possibly now-empty after expiry, which the caller reuses) or nullptr.
  TaggedPtr* ProbeExtensionVector(uint32_t ext_bid, std::string_view str, uint64_t ext_hash);

  // Outcome of a key probe. A raw slot (not an iterator) so the caller can reuse a
  // matched-but-just-expired entry, which is Empty() and would trip operator[]'s assert.
  struct MatchResult {
    TaggedPtr* matched;   // ptr to matched cell, or null if absent; may be 0 (just expired)
    uint32_t bucket_id;   // location of `matched`, for building an iterator
    uint32_t pos_in_vec;  // position within a vector bucket (0 for single entries)
  };

  // Shared core of AddImpl and FindInternal: scans the window (cand_bits from a
  // prior ProbeLanes) then the extension vector for `str`.
  MatchResult FindMatch(uint32_t bid, uint32_t ext_bid, uint32_t cand_bits, std::string_view str,
                        uint64_t ext_hash);

  // Probes for `str`; returns an iterator to the live entry or end(). Shared by
  // Find and Erase.
  iterator FindInternal(uint32_t bid, std::string_view str, uint64_t hash);

  static uint64_t CalcExtHash(uint64_t hash, uint32_t capacity_log) {
    const uint32_t start_hash_bit = capacity_log > kShiftLog ? capacity_log - kShiftLog : 0;
    const uint32_t ext_hash_shift = 64 - start_hash_bit - OAHEntry::kExtHashSize;
    return (hash >> ext_hash_shift) & OAHEntry::kExtHashMask;
  }

  bool CheckBucketAffiliation(OAHEntry entry, uint32_t bucket_id) {
    if (entry.Empty())
      return false;
    uint32_t bucket_id_hash_part = capacity_log_ > kShiftLog ? kShiftLog : capacity_log_;
    uint32_t bucket_mask = (1 << bucket_id_hash_part) - 1;
    bucket_id &= bucket_mask;
    uint32_t stored_bucket_id = entry.GetHash() >> (OAHEntry::kExtHashSize - bucket_id_hash_part);
    return bucket_id == stored_bucket_id;
  }

  // Recomputes the entry's hash, refreshes its stored ext-hash, and returns its new bucket.
  uint32_t RehashEntry(OAHEntry entry) {
    uint64_t hash = Hash(entry.Key());
    entry.SetExtHash(CalcExtHash(hash, capacity_log_));
    return BucketId(hash, capacity_log_);
  }

  mutable size_t obj_alloc_used_ = 0;
  mutable size_t ptr_vectors_alloc_used_ = 0;

  std::uint32_t capacity_log_ = 0;
  std::uint32_t size_ = 0;  // number of elements in the set.
  std::uint32_t time_now_ = 0;
  bool expiration_used_ = false;
  Buckets entries_;
};

// Snapshot of --use_oah_set captured once at startup.
inline bool g_use_oah_set = false;

// Dispatches a generic lambda over the runtime-selected set type (StringSet or
// OAHSet) backing kEncodingStrMap2 SETs; both expose the same surface.
template <typename Fn> auto VisitSet(void* ptr, Fn&& fn) {
  return g_use_oah_set ? fn(static_cast<OAHSet*>(ptr)) : fn(static_cast<StringSet*>(ptr));
}

// Current member as a string_view from either iterator type. Free functions so
// generic code (e.g. VisitSet lambdas) can write `Key(it)` uniformly.
inline std::string_view Key(StringSet::iterator it) {
  sds s = *it;
  return {s, sdslen(s)};
}

inline std::string_view Key(OAHSet::iterator it) {
  return it->Key();
}

}  // namespace dfly
