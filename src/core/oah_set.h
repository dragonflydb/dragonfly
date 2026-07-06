// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/numeric/bits.h>
#include <absl/random/random.h>
#include <absl/types/span.h>

#include <array>
#include <bit>
#include <cassert>
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
      // During serialization callers set time to 0; real expiries are always > 0, so
      // GetExpiry() <= 0 never matches and nothing is expired (as in DenseSet). HasExpiry()
      // short-circuits the no-TTL case before the GetExpiry() read.
      const uint32_t now = owner_->time_now_;
      for (auto num_entries = owner_->entries_.size(); bucket_ < num_entries; ++bucket_) {
        auto bucket = owner_->At(bucket_);
        for (uint32_t bucket_size = bucket.ElementsNum(); pos_ < bucket_size; ++pos_) {
          auto entry = bucket[pos_];
          if (!entry)
            continue;
          if (entry.HasExpiry() && entry.GetExpiry() <= now) {
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

  // How many entries ahead Scan/Shrink/Rehash prefetch key blobs. Sized to hide a memory
  // load behind a few entries of hashing work.
  static constexpr std::uint32_t kScanVecLookahead = 2;
  static constexpr std::uint32_t kIterBlobLookahead = 8;

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
      // Prime the window's key blobs: ScanBucket hashes each entry's key, so the scattered
      // blobs are the miss source. SIMD-mask the whole kDisplacementSize-slot window, then
      // prefetch.
      PrefetchWindow<kDisplacementSize>(&entries_[bucket_id]);

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

  uint32_t BucketId(uint64_t hash) const {
    return hash >> (64 - capacity_log_);
  }

  // A non-owning OAHPtr over bucket slot `i`.
  OAHPtr At(uint32_t i) {
    return OAHPtr(entries_[i]);
  }

  // Prefetches the key blob / vector backing array a bucket word points at. Used by
  // Shrink/Rehash, which read scattered key blobs (to hash the key) and would otherwise stall
  // on a cache miss per entry. Branch-free: an empty word (0) masks to a null address, and
  // prefetching an invalid/null address is a harmless no-op, so no `if` on emptiness is needed.
  static FORCE_INLINE void PrefetchTaggedRaw(TaggedPtr tp) {
    PREFETCH_READ(reinterpret_cast<const void*>(tp & ~OAHEntry::kTagMask));
  }

  // Prefetches the blobs of a compile-time-sized window base[0..N). The tag bits are stripped
  // from all N words with SIMD (EntryWide) AND, then the (necessarily scalar -- x86 has no
  // vector prefetch) prefetches are issued branch-free: an empty slot masks to 0 and a
  // prefetch of a null/invalid address is a harmless no-op, so no per-slot test is needed.
  template <uint32_t N> FORCE_INLINE void PrefetchWindow(const TaggedPtr* base) {
    static_assert(N % EntryWide::kLanes == 0);
    const EntryWide mask = EntryWide::Fill(~OAHEntry::kTagMask);
    alignas(EntryWide) TaggedPtr addr[N];
    for (uint32_t off = 0; off < N; off += EntryWide::kLanes)
      (EntryWide::Load(base + off) & mask).Store(addr + off);
    for (uint32_t i = 0; i < N; ++i)
      PREFETCH_READ(reinterpret_cast<const void*>(addr[i]));
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
      OAHEntry entry = bucket[pos];
      if (!entry)
        continue;
      // Drop entries whose TTL has passed instead of rehashing them.
      if (IsExpired(entry)) {
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

  // Emits `e` via `cb` iff its home bucket is `bucket_id`. Decodes the key once and reuses
  // it for both the affiliation hash and the callback (CheckBucketAffiliation would re-decode).
  template <std::invocable<std::string_view> T>
  bool EmitIfAffiliated(OAHEntry e, const T& cb, uint32_t bucket_id) {
    if (e.Empty())
      return false;
    const std::string_view key = e.Key();
    if (BucketId(Hash(key)) != bucket_id)
      return false;
    cb(key);
    return true;
  }

  template <std::invocable<std::string_view> T>
  bool ScanBucket(OAHPtr entry, const T& cb, uint32_t bucket_id) {
    if (!entry.IsVector()) {
      OAHEntry e = entry[0];
      ExpireIfNeeded(e);
      return EmitIfAffiliated(e, cb, bucket_id);
    }
    auto arr = entry.AsVector();
    TaggedPtr* raw = arr.Raw();
    const uint32_t size = arr.Size();
    for (uint32_t i = 0; i < std::min(size, kScanVecLookahead); ++i)
      PrefetchTaggedRaw(raw[i]);
    bool result = false;
    for (uint32_t pos = 0; pos < size; ++pos) {
      if (pos + kScanVecLookahead < size)
        PrefetchTaggedRaw(raw[pos + kScanVecLookahead]);
      OAHEntry el(raw[pos]);
      ExpireIfNeeded(el);
      result |= EmitIfAffiliated(el, cb, bucket_id);
    }
    return result;
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

  // Lazily expires `entry` (deleting it and updating size_/obj_alloc_used_) when it carries
  // a live TTL. Templated on Expire so sets with no TTLs select the <false> instantiation,
  // whose `if constexpr` body compiles to nothing.
  template <bool Expire> FORCE_INLINE void ExpireIfNeeded(OAHEntry entry) {
    if constexpr (Expire)
      entry.ExpireIfNeeded(time_now_, &size_, &obj_alloc_used_);
  }

  // Runtime-dispatches to the compile-time-specialized worker above: the <false> branch is
  // free of expiry code, so no-TTL sets pay only a well-predicted branch on expiration_used_.
  FORCE_INLINE void ExpireIfNeeded(OAHEntry entry) {
    expiration_used_ ? ExpireIfNeeded<true>(entry) : ExpireIfNeeded<false>(entry);
  }

  // True when `entry`'s TTL has already elapsed. HasExpiry() is false for entries without a TTL,
  // so it already covers the no-TTL case -- a separate expiration_used_ guard would be redundant.
  FORCE_INLINE bool IsExpired(OAHEntry entry) const {
    return entry.HasExpiry() && entry.GetExpiry() <= time_now_;
  }

  // Vectorized hash probe over Wide::kLanes consecutive lanes from `base`, returning a
  // candidate bitmask (lane i -> bit i). Backs the window (EntryWide) and extension-vector
  // (VectorWide) scans. `shifted_ext_hash` is the query ext-hash pre-shifted to bits
  // [kExtHashShift, 64) so the stored hash is compared in place (no per-lane down-shift).
  // Fingerprints are never 0 (CalcExtHash), so empty lanes need no exclusion.
  template <typename Wide>
  static uint32_t ProbeLanes(const TaggedPtr* base, uint64_t shifted_ext_hash) noexcept;

  // Candidate bitmask over the whole window (lane i -> bit i).
  uint32_t ProbeWindow(const TaggedPtr* base, uint64_t ext_hash) noexcept;

  // Searches the extension-point vector for `str`. Returns the matched slot
  // (possibly now-empty after expiry, which the caller reuses) or nullptr. Expire gates the
  // per-candidate lazy expiration (see FindMatch).
  template <bool Expire = true>
  TaggedPtr* ProbeExtensionVector(uint32_t ext_bid, std::string_view str, uint64_t ext_hash);

  // Outcome of a key probe. A raw slot (not an iterator) so the caller can reuse a
  // matched-but-just-expired entry, which is Empty() and would trip operator[]'s assert.
  struct MatchResult {
    TaggedPtr* matched;   // ptr to matched cell, or null if absent; may be 0 (just expired)
    uint32_t bucket_id;   // location of `matched`, for building an iterator
    uint32_t pos_in_vec;  // position within a vector bucket (0 for single entries)
  };

  // Shared core of AddImpl, FindInternal and Erase: scans the window (cand_bits from a prior
  // ProbeLanes) then the extension vector for `str`. Expire (default true) gates the per-candidate
  // lazy expiration: Find/Add reap expired entries they pass over, but Erase (Expire=false) skips
  // that -- it won't revisit the set, so reaping bystanders is wasted work. With Expire=false a
  // returned `matched` is always a live-looking cell (never a reaped 0).
  template <bool Expire = true>
  MatchResult FindMatch(uint32_t bid, uint32_t ext_bid, uint32_t cand_bits, std::string_view str,
                        uint64_t ext_hash);

  // Probes for `str`; returns an iterator to the live entry or end(). Used by Find.
  iterator FindInternal(uint32_t bid, std::string_view str, uint64_t hash);

  // The stored 12-bit fingerprint is a fixed low slice of the hash, independent of
  // capacity_log so it never changes across grow/shrink (RehashEntry needn't rewrite it).
  // Mapping the all-zero fingerprint to 1 keeps stored fingerprints nonzero, so an empty
  // (all-zero) bucket can never match a query in the SIMD probe and ProbeLanes skips
  // empty-lane exclusion. Cost: fingerprints 0 and 1 collide (a negligible bump in the
  // ~1/4096 false-candidate rate).
  static uint64_t CalcExtHash(uint64_t hash) {
    const uint64_t h = hash & OAHEntry::kExtHashMask;
    return h | (h == 0);
  }

  // Recomputes the entry's hash and returns its new bucket. The stored fingerprint is
  // capacity-independent, so grow/shrink leave it valid -- no SetExtHash needed here.
  uint32_t RehashEntry(OAHEntry entry) {
    return BucketId(Hash(entry.Key()));
  }

  mutable size_t obj_alloc_used_ = 0;
  mutable size_t ptr_vectors_alloc_used_ = 0;

  std::uint32_t capacity_log_ = 0;
  std::uint32_t size_ = 0;  // number of elements in the set.
  std::uint32_t time_now_ = 0;
  bool expiration_used_ = false;
  Buckets entries_;
};

// Out-of-line member definitions, kept header-inline so hot ops (Find/Add/Erase) can inline
// into callers across translation units. Order matches call dependencies so each always_inline
// (FORCE_INLINE) callee is defined before its callers.

template <typename Wide>
inline FORCE_INLINE uint32_t OAHSet::ProbeLanes(const TaggedPtr* base,
                                                uint64_t shifted_ext_hash) noexcept {
  auto data_v = Wide::Load(base);
  // Compare the stored ext-hash in place (bits [kExtHashShift, 64)) against the query, which
  // the caller pre-shifted to the same position (no per-lane down-shift). The mask also covers
  // the vector bit (bit 0): a vector slot sets it while an entry and the query never do, so a
  // vector can never match a query here -- the window scan needs no per-candidate IsVector test.
  // Stored fingerprints are never 0 (CalcExtHash maps 0->1), so empty (all-zero) lanes can never
  // match a nonzero query either -- no empty-lane exclusion needed.
  auto stored_hash = data_v & Wide::Fill(OAHEntry::kExtHashShiftedMask | OAHPtr::kVectorBit);
  auto candidate = (stored_hash == shifted_ext_hash);
  return candidate.GetMSBs();
}

// Window may exceed one SIMD register: sweep in EntryWide strides, packing each stride's
// candidate mask (lane i of stride `off` -> bit off+i). uint32_t mask => <= 32 lanes.
inline FORCE_INLINE uint32_t OAHSet::ProbeWindow(const TaggedPtr* base,
                                                 uint64_t ext_hash) noexcept {
  const uint64_t shifted_ext_hash = ext_hash << OAHEntry::kExtHashShift;
  uint32_t candidates = 0;
  for (uint32_t off = 0; off < kDisplacementSize; off += EntryWide::kLanes)
    candidates |= ProbeLanes<EntryWide>(base + off, shifted_ext_hash) << off;
  return candidates;
}

// 2-lane SIMD strides. Vector sizes are always even (PtrVector grows by 2), so the
// stride covers the array with no tail.
template <bool Expire>
inline FORCE_INLINE TaggedPtr* OAHSet::ProbeExtensionVector(uint32_t ext_bid, std::string_view str,
                                                            uint64_t ext_hash) {
  auto vec = At(ext_bid).AsVector();
  TaggedPtr* raw_arr = vec.Raw();
  const size_t size = vec.Size();
  assert(size >= size_t(kVectorLaneStep));
  assert(size % kVectorLaneStep == 0u);

  const uint64_t shifted_ext_hash = ext_hash << OAHEntry::kExtHashShift;
  for (size_t base = 0; base < size; base += kVectorLaneStep) {
    auto cand_bits =
        ProbeLanes<VectorWide>(reinterpret_cast<const uint64_t*>(&raw_arr[base]), shifted_ext_hash);
    while (cand_bits) {
      const uint32_t j = std::countr_zero(cand_bits);
      cand_bits &= cand_bits - 1;
      OAHEntry re(raw_arr[base + j]);
      const bool match = re.Key() == str;
      if constexpr (Expire)
        ExpireIfNeeded(re);
      if (match)
        return &raw_arr[base + j];
    }
  }
  return nullptr;
}

// Window read stays in bounds: entries_ has kDisplacementSize-1 slack past BucketCount.
template <bool Expire>
inline FORCE_INLINE OAHSet::MatchResult OAHSet::FindMatch(uint32_t bid, uint32_t ext_bid,
                                                          uint32_t cand_bits, std::string_view str,
                                                          uint64_t ext_hash) {
  // Cache the bucket base: entries_ is not reallocated during a lookup, so the compiler
  // otherwise reloads entries_.data() each candidate (ExpireIfNeeded's zfree is opaque to it).
  TaggedPtr* base = entries_.data();
  while (cand_bits) {
    const uint32_t i = std::countr_zero(cand_bits);
    cand_bits &= cand_bits - 1;
    const uint32_t bucket_id = bid + i;
    // Vector slots carry the vector bit and are masked out of the probe, so a candidate is
    // always a single entry -- no IsVector check needed here.
    OAHEntry e(base[bucket_id]);
    const bool match = e.Key() == str;
    if constexpr (Expire)
      ExpireIfNeeded(e);
    if (match)
      return {&base[bucket_id], bucket_id, 0};
  }
  OAHPtr ext(base[ext_bid]);
  if (ext.IsVector()) {
    if (TaggedPtr* hit = ProbeExtensionVector<Expire>(ext_bid, str, ext_hash))
      return {hit, ext_bid, static_cast<uint32_t>(hit - ext.AsVector().Raw())};
  }
  return {nullptr, 0, 0};
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
  assert(ext < entries_.size());
  return ext;
}

inline FORCE_INLINE bool OAHSet::AddImpl(std::string_view str, uint32_t ttl_sec) {
  // Grow at load factor 1; until then overflow lands in the window / extension vectors.
  if (size_ >= entries_.size()) [[unlikely]] {
    Reserve(BucketCount() * 2);
  }
  assert(Capacity() >= kDisplacementSize);

  uint64_t hash = Hash(str);
  auto bucket_id = BucketId(hash);
  PREFETCH_READ(entries_.data() + bucket_id);

  const ssize_t mem_before = zmalloc_used_memory_tl;
  TaggedPtr entry_tagged_ptr = OAHEntry::Create(str, EntryTTL(ttl_sec));
  if (ttl_sec != UINT32_MAX)
    expiration_used_ = true;
  const size_t entry_alloc_size = zmalloc_used_memory_tl - mem_before;

  const uint32_t ext_bid = GetExtensionPoint(bucket_id);
  PREFETCH_READ(At(ext_bid).Raw());

  const uint64_t ext_hash = CalcExtHash(hash);
  OAHEntry new_entry(entry_tagged_ptr);
  new_entry.SetExtHash(ext_hash);

  const uint32_t cand_bits = ProbeWindow(&entries_[bucket_id], ext_hash);
  const MatchResult m = FindMatch(bucket_id, ext_bid, cand_bits, str, ext_hash);
  if (m.matched && *m.matched != 0) {
    OAHEntry::Destroy(entry_tagged_ptr);  // duplicate already present: discard the new blob
    return false;
  }

  obj_alloc_used_ += entry_alloc_size;
  ++size_;
  // Reuse an expired duplicate's slot, else the first free window lane (or the extension
  // point when the window is full), located by a separate SIMD empty scan.
  if (m.matched) {
    *m.matched = entry_tagged_ptr;
  } else {
    ptr_vectors_alloc_used_ += At(FindEmptyAround(bucket_id)).Insert(entry_tagged_ptr);
  }
  return true;
}

inline FORCE_INLINE OAHSet::iterator OAHSet::FindInternal(uint32_t bid, std::string_view str,
                                                          uint64_t hash) {
  const uint64_t ext_hash = CalcExtHash(hash);
  const uint32_t cand_bits = ProbeWindow(&entries_[bid], ext_hash);
  const MatchResult m = FindMatch(bid, GetExtensionPoint(bid), cand_bits, str, ext_hash);
  if (m.matched && *m.matched != 0)  // empty => matched but just expired, i.e. gone
    return iterator{this, m.bucket_id, m.pos_in_vec};
  return end();
}

inline FORCE_INLINE OAHSet::iterator OAHSet::PickFromBucket(uint32_t b) {
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

inline bool OAHSet::Add(std::string_view str, uint32_t ttl_sec) {
  return AddImpl(str, ttl_sec);
}

inline unsigned OAHSet::AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec, bool keepttl) {
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

inline OAHSet::iterator OAHSet::Find(std::string_view member) {
  if (entries_.empty())
    return end();
  const uint64_t hash = Hash(member);
  return FindInternal(BucketId(hash), member, hash);
}

inline bool OAHSet::Erase(std::string_view str) {
  if (entries_.empty())
    return false;
  const uint64_t hash = Hash(str);
  const uint32_t bid = BucketId(hash);
  const uint64_t ext_hash = CalcExtHash(hash);
  const uint32_t cand_bits = ProbeWindow(&entries_[bid], ext_hash);
  // Expire=false: don't lazily reap bystander candidates -- Erase won't revisit the set. The
  // matched target's own expiry is resolved below (so it's never returned as a reaped 0 here).
  const MatchResult m = FindMatch<false>(bid, GetExtensionPoint(bid), cand_bits, str, ext_hash);
  if (!m.matched)  // absent
    return false;

  OAHEntry victim(*m.matched);
  // An already-expired target is logically absent, so report not-removed (0) to match the
  // StringSet encoding and Redis SREM/HDEL; the reaping below is identical either way.
  const bool removed = !IsExpired(victim);

  --size_;
  obj_alloc_used_ -= victim.AllocSize();
  OAHEntry::Destroy(victim.Release());  // zeroes the matched cell

  // A window single entry sits directly in entries_[bucket_id], so erasing it needs no cleanup.
  // Only a match whose cell lies inside an extension vector (matched != the bucket slot) can
  // leave that vector empty; there the slot is necessarily a vector, so AsVector() needs no
  // IsVector() re-check.
  if (m.matched != &entries_[m.bucket_id]) {
    OAHPtr bucket = At(m.bucket_id);
    auto vec = bucket.AsVector();
    if (vec.Empty()) {
      ptr_vectors_alloc_used_ -= vec.AllocSize();
      bucket.Clear();
    }
  }
  return removed;
}

inline OAHSet::iterator OAHSet::ScanRange(uint32_t lo, uint32_t hi) {
  for (; lo + EntryWide::kLanes <= hi; lo += EntryWide::kLanes) {
    const EntryWide data = EntryWide::Load(&entries_[lo]);
    // Invert the empty mask in a scalar register (cheap not+and on the active lane bits)
    // instead of a vector NOT of the compare result.
    uint32_t used = ~(data == uint64_t(0)).GetMSBs() & ((1u << EntryWide::kLanes) - 1);
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

inline OAHSet::iterator OAHSet::GetRandomMember() {
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

inline void OAHSet::Rehash(uint32_t prev_size) {
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

  // Prefetch the key blob a few buckets ahead of the sweep: RedistributeBucket hashes each
  // key (RehashEntry), so the scattered blobs are the miss source.
  for (size_t bucket_id = prev_size - 1; bucket_id >= mix_size; --bucket_id) {
    if (bucket_id >= mix_size + kIterBlobLookahead)
      PrefetchTaggedRaw(entries_[bucket_id - kIterBlobLookahead]);
    TaggedPtr slot = entries_[bucket_id];
    entries_[bucket_id] = 0;
    RedistributeBucket(slot);
  }

  for (size_t bucket_id = 0; bucket_id < mix_size; ++bucket_id) {
    if (bucket_id + kIterBlobLookahead < mix_size)
      PrefetchTaggedRaw(old_buckets[bucket_id + kIterBlobLookahead]);
    RedistributeBucket(old_buckets[bucket_id]);
  }
}

inline void OAHSet::RedistributeBucket(TaggedPtr& slot) {
  OAHPtr bucket(slot);
  for (uint32_t pos = 0, size = bucket.ElementsNum(); pos < size; ++pos) {
    OAHEntry entry = bucket[pos];
    if (entry) {
      uint32_t new_bucket_id = FindEmptyAround(RehashEntry(entry));
      ptr_vectors_alloc_used_ += At(new_bucket_id).Insert(bucket.Remove(pos));
    }
  }
  if (bucket.IsVector())
    ptr_vectors_alloc_used_ -= bucket.AsVector().AllocSize();
  bucket.Clear();
}

inline void OAHSet::Shrink(size_t new_size) {
  assert(absl::has_single_bit(new_size));
  assert(new_size >= (1u << kMinCapacityLog));
  assert(new_size < entries_.size());

  size_t prev_size = entries_.size();
  capacity_log_ = absl::bit_width(new_size) - 1;

  // Process from low to high (opposite of Grow/Rehash). Prefetch the key blob a few buckets
  // ahead: ShrinkBucket hashes each key (RehashEntry), so the scattered blobs are the miss
  // source while entries_ itself streams sequentially.
  for (size_t i = 0; i < prev_size; ++i) {
    if (i + kIterBlobLookahead < prev_size)
      PrefetchTaggedRaw(entries_[i + kIterBlobLookahead]);
    ShrinkBucket(i);
  }

  entries_.resize(Capacity());
  entries_.shrink_to_fit();
}

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
