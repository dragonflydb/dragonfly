// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>

#if defined(__aarch64__)
#include "base/sse2neon.h"
#else
#include <emmintrin.h>
#endif

namespace dfly {
namespace detail {

template <unsigned NUM_SLOTS> class SlotBitmap {
  static_assert(NUM_SLOTS > 0 && NUM_SLOTS <= 28);
  static constexpr unsigned kLen = NUM_SLOTS > 14 ? 2 : 1;
  static constexpr unsigned kAllocMask = (1u << NUM_SLOTS) - 1;
  static constexpr unsigned kBitmapLenMask = (1 << 4) - 1;
  static constexpr bool SINGLE = NUM_SLOTS <= 14;

 public:
  // probe - true means the entry is probing, i.e. not owning.
  // probe=true GetProbe returns index of probing entries, i.e. hosted but not owned by this bucket.
  // probe=false - mask of owning entries
  uint32_t GetProbe(bool probe) const {
    if constexpr (SINGLE)
      return ((val_[0].d >> 4) & kAllocMask) ^ ((!probe) * kAllocMask);
    return (val_[1].d & kAllocMask) ^ ((!probe) * kAllocMask);
  }

  // GetBusy returns the busy mask.
  uint32_t GetBusy() const {
    return SINGLE ? val_[0].d >> 18 : val_[0].d;
  }

  bool IsFull() const {
    return Size() == NUM_SLOTS;
  }

  unsigned Size() const {
    return SINGLE ? (val_[0].d & kBitmapLenMask) : __builtin_popcount(val_[0].d);
  }

  // Precondition: Must have empty slot
  // returns result in [0, NUM_SLOTS) range.
  int FindEmptySlot() const {
    uint32_t mask = ~(GetBusy());

    // returns the index for first set bit (FindLSBSetNonZero). mask must be non-zero.
    int slot = __builtin_ctz(mask);
    assert(slot < int(NUM_SLOTS));
    return slot;
  }

  // mask is NUM_SLOTS bits saying which slots needs to be freed (1 - should clear).
  void ClearSlots(uint32_t mask);

  void Clear() {
    if (SINGLE) {
      val_[0].d = 0;
    } else {
      val_[0].d = val_[1].d = 0;
    }
  }

  void ClearSlot(unsigned index);
  void SetSlot(unsigned index, bool probe);

  // cell 0 corresponds to first lsb bit in the busy mask, hence we need to shift left
  // the bitmap in order to shift right the cell-array.
  // Returns true if discarded the last slot (i.e. it was busy).
  bool ShiftLeft();

  void Swap(unsigned slot_a, unsigned slot_b);

 private:
  // SINGLE:
  //   val_[0] is [14 bit- busy][14bit-probing, whether the key does not belong to this
  //   bucket][4bit-count]
  // kLen == 2:
  //  val_[0] is 28 bit busy
  //  val_[1] is 28 bit probing
  //  count is implemented via popcount of val_[0].
  struct Unaligned {
    // Apparently with wrapping struct we can persuade compiler to declare an unaligned int.
    // https://stackoverflow.com/questions/19915303/packed-qualifier-ignored
    uint32_t d __attribute__((packed, aligned(1)));

    Unaligned() : d(0) {
    }
  };

  Unaligned val_[kLen];
};  // SlotBitmap

template <unsigned NUM_SLOTS, unsigned NUM_STASH_FPS> class BucketBase {
  // We can not allow more than 4 stash fps because we hold stash positions in single byte
  // stash_pos_ variable that uses 2 bits per stash bucket to point which bucket holds that fp.
  // Hence we can point at most from 4 fps to 4 stash buckets.
  // If any of those limits need to be raised we should increase stash_pos_ similarly to how we did
  // with SlotBitmap.
  static_assert(NUM_STASH_FPS <= 4, "Can only hold at most 4 fp slots");

  static constexpr unsigned kStashFpLen = NUM_STASH_FPS;
  static constexpr unsigned kStashPresentBit = 1 << 4;

  using FpArray = std::array<uint8_t, NUM_SLOTS>;
  using StashFpArray = std::array<uint8_t, NUM_STASH_FPS>;

 public:
  using SlotId = uint8_t;
  static constexpr SlotId kNanSlot = 255;

  bool IsFull() const {
    return Size() == NUM_SLOTS;
  }

  bool IsEmpty() const {
    return GetBusy() == 0;
  }

  unsigned Size() const {
    return slotb_.Size();
  }

  // Returns -1 if no free slots found or smallest slot index
  int FindEmptySlot() const {
    if (IsFull()) {
      return -1;
    }

    return slotb_.FindEmptySlot();
  }

  void Delete(SlotId sid) {
    slotb_.ClearSlot(sid);
  }

  unsigned Find(uint8_t fp_hash, bool probe) const {
    unsigned mask = CompareFP(fp_hash) & GetBusy();
    return mask & GetProbe(probe);
  }

  uint8_t Fp(unsigned i) const {
    assert(i < finger_arr_.size());
    return finger_arr_[i];
  }

  void SetStashPtr(unsigned stash_pos, uint8_t meta_hash, BucketBase* next);

  // returns 0 if stash was cleared from this bucket, 1 if it was cleared from next bucket.
  unsigned UnsetStashPtr(uint8_t fp_hash, unsigned stash_pos, BucketBase* next);

  // probe - true means the entry is probing, i.e. not owning.
  // probe=true GetProbe returns index of probing entries, i.e. hosted but not owned by this bucket.
  // probe=false - mask of owning entries
  uint32_t GetProbe(bool probe) const {
    return slotb_.GetProbe(probe);
  }

  // GetBusy returns the busy mask.
  uint32_t GetBusy() const {
    return slotb_.GetBusy();
  }

  // mask is saying which slots needs to be freed (1 - should clear).
  void ClearSlots(uint32_t mask) {
    slotb_.ClearSlots(mask);
  }

  void Clear() {
    slotb_.Clear();
  }

  bool HasStash() const {
    return stash_busy_ & kStashPresentBit;
  }

  void SetHash(unsigned slot_id, uint8_t meta_hash, bool probe);

  bool HasStashOverflow() const {
    return overflow_count_ > 0;
  }

  // func accepts an fp_index in range [0, kStashFpLen) and
  // stash position [0, STASH_BUCKET_NUM) that with fingerprint=fp. func must return
  // a slot id if it found whatever it searched for when iterating or kNanSlot to continue.
  // IterateStash returns: first - stash position [0, STASH_BUCKET_NUM), second - slot id
  // pointing to that stash.
  template <typename F>
  std::pair<unsigned, SlotId> IterateStash(uint8_t fp, bool is_probe, F&& func) const;

  // calls for each busy slot: cb(iterator, probe)
  template <typename Cb> void ForEachSlot(Cb&& cb) const {
    uint32_t mask = this->GetBusy();
    uint32_t probe_mask = this->GetProbe(true);

    for (unsigned j = 0; j < NUM_SLOTS; ++j) {
      if (mask & 1) {
        cb(j, probe_mask & 1);
      }
      mask >>= 1;
      probe_mask >>= 1;
    }
  }

  void Swap(unsigned slot_a, unsigned slot_b) {
    slotb_.Swap(slot_a, slot_b);
    std::swap(finger_arr_[slot_a], finger_arr_[slot_b]);
  }

 protected:
  uint32_t CompareFP(uint8_t fp) const;
  bool ShiftRight();

  // Returns true if stash_pos was stored, false overwise
  bool SetStash(uint8_t fp, unsigned stash_pos, bool probe);
  bool ClearStash(uint8_t fp, unsigned stash_pos, bool probe);

  SlotBitmap<NUM_SLOTS> slotb_;  // allocation bitmap + pointer bitmap + counter

  /*only use the first 14 bytes, can be accelerated by
    SSE instruction,0-13 for finger, 14-17 for overflowed*/
  FpArray finger_arr_;
  StashFpArray stash_arr_;

  uint8_t stash_busy_ = 0;  // kStashFpLen+1 bits are used
  uint8_t stash_pos_ = 0;   // 4x2 bits for pointing to stash bucket.

  // stash_probe_mask_ indicates whether the overflow fingerprint is for the neighbour (1)
  // or for this bucket (0). kStashFpLen bits are used.
  uint8_t stash_probe_mask_ = 0;

  // number of overflowed items stored in stash buckets that do not have fp hashes.
  uint8_t overflow_count_ = 0;
};  // BucketBase

static_assert(sizeof(BucketBase<12, 4>) == 24, "");
static_assert(alignof(BucketBase<14, 4>) == 1, "");
static_assert(alignof(BucketBase<12, 4>) == 1, "");

// Optional version support as part of DashTable.
// This works like this: each slot has 2 bytes for version and a bucket has another 6.
// therefore all slots in the bucket shared the same 6 high bytes of 8-byte version.
// In order to achieve this we store high6(max{version(entry)}) for every entry.
// Hence our version control may have false positives, i.e. signal that an entry has changed
// when in practice its neighbour incremented the high6 part of its bucket.
template <unsigned NUM_SLOTS, unsigned NUM_STASH_FPS>
class VersionedBB : public BucketBase<NUM_SLOTS, NUM_STASH_FPS> {
  using Base = BucketBase<NUM_SLOTS, NUM_STASH_FPS>;

 public:
  // one common version per bucket.
  void SetVersion(uint64_t version);

  uint64_t GetVersion() const {
    uint64_t c = absl::little_endian::Load64(version_);
    // c |= low_[slot_id];
    return c;
  }

#if 0
  // Returns 64 bit bucket version of 2 low bytes zeroed.
  /*uint64_t BaseVersion() const {
    // high_ is followed by low array.
    // Hence little endian load from high_ ptr copies 2 bytes from low_ into 2 highest bytes of c.
    uint64_t c = absl::little_endian::Load64(high_);

    // Fix the version by getting rid of 2 garbage bytes.
    return c << 16;
  }*/
#endif

  void Clear() {
    Base::Clear();
    // low_.fill(0);
    memset(version_, 0, sizeof(version_));
  }

  bool ShiftRight() {
    bool res = Base::ShiftRight();
    /*for (int i = NUM_SLOTS - 1; i > 0; i--) {
      low_[i] = low_[i - 1];
    }*/
    return res;
  }

  void Swap(unsigned slot_a, unsigned slot_b) {
    Base::Swap(slot_a, slot_b);
    // std::swap(low_[slot_a], low_[slot_b]);
  }

 private:
  uint8_t version_[8] = {0};
  // std::array<uint8_t, NUM_SLOTS> low_ = {0};
};

static_assert(alignof(VersionedBB<14, 4>) == 1, "");
static_assert(sizeof(VersionedBB<12, 4>) == 12 * 2 + 8, "");
static_assert(sizeof(VersionedBB<14, 4>) <= 14 * 2 + 8, "");

// Segment - static-hashtable of size NUM_SLOTS*(BUCKET_CNT + STASH_BUCKET_NUM).
struct DefaultSegmentPolicy {
  static constexpr unsigned NUM_SLOTS = 12;
  static constexpr unsigned BUCKET_CNT = 64;
  static constexpr unsigned STASH_BUCKET_NUM = 2;
  static constexpr bool USE_VERSION = true;
};

template <typename _Key, typename _Value, typename Policy = DefaultSegmentPolicy> class Segment {
  static constexpr unsigned BUCKET_CNT = Policy::BUCKET_CNT;
  static constexpr unsigned STASH_BUCKET_NUM = Policy::STASH_BUCKET_NUM;
  static constexpr unsigned NUM_SLOTS = Policy::NUM_SLOTS;
  static constexpr bool USE_VERSION = Policy::USE_VERSION;

  static_assert(BUCKET_CNT + STASH_BUCKET_NUM < 255);
  static constexpr unsigned kFingerBits = 8;

  using BucketType =
      std::conditional_t<USE_VERSION, VersionedBB<NUM_SLOTS, 4>, BucketBase<NUM_SLOTS, 4>>;

  struct Bucket : public BucketType {
    using BucketType::kNanSlot;
    using typename BucketType::SlotId;

    _Key key[NUM_SLOTS];
    _Value value[NUM_SLOTS];

    template <typename U, typename V>
    void Insert(uint8_t slot, U&& u, V&& v, uint8_t meta_hash, bool probe) {
      assert(slot < NUM_SLOTS);

      key[slot] = std::forward<U>(u);
      value[slot] = std::forward<V>(v);

      this->SetHash(slot, meta_hash, probe);
    }

    template <typename U, typename Pred>
    SlotId FindByFp(uint8_t fp_hash, bool probe, U&& k, Pred&& pred) const;

    bool ShiftRight();

    void Swap(unsigned slot_a, unsigned slot_b) {
      BucketType::Swap(slot_a, slot_b);
      std::swap(key[slot_a], key[slot_b]);
      std::swap(value[slot_a], value[slot_b]);
    }
  };  // class Bucket

  static constexpr uint8_t kNanBid = 0xFF;
  using SlotId = typename BucketType::SlotId;

 public:
  struct Iterator {
    uint8_t index;  // bucket index
    uint8_t slot;

    Iterator() : index(kNanBid), slot(BucketType::kNanSlot) {
    }

    Iterator(uint8_t bi, uint8_t sid) : index(bi), slot(sid) {
    }

    bool found() const {
      return index != kNanBid;
    }
  };

  struct Stats {
    size_t neighbour_probes = 0;
    size_t stash_probes = 0;
    size_t stash_overflow_probes = 0;
  };

  /* number of normal buckets in one segment*/
  static constexpr uint8_t kNumBuckets = BUCKET_CNT;
  static constexpr uint8_t kTotalBuckets = kNumBuckets + STASH_BUCKET_NUM;
  static constexpr size_t kFpMask = (1 << kFingerBits) - 1;
  static constexpr size_t kNumSlots = NUM_SLOTS;

  using Value_t = _Value;
  using Key_t = _Key;
  using Hash_t = uint64_t;

  explicit Segment(size_t depth) : local_depth_(depth) {
  }

  // Returns (iterator, true) if insert succeeds,
  // (iterator, false) for duplicate and (invalid-iterator, false) if it's full
  template <typename K, typename V, typename Pred>
  std::pair<Iterator, bool> Insert(K&& key, V&& value, Hash_t key_hash, Pred&& cmp_fun);

  template <typename HashFn> void Split(HashFn&& hfunc, Segment* dest);

  void Delete(const Iterator& it, Hash_t key_hash);

  void Clear();  // clears the segment.

  size_t SlowSize() const;

  static constexpr size_t capacity() {
    return kMaxSize;
  }

  size_t local_depth() const {
    return local_depth_;
  }

  void set_local_depth(uint32_t depth) {
    local_depth_ = depth;
  }

  template <bool UV = Policy::USE_VERSION>
  std::enable_if_t<UV, uint64_t> GetVersion(uint8_t bid) const {
    return bucket_[bid].GetVersion();
  }

  template <bool UV = Policy::USE_VERSION>
  std::enable_if_t<UV> SetVersion(uint8_t bid, uint64_t v) {
    return bucket_[bid].SetVersion(v);
  }

  // Traverses over Segment's bucket bid and calls cb(const Iterator& it) 0 or more times
  // for each slot in the bucket. returns false if bucket is empty.
  // Please note that `it` will not necessary point to bid due to probing and stash buckets
  // containing items that should have been resided in bid.
  template <typename Cb, typename HashFn>
  bool TraverseLogicalBucket(uint8_t bid, HashFn&& hfun, Cb&& cb) const;

  // Cb  accepts (const Iterator&).
  template <typename Cb> void TraverseAll(Cb&& cb) const;

  // Used in test.
  unsigned NumProbingBuckets() const {
    unsigned res = 0;
    for (unsigned i = 0; i < kNumBuckets; ++i) {
      res += (bucket_[i].GetProbe(true) != 0);
    }
    return res;
  };

  const BucketType& GetBucket(size_t i) const {
    return bucket_[i];
  }

  Key_t& Key(unsigned bid, unsigned slot) {
    assert(bucket_[bid].GetBusy() & (1U << slot));
    return bucket_[bid].key[slot];
  }

  const Key_t& Key(unsigned bid, unsigned slot) const {
    assert(bucket_[bid].GetBusy() & (1U << slot));
    return bucket_[bid].key[slot];
  }

  Value_t& Value(unsigned bid, unsigned slot) {
    assert(bucket_[bid].GetBusy() & (1U << slot));
    return bucket_[bid].value[slot];
  }

  const Value_t& Value(unsigned bid, unsigned slot) const {
    assert(bucket_[bid].GetBusy() & (1U << slot));
    return bucket_[bid].value[slot];
  }

  // fill bucket ids that may be used probing for this key_hash.
  // The order is: exact, neighbour buckets.
  static void FillProbeArray(Hash_t key_hash, uint8_t dest[4]) {
    dest[1] = BucketIndex(key_hash);
    dest[0] = PrevBid(dest[1]);
    dest[2] = NextBid(dest[1]);
    dest[3] = NextBid(dest[2]);
  }

  template <typename U, typename Pred> Iterator FindIt(U&& key, Hash_t key_hash, Pred&& cf) const;

  // Returns valid iterator if succeeded or invalid if not (it's full).
  // Requires: key should be not present in the segment.
  template <typename U, typename V> Iterator InsertUniq(U&& key, V&& value, Hash_t key_hash);

  // capture version change in case of insert.
  // Returns ids of buckets whose version would cross ver_threshold upon insertion of key_hash
  // into the segment.
  // Returns UINT16_MAX if segment is full. Otherwise, returns number of touched bucket ids (1 or 2)
  // if the insertion would happen. The ids are put into bid array that should have at least 2
  // spaces.
  template <bool UV = Policy::USE_VERSION>
  std::enable_if_t<UV, unsigned> CVCOnInsert(uint64_t ver_threshold, Hash_t key_hash,
                                             uint8_t bid[2]) const;

  // Returns bucket ids whose versions will change as a result of bumping up the item
  // Can return upto 3 buckets.
  template <bool UV = Policy::USE_VERSION>
  std::enable_if_t<UV, unsigned> CVCOnBump(uint64_t ver_threshold, unsigned bid, unsigned slot,
                                           Hash_t hash, uint8_t result_bid[2]) const;

  // Finds a valid entry going from specified indices up.
  Iterator FindValidStartingFrom(unsigned bid, unsigned slot) const;

  // Shifts all slots in the bucket right.
  // Returns true if the last slot was busy and the entry has been deleted.
  bool ShiftRight(unsigned bid, Hash_t right_hashval) {
    if (bid >= kNumBuckets) {  // Stash
      constexpr auto kLastSlotMask = 1u << (kNumSlots - 1);
      if (bucket_[bid].GetBusy() & kLastSlotMask)
        RemoveStashReference(bid - kNumBuckets, right_hashval);
    }

    return bucket_[bid].ShiftRight();
  }

  // Bumps up this entry making it more "important" for the eviction policy.
  Iterator BumpUp(uint8_t bid, SlotId slot, Hash_t key_hash);

  // Tries to move stash entries back to their normal buckets (exact or neighour).
  // Returns number of entries that succeeded to unload.
  // Important! Affects versions of the moved items and the items in the destination
  // buckets.
  template <typename HFunc> unsigned UnloadStash(HFunc&& hfunc);

 private:
  static_assert(sizeof(Iterator) == 2);

  static unsigned BucketIndex(Hash_t hash) {
    return (hash >> kFingerBits) % kNumBuckets;
  }

  static uint8_t NextBid(uint8_t bid) {
    return bid < kNumBuckets - 1 ? bid + 1 : 0;
  }

  static uint8_t PrevBid(uint8_t bid) {
    return bid ? bid - 1 : kNumBuckets - 1;
  }

  // if own_items is true it means we try to move owned item to probing bucket.
  // if own_items false it means we try to move non-owned item from probing bucket back to its host.
  int MoveToOther(bool own_items, unsigned from, unsigned to);

  // dry-run version of MoveToOther.
  bool CheckIfMovesToOther(bool own_items, unsigned from, unsigned to) const;

  /*both clear this bucket and its neighbor bucket*/
  void RemoveStashReference(unsigned stash_pos, Hash_t key_hash);

  // Returns slot id if insertion is successful, -1 if no free slots are found.
  template <typename U, typename V>
  int TryInsertToBucket(unsigned bidx, U&& key, V&& value, uint8_t meta_hash, bool probe) {
    auto& b = bucket_[bidx];
    auto slot = b.FindEmptySlot();
    assert(slot < int(kNumSlots));
    if (slot < 0) {
      return -1;
    }

    b.Insert(slot, std::forward<U>(key), std::forward<V>(value), meta_hash, probe);

    return slot;
  }

  // returns a valid iterator if succeeded.
  Iterator TryMoveFromStash(unsigned stash_id, unsigned stash_slot_id, Hash_t key_hash);

  Bucket bucket_[kTotalBuckets];
  size_t local_depth_;

 public:
  static constexpr size_t kBucketSz = sizeof(Bucket);
  static constexpr size_t kMaxSize = kTotalBuckets * kNumSlots;
  static constexpr double kTaxSize =
      (double(sizeof(Segment)) / kMaxSize) - sizeof(Key_t) - sizeof(Value_t);

#ifdef ENABLE_DASH_STATS
  mutable Stats stats;
#endif
};  // Segment

class DashTableBase {
  DashTableBase(const DashTableBase&) = delete;
  DashTableBase& operator=(const DashTableBase&) = delete;

 public:
  explicit DashTableBase(uint32_t gd)
      : unique_segments_(1 << gd), initial_depth_(gd), global_depth_(gd) {
  }

  uint32_t unique_segments() const {
    return unique_segments_;
  }

  uint16_t depth() const {
    return global_depth_;
  }

  size_t size() const {
    return size_;
  }

 protected:
  uint32_t SegmentId(size_t hash) const {
    if (global_depth_) {
      return hash >> (64 - global_depth_);
    }

    return 0;
  }

  size_t size_ = 0;
  uint32_t unique_segments_;
  uint8_t initial_depth_;
  uint8_t global_depth_;
};  // DashTableBase

template <typename _Key, typename _Value> class IteratorPair {
 public:
  IteratorPair(_Key& k, _Value& v) : first(k), second(v) {
  }

  IteratorPair* operator->() {
    return this;
  }

  const IteratorPair* operator->() const {
    return this;
  }

  _Key& first;
  _Value& second;
};

// Represents a cursor that points to a bucket in dash table.
// One major difference with iterator is that the cursor survives dash table resizes and
// will always point to the most appropriate segment with the same bucket.
// It uses 40 lsb bits out of 64 assuming that number of segments does not cross 4B.
// It's a reasonable assumption in shared nothing architecture when we usually have no more than
// 32GB per CPU. Each segment spawns hundreds of entries so we can not grow segment table
// to billions.
class DashCursor {
 public:
  DashCursor(uint64_t val = 0) : val_(val) {
  }

  DashCursor(uint8_t depth, uint32_t seg_id, uint8_t bid)
      : val_((uint64_t(seg_id) << (40 - depth)) | bid) {
  }

  uint8_t bucket_id() const {
    return val_ & 0xFF;
  }

  // segment_id is padded to the left of 32 bit region:
  // | segment_id......| bucket_id
  // 40                8          0
  // By using depth we take most significant bits of segment_id if depth has decreased
  // since the cursort was created, or extend the least significant bits with zeros if
  // depth has increased.
  uint32_t segment_id(uint8_t depth) {
    return val_ >> (40 - depth);
  }

  uint64_t value() const {
    return val_;
  }

  explicit operator bool() const {
    return val_ != 0;
  }

 private:
  uint64_t val_;
};

/***********************************************************
 * Implementation section.
 */

template <unsigned NUM_SLOTS> void SlotBitmap<NUM_SLOTS>::SetSlot(unsigned index, bool probe) {
  if constexpr (SINGLE) {
    assert(((val_[0].d >> (index + 18)) & 1) == 0);
    val_[0].d |= (1 << (index + 18));
    val_[0].d |= (unsigned(probe) << (index + 4));

    assert((val_[0].d & kBitmapLenMask) < NUM_SLOTS);
    ++val_[0].d;
    assert(__builtin_popcount(val_[0].d >> 18) == (val_[0].d & kBitmapLenMask));
  } else {
    assert(((val_[0].d >> index) & 1) == 0);
    val_[0].d |= (1u << index);
    val_[1].d |= (unsigned(probe) << index);
  }
}

template <unsigned NUM_SLOTS> void SlotBitmap<NUM_SLOTS>::ClearSlot(unsigned index) {
  assert(Size() > 0);
  if constexpr (SINGLE) {
    uint32_t new_bitmap = val_[0].d & (~(1u << (index + 18))) & (~(1u << (index + 4)));
    new_bitmap -= 1;
    val_[0].d = new_bitmap;
  } else {
    uint32_t mask = 1u << index;
    val_[0].d &= ~mask;
    val_[1].d &= ~mask;
  }
}

template <unsigned NUM_SLOTS> bool SlotBitmap<NUM_SLOTS>::ShiftLeft() {
  constexpr uint32_t kBusyLastSlot = (kAllocMask >> 1) + 1;
  bool res;
  if constexpr (SINGLE) {
    constexpr uint32_t kShlMask = kAllocMask - 1;  // reset lsb
    res = (val_[0].d & (kBusyLastSlot << 18)) != 0;
    uint32_t l = (val_[0].d << 1) & (kShlMask << 4);
    uint32_t p = (val_[0].d << 1) & (kShlMask << 18);
    val_[0].d = __builtin_popcount(p) | l | p;
  } else {
    res = (val_[0].d & kBusyLastSlot) != 0;
    val_[0].d <<= 1;
    val_[0].d &= kAllocMask;
    val_[1].d <<= 1;
    val_[1].d &= kAllocMask;
  }
  return res;
}

template <unsigned NUM_SLOTS> void SlotBitmap<NUM_SLOTS>::ClearSlots(uint32_t mask) {
  if (SINGLE) {
    uint32_t count = __builtin_popcount(mask);
    assert(count <= (val_[0].d & 0xFF));
    mask = (mask << 4) | (mask << 18);
    val_[0].d &= ~mask;
    val_[0].d -= count;
  } else {
    val_[0].d &= ~mask;
    val_[1].d &= ~mask;
  }
}

template <unsigned NUM_SLOTS> void SlotBitmap<NUM_SLOTS>::Swap(unsigned slot_a, unsigned slot_b) {
  if (slot_a > slot_b)
    std::swap(slot_a, slot_b);

  if constexpr (SINGLE) {
    uint32_t a = (val_[0].d << (slot_b - slot_a)) ^ val_[0].d;
    uint32_t bm = (1 << (slot_b + 4)) | (1 << (slot_b + 18));
    a &= bm;
    a |= (a >> (slot_b - slot_a));
    val_[0].d ^= a;
  } else {
    uint32_t a = (val_[0].d << (slot_b - slot_a)) ^ val_[0].d;
    a &= (1 << slot_b);
    a |= (a >> (slot_b - slot_a));
    val_[0].d ^= a;

    a = (val_[1].d << (slot_b - slot_a)) ^ val_[1].d;
    a &= (1 << slot_b);
    a |= (a >> (slot_b - slot_a));
    val_[1].d ^= a;
  }
}

/*
___  _  _ ____ _  _ ____ ___    ___  ____ ____ ____
|__] |  | |    |_/  |___  |     |__] |__| [__  |___
|__] |__| |___ | \_ |___  |     |__] |  | ___] |___

*/

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
bool BucketBase<NUM_SLOTS, NUM_OVR>::ClearStash(uint8_t fp, unsigned stash_pos, bool probe) {
  auto cb = [stash_pos, this](unsigned i, unsigned pos) -> SlotId {
    if (pos == stash_pos) {
      stash_busy_ &= (~(1u << i));
      stash_probe_mask_ &= (~(1u << i));
      stash_pos_ &= (~(3u << (i * 2)));

      assert(0u == ((stash_pos_ >> (i * 2)) & 3));
      return 0;
    }
    return kNanSlot;
  };

  std::pair<unsigned, SlotId> res = IterateStash(fp, probe, std::move(cb));
  return res.second != kNanSlot;
}

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
void BucketBase<NUM_SLOTS, NUM_OVR>::SetHash(unsigned slot_id, uint8_t meta_hash, bool probe) {
  assert(slot_id < finger_arr_.size());

  finger_arr_[slot_id] = meta_hash;
  slotb_.SetSlot(slot_id, probe);
}

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
bool BucketBase<NUM_SLOTS, NUM_OVR>::SetStash(uint8_t fp, unsigned stash_pos, bool probe) {
  // stash_busy_ is never 0xFFFFF so it's safe to run __builtin_ctz below.
  unsigned free_slot = __builtin_ctz(~stash_busy_);
  if (free_slot >= kStashFpLen)
    return false;

  stash_arr_[free_slot] = fp;
  stash_busy_ |= (1u << free_slot);  // set the overflow slot

  // stash_probe_mask_ specifies which records relate to other bucket.
  stash_probe_mask_ |= (unsigned(probe) << free_slot);

  // 2 bits denote the bucket index.
  free_slot *= 2;
  stash_pos_ &= (~(3 << free_slot));       // clear (can be removed?)
  stash_pos_ |= (stash_pos << free_slot);  // and set
  return true;
}

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
void BucketBase<NUM_SLOTS, NUM_OVR>::SetStashPtr(unsigned stash_pos, uint8_t meta_hash,
                                                 BucketBase* next) {
  assert(stash_pos < 4);

  // we use only kStashFpLen fp slots for handling stash buckets,
  // therefore if all those slots are used we try neighbor (probing bucket) as a fallback to point
  // to stash buckets. otherwise we increment overflow count.
  // if overflow is incremented we will need to check all the stash buckets when looking for a key,
  //  otherwise we can use overflow_index_ to find the the stash bucket efficiently.
  if (!SetStash(meta_hash, stash_pos, false)) {
    if (!next->SetStash(meta_hash, stash_pos, true)) {
      overflow_count_++;
    }
  }
  stash_busy_ |= kStashPresentBit;
}

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
unsigned BucketBase<NUM_SLOTS, NUM_OVR>::UnsetStashPtr(uint8_t fp_hash, unsigned stash_pos,
                                                       BucketBase* next) {
  /*also needs to ensure that this meta_hash must belongs to other bucket*/
  bool clear_success = ClearStash(fp_hash, stash_pos, false);
  unsigned res = 0;

  if (!clear_success) {
    clear_success = next->ClearStash(fp_hash, stash_pos, true);
    res += clear_success;
  }

  if (!clear_success) {
    assert(overflow_count_ > 0);
    overflow_count_--;
  }

  // kStashPresentBit helps with summarizing all the stash states into a single binary flag.
  // We need it because of the next, though if we make sure to move stash pointers upon split/delete
  // towards the owner we should not reach the state where mask1 == 0 but mask2 &
  // next->stash_probe_mask_ != 0.
  unsigned mask1 = stash_busy_ & (kStashPresentBit - 1);
  unsigned mask2 = next->stash_busy_ & (kStashPresentBit - 1);

  if (((mask1 & (~stash_probe_mask_)) == 0) && (overflow_count_ == 0) &&
      ((mask2 & next->stash_probe_mask_) == 0)) {
    stash_busy_ &= ~kStashPresentBit;
  }

  return res;
}

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
uint32_t BucketBase<NUM_SLOTS, NUM_OVR>::CompareFP(uint8_t fp) const {
  static_assert(FpArray{}.size() <= 16);

  // Replicate 16 times fp to key_data.
  const __m128i key_data = _mm_set1_epi8(fp);

  // Loads 16 bytes of src into seg_data.
  __m128i seg_data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(finger_arr_.data()));

  // compare 16-byte vectors seg_data and key_data, dst[i] := ( a[i] == b[i] ) ? 0xFF : 0.
  __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data);

  // collapses 16 msb bits from each byte in rv_mask into mask.
  int mask = _mm_movemask_epi8(rv_mask);

  // Note: Last 2 operations can be combined in skylake with _mm_cmpeq_epi8_mask.
  return mask;
}

// Bucket slot array goes from left to right: [x, x, ...]
// Shift right vacates the first slot on the left by shifting all the elements right and
// possibly deleting the last one on the right.
template <unsigned NUM_SLOTS, unsigned NUM_OVR> bool BucketBase<NUM_SLOTS, NUM_OVR>::ShiftRight() {
  for (int i = NUM_SLOTS - 1; i > 0; --i) {
    finger_arr_[i] = finger_arr_[i - 1];
  }

  // confusing but correct - slot bit mask LSB corresponds to left part of slot array.
  // therefore, we shift left slot mask.
  bool res = slotb_.ShiftLeft();
  assert(slotb_.FindEmptySlot() == 0);
  return res;
}

template <unsigned NUM_SLOTS, unsigned NUM_OVR>
template <typename F>
auto BucketBase<NUM_SLOTS, NUM_OVR>::IterateStash(uint8_t fp, bool is_probe, F&& func) const
    -> ::std::pair<unsigned, SlotId> {
  unsigned om = is_probe ? stash_probe_mask_ : ~stash_probe_mask_;
  unsigned ob = stash_busy_;

  for (unsigned i = 0; i < kStashFpLen; ++i) {
    if ((ob & 1) && (stash_arr_[i] == fp) && (om & 1)) {
      unsigned pos = (stash_pos_ >> (i * 2)) & 3;
      auto sid = func(i, pos);
      if (sid != BucketBase::kNanSlot) {
        return std::pair<unsigned, SlotId>(pos, sid);
      }
    }
    ob >>= 1;
    om >>= 1;
  }
  return std::pair<unsigned, SlotId>(0, BucketBase::kNanSlot);
}

template <unsigned NUM_SLOTS, unsigned NUM_STASH_FPS>
void VersionedBB<NUM_SLOTS, NUM_STASH_FPS>::SetVersion(uint64_t version) {
  absl::little_endian::Store64(version_, version);
}

#if 0
template <unsigned NUM_SLOTS, unsigned NUM_STASH_FPS>
uint64_t VersionedBB<NUM_SLOTS, NUM_STASH_FPS>::MinVersion() const {
  uint32_t mask = this->GetBusy();
  if (mask == 0)
    return 0;

  // it's enough to compare low_ parts since base version is the same for all of them.
  uint16_t res = 0xFFFF;
  for (unsigned j = 0; j < NUM_SLOTS; ++j) {
    if ((mask & 1) && low_[j] < res) {
      res = low_[j];
    }
    mask >>= 1;
  }
  return BaseVersion() + res;
}
#endif

/*
____ ____ ____ _  _ ____ _  _ ___
[__  |___ | __ |\/| |___ |\ |  |
___] |___ |__] |  | |___ | \|  |

*/

template <typename Key, typename Value, typename Policy>
template <typename U, typename Pred>
auto Segment<Key, Value, Policy>::Bucket::FindByFp(uint8_t fp_hash, bool probe, U&& k,
                                                   Pred&& pred) const -> SlotId {
  unsigned mask = this->Find(fp_hash, probe);
  if (!mask)
    return kNanSlot;

  unsigned delta = __builtin_ctz(mask);
  mask >>= delta;
  for (unsigned i = delta; i < NUM_SLOTS; ++i) {
    if ((mask & 1) && pred(key[i], k)) {
      return i;
    }
    mask >>= 1;
  };

  return kNanSlot;
}

template <typename Key, typename Value, typename Policy>
bool Segment<Key, Value, Policy>::Bucket::ShiftRight() {
  bool res = BucketType::ShiftRight();
  for (int i = NUM_SLOTS - 1; i > 0; i--) {
    std::swap(key[i], key[i - 1]);
    std::swap(value[i], value[i - 1]);
  }
  return res;
}

// stash_pos is index of the stash bucket, in the range of [0, STASH_BUCKET_NUM).
template <typename Key, typename Value, typename Policy>
void Segment<Key, Value, Policy>::RemoveStashReference(unsigned stash_pos, Hash_t key_hash) {
  unsigned y = BucketIndex(key_hash);
  uint8_t fp_hash = key_hash & kFpMask;
  auto* target = &bucket_[y];
  auto* next = &bucket_[NextBid(y)];

  target->UnsetStashPtr(fp_hash, stash_pos, next);
}

template <typename Key, typename Value, typename Policy>
auto Segment<Key, Value, Policy>::TryMoveFromStash(unsigned stash_id, unsigned stash_slot_id,
                                                   Hash_t key_hash) -> Iterator {
  uint8_t bid = BucketIndex(key_hash);
  uint8_t hash_fp = key_hash & kFpMask;
  uint8_t stash_bid = kNumBuckets + stash_id;
  auto& key = Key(stash_bid, stash_slot_id);
  auto& value = Value(stash_bid, stash_slot_id);

  int reg_slot = TryInsertToBucket(bid, std::forward<Key_t>(key), std::forward<Value_t>(value),
                                   hash_fp, false);
  if (reg_slot < 0) {
    bid = NextBid(bid);
    reg_slot = TryInsertToBucket(bid, std::forward<Key_t>(key), std::forward<Value_t>(value),
                                 hash_fp, true);
  }

  if (reg_slot >= 0) {
    if constexpr (USE_VERSION) {
      // We maintain the invariant for the physical bucket by updating the version when
      // the entries move between buckets.
      uint64_t ver = bucket_[stash_bid].GetVersion();
      uint64_t dest_ver = bucket_[bid].GetVersion();
      if (dest_ver < ver) {
        bucket_[bid].SetVersion(ver);
      }
    }
    RemoveStashReference(stash_id, key_hash);
    return Iterator{bid, SlotId(reg_slot)};
  }

  return Iterator{};
}

template <typename Key, typename Value, typename Policy>
template <typename U, typename V, typename Pred>
auto Segment<Key, Value, Policy>::Insert(U&& key, V&& value, Hash_t key_hash, Pred&& cmp_fun)
    -> std::pair<Iterator, bool> {
  Iterator it = FindIt(key, key_hash, std::forward<Pred>(cmp_fun));
  if (it.found()) {
    return std::make_pair(it, false); /* duplicate insert*/
  }

  it = InsertUniq(std::forward<U>(key), std::forward<V>(value), key_hash);

  return std::make_pair(it, it.found());
}

template <typename Key, typename Value, typename Policy>
template <typename U, typename Pred>
auto Segment<Key, Value, Policy>::FindIt(U&& key, Hash_t key_hash, Pred&& cf) const -> Iterator {
  uint8_t bidx = BucketIndex(key_hash);
  const Bucket& target = bucket_[bidx];

  // It helps a bit (10% on my home machine) and more importantly, it does not hurt
  // since we are going to access this memory in a bit.
  __builtin_prefetch(&target);

  uint8_t fp_hash = key_hash & kFpMask;
  SlotId sid = target.FindByFp(fp_hash, false, key, cf);
  if (sid != BucketType::kNanSlot) {
    return Iterator{bidx, sid};
  }

  uint8_t nid = NextBid(bidx);
  const Bucket& probe = bucket_[nid];

  sid = probe.FindByFp(fp_hash, true, key, cf);

#ifdef ENABLE_DASH_STATS
  stats.neighbour_probes++;
#endif

  if (sid != BucketType::kNanSlot) {
    return Iterator{nid, sid};
  }

  if (!target.HasStash()) {
    return Iterator{};
  }

  auto stash_cb = [&](unsigned overflow_index, unsigned pos) -> SlotId {
    assert(pos < STASH_BUCKET_NUM);

    pos += kNumBuckets;
    const Bucket& bucket = bucket_[pos];
    return bucket.FindByFp(fp_hash, false, key, cf);
  };

  if (target.HasStashOverflow()) {
#ifdef ENABLE_DASH_STATS
    stats.stash_overflow_probes++;
#endif

    for (unsigned i = 0; i < STASH_BUCKET_NUM; ++i) {
      auto sid = stash_cb(0, i);
      if (sid != BucketType::kNanSlot) {
        return Iterator{uint8_t(kNumBuckets + i), sid};
      }
    }

    // We exit because we searched through all stash buckets anyway, no need to use overflow fps.
    return Iterator{};
  }

#ifdef ENABLE_DASH_STATS
  stats.stash_probes++;
#endif

  auto stash_res = target.IterateStash(fp_hash, false, stash_cb);
  if (stash_res.second != BucketType::kNanSlot) {
    return Iterator{uint8_t(kNumBuckets + stash_res.first), stash_res.second};
  }

  stash_res = probe.IterateStash(fp_hash, true, stash_cb);
  if (stash_res.second != BucketType::kNanSlot) {
    return Iterator{uint8_t(kNumBuckets + stash_res.first), stash_res.second};
  }
  return Iterator{};
}

template <typename Key, typename Value, typename Policy>
template <typename Cb>
void Segment<Key, Value, Policy>::TraverseAll(Cb&& cb) const {
  for (uint8_t i = 0; i < kTotalBuckets; ++i) {
    bucket_[i].ForEachSlot([&](SlotId slot, bool) { cb(Iterator{i, slot}); });
  }
}

template <typename Key, typename Value, typename Policy> void Segment<Key, Value, Policy>::Clear() {
  for (unsigned i = 0; i < kTotalBuckets; ++i) {
    bucket_[i].Clear();
  }
}

template <typename Key, typename Value, typename Policy>
void Segment<Key, Value, Policy>::Delete(const Iterator& it, Hash_t key_hash) {
  assert(it.found());

  auto& b = bucket_[it.index];

  if (it.index >= kNumBuckets) {
    RemoveStashReference(it.index - kNumBuckets, key_hash);
  }

  b.Delete(it.slot);
}

// Split items from the left segment to the right during the growth phase.
// right segment will have all the items with lsb at local_depth ==1 .
template <typename Key, typename Value, typename Policy>
template <typename HFunc>
void Segment<Key, Value, Policy>::Split(HFunc&& hfn, Segment* dest_right) {
  ++local_depth_;
  dest_right->local_depth_ = local_depth_;

  // versioning does not work when entries move across buckets.
  // we need to setup rules on how we do that
  // do_versioning();
  auto is_mine = [this](Hash_t hash) { return (hash >> (64 - local_depth_) & 1) == 0; };

  for (unsigned i = 0; i < kNumBuckets; ++i) {
    uint32_t invalid_mask = 0;

    auto cb = [&](unsigned slot, bool probe) {
      auto& key = Key(i, slot);
      Hash_t hash = hfn(key);

      // we extract local_depth bits from the left part of the hash. Since we extended local_depth,
      // we added an additional bit to the right, therefore we need to look at lsb of the extract.
      if (is_mine(hash))
        return;  // keep this key in the source

      invalid_mask |= (1u << slot);

      auto it = dest_right->InsertUniq(std::forward<Key_t>(key),
                                       std::forward<Value_t>(Value(i, slot)), hash);
      (void)it;
      if constexpr (USE_VERSION) {
        // Maintaining consistent versioning.
        uint64_t ver = bucket_[i].GetVersion();
        uint64_t dest_ver = dest_right->bucket_[it.index].GetVersion();
        if (dest_ver < ver) {
          dest_right->bucket_[it.index].SetVersion(ver);
        }
      }
    };

    bucket_[i].ForEachSlot(std::move(cb));
    bucket_[i].ClearSlots(invalid_mask);
  }

  for (unsigned i = 0; i < STASH_BUCKET_NUM; ++i) {
    uint32_t invalid_mask = 0;
    unsigned bid = kNumBuckets + i;
    Bucket& stash = bucket_[bid];

    auto cb = [&](unsigned slot, bool probe) {
      auto& key = Key(bid, slot);
      Hash_t hash = hfn(key);

      if (is_mine(hash)) {
        // If the entry stays in the same segment we try to unload it back to the regular bucket.
        Iterator it = TryMoveFromStash(i, slot, hash);
        if (it.found()) {
          invalid_mask |= (1u << slot);
        }

        return;
      }

      invalid_mask |= (1u << slot);
      auto it = dest_right->InsertUniq(std::forward<Key_t>(Key(bid, slot)),
                                       std::forward<Value_t>(Value(bid, slot)), hash);
      (void)it;
      if constexpr (USE_VERSION) {
        // Update the version in the destination bucket.
        uint64_t ver = stash.GetVersion();
        uint64_t dest_ver = dest_right->bucket_[it.index].GetVersion();
        if (dest_ver < ver) {
          dest_right->bucket_[it.index].SetVersion(ver);
        }
      }

      // Remove stash reference pointing to stach bucket i.
      RemoveStashReference(i, hash);
    };

    stash.ForEachSlot(std::move(cb));
    stash.ClearSlots(invalid_mask);
  }
}

template <typename Key, typename Value, typename Policy>
int Segment<Key, Value, Policy>::MoveToOther(bool own_items, unsigned from_bid, unsigned to_bid) {
  auto& src = bucket_[from_bid];
  uint32_t mask = src.GetProbe(!own_items);
  if (mask == 0) {
    return -1;
  }

  int src_slot = __builtin_ctz(mask);
  int dst_slot =
      TryInsertToBucket(to_bid, std::forward<Key_t>(src.key[src_slot]),
                        std::forward<Value_t>(src.value[src_slot]), src.Fp(src_slot), own_items);
  if (dst_slot < 0)
    return -1;

  // We never decrease the version of the entry.
  if constexpr (USE_VERSION) {
    auto& dst = bucket_[to_bid];
    if (dst.GetVersion() < src.GetVersion()) {
      dst.SetVersion(src.GetVersion());
    }
  }

  src.Delete(src_slot);

  return src_slot;
}

template <typename Key, typename Value, typename Policy>
bool Segment<Key, Value, Policy>::CheckIfMovesToOther(bool own_items, unsigned from,
                                                      unsigned to) const {
  const auto& src = bucket_[from];
  uint32_t mask = src.GetProbe(!own_items);
  if (mask == 0) {
    return false;
  }

  const auto& dest = bucket_[to];
  return dest.IsFull() ? false : true;
}

template <typename Key, typename Value, typename Policy>
template <typename U, typename V>
auto Segment<Key, Value, Policy>::InsertUniq(U&& key, V&& value, Hash_t key_hash) -> Iterator {
  const uint8_t bid = BucketIndex(key_hash);
  const uint8_t nid = NextBid(bid);

  Bucket& target = bucket_[bid];
  Bucket& neighbor = bucket_[nid];
  Bucket* insert_first = &target;

  uint8_t meta_hash = key_hash & kFpMask;
  unsigned ts = target.Size(), ns = neighbor.Size();
  bool probe = false;

  if (ts > ns) {
    insert_first = &neighbor;
    probe = true;
  }

  int slot = insert_first->FindEmptySlot();
  if (slot >= 0) {
    insert_first->Insert(slot, std::forward<U>(key), std::forward<V>(value), meta_hash, probe);

    return Iterator{uint8_t(insert_first - bucket_), uint8_t(slot)};
  }

  int displace_index = MoveToOther(true, nid, NextBid(nid));
  if (displace_index >= 0) {
    neighbor.Insert(displace_index, std::forward<U>(key), std::forward<V>(value), meta_hash, true);
    return Iterator{nid, uint8_t(displace_index)};
  }

  unsigned prev_idx = PrevBid(bid);
  displace_index = MoveToOther(false, bid, prev_idx);
  if (displace_index >= 0) {
    target.Insert(displace_index, std::forward<U>(key), std::forward<V>(value), meta_hash, false);
    return Iterator{bid, uint8_t(displace_index)};
  }

  // we balance stash fill rate  by starting from y % STASH_BUCKET_NUM.
  for (unsigned i = 0; i < STASH_BUCKET_NUM; ++i) {
    unsigned stash_pos = (bid + i) % STASH_BUCKET_NUM;
    int stash_slot = TryInsertToBucket(kNumBuckets + stash_pos, std::forward<U>(key),
                                       std::forward<V>(value), meta_hash, false);
    if (stash_slot >= 0) {
      target.SetStashPtr(stash_pos, meta_hash, &neighbor);
      return Iterator{uint8_t(kNumBuckets + stash_pos), uint8_t(stash_slot)};
    }
  }

  return Iterator{};
}

template <typename Key, typename Value, typename Policy>
template <bool UV>
std::enable_if_t<UV, unsigned> Segment<Key, Value, Policy>::CVCOnInsert(uint64_t ver_threshold,
                                                                        Hash_t key_hash,
                                                                        uint8_t bid_res[2]) const {
  const uint8_t bid = BucketIndex(key_hash);
  const uint8_t nid = NextBid(bid);

  const Bucket& target = bucket_[bid];
  const Bucket& neighbor = bucket_[nid];
  uint8_t first = target.Size() > neighbor.Size() ? nid : bid;
  unsigned cnt = 0;

  const Bucket& bfirst = bucket_[first];
  if (!bfirst.IsFull()) {
    if (!bfirst.IsEmpty() && bfirst.GetVersion() < ver_threshold) {
      bid_res[cnt++] = first;
    }
    return cnt;
  }

  // both nid and bid are full.
  const uint8_t after_next = NextBid(nid);

  if (CheckIfMovesToOther(true, nid, after_next)) {
    // We could tighten the checks here and below because
    // if nid is less than ver_threshold, than after_next won't be affected and won't cross
    // ver_threshold as well.
    if (bucket_[nid].GetVersion() < ver_threshold)
      bid_res[cnt++] = nid;

    if (!bucket_[after_next].IsEmpty() && bucket_[after_next].GetVersion() < ver_threshold)
      bid_res[cnt++] = after_next;

    return cnt;
  }

  const uint8_t prev_bid = PrevBid(bid);
  if (CheckIfMovesToOther(false, bid, prev_bid)) {
    if (bucket_[bid].GetVersion() < ver_threshold)
      bid_res[cnt++] = bid;

    if (!bucket_[prev_bid].IsEmpty() && bucket_[prev_bid].GetVersion() < ver_threshold)
      bid_res[cnt++] = prev_bid;

    return cnt;
  }

  // Important to repeat exactly the insertion logic of InsertUnique.
  for (unsigned i = 0; i < STASH_BUCKET_NUM; ++i) {
    unsigned stash_bid = kNumBuckets + ((bid + i) % STASH_BUCKET_NUM);
    const Bucket& stash = bucket_[stash_bid];
    if (!stash.IsFull()) {
      if (!stash.IsEmpty() && stash.GetVersion() < ver_threshold)
        bid_res[cnt++] = stash_bid;

      return cnt;
    }
  }

  return UINT16_MAX;
}

template <typename Key, typename Value, typename Policy>
template <bool UV>
std::enable_if_t<UV, unsigned> Segment<Key, Value, Policy>::CVCOnBump(uint64_t ver_threshold,
                                                                      unsigned bid, unsigned slot,
                                                                      Hash_t hash,
                                                                      uint8_t result_bid[2]) const {
  if (bid < kNumBuckets) {
    // right now we do not migrate entries from nid to bid, only from stash to normal buckets.
    return 0;
  }

  // Stash case.
  if (bucket_[bid].GetVersion() < ver_threshold) {
    // if we move an entry from a bucket that has lower version
    // then the destination bucket whatever it is - won't cross ver_threshold,
    // but we may need to save that entry.
    result_bid[0] = bid;
    return 1;
  }

  // At this point we know that the source (stash) bucket has version >= ver_threshold.
  unsigned result = 0;

  const uint8_t target_bid = BucketIndex(hash);
  const auto& target = bucket_[target_bid];

  if (!target.IsFull()) {
    if (target.GetVersion() < ver_threshold)
      result_bid[result++] = target_bid;

    return result;
  }

  const uint8_t probing_bid = NextBid(target_bid);
  const auto& probing = bucket_[probing_bid];

  if (!probing.IsFull()) {
    if (probing.GetVersion() < ver_threshold)
      result_bid[result++] = probing_bid;

    return result;
  }

  assert(result == 0);

  unsigned stash_pos = bid - kNumBuckets;
  uint8_t fp_hash = hash & kFpMask;

  auto find_stash = [&](unsigned, unsigned pos) {
    return stash_pos == pos ? slot : BucketType::kNanSlot;
  };

  if (target.GetVersion() < ver_threshold) {
    if (target.HasStashOverflow()) {
      result_bid[result++] = target_bid;
    } else {
      SlotId slot_id = target.IterateStash(fp_hash, false, find_stash).second;
      if (slot_id != BucketType::kNanSlot) {
        result_bid[result++] = target_bid;
      }
    }
  }

  if (probing.GetVersion() < ver_threshold) {
    if (probing.HasStashOverflow()) {
      result_bid[result++] = probing_bid;
    } else {
      SlotId slot_id = probing.IterateStash(fp_hash, true, find_stash).second;
      if (slot_id != BucketType::kNanSlot) {
        result_bid[result++] = probing_bid;
      }
    }
  }

  return result;
}

template <typename Key, typename Value, typename Policy>
template <typename Cb, typename HashFn>
bool Segment<Key, Value, Policy>::TraverseLogicalBucket(uint8_t bid, HashFn&& hfun, Cb&& cb) const {
  assert(bid < kNumBuckets);

  const Bucket& b = bucket_[bid];
  bool found = false;
  if (b.GetProbe(false)) {  // Check items that this bucket owns.
    b.ForEachSlot([&](SlotId slot, bool probe) {
      if (!probe) {
        found = true;
        cb(Iterator{bid, slot});
      }
    });
  }

  uint8_t nid = NextBid(bid);
  const Bucket& next = bucket_[nid];

  // check for probing entries in the next bucket, i.e. those that should reside in b.
  if (next.GetProbe(true)) {
    next.ForEachSlot([&](SlotId slot, bool probe) {
      if (probe) {
        found = true;
        assert(BucketIndex(hfun(next.key[slot])) == bid);
        cb(Iterator{nid, slot});
      }
    });
  }

  // Finally go over stash buckets and find those entries that belong to b.
  if (b.HasStash()) {
    // do not bother with overflow fps. Just go over all the stash buckets.
    for (uint8_t j = kNumBuckets; j < kTotalBuckets; ++j) {
      const auto& stashb = bucket_[j];
      stashb.ForEachSlot([&](SlotId slot, bool probe) {
        if (BucketIndex(hfun(stashb.key[slot])) == bid) {
          found = true;
          cb(Iterator{j, slot});
        }
      });
    }
  }

  return found;
}

template <typename Key, typename Value, typename Policy>
size_t Segment<Key, Value, Policy>::SlowSize() const {
  size_t res = 0;
  for (unsigned i = 0; i < kTotalBuckets; ++i) {
    res += bucket_[i].Size();
  }
  return res;
}

template <typename Key, typename Value, typename Policy>
auto Segment<Key, Value, Policy>::FindValidStartingFrom(unsigned bid, unsigned slot) const
    -> Iterator {
  uint32_t mask = bucket_[bid].GetBusy();
  mask >>= slot;
  if (mask)
    return Iterator(bid, slot + __builtin_ctz(mask));

  ++bid;
  while (bid < kTotalBuckets) {
    uint32_t mask = bucket_[bid].GetBusy();
    if (mask) {
      return Iterator(bid, __builtin_ctz(mask));
    }
    ++bid;
  }
  return Iterator{};
}

template <typename Key, typename Value, typename Policy>
auto Segment<Key, Value, Policy>::BumpUp(uint8_t bid, SlotId slot, Hash_t key_hash) -> Iterator {
  auto& from = bucket_[bid];

  uint8_t target_bid = BucketIndex(key_hash);
  uint8_t nid = NextBid(target_bid);
  uint8_t fp_hash = key_hash & kFpMask;
  assert(fp_hash == from.Fp(slot));

  if (bid < kNumBuckets) {
    // non stash case.
    if (slot > 0) {
      from.Swap(slot - 1, slot);
      return Iterator{bid, uint8_t(slot - 1)};
    }
    // TODO: We could promote further, by swapping probing bucket with its previous one.
    return  Iterator{bid, slot};
  }

  // stash bucket
  // We swap the item with the item in the "normal" bucket in the last slot.
  unsigned stash_pos = bid - kNumBuckets;

  // If we have an empty space for some reason just unload the stash entry.
  if (Iterator it = TryMoveFromStash(stash_pos, slot, key_hash); it.found()) {
    // TryMoveFromStash handles versions internally.
    from.Delete(slot);
    return it;
  }

  // determine which bucket one we gonna swap.
  // we swap with the bucket the references the stash entry, not necessary its owning
  // bucket.
  auto& target = bucket_[target_bid];
  auto& next = bucket_[nid];

  // bucket_offs - 0 if exact bucket, 1 if neighbour
  unsigned bucket_offs = target.UnsetStashPtr(fp_hash, stash_pos, &next);
  uint8_t swap_bid = (target_bid + bucket_offs) % kNumBuckets;
  auto& swapb = bucket_[swap_bid];

  constexpr unsigned kLastSlot = kNumSlots - 1;
  assert(swapb.GetBusy() & (1 << kLastSlot));

  uint8_t swap_fp = swapb.Fp(kLastSlot);

  // is_probing for the existing entry in swapb. It's unrelated to bucket_offs,
  // i.e. it could be true even if bucket_offs is 0.
  bool is_probing = swapb.GetProbe(true) & (1 << kLastSlot);

  // swap keys, values and fps. update slots meta.
  std::swap(from.key[slot], swapb.key[kLastSlot]);
  std::swap(from.value[slot], swapb.value[kLastSlot]);
  from.Delete(slot);
  from.SetHash(slot, swap_fp, false);

  swapb.Delete(kLastSlot);
  swapb.SetHash(kLastSlot, fp_hash, bucket_offs == 1);

  // update versions.
  if constexpr (USE_VERSION) {
    uint64_t from_ver = from.GetVersion();
    uint64_t swap_ver = swapb.GetVersion();
    if (from_ver < swap_ver) {
      from.SetVersion(swap_ver);
    } else {
      swapb.SetVersion(from_ver);
    }
  }

  // update ptr for swapped items
  if (is_probing) {
    unsigned prev_bid = PrevBid(swap_bid);
    auto& prevb = bucket_[prev_bid];
    prevb.SetStashPtr(stash_pos, swap_fp, &swapb);
  } else {
    // stash_ptr resides in the current or the next bucket.
    unsigned next_bid = NextBid(swap_bid);
    swapb.SetStashPtr(stash_pos, swap_fp, bucket_ + next_bid);
  }

  return Iterator{swap_bid, kLastSlot};
}

template <typename Key, typename Value, typename Policy>
template <typename HFunc>
unsigned Segment<Key, Value, Policy>::UnloadStash(HFunc&& hfunc) {
  unsigned moved = 0;

  for (unsigned i = 0; i < STASH_BUCKET_NUM; ++i) {
    unsigned bid = kNumBuckets + i;
    Bucket& stash = bucket_[bid];
    uint32_t invalid_mask = 0;

    auto cb = [&](unsigned slot, bool probe) {
      auto& key = Key(bid, slot);
      Hash_t hash = hfunc(key);
      Iterator res = TryMoveFromStash(i, slot, hash);
      if (res.found()) {
        ++moved;
        invalid_mask |= (1u << slot);
      }
    };

    stash.ForEachSlot(cb);
    stash.ClearSlots(invalid_mask);
  }

  return moved;
}

}  // namespace detail
}  // namespace dfly
