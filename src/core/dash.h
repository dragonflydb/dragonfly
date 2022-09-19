// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory_resource>
#include <vector>

#include "core/dash_internal.h"

namespace dfly {

// DASH: Dynamic And Scalable Hashing.
// TODO: We could name it DACHE: Dynamic and Adaptive caCHE.
// After all, we added additional improvements we added as part of the dragonfly project,
// that probably justify a right to choose our own name for this data structure.
struct BasicDashPolicy {
  enum { kSlotNum = 12, kBucketNum = 64, kStashBucketNum = 2 };
  static constexpr bool kUseVersion = false;

  template <typename U> static void DestroyValue(const U&) {
  }
  template <typename U> static void DestroyKey(const U&) {
  }

  template <typename U, typename V> static bool Equal(U&& u, V&& v) {
    return u == v;
  }
};

template <typename _Key, typename _Value, typename Policy>
class DashTable : public detail::DashTableBase {
  DashTable(const DashTable&) = delete;
  DashTable& operator=(const DashTable&) = delete;

  struct SegmentPolicy {
    static constexpr unsigned NUM_SLOTS = Policy::kSlotNum;
    static constexpr unsigned BUCKET_CNT = Policy::kBucketNum;
    static constexpr unsigned STASH_BUCKET_NUM = Policy::kStashBucketNum;
    static constexpr bool USE_VERSION = Policy::kUseVersion;
  };

  using Base = detail::DashTableBase;
  using SegmentType = detail::Segment<_Key, _Value, SegmentPolicy>;
  using SegmentIterator = typename SegmentType::Iterator;

 public:
  using Key_t = _Key;
  using Value_t = _Value;
  using Segment_t = SegmentType;

  //! Number of "official" buckets that are used to position a key. In other words, does not include
  //! stash buckets.
  static constexpr unsigned kLogicalBucketNum = Policy::kBucketNum;

  //! Total number of buckets in a segment (including stash).
  static constexpr unsigned kPhysicalBucketNum = SegmentType::kTotalBuckets;
  static constexpr unsigned kBucketWidth = Policy::kSlotNum;
  static constexpr double kTaxAmount = SegmentType::kTaxSize;
  static constexpr size_t kSegBytes = sizeof(SegmentType);
  static constexpr size_t kSegCapacity = SegmentType::capacity();
  static constexpr bool kUseVersion = Policy::kUseVersion;

  // if IsSingleBucket is true - iterates only over a single bucket.
  template <bool IsConst, bool IsSingleBucket = false> class Iterator;

  using const_iterator = Iterator<true>;
  using iterator = Iterator<false>;

  using const_bucket_iterator = Iterator<true, true>;
  using bucket_iterator = Iterator<false, true>;
  using Cursor = detail::DashCursor;

  struct HotspotBuckets {
    static constexpr size_t kRegularBuckets = 4;
    static constexpr size_t kNumBuckets = kRegularBuckets + Policy::kStashBucketNum;

    struct ByType {
      bucket_iterator regular_buckets[kRegularBuckets];
      bucket_iterator stash_buckets[Policy::kStashBucketNum];
    };

    union Probes {
      ByType by_type;
      bucket_iterator arr[kNumBuckets];

      Probes() : arr() {
      }
    } probes;

    // id must be in the range [0, kNumBuckets).
    bucket_iterator at(unsigned id) const {
      return probes.arr[id];
    }

    unsigned num_buckets;
    // key_hash of a key that we try to insert.
    // I use it as pseudo-random number in my gc/eviction heuristics.
    uint64_t key_hash;
  };

  struct DefaultEvictionPolicy {
    static constexpr bool can_gc = false;
    static constexpr bool can_evict = false;

    bool CanGrow(const DashTable&) {
      return true;
    }

    void RecordSplit(SegmentType* segment) {
    }
    /*
       /// Required interface in case can_gc is true
       // Returns number of garbage collected items deleted. 0 - means nothing has been
       // deleted.
       unsigned GarbageCollect(const EvictionBuckets& eb, DashTable* me) const {
         return 0;
       }

       // Required interface in case can_gc is true
       // returns number of items evicted from the table.
       // 0 means - nothing has been evicted.
       unsigned Evict(const EvictionBuckets& eb, DashTable* me) {
         return 0;
       }
   */
  };

  DashTable(size_t capacity_log = 1, const Policy& policy = Policy{},
            std::pmr::memory_resource* mr = std::pmr::get_default_resource());
  ~DashTable();

  void Reserve(size_t size);

  // false for duplicate, true if inserted.
  template <typename U, typename V> std::pair<iterator, bool> Insert(U&& key, V&& value) {
    DefaultEvictionPolicy policy;
    return InsertInternal(std::forward<U>(key), std::forward<V>(value), policy);
  }

  template <typename U, typename V, typename EvictionPolicy>
  std::pair<iterator, bool> Insert(U&& key, V&& value, EvictionPolicy& ev) {
    return InsertInternal(std::forward<U>(key), std::forward<V>(value), ev);
  }

  template <typename U> const_iterator Find(U&& key) const;
  template <typename U> iterator Find(U&& key);

  // it must be valid.
  void Erase(iterator it);

  size_t Erase(const Key_t& k);

  iterator begin() {
    iterator it{this, 0, 0, 0};
    it.Seek2Occupied();
    return it;
  }

  const_iterator cbegin() const {
    const_iterator it{this, 0, 0, 0};
    it.Seek2Occupied();
    return it;
  }

  iterator end() const {
    return iterator{};
  }
  const_iterator cend() const {
    return const_iterator{};
  }

  using Base::depth;
  using Base::size;
  using Base::unique_segments;

  // Direct access to the segment for debugging purposes.
  Segment_t* GetSegment(unsigned segment_id) {
    return segment_[segment_id];
  }

  template <typename U> uint64_t DoHash(const U& k) const {
    return policy_.HashFn(k);
  }

  // Flat memory usage (allocated) of the table, not including the the memory allocated
  // by the hosted objects.
  size_t mem_usage() const {
    return segment_.capacity() * sizeof(void*) + sizeof(SegmentType) * unique_segments_;
  }

  size_t bucket_count() const {
    return unique_segments_ * SegmentType::capacity();
  }

  // Overall capacity of the table (including stash buckets).
  size_t capacity() const {
    return bucket_count();
  }

  double load_factor() const {
    return double(size()) / (SegmentType::capacity() * unique_segments());
  }

  // Traverses over a single bucket in table and calls cb(iterator) 0 or more
  // times. if cursor=0 starts traversing from the beginning, otherwise continues from where it
  // stopped. returns 0 if the supplied cursor reached end of traversal. Traverse iterates at bucket
  // granularity, which means for each non-empty bucket it calls cb per each entry in the bucket
  // before returning. Unlike begin/end interface, traverse is stable during table mutations.
  // It guarantees that if key exists (1)at the beginning of traversal, (2) stays in the table
  // during the traversal, then Traverse() will eventually reach it even when the
  // table shrinks or grows.
  // Returns: cursor that is guaranteed to be less than 2^40.
  template <typename Cb> Cursor Traverse(Cursor curs, Cb&& cb);

  // Takes an iterator pointing to an entry in a dash bucket and traverses all bucket's entries by
  // calling cb(iterator) for every non-empty slot. The iteration goes over a physical bucket.
  template <typename Cb> void TraverseBucket(const_iterator it, Cb&& cb);

  // Discards slots information.
  static const_bucket_iterator BucketIt(const_iterator it) {
    return const_bucket_iterator{it.owner_, it.seg_id_, it.bucket_id_, 0};
  }

  // Seeks to the first occupied slot if exists in the bucket.
  const_bucket_iterator BucketIt(unsigned segment_id, unsigned bucket_id) const {
    return const_bucket_iterator{this, segment_id, uint8_t(bucket_id)};
  }

  iterator GetIterator(unsigned segment_id, unsigned bucket_id, unsigned slot_id) {
    return iterator{this, segment_id, uint8_t(bucket_id), uint8_t(slot_id)};
  }

  const_bucket_iterator CursorToBucketIt(Cursor c) const {
    return const_bucket_iterator{this, c.segment_id(global_depth_), c.bucket_id(), 0};
  }

  // Capture Version Change. Runs cb(it) on every bucket! (not entry) in the table whose version
  // would potentially change upon insertion of 'k'.
  // In practice traversal is limited to a single segment. The operation is read-only and
  // simulates insertion process. 'cb' must accept bucket_iterator.
  // Note: the interface a bit hacky.
  // The functions call cb on physical buckets with version smaller than ver_threshold that
  // due to entry movements might update its version to version greater than ver_threshold.
  //
  // These are not const functions because they send non-const iterators that allow
  // updating contents/versions of the passed iterators.
  template <typename U, typename Cb>
  void CVCUponInsert(uint64_t ver_threshold, const U& key, Cb&& cb);

  template <typename Cb> void CVCUponBump(uint64_t ver_threshold, const_iterator it, Cb&& cb);

  void Clear();

  // Returns true if an element was deleted i.e the rightmost slot was busy.
  bool ShiftRight(bucket_iterator it);

  template <typename BumpPolicy> iterator BumpUp(iterator it, const BumpPolicy& bp) {
    SegmentIterator seg_it =
        segment_[it.seg_id_]->BumpUp(it.bucket_id_, it.slot_id_, DoHash(it->first), bp);

    return iterator{this, it.seg_id_, seg_it.index, seg_it.slot};
  }

  uint64_t garbage_collected() const {
    return garbage_collected_;
  }

  uint64_t stash_unloaded() const {
    return stash_unloaded_;
  }

 private:
  template <typename U, typename V, typename EvictionPolicy>
  std::pair<iterator, bool> InsertInternal(U&& key, V&& value, EvictionPolicy& policy);

  void IncreaseDepth(unsigned new_depth);
  void Split(uint32_t seg_id);

  // Segment directory contains multiple segment pointers, some of them pointing to
  // the same object. IterateDistinct goes over all distinct segments in the table.
  template <typename Cb> void IterateDistinct(Cb&& cb);

  size_t NextSeg(size_t sid) const {
    size_t delta = (1u << (global_depth_ - segment_[sid]->local_depth()));
    return sid + delta;
  }

  auto EqPred() const {
    return [p = &policy_](const auto& a, const auto& b) -> bool { return p->Equal(a, b); };
  }

  Policy policy_;
  std::pmr::vector<SegmentType*> segment_;

  uint64_t garbage_collected_ = 0;
  uint64_t stash_unloaded_ = 0;
};  // DashTable

template <typename _Key, typename _Value, typename Policy>
template <bool IsConst, bool IsSingleBucket>
class DashTable<_Key, _Value, Policy>::Iterator {
  using Owner = std::conditional_t<IsConst, const DashTable, DashTable>;

  Owner* owner_;
  uint32_t seg_id_;
  uint8_t bucket_id_;
  uint8_t slot_id_;

  friend class DashTable;

  Iterator(Owner* me, uint32_t seg_id, uint8_t bid, uint8_t sid)
      : owner_(me), seg_id_(seg_id), bucket_id_(bid), slot_id_(sid) {
  }

  Iterator(Owner* me, uint32_t seg_id, uint8_t bid)
      : owner_(me), seg_id_(seg_id), bucket_id_(bid), slot_id_(0) {
    Seek2Occupied();
  }

 public:
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;

  // Copy constructor from iterator to const_iterator.
  template <bool TIsConst = IsConst, bool TIsSingleB,
            typename std::enable_if<TIsConst>::type* = nullptr>
  Iterator(const Iterator<!TIsConst, TIsSingleB>& other) noexcept
      : owner_(other.owner_), seg_id_(other.seg_id_), bucket_id_(other.bucket_id_),
        slot_id_(other.slot_id_) {
  }

  // Copy constructor from iterator to bucket_iterator and vice versa.
  template <bool TIsSingle>
  Iterator(const Iterator<IsConst, TIsSingle>& other) noexcept
      : owner_(other.owner_), seg_id_(other.seg_id_), bucket_id_(other.bucket_id_),
        slot_id_(IsSingleBucket ? 0 : other.slot_id_) {
    // if this - is a bucket_iterator - we reset slot_id to the first occupied space.
    if constexpr (IsSingleBucket) {
      Seek2Occupied();
    }
  }

  Iterator() : owner_(nullptr), seg_id_(0), bucket_id_(0), slot_id_(0) {
  }

  Iterator(const Iterator& other) = default;

  Iterator(Iterator&& other) = default;

  Iterator& operator=(const Iterator& other) = default;
  Iterator& operator=(Iterator&& other) = default;

  // pre
  Iterator& operator++() {
    ++slot_id_;
    Seek2Occupied();
    return *this;
  }

  Iterator& operator+=(int delta) {
    slot_id_ += delta;
    Seek2Occupied();
    return *this;
  }

  detail::IteratorPair<Key_t, Value_t> operator->() {
    auto* seg = owner_->segment_[seg_id_];
    return detail::IteratorPair<Key_t, Value_t>{seg->Key(bucket_id_, slot_id_),
                                                seg->Value(bucket_id_, slot_id_)};
  }

  const detail::IteratorPair<Key_t, Value_t> operator->() const {
    auto* seg = owner_->segment_[seg_id_];
    return detail::IteratorPair<Key_t, Value_t>{seg->Key(bucket_id_, slot_id_),
                                                seg->Value(bucket_id_, slot_id_)};
  }

  // Make it self-contained. Does not need container::end().
  bool is_done() const {
    return owner_ == nullptr;
  }

  template <bool B = Policy::kUseVersion> std::enable_if_t<B, uint64_t> GetVersion() const {
    assert(owner_ && seg_id_ < owner_->segment_.size());
    return owner_->segment_[seg_id_]->GetVersion(bucket_id_);
  }

  // Returns the minimum version of the physical bucket that this iterator points to.
  // Note: In an ideal world I would introduce a bucket iterator...
  /*template <bool B = Policy::kUseVersion> std::enable_if_t<B, uint64_t> MinVersion() const {
    return owner_->segment_[seg_id_]->MinVersion(bucket_id_);
  }*/

  template <bool B = Policy::kUseVersion> std::enable_if_t<B> SetVersion(uint64_t v) {
    return owner_->segment_[seg_id_]->SetVersion(bucket_id_, v);
  }

  friend bool operator==(const Iterator& lhs, const Iterator& rhs) {
    if (lhs.owner_ == nullptr && rhs.owner_ == nullptr)
      return true;
    return lhs.owner_ == rhs.owner_ && lhs.seg_id_ == rhs.seg_id_ &&
           lhs.bucket_id_ == rhs.bucket_id_ && lhs.slot_id_ == rhs.slot_id_;
  }

  friend bool operator!=(const Iterator& lhs, const Iterator& rhs) {
    return !(lhs == rhs);
  }

  // Bucket resolution cursor that is safe to use with insertions/removals.
  // Serves as a hint really to the placement of the original item, i.e. the item
  // could have moved.
  detail::DashCursor bucket_cursor() const {
    return detail::DashCursor(owner_->global_depth_, seg_id_, bucket_id_);
  }

  unsigned bucket_id() const {
    return bucket_id_;
  }

  unsigned slot_id() const {
    return slot_id_;
  }

  unsigned segment_id() const {
    return seg_id_;
  }

 private:
  void Seek2Occupied();
};  // Iterator

/**
  _____                 _                           _        _   _
 |_   _|               | |                         | |      | | (_)
   | |  _ __ ___  _ __ | | ___ _ __ ___   ___ _ __ | |_ __ _| |_ _  ___  _ __
   | | | '_ ` _ \| '_ \| |/ _ \ '_ ` _ \ / _ \ '_ \| __/ _` | __| |/ _ \| '_ \
  _| |_| | | | | | |_) | |  __/ | | | | |  __/ | | | || (_| | |_| | (_) | | | |
 |_____|_| |_| |_| .__/|_|\___|_| |_| |_|\___|_| |_|\__\__,_|\__|_|\___/|_| |_|
                 | |
                 |_|

**/

template <typename _Key, typename _Value, typename Policy>
template <bool IsConst, bool IsSingleBucket>
void DashTable<_Key, _Value, Policy>::Iterator<IsConst, IsSingleBucket>::Seek2Occupied() {
  if (owner_ == nullptr)
    return;
  assert(seg_id_ < owner_->segment_.size());

  if constexpr (IsSingleBucket) {
    const auto& b = owner_->segment_[seg_id_]->GetBucket(bucket_id_);
    uint32_t mask = b.GetBusy() >> slot_id_;
    if (mask) {
      int slot = __builtin_ctz(mask);
      slot_id_ += slot;
      return;
    }
  } else {
    while (seg_id_ < owner_->segment_.size()) {
      auto seg_it = owner_->segment_[seg_id_]->FindValidStartingFrom(bucket_id_, slot_id_);
      if (seg_it.found()) {
        bucket_id_ = seg_it.index;
        slot_id_ = seg_it.slot;
        return;
      }
      seg_id_ = owner_->NextSeg(seg_id_);
      bucket_id_ = slot_id_ = 0;
    }
  }
  owner_ = nullptr;
}

template <typename _Key, typename _Value, typename Policy>
DashTable<_Key, _Value, Policy>::DashTable(size_t capacity_log, const Policy& policy,
                                           std::pmr::memory_resource* mr)
    : Base(capacity_log), policy_(policy), segment_(mr) {
  segment_.resize(unique_segments_);
  std::pmr::polymorphic_allocator<SegmentType> pa(mr);

  // I assume we have enough memory to create the initial table and do not check allocations.
  for (auto& ptr : segment_) {
    ptr = pa.allocate(1);
    pa.construct(ptr, global_depth_);  //   new SegmentType(global_depth_);
  }
}

template <typename _Key, typename _Value, typename Policy>
DashTable<_Key, _Value, Policy>::~DashTable() {
  Clear();
  auto* resource = segment_.get_allocator().resource();
  std::pmr::polymorphic_allocator<SegmentType> pa(resource);
  using alloc_traits = std::allocator_traits<decltype(pa)>;

  IterateDistinct([&](SegmentType* seg) {
    alloc_traits::destroy(pa, seg);
    alloc_traits::deallocate(pa, seg, 1);
    return false;
  });
}

template <typename _Key, typename _Value, typename Policy>
template <typename U, typename Cb>
void DashTable<_Key, _Value, Policy>::CVCUponInsert(uint64_t ver_threshold, const U& key, Cb&& cb) {
  uint64_t key_hash = DoHash(key);
  uint32_t seg_id = SegmentId(key_hash);
  assert(seg_id < segment_.size());
  const SegmentType* target = segment_[seg_id];

  uint8_t bids[2];
  unsigned num_touched = target->CVCOnInsert(ver_threshold, key_hash, bids);
  if (num_touched < UINT16_MAX) {
    for (unsigned i = 0; i < num_touched; ++i) {
      cb(bucket_iterator{this, seg_id, bids[i]});
    }
    return;
  }

  static_assert(kPhysicalBucketNum < 0xFF, "");

  // Segment is full, we need to return the whole segment, because it can be split
  // and its entries can be reshuffled into different buckets.
  for (uint8_t i = 0; i < kPhysicalBucketNum; ++i) {
    if (target->GetVersion(i) < ver_threshold && !target->GetBucket(i).IsEmpty()) {
      cb(bucket_iterator{this, seg_id, i});
    }
  }
}

template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
void DashTable<_Key, _Value, Policy>::CVCUponBump(uint64_t ver_upperbound, const_iterator it,
                                                  Cb&& cb) {
  uint64_t key_hash = DoHash(it->first);
  uint32_t seg_id = it.segment_id();
  assert(seg_id < segment_.size());
  const SegmentType* target = segment_[seg_id];

  uint8_t bids[3];
  unsigned num_touched =
      target->CVCOnBump(ver_upperbound, it.bucket_id(), it.slot_id(), key_hash, bids);

  for (unsigned i = 0; i < num_touched; ++i) {
    cb(bucket_iterator{this, seg_id, bids[i]});
  }
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::Clear() {
  auto cb = [this](SegmentType* seg) {
    seg->TraverseAll([this, seg](const SegmentIterator& it) {
      policy_.DestroyKey(seg->Key(it.index, it.slot));
      policy_.DestroyValue(seg->Value(it.index, it.slot));
    });
    seg->Clear();
    return false;
  };

  IterateDistinct(cb);
  size_ = 0;

  // Consider the following case: table with 8 segments overall, 4 distinct.
  // S1, S1, S1, S1, S2, S3, S4, S4
  /* This corresponds to the tree:
            R
          /  \
        S1   /\
            /\ S4
           S2 S3
     We want to collapse this tree into, say, 2 segment directory.
     That means we need to keep S1, S2 but delete S3, S4.
     That means, we need to move representative segments until we reached the desired size
     and then erase all other distinct segments.
  **********/
  if (global_depth_ > initial_depth_) {
    std::pmr::polymorphic_allocator<SegmentType> pa(segment_.get_allocator());
    using alloc_traits = std::allocator_traits<decltype(pa)>;

    size_t dest = 0, src = 0;
    size_t new_size = (1 << initial_depth_);

    while (src < segment_.size()) {
      auto* seg = segment_[src];
      size_t next_src = NextSeg(src);  // must do before because NextSeg is dependent on seg.
      if (dest < new_size) {
        seg->set_local_depth(initial_depth_);
        segment_[dest++] = seg;
      } else {
        alloc_traits::destroy(pa, seg);
        alloc_traits::deallocate(pa, seg, 1);
      }

      src = next_src;
    }

    global_depth_ = initial_depth_;
    unique_segments_ = new_size;
    segment_.resize(new_size);
  }
}

template <typename _Key, typename _Value, typename Policy>
bool DashTable<_Key, _Value, Policy>::ShiftRight(bucket_iterator it) {
  auto* seg = segment_[it.seg_id_];

  typename Segment_t::Hash_t hash_val = 0;
  auto& bucket = seg->GetBucket(it.bucket_id_);

  if (bucket.GetBusy() & (1 << (kBucketWidth - 1))) {
    it.slot_id_ = kBucketWidth - 1;
    hash_val = DoHash(it->first);
    policy_.DestroyKey(it->first);
    policy_.DestroyValue(it->second);
  }

  bool deleted = seg->ShiftRight(it.bucket_id_, hash_val);
  size_ -= unsigned(deleted);

  return deleted;
}

template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
void DashTable<_Key, _Value, Policy>::IterateDistinct(Cb&& cb) {
  size_t i = 0;
  while (i < segment_.size()) {
    auto* seg = segment_[i];
    size_t next_id = NextSeg(i);
    if (cb(seg))
      break;
    i = next_id;
  }
}

template <typename _Key, typename _Value, typename Policy>
template <typename U>
auto DashTable<_Key, _Value, Policy>::Find(U&& key) const -> const_iterator {
  uint64_t key_hash = DoHash(key);
  size_t seg_id = SegmentId(key_hash);  // seg_id takes up global_depth_ high bits.
  const auto* target = segment_[seg_id];

  // Hash structure is like this: [SSUUUUBF], where S is segment id, U - unused,
  // B - bucket id and F is a fingerprint. Segment id is needed to identify the correct segment.
  // Once identified, the segment instance uses the lower part of hash to locate the key.
  // It uses 8 least significant bits for a fingerprint and few more bits for bucket id.
  auto seg_it = target->FindIt(key, key_hash, EqPred());

  if (seg_it.found()) {
    return const_iterator{this, seg_id, seg_it.index, seg_it.slot};
  }
  return const_iterator{};
}

template <typename _Key, typename _Value, typename Policy>
template <typename U>
auto DashTable<_Key, _Value, Policy>::Find(U&& key) -> iterator {
  uint64_t key_hash = DoHash(key);
  uint32_t segid = SegmentId(key_hash);
  const auto* target = segment_[segid];

  auto seg_it = target->FindIt(key, key_hash, EqPred());
  if (seg_it.found()) {
    return iterator{this, segid, seg_it.index, seg_it.slot};
  }
  return iterator{};
}

template <typename _Key, typename _Value, typename Policy>
size_t DashTable<_Key, _Value, Policy>::Erase(const Key_t& key) {
  uint64_t key_hash = DoHash(key);
  size_t x = SegmentId(key_hash);
  auto* target = segment_[x];
  auto it = target->FindIt(key, key_hash, EqPred());
  if (!it.found())
    return 0;

  policy_.DestroyKey(target->Key(it.index, it.slot));
  policy_.DestroyValue(target->Value(it.index, it.slot));
  target->Delete(it, key_hash);
  --size_;

  return 1;
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::Erase(iterator it) {
  auto* target = segment_[it.seg_id_];
  uint64_t key_hash = DoHash(it->first);
  SegmentIterator sit{it.bucket_id_, it.slot_id_};

  policy_.DestroyKey(it->first);
  policy_.DestroyValue(it->second);

  target->Delete(sit, key_hash);
  --size_;
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::Reserve(size_t size) {
  if (size <= bucket_count())
    return;

  size_t sg_floor = (size - 1) / SegmentType::capacity();
  if (sg_floor < segment_.size()) {
    return;
  }
  assert(sg_floor > 1u);
  unsigned new_depth = 1 + (63 ^ __builtin_clzll(sg_floor));

  IncreaseDepth(new_depth);
}

template <typename _Key, typename _Value, typename Policy>
template <typename U, typename V, typename EvictionPolicy>
auto DashTable<_Key, _Value, Policy>::InsertInternal(U&& key, V&& value, EvictionPolicy& ev)
    -> std::pair<iterator, bool> {
  uint64_t key_hash = DoHash(key);
  uint32_t target_seg_id = SegmentId(key_hash);

  while (true) {
    // Keep last global_depth_ msb bits of the hash.
    assert(target_seg_id < segment_.size());
    SegmentType* target = segment_[target_seg_id];

    // Load heap allocated segment data - to avoid TLB miss when accessing the bucket.
    __builtin_prefetch(target, 0, 1);

    auto [it, res] =
        target->Insert(std::forward<U>(key), std::forward<V>(value), key_hash, EqPred());

    if (res) {  // success
      ++size_;
      return std::make_pair(iterator{this, target_seg_id, it.index, it.slot}, true);
    }

    /*duplicate insert, insertion failure*/
    if (it.found()) {
      return std::make_pair(iterator{this, target_seg_id, it.index, it.slot}, false);
    }

    // At this point we must split the segment.
    // try garbage collect or evict.
    if constexpr (EvictionPolicy::can_evict || EvictionPolicy::can_gc) {
      // Try gc.
      uint8_t bid[HotspotBuckets::kRegularBuckets];
      SegmentType::FillProbeArray(key_hash, bid);
      HotspotBuckets hotspot;
      hotspot.key_hash = key_hash;

      for (unsigned j = 0; j < HotspotBuckets::kRegularBuckets; ++j) {
        hotspot.probes.by_type.regular_buckets[j] = bucket_iterator{this, target_seg_id, bid[j]};
      }

      for (unsigned i = 0; i < Policy::kStashBucketNum; ++i) {
        hotspot.probes.by_type.stash_buckets[i] =
            bucket_iterator{this, target_seg_id, uint8_t(kLogicalBucketNum + i), 0};
      }
      hotspot.num_buckets = HotspotBuckets::kNumBuckets;

      // The difference between gc and eviction is that gc can be applied even if
      // the table can grow since we throw away logically deleted items.
      // For eviction to be applied we should reach the growth limit.
      if constexpr (EvictionPolicy::can_gc) {
        unsigned res = ev.GarbageCollect(hotspot, this);
        garbage_collected_ += res;
        if (res) {
          // We succeeded to gc. Lets continue with the momentum.
          // In terms of API abuse it's an awful hack, just to see if it works.
          /*unsigned start = (bid[HotspotBuckets::kNumBuckets - 1] + 1) % kLogicalBucketNum;
          for (unsigned i = 0; i < HotspotBuckets::kNumBuckets; ++i) {
            uint8_t id = (start + i) % kLogicalBucketNum;
            buckets.probes.arr[i] = bucket_iterator{this, target_seg_id, id};
          }
          garbage_collected_ += ev.GarbageCollect(buckets, this);
          */
          continue;
        }
      }

      auto hash_fn = [this](const auto& k) { return policy_.HashFn(k); };
      unsigned moved = target->UnloadStash(hash_fn);
      if (moved > 0) {
        stash_unloaded_ += moved;
        continue;
      }

      // We evict only if our policy says we can not grow
      if constexpr (EvictionPolicy::can_evict) {
        bool can_grow = ev.CanGrow(*this);
        if (!can_grow) {
          unsigned res = ev.Evict(hotspot, this);
          if (res)
            continue;
        }
      }
    }

    if (!ev.CanGrow(*this)) {
      throw std::bad_alloc{};
    }

    // Split the segment.
    if (target->local_depth() == global_depth_) {
      IncreaseDepth(global_depth_ + 1);

      target_seg_id = SegmentId(key_hash);
      assert(target_seg_id < segment_.size() && segment_[target_seg_id] == target);
    }

    ev.RecordSplit(target);
    Split(target_seg_id);
  }

  return std::make_pair(iterator{}, false);
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::IncreaseDepth(unsigned new_depth) {
  assert(!segment_.empty());
  assert(new_depth > global_depth_);
  size_t prev_sz = segment_.size();
  size_t repl_cnt = 1ul << (new_depth - global_depth_);
  segment_.resize(1ul << new_depth);

  for (int i = prev_sz - 1; i >= 0; --i) {
    size_t offs = i * repl_cnt;
    std::fill(segment_.begin() + offs, segment_.begin() + offs + repl_cnt, segment_[i]);
  }
  global_depth_ = new_depth;
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::Split(uint32_t seg_id) {
  SegmentType* source = segment_[seg_id];

  size_t chunk_size = 1u << (global_depth_ - source->local_depth());
  size_t start_idx = seg_id & (~(chunk_size - 1));
  assert(segment_[start_idx] == source && segment_[start_idx + chunk_size - 1] == source);
  std::pmr::polymorphic_allocator<SegmentType> alloc(segment_.get_allocator().resource());
  SegmentType* target = alloc.allocate(1);
  alloc.construct(target, source->local_depth() + 1);

  auto hash_fn = [this](const auto& k) { return policy_.HashFn(k); };

  source->Split(std::move(hash_fn), target);  // increases the depth.
  ++unique_segments_;

  for (size_t i = start_idx + chunk_size / 2; i < start_idx + chunk_size; ++i) {
    segment_[i] = target;
  }
}

template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
auto DashTable<_Key, _Value, Policy>::Traverse(Cursor curs, Cb&& cb) -> Cursor {
  if (curs.bucket_id() >= kLogicalBucketNum)  // sanity.
    return 0;

  uint32_t sid = curs.segment_id(global_depth_);
  uint8_t bid = curs.bucket_id();

  auto hash_fun = [this](const auto& k) { return policy_.HashFn(k); };

  bool fetched = false;

  // We fix bid and go over all segments. Once we reach the end we increase bid and repeat.
  do {
    SegmentType* s = segment_[sid];
    assert(s);

    auto dt_cb = [&](const SegmentIterator& it) { cb(iterator{this, sid, it.index, it.slot}); };

    fetched = s->TraverseLogicalBucket(bid, hash_fun, std::move(dt_cb));
    sid = NextSeg(sid);
    if (sid >= segment_.size()) {
      sid = 0;
      ++bid;

      if (bid >= kLogicalBucketNum)
        return 0;  // "End of traversal" cursor.
    }
  } while (!fetched);

  return Cursor{global_depth_, sid, bid};
}

template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
void DashTable<_Key, _Value, Policy>::TraverseBucket(const_iterator it, Cb&& cb) {
  SegmentType& s = *segment_[it.seg_id_];
  const auto& b = s.GetBucket(it.bucket_id_);
  b.ForEachSlot([&](uint8_t slot, bool probe) {
    cb(iterator{this, it.seg_id_, it.bucket_id_, slot});
  });
}

}  // namespace dfly
