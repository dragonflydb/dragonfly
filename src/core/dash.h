// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory_resource>
#include <vector>

#include "core/dash_internal.h"

namespace dfly {

// DASH: Dynamic And Scalable Hashing.
// TODO: We could name it DACHE: Dynamic and Adaptive caCHE.
// After all, we added additionaly improvements we added as part of the dragonfly project,
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

  //! Number of "official" buckets that are used to position a key. In other words, does not include
  //! stash buckets.
  static constexpr unsigned kLogicalBucketNum = Policy::kBucketNum;

  //! Total number of buckets in a segment (including stash).
  static constexpr unsigned kPhysicalBucketNum = SegmentType::kTotalBuckets;
  static constexpr unsigned kBucketSize = Policy::kSlotNum;
  static constexpr double kTaxAmount = SegmentType::kTaxSize;
  static constexpr size_t kSegBytes = sizeof(SegmentType);
  static constexpr size_t kSegCapacity = SegmentType::capacity();
  static constexpr bool kUseVersion = Policy::kUseVersion;

  // if IsSingleBucket is true - iterates only over a single bucket.
  template <bool IsConst, bool IsSingleBucket = false> class Iterator {
    using Owner = std::conditional_t<IsConst, const DashTable, DashTable>;

    Owner* owner_;
    uint32_t seg_id_;
    uint8_t bucket_id_;
    uint8_t slot_id_;

    friend class DashTable;

    Iterator(Owner* me, uint32_t seg_id, uint8_t bid, uint8_t sid)
        : owner_(me), seg_id_(seg_id), bucket_id_(bid), slot_id_(sid) {
    }

    void FindValid() {
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

    // Copy constructor from bucket_iterator to iterator.
    template <bool TIsSingleB>
    Iterator(const Iterator<IsConst, TIsSingleB>& other) noexcept
        : owner_(other.owner_), seg_id_(other.seg_id_), bucket_id_(other.bucket_id_),
          slot_id_(other.slot_id_) {
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
      FindValid();
      return *this;
    }

    Iterator& operator+=(int delta) {
      slot_id_ += delta;
      FindValid();
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
      return owner_->segment_[seg_id_]->GetVersion(bucket_id_, slot_id_);
    }

    // Returns the minimum version of the physical bucket that this iterator points to.
    // Note: In an ideal world I would introduce a bucket iterator...
    template <bool B = Policy::kUseVersion> std::enable_if_t<B, uint64_t> MinVersion() const {
      return owner_->segment_[seg_id_]->MinVersion(bucket_id_);
    }

    template <bool B = Policy::kUseVersion> std::enable_if_t<B> SetVersion(uint64_t v) {
      return owner_->segment_[seg_id_]->SetVersion(bucket_id_, slot_id_, v);
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
  };

  using const_iterator = Iterator<true>;
  using iterator = Iterator<false>;

  using const_bucket_iterator = Iterator<true, true>;
  using bucket_iterator = Iterator<false, true>;
  using cursor = detail::DashCursor;

  struct EvictionBuckets {
    bucket_iterator iter[2 + Policy::kStashBucketNum];
    // uint64_t key_hash;  // key_hash of a key that we try to insert.
  };

  struct DefaultEvictionPolicy {
    static constexpr bool can_gc = false;
    static constexpr bool can_evict = false;

    bool CanGrow(const DashTable&) {
      return true;
    }

    void RecordSplit() {}
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
    return InsertInternal(std::forward<U>(key), std::forward<V>(value), DefaultEvictionPolicy{});
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
    it.FindValid();
    return it;
  }

  const_iterator cbegin() const {
    const_iterator it{this, 0, 0, 0};
    it.FindValid();
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

  double load_factor() const {
    return double(size()) / (SegmentType::capacity() * unique_segments());
  }

  // Traverses over a single bucket in table and calls cb(iterator) 0 or more
  // times. if cursor=0 starts traversing from the beginning, otherwise continues from where it
  // stopped. returns 0 if the supplied cursor reached end of traversal. Traverse iterates at bucket
  // granularity, which means for each non-empty bucket it calls cb per each entry in the bucket
  // before returning. Unlike begin/end interface, traverse is more stable during table mutations.
  // It guarantees that if key exists at the beginning of traversal, stays in the table during the
  // traversal, it will eventually reach it even when the table shrinks or grows.
  // Returns: cursor that is guaranteed to be less than 2^40.
  template <typename Cb> cursor Traverse(cursor curs, Cb&& cb);

  // Takes an iterator pointing to an entry in a dash bucket and traverses all bucket's entries by
  // calling cb(iterator) for every non-empty slot. The iteration goes over a physical bucket.
  template <typename Cb> void TraverseBucket(const_iterator it, Cb&& cb);

  static const_bucket_iterator bucket_it(const_iterator it) {
    return const_bucket_iterator{it.owner_, it.seg_id_, it.bucket_id_, 0};
  }

  const_bucket_iterator CursorToBucketIt(cursor c) const {
    return const_bucket_iterator{this, c.segment_id(global_depth_), c.bucket_id(), 0};
  }

  // Capture Version Change. Runs cb(it) on every bucket! (not entry) in the table whose version
  // would potentially change upon insertion of 'k'.
  // In practice traversal is limited to a single segment. The operation is read-only and
  // simulates insertion process. 'cb' must accept const_iterator. Note: the interface a bit hacky
  // since the iterator may point to non-existing slot. In practice it only can be sent to
  // TraverseBucket function.
  // The function returns any bucket that is touched by the insertion flow. In practice, we
  // could tighten estimates by taking the new version into account.
  // Not sure how important this is though.
  template <typename U, typename Cb> void CVCUponInsert(const U& key, Cb&& cb) const;

  void Clear();

  uint64_t garbage_collected() const { return garbage_collected_;}
  uint64_t evicted() const { return evicted_;}

 private:
  template <typename U, typename V, typename EvictionPolicy>
  std::pair<iterator, bool> InsertInternal(U&& key, V&& value, EvictionPolicy&& policy);

  void IncreaseDepth(unsigned new_depth);
  void Split(uint32_t seg_id);

  template <typename Cb> void IterateUnique(Cb&& cb);

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
  uint64_t evicted_ = 0;
};

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

  IterateUnique([&](SegmentType* seg) {
    alloc_traits::destroy(pa, seg);
    alloc_traits::deallocate(pa, seg, 1);
    return false;
  });
}

template <typename _Key, typename _Value, typename Policy>
template <typename U, typename Cb>
void DashTable<_Key, _Value, Policy>::CVCUponInsert(const U& key, Cb&& cb) const {
  uint64_t key_hash = DoHash(key);
  uint32_t seg_id = SegmentId(key_hash);
  assert(seg_id < segment_.size());
  const SegmentType* target = segment_[seg_id];

  uint8_t bids[2];
  unsigned num_touched = target->CVCOnInsert(key_hash, bids);
  if (num_touched > 0) {
    for (unsigned i = 0; i < num_touched; ++i) {
      cb(const_iterator{this, seg_id, bids[i], 0});
    }
    return;
  }

  static_assert(kPhysicalBucketNum < 0xFF, "");

  // Segment is full, we need to return the whole segment, because it can be split
  // and its entries can be reshuffled into different buckets.
  for (uint8_t i = 0; i < kPhysicalBucketNum; ++i) {
    cb(const_iterator{this, seg_id, i, 0});
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

  IterateUnique(cb);
  size_ = 0;

  // Consider the following case: table with 8 segments overall, 4 unique.
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
     and the erase all other unique segments.
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
template <typename Cb>
void DashTable<_Key, _Value, Policy>::IterateUnique(Cb&& cb) {
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
  size_t x = SegmentId(key_hash);
  const auto* target = segment_[x];

  auto seg_it = target->FindIt(key, key_hash, EqPred());
  if (seg_it.found()) {
    return iterator{this, uint32_t(x), seg_it.index, seg_it.slot};
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
auto DashTable<_Key, _Value, Policy>::InsertInternal(U&& key, V&& value, EvictionPolicy&& ev)
    -> std::pair<iterator, bool> {
  uint64_t key_hash = DoHash(key);
  uint32_t seg_id = SegmentId(key_hash);

  while (true) {
    // Keep last global_depth_ msb bits of the hash.
    assert(seg_id < segment_.size());
    SegmentType* target = segment_[seg_id];

    auto [it, res] =
        target->Insert(std::forward<U>(key), std::forward<V>(value), key_hash, EqPred());

    if (res) {  // success
      ++size_;
      return std::make_pair(iterator{this, seg_id, it.index, it.slot}, true);
    }

    /*duplicate insert, insertion failure*/
    if (it.found()) {
      return std::make_pair(iterator{this, seg_id, it.index, it.slot}, false);
    }

    // try garbage collect or evict.
    if constexpr (ev.can_evict || ev.can_gc) {
      // Try eviction.
      uint8_t bid[2];
      SegmentType::FillProbeArray(key_hash, bid);
      EvictionBuckets buckets;

      buckets.iter[0] = bucket_iterator{this, seg_id, bid[0], 0};
      buckets.iter[1] = bucket_iterator{this, seg_id, bid[1], 0};

      for (unsigned i = 0; i < Policy::kStashBucketNum; ++i) {
        buckets.iter[2 + i] = bucket_iterator{this, seg_id, uint8_t(kLogicalBucketNum + i), 0};
      }

      // The difference between gc and eviction is that gc can be applied even if
      // the table can grow since we throw away logically deleted items.
      // For eviction to be applied we should reach the growth limit.
      if constexpr (ev.can_gc) {
        unsigned res = ev.GarbageCollect(buckets, this);
        garbage_collected_ += res;
        if (res)
          continue;
      }

      // We evict only if our policy says we can not grow
      if constexpr (ev.can_evict) {
        bool can_grow = ev.CanGrow(*this);
        if (!can_grow) {
          unsigned res = ev.Evict(buckets, this);
          evicted_ += res;
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

      seg_id = SegmentId(key_hash);
      assert(seg_id < segment_.size() && segment_[seg_id] == target);
    }

    Split(seg_id);
    ev.RecordSplit();
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

  auto cb = [this](const auto& k) { return policy_.HashFn(k); };

  source->Split(std::move(cb), target);  // increases the depth.
  ++unique_segments_;

  for (size_t i = start_idx + chunk_size / 2; i < start_idx + chunk_size; ++i) {
    segment_[i] = target;
  }
}

template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
auto DashTable<_Key, _Value, Policy>::Traverse(cursor curs, Cb&& cb) -> cursor {
  if (curs.bucket_id() >= kLogicalBucketNum)  // sanity.
    return 0;

  uint32_t sid = curs.segment_id(global_depth_);
  uint8_t bid = curs.bucket_id();

  auto hash_fun = [this](const auto& k) { return policy_.HashFn(k); };

  bool fetched = false;

  // We fix bid and go over all segments. Once we reach the end we increase bid and repeat.
  do {
    SegmentType& s = *segment_[sid];
    auto dt_cb = [&](const SegmentIterator& it) { cb(iterator{this, sid, it.index, it.slot}); };

    fetched = s.TraverseLogicalBucket(bid, hash_fun, std::move(dt_cb));
    sid = NextSeg(sid);
    if (sid >= segment_.size()) {
      sid = 0;
      ++bid;

      if (bid >= kLogicalBucketNum)
        return 0;  // "End of traversal" cursor.
    }
  } while (!fetched);

  return cursor{global_depth_, sid, bid};
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
