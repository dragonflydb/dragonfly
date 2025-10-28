// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/numeric/bits.h>
#include <absl/types/span.h>

#include <vector>

#include "base/pmr/memory_resource.h"
#include "oah_entry.h"

namespace dfly {

class OAHSet {  // Open Addressing Hash Set
  using Buckets = std::vector<OAHEntry, PMR_NS::polymorphic_allocator<OAHEntry>>;

 public:
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = OAHEntry;
    using pointer = OAHEntry*;
    using reference = OAHEntry&;

    iterator(OAHSet* owner, uint32_t bucket_id, uint32_t pos_in_bucket)
        : owner_(owner), bucket_(bucket_id), pos_(pos_in_bucket) {
      // TODO rewrite, it's inefficient
      SetEntryIt();
    }

    void SetExpiryTime(uint32_t ttl_sec, size_t* obj_malloc_used) {
      owner_->entries_[bucket_][pos_].SetExpiry(owner_->EntryTTL(ttl_sec));
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
      DCHECK(owner_ == r.owner_);
      return bucket_ == r.bucket_ && pos_ == r.pos_;
    }

    bool operator!=(const iterator& r) const {
      return !operator==(r);
    }

    reference operator*() {
      return owner_->entries_[bucket_][pos_];
    }

    reference operator->() {
      return owner_->entries_[bucket_][pos_];
    }

    bool HasExpiry() {
      return owner_->entries_[bucket_][pos_].HasExpiry();
    }

    uint32_t ExpiryTime() {
      return owner_->entries_[bucket_][pos_].GetExpiry();
    }

    operator bool() const {
      return owner_;
    }

   private:
    // find valid entry_ iterator starting from buckets_it_ and set it
    void SetEntryIt() {
      if (!owner_)
        return;
      for (auto size = owner_->entries_.size(); bucket_ < size; ++bucket_) {
        auto& bucket = owner_->entries_[bucket_];
        for (uint32_t bucket_size = bucket.ElementsNum(); pos_ < bucket_size; ++pos_) {
          if (bucket[pos_])
            return;
        }
        pos_ = 0;
      }
      owner_ = nullptr;
    }

   private:
    OAHSet* owner_;
    uint32_t bucket_;
    uint32_t pos_;
  };

  iterator begin() {
    return iterator(this, 0, 0);
  }

  iterator end() {
    return iterator(nullptr, 0, 0);
  }

  explicit OAHSet(PMR_NS::memory_resource* mr = PMR_NS::get_default_resource()) : entries_(mr) {
  }

  bool Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX) {
    if (entries_.empty() || size_ >= entries_.size()) {
      Reserve(Capacity() * 2);
    }
    uint64_t hash = Hash(str);
    const auto bucket_id = BucketId(hash, capacity_log_);

    PREFETCH_READ(entries_.data() + bucket_id);
    uint32_t at = EntryTTL(ttl_sec);
    // TODO maybe we should split memory allocation and copying for the case when we can't add it
    // into set
    OAHEntry entry(str, at);

    if (auto item = FastCheck(bucket_id, str, hash); item != end()) {
      return false;
    }

    uint32_t bucket = FindEmptyAround(bucket_id);

    DCHECK(bucket_id + kDisplacementSize > bucket);
    AddUnique(std::move(entry), bucket, hash, ttl_sec);
    return true;
  }

  void Reserve(size_t sz) {
    sz = absl::bit_ceil(sz);
    if (sz > entries_.size()) {
      auto prev_capacity_log = capacity_log_;
      capacity_log_ = std::max(kMinCapacityLog, uint32_t(absl::bit_width(sz) - 1));
      entries_.resize(Capacity());
      Rehash(prev_capacity_log);
    }
  }

  void Clear() {
    capacity_log_ = 0;
    entries_.resize(0);
    size_ = 0;
  }

  iterator AddUnique(OAHEntry&& e, uint32_t bucket, uint64_t hash, uint32_t ttl_sec = UINT32_MAX) {
    ++size_;
    uint32_t pos = entries_[bucket].Insert(std::move(e));
    entries_[bucket][pos].SetHash(hash, capacity_log_, kShiftLog);
    return iterator(this, bucket, pos);
  }

  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec = UINT32_MAX) {
    Reserve(span.size());
    unsigned res = 0;
    for (auto& s : span) {
      if (Add(s, ttl_sec) != end()) {
        res++;
      }
    }
    return res;
  }

  // TODO: Consider using chunks for this as in StringSet
  void Fill(OAHSet* other) {
    DCHECK(other->entries_.empty());
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
    const uint32_t capacity_mask = Capacity() - 1;
    uint32_t bucket_id = cursor >> (32 - capacity_log_);
    const uint32_t displacement_size = std::min(kDisplacementSize, BucketCount());

    // First find the bucket to scan, skip empty buckets.
    for (; bucket_id < entries_.size(); ++bucket_id) {
      bool res = false;
      for (uint32_t i = 0; i < displacement_size; i++) {
        const uint32_t shifted_bid = (bucket_id + i) & capacity_mask;
        res |= ScanBucket(entries_[shifted_bid], cb, bucket_id);
      }
      if (res)
        break;
    }

    if (++bucket_id >= entries_.size()) {
      return 0;
    }

    return bucket_id << (32 - capacity_log_);
  }

  OAHEntry Pop() {
    for (auto& bucket : entries_) {
      if (auto res = bucket.Pop(); !res.Empty()) {
        DCHECK(!res.IsVector());
        --size_;
        return res;
      }
    }
    return {};
  }

  bool Erase(std::string_view str) {
    if (entries_.empty())
      return false;

    uint64_t hash = Hash(str);
    auto bucket_id = BucketId(hash, capacity_log_);
    auto item = FindInternal(bucket_id, str, hash);
    if (item != end()) {
      *item = OAHEntry();
      return true;
    }
    return false;
  }

  iterator Find(std::string_view member) {
    if (entries_.empty())
      return end();

    uint64_t hash = Hash(member);
    auto bucket_id = BucketId(hash, capacity_log_);
    auto res = FindInternal(bucket_id, member, hash);
    return res;
  }

  bool Contains(std::string_view member) {
    return Find(member) != end();
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
    return entries_.size();  // the same as Capacity()
  }

  std::uint32_t Capacity() const {
    return 1 << capacity_log_;
  }

  // set an abstract time that allows expiry.
  void set_time(uint32_t val) {
    time_now_ = val;
  }

  uint32_t time_now() const {
    return time_now_;
  }

  size_t ObjMallocUsed() const {
    // TODO implement
    LOG(FATAL) << "ExpirationUsed() isn't implemented";
    return 0;
  }

  size_t SetMallocUsed() const {
    // TODO implement
    LOG(FATAL) << "ExpirationUsed() isn't implemented";
    return 0;
  }

  bool ExpirationUsed() const {
    // TODO
    LOG(FATAL) << "ExpirationUsed() isn't implemented";
    return true;
  }

  size_t SizeSlow() {
    // TODO
    LOG(FATAL) << "SizeSlow() isn't implemented";
    // CollectExpired();
    return size_;
  }

 private:
  // was Grow in StringSet
  void Rehash(uint32_t prev_capacity_log) {
    size_t prev_size = 1ul << prev_capacity_log;
    // we should prevent moving elements before current possition to avoid double processing
    constexpr size_t mix_size = 2 << kShiftLog;
    for (size_t bucket_id = prev_size - 1; bucket_id >= mix_size; --bucket_id) {
      auto bucket = std::move(entries_[bucket_id]);  // can be redundant
      // TODO add optimization for package processing
      for (uint32_t pos = 0, size = bucket.ElementsNum(); pos < size; ++pos) {
        // TODO operator [] is inefficient and it is better to avoid it
        if (bucket[pos]) {
          auto new_bucket_id =
              bucket[pos].Rehash(bucket_id, prev_capacity_log, capacity_log_, kShiftLog);

          // TODO add optimization for package processing
          new_bucket_id = FindEmptyAround(new_bucket_id);

          // insert method is inefficient
          entries_[new_bucket_id]->Insert(std::move(bucket[pos]));
        }
      }
    }
    auto max_element = std::min(mix_size, prev_size);
    std::array<OAHEntry, mix_size> old_buckets{};
    for (size_t i = 0; i < max_element; ++i) {
      old_buckets[i] = std::move(entries_[i]);
    }

    for (size_t bucket_id = 0; bucket_id < max_element; ++bucket_id) {
      auto& bucket = old_buckets[bucket_id];
      // TODO add optimization for package processing
      for (uint32_t pos = 0, size = bucket.ElementsNum(); pos < size; ++pos) {
        // TODO operator [] is inefficient and it is better to avoid it
        if (bucket[pos]) {
          auto new_bucket_id =
              bucket[pos].Rehash(bucket_id, prev_capacity_log, capacity_log_, kShiftLog);

          // TODO add optimization for package processing
          new_bucket_id = FindEmptyAround(new_bucket_id);

          // insert method is inefficient
          entries_[new_bucket_id]->Insert(std::move(bucket[pos]));
        }
      }
    }
  }

  // return bucket_id and position otherwise max
  iterator FastCheck(const uint32_t bid, std::string_view str, uint64_t hash) {
    const uint32_t displacement_size = std::min(kDisplacementSize, BucketCount());
    const uint32_t capacity_mask = Capacity() - 1;
    const auto ext_hash = OAHEntry::CalcExtHash(hash, capacity_log_, kShiftLog);

    bool res = false;
    for (uint32_t i = 0; i < displacement_size; i++) {
      const uint32_t bucket_id = (bid + i) & capacity_mask;
      res |= entries_[bucket_id].CheckExtendedHash(ext_hash, capacity_log_, kShiftLog);
    }

    if (!res) {
      const uint32_t extension_point_shift = displacement_size - 1;
      auto ext_bid = bid | extension_point_shift;
      auto pos = entries_[ext_bid].Find(str, ext_hash, capacity_log_, kShiftLog, &size_, time_now_);
      if (pos) {
        return iterator{this, ext_bid, *pos};
      }
    } else {
      return FindInternal(bid, str, hash);
    }
    return end();
  }

  template <class T, std::enable_if_t<std::is_invocable_v<T, std::string_view>>* = nullptr>
  bool ScanBucket(OAHEntry& entry, const T& cb, uint32_t bucket_id) {
    if (!entry.IsVector()) {
      entry.ExpireIfNeeded(time_now_, &size_);
      if (entry.CheckBucketAffiliation(bucket_id, capacity_log_, kShiftLog)) {
        cb(entry.Key());
        return true;
      }
    } else {
      auto& arr = entry.AsVector();
      bool result = false;
      for (auto& el : arr) {
        el.ExpireIfNeeded(time_now_, &size_);
        if (el.CheckBucketAffiliation(bucket_id, capacity_log_, kShiftLog)) {
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

  uint32_t FindEmptyAround(uint32_t bid) {
    const uint32_t displacement_size = std::min(kDisplacementSize, BucketCount());
    const uint32_t capacity_mask = Capacity() - 1;
    for (uint32_t i = 0; i < displacement_size; i++) {
      const uint32_t bucket_id = (bid + i) & capacity_mask;
      if (entries_[bucket_id].Empty())
        return bucket_id;
      // TODO add expiration logic
    }

    DCHECK(Capacity() > kDisplacementSize);
    const uint32_t extension_point_shift = displacement_size - 1;
    bid |= extension_point_shift;
    DCHECK(bid < Capacity());
    return bid;
  }

  // return bucket_id and position otherwise max
  iterator FindInternal(uint32_t bid, std::string_view str, uint64_t hash) {
    const uint32_t displacement_size = std::min(kDisplacementSize, BucketCount());
    const uint32_t capacity_mask = Capacity() - 1;
    const auto ext_hash = OAHEntry::CalcExtHash(hash, capacity_log_, kShiftLog);
    for (uint32_t i = 0; i < displacement_size; i++) {
      const uint32_t bucket_id = (bid + i) & capacity_mask;
      auto pos =
          entries_[bucket_id].Find(str, ext_hash, capacity_log_, kShiftLog, &size_, time_now_);
      if (pos) {
        return iterator{this, bucket_id, *pos};
      }
    }
    return end();
  }

 private:
  static constexpr std::uint32_t kMinCapacityLog = 3;                   // TODO make template
  static constexpr std::uint32_t kShiftLog = 2;                         // TODO make template
  static constexpr std::uint32_t kDisplacementSize = (1 << kShiftLog);  // TODO check
  std::uint32_t capacity_log_ = 0;
  std::uint32_t size_ = 0;  // number of elements in the set.
  std::uint32_t time_now_ = 0;
  Buckets entries_;
};

}  // namespace dfly
