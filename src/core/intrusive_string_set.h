// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/numeric/bits.h>
#include <absl/types/span.h>

#include <vector>

#include "base/hash.h"
#include "base/pmr/memory_resource.h"
#include "intrusive_string_list.h"

namespace dfly {

// NOTE: do not change the ABI of ISLEntry struct as long as we support
// --stringset_experimental=false

class IntrusiveStringSet {
  using Buckets =
      std::vector<IntrusiveStringList, PMR_NS::polymorphic_allocator<IntrusiveStringList>>;

 public:
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = ISLEntry;
    using pointer = ISLEntry*;
    using reference = ISLEntry&;

    iterator(Buckets::iterator it, Buckets::iterator end, IntrusiveStringList::iterator entry)
        : buckets_it_(it), end_(end), entry_(entry) {
    }

    iterator(Buckets::iterator it, Buckets::iterator end) : buckets_it_(it), end_(end) {
      SetEntryIt();
    }

    // uint32_t ExpiryTime() const {
    //   return prev.Next()->ExpiryTime();
    // }

    void SetExpiryTime(uint32_t ttl_sec, size_t* obj_malloc_used) {
      entry_.SetExpiryTime(ttl_sec, obj_malloc_used);
    }

    // bool HasExpiry() const {
    //   return curr_entry_.HasExpiry();
    // }

    // void Advance();

    iterator& operator++() {
      // TODO add expiration logic
      if (entry_)
        ++entry_;
      if (!entry_) {
        ++buckets_it_;
        SetEntryIt();
      }
      return *this;
    }

    bool operator==(const iterator& r) const {
      return buckets_it_ == r.buckets_it_ && entry_ == r.entry_;
    }

    bool operator!=(const iterator& r) const {
      return !operator==(r);
    }

    IntrusiveStringList::iterator::value_type operator*() {
      return *entry_;
    }

    IntrusiveStringList::iterator operator->() {
      return entry_;
    }

   private:
    // find valid entry_ iterator starting from buckets_it_ and set it
    void SetEntryIt() {
      for (; buckets_it_ != end_; ++buckets_it_) {
        if (!buckets_it_->Empty()) {
          entry_ = buckets_it_->begin();
          break;
        }
      }
    }

   private:
    Buckets::iterator buckets_it_;
    Buckets::iterator end_;
    IntrusiveStringList::iterator entry_;
  };

  iterator begin() {
    return iterator(entries_.begin(), entries_.end());
  }

  iterator end() {
    return iterator(entries_.end(), entries_.end());
  }

  explicit IntrusiveStringSet(PMR_NS::memory_resource* mr = PMR_NS::get_default_resource())
      : entries_(mr) {
  }

  static constexpr uint32_t kMaxBatchLen = 32;

  ISLEntry Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX) {
    if (entries_.empty() || size_ >= entries_.size()) {
      Reserve(Capacity() * 2);
    }
    uint64_t hash = Hash(str);
    const auto bucket_id = BucketId(hash);

    if (auto item = FindInternal(bucket_id, str, hash); item.first != IntrusiveStringList::end()) {
      return {};
    }

    auto bucket = FindEmptyAround(bucket_id);

    if (bucket == entries_.end()) {
      // NO empty bucket around bucket_id + allowed displacement
      bucket = entries_.begin() + bucket_id;
    }

    return AddUnique(str, bucket, hash, EntryTTL(ttl_sec));
  }

  void Reserve(size_t sz) {
    sz = absl::bit_ceil(sz);
    if (sz > entries_.size()) {
      size_t prev_size = entries_.size();
      capacity_log_ = std::max(kMinCapacityLog, uint32_t(absl::bit_width(sz) - 1));
      entries_.resize(Capacity());
      Rehash(prev_size);
    }
  }

  void Clear() {
    capacity_log_ = 0;
    entries_.resize(0);
    size_ = 0;
  }

  ISLEntry AddUnique(std::string_view str, Buckets::iterator bucket, uint64_t hash,
                     uint32_t ttl_sec = UINT32_MAX) {
    ++size_;
    auto& entry = bucket->Emplace(str, ttl_sec);
    entry.SetExtendedHash(hash, capacity_log_);
    return entry;
  }

  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec, bool keepttl) {
    Reserve(span.size());
    unsigned res = 0;
    for (auto& s : span) {
      res++;
      Add(s, ttl_sec);
    }
    return res;
  }

  // TODO: Consider using chunks for this as string set
  void Fill(IntrusiveStringSet* other) {
    DCHECK(other->entries_.empty());
    other->Reserve(UpperBoundSize());
    other->set_time(time_now());
    for (uint32_t bucket_id = 0; bucket_id < entries_.size(); ++bucket_id) {
      for (auto it = entries_[bucket_id].begin(); it != entries_[bucket_id].end(); ++it) {
        other->Add(it->Key(), it.HasExpiry() ? it->ExpiryTime() : UINT32_MAX);
      }
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
    uint32_t entries_idx = cursor >> (32 - capacity_log_);

    // First find the bucket to scan, skip empty buckets.
    for (; entries_idx < entries_.size(); ++entries_idx) {
      if (entries_[entries_idx].Scan(cb, &size_, time_now_)) {
        break;
      }
    }

    if (++entries_idx >= entries_.size()) {
      return 0;
    }

    return entries_idx << (32 - capacity_log_);
  }

  UniqueISLEntry Pop() {
    for (auto& bucket : entries_) {
      if (auto res = bucket.Pop(time_now_); res) {
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
    auto bucket_id = BucketId(hash);
    auto item = FindInternal(bucket_id, str, hash);
    return entries_[item.second].Erase(str);
  }

  iterator Find(std::string_view member) {
    if (entries_.empty())
      return end();

    uint64_t hash = Hash(member);
    auto bucket_id = BucketId(hash);
    auto res = FindInternal(bucket_id, member, hash);
    return {res.first ? entries_.begin() + res.second : entries_.end(), entries_.end(), res.first};
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
    size_t bucket_obj_memory = 0;
    for (const auto& bucket : entries_) {
      bucket_obj_memory += bucket.ObjMallocUsed();
    }
    return bucket_obj_memory;
  }

  size_t SetMallocUsed() const {
    return entries_.capacity() * sizeof(IntrusiveStringList);
  }

 private:
  // was Grow in StringSet
  void Rehash(size_t prev_size) {
    for (int64_t i = prev_size - 1; i >= 0; --i) {
      auto list = std::move(entries_[i]);
      for (auto entry = list.Pop(time_now_); entry; entry = list.Pop(time_now_)) {
        uint64_t hash = Hash(entry.Key());
        auto bucket_id = BucketId(hash);
        auto& inserted_entry = entries_[bucket_id].Insert(entry.Release());
        inserted_entry.SetExtendedHash(hash, capacity_log_);
      }
    }
  }

  uint32_t BucketId(uint64_t hash) const {
    assert(capacity_log_ > 0);
    return hash >> (64 - capacity_log_);
  }

  uint64_t Hash(std::string_view str) const {
    constexpr XXH64_hash_t kHashSeed = 24061983;
    return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
  }

  uint32_t EntryTTL(uint32_t ttl_sec) const {
    return ttl_sec == UINT32_MAX ? ttl_sec : time_now_ + ttl_sec;
  }

  Buckets::iterator FindEmptyAround(uint32_t bid) {
    if (entries_[bid].Empty()) {
      return entries_.begin() + bid;
    }
    uint32_t displacement = std::min(kDisplacementSize, BucketCount() - 1);
    for (uint32_t i = 0; i < displacement; i++) {
      auto it = entries_.begin() + ((bid + i) & (Capacity() - 1));
      // Expire top element or whole bucket ?!
      it->ExpireIfNeeded(time_now_, &size_);
      if (it->Empty()) {
        return it;
      }
    }

    return entries_.end();
  }

  std::pair<IntrusiveStringList::iterator, uint32_t> FindInternal(uint32_t bid,
                                                                  std::string_view str,
                                                                  uint64_t hash) {
    uint32_t displacement = std::min(kDisplacementSize, BucketCount() - 1);
    for (uint32_t i = 0; i < displacement; i++) {
      uint32_t bucket_id = (bid + i) & (Capacity() - 1);
      auto it = entries_.begin() + bucket_id;
      if (it->Empty()) {
        continue;
      }
      auto res = it->Find(str, hash, capacity_log_, &size_, time_now_);
      if (res) {
        return {res, bucket_id};
      }
    }

    return {IntrusiveStringList::end(), bid};
  }

 private:
  static constexpr std::uint32_t kMinCapacityLog = 3;
  static constexpr std::uint32_t kDisplacementSize = 16;
  std::uint32_t capacity_log_ = 0;
  std::uint32_t size_ = 0;  // number of elements in the set.
  std::uint32_t time_now_ = 0;

  Buckets entries_;
};

}  // namespace dfly
