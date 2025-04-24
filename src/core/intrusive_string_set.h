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

class IntrusiveStringSet {
  using Buckets =
      std::vector<IntrusiveStringList, PMR_NS::polymorphic_allocator<IntrusiveStringList>>;

 public:
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::string_view;
    using pointer = std::string_view*;
    using reference = std::string_view&;

    iterator(Buckets::iterator it,
             IntrusiveStringList::Iterator entry = IntrusiveStringList::Iterator())
        : buckets_it_(it), entry_(entry) {
    }

    // uint32_t ExpiryTime() const {
    //   return prev.Next()->ExpiryTime();
    // }

    void SetExpiryTime(uint32_t ttl_sec) {
      entry_.SetExpiryTime(ttl_sec);
    }

    // bool HasExpiry() const {
    //   return curr_entry_.HasExpiry();
    // }

    // void Advance();

    bool operator==(const iterator& r) const {
      return buckets_it_ == r.buckets_it_;
    }

    bool operator!=(const iterator& r) const {
      return !operator==(r);
    }

    IntrusiveStringList::Iterator::value_type operator*() {
      return *entry_;
    }

    IntrusiveStringList::Iterator operator->() {
      return entry_;
    }

   private:
    Buckets::iterator buckets_it_;
    IntrusiveStringList::Iterator entry_;
  };

  iterator end() {
    return iterator(entries_.end());
  }

  explicit IntrusiveStringSet(PMR_NS::memory_resource* mr = PMR_NS::get_default_resource())
      : entries_(mr) {
  }

  ISLEntry Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX) {
    if (entries_.empty() || size_ >= entries_.size()) {
      Reserve(Capacity() * 2);
    }
    const auto bucket_id = BucketId(Hash(str));
    auto& bucket = entries_[bucket_id];

    if (auto existed_item = bucket.Find(str); existed_item) {
      // TODO consider common implementation for key value pair
      return ISLEntry();
    }

    return AddUnique(str, bucket, ttl_sec);
  }

  void Reserve(size_t sz) {
    sz = absl::bit_ceil(sz);
    if (sz > entries_.size()) {
      size_t prev_size = entries_.size();
      capacity_log_ = absl::bit_width(sz) - 1;
      entries_.resize(sz);
      Rehash(prev_size);
    }
  }

  void Clear() {
    capacity_log_ = 0;
    entries_.resize(0);
    size_ = 0;
  }

  ISLEntry AddUnique(std::string_view str, IntrusiveStringList& bucket,
                     uint32_t ttl_sec = UINT32_MAX) {
    ++size_;
    return bucket.Emplace(str, ttl_sec);
  }

  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec, bool keepttl) {
    Reserve(span.size());
    unsigned res = 0;
    for (auto& s : span) {
      const auto bucket_id = BucketId(Hash(s));
      auto& bucket = entries_[bucket_id];
      if (auto existed_item = bucket.Find(s); existed_item) {
        // TODO update TTL
      } else {
        ++res;
        AddUnique(s, bucket, ttl_sec);
      }
    }
    return res;
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
      if (entries_[entries_idx].Scan(cb, time_now_)) {
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
    auto bucket_id = BucketId(Hash(str));
    return entries_[bucket_id].Erase(str);
  }

  iterator Find(std::string_view member) {
    if (entries_.empty())
      return iterator(entries_.end());
    auto bucket_id = BucketId(Hash(member));
    auto entry_it = entries_.begin() + bucket_id;
    auto res = entry_it->Find(member);
    return iterator(res ? entry_it : entries_.end(), res);
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

 private:
  // was Grow in StringSet
  void Rehash(size_t prev_size) {
    for (int64_t i = prev_size - 1; i >= 0; --i) {
      auto list = std::move(entries_[i]);
      for (auto entry = list.Pop(time_now_); entry; entry = list.Pop(time_now_)) {
        auto bucket_id = BucketId(Hash(entry.Key()));
        entries_[bucket_id].Insert(entry.Release());
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

 private:
  std::uint32_t capacity_log_ = 0;
  std::uint32_t size_ = 0;  // number of elements in the set.
  std::uint32_t time_now_ = 0;

  static_assert(sizeof(IntrusiveStringList) == sizeof(void*),
                "IntrusiveStringList should be just a pointer");
  Buckets entries_;
};

}  // namespace dfly
