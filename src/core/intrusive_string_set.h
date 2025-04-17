// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/numeric/bits.h>
#include <absl/types/span.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

#include "base/hash.h"
#include "base/pmr/memory_resource.h"

namespace dfly {

class ISLEntry {
  friend class IntrusiveStringList;

  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  static constexpr size_t kTtlBit = 1ULL << 55;
  static constexpr size_t kTagMask = 4095ULL << 52;  // we reserve 12 high bits.

 public:
  ISLEntry() = default;

  ISLEntry(char* data) {
    data_ = data;
  }

  operator bool() const {
    return data_;
  }

  std::string_view Key() const {
    return {GetKeyData(), GetKeySize()};
  }

  bool HasExpiry() const {
    return HasTtl();
  }

  // returns the expiry time of the current entry or UINT32_MAX if no ttl is set.
  uint32_t ExpiryTime() const {
    std::uint32_t res = UINT32_MAX;
    if (HasTtl()) {
      std::memcpy(&res, Raw() + sizeof(ISLEntry*), sizeof(res));
    }
    return res;
  }

 private:
  static ISLEntry Create(std::string_view key, uint32_t ttl_sec = UINT32_MAX) {
    char* next = nullptr;
    uint32_t key_size = key.size();

    bool has_ttl = ttl_sec != UINT32_MAX;

    auto size = sizeof(next) + sizeof(key_size) + key_size + has_ttl * sizeof(ttl_sec);

    char* data = (char*)malloc(size);
    ISLEntry res(data);

    std::memcpy(data, &next, sizeof(next));

    auto* ttl_pos = data + sizeof(next);
    if (has_ttl) {
      res.SetTtlBit(true);
      std::memcpy(ttl_pos, &ttl_sec, sizeof(ttl_sec));
    }

    auto* key_size_pos = ttl_pos + res.GetTtlSize();
    std::memcpy(key_size_pos, &key_size, sizeof(key_size));

    auto* key_pos = key_size_pos + sizeof(key_size);
    std::memcpy(key_pos, key.data(), key_size);

    return res;
  }

  static void Destroy(ISLEntry entry) {
    free(entry.Raw());
  }

  ISLEntry Next() const {
    ISLEntry next;
    std::memcpy(&next.data_, Raw(), sizeof(next));
    return next;
  }

  void SetNext(ISLEntry next) {
    std::memcpy(Raw(), &next, sizeof(next));
  }

  const char* GetKeyData() const {
    return Raw() + sizeof(ISLEntry*) + sizeof(uint32_t) + GetTtlSize();
  }

  uint32_t GetKeySize() const {
    uint32_t size = 0;
    std::memcpy(&size, Raw() + sizeof(ISLEntry*) + GetTtlSize(), sizeof(size));
    return size;
  }

  uint64_t uptr() const {
    return uint64_t(data_);
  }

  char* Raw() const {
    return (char*)(uptr() & ~kTagMask);
  }

  void SetTtlBit(bool b) {
    if (b)
      data_ = (char*)(uptr() | kTtlBit);
    else
      data_ = (char*)(uptr() & (~kTtlBit));
  }

  bool HasTtl() const {
    return (uptr() & kTtlBit) != 0;
  }

  std::uint32_t GetTtlSize() const {
    return HasTtl() ? sizeof(std::uint32_t) : 0;
  }

  // TODO consider use SDS strings or other approach
  // TODO add optimization for big keys
  // memory daya layout [ISLEntry*, key_size, key]
  char* data_ = nullptr;
};

class IntrusiveStringList {
 public:
  ~IntrusiveStringList() {
    while (start_) {
      auto next = start_.Next();
      ISLEntry::Destroy(start_);
      start_ = next;
    }
  }

  IntrusiveStringList() = default;
  IntrusiveStringList(IntrusiveStringList&& r) {
    start_ = r.start_;
    r.start_ = {};
  }

  ISLEntry Insert(ISLEntry e) {
    e.SetNext(start_);
    start_ = e;
    return start_;
  }

  ISLEntry Pop() {
    auto res = start_;
    if (start_) {
      start_ = start_.Next();
      // TODO consider to res.SetNext(nullptr); for now it looks superfluous
    }
    return res;
  }

  bool Empty() {
    return start_;
  }

  ISLEntry Emplace(std::string_view key, uint32_t ttl_sec = UINT32_MAX) {
    return Insert(ISLEntry::Create(key, ttl_sec));
  }

  ISLEntry Find(std::string_view str) const {
    auto it = start_;
    for (; it && it.Key() != str; it = it.Next())
      ;
    return it;
  }

  bool Erase(std::string_view str) {
    auto cond = [str](const ISLEntry e) { return str == e.Key(); };
    return Erase(cond);
  }

  template <class T, std::enable_if_t<std::is_invocable_v<T, ISLEntry>>* = nullptr>
  bool Erase(const T& cond) {
    if (!start_) {
      return false;
    }

    if (auto it = start_; cond(it)) {
      start_ = it.Next();
      ISLEntry::Destroy(it);
      return true;
    }

    for (auto prev = start_, it = start_.Next(); it; prev = it, it = it.Next()) {
      if (cond(it)) {
        prev.SetNext(it.Next());
        ISLEntry::Destroy(it);
        return true;
      }
    }
    return false;
  }

  template <class T, std::enable_if_t<std::is_invocable_v<T, std::string_view>>* = nullptr>
  bool Scan(const T& cb, uint32_t curr_time) {
    for (auto it = start_; it && it.ExpiryTime() < curr_time; it = start_) {
      start_ = it.Next();
      ISLEntry::Destroy(it);
    }

    for (auto curr = start_, next = start_; curr; curr = next) {
      cb(curr.Key());
      next = curr.Next();
      for (auto tmp = next; tmp && tmp.ExpiryTime() < curr_time; tmp = next) {
        next = next.Next();
        ISLEntry::Destroy(tmp);
      }
      curr.SetNext(next);
    }
    return start_;
  }

 private:
  ISLEntry start_;
};

class IntrusiveStringSet {
 public:
  explicit IntrusiveStringSet(PMR_NS::memory_resource* mr = PMR_NS::get_default_resource())
      : entries_(mr) {
  }

  // TODO add TTL processing
  ISLEntry Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX) {
    if (entries_.empty() || size_ >= entries_.size()) {
      Resize(Capacity() * 2);
    }
    const auto bucket_id = BucketId(Hash(str));
    auto& bucket = entries_[bucket_id];

    if (auto existed_item = bucket.Find(str); existed_item) {
      // TODO consider common implementation for key value pair
      return ISLEntry();
    }

    return AddUnique(str, bucket, ttl_sec);
  }

  void Resize(size_t sz) {
    sz = absl::bit_ceil(sz);
    if (sz > entries_.size()) {
      size_t prev_size = entries_.size();
      capacity_log_ = absl::bit_width(sz) - 1;
      entries_.resize(sz);
      Rehash(prev_size);
    }
  }

  ISLEntry AddUnique(std::string_view str, IntrusiveStringList& bucket,
                     uint32_t ttl_sec = UINT32_MAX) {
    ++size_;
    return bucket.Emplace(str, ttl_sec);
  }

  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec, bool keepttl) {
    Resize(span.size());
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

  bool Erase(std::string_view str) {
    if (entries_.empty())
      return false;
    auto bucket_id = BucketId(Hash(str));
    return entries_[bucket_id].Erase(str);
  }

  ISLEntry Find(std::string_view member) const {
    if (entries_.empty())
      return {};
    auto bucket_id = BucketId(Hash(member));
    auto res = entries_[bucket_id].Find(member);
    if (!res) {
      bucket_id = BucketId(Hash(member));
      res = entries_[bucket_id].Find(member);
    }
    return res;
  }

  bool Contains(std::string_view member) const {
    return Find(member);
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
      for (auto entry = list.Pop(); entry; entry = list.Pop()) {
        auto bucket_id = BucketId(Hash(entry.Key()));
        entries_[bucket_id].Insert(entry);
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
  std::vector<IntrusiveStringList, PMR_NS::polymorphic_allocator<IntrusiveStringList>> entries_;
};

}  // namespace dfly
