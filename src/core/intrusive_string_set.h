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

 private:
  static ISLEntry Create(std::string_view key) {
    char* next = nullptr;
    uint32_t key_size = key.size();

    auto size = sizeof(next) + sizeof(key_size) + key_size;

    char* data = (char*)malloc(size);

    std::memcpy(data, &next, sizeof(next));

    auto* key_size_pos = data + sizeof(next);
    std::memcpy(key_size_pos, &key_size, sizeof(key_size));

    auto* key_pos = key_size_pos + sizeof(key_size);
    std::memcpy(key_pos, key.data(), key_size);

    return ISLEntry(data);
  }

  static void Destroy(ISLEntry entry) {
    free(entry.data_);
  }

  ISLEntry Next() const {
    ISLEntry next;
    std::memcpy(&next.data_, data_, sizeof(next));
    return next;
  }

  void SetNext(ISLEntry next) {
    std::memcpy(data_, &next, sizeof(next));
  }

  const char* GetKeyData() const {
    return data_ + sizeof(ISLEntry*) + sizeof(uint32_t);
  }

  uint32_t GetKeySize() const {
    uint32_t size = 0;
    std::memcpy(&size, data_ + sizeof(ISLEntry*), sizeof(size));
    return size;
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

  ISLEntry Emplace(std::string_view key) {
    return Insert(ISLEntry::Create(key));
  }

  ISLEntry Find(std::string_view str) const {
    auto it = start_;
    for (; it && it.Key() != str; it = it.Next())
      ;
    return it;
  }

  bool Erase(std::string_view str) {
    if (!start_) {
      return false;
    }
    auto it = start_;
    if (it.Key() == str) {
      start_ = it.Next();
      ISLEntry::Destroy(it);
      return true;
    }

    auto prev = it;
    for (it = it.Next(); it; prev = it, it = it.Next()) {
      if (it.Key() == str) {
        prev.SetNext(it.Next());
        ISLEntry::Destroy(it);
        return true;
      }
    }
    return false;
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
    return bucket.Emplace(str);
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
    return entries_[bucket_id].Find(member);
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
  std::uint32_t capacity_log_ = 1;
  std::uint32_t size_ = 0;  // number of elements in the set.

  static_assert(sizeof(IntrusiveStringList) == sizeof(void*),
                "IntrusiveStringList should be just a pointer");
  std::vector<IntrusiveStringList, PMR_NS::polymorphic_allocator<IntrusiveStringList>> entries_;
};

}  // namespace dfly
