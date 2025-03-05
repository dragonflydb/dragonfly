// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

#include "base/hash.h"

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

class FakePrevISLEntry : public ISLEntry {
  FakePrevISLEntry(ISLEntry) {
    fake_allocated_mem_ = ;
  }

 private:
  void* fake_allocated_mem_;
}

class IntrusiveStringList {
 public:
  ~IntrusiveStringList() {
    while (start_) {
      auto next = start_.Next();
      ISLEntry::Destroy(start_);
      start_ = next;
    }
  }

  ISLEntry Insert(ISLEntry e) {
    e.SetNext(start_);
    start_ = e;
    return start_;
  }

  ISLEntry Emplace(std::string_view key) {
    return Insert(ISLEntry::Create(key));
  }

  ISLEntry Find(std::string_view str) {
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

  void MoveNext(ISLEntry& prev) {
    auto next = prev.Next();
    prev.SetNext(next.Next());
    Insert(next);
  }

 private:
  ISLEntry start_;
};

class IntrusiveStringSet {
 public:
  // TODO add TTL processing
  ISLEntry Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX) {
    if (size_ >= entries_.size()) {
      Grow();
    }
    auto bucket_id = BucketId(Hash(str));
    auto& bucket = entries_[bucket_id];

    if (auto existed_item = bucket.Find(str); existed_item) {
      // TODO consider common implementation for key value pair
      return ISLEntry();
    }

    ++size_;
    return bucket.Emplace(str);
  }

  bool Erase(std::string_view str) {
    auto bucket_id = BucketId(Hash(str));
    return entries_[bucket_id].Erase(str);
  }

  ISLEntry Find(std::string_view member) {
    auto bucket_id = BucketId(Hash(member));
    return entries_[bucket_id].Find(member);
  }

  // Returns the number of elements in the map. Note that it might be that some of these elements
  // have expired and can't be accessed.
  size_t UpperBoundSize() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

 private:
  std::uint32_t Capacity() const {
    return 1 << capacity_log_;
  }

  void Grow() {
    ++capacity_log_;
    entries_.resize(Capacity());

    // TODO rehashing
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
  static constexpr size_t kMinSizeShift = 2;
  std::uint32_t capacity_log_ = 1;
  std::uint32_t size_ = 0;  // number of elements in the set.

  static_assert(sizeof(IntrusiveStringList) == sizeof(void*),
                "IntrusiveStringList should be just a pointer");
  std::vector<IntrusiveStringList> entries_;
};

}  // namespace dfly
