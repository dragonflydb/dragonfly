// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// #include <cassert>
#include <cstring>
#include <string_view>

#include "base/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {
// doesn't possess memory, it should be created and release manually
class ISLEntry {
  friend class IntrusiveStringList;

  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  static constexpr size_t kTtlBit = 1ULL << 52;

  // if bit is set the string length field is 1 byte instead of 4
  static constexpr size_t kSsoBit = 1ULL << 53;

  // extended hash allows us to reduce keys comparisons
  static constexpr size_t kExtHashShift = 56;
  static constexpr uint32_t ext_hash_bit_size = 8;
  static constexpr size_t kExtHashMask = 0xFFULL;
  static constexpr size_t kExtHashShiftedMask = kExtHashMask << kExtHashShift;

  static constexpr size_t kTagMask = 4095ULL << 52;  // we reserve 12 high bits.

 public:
  ISLEntry() = default;

  ISLEntry(char* data) {
    data_ = data;
  }

  ISLEntry(const ISLEntry& e) = default;
  ISLEntry(ISLEntry&& e) {
    data_ = e.data_;
    e.data_ = nullptr;
  }

  ISLEntry& operator=(const ISLEntry& e) = default;
  ISLEntry& operator=(ISLEntry&& e) {
    data_ = e.data_;
    e.data_ = nullptr;
    return *this;
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

  // TODO consider another option to implement iterator
  ISLEntry* operator->() {
    return this;
  }

  uint64_t GetExtendedHash() const {
    return (uptr() & kExtHashShiftedMask) >> kExtHashShift;
  }

  bool CheckBucketAffiliation(uint32_t bucket_id, uint32_t capacity_log, uint32_t shift_log) const {
    uint32_t bucket_id_hash_part = capacity_log > shift_log ? shift_log : capacity_log;
    uint32_t bucket_mask = (1 << bucket_id_hash_part) - 1;
    bucket_id &= bucket_mask;
    uint32_t stored_bucket_id = GetExtendedHash() >> (ext_hash_bit_size - bucket_id_hash_part);
    return bucket_id == stored_bucket_id;
  }

  bool CheckExtendedHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) const {
    uint32_t start_hash_bit = capacity_log > shift_log ? capacity_log - shift_log : 0;
    uint32_t ext_hash_shift = 64 - start_hash_bit - ext_hash_bit_size;
    uint64_t ext_hash = (hash >> ext_hash_shift) & kExtHashMask;
    return GetExtendedHash() == ext_hash;
  }

  // TODO rename to SetHash
  // shift_log identify which bucket the element belongs to
  void SetExtendedHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
    uint32_t start_hash_bit = capacity_log > shift_log ? capacity_log - shift_log : 0;
    uint32_t ext_hash_shift = 64 - start_hash_bit - ext_hash_bit_size;
    uint64_t ext_hash = ((hash >> ext_hash_shift) << kExtHashShift) & kExtHashShiftedMask;
    data_ = (char*)((uptr() & ~kExtHashShiftedMask) | ext_hash);
  }

 protected:
  static ISLEntry Create(std::string_view key, char* next = nullptr,
                         uint32_t ttl_sec = UINT32_MAX) {
    uint32_t key_size = key.size();

    bool has_ttl = ttl_sec != UINT32_MAX;

    uint32_t key_len_field_size = key_size <= std::numeric_limits<uint8_t>::max() ? 1 : 4;

    auto size = sizeof(next) + key_len_field_size + key_size + has_ttl * sizeof(ttl_sec);

    char* data = (char*)zmalloc(size);
    ISLEntry res(data);

    std::memcpy(data, &next, sizeof(next));

    auto* ttl_pos = data + sizeof(next);
    if (has_ttl) {
      res.SetTtlBit(true);
      std::memcpy(ttl_pos, &ttl_sec, sizeof(ttl_sec));
    }

    auto* key_size_pos = ttl_pos + res.GetTtlSize();
    if (key_len_field_size == 1) {
      res.SetSsoBit();
      uint8_t sso_key_size = key_size;
      std::memcpy(key_size_pos, &sso_key_size, key_len_field_size);
    } else {
      std::memcpy(key_size_pos, &key_size, key_len_field_size);
    }

    auto* key_pos = key_size_pos + key_len_field_size;
    std::memcpy(key_pos, key.data(), key_size);

    return res;
  }

  static void Destroy(ISLEntry& entry) {
    zfree(entry.Raw());
  }

  ISLEntry Next() const {
    ISLEntry next;
    std::memcpy(&next.data_, Raw(), sizeof(next));
    return next;
  }

  ISLEntry FakePrev() {
    return ISLEntry(reinterpret_cast<char*>(&data_));
  }

  void SetNext(ISLEntry next) {
    std::memcpy(Raw(), &next, sizeof(next));
  }

  const char* GetKeyData() const {
    uint32_t key_field_size = HasSso() ? 1 : 4;
    return Raw() + sizeof(ISLEntry*) + GetTtlSize() + key_field_size;
  }

  uint32_t GetKeySize() const {
    if (HasSso()) {
      uint8_t size = 0;
      std::memcpy(&size, Raw() + sizeof(ISLEntry*) + GetTtlSize(), sizeof(size));
      return size;
    }
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

  void SetSsoBit() {
    data_ = (char*)(uptr() | kSsoBit);
  }

  bool HasSso() const {
    return (uptr() & kSsoBit) != 0;
  }

  bool HasTtl() const {
    return (uptr() & kTtlBit) != 0;
  }

  size_t Size() {
    size_t key_field_size = HasSso() ? 1 : 4;
    size_t ttl_field_size = HasTtl() ? 4 : 0;
    return (sizeof(char*) + ttl_field_size + key_field_size + GetKeySize());
  }

  [[nodiscard]] bool UpdateTtl(uint32_t ttl_sec) {
    if (HasTtl()) {
      auto* ttl_pos = Raw() + sizeof(char*);
      std::memcpy(ttl_pos, &ttl_sec, sizeof(ttl_sec));
      return true;
    }
    return false;
  }

  std::uint32_t GetTtlSize() const {
    return HasTtl() ? sizeof(std::uint32_t) : 0;
  }

  // TODO consider use SDS strings or other approach
  // TODO add optimization for big keys
  // memory daya layout [ISLEntry*, TTL, key_size, key]
  char* data_ = nullptr;
};

class UniqueISLEntry : private ISLEntry {
 public:
  ~UniqueISLEntry() {
    Destroy(*this);
  }

  UniqueISLEntry() = default;
  UniqueISLEntry(ISLEntry e) : ISLEntry(e) {
  }
  UniqueISLEntry(UniqueISLEntry&& e) = default;
  UniqueISLEntry& operator=(UniqueISLEntry&& e) = default;

  using ISLEntry::ExpiryTime;
  using ISLEntry::HasExpiry;
  using ISLEntry::Key;
  using ISLEntry::operator bool;

  ISLEntry Release() {
    ISLEntry res = *this;
    data_ = nullptr;
    return res;
  }

 private:
  UniqueISLEntry(const UniqueISLEntry&) = delete;
};

class IntrusiveStringList {
 public:
  size_t obj_malloc_used_;  // TODO: think how we can keep track of size
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = ISLEntry;
    using pointer = ISLEntry*;
    using reference = ISLEntry&;

    iterator(ISLEntry prev = end_.FakePrev()) : prev_(prev) {
      DCHECK(prev);
    }

    uint32_t ExpiryTime() const {
      return prev_.Next().ExpiryTime();
    }

    void SetExpiryTime(uint32_t ttl_sec, size_t* obj_malloc_used) {
      auto entry = prev_.Next();

      if (!entry.UpdateTtl(ttl_sec)) {
        ISLEntry new_entry = ISLEntry::Create(entry.Key(), entry.Next().data_, ttl_sec);
        (*obj_malloc_used) += sizeof(new_entry) + new_entry.Size();
        (*obj_malloc_used) -= sizeof(entry) + entry.Size();
        ISLEntry::Destroy(entry);
        prev_.SetNext(new_entry);
      }
    }

    bool ExpireIfNeeded(uint32_t time_now, size_t* obj_malloc_used) {
      auto entry = prev_.Next();

      if (auto entry_time = entry.ExpiryTime();
          entry_time != UINT32_MAX && time_now >= entry_time) {
        prev_.SetNext(prev_.Next().Next());
        (*obj_malloc_used) -= sizeof(entry) + entry.Size();
        ISLEntry::Destroy(entry);
        return true;
      }
      return false;
    }

    bool HasExpiry() const {
      return prev_.Next().HasExpiry();
    }

    iterator& operator++() {
      prev_ = prev_.Next();
      return *this;
    }

    operator bool() const {
      return prev_.data_ && prev_.Next();
    }

    value_type operator*() {
      return prev_.Next();
    }

    value_type operator->() {
      return prev_.Next();
    }

    bool operator==(const iterator& r) {
      return prev_.Next() == r.prev_.Next();
    }

    bool operator!=(const iterator& r) {
      return !operator==(r);
    }

   private:
    ISLEntry prev_;
  };

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

  iterator begin() {
    return start_.FakePrev();
  }

  static iterator end() {
    return end_.FakePrev();
  }

  ISLEntry& Insert(ISLEntry e) {
    e.SetNext(start_);
    start_ = e;
    obj_malloc_used_ += sizeof(e) + e.Size();
    return start_;
  }

  UniqueISLEntry Pop(uint32_t curr_time) {
    for (auto it = start_; it && it.ExpiryTime() < curr_time; it = start_) {
      start_ = it.Next();
      obj_malloc_used_ -= sizeof(it) + it.Size();
      ISLEntry::Destroy(it);
    }
    auto res = start_;
    if (start_) {
      start_ = start_.Next();
      // TODO consider to res.SetNext(nullptr); for now it looks superfluous
    }
    return res;
  }

  bool Empty() {
    return !start_;
  }

  // TODO consider to wrap ISLEntry to prevent usage out of the list
  ISLEntry& Emplace(std::string_view key, uint32_t ttl_sec = UINT32_MAX) {
    return Insert(ISLEntry::Create(key, nullptr, ttl_sec));
  }

  // TODO consider to wrap ISLEntry to prevent usage out of the list
  IntrusiveStringList::iterator Find(std::string_view str, uint32_t* set_size,
                                     uint32_t time_now = UINT32_MAX) {
    auto it = begin();

    for (; it; ++it) {
      if (it.ExpireIfNeeded(time_now, &obj_malloc_used_)) {
        (*set_size)--;
        continue;
      }
      if (it->Key() == str) {
        break;
      }
    }

    return it;
  }

  // TODO consider to wrap ISLEntry to prevent usage out of the list
  IntrusiveStringList::iterator Find(std::string_view str, uint64_t hash, uint32_t capacity_log,
                                     uint32_t shift_log, uint32_t* set_size,
                                     uint32_t time_now = UINT32_MAX) {
    auto entry = begin();
    for (; entry; ++entry) {
      if (entry.ExpireIfNeeded(time_now, &obj_malloc_used_)) {
        (*set_size)--;
        continue;
      }
      if (entry->CheckExtendedHash(hash, capacity_log, shift_log) && entry->Key() == str)
        break;
    }
    return entry;
  }

  bool Erase(std::string_view str, uint32_t* set_size) {
    auto cond = [str](const ISLEntry e) { return str == e.Key(); };
    return Erase(cond, set_size);
  }

  template <class T, std::enable_if_t<std::is_invocable_v<T, ISLEntry>>* = nullptr>
  bool Erase(const T& cond, uint32_t* set_size) {
    if (!start_) {
      return false;
    }

    if (auto it = start_; cond(it)) {
      (*set_size)--;
      start_ = it.Next();
      ISLEntry::Destroy(it);
      return true;
    }

    for (auto prev = start_, it = start_.Next(); it; prev = it, it = it.Next()) {
      if (cond(it)) {
        (*set_size)--;
        prev.SetNext(it.Next());
        ISLEntry::Destroy(it);
        return true;
      }
    }
    return false;
  }

  template <class T, std::enable_if_t<std::is_invocable_v<T, std::string_view>>* = nullptr>
  bool Scan(const T& cb, uint32_t* set_size, uint32_t curr_time) {
    for (auto it = start_; it && it.ExpiryTime() <= curr_time; it = start_) {
      start_ = it.Next();
      ISLEntry::Destroy(it);
      (*set_size)--;
    }

    for (auto curr = start_, next = start_; curr; curr = next) {
      cb(curr.Key());
      next = curr.Next();
      for (auto tmp = next; tmp && tmp.ExpiryTime() <= curr_time; tmp = next) {
        (*set_size)--;
        next = next.Next();
        ISLEntry::Destroy(tmp);
      }
      curr.SetNext(next);
    }
    return start_;
  }

  size_t ObjMallocUsed() const {
    return obj_malloc_used_;
  };

  bool ExpireIfNeeded(uint32_t time_now, uint32_t* set_size) {
    if (!start_) {
      return false;
    }

    auto it = begin();

    if (it->HasTtl()) {
      if (it.ExpireIfNeeded(time_now, &obj_malloc_used_)) {
        (*set_size)--;
        return true;
      }
      return false;
    }

    return false;
  }

 private:
  ISLEntry start_;
  static ISLEntry end_;
};

}  // namespace dfly
