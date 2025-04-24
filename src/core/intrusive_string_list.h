// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstring>
#include <string_view>

extern "C" {
#include "redis/zmalloc.h"
}

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

  static void Destroy(ISLEntry entry) {
    zfree(entry.Raw());
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

  void SetExpiryTime(uint32_t ttl_sec) {
    if (HasExpiry()) {
      auto* ttl_pos = Raw() + sizeof(char*);
      std::memcpy(ttl_pos, &ttl_sec, sizeof(ttl_sec));
    } else {
      ISLEntry new_entry = Create(Key(), ttl_sec);
      std::swap(data_, new_entry.data_);
      Destroy(new_entry);
    }
  }

 private:
  static ISLEntry Create(std::string_view key, uint32_t ttl_sec = UINT32_MAX) {
    char* next = nullptr;
    uint32_t key_size = key.size();

    bool has_ttl = ttl_sec != UINT32_MAX;

    auto size = sizeof(next) + sizeof(key_size) + key_size + has_ttl * sizeof(ttl_sec);

    char* data = (char*)zmalloc(size);
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

  // TODO rewrite, because it's dangerous operations, ISLEntry shouldn't returned without owner
  [[nodiscard]] ISLEntry Pop(uint32_t curr_time) {
    for (auto it = start_; it && it.ExpiryTime() < curr_time; it = start_) {
      start_ = it.Next();
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
    return start_;
  }

  // TODO consider to wrap ISLEntry to prevent usage out of the list
  ISLEntry Emplace(std::string_view key, uint32_t ttl_sec = UINT32_MAX) {
    return Insert(ISLEntry::Create(key, ttl_sec));
  }

  // TODO consider to wrap ISLEntry to prevent usage out of the list
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

}  // namespace dfly
