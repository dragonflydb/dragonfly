// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstring>
#include <string_view>

#include "base/hash.h"
#include "base/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

static uint64_t Hash(std::string_view str) {
  constexpr XXH64_hash_t kHashSeed = 24061983;
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}

static uint32_t BucketId(uint64_t hash, uint32_t capacity_log) {
  assert(capacity_log > 0);
  return hash >> (64 - capacity_log);
}
// doesn't possess memory, it should be created and release manually
class ISLEntry {
  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  static constexpr size_t kTtlBit = 1ULL << 52;

  // if bit is set the string length field is 1 byte instead of 4
  static constexpr size_t kSsoBit = 1ULL << 53;
  static constexpr size_t kVectorBit = 1ULL << 54;

  // extended hash allows us to reduce keys comparisons
  static constexpr size_t kExtHashShift = 56;
  static constexpr uint32_t kExtHashSize = 8;
  static constexpr size_t kExtHashMask = 0xFFULL;
  static constexpr size_t kExtHashShiftedMask = kExtHashMask << kExtHashShift;

  static constexpr size_t kTagMask = 4095ULL << 52;  // we reserve 12 high bits.

 public:
  ISLEntry() = default;

  ISLEntry(std::string_view key, uint32_t ttl_sec = UINT32_MAX) {
    uint32_t key_size = key.size();

    uint32_t ttl_size = (ttl_sec != UINT32_MAX) * sizeof(ttl_sec);

    uint32_t key_len_field_size = key_size <= std::numeric_limits<uint8_t>::max() ? 1 : 4;

    auto size = key_len_field_size + key_size + ttl_size;

    data_ = (char*)zmalloc(size);

    auto* ttl_pos = data_;
    if (ttl_size) {
      SetTtlBit(true);
      std::memcpy(ttl_pos, &ttl_sec, sizeof(ttl_sec));
    }

    auto* key_size_pos = ttl_pos + ttl_size;
    if (key_len_field_size == 1) {
      SetSsoBit();
      uint8_t sso_key_size = key_size;
      std::memcpy(key_size_pos, &sso_key_size, key_len_field_size);
    } else {
      std::memcpy(key_size_pos, &key_size, key_len_field_size);
    }

    auto* key_pos = key_size_pos + key_len_field_size;
    std::memcpy(key_pos, key.data(), key_size);
  }

  // TODO add initializer list constructor
  ISLEntry(size_t vector_size) {
    // TODO rewrite to simple array
    data_ = reinterpret_cast<char*>(new std::vector<ISLEntry>(vector_size));
    SetVectorBit();
  }

  ISLEntry(const ISLEntry& e) = delete;
  ISLEntry(ISLEntry&& e) {
    data_ = e.data_;
    e.data_ = nullptr;
  }

  ~ISLEntry() {
    Clear();
  }

  ISLEntry& operator=(const ISLEntry& e) = delete;
  ISLEntry& operator=(ISLEntry&& e) {
    std::swap(data_, e.data_);
    return *this;
  }

  bool Empty() const {
    return !Raw();
  }

  operator bool() const {
    return !Empty();
  }

  bool IsVector() const {
    return (uptr() & kVectorBit) != 0;
  }

  std::vector<ISLEntry>& AsVector() {
    return *reinterpret_cast<std::vector<ISLEntry>*>(Raw());
  }

  std::string_view Key() const {
    DCHECK(!IsVector());
    return {GetKeyData(), GetKeySize()};
  }

  bool HasTtl() const {
    return (uptr() & kTtlBit) != 0;
  }

  // returns the expiry time of the current entry or UINT32_MAX if no ttl is set.
  uint32_t GetTtl() const {
    std::uint32_t res = UINT32_MAX;
    if (HasTtl()) {
      DCHECK(!IsVector());
      std::memcpy(&res, Raw(), sizeof(res));
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

  bool CheckBucketAffiliation(uint32_t bucket_id, uint32_t capacity_log, uint32_t shift_log) {
    DCHECK(!IsVector());
    if (Empty())
      return false;
    uint32_t bucket_id_hash_part = capacity_log > shift_log ? shift_log : capacity_log;
    uint32_t bucket_mask = (1 << bucket_id_hash_part) - 1;
    bucket_id &= bucket_mask;
    auto stored_hash = GetExtendedHash();
    if (!stored_hash) {
      stored_hash = SetExtendedHash(Hash(Key()), capacity_log, shift_log);
    }
    uint32_t stored_bucket_id = stored_hash >> (kExtHashSize - bucket_id_hash_part);
    return bucket_id == stored_bucket_id;
  }

  bool CheckExtendedHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
    if (Empty())
      return false;
    const uint32_t start_hash_bit = capacity_log > shift_log ? capacity_log - shift_log : 0;
    const uint32_t ext_hash_shift = 64 - start_hash_bit - kExtHashSize;
    const uint64_t ext_hash = (hash >> ext_hash_shift) & kExtHashMask;
    auto stored_hash = GetExtendedHash();
    if (!stored_hash && !IsVector()) {
      stored_hash = SetExtendedHash(Hash(Key()), capacity_log, shift_log);
    }
    return stored_hash == ext_hash;
  }

  // TODO rename to SetHash
  // shift_log identify which bucket the element belongs to
  uint64_t SetExtendedHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
    DCHECK(!IsVector());
    const uint32_t start_hash_bit = capacity_log > shift_log ? capacity_log - shift_log : 0;
    const uint32_t ext_hash_shift = 64 - start_hash_bit - kExtHashSize;
    const uint64_t result_hash = (hash >> ext_hash_shift) & kExtHashMask;
    const uint64_t ext_hash = result_hash << kExtHashShift;
    data_ = (char*)((uptr() & ~kExtHashShiftedMask) | ext_hash);
    return result_hash;
  }

  void ClearHash() {
    data_ = (char*)((uptr() & ~kExtHashShiftedMask));
  }

  // return new bucket_id
  uint32_t Rehash(uint32_t current_bucket_id, uint32_t prev_capacity_log, uint32_t new_capacity_log,
                  uint32_t shift_log) {
    DCHECK(!IsVector());
    auto stored_hash = GetExtendedHash();

    const uint32_t logs_diff = new_capacity_log - prev_capacity_log;
    const uint32_t prev_significant_bits =
        prev_capacity_log > shift_log ? shift_log : prev_capacity_log;
    const uint32_t needed_hash_bits = prev_significant_bits + logs_diff;

    if (!stored_hash || needed_hash_bits > kExtHashSize) {
      auto hash = Hash(Key());
      SetExtendedHash(hash, new_capacity_log, shift_log);
      return BucketId(hash, new_capacity_log);
    }

    const uint32_t real_bucket_end = stored_hash >> (kExtHashSize - prev_significant_bits);
    const uint32_t prev_shift_mask = (1 << prev_significant_bits) - 1;
    const uint32_t curr_shift = (current_bucket_id - real_bucket_end) & prev_shift_mask;
    const uint32_t prev_bucket_mask = (1 << prev_capacity_log) - 1;
    const uint32_t base_bucket_id = (current_bucket_id - curr_shift) & prev_bucket_mask;

    const uint32_t last_bits_mask = (1 << logs_diff) - 1;
    const uint32_t stored_hash_shift = kExtHashSize - needed_hash_bits;
    const uint32_t last_bits = (stored_hash >> stored_hash_shift) & last_bits_mask;
    const uint32_t new_bucket_id = (base_bucket_id << logs_diff) | last_bits;

    ClearHash();  // the cache is invalid after rehash operation

    DCHECK_EQ(BucketId(Hash(Key()), new_capacity_log), new_bucket_id);

    return new_bucket_id;
  }

  void SetTtl(uint32_t ttl_sec) {
    DCHECK(!IsVector());
    if (HasTtl()) {
      auto* ttl_pos = Raw();
      std::memcpy(ttl_pos, &ttl_sec, sizeof(ttl_sec));
    } else {
      *this = ISLEntry(ttl_sec);
    }
  }

  // TODO refactor, because it's inefficient
  std::optional<uint32_t> Find(std::string_view str, uint64_t hash, uint32_t capacity_log,
                               uint32_t shift_log, uint32_t* set_size,
                               uint32_t time_now = UINT32_MAX) {
    if (Empty())
      return std::nullopt;
    if (!IsVector()) {
      return CheckExtendedHash(hash, capacity_log, shift_log) && Key() == str
                 ? 0
                 : std::optional<uint32_t>();
    }
    auto& vec = AsVector();
    for (size_t i = 0, size = vec.size(); i < size; ++i) {
      if (vec[i].CheckExtendedHash(hash, capacity_log, shift_log) && vec[i].Key() == str) {
        return i;
      }
    }
    return std::nullopt;
  }

  // TODO refactor, because it's inefficient
  uint32_t Insert(ISLEntry&& e) {
    if (Empty()) {
      *this = std::move(e);
      return 0;
    } else if (!IsVector()) {
      ISLEntry tmp(2);
      auto& arr = tmp.AsVector();
      arr[0] = std::move(*this);
      arr[1] = std::move(e);
      *this = std::move(tmp);
      return 1;
    } else {
      auto& arr = AsVector();
      size_t i = 0;
      for (; i < arr.size(); ++i) {
        if (!arr[i]) {
          arr[i] = std::move(e);
        }
      }
      arr.push_back(std::move(e));
      return arr.size() - 1;
    }
  }

  uint32_t ElementsNum() {
    if (Empty()) {
      return 0;
    } else if (!IsVector()) {
      return 1;
    }
    return AsVector().size();
  }

  ISLEntry& operator[](uint32_t pos) {
    DCHECK(!Empty());
    if (!IsVector()) {
      DCHECK(pos == 0);
      return *this;
    } else {
      auto& arr = AsVector();
      DCHECK(pos < arr.size());
      return arr[pos];
    }
  }

  ISLEntry Remove(uint32_t pos) {
    if (Empty()) {
      // I'm not sure that this scenario should be check at all
      DCHECK(pos == 0);
      return ISLEntry();
    } else if (!IsVector()) {
      DCHECK(pos == 0);
      return std::move(*this);
    } else {
      auto& arr = AsVector();
      DCHECK(pos < arr.size());
      return std::move(arr[pos]);
    }
  }

  ISLEntry Pop() {
    if (IsVector()) {
      auto& arr = AsVector();
      for (auto& e : arr) {
        if (e)
          return std::move(e);
      }
    }
    return std::move(*this);
  }

  template <class T, std::enable_if_t<std::is_invocable_v<T, std::string_view>>* = nullptr>
  bool Scan(const T& cb, uint32_t bucket_id, uint32_t capacity_log, uint32_t shift_log) {
    if (!IsVector()) {
      if (CheckBucketAffiliation(bucket_id, capacity_log, shift_log)) {
        cb(Key());
        return true;
      }
    } else {
      auto& arr = AsVector();
      bool result = false;
      for (auto& el : arr) {
        if (el.CheckBucketAffiliation(bucket_id, capacity_log, shift_log)) {
          cb(el.Key());
          result = true;
        }
      }
      return result;
    }
    return false;
  }

 protected:
  void Clear() {
    if (IsVector()) {
      delete &AsVector();
    } else {
      zfree(Raw());
    }
  }

  const char* GetKeyData() const {
    uint32_t key_field_size = HasSso() ? 1 : 4;
    return Raw() + GetTtlSize() + key_field_size;
  }

  uint32_t GetKeySize() const {
    if (HasSso()) {
      uint8_t size = 0;
      std::memcpy(&size, Raw() + GetTtlSize(), sizeof(size));
      return size;
    }
    uint32_t size = 0;
    std::memcpy(&size, Raw() + GetTtlSize(), sizeof(size));
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

  void SetVectorBit() {
    data_ = (char*)(uptr() | kVectorBit);
  }

  void SetSsoBit() {
    data_ = (char*)(uptr() | kSsoBit);
  }

  bool HasSso() const {
    return (uptr() & kSsoBit) != 0;
  }

  size_t Size() {
    size_t key_field_size = HasSso() ? 1 : 4;
    size_t ttl_field_size = HasTtl() ? 4 : 0;
    return ttl_field_size + key_field_size + GetKeySize();
  }
  std::uint32_t GetTtlSize() const {
    return HasTtl() ? sizeof(std::uint32_t) : 0;
  }

  // TODO add optimization for big keys
  // memory daya layout [TTL, key_size, key]
  char* data_ = nullptr;
};

// template <class T, std::enable_if_t<std::is_invocable_v<T, std::string_view>>* = nullptr>
// bool Scan(const T& cb, uint32_t* set_size, uint32_t curr_time) {
//   for (auto it = start_; it && it.ExpiryTime() <= curr_time; it = start_) {
//     start_ = it.Next();
//     ISLEntry::Destroy(it);
//     (*set_size)--;
//   }

//   for (auto curr = start_, next = start_; curr; curr = next) {
//     cb(curr.Key());
//     next = curr.Next();
//     for (auto tmp = next; tmp && tmp.ExpiryTime() <= curr_time; tmp = next) {
//       (*set_size)--;
//       next = next.Next();
//       ISLEntry::Destroy(tmp);
//     }
//     curr.SetNext(next);
//   }
//   return start_;
// }

// size_t ObjMallocUsed() const {
//   return obj_malloc_used_;
// };

}  // namespace dfly
