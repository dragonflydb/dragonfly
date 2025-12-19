// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstring>
#include <string_view>

#include "base/hash.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

#define PREFETCH_READ(x) __builtin_prefetch(x, 0, 1)
#define FORCE_INLINE __attribute__((always_inline))

static uint64_t Hash(std::string_view str) {
  constexpr XXH64_hash_t kHashSeed = 24061983;
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}

static uint32_t BucketId(uint64_t hash, uint32_t capacity_log) {
  return hash >> (64 - capacity_log);
}

// TODO add allocator support
template <class T> class PtrVector {
  static constexpr size_t kVectorBit = 1ULL << 0;          // first 3 bits aren't used by pointer
  static constexpr size_t kTagMask = (4095ULL << 52) | 7;  // we reserve 12 high bits and 3 low bits

  static constexpr size_t kLogSizeShift = 56;
  static constexpr size_t kLogSizeMask = 0xFFULL;
  static constexpr size_t kLogSizeShiftedMask = kLogSizeMask << kLogSizeShift;

 public:
  static PtrVector FromLogSize(uint64_t log_size) {
    return PtrVector(log_size);
  }

  T* begin() const {
    return &Raw()[0];
  }

  T* end() const {
    return &Raw()[Size()];
  }

  PtrVector(PtrVector&& other) {
    uptr_ = other.uptr_;
    other.uptr_ = 0;
  }

  ~PtrVector() {
    Clear();
  }

  size_t LogSize() const {
    return (uptr_ >> kLogSizeShift) & kLogSizeMask;
  }

  size_t Size() const {
    return 1 << LogSize();
  }

  uint64_t Release() {
    uint64_t res = uptr_;
    uptr_ = 0;
    return res;
  }

  void ResizeLog(uint64_t new_log_size) {
    auto new_ptr = reinterpret_cast<T*>(zmalloc(sizeof(T) << new_log_size));
    size_t new_size = 1 << new_log_size;
    const size_t size = std::min(Size(), new_size);
    for (size_t i = 0; i < size; ++i) {
      new (new_ptr + i) T(std::move(Raw()[i]));
    }
    for (size_t i = size; i < new_size; ++i) {
      new (new_ptr + i) T();
    }
    Clear();
    uptr_ = reinterpret_cast<uint64_t>(new_ptr);
    SetLogSize(new_log_size);
  }

  T& operator[](size_t idx) {
    return Raw()[idx];
  }

  const T& operator[](size_t idx) const {
    return Raw()[idx];
  }

  T* Raw() const {
    return (T*)(uptr_ & ~kTagMask);
  }

 private:
  void Clear() {
    const size_t size = Size();
    T* raw = Raw();
    if (!raw)
      return;
    for (size_t i = 0; i < size; ++i) {
      if (raw[i])
        raw[i].~T();
    }

    zfree(Raw());
    uptr_ = 0;
  }

  // because of log_size I prefer to hide it
  PtrVector(uint64_t log_size) {
    assert(log_size <= 32);
    uptr_ = reinterpret_cast<uint64_t>(zmalloc(sizeof(T) << log_size));
    const uint64_t size = 1 << log_size;
    for (uint64_t i = 0; i < size; ++i) {
      new (reinterpret_cast<T*>(uptr_) + i) T();
    }
    SetLogSize(log_size);
  }

  void SetLogSize(uint64_t log_size) {
    uptr_ = (uptr_ & ~kLogSizeShiftedMask) | kVectorBit | (uint64_t(log_size) << kLogSizeShift);
  }

  uint64_t uptr_ = 0;
};

// doesn't possess memory, it should be created and release manually
class OAHEntry {
  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  // first 3 bits aren't used by pointer
  static constexpr size_t kVectorBit = 1ULL << 0;
  static constexpr size_t kExpiryBit = 1ULL << 1;
  // if bit is set the string length field is 1 byte instead of 4
  static constexpr size_t kSsoBit = 1ULL << 2;

  // extended hash allows us to reduce keys comparisons
  static constexpr size_t kExtHashShift = 52;
  static constexpr uint32_t kExtHashSize = 12;
  static constexpr size_t kExtHashMask = 0xFFFULL;
  static constexpr size_t kExtHashShiftedMask = kExtHashMask << kExtHashShift;

  static constexpr size_t kTagMask = (4095ULL << 52) | 7;  // we reserve 12 high bits and 3 low.

 public:
  OAHEntry() = default;

  OAHEntry(std::string_view key, uint32_t expiry = UINT32_MAX);

  // TODO add initializer list constructor
  OAHEntry(PtrVector<OAHEntry>&& vec) {
    data_ = vec.Release() | kVectorBit;
  }

  OAHEntry(const OAHEntry& e) = delete;
  OAHEntry(OAHEntry&& e) {
    data_ = e.data_;
    e.data_ = 0;
  }

  // consider manual removing, we waste a lot of time to check nullptr
  ~OAHEntry() {
    Clear();
  }

  OAHEntry& operator=(const OAHEntry& e) = delete;
  OAHEntry& operator=(OAHEntry&& e) {
    std::swap(data_, e.data_);
    return *this;
  }

  bool Empty() const {
    return data_ == 0;
  }

  operator bool() const {
    return !Empty();
  }

  bool IsVector() const {
    return (data_ & kVectorBit) != 0;
  }

  bool IsEntry() const {
    return (data_ != 0) && !(data_ & kVectorBit);
  }

  size_t AllocSize() const {
    return zmalloc_usable_size(Raw());
  }

  PtrVector<OAHEntry>& AsVector() {
    static_assert(sizeof(PtrVector<OAHEntry>) == sizeof(uint64_t));
    return *reinterpret_cast<PtrVector<OAHEntry>*>(&data_);
  }

  std::string_view Key() const {
    assert(!IsVector());
    return {GetKeyData(), GetKeySize()};
  }

  bool HasExpiry() const {
    return (data_ & kExpiryBit) != 0;
  }

  // returns the expiry time of the current entry or UINT32_MAX if no expiry is set.
  uint32_t GetExpiry() const;

  // TODO consider another option to implement iterator
  OAHEntry* operator->() {
    return this;
  }

  uint64_t GetHash() const {
    return (data_ & kExtHashShiftedMask) >> kExtHashShift;
  }

  bool CheckBucketAffiliation(uint32_t bucket_id, uint32_t capacity_log, uint32_t shift_log);

  static uint64_t CalcExtHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log);

  bool CheckNoCollisions(const uint64_t ext_hash);

  bool CheckExtendedHash(const uint64_t ext_hash, uint32_t capacity_log, uint32_t shift_log);

  // shift_log identify which bucket the element belongs to
  uint64_t SetHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log);

  void ClearHash() {
    data_ &= ~kExtHashShiftedMask;
  }

  // return new bucket_id
  uint32_t Rehash(uint32_t current_bucket_id, uint32_t prev_capacity_log, uint32_t new_capacity_log,
                  uint32_t shift_log);

  void SetExpiry(uint32_t at_sec);

  std::optional<uint32_t> Find(std::string_view str, uint64_t ext_hash, uint32_t capacity_log,
                               uint32_t shift_log, uint32_t* set_size, uint32_t time_now = 0);

  void ExpireIfNeeded(uint32_t time_now, uint32_t* set_size);

  // TODO refactor, because it's inefficient
  uint32_t Insert(OAHEntry&& e);

  uint32_t ElementsNum();

  // TODO remove, it is inefficient
  OAHEntry& operator[](uint32_t pos);

  OAHEntry Remove(uint32_t pos);

  OAHEntry Pop();

  char* Raw() const {
    return (char*)(data_ & ~kTagMask);
  }

 protected:
  void Clear();

  const char* GetKeyData() const {
    uint32_t key_field_size = HasSso() ? 1 : 4;
    return Raw() + GetExpirySize() + key_field_size;
  }

  uint32_t GetKeySize() const;

  void SetExpiryBit(bool b);

  void SetVectorBit() {
    data_ |= kVectorBit;
  }

  void SetSsoBit() {
    data_ |= kSsoBit;
  }

  bool HasSso() const {
    return (data_ & kSsoBit) != 0;
  }

  size_t Size();

  std::uint32_t GetExpirySize() const {
    return HasExpiry() ? sizeof(std::uint32_t) : 0;
  }

  // memory daya layout [Expiry, key_size, key]
  uint64_t data_ = 0;
};

}  // namespace dfly
