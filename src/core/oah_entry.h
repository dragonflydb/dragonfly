// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bit>
#include <cassert>
#include <cstring>
#include <string_view>

#include "base/hash.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

class PageUsage;

#define PREFETCH_READ(x) __builtin_prefetch(x, 0, 1)
#define FORCE_INLINE __attribute__((always_inline))

// TODO add allocator support
//
// Capacity grows by 2 below kLinearMax (sizes stay even, keeping the 2-lane SIMD
// probe tail-free), then doubles at/above it. The 11-bit tag field holds either a
// literal count or, with kLogModeBit set, its base-2 log (power-of-2 sizes).
template <class T> class PtrVector {
  static constexpr size_t kVectorBit = 1ULL << 0;          // first 3 bits aren't used by pointer
  static constexpr size_t kTagMask = (4095ULL << 52) | 7;  // we reserve 12 high bits and 3 low bits

  static constexpr size_t kSizeShift = 52;
  static constexpr size_t kSizeMask = 0x7FFULL;  // 11-bit field: count, or log2 in log mode
  static constexpr size_t kLogModeBit = 1ULL << 63;
  static constexpr size_t kSizeFieldMask = (kSizeMask << kSizeShift) | kLogModeBit;

  static constexpr size_t kLinearMax = 128;

 public:
  // Allocates exactly `count` elements (used for the initial small vector).
  static PtrVector FromSize(size_t count) {
    return PtrVector(count);
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

  // Number of elements. Normal mode stores the count directly; log mode stores
  // its base-2 log.
  size_t Size() const {
    const uint64_t field = (uptr_ >> kSizeShift) & kSizeMask;
    return (uptr_ & kLogModeBit) ? (size_t(1) << field) : size_t(field);
  }

  uint64_t Release() {
    uint64_t res = uptr_;
    uptr_ = 0;
    return res;
  }

  bool Empty() const {
    if (uptr_ == 0)
      return true;

    for (auto& el : *this) {
      if (el)
        return false;
    }
    return true;
  }

  // Grows per policy: +2 elements below kLinearMax, doubling at/above it. Starting
  // from an even size (2), this keeps Size() even (a multiple of the 2-lane probe).
  void Grow() {
    const size_t cur = Size();
    Reallocate(cur < kLinearMax ? cur + 2 : cur * 2);
  }

  // Reallocates to the same size (defrag), preserving contents.
  void Realloc() {
    Reallocate(Size());
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

  size_t AllocSize() const {
    return Size() * sizeof(T);
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

  // Allocates `new_count` slots, moves existing elements over, frees the old
  // buffer, and records the new size.
  void Reallocate(size_t new_count) {
    auto* new_ptr = reinterpret_cast<T*>(zmalloc(sizeof(T) * new_count));
    const size_t keep = std::min(Size(), new_count);
    for (size_t i = 0; i < keep; ++i) {
      new (new_ptr + i) T(std::move(Raw()[i]));
    }
    for (size_t i = keep; i < new_count; ++i) {
      new (new_ptr + i) T();
    }
    Clear();
    uptr_ = reinterpret_cast<uint64_t>(new_ptr);
    SetSize(new_count);
  }

  explicit PtrVector(size_t count) {
    uptr_ = reinterpret_cast<uint64_t>(zmalloc(sizeof(T) * count));
    for (size_t i = 0; i < count; ++i) {
      new (reinterpret_cast<T*>(uptr_) + i) T();
    }
    SetSize(count);
  }

  // Encodes the element count: a literal count below kLinearMax, or its base-2
  // log at/above it (countr_zero == log2 since such sizes are powers of two).
  void SetSize(size_t count) {
    const bool log_mode = count >= kLinearMax;
    const uint64_t field = log_mode ? std::countr_zero(count) : count;
    assert(field <= kSizeMask);
    uptr_ = (uptr_ & ~kSizeFieldMask) | kVectorBit | (field << kSizeShift) |
            (log_mode ? kLogModeBit : 0);
  }

  uint64_t uptr_ = 0;
};

// doesn't possess memory, it should be created and release manually
class OAHEntry {
 public:
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
    return (data_ != 0) & !(data_ & kVectorBit);
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

  bool CheckNoCollisions(const uint64_t ext_hash);

  void SetExtHash(uint64_t ext_hash);

  void ClearHash() {
    data_ &= ~kExtHashShiftedMask;
  }

  void SetExpiry(uint32_t at_sec);

  void ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used);

  // Reallocates fragmented buffers under this entry: its string buffer, or for a vector
  // every inner entry plus the array buffer. Returns the cumulative zmalloc_usable_size
  // delta across inner buffers (the array buffer keeps its size). *realloced = any moved.
  ssize_t ReallocIfNeeded(PageUsage* page_usage, bool* realloced);

  // TODO refactor, because it's inefficient
  // Returns additional allocation size of ptrVector
  [[nodiscard]] size_t Insert(OAHEntry&& e);

  uint32_t ElementsNum();

  // TODO remove, it is inefficient
  OAHEntry& operator[](uint32_t pos);

  OAHEntry Remove(uint32_t pos);

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
