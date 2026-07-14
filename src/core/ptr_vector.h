// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <new>
#include <type_traits>

#include "core/oah_base.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

// ptr_vector.h - a growable heap array of T addressed through a single TaggedPtr.
//
// PtrVector is a non-owning accessor over a TaggedPtr that encodes (array pointer, element count).
// It manages the element array: the static Create()/Destroy() allocate and free it,
// Grow()/Realloc() resize it in place. Capacity grows by 2 up to kLinearMax then doubles, so sizes
// stay even for the 2-lane SIMD probe; the 11-bit size field holds the count, or its base-2 log
// when kLogModeBit is set (power-of-two sizes).
template <class T> class PtrVector {
  using TaggedPtr = oah::TaggedPtr;
  static constexpr size_t kSizeShift = 52;
  static constexpr size_t kSizeMask = 0x7FFULL;
  static constexpr size_t kLogModeBit = 1ULL << 63;
  static constexpr size_t kSizeFieldMask = (kSizeMask << kSizeShift) | kLogModeBit;
  static constexpr size_t kLinearMax = 128;

 public:
  static TaggedPtr Create(size_t count) {
    T* p = reinterpret_cast<T*>(zmalloc(sizeof(T) * count));
    for (size_t i = 0; i < count; ++i)
      new (p + i) T();
    return EncodeSize(reinterpret_cast<TaggedPtr>(p), count);
  }

  // Destroys every slot (all are constructed by Create/Reallocate) before freeing the array.
  static void Destroy(TaggedPtr tagged_ptr) {
    T* raw = RawOf(tagged_ptr);
    if (!raw)
      return;
    if constexpr (!std::is_trivially_destructible_v<T>) {
      const size_t size = SizeOf(tagged_ptr);
      for (size_t i = 0; i < size; ++i)
        raw[i].~T();
    }
    zfree(raw);
  }

  explicit PtrVector(TaggedPtr& slot) : slot_(&slot) {
  }
  PtrVector(const PtrVector&) = default;
  PtrVector& operator=(const PtrVector&) = default;

  T* begin() const {
    return Raw();
  }
  T* end() const {
    return Raw() + Size();
  }

  size_t Size() const {
    return SizeOf(GetTaggedPtr());
  }

  size_t AllocSize() const {
    return Size() * sizeof(T);
  }

  T& operator[](size_t idx) {
    return Raw()[idx];
  }
  const T& operator[](size_t idx) const {
    return Raw()[idx];
  }

  T* Raw() const {
    return RawOf(GetTaggedPtr());
  }

  bool Empty() const {
    if (GetTaggedPtr() == 0)
      return true;
    for (auto& el : *this)
      if (el)
        return false;
    return true;
  }

  void Grow() {
    const size_t cur = Size();
    Reallocate(cur < kLinearMax ? cur + 2 : cur * 2);
  }

  // Defragments into a fresh same-size buffer.
  void Realloc() {
    Reallocate(Size());
  }

  // Returns the control word and zeroes the slot, transferring ownership to the caller.
  TaggedPtr Release() {
    TaggedPtr res = GetTaggedPtr();
    SetTaggedPtr(0);
    return res;
  }

 private:
  static T* RawOf(TaggedPtr tagged_ptr) {
    return reinterpret_cast<T*>(tagged_ptr & ~oah::kTagMask);
  }

  static size_t SizeOf(TaggedPtr tagged_ptr) {
    const uint64_t field = (tagged_ptr >> kSizeShift) & kSizeMask;
    return (tagged_ptr & kLogModeBit) ? (size_t(1) << field) : size_t(field);
  }

  // Stores `count` literally below kLinearMax, else its base-2 log; always sets kVectorBit.
  static TaggedPtr EncodeSize(TaggedPtr tagged_ptr, size_t count) {
    const bool log_mode = count >= kLinearMax;
    // Log mode keeps only log2(count), so at/above kLinearMax count must be a power of two.
    assert(!log_mode || std::has_single_bit(count));
    const uint64_t field = log_mode ? std::countr_zero(count) : count;
    assert(field <= kSizeMask);
    return (tagged_ptr & ~kSizeFieldMask) | oah::kVectorBit | (field << kSizeShift) |
           (log_mode ? kLogModeBit : 0);
  }

  void Reallocate(size_t new_count) {
    T* new_ptr = reinterpret_cast<T*>(zmalloc(sizeof(T) * new_count));
    const size_t keep = std::min(Size(), new_count);
    for (size_t i = 0; i < keep; ++i)
      new (new_ptr + i) T(std::move(Raw()[i]));
    for (size_t i = keep; i < new_count; ++i)
      new (new_ptr + i) T();
    const TaggedPtr old = GetTaggedPtr();
    SetTaggedPtr(EncodeSize(reinterpret_cast<TaggedPtr>(new_ptr), new_count));
    Destroy(old);
  }

  TaggedPtr GetTaggedPtr() const {
    return *slot_;
  }
  void SetTaggedPtr(TaggedPtr tagged_ptr) {
    *slot_ = tagged_ptr;
  }

  TaggedPtr* slot_;
};

}  // namespace dfly
