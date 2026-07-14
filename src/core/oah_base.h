// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dfly {

// Shared building blocks for the open-addressing hash containers (OAHEntry/OAHPair/OAHTable).
namespace oah {

// A uint64_t that packs a heap pointer together with tag/flag bits in its unused low/high bits.
using TaggedPtr = uint64_t;

// Pointer tags shared by OAHEntry and OAHPair. Bit 0 is reserved for OAHPtr's vector tag, bit 1
// marks an expiry field, and bits 52-63 hold the cached hash. All three low bits are masked when
// recovering the allocation pointer so an entry type can use bit 2 for its own metadata.
inline constexpr size_t kVectorBit = 1ULL << 0;
inline constexpr size_t kExpiryBit = 1ULL << 1;
inline constexpr size_t kExtHashShift = 52;
inline constexpr uint32_t kExtHashSize = 12;
inline constexpr size_t kExtHashMask = 0xFFFULL;
inline constexpr size_t kExtHashShiftedMask = kExtHashMask << kExtHashShift;
inline constexpr size_t kLowTagMask = 7;
inline constexpr size_t kTagMask = kExtHashShiftedMask | kLowTagMask;

inline void PrefetchRead(const void* ptr) noexcept {
  __builtin_prefetch(ptr, 0, 1);
}

// Variable-width size field shared by OAHEntry and OAHPair.
//
// Control-byte layout:
//   bit 7    : reserved ASCII/raw encoding flag (currently always 0)
//   bit 6    : kBigSizeBit    - size doesn't fit in 6 bits, extra bytes follow
//   bit 5    : kThreeBytesBit - when kBigSizeBit is set: 3 extra bytes (otherwise 1)
//   bits 0-5 : inline size (< 64B); or bits 0-4 hold the low 5 bits of a larger size
namespace size {

inline constexpr uint8_t kEncodingBit = 1u << 7;
inline constexpr uint8_t kBigSizeBit = 1u << 6;
inline constexpr uint8_t kThreeBytesBit = 1u << 5;
inline constexpr uint8_t kInlineSizeMask = 0x3F;
inline constexpr uint8_t kLowSizeMask = 0x1F;
inline constexpr uint32_t kLowSizeBits = 5;
inline constexpr uint32_t kInlineSizeMax = kInlineSizeMask;
inline constexpr uint32_t kOneExtraByteMax = (1u << 13) - 1;
inline constexpr uint32_t kMaxSize = (1u << 29) - 1;

inline constexpr uint32_t kInlineFieldSize = 1;
inline constexpr uint32_t kMediumFieldSize = 2;
inline constexpr uint32_t kLargeFieldSize = 4;

struct Decoded {
  uint32_t size;
  uint32_t field_size;
};

inline uint32_t FieldSize(size_t size) {
  assert(size <= kMaxSize);
  return size <= kInlineSizeMax     ? kInlineFieldSize
         : size <= kOneExtraByteMax ? kMediumFieldSize
                                    : kLargeFieldSize;
}

inline uint32_t FieldSizeFromControl(uint8_t control) {
  return (control & kBigSizeBit) == 0 ? kInlineFieldSize
         : control & kThreeBytesBit   ? kLargeFieldSize
                                      : kMediumFieldSize;
}

inline void Write(size_t size, uint32_t field_size, char* dest) {
  assert(field_size == FieldSize(size));
  uint8_t control;
  if (field_size == kInlineFieldSize) {
    control = static_cast<uint8_t>(size);
  } else if (field_size == kMediumFieldSize) {
    control = static_cast<uint8_t>(kBigSizeBit | (size & kLowSizeMask));
  } else {
    control = static_cast<uint8_t>(kBigSizeBit | kThreeBytesBit | (size & kLowSizeMask));
  }

  *dest++ = static_cast<char>(control);
  size_t extra = size >> kLowSizeBits;
  for (uint32_t i = 1; i < field_size; ++i, extra >>= 8) {
    *dest++ = static_cast<char>(extra & 0xFF);
  }
}

inline Decoded Read(const char* src) {
  const uint8_t control = static_cast<uint8_t>(*src);
  if ((control & kBigSizeBit) == 0)
    return {static_cast<uint32_t>(control & kInlineSizeMask), kInlineFieldSize};

  const bool three_bytes = control & kThreeBytesBit;
  uint32_t extra = static_cast<uint8_t>(src[1]);
  if (three_bytes) {
    extra |= static_cast<uint32_t>(static_cast<uint8_t>(src[2])) << 8;
    extra |= static_cast<uint32_t>(static_cast<uint8_t>(src[3])) << 16;
  }
  const uint32_t size = (control & kLowSizeMask) | (extra << kLowSizeBits);
  return {size, three_bytes ? kLargeFieldSize : kMediumFieldSize};
}

inline uint32_t ReadLarge(const char* src) {
  const uint8_t control = static_cast<uint8_t>(src[0]);
  assert(FieldSizeFromControl(control) == kLargeFieldSize);
  uint32_t extra = static_cast<uint8_t>(src[1]);
  extra |= static_cast<uint32_t>(static_cast<uint8_t>(src[2])) << 8;
  extra |= static_cast<uint32_t>(static_cast<uint8_t>(src[3])) << 16;
  return (control & kLowSizeMask) | (extra << kLowSizeBits);
}

}  // namespace size
}  // namespace oah
}  // namespace dfly
