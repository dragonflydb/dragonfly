// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "core/detail/bitpacking.h"

namespace dfly {

// A key reduced once to the bytes used for hashing and comparison: 7-bit ASCII-packed into buf_
// when worth it (ascii, length 8..128), else a view of the original bytes. Uses the shared
// detail::bitpacking codec. Non-copyable: content() may alias buf_.
class ASCIIStr {
 public:
  static constexpr uint32_t kMinLen = 8;    // shorter strings pack to the same size, so stay raw
  static constexpr uint32_t kMaxLen = 128;  // largest length we ascii-encode

  explicit ASCIIStr(std::string_view s) : len_(static_cast<uint32_t>(s.size())) {
    if (s.size() >= kMinLen && s.size() <= kMaxLen &&
        detail::validate_ascii_fast(s.data(), s.size())) {
      detail::ascii_pack_simd2(s.data(), s.size(), reinterpret_cast<uint8_t*>(buf_));
      content_ = {buf_, detail::binpacked_len(s.size())};
    } else {
      content_ = s;
    }
  }

  ASCIIStr(const ASCIIStr&) = delete;
  ASCIIStr& operator=(const ASCIIStr&) = delete;

  std::string_view content() const {
    return content_;
  }
  uint32_t len() const {
    return len_;
  }
  bool encoded() const {
    return content_.size() != len_;  // packed keys are strictly shorter than their logical length
  }

 private:
  std::string_view content_;
  uint32_t len_;
  char buf_[kMaxLen];
};

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

// Variable-width size field shared by OAHEntry and OAHPair. Control-byte layout (size::Read masks
// bit 7 off, so the key codec below can layer its flag there without disturbing the size):
//   bit 7    : kEncodingBit   - key-codec flag (ascii-packed key); 0 for a plain size
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

}  // namespace size

// Key codec shared by OAHEntry/OAHPair. A stored key is a control/size header followed by content
// bytes (ascii-packed or raw). Hashing and comparison run on the content bytes; only retrieval
// (Decode) reconstructs the logical key. The header and content need not be contiguous (the map
// keeps the value pointer between them), so read/compare/decode take the two pointers separately.
namespace key {

inline constexpr uint8_t kEncodedBit = size::kEncodingBit;  // control bit 7 marks an encoded key

struct Header {
  bool encoded;
  uint32_t len;           // logical key length
  uint32_t field_size;    // header (control/size) bytes
  uint32_t content_size;  // stored content bytes (packed or raw)
};

// A deserialized key: its header plus a pointer to the stored content bytes (packed or raw).
// OAHEntry/OAHPair produce this from a blob; the table compares (Matches) or decodes (Decode) it.
struct Stored {
  Header header;
  const char* content;
};

// Writes the key header for a logical length `len` at `hdr`; returns its size. The length uses the
// oah::size encoding (bit 7 clear); `encoded_bit` (kEncodedBit or 0) is then set on top to flag an
// ascii-packed key, keeping the size format independent of the flag.
inline uint32_t WriteHeader(uint8_t encoded_bit, uint32_t len, char* hdr) {
  const uint32_t fs = size::FieldSize(len);
  size::Write(len, fs, hdr);
  *hdr = static_cast<char>(*hdr | encoded_bit);
  return fs;
}

inline Header ReadHeader(const char* hdr) {
  const bool encoded = static_cast<uint8_t>(*hdr) & kEncodedBit;
  const size::Decoded d = size::Read(hdr);  // ignores bit 7, so the flag doesn't disturb the size
  const uint32_t content_size =
      encoded ? static_cast<uint32_t>(detail::binpacked_len(d.size)) : d.size;
  return {encoded, d.size, d.field_size, content_size};
}

// Compares a stored key (header `h`, content `content`) against a query key (its stored content
// bytes `key_content` and logical length `key_len`). The len check guards packed keys whose
// distinct lengths collide to the same packed size (e.g. 15 and 16 chars both pack to 14 bytes).
inline bool Matches(const Header& h, const char* content, std::string_view key_content,
                    uint32_t key_len) {
  const bool key_encoded = key_content.size() != key_len;
  if (h.encoded != key_encoded || (h.encoded && h.len != key_len))
    return false;
  return h.content_size == key_content.size() &&
         std::memcmp(content, key_content.data(), key_content.size()) == 0;
}

// A logical-key view that keeps decoded content inline. Raw keys remain a zero-copy view into their
// entry; encoded keys are unpacked into the bounded inline buffer. Keeping this object alive keeps
// an encoded key view valid across nested calls and fiber yields, without thread-local storage.
class Decoded {
 public:
  explicit Decoded(const Stored& stored)
      : content_(stored.content), len_(stored.header.len), encoded_(stored.header.encoded) {
    assert(!encoded_ || (len_ >= ASCIIStr::kMinLen && len_ <= ASCIIStr::kMaxLen));
    if (encoded_) {
      detail::ascii_unpack(reinterpret_cast<const uint8_t*>(content_), len_, decoded_);
      content_ = nullptr;
    }
  }

  Decoded(const Decoded& other) noexcept
      : content_(other.content_), len_(other.len_), encoded_(other.encoded_) {
    if (encoded_)
      std::memcpy(decoded_, other.decoded_, len_);
  }

  Decoded& operator=(const Decoded& other) noexcept {
    if (this != &other) {
      content_ = other.content_;
      len_ = other.len_;
      encoded_ = other.encoded_;
      if (encoded_)
        std::memcpy(decoded_, other.decoded_, len_);
    }
    return *this;
  }

  Decoded(Decoded&& other) noexcept : Decoded(other) {
  }

  Decoded& operator=(Decoded&& other) noexcept {
    return operator=(other);
  }

  std::string_view view() const& {
    return {data(), len_};
  }
  std::string_view view() const&& = delete;

  const char* data() const& {
    return encoded_ ? decoded_ : content_;
  }
  const char* data() const&& = delete;
  size_t size() const {
    return len_;
  }
  bool empty() const {
    return len_ == 0;
  }

 private:
  const char* content_;
  uint32_t len_;
  bool encoded_;
  char decoded_[ASCIIStr::kMaxLen];
};

inline Decoded Decode(const Stored& stored) {
  return Decoded{stored};
}

}  // namespace key

}  // namespace oah
}  // namespace dfly
