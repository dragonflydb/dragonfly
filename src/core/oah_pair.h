// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/types.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>

#include "core/oah_base.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

class PageUsage;

// oah_pair.h - a map member (key + value, plus expiry and cached hash), the map counterpart of
// OAHEntry, sharing its pointer-tag layout so OAHTable SIMD probes stay entry-agnostic. Blob
// layout:
//   [expiry?, key_size, value_size, value_ptr:8B, key]      (sizes use variable-width oah::size)
// The value always lives in its own zmalloc buffer, so its bytes stay >=8-byte aligned for typed
// reinterpretation (e.g. vector search). An empty value stores a null value_ptr and no buffer.
class OAHPair {
 public:
  using TaggedPtr = oah::TaggedPtr;

  // Creates a pair blob storing `key` (already-encoded content) verbatim plus a fresh copy of
  // `value`. The header records `real_size`; whether the key is encoded is derived from
  // key.size() != real_size.
  static TaggedPtr Create(std::string_view key, uint32_t real_size, std::string_view value,
                          uint32_t expiry = UINT32_MAX);
  static void Destroy(TaggedPtr tagged_ptr);

  explicit OAHPair(TaggedPtr& slot) : slot_(&slot) {
  }
  OAHPair(const OAHPair&) = default;
  OAHPair& operator=(const OAHPair&) = default;

  bool Empty() const {
    return GetTaggedPtr() == 0;
  }
  operator bool() const {
    return !Empty();
  }

  size_t AllocSize() const {
    size_t result = zmalloc_usable_size(Raw());
    if (char* value = ValuePtr())
      result += zmalloc_usable_size(value);
    return result;
  }

  // Deserializes the stored key: its codec header plus a pointer to the stored content bytes. The
  // table decodes/compares this (OAHPair itself never ascii-encodes or decodes).
  oah::key::Stored StoredKey() const {
    const Header header = ParseHeader();
    return {header.key, header.key_content};
  }

  std::string_view Value() const {
    const Header header = ParseHeader();
    return {ReadValuePtr(header), header.value_size};
  }

  // Stored key content (ascii-packed or raw) used for hashing. Not the logical key.
  std::string_view KeyContent() const {
    const Header header = ParseHeader();
    return {header.key_content, header.key.content_size};
  }

  // Compares this pair's key against a query key (content bytes + logical length). Reads the header
  // inline (not via StoredKey) to stay register-friendly on the hot path.
  bool KeyMatches(std::string_view content, uint32_t len) const {
    const Header header = ParseHeader();
    return oah::key::Matches(header.key, header.key_content, content, len);
  }

  bool HasExpiry() const {
    return (GetTaggedPtr() & oah::kExpiryBit) != 0;
  }
  uint32_t GetExpiry() const;
  void SetExpiry(uint32_t at_sec);
  void ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used);

  uint64_t GetHash() const {
    return (GetTaggedPtr() & oah::kExtHashShiftedMask) >> oah::kExtHashShift;
  }
  void SetExtHash(uint64_t ext_hash);

  void SetShiftedExtHash(uint64_t shifted_ext_hash) {
    assert((shifted_ext_hash & ~oah::kExtHashShiftedMask) == 0);
    SetTaggedPtr(GetTaggedPtr() | shifted_ext_hash);
  }

  ssize_t ReallocIfNeeded(PageUsage* page_usage, bool* realloced);

  char* Raw() const {
    return (char*)(GetTaggedPtr() & ~oah::kTagMask);
  }

  TaggedPtr Release() {
    TaggedPtr res = GetTaggedPtr();
    SetTaggedPtr(0);
    return res;
  }

  OAHPair* operator->() {
    return this;
  }

 protected:
  struct Header {
    char* value_ptr_field;
    char* key_content;  // packed/raw key content bytes (stored after value_ptr)
    uint32_t value_size;
    oah::key::Header key;  // encoded flag, logical length, header/content sizes
  };

  Header ParseHeader() const {
    char* p = Raw() + GetExpirySize();
    const oah::key::Header key = oah::key::ReadHeader(p);
    p += key.field_size;
    const oah::size::Decoded value = oah::size::Read(p);
    p += value.field_size;
    return {p, p + sizeof(char*), value.size, key};
  }

  static char* ReadValuePtr(const Header& header) {
    char* value = nullptr;
    std::memcpy(&value, header.value_ptr_field, sizeof(value));
    return value;
  }

  char* ValuePtr() const {
    return ReadValuePtr(ParseHeader());
  }

  // Builds a blob that takes ownership of `value_ptr` (null for an empty value), storing the
  // already-encoded `key` content verbatim. The header records `real_size`; whether the key is
  // encoded is derived from key.size() != real_size. Create allocates a fresh value buffer; Rebuild
  // passes the existing key/value through unchanged.
  static TaggedPtr BuildBlob(std::string_view key, uint32_t real_size, char* value_ptr,
                             size_t value_size, uint32_t expiry);

  // Reallocates the blob with `expiry`, preserving key, value and the cached ext-hash.
  void Rebuild(uint32_t expiry);

  std::uint32_t GetExpirySize() const {
    return HasExpiry() ? sizeof(std::uint32_t) : 0;
  }

  TaggedPtr GetTaggedPtr() const {
    return *slot_;
  }
  void SetTaggedPtr(TaggedPtr tagged_ptr) {
    *slot_ = tagged_ptr;
  }

  TaggedPtr* slot_;
};

// RAII owner of an OAHPair blob handed out by OAHMap::Extract / AddOrExchange; move-only, frees the
// blob on destruction. Read the key/value through pair(); empty when the operation returned
// nothing.
class OwnedOAHPair {
 public:
  using TaggedPtr = oah::TaggedPtr;

  OwnedOAHPair() = default;
  explicit OwnedOAHPair(TaggedPtr tp) : tp_(tp) {
  }
  ~OwnedOAHPair() {
    OAHPair::Destroy(tp_);
  }

  OwnedOAHPair(OwnedOAHPair&& o) noexcept : tp_(std::exchange(o.tp_, 0)) {
  }
  OwnedOAHPair& operator=(OwnedOAHPair&& o) noexcept {
    if (this != &o) {
      OAHPair::Destroy(tp_);
      tp_ = std::exchange(o.tp_, 0);
    }
    return *this;
  }
  OwnedOAHPair(const OwnedOAHPair&) = delete;
  OwnedOAHPair& operator=(const OwnedOAHPair&) = delete;

  explicit operator bool() const {
    return tp_ != 0;
  }

  OAHPair pair() {
    return OAHPair(tp_);
  }

  TaggedPtr get() const {
    return tp_;
  }
  TaggedPtr release() {
    return std::exchange(tp_, 0);
  }

 private:
  TaggedPtr tp_ = 0;
};

}  // namespace dfly
