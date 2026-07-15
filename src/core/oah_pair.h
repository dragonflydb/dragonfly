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

  static TaggedPtr Create(std::string_view key, std::string_view value,
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

  std::string_view Key() const {
    const Header header = ParseHeader();
    return {header.key, header.key_size};
  }

  std::string_view Value() const {
    const Header header = ParseHeader();
    return {ReadValuePtr(header), header.value_size};
  }

  std::pair<std::string_view, std::string_view> KeyValue() const {
    const Header header = ParseHeader();
    return {{header.key, header.key_size}, {ReadValuePtr(header), header.value_size}};
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
    char* key;
    uint32_t key_size;
    uint32_t value_size;
  };

  Header ParseHeader() const {
    char* p = Raw() + GetExpirySize();
    const oah::size::Decoded key = oah::size::Read(p);
    p += key.field_size;
    const oah::size::Decoded value = oah::size::Read(p);
    p += value.field_size;
    return {p, p + sizeof(char*), key.size, value.size};
  }

  static char* ReadValuePtr(const Header& header) {
    char* value = nullptr;
    std::memcpy(&value, header.value_ptr_field, sizeof(value));
    return value;
  }

  char* ValuePtr() const {
    return ReadValuePtr(ParseHeader());
  }

  // Builds a blob that takes ownership of `value_ptr` (null for an empty value): Create allocates a
  // fresh value buffer, Rebuild passes the existing one through unchanged.
  static TaggedPtr BuildBlob(std::string_view key, char* value_ptr, size_t value_size,
                             uint32_t expiry);

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
