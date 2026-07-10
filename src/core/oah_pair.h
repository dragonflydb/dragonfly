// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/types.h>

#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>

#include "core/oah_entry.h"

namespace dfly {

class PageUsage;

// oah_pair.h - a map member (key + value, plus expiry and cached hash), the map counterpart of
// OAHEntry. It shares OAHEntry's pointer-tag layout so the OAHTable SIMD probes stay
// entry-agnostic. Heap blob: [expiry?, key_size:4B, key, value_size:4B, value].
class OAHPair {
 public:
  using TaggedPtr = uint64_t;

  // The tag lives in the TaggedPtr (not the blob), so it must match OAHEntry's for shared probes.
  static constexpr size_t kExpiryBit = OAHEntry::kExpiryBit;
  static constexpr size_t kExtHashShift = OAHEntry::kExtHashShift;
  static constexpr uint32_t kExtHashSize = OAHEntry::kExtHashSize;
  static constexpr size_t kExtHashMask = OAHEntry::kExtHashMask;
  static constexpr size_t kExtHashShiftedMask = OAHEntry::kExtHashShiftedMask;
  static constexpr size_t kTagMask = OAHEntry::kTagMask;

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
    return zmalloc_usable_size(Raw());
  }

  std::string_view Key() const {
    const char* p = Raw() + GetExpirySize();
    uint32_t key_size;
    std::memcpy(&key_size, p, sizeof(key_size));
    return {p + sizeof(key_size), key_size};
  }

  std::string_view Value() const {
    return KeyValue().second;
  }

  std::pair<std::string_view, std::string_view> KeyValue() const {
    const char* val = nullptr;
    std::string_view key = KeyInternal(&val);
    return {key, ValueFrom(val)};
  }

  bool HasExpiry() const {
    return (GetTaggedPtr() & kExpiryBit) != 0;
  }
  uint32_t GetExpiry() const;
  void SetExpiry(uint32_t at_sec);
  void ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used);

  uint64_t GetHash() const {
    return (GetTaggedPtr() & kExtHashShiftedMask) >> kExtHashShift;
  }
  void SetExtHash(uint64_t ext_hash);

  void SetShiftedExtHash(uint64_t shifted_ext_hash) {
    assert((shifted_ext_hash & ~kExtHashShiftedMask) == 0);
    SetTaggedPtr(GetTaggedPtr() | shifted_ext_hash);
  }

  ssize_t ReallocIfNeeded(PageUsage* page_usage, bool* realloced);

  char* Raw() const {
    return (char*)(GetTaggedPtr() & ~kTagMask);
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
  // Returns the key and, via out_val, the pointer to the following value_size field.
  std::string_view KeyInternal(const char** out_val) const {
    const char* p = Raw() + GetExpirySize();
    uint32_t key_size;
    std::memcpy(&key_size, p, sizeof(key_size));
    const char* key = p + sizeof(key_size);
    *out_val = key + key_size;
    return {key, key_size};
  }

  static std::string_view ValueFrom(const char* val) {
    uint32_t val_size;
    std::memcpy(&val_size, val, sizeof(val_size));
    return {val + sizeof(val_size), val_size};
  }

  void SetExpiryBit(bool b);

  // Reallocates the blob with `expiry`, preserving key, value and stored ext-hash.
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

// RAII owner of an OAHPair blob handed out by OAHMap::Extract / AddOrExchange. Frees it on
// destruction; move-only. Read the key/value through pair(); empty when the operation returned
// nothing.
class OwnedOAHPair {
 public:
  using TaggedPtr = OAHPair::TaggedPtr;

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

  // Non-owning view over the owned blob, for reading the key/value.
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
