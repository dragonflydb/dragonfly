// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/types.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

class PageUsage;

#define PREFETCH_READ(x) __builtin_prefetch(x, 0, 1)

// oah_entry.h - a single set member: a string key plus its expiry and cached hash.
//
// OAHEntry is a non-owning accessor over a TaggedPtr that points at a [expiry, key_size, key] heap
// blob. It manages that string: the static Create()/Destroy() allocate and free the blob, and
// SetExpiry/SetExtHash/ReallocIfNeeded update it in place. The top 12 and bottom 3 bits of a heap
// pointer are always free (user addresses are <= 52 bits), so flags live there: bit 0 stays clear
// for OAHPtr's entry/vector tag, OAHEntry uses bits 1-2 (expiry/sso) and 52-63 (cached hash).
class OAHEntry {
 public:
  // A uint64_t packing the heap pointer together with tag/flag bits in its free low/high bits.
  using TaggedPtr = uint64_t;

  static constexpr size_t kExpiryBit = 1ULL << 1;
  static constexpr size_t kSsoBit = 1ULL << 2;  // 1-byte (vs 4) key-length field

  static constexpr size_t kExtHashShift = 52;
  static constexpr uint32_t kExtHashSize = 12;
  static constexpr size_t kExtHashMask = 0xFFFULL;
  static constexpr size_t kExtHashShiftedMask = kExtHashMask << kExtHashShift;

  static constexpr size_t kTagMask = (4095ULL << 52) | 7;  // 12 high + 3 low tag bits

  static TaggedPtr Create(std::string_view key, uint32_t expiry = UINT32_MAX);
  static void Destroy(TaggedPtr tagged_ptr);

  explicit OAHEntry(TaggedPtr& slot) : slot_(&slot) {
  }
  OAHEntry(const OAHEntry&) = default;
  OAHEntry& operator=(const OAHEntry&) = default;

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
    return {GetKeyData(), GetKeySize()};
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

  // Sets the fingerprint on a FRESH entry (ext-hash bits still 0, heap ptr <= 52 bits): ORs
  // directly, no read-mask. `shifted_ext_hash` must carry bits only in [kExtHashShift, 64); use
  // SetExtHash if the entry may already hold a fingerprint.
  void SetShiftedExtHash(uint64_t shifted_ext_hash) {
    assert((shifted_ext_hash & ~kExtHashShiftedMask) == 0);
    SetTaggedPtr(GetTaggedPtr() | shifted_ext_hash);
  }

  // Reallocates the key blob if its page is underutilized; returns the usable-size delta and sets
  // *realloced when the buffer moved.
  ssize_t ReallocIfNeeded(PageUsage* page_usage, bool* realloced);

  char* Raw() const {
    return (char*)(GetTaggedPtr() & ~kTagMask);
  }

  // Returns the control word and zeroes the slot, transferring ownership to the caller.
  TaggedPtr Release() {
    TaggedPtr res = GetTaggedPtr();
    SetTaggedPtr(0);
    return res;
  }

  // Lets the iterator drill down (it->Key()).
  OAHEntry* operator->() {
    return this;
  }

 protected:
  const char* GetKeyData() const {
    uint32_t key_field_size = HasSso() ? 1 : 4;
    return Raw() + GetExpirySize() + key_field_size;
  }
  uint32_t GetKeySize() const;

  void SetExpiryBit(bool b);

  // Reallocates the key blob with `expiry`, preserving the key and stored ext-hash.
  void Rebuild(uint32_t expiry);

  void SetSsoBit() {
    SetTaggedPtr(GetTaggedPtr() | kSsoBit);
  }
  bool HasSso() const {
    return (GetTaggedPtr() & kSsoBit) != 0;
  }
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

}  // namespace dfly
