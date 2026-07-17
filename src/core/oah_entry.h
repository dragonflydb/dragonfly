// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/types.h>

#include <cassert>
#include <cstdint>
#include <string_view>

#include "core/oah_base.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

class PageUsage;

// oah_entry.h - a single set member: a string key plus its expiry and cached hash.
//
// OAHEntry is a non-owning accessor over a TaggedPtr that points at a [expiry?, control, key] heap
// blob. It manages that string: the static Create()/Destroy() allocate and free the blob, and
// SetExpiry/SetExtHash/ReallocIfNeeded update it in place. The top 12 and bottom 3 bits of a heap
// pointer are always free (user addresses are <= 52 bits), so flags live there: bit 0 stays clear
// for OAHPtr's entry/vector tag, OAHEntry uses bit 1 (expiry) and 52-63 (cached hash).
//
// The blob begins with an optional 4-byte expiry, then an oah::size-encoded key size followed by
// the key bytes. See oah::size (core/oah_base.h) for the size encoding.
class OAHEntry {
 public:
  using TaggedPtr = oah::TaggedPtr;

  // Builds an entry blob [expiry?, key_header, key] storing `key` (already-encoded content)
  // verbatim. The header records `real_size`; whether the key is encoded is derived from
  // key.size() != real_size.
  static TaggedPtr Create(std::string_view key, uint32_t real_size, uint32_t expiry = UINT32_MAX);
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

  // Deserializes the stored key: its codec header plus a pointer to the stored content bytes. The
  // table decodes/compares this (OAHEntry itself never ascii-encodes or decodes).
  oah::key::Stored StoredKey() const {
    const char* hdr = Raw() + GetExpirySize();
    const oah::key::Header h = oah::key::ReadHeader(hdr);
    return {h, hdr + h.field_size};
  }

  // Stored key content (ascii-packed or raw) used for hashing. Not the logical key.
  std::string_view KeyContent() const {
    const oah::key::Stored s = StoredKey();
    return {s.content, s.header.content_size};
  }

  // Compares this entry's key against a query key (content bytes + logical length). Reads the
  // header inline (not via StoredKey) to stay register-friendly on the hot path.
  bool KeyMatches(std::string_view content, uint32_t len) const {
    const char* hdr = Raw() + GetExpirySize();
    const oah::key::Header h = oah::key::ReadHeader(hdr);
    return oah::key::Matches(h, hdr + h.field_size, content, len);
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

  // Sets the fingerprint on a FRESH entry (ext-hash bits still 0, heap ptr <= 52 bits): ORs
  // directly, no read-mask. `shifted_ext_hash` must carry bits only in [kExtHashShift, 64); use
  // SetExtHash if the entry may already hold a fingerprint.
  void SetShiftedExtHash(uint64_t shifted_ext_hash) {
    assert((shifted_ext_hash & ~oah::kExtHashShiftedMask) == 0);
    SetTaggedPtr(GetTaggedPtr() | shifted_ext_hash);
  }

  // Reallocates the key blob if its page is underutilized; returns the usable-size delta and sets
  // *realloced when the buffer moved.
  ssize_t ReallocIfNeeded(PageUsage* page_usage, bool* realloced);

  char* Raw() const {
    return (char*)(GetTaggedPtr() & ~oah::kTagMask);
  }

  // Returns the control word and zeroes the slot, transferring ownership to the caller.
  TaggedPtr Release() {
    TaggedPtr res = GetTaggedPtr();
    SetTaggedPtr(0);
    return res;
  }

  // Lets the iterator drill down (it->KeyContent()).
  OAHEntry* operator->() {
    return this;
  }

 protected:
  // Reallocates the key blob with `expiry`, preserving the key and stored ext-hash.
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

}  // namespace dfly
