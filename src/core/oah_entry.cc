// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_entry.h"

#include "core/page_usage/page_usage_stats.h"

namespace dfly {

OAHEntry::TaggedPtr OAHEntry::Create(std::string_view key, uint32_t expiry) {
  const uint32_t key_size = key.size();
  assert(key_size <= kMaxKeySize);
  uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);

  uint8_t control;
  uint32_t size_field_len;  // control byte + trailing extra size bytes
  if (key_size <= kInlineSizeMax) {
    control = static_cast<uint8_t>(key_size);
    size_field_len = 1;
  } else if (key_size <= kOneExtraByteMax) {
    control = static_cast<uint8_t>(kBigSizeBit | (key_size & kLowSizeMask));
    size_field_len = 2;
  } else {
    control = static_cast<uint8_t>(kBigSizeBit | kThreeBytesBit | (key_size & kLowSizeMask));
    size_field_len = 4;
  }

  auto* blob = (char*)zmalloc(expiry_size + size_field_len + key_size);
  TaggedPtr tagged_ptr = reinterpret_cast<TaggedPtr>(blob);
  if (expiry_size) {
    tagged_ptr |= kExpiryBit;
    std::memcpy(blob, &expiry, sizeof(expiry));
  }

  char* p = blob + expiry_size;
  *p++ = static_cast<char>(control);
  for (uint32_t extra = key_size >> kLowSizeBits, i = 1; i < size_field_len; ++i, extra >>= 8)
    *p++ = static_cast<char>(extra & 0xFF);
  std::memcpy(p, key.data(), key_size);
  return tagged_ptr;
}

void OAHEntry::Destroy(TaggedPtr tagged_ptr) {
  if (tagged_ptr == 0)
    return;
  zfree(reinterpret_cast<void*>(tagged_ptr & ~kTagMask));
}

uint32_t OAHEntry::GetExpiry() const {
  std::uint32_t res = UINT32_MAX;
  if (HasExpiry())
    std::memcpy(&res, Raw(), sizeof(res));
  return res;
}

void OAHEntry::SetExtHash(uint64_t ext_hash) {
  assert(GetTaggedPtr());
  SetTaggedPtr((GetTaggedPtr() & ~kExtHashShiftedMask) | (ext_hash << kExtHashShift));
}

void OAHEntry::Rebuild(uint32_t expiry) {
  const uint64_t saved_hash = GetHash();
  TaggedPtr rebuilt = Create(Key(), expiry);
  OAHEntry(rebuilt).SetExtHash(saved_hash);
  Destroy(Release());
  SetTaggedPtr(rebuilt);
}

void OAHEntry::SetExpiry(uint32_t at_sec) {
  if (HasExpiry()) {
    std::memcpy(Raw(), &at_sec, sizeof(at_sec));
  } else {
    Rebuild(at_sec);  // no expiry field yet: rebuild the blob with room for one
  }
}

void OAHEntry::ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used) {
  if (GetExpiry() <= time_now) {
    *alloc_used -= AllocSize();
    Destroy(Release());
    --*set_size;
  }
}

ssize_t OAHEntry::ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
  *realloced = false;
  if (Empty())
    return 0;
  if (!page_usage->IsPageForObjectUnderUtilized(Raw()))
    return 0;

  const size_t old_alloc = AllocSize();
  Rebuild(HasExpiry() ? GetExpiry() : UINT32_MAX);
  *realloced = true;
  return static_cast<ssize_t>(AllocSize()) - static_cast<ssize_t>(old_alloc);
}

std::string_view OAHEntry::KeyBig(const char* p, uint8_t control) const {
  uint32_t size = control & kLowSizeMask;
  uint32_t extra = static_cast<uint8_t>(p[1]);
  const char* key = p + 2;  // control byte + 1 extra size byte
  if (control & kThreeBytesBit) {
    extra |= static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8;
    extra |= static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 16;
    key = p + 4;  // control byte + 3 extra size bytes
  }
  size |= extra << kLowSizeBits;
  return {key, size};
}

}  // namespace dfly
