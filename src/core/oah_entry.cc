// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_entry.h"

#include "core/page_usage/page_usage_stats.h"

namespace dfly {

OAHEntry::TaggedPtr OAHEntry::Create(std::string_view key, uint32_t expiry) {
  uint32_t key_size = key.size();
  uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);
  uint32_t key_len_field_size = key_size <= std::numeric_limits<uint8_t>::max() ? 1 : 4;

  auto* blob = (char*)zmalloc(key_len_field_size + key_size + expiry_size);

  TaggedPtr tagged_ptr = reinterpret_cast<TaggedPtr>(blob);
  OAHEntry entry(tagged_ptr);  // accessor over the local control word being built
  if (expiry_size) {
    entry.SetExpiryBit(true);
    std::memcpy(blob, &expiry, sizeof(expiry));
  }

  auto* key_size_pos = blob + expiry_size;
  if (key_len_field_size == 1) {
    entry.SetSsoBit();
    uint8_t sso_key_size = key_size;
    std::memcpy(key_size_pos, &sso_key_size, key_len_field_size);
  } else {
    std::memcpy(key_size_pos, &key_size, key_len_field_size);
  }
  std::memcpy(key_size_pos + key_len_field_size, key.data(), key_size);
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

uint32_t OAHEntry::GetKeySize() const {
  if (HasSso()) {
    uint8_t size = 0;
    std::memcpy(&size, Raw() + GetExpirySize(), sizeof(size));
    return size;
  }
  uint32_t size = 0;
  std::memcpy(&size, Raw() + GetExpirySize(), sizeof(size));
  return size;
}

void OAHEntry::SetExpiryBit(bool b) {
  if (b)
    SetTaggedPtr(GetTaggedPtr() | kExpiryBit);
  else
    SetTaggedPtr(GetTaggedPtr() & ~kExpiryBit);
}

}  // namespace dfly
