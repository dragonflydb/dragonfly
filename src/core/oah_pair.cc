// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_pair.h"

#include "core/page_usage/page_usage_stats.h"

namespace dfly {

OAHPair::TaggedPtr OAHPair::Create(std::string_view key, std::string_view value, uint32_t expiry) {
  const uint32_t key_size = key.size();
  const uint32_t val_size = value.size();
  const uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);

  char* blob =
      (char*)zmalloc(expiry_size + sizeof(key_size) + key_size + sizeof(val_size) + val_size);
  TaggedPtr tagged_ptr = reinterpret_cast<TaggedPtr>(blob);
  if (expiry_size) {
    OAHPair(tagged_ptr).SetExpiryBit(true);
    std::memcpy(blob, &expiry, sizeof(expiry));
  }

  char* p = blob + expiry_size;
  std::memcpy(p, &key_size, sizeof(key_size));
  p += sizeof(key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  std::memcpy(p, &val_size, sizeof(val_size));
  p += sizeof(val_size);
  std::memcpy(p, value.data(), val_size);
  return tagged_ptr;
}

void OAHPair::Destroy(TaggedPtr tagged_ptr) {
  if (tagged_ptr == 0)
    return;
  zfree(reinterpret_cast<void*>(tagged_ptr & ~kTagMask));
}

uint32_t OAHPair::GetExpiry() const {
  std::uint32_t res = UINT32_MAX;
  if (HasExpiry())
    std::memcpy(&res, Raw(), sizeof(res));
  return res;
}

void OAHPair::SetExtHash(uint64_t ext_hash) {
  assert(GetTaggedPtr());
  SetTaggedPtr((GetTaggedPtr() & ~kExtHashShiftedMask) | (ext_hash << kExtHashShift));
}

void OAHPair::Rebuild(uint32_t expiry) {
  const uint64_t saved_hash = GetHash();
  auto [key, value] = KeyValue();  // views into the old blob; Create copies them before Destroy
  TaggedPtr rebuilt = Create(key, value, expiry);
  OAHPair(rebuilt).SetExtHash(saved_hash);
  Destroy(Release());
  SetTaggedPtr(rebuilt);
}

void OAHPair::SetExpiry(uint32_t at_sec) {
  if (HasExpiry()) {
    std::memcpy(Raw(), &at_sec, sizeof(at_sec));
  } else {
    Rebuild(at_sec);  // no expiry field yet: rebuild the blob with room for one
  }
}

void OAHPair::ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used) {
  if (GetExpiry() <= time_now) {
    *alloc_used -= AllocSize();
    Destroy(Release());
    --*set_size;
  }
}

ssize_t OAHPair::ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
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

void OAHPair::SetExpiryBit(bool b) {
  if (b)
    SetTaggedPtr(GetTaggedPtr() | kExpiryBit);
  else
    SetTaggedPtr(GetTaggedPtr() & ~kExpiryBit);
}

}  // namespace dfly
