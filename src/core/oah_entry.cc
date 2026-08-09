// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_entry.h"

#include "base/logging.h"
#include "core/page_usage/page_usage_stats.h"

namespace dfly {

using namespace oah;

TaggedPtr OAHEntry::Create(std::string_view key, uint32_t real_size, uint32_t expiry) {
  const uint8_t encoded_bit = (key.size() != real_size) * key::kEncodedBit;
  const uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);
  const uint32_t hdr_size = size::FieldSize(real_size);

  auto* blob = (char*)zmalloc(expiry_size + hdr_size + key.size());
  TaggedPtr tagged_ptr = reinterpret_cast<TaggedPtr>(blob);
  if (expiry_size) {
    tagged_ptr |= kExpiryBit;
    std::memcpy(blob, &expiry, sizeof(expiry));
  }

  char* p = blob + expiry_size;
  key::WriteHeader(encoded_bit, real_size, p);
  // args are never null, even when empty (see CmdArgParser::SafeSV), so memcpy needs no size guard.
  DCHECK(key.data());
  std::memcpy(p + hdr_size, key.data(), key.size());
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
  // Copy the stored key verbatim (already encoded): no decode/re-encode, keep the encoded flag.
  const char* hdr = Raw() + GetExpirySize();
  const key::Header h = key::ReadHeader(hdr);
  TaggedPtr rebuilt = Create({hdr + h.field_size, h.content_size}, h.len, expiry);
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

}  // namespace dfly
