// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_entry.h"

#include "base/logging.h"
#include "core/page_usage/page_usage_stats.h"

namespace dfly {

using namespace oah;

TaggedPtr OAHEntry::Create(std::string_view key, uint32_t expiry) {
  const size_t key_size = key.size();
  assert(key_size <= size::kMaxSize);
  const uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);
  const uint32_t key_size_field = size::FieldSize(key_size);

  auto* blob = (char*)zmalloc(expiry_size + key_size_field + key_size);
  TaggedPtr tagged_ptr = reinterpret_cast<TaggedPtr>(blob);
  if (expiry_size) {
    tagged_ptr |= kExpiryBit;
    std::memcpy(blob, &expiry, sizeof(expiry));
  }

  char* p = blob + expiry_size;
  size::Write(key_size, key_size_field, p);
  p += key_size_field;
  DCHECK(key.data());  // args are never null, even when empty (see CmdArgParser::SafeSV)
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

int64_t OAHEntry::ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
  *realloced = false;
  if (Empty())
    return 0;
  if (!page_usage->IsPageForObjectUnderUtilized(Raw()))
    return 0;

  const size_t old_alloc = AllocSize();
  Rebuild(HasExpiry() ? GetExpiry() : UINT32_MAX);
  *realloced = true;
  return static_cast<int64_t>(AllocSize()) - static_cast<int64_t>(old_alloc);
}

}  // namespace dfly
