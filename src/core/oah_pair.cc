// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_pair.h"

#include "base/logging.h"
#include "core/page_usage/page_usage_stats.h"

namespace dfly {

using namespace oah;

TaggedPtr OAHPair::BuildBlob(std::string_view key, char* value_ptr, size_t value_size,
                             uint32_t expiry) {
  const size_t key_size = key.size();
  assert(key_size <= size::kMaxSize);
  assert(value_size <= size::kMaxSize);
  const uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);
  const uint32_t key_size_field = size::FieldSize(key_size);
  const uint32_t val_size_field = size::FieldSize(value_size);

  const size_t blob_size = expiry_size + key_size_field + val_size_field + sizeof(char*) + key_size;
  char* blob = (char*)zmalloc(blob_size);
  TaggedPtr tagged_ptr = reinterpret_cast<TaggedPtr>(blob);
  if (expiry_size) {
    tagged_ptr |= kExpiryBit;
    std::memcpy(blob, &expiry, sizeof(expiry));
  }

  char* p = blob + expiry_size;
  size::Write(key_size, key_size_field, p);
  p += key_size_field;
  size::Write(value_size, val_size_field, p);
  p += val_size_field;

  std::memcpy(p, &value_ptr, sizeof(value_ptr));
  p += sizeof(value_ptr);

  // args are never null, even when empty (see CmdArgParser::SafeSV), so memcpy needs no size guard.
  DCHECK(key.data());
  std::memcpy(p, key.data(), key_size);
  return tagged_ptr;
}

TaggedPtr OAHPair::Create(std::string_view key, std::string_view value, uint32_t expiry) {
  char* value_ptr = nullptr;
  if (!value.empty()) {
    value_ptr = (char*)zmalloc(value.size());
    std::memcpy(value_ptr, value.data(), value.size());
  }
  return BuildBlob(key, value_ptr, value.size(), expiry);
}

void OAHPair::Destroy(TaggedPtr tagged_ptr) {
  if (tagged_ptr == 0)
    return;
  OAHPair pair(tagged_ptr);
  if (char* value = pair.ValuePtr())
    zfree(value);
  zfree(pair.Raw());
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
  const Header header = ParseHeader();
  // The rebuilt blob references the same value buffer, so only the old blob is freed.
  TaggedPtr rebuilt =
      BuildBlob({header.key, header.key_size}, ReadValuePtr(header), header.value_size, expiry);
  OAHPair(rebuilt).SetExtHash(saved_hash);
  zfree(Raw());
  SetTaggedPtr(rebuilt);
}

void OAHPair::SetExpiry(uint32_t at_sec) {
  if (HasExpiry()) {
    std::memcpy(Raw(), &at_sec, sizeof(at_sec));
  } else {
    Rebuild(at_sec);
  }
}

void OAHPair::ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used) {
  if (GetExpiry() <= time_now) {
    *alloc_used -= AllocSize();
    Destroy(Release());
    --*set_size;
  }
}

int64_t OAHPair::ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
  *realloced = false;
  if (Empty())
    return 0;

  // Relocate the blob and its separate value buffer independently: each moves only when its own
  // page is underutilized (a large value on a fully-used page stays put).
  char* value_buffer = ValuePtr();
  const bool realloc_blob = page_usage->IsPageForObjectUnderUtilized(Raw());
  const bool realloc_value = value_buffer && page_usage->IsPageForObjectUnderUtilized(value_buffer);
  if (!realloc_blob && !realloc_value)
    return 0;

  const size_t old_alloc = AllocSize();
  if (realloc_blob)
    Rebuild(HasExpiry() ? GetExpiry() : UINT32_MAX);

  if (realloc_value) {
    const Header header = ParseHeader();
    char* old_value = ReadValuePtr(header);
    char* new_value = (char*)zmalloc(header.value_size);
    std::memcpy(new_value, old_value, header.value_size);
    std::memcpy(header.value_ptr_field, &new_value, sizeof(new_value));
    zfree(old_value);
  }

  *realloced = true;
  return static_cast<int64_t>(AllocSize()) - static_cast<int64_t>(old_alloc);
}

}  // namespace dfly
