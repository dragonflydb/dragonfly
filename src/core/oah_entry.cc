// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_entry.h"

#include "base/hash.h"
#include "base/logging.h"
#include "core/page_usage/page_usage_stats.h"

namespace dfly {

OAHEntry::OAHEntry(std::string_view key, uint32_t expiry) {
  uint32_t key_size = key.size();

  uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);

  uint32_t key_len_field_size = key_size <= std::numeric_limits<uint8_t>::max() ? 1 : 4;

  auto size = key_len_field_size + key_size + expiry_size;

  auto* expiry_pos = (char*)zmalloc(size);
  data_ = reinterpret_cast<uint64_t>(expiry_pos);
  if (expiry_size) {
    SetExpiryBit(true);
    std::memcpy(expiry_pos, &expiry, sizeof(expiry));
  }

  auto* key_size_pos = expiry_pos + expiry_size;
  if (key_len_field_size == 1) {
    SetSsoBit();
    uint8_t sso_key_size = key_size;
    std::memcpy(key_size_pos, &sso_key_size, key_len_field_size);
  } else {
    std::memcpy(key_size_pos, &key_size, key_len_field_size);
  }

  auto* key_pos = key_size_pos + key_len_field_size;
  std::memcpy(key_pos, key.data(), key_size);
}

// returns the expiry time of the current entry or UINT32_MAX if no expiry is set.
uint32_t OAHEntry::GetExpiry() const {
  std::uint32_t res = UINT32_MAX;
  if (HasExpiry()) {
    assert(!IsVector());
    std::memcpy(&res, Raw(), sizeof(res));
  }
  return res;
}

bool OAHEntry::CheckNoCollisions(const uint64_t ext_hash) {
  auto stored_hash = GetHash();
  return ((stored_hash != ext_hash) & (stored_hash != 0)) | (Empty());
}

void OAHEntry::SetExtHash(uint64_t ext_hash) {
  assert(data_);
  assert(!IsVector());
  data_ = (data_ & ~kExtHashShiftedMask) | (ext_hash << kExtHashShift);
}

void OAHEntry::SetExpiry(uint32_t at_sec) {
  assert(!IsVector());
  if (HasExpiry()) {
    auto* expiry_pos = Raw();
    std::memcpy(expiry_pos, &at_sec, sizeof(at_sec));
  } else {
    *this = OAHEntry(Key(), at_sec);
  }
}

void OAHEntry::ExpireIfNeeded(uint32_t time_now, uint32_t* set_size, size_t* alloc_used) {
  assert(!IsVector());
  if (GetExpiry() <= time_now) {
    *alloc_used -= AllocSize();
    Clear();
    --*set_size;
  }
}

ssize_t OAHEntry::ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
  *realloced = false;
  if (!data_)
    return 0;

  ssize_t obj_alloc_delta = 0;

  if (IsVector()) {
    // Recurse into each inner entry first — every element's own buffer must be checked.
    auto& vec = AsVector();
    for (size_t i = 0, n = vec.Size(); i < n; ++i) {
      if (vec[i]) {
        bool inner_moved = false;
        obj_alloc_delta += vec[i].ReallocIfNeeded(page_usage, &inner_moved);
        if (inner_moved)
          *realloced = true;
      }
    }
    // Then defrag the vector's array buffer if its page is underutilized. ResizeLog
    // with the same log_size allocates a fresh buffer, move-constructs each element
    // (OAHEntry's move-ctor swaps data_, leaving sources empty), and frees the old
    // buffer via Clear(). The vector's logical AllocSize is unchanged, so no delta
    // is reported for ptr_vectors_alloc_used_.
    if (page_usage->IsPageForObjectUnderUtilized(Raw())) {
      vec.ResizeLog(vec.LogSize());
      *realloced = true;
    }
    return obj_alloc_delta;
  }

  if (!page_usage->IsPageForObjectUnderUtilized(Raw()))
    return 0;

  // Single-entry realloc via ctor + move-assignment: build a fresh OAHEntry from this
  // one's key/expiry/ext_hash; move-assign to swap data_, then the temporary's
  // destructor frees the old buffer.
  const size_t old_alloc = AllocSize();
  const uint64_t saved_hash = GetHash();
  const uint32_t expiry = HasExpiry() ? GetExpiry() : UINT32_MAX;
  {
    OAHEntry replacement(Key(), expiry);
    if (saved_hash)
      replacement.SetExtHash(saved_hash);
    *this = std::move(replacement);
  }
  *realloced = true;
  return static_cast<ssize_t>(AllocSize()) - static_cast<ssize_t>(old_alloc);
}

// TODO refactor, because it's inefficient
size_t OAHEntry::Insert(OAHEntry&& e) {
  if (Empty()) {
    *this = std::move(e);
    return 0;
  } else if (!IsVector()) {
    OAHEntry tmp(PtrVector<OAHEntry>::FromLogSize(1));
    auto& arr = tmp.AsVector();
    arr[0] = std::move(*this);
    arr[1] = std::move(e);
    auto res = arr.AllocSize();
    *this = std::move(tmp);
    return res;
  } else {
    auto& arr = AsVector();
    size_t i = 0;
    for (; i < arr.Size(); ++i) {
      if (!arr[i]) {
        arr[i] = std::move(e);
        return 0;
      }
    }
    size_t prev_alloc_size = arr.AllocSize();
    auto new_pos = arr.Size();
    arr.ResizeLog(arr.LogSize() + 1);
    arr[new_pos] = (std::move(e));
    return arr.AllocSize() - prev_alloc_size;
  }
}

uint32_t OAHEntry::ElementsNum() {
  if (Empty()) {
    return 0;
  } else if (!IsVector()) {
    return 1;
  }
  return AsVector().Size();
}

// TODO remove, it is inefficient
OAHEntry& OAHEntry::operator[](uint32_t pos) {
  assert(!Empty());
  if (!IsVector()) {
    assert(pos == 0);
    return *this;
  } else {
    auto& arr = AsVector();
    assert(pos < arr.Size());
    return arr[pos];
  }
}

OAHEntry OAHEntry::Remove(uint32_t pos) {
  if (Empty()) {
    // I'm not sure that this scenario should be check at all
    assert(pos == 0);
    return OAHEntry();
  } else if (!IsVector()) {
    assert(pos == 0);
    return std::move(*this);
  } else {
    auto& arr = AsVector();
    assert(pos < arr.Size());
    return std::move(arr[pos]);
  }
}

OAHEntry OAHEntry::Pop() {
  if (IsVector()) {
    auto& arr = AsVector();
    for (auto& e : arr) {
      if (e)
        return std::move(e);
    }
    return {};
  }
  return std::move(*this);
}

void OAHEntry::Clear() {
  // TODO add optimization to avoid destructor calls during vector allocator
  if (!data_)
    return;

  if (IsVector()) {
    AsVector().~PtrVector<OAHEntry>();
  } else {
    zfree(Raw());
  }
  data_ = 0;
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
    data_ |= kExpiryBit;
  else
    data_ &= ~kExpiryBit;
}

size_t OAHEntry::Size() {
  size_t key_field_size = HasSso() ? 1 : 4;
  size_t expiry_field_size = HasExpiry() ? 4 : 0;
  return expiry_field_size + key_field_size + GetKeySize();
}

}  // namespace dfly
