// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_blob.h"

#include <mimalloc.h>

#include "base/logging.h"
#include "core/detail/stateless_allocator.h"

namespace dfly::detail {

InternedBlobHandle InternedBlobHandle::Create(std::string_view sv) {
  if (sv.empty()) {
    return InternedBlobHandle{nullptr};
  }

  constexpr uint32_t ref_count = 1;
  DCHECK_LE(sv.size(), std::numeric_limits<uint32_t>::max());

  const uint32_t str_len = sv.size();

  // We need +1 byte for \0 because jsoncons expects c_str() and data() style accessors on keys
  BlobPtr blob = StatelessAllocator<char>{}.allocate(kHeaderSize + str_len + 1);

  std::memcpy(blob, &str_len, sizeof(uint32_t));
  std::memcpy(blob + sizeof(uint32_t), &ref_count, sizeof(uint32_t));

  std::memcpy(blob + kHeaderSize, sv.data(), str_len);

  // null terminate so jsoncons can directly access the char* as string
  blob[kHeaderSize + str_len] = '\0';
  return InternedBlobHandle{blob + kHeaderSize};
}

size_t InternedBlobHandle::MemUsed() const {
  return blob_ ? mi_usable_size(blob_ - kHeaderSize) : 0;
}

void InternedBlobHandle::Destroy(InternedBlobHandle& handle) {
  if (handle.blob_) {
    const size_t to_destroy = kHeaderSize + handle.Size() + 1;
    StatelessAllocator<char>{}.deallocate(handle.blob_ - kHeaderSize, to_destroy);
    handle.blob_ = nullptr;
  }
}

}  // namespace dfly::detail
