// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_blob.h"

#include <glog/logging.h>

#include "core/detail/stateless_allocator.h"

namespace {
constexpr size_t kUint32Size = sizeof(uint32_t);
constexpr size_t kHeaderSize = sizeof(uint32_t) * 2;
}  // namespace

namespace dfly::detail {

InternedBlobHandle InternedBlobHandle::Create(std::string_view sv) {
  constexpr uint32_t ref_count = 1;
  DCHECK_LE(sv.size(), std::numeric_limits<uint32_t>::max());

  const uint32_t str_len = sv.size();

  // We need +1 byte for \0 because jsoncons expects c_str() and data() style accessors on keys
  BlobPtr blob = StatelessAllocator<char>{}.allocate(kHeaderSize + str_len + 1);

  std::memcpy(blob, &str_len, kUint32Size);
  std::memcpy(blob + kUint32Size, &ref_count, kUint32Size);

  std::memcpy(blob + kHeaderSize, sv.data(), str_len);

  // null terminate so jsoncons can directly access the char* as string
  blob[kHeaderSize + str_len] = '\0';
  return InternedBlobHandle{blob + kHeaderSize};
}

uint32_t InternedBlobHandle::Size() const {
  DCHECK(blob_) << "Called Size() on empty blob";
  uint32_t size;
  std::memcpy(&size, blob_ - kHeaderSize, kUint32Size);
  return size;
}

uint32_t InternedBlobHandle::RefCount() const {
  DCHECK(blob_) << "Called RefCount() on empty blob";
  uint32_t ref_count;
  std::memcpy(&ref_count, blob_ - kUint32Size, kUint32Size);
  return ref_count;
}

void InternedBlobHandle::IncrRefCount() {  // NOLINT - non-const, mutates via ptr
  const uint32_t ref_count = RefCount();
  DCHECK_LT(ref_count, std::numeric_limits<uint32_t>::max()) << "Attempt to increase max refcount";
  const uint32_t updated_count = ref_count + 1;
  std::memcpy(blob_ - kUint32Size, &updated_count, kUint32Size);
}

void InternedBlobHandle::DecrRefCount() {  // NOLINT - non-const, mutates via ptr
  const uint32_t ref_count = RefCount();
  DCHECK_GE(ref_count, 1ul) << "Attempt to decrease zero refcount";
  const uint32_t updated_count = ref_count - 1;
  std::memcpy(blob_ - kUint32Size, &updated_count, kUint32Size);
}

void InternedBlobHandle::SetRefCount(uint32_t ref_count) {  // NOLINT - non-const, mutates via ptr
  std::memcpy(blob_ - kUint32Size, &ref_count, kUint32Size);
}

size_t InternedBlobHandle::MemUsed() const {
  return blob_ ? Size() + kHeaderSize + 1 : 0;
}

void InternedBlobHandle::Destroy(InternedBlobHandle& handle) {
  if (handle.blob_) {
    const size_t to_destroy = kHeaderSize + handle.Size() + 1;
    StatelessAllocator<char>{}.deallocate(handle.blob_ - kHeaderSize, to_destroy);
    handle.blob_ = nullptr;
  }
}

InternedBlobHandle::operator std::string_view() const {
  DCHECK(blob_) << "Attempt to convert empty blob to string_view";
  return {blob_, Size()};
}

}  // namespace dfly::detail
