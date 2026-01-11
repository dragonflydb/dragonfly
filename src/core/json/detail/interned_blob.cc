// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_blob.h"

#include <cstring>

#include "core/detail/stateless_allocator.h"

namespace dfly::detail {
InternedBlob::InternedBlob(const std::string_view sv) {
  auto alloc = StatelessAllocator<char>{};

  constexpr uint32_t ref_count = 1;
  const uint32_t str_len = sv.size();

  // We need \0 because jsoncons expects c_str() and data() style accessors on keys
  blob_ = alloc.allocate(kHeaderSize + str_len + 1);

  std::memcpy(blob_, &str_len, sizeof(str_len));
  std::memcpy(blob_ + sizeof(str_len), &ref_count, sizeof(ref_count));

  std::memcpy(blob_ + kHeaderSize, sv.data(), str_len);

  // null terminate so jsoncons can directly access the char* as string
  blob_[kHeaderSize + str_len] = '\0';
}

InternedBlob::~InternedBlob() {
  if (blob_) {
    const size_t to_destroy = kHeaderSize + Size();
    StatelessAllocator<char>{}.deallocate(blob_, to_destroy);
    blob_ = nullptr;
  }
}

uint32_t InternedBlob::Size() const {
  // TODO - should this check be removed to uncover bugs? or an assert here?
  if (!blob_)
    return 0;

  uint32_t size;
  std::memcpy(&size, blob_, sizeof(size));
  return size;
}

uint32_t InternedBlob::RefCount() const {
  // TODO - should this check be removed to uncover bugs? or an assert here?
  if (!blob_)
    return 0;

  uint32_t ref_count;
  // Assumes size and refcount are both 4 bytes
  std::memcpy(&ref_count, blob_ + sizeof(ref_count), sizeof(ref_count));
  return ref_count;
}

std::string_view InternedBlob::View() const {
  return {blob_ + kHeaderSize, Size()};
}

const char* InternedBlob::Data() const {
  return blob_ ? blob_ + kHeaderSize : nullptr;
}

void InternedBlob::IncrRefCount() const {
  const uint32_t updated_count = RefCount() + 1;
  std::memcpy(blob_ + sizeof(updated_count), &updated_count, sizeof(updated_count));
}

void InternedBlob::DecrRefCount() const {
  // Caller must ensure refcount does not go below 0
  const uint32_t updated_count = RefCount() - 1;
  std::memcpy(blob_ + sizeof(updated_count), &updated_count, sizeof(updated_count));
}

size_t BlobHash::operator()(const InternedBlob& b) const {
  return std::hash<std::string_view>{}(b.View());
}

size_t BlobHash::operator()(const std::string_view& sv) const {
  return std::hash<std::string_view>{}(sv);
}

bool BlobEq::operator()(const InternedBlob& a, const InternedBlob& b) const {
  return a.View() == b.View();
}

bool BlobEq::operator()(const InternedBlob& a, std::string_view b) const {
  return a.View() == b;
}

bool BlobEq::operator()(std::string_view a, const InternedBlob& b) const {
  return a == b.View();
}

}  // namespace dfly::detail
