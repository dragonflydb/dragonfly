// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_blob.h"

#include <glog/logging.h>

#include "core/detail/stateless_allocator.h"

namespace {
constexpr size_t kUint32Size = sizeof(uint32_t);
}

namespace dfly::detail {

InternedBlob::InternedBlob(const std::string_view sv) {
  auto alloc = StatelessAllocator<char>{};

  constexpr uint32_t ref_count = 1;
  DCHECK_LE(sv.size(), std::numeric_limits<uint32_t>::max());

  const uint32_t str_len = sv.size();

  // We need +1 byte for \0 because jsoncons expects c_str() and data() style accessors on keys
  blob_ = alloc.allocate(kHeaderSize + str_len + 1);

  std::memcpy(blob_, &str_len, kUint32Size);
  std::memcpy(blob_ + kUint32Size, &ref_count, kUint32Size);

  std::memcpy(blob_ + kHeaderSize, sv.data(), str_len);

  // null terminate so jsoncons can directly access the char* as string
  blob_[kHeaderSize + str_len] = '\0';
  blob_ += kHeaderSize;
}

InternedBlob::~InternedBlob() {
  Destroy();
}

InternedBlob::InternedBlob(InternedBlob&& other) noexcept : blob_(other.blob_) {
  other.blob_ = nullptr;
}

InternedBlob& InternedBlob::operator=(InternedBlob&& other) noexcept {
  if (this != &other) {
    Destroy();
    blob_ = other.blob_;
    other.blob_ = nullptr;
  }
  return *this;
}

uint32_t InternedBlob::Size() const {
  DCHECK(blob_) << "Called Size() on empty blob";
  uint32_t size;
  std::memcpy(&size, blob_ - kHeaderSize, kUint32Size);
  return size;
}

uint32_t InternedBlob::RefCount() const {
  DCHECK(blob_) << "Called RefCount() on empty blob";
  uint32_t ref_count;
  // Assumes size and refcount are both 4 bytes
  std::memcpy(&ref_count, blob_ - kUint32Size, kUint32Size);
  return ref_count;
}

std::string_view InternedBlob::View() const {
  DCHECK(blob_) << "Called View() on empty blob";
  return {blob_, Size()};
}

const char* InternedBlob::Data() const {
  return blob_;
}

void InternedBlob::IncrRefCount() {  // NOLINT - non-const because mutates state via ptr
  const uint32_t ref_count = RefCount();
  DCHECK_LT(ref_count, std::numeric_limits<uint32_t>::max()) << "Attempt to increase max refcount";
  const uint32_t updated_count = ref_count + 1;
  std::memcpy(blob_ - kUint32Size, &updated_count, kUint32Size);
}

void InternedBlob::DecrRefCount() {  // NOLINT - non-const because mutates state via ptr
  const uint32_t ref_count = RefCount();
  DCHECK_GE(ref_count, 1ul) << "Attempt to decrease zero refcount";
  const uint32_t updated_count = ref_count - 1;
  std::memcpy(blob_ - kUint32Size, &updated_count, kUint32Size);
}

void InternedBlob::SetRefCount(uint32_t ref_count) {  // NOLINT - non-const because mutates ptr
  std::memcpy(blob_ - kUint32Size, &ref_count, kUint32Size);
}

size_t InternedBlob::MemUsed() const {
  return blob_ ? Size() + kHeaderSize + 1 : 0;
}

void InternedBlob::Destroy() {
  if (blob_) {
    const size_t to_destroy = kHeaderSize + Size() + 1;
    StatelessAllocator<char>{}.deallocate(blob_ - kHeaderSize, to_destroy);
    blob_ = nullptr;
  }
}

size_t BlobHash::operator()(const InternedBlob* b) const {
  return std::hash<std::string_view>{}(b->View());
}

size_t BlobHash::operator()(std::string_view sv) const {
  return std::hash<std::string_view>{}(sv);
}

bool BlobEq::operator()(const InternedBlob* a, const InternedBlob* b) const {
  return a->View() == b->View();
}

bool BlobEq::operator()(const InternedBlob* a, std::string_view b) const {
  return a->View() == b;
}

bool BlobEq::operator()(std::string_view a, const InternedBlob* b) const {
  return a == b->View();
}

}  // namespace dfly::detail
