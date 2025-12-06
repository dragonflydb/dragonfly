// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <string_view>

namespace cmn {

class BackedArguments {
  constexpr static size_t kLenCap = 5;
#ifdef ABSL_HAVE_ADDRESS_SANITIZER
  constexpr static size_t kStorageCap = 88;
#else
  constexpr static size_t kStorageCap = 88;
#endif
 public:
  BackedArguments() {
  }

  template <typename I> BackedArguments(I begin, I end);

  size_t HeapMemory() const {
    size_t s1 = lens_.capacity() <= kLenCap ? 0 : lens_.capacity() * sizeof(uint32_t);
    size_t s2 = storage_.capacity() <= kStorageCap ? 0 : storage_.capacity();
    return s1 + s2;
  }

  // The capacity is chosen so that we allocate a fully utilized (128 bytes) block.
  using StorageType = absl::InlinedVector<char, kStorageCap>;

  std::string_view Front() const {
    return std::string_view{storage_.data(), lens_[0]};
  }

  size_t size() const {
    return lens_.size();
  }

  size_t elem_len(size_t i) const {
    return lens_[i];
  }

  size_t elem_capacity(size_t i) const {
    return elem_len(i) + 1;
  }

  std::string_view at(uint32_t index, uint32_t offset) const {
    const char* ptr = storage_.data() + offset;
    return std::string_view{ptr, lens_[index]};
  }

 protected:
  absl::InlinedVector<uint32_t, kLenCap> lens_;
  StorageType storage_;
};

static_assert(sizeof(BackedArguments) == 128);

template <typename I> BackedArguments::BackedArguments(I begin, I end) {
  lens_.reserve(end - begin);
  size_t total_size = 0;
  for (auto it = begin; it != end; ++it) {
    lens_.push_back(it->size());
    total_size += it->size() + 1;  // +1 for '\0'
  }
  storage_.resize(total_size);

  char* next = storage_.data();
  for (auto it = begin; it != end; ++it) {
    if (!it->empty()) {
      memcpy(next, it->data(), it->size());
    }
    next[it->size()] = '\0';
    next += it->size() + 1;
  }
}

}  // namespace cmn
