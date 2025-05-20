// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <memory_resource>
#include <type_traits>

#include "base/logging.h"

namespace dfly::detail {

/* Analogous to absl::FixedArray but uses a memory resource for allocation. */
template <typename T> class PmrFixedArray {
 private:
  static_assert(std::is_default_constructible_v<T>,
                "PmrFixedArray<T> requires default-constructible T");

  static_assert(std::is_nothrow_destructible_v<T>,
                "PmrFixedArray<T> requires nothrow-destructible T");

  using Allocator = std::pmr::polymorphic_allocator<T>;

 public:
  PmrFixedArray(size_t size, std::pmr::memory_resource* mr);

  PmrFixedArray(const PmrFixedArray&) = delete;
  PmrFixedArray& operator=(const PmrFixedArray&) = delete;

  PmrFixedArray(PmrFixedArray&& other) noexcept;
  PmrFixedArray& operator=(PmrFixedArray&&) = delete;

  ~PmrFixedArray() noexcept;

  T& operator[](size_t i);
  const T& operator[](size_t i) const;

  size_t size() const;

 private:
  void Reset();

 private:
  size_t size_ = 0;
  T* data_ = nullptr;
  Allocator alloc_;
};

// Implementation
/******************************************************************/
template <typename T>
PmrFixedArray<T>::PmrFixedArray(size_t size, std::pmr::memory_resource* mr)
    : size_(size), alloc_(mr) {
  DCHECK(mr);
  if (!size_) {
    return;
  }

  data_ = alloc_.allocate(size_);

  // Construct elements one by one, with rollback on exception
  size_t constructed = 0;
  try {
    for (; constructed < size_; ++constructed) {
      ::new (static_cast<void*>(data_ + constructed)) T();
    }
  } catch (...) {
    // Destroy all already-constructed objects
    std::destroy_n(data_, constructed);
    alloc_.deallocate(data_, size_);
    Reset();
    throw;
  }
}

template <typename T>
PmrFixedArray<T>::PmrFixedArray(PmrFixedArray&& other) noexcept
    : size_(other.size_), data_(other.data_), alloc_(std::move(other.alloc_)) {
  other.Reset();
}

template <typename T> PmrFixedArray<T>::~PmrFixedArray() noexcept {
  DCHECK((size_ > 0 && data_) || (size_ == 0 && !data_));
  if (!data_) {
    return;
  }

  // Deallocate memory (should not throw)
  std::destroy_n(data_, size_);
  alloc_.deallocate(data_, size_);
  Reset();
}

template <typename T> T& PmrFixedArray<T>::operator[](size_t i) {
  CHECK(i < size_);
  return data_[i];
}

template <typename T> const T& PmrFixedArray<T>::operator[](size_t i) const {
  CHECK(i < size_);
  return data_[i];
}

template <typename T> size_t PmrFixedArray<T>::size() const {
  return size_;
}

template <typename T> void PmrFixedArray<T>::Reset() {
  size_ = 0;
  data_ = nullptr;
}

}  // namespace dfly::detail
