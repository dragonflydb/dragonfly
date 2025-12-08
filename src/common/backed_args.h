// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <string_view>

namespace cmn {

class BackedArguments {
  constexpr static size_t kLenCap = 5;
  constexpr static size_t kStorageCap = 88;

 public:
  BackedArguments() {
  }

  class iterator {
   public:
    iterator(const BackedArguments* ba, size_t index) : ba_(ba), index_(index) {
    }

    iterator& operator++() {
      offset_ += ba_->elem_capacity(index_);
      ++index_;
      return *this;
    }

    bool operator==(const iterator& other) const {
      return index_ == other.index_;
    }

    bool operator!=(const iterator& other) const {
      return !(*this == other);
    }

    std::string_view operator*() const {
      return ba_->at(index_, offset_);
    }

   private:
    const BackedArguments* ba_;
    size_t index_;
    size_t offset_ = 0;
  };

  // Construct the arguments from iterator range.
  // TODO: In general we could get away without the len argument,
  // but that would require fixing base::it::CompoundIterator to support subtraction.
  // Similarly, I wish that CompoundIterator supported the -> operator.
  template <typename I> BackedArguments(I begin, I end, size_t len) {
    Assign(begin, end, len);
  }

  template <typename I> void Assign(I begin, I end, size_t len);

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

  bool empty() const {
    return lens_.empty();
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

  iterator begin() const {
    return {this, 0};
  }

  iterator end() const {
    return {this, lens_.size()};
  }

  void clear() {
    lens_.clear();
    storage_.clear();
  }

 protected:
  absl::InlinedVector<uint32_t, kLenCap> lens_;
  StorageType storage_;
};

static_assert(sizeof(BackedArguments) == 128);

template <typename I> void BackedArguments::Assign(I begin, I end, size_t len) {
  lens_.resize(len);
  size_t total_size = 0;
  unsigned idx = 0;
  for (auto it = begin; it != end; ++it) {
    size_t sz = (*it).size();
    lens_[idx++] = sz;
    total_size += sz + 1;  // +1 for '\0'
  }
  storage_.resize(total_size);

  // Reclaim memory if we have too much allocated.
  if (storage_.capacity() > kStorageCap && total_size < storage_.capacity() / 2)
    storage_.shrink_to_fit();

  char* next = storage_.data();
  for (auto it = begin; it != end; ++it) {
    size_t sz = (*it).size();
    if (sz > 0) {
      memcpy(next, (*it).data(), sz);
    }
    next[sz] = '\0';
    next += sz + 1;
  }
}

}  // namespace cmn
