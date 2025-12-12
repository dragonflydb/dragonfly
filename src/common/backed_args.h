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
    using iterator_category = std::random_access_iterator_tag;
    using value_type = std::string_view;
    using difference_type = std::ptrdiff_t;
    using pointer = const std::string_view*;
    using reference = std::string_view;

    iterator(const BackedArguments* ba, size_t index) : ba_(ba), index_(index) {
    }

    iterator& operator++() {
      ++index_;
      return *this;
    }

    iterator& operator--() {
      --index_;
      return *this;
    }

    iterator& operator+=(int delta) {
      index_ += delta;
      return *this;
    }

    iterator operator+(int delta) const {
      iterator res(*this);
      res += delta;
      return res;
    }

    ptrdiff_t operator-(iterator other) const {
      return ptrdiff_t(index_) - ptrdiff_t(other.index_);
    }

    bool operator==(const iterator& other) const {
      return index_ == other.index_ && ba_ == other.ba_;
    }

    bool operator!=(const iterator& other) const {
      return !(*this == other);
    }

    std::string_view operator*() const {
      return ba_->at(index_);
    }

   private:
    const BackedArguments* ba_;
    size_t index_;
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
    size_t s1 = offsets_.capacity() <= kLenCap ? 0 : offsets_.capacity() * sizeof(uint32_t);
    size_t s2 = storage_.capacity() <= kStorageCap ? 0 : storage_.capacity();
    return s1 + s2;
  }

  // The capacity is chosen so that we allocate a fully utilized (128 bytes) block.
  using StorageType = absl::InlinedVector<char, kStorageCap>;

  std::string_view Front() const {
    return std::string_view{storage_.data(), elem_len(0)};
  }

  size_t size() const {
    return offsets_.size();
  }

  bool empty() const {
    return offsets_.empty();
  }

  size_t elem_len(size_t i) const {
    return elem_capacity(i) - 1;
  }

  size_t elem_capacity(size_t i) const {
    uint32_t next_offs = i + 1 >= offsets_.size() ? storage_.size() : offsets_[i + 1];
    return next_offs - offsets_[i];
  }

  std::string_view at(uint32_t index) const {
    uint32_t offset = offsets_[index];
    return std::string_view{storage_.data() + offset, elem_len(index)};
  }

  std::string_view operator[](uint32_t index) const {
    return at(index);
  }

  iterator begin() const {
    return {this, 0};
  }

  iterator end() const {
    return {this, offsets_.size()};
  }

  void clear() {
    offsets_.clear();
    storage_.clear();
  }

  // Reserves space for additional argument of given length at the end.
  void PushArg(size_t len) {
    size_t old_size = storage_.size();
    offsets_.push_back(old_size);
    storage_.resize(old_size + len + 1);
  }

 protected:
  absl::InlinedVector<uint32_t, kLenCap> offsets_;
  StorageType storage_;
};

static_assert(sizeof(BackedArguments) == 128);

template <typename I> void BackedArguments::Assign(I begin, I end, size_t len) {
  offsets_.resize(len);
  size_t total_size = 0;
  unsigned idx = 0;
  for (auto it = begin; it != end; ++it) {
    offsets_[idx++] = total_size;
    total_size += (*it).size() + 1;  // +1 for '\0'
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
