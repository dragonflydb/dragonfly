// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <cstdint>
#include <string_view>

namespace cmn {

class BackedArguments {
  constexpr static size_t kLenCap = 5;
  constexpr static size_t kStorageCap = 88;

 public:
  using value_type = std::string_view;

  BackedArguments() {
  }

  // Construct the arguments from iterator range.
  // TODO: In general we could get away without the len argument,
  // but that would require fixing base::it::CompoundIterator to support subtraction.
  // Similarly, I wish that CompoundIterator supported the -> operator.
  template <typename I> BackedArguments(I begin, I end, size_t len) {
    Assign(begin, end, len);
  }

  template <typename I> void Assign(I begin, I end, size_t len);

  void Reserve(size_t arg_cnt, size_t total_size) {
    offsets_.reserve(arg_cnt);
    storage_.reserve(total_size);
  }

  size_t HeapMemory() const {
    size_t s1 = offsets_.capacity() <= kLenCap ? 0 : offsets_.capacity() * sizeof(uint32_t);
    size_t s2 = storage_.capacity() <= kStorageCap ? 0 : storage_.capacity();
    return s1 + s2;
  }

  void SwapArgs(cmn::BackedArguments& other) {
    offsets_.swap(other.offsets_);
    storage_.swap(other.storage_);
  }

  // The capacity is chosen so that we allocate a fully utilized (128 bytes) block.
  using StorageType = absl::InlinedVector<char, kStorageCap>;

  size_t size() const {
    return offsets_.size();
  }

  bool empty() const {
    return offsets_.empty();
  }

  size_t elem_len(size_t i) const {
    return elem_capacity(i) - 1;
  }

  std::string_view at(uint32_t index) const {
    uint32_t offset = offsets_[index];
    return std::string_view{storage_.data() + offset, elem_len(index)};
  }

  char* data(uint32_t index) {
    uint32_t offset = offsets_[index];
    return storage_.data() + offset;
  }

  std::string_view operator[](uint32_t index) const {
    return at(index);
  }

  auto view() const {
    return std::views::iota(size_t{0}, size()) |
           std::views::transform([this](size_t i) { return at(i); });
  }

  auto begin() const {
    return view().begin();
  }

  auto end() const {
    return view().end();
  }

  void clear() {
    // Clear the contents without deallocating memory. clear() deallocates inlined_vector.
    offsets_.resize(0);
    storage_.resize(0);
  }

  std::string_view back() const {
    assert(size() > 0);
    return at(size() - 1);
  }

  std::string_view front() const {
    assert(size() > 0);
    return at(0);
  }

  // Reserves space for additional argument of given length at the end.
  void PushArg(size_t len) {
    size_t old_size = storage_.size();
    offsets_.push_back(old_size);
    storage_.resize(old_size + len + 1);
  }

  void PushArg(std::string_view arg) {
    PushArg(arg.size());
    char* dest = storage_.data() + offsets_.back();
    if (arg.size() > 0)
      memcpy(dest, arg.data(), arg.size());
    dest[arg.size()] = '\0';
  }

  void PopArg() {
    uint32_t last_offs = offsets_.back();
    offsets_.pop_back();
    storage_.resize(last_offs);
  }

 protected:
  size_t elem_capacity(size_t i) const {
    uint32_t next_offs = i + 1 >= offsets_.size() ? storage_.size() : offsets_[i + 1];
    return next_offs - offsets_[i];
  }

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
