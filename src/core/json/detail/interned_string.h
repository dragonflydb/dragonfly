// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "core/detail/stateless_allocator.h"
#include "core/json/detail/interned_blob.h"

namespace dfly::detail {

// InternedString handles incrementing and decrementing reference counts of the blobs tied to its
// own lifecycle. It deletes the blob from a shard local pool when refcount is 0.
// TODO examine cross shard json object interactions. Can a pool end up access from another shard?
class InternedString {
 public:
  using allocator_type = StatelessAllocator<char>;

  InternedString() = default;

  explicit InternedString(const std::string_view sv) : entry_(Intern(sv)) {
  }

  // The following constructors and members are added because they are required by jsoncons for
  // keys. Each of these is added in response to compiler errors and should not be removed, even if
  // they are seemingly a no-op or duplicated.

  // jsoncons sometimes creates empty obj with custom allocator. If it creates an object with any
  // other allocator, we should fail during compilation.
  template <typename T> explicit InternedString(StatelessAllocator<T> /*unused*/) {
  }

  template <typename Alloc> InternedString(const char* data, size_t size, Alloc alloc);

  template <std::contiguous_iterator It> InternedString(It begin, It end);

  InternedString(const InternedString& other) : entry_{other.entry_} {
    Acquire();
  }

  InternedString(InternedString&& other) noexcept : entry_{other.entry_} {
    other.entry_ = {};
  }

  InternedString& operator=(InternedString other);

  ~InternedString() {
    Release();
  }

  operator std::string_view() const {
    return entry_;
  }

  const char* data() const {
    return entry_ ? entry_.Data() : "";
  }

  const char* c_str() const {
    return data();
  }

  void swap(InternedString& other) noexcept {
    std::swap(entry_, other.entry_);
  }

  size_t length() const {
    return size();
  }

  size_t size() const {
    return entry_.Size();
  }

  int compare(const InternedString& other) const {
    return std::string_view{*this}.compare(other);
  }

  int compare(std::string_view other) const {
    return std::string_view{*this}.compare(other);
  }

  // lex. comparison
  auto operator<=>(const InternedString& other) const {
    return std::string_view{*this} <=> std::string_view{other};
  }

  bool operator==(const InternedString& other) const = default;

  void shrink_to_fit() {  // NOLINT (must be non-const to align with jsoncons usage)
  }

  // Destroys all strings in the pool. Must be called on process shutdown before the backing memory
  // resource is destroyed.
  static void ResetPool();
  static InternedBlobPool& GetPoolRef();

  size_t MemUsed() const {
    return entry_.MemUsed();
  }

 private:
  // If a string exists in the pool, increments its refcount. If not, adds the string to the pool.
  // Returns a handle wrapping the string.
  static InternedBlobHandle Intern(std::string_view sv);

  // Increments the refcount if the entry is not null
  void Acquire();

  // Decrements the refcount, removes entry from the pool if necessary, destroying the interned
  // blob. A side effect may be shrinking the pool if the load factor is suboptimal (see
  // kLoadFactorToShrinkPool in the implementation)
  void Release();

  // Wraps a null pointer by default
  InternedBlobHandle entry_;
};

template <typename Alloc>
InternedString::InternedString(const char* data, size_t size, Alloc /*unused*/)
    : InternedString(std::string_view{data, size}) {
}

template <std::contiguous_iterator It> InternedString::InternedString(It begin, It end) {
  if (begin == end) {
    return;
  }

  const auto size = std::distance(begin, end);
  const auto data_ptr = &*begin;
  entry_ = Intern(std::string_view(data_ptr, size));
}

}  // namespace dfly::detail

namespace dfly {

struct InternedStringStats {
  size_t pool_entries = 0;
  size_t pool_bytes = 0;
  size_t hits = 0;
  size_t misses = 0;
  size_t pool_table_bytes = 0;
  size_t live_references = 0;

  InternedStringStats& operator+=(const InternedStringStats& other);
};

InternedStringStats GetInternedStringStats();

}  // namespace dfly
