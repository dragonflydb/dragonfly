// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "core/detail/stateless_allocator.h"
#include "core/json/detail/interned_blob.h"

namespace dfly::detail {

// The interned string has access to a thread local pool of InternedBlob pointers.
// InternedString handles incrementing and decrementing reference counts of the blobs tied to its
// own lifecycle. It deletes the blob from the pool when refcount is 0.
// TODO examine cross shard json object interactions. Can a pool end up access from another shard?
class InternedString {
 public:
  using allocator_type = StatelessAllocator<char>;

  InternedString();

  explicit InternedString(std::string_view);

  // The following constructors and members are added because they are required by jsoncons for
  // keys. Each of these is added in response to compiler errors and should not be removed, even if
  // they are seemingly a no-op or duplicated.

  // jsoncons sometimes creates empty obj with custom allocator. If it creates object with any other
  // allocator, we should fail during compilation.
  template <typename T> explicit InternedString(StatelessAllocator<T> /*unused*/) {
  }

  template <typename Alloc> InternedString(const char* data, size_t size, Alloc alloc);

  template <typename It> InternedString(It begin, It end);

  InternedString(const InternedString& other);
  InternedString(InternedString&& other) noexcept;
  InternedString& operator=(const InternedString& other);
  InternedString& operator=(InternedString&& other) noexcept;

  ~InternedString();

  operator std::string_view() const;

  const char* data() const;

  const char* c_str() const;

  void swap(InternedString&) noexcept;

  size_t length() const;

  size_t size() const;

  int compare(const InternedString& other) const;
  int compare(std::string_view) const;

  bool operator==(const InternedString& other) const;
  bool operator!=(const InternedString& other) const;
  bool operator<(const InternedString& other) const;

  void shrink_to_fit();

  // For tests
  static void ResetPool();
  static InternedBlobPool& GetPoolRef();

  size_t MemUsed() const;

 private:
  // If a string exists in pool, increments its refcount and returns a pointer to it. If not, adds
  // the string to the pool.
  static InternedBlob* Intern(std::string_view sv);

  // Increments the refcount if the entry is not null
  void Acquire();

  // Decrements the refcount, removes entry from pool if necessary, destroying the interned blob
  void Release();

  InternedBlob* entry_ = nullptr;
};

template <typename Alloc>
InternedString::InternedString(const char* data, size_t size, Alloc /*unused*/)
    : InternedString(std::string_view{data, size}) {
}

template <typename It> InternedString::InternedString(It begin, It end) {
  if (begin == end)
    return;

  const auto size = std::distance(begin, end);
  const auto data_ptr = &*begin;
  entry_ = Intern(std::string_view(data_ptr, size));
}

}  // namespace dfly::detail
