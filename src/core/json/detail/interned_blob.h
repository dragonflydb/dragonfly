// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/container/flat_hash_set.h>

#include <string_view>

namespace dfly::detail {

// Layout is: 4 bytes size, 4 bytes refcount, char data, followed by nul-char.
// The trailing nul-char is required because jsoncons needs to access c_str/data without a
// size. The blob_ itself points directly to the data, so that callers do not have to perform
// pointer arithmetic for c_str() and data() calls:
//     [size:4] [refcount:4] [string] [\0]
//     ^-8      ^- 4         ^blob_
using BlobPtr = char*;

// A lightweight handle around a blob pointer, used to wrap the blob data when storing it in hashset
// and also within interned strings. Does not handle lifetime of the data. Only provides convenience
// methods to change state inside the blob and "view" style methods to access the string inside the
// blob. Multiple handles can point to the same blob.
class InternedBlobHandle {
 public:
  InternedBlobHandle() = default;

  [[nodiscard]] static InternedBlobHandle Create(std::string_view sv);

  uint32_t Size() const;

  uint32_t RefCount() const;

  const char* Data() const {
    return blob_;
  }

  // The refcount methods are explicitly part of the public API and not tied to the handle lifetime
  // to keep control over exactly when we modify data in the blob ptr. We do not want to increase
  // ref count on each handle creation and conversely decrease it when a handle is destroyed, eg on
  // every hash table lookup etc. The ref count is only increased or decreased at the InternedString
  // API level, when a new string is created, and when a string is destroyed. This allows us to
  // avoid writing to memory unless absolutely necessary, making the handle cheap.

  // Increment ref count, asserts if count grows over type max limit
  void IncrRefCount();

  // Decrement ref count, asserts if count falls below 0
  void DecrRefCount();

  // Returns bytes used, including string, header and trailing byte
  size_t MemUsed() const;

  // Convenience method to deallocate storage. Not for use in destructor.
  static void Destroy(InternedBlobHandle& handle);

  operator std::string_view() const;  // NOLINT (non-explicit operator for easier comparisons)

  auto operator<=>(const InternedBlobHandle& other) const = default;
  bool operator==(const InternedBlobHandle& other) const = default;

  explicit operator bool() const {
    return blob_;
  }

 private:
  explicit InternedBlobHandle(BlobPtr blob) : blob_{blob} {
  }

  BlobPtr blob_{nullptr};
};

struct BlobHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const {
    return std::hash<std::string_view>{}(sv);
  }
};

struct BlobEq {
  using is_transparent = void;
  bool operator()(const InternedBlobHandle& a, const InternedBlobHandle& b) const {
    return a.Data() == b.Data();
  }

  bool operator()(std::string_view a, std::string_view b) const {
    return a == b;
  }
};

// This pool holds blob handles and is used by InternedString to manage string access. It would be
// nice to keep this on the mimalloc heap by using StatelessAllocator. However, JSON memory usage is
// estimated by comparing mimalloc usage before and after creating an object. If we keep this pool
// on mimalloc, it can introduce variations such as resizing of its internal store when adding a new
// object. This results in non-deterministic memory usage, which introduces incorrectness in tests
// and the memory usage command. To keep memory estimation per object accurate, the pool is
// allocated on the default heap.
using InternedBlobPool = absl::flat_hash_set<InternedBlobHandle, BlobHash, BlobEq>;
static_assert(sizeof(InternedBlobHandle) == sizeof(char*));

}  // namespace dfly::detail
