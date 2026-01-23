// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/container/flat_hash_set.h>

#include <string_view>

namespace dfly::detail {
// The blob class simply holds a size, a ref-count and some characters, followed by the
// nul-character. It is intended to hold reference counted strings with quick size access. The blob
// does not manage its own reference count but provides an API for this. There are checks to see if
// the ref-count does not over or underflow.
class InternedBlob {
  static constexpr auto kHeaderSize = sizeof(uint32_t) * 2;

 public:
  explicit InternedBlob(std::string_view sv);
  ~InternedBlob();

  InternedBlob(const InternedBlob& other) = delete;
  InternedBlob& operator=(const InternedBlob& other) = delete;

  InternedBlob(InternedBlob&& other) noexcept;
  InternedBlob& operator=(InternedBlob&& other) noexcept;

  uint32_t Size() const;

  uint32_t RefCount() const;

  std::string_view View() const;

  // Returns nul terminated string
  const char* Data() const;

  // Increment ref count, asserts if count grows over type max limit
  void IncrRefCount();

  // Decrement ref count, asserts if count falls below 0
  void DecrRefCount();

  void SetRefCount(uint32_t ref_count);

  // Returns bytes used, including string, header and trailing byte
  size_t MemUsed() const;

 private:
  void Destroy();

  // Layout is: 4 bytes size, 4 bytes refcount, char data, followed by nul-char.
  // The trailing nul-char is required because jsoncons needs to access c_str/data without a
  // size. The blob_ itself points directly to the data, so that callers do not have to perform
  // pointer arithmetic for c_str() and data() calls:
  //     [size:4] [refcount:4] [string] [\0]
  //     ^-8      ^- 4         ^blob_
  char* blob_ = nullptr;
};

// Custom hash/eq operators allow only checking the string part of the blob. They also allow direct
// comparison with string views, so intermediate objects need not be created in application code
// using the blob.
struct BlobHash {
  // allow heterogeneous lookup, so there are no conversions from string_view to InternedBlob:
  // https://abseil.io/tips/144
  using is_transparent = void;
  size_t operator()(const InternedBlob*) const;
  size_t operator()(std::string_view) const;
};

struct BlobEq {
  using is_transparent = void;
  bool operator()(const InternedBlob* a, const InternedBlob* b) const;
  bool operator()(const InternedBlob* a, std::string_view b) const;
  bool operator()(std::string_view a, const InternedBlob* b) const;
};

// This pool holds blob pointers and is used by InternedString to manage string access. It would be
// nice to keep this on the mimalloc heap by using StatelessAllocator. However, JSON memory usage is
// estimated by comparing mimalloc usage before and after creating an object. If we keep this pool
// on mimalloc, it can introduce variations such as resizing of its internal store when adding a new
// object. This results in non-deterministic memory usage, which introduces incorrectness in tests
// and the memory usage command. To keep memory estimation per object accurate, the pool is
// allocated on the default heap.
using InternedBlobPool = absl::flat_hash_set<InternedBlob*, BlobHash, BlobEq>;
static_assert(sizeof(InternedBlob) == sizeof(char*));

}  // namespace dfly::detail
