// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/container/node_hash_set.h>

#include <string_view>

namespace dfly::detail {
class InternedBlob {
  static constexpr auto kHeaderSize = sizeof(uint32_t) * 2;

 public:
  // TODO - does this need to accept an allocator??
  explicit InternedBlob(std::string_view sv);
  ~InternedBlob();

  uint32_t Size() const;

  uint32_t RefCount() const;

  std::string_view View() const;

  const char* Data() const;

  // These setters are const as a hack. They are allowed to be const because they mutate the
  // refcount via pointer. If they were non-const InternedString would not be able to mutate pool
  // entries to incr/decr refcounts. Since we do not use the refcount or size in hashing, this is
  // fine.
  void IncrRefCount() const;

  void DecrRefCount() const;

 private:
  // Layout is: 4 bytes size, 4 bytes refcount, char data, followed by nul-char
  // nul-char is required because jsoncons attempts to access c_str/data without a size.
  char* blob_;
};

struct BlobHash {
  // allow heterogeneous lookup, so there are no conversions from string_view to InternedBlob:
  // https://abseil.io/tips/144
  using is_transparent = void;
  size_t operator()(const InternedBlob&) const;
  size_t operator()(const std::string_view&) const;
};

struct BlobEq {
  using is_transparent = void;
  bool operator()(const InternedBlob& a, const InternedBlob& b) const;
  bool operator()(const InternedBlob& a, std::string_view b) const;
  bool operator()(std::string_view a, const InternedBlob& b) const;
};

// This pool holds blobs and is used by InternedString to manage string access. node_hash_set
// instead of flat to maintain pointer stability, as interned strings hold onto pointers into this
// pool. Note that the pool itself does not use stateless allocator or mimalloc heap, it probably
// could.
using InternedBlobPool = absl::node_hash_set<InternedBlob, BlobHash, BlobEq>;

}  // namespace dfly::detail
