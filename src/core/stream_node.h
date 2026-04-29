// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>

namespace dfly {

// StreamNodeObj represents a stream node stored in the rax tree.
//
// Each node is:
//   - Raw: a pointer to a listpack
//
// The representation is explicit and zero-copy.
class StreamNodeObj {
 public:
  static constexpr uintptr_t kTagMask = 1ULL << 52;

  // Construct from a raw tagged value retrieved from the rax tree.
  explicit StreamNodeObj(void* p = nullptr) : ptr_(reinterpret_cast<uintptr_t>(p)) {
  }

  bool operator==(StreamNodeObj other) const {
    return ptr_ == other.ptr_;
  }
  bool operator!=(StreamNodeObj other) const {
    return ptr_ != other.ptr_;
  }

  static StreamNodeObj Raw(const uint8_t* lp) {
    StreamNodeObj r;
    r.ptr_ = reinterpret_cast<uintptr_t>(lp);
    return r;
  }

  bool IsRaw() const {
    return (ptr_ & kTagMask) == 0;
  }

  // Raw pointer with tag bits stripped.
  uint8_t* Ptr() const {
    return std::bit_cast<uint8_t*>(ptr_ & ~kTagMask);
  }

  // Raw tagged pointer
  void* Get() const {
    return std::bit_cast<void*>(ptr_);
  }

  // Returns the uncompressed listpack pointer.
  uint8_t* GetListpack() const;

  // Uncompressed listpack size in bytes.
  uint32_t UncompressedSize() const;

  // Frees the node's underlying pointer
  void Free() const;

  // Total allocated bytes for this node.
  size_t MallocSize() const;

 private:
  uintptr_t ptr_;
};

}  // namespace dfly
