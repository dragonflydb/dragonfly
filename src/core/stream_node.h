// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>

namespace dfly {

// StreamNodeObj is a compact tagged-pointer representation of a stream node
// stored inside a radix tree entry.
//
// It encodes both the node payload pointer and its representation type
// within a single uintptr_t value using bit 52 as a state flag.
//
// Supported representations:
//
//   Raw (bit 52 = 0):
//     ptr_ points directly to a listpack containing the stream entry data.
//
//   Compressed (bit 52 = 1):
//     ptr_ points to a ZSTD-compressed buffer with layout:
//       [4B uncompressed size][4B compressed size][compressed payload]
//
// Important invariants:
//   - Ptr() always returns a usable pointer with tag bits stripped.
//   - Get() returns the raw encoded value and must not be dereferenced.
//   - Ownership of the underlying memory depends on the representation:
//       * Raw: listpack memory
//       * Compressed: allocated compression buffer
class StreamNodeObj {
 public:
  static constexpr uintptr_t kCompressedBit = 1ULL << 52;
  static constexpr uintptr_t kTagMask = 1ULL << 52;

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

  bool IsCompressed() const {
    return (ptr_ & kTagMask) == kCompressedBit;
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

  // Prerequisite: IsRaw() and flag stream_node_zstd_dict_threshold > 0.
  // Attempts compression of the listpack using ZSTD with a trained dictionary.
  // Compression is a no-op if:
  //   1. the dictionary is not ready (still training or dictionary construction failed),
  //   2. raw size is less than 512 bytes,
  //   3. the data compression returned error,
  //   4. the compressed result does not achieve ≥30% size reduction.
  // Returns Compressed StreamNodeObj if compression is applied, otherwise *this.
  StreamNodeObj TryCompress() const;

  // Frees the node's underlying pointer.
  void Free() const;

  // Nullifies the thread-local decompression buffer pointer and resets its capacity.
  void InvalidateDecompressionState();

  // Materializes a decompressed listpack into stable, heap-owned memory.
  // Must only be called on compressed nodes (tl_zstd_ctx must be ready).
  // If `lp` points to the thread-local decompression buffer, allocates a new
  // heap buffer and copies the contents. Otherwise returns `lp` unchanged.
  static uint8_t* MaterializeListpack(uint8_t* lp);

  // Total allocated bytes for this node.
  size_t MallocSize() const;

 private:
  uintptr_t ptr_;
};

}  // namespace dfly
