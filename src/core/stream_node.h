// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

class __attribute__((packed)) StreamNode {
 public:
  StreamNode() = delete;

  // Allocates a new StreamNode and takes ownership of the given listpack pointer.
  static StreamNode* New(uint8_t* lp);

  // Frees the node's data and the node itself.
  static void Free(void* node);

  // Updates the node's listpack state. Always refreshes the uncompressed size.
  // Updates the pointer if the listpack was reallocated.
  void SetListpack(uint8_t* lp);

  // Returns a pointer to the raw uncompressed listpack.
  uint8_t* GetListpack() const;

  // Uncompressed listpack size in bytes.
  uint32_t UncompressedSize() const {
    return uncompressed_size_;
  }

  // Returns the total allocated memory in bytes for this node (header + data).
  std::size_t MallocSize() const;

 private:
  uint32_t encoding_ : 2;
  uint32_t uncompressed_size_ : 30;
  uint8_t* data_;
};

}  // namespace dfly
