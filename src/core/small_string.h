// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <string_view>

namespace dfly {

// blob strings of upto ~256B. Small sizes are probably predominant
// for in-memory workloads, especially for keys.
// Please note that this class does not have automatic constructors and destructors, therefore
// it requires explicit management.
class SmallString {
  static constexpr unsigned kPrefLen = 10;
  static constexpr unsigned kMaxSize = (1 << 8) - 1;

 public:
  static void InitThreadLocal(void* heap);
  static size_t UsedThreadLocal();
  static bool CanAllocate(size_t size);

  void Reset() {
    size_ = 0;
  }

  // Returns malloc used.
  size_t Assign(std::string_view s);
  void Free();

  bool Equal(std::string_view o) const;
  bool Equal(const SmallString& mps) const;

  uint16_t size() const {
    return size_;
  }

  uint64_t HashCode() const;

  // I am lying here. we should use mi_malloc_usable size really.
  uint16_t MallocUsed() const;

  void Get(std::string* dest) const;

  // returns 1 or 2 slices representing this small string.
  // Guarantees zero copy, i.e. dest will not point to any of external buffers.
  // With current implementation, it will return 2 slices for a non-empty string.
  unsigned GetV(std::string_view dest[2]) const;

  bool DefragIfNeeded(float ratio);

 private:
  // prefix of the string that is broken down into 2 parts.
  char prefix_[kPrefLen];

  uint32_t small_ptr_;  // 32GB capacity because we ignore 3 lsb bits (i.e. x8).
  uint16_t size_;       // uint16_t - total size (including prefix)

} __attribute__((packed));

}  // namespace dfly
