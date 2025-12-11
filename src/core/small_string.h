// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <array>
#include <cstdint>
#include <string_view>

namespace dfly {

class PageUsage;

// Efficient storage of strings longer than 10 bytes.
// Requires explicit memory management
class SmallString {
  static constexpr unsigned kPrefLen = 10;
  static constexpr unsigned kMaxSize = (1 << 8) - 1;

 public:
  static void InitThreadLocal(void* heap);
  static size_t UsedThreadLocal();
  static bool CanAllocate(size_t size);

  // Returns malloc used.
  size_t Assign(std::string_view s);
  void Free();

  bool Equal(std::string_view o) const;
  bool Equal(const SmallString& mps) const;

  uint64_t HashCode() const;
  uint16_t MallocUsed() const;

  std::array<std::string_view, 2> Get() const;
  void Get(char* out) const;
  void Get(std::string* dest) const;

  bool DefragIfNeeded(PageUsage* page_usage);

  size_t size() const {
    return size_;
  }

  uint8_t first_byte() const {
    return prefix_[0];
  }

 private:
  // The string is stored broken up into two parts, the first one - in this array
  char prefix_[kPrefLen];

  uint32_t small_ptr_;  // 32GB capacity because we ignore 3 lsb bits (i.e. x8).
  uint16_t size_;       // uint16_t - total size (including prefix)

} __attribute__((packed));

}  // namespace dfly
