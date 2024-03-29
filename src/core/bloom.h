// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>

typedef struct mi_heap_s mi_heap_t;

namespace dfly {

// Bloom filter based on the design of https://github.com/jvirkki/libbloom
class Bloom {
 public:
  // error must be in (0, 1) range.
  // entries are silently rounded up to the minimum capacity.
  Bloom(uint32_t entries, double error, mi_heap_t* heap);
  ~Bloom();

  bool Exists(std::string_view str) const;

  /*
   * Return true if element was not present and was added,
   *  false - element (or a collision) had already been added previously.
   */
  bool Add(std::string_view str);

 private:
  bool IsSet(size_t index) const;
  bool Set(size_t index);  // return true if bit was set (i.e was 0 before)

  uint64_t GetMask() const {
    return (1ULL << bit_log_) - 1;
  }

  uint8_t hash_cnt_;
  uint8_t bit_log_;

  uint8_t* bf_;
};

}  // namespace dfly
