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
  Bloom(const Bloom&) = delete;
  Bloom& operator=(const Bloom&) = delete;

 public:
  /**
   * @brief Construct a new Bloom object
   *
   * @param entries - entries are silently rounded up to the minimum capacity.
   * @param error must be in (0, 1) range.
   * @param heap
   */
  Bloom(uint32_t entries, double error, mi_heap_t* heap);
  ~Bloom();

  Bloom(Bloom&&) = default;
  Bloom& operator=(Bloom&&) = default;

  bool Exists(std::string_view str) const;

  /**
   * @brief Adds an item to the bloom filter.
   * @param str  -
   * @return true if element was not present and was added,
   * @return false - if element (or a collision) had already been added previously.
   */
  bool Add(std::string_view str);

 private:
  bool IsSet(size_t index) const;
  bool Set(size_t index);  // return true if bit was set (i.e was 0 before)

  uint8_t hash_cnt_;
  uint8_t bit_log_;  // log of bit length of the filter. bit length is always power of 2.
  uint8_t* bf_;      // pointer to the blob.
};

}  // namespace dfly
