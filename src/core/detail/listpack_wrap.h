// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <cstdio>
#include <string_view>

namespace dfly::detail {

// Wrapper around map data structure based on listpack
struct ListpackWrap {
 private:
  using IntBuf = uint8_t[2][24];

 public:
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<std::string_view, std::string_view>;
    using reference = value_type;
    using pointer = value_type*;

    Iterator(uint8_t* lp, uint8_t* ptr, IntBuf& intbuf);
    Iterator& operator++();

    value_type operator*() const {
      return {key_v_, value_v_};
    }

    bool operator==(const Iterator& other) const;

    bool operator!=(const Iterator& other) const {
      return !(operator==(other));
    }

   private:
    void Read();  // Read next entry at ptr and determine next_ptr

    uint8_t *lp_ = nullptr, *ptr_ = nullptr, *next_ptr_ = nullptr;
    std::string_view key_v_, value_v_;
    IntBuf& intbuf_;
  };

  explicit ListpackWrap(uint8_t* lp) : lp_{lp} {
  }

  Iterator Find(std::string_view key) const;  // Linear search
  Iterator begin() const;
  Iterator end() const;
  size_t size() const;  // number of entries

 private:
  uint8_t* lp_;
  mutable IntBuf intbuf_;
};

}  // namespace dfly::detail
