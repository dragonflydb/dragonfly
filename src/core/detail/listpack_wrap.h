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
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<std::string_view, std::string_view>;

    Iterator(uint8_t* lp, uint8_t* ptr);
    Iterator(const Iterator&) = delete;  // self-referential with intbuf
    Iterator(Iterator&&) = delete;       // self-referential with intbuf
    Iterator& operator=(Iterator&&) = delete;
    Iterator& operator=(const Iterator&) = delete;

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
    uint8_t intbuf_[2][24];
  };

  explicit ListpackWrap(uint8_t* lp) : lp_{lp} {
  }

  Iterator Find(std::string_view key) const;  // Linear search
  Iterator begin() const;
  Iterator end() const;
  size_t size() const;  // number of entries

 private:
  uint8_t* lp_;
};

}  // namespace dfly::detail
