#pragma once

#include <cstdint>
#include <cstdio>
#include <string_view>

#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
}

namespace dfly::detail {

// Wrapper around listpack map
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
      return {key_v, value_v};
    }

    bool operator==(const Iterator& other) const;

    bool operator!=(const Iterator& other) const {
      return !(operator==(other));
    }

   private:
    void Read();

    uint8_t *lp = nullptr, *ptr = nullptr, *next_ptr = nullptr;
    std::string_view key_v, value_v;
    uint8_t intbuf[2][LP_INTBUF_SIZE];
  };

  explicit ListpackWrap(uint8_t* lp) : lp{lp} {
  }

  Iterator Find(std::string_view key) const;
  Iterator begin() const;
  Iterator end() const;
  size_t size() const;  // number of entries

 private:
  uint8_t* lp;
};

}  // namespace dfly::detail
