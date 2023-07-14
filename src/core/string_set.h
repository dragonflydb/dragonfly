// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <stddef.h>

#include <cstdint>
#include <experimental/memory_resource>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/dense_set.h"

extern "C" {
#include "redis/sds.h"
}

namespace dfly {

class StringSet : public DenseSet {
 public:
  StringSet(MemoryResource* res = PMR_NS::get_default_resource()) : DenseSet(res) {
  }

  ~StringSet();

  // Returns true if elem was added.
  bool Add(std::string_view s1, uint32_t ttl_sec = UINT32_MAX);

  // Used currently by rdb_load. Returns true if elem was added.
  bool AddSds(sds elem);

  bool Erase(std::string_view s1);

  bool Contains(std::string_view s1) const;

  void Clear();

  std::optional<std::string> Pop();

  class iterator : private IteratorBase {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = sds;
    using pointer = sds*;
    using reference = sds&;

    iterator() : IteratorBase() {
    }

    iterator(DenseSet* set, bool is_end) : IteratorBase(set, is_end) {
    }

    iterator& operator++() {
      Advance();
      return *this;
    }

    bool operator==(const iterator& b) const {
      return curr_list_ == b.curr_list_;
    }

    bool operator!=(const iterator& b) const {
      return !(*this == b);
    }

    value_type operator*() {
      return (value_type)curr_entry_->GetObject();
    }

    value_type operator->() {
      return (value_type)curr_entry_->GetObject();
    }
  };

  class const_iterator : private IteratorBase {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = const char*;
    using pointer = value_type*;
    using reference = value_type&;

    const_iterator() : IteratorBase() {
    }

    const_iterator(const DenseSet* set, bool is_end) : IteratorBase(set, is_end) {
    }

    const_iterator& operator++() {
      Advance();
      return *this;
    }

    bool operator==(const const_iterator& b) const {
      return curr_list_ == b.curr_list_;
    }

    bool operator!=(const const_iterator& b) const {
      return !(*this == b);
    }

    value_type operator*() const {
      return (value_type)curr_entry_->GetObject();
    }

    value_type operator->() const {
      return (value_type)curr_entry_->GetObject();
    }
  };

  iterator begin() {
    return iterator{this, false};
  }

  iterator end() {
    return iterator{this, true};
  }

  const_iterator cbegin() const {
    return const_iterator{this, false};
  }

  const_iterator cend() const {
    return const_iterator{this, true};
  }

  uint32_t Scan(uint32_t, const std::function<void(sds)>&) const;

 protected:
  uint64_t Hash(const void* ptr, uint32_t cookie) const override;

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const override;

  size_t ObjectAllocSize(const void* s1) const override;
  uint32_t ObjExpireTime(const void* obj) const override;
  void ObjDelete(void* obj, bool has_ttl) const override;
};

}  // end namespace dfly
