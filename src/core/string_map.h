// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/dense_set.h"

extern "C" {
#include "redis/sds.h"
}

namespace dfly {

namespace detail {

class SdsPair {
 public:
  SdsPair(sds k, sds v) : first(k), second(v) {
  }

  SdsPair* operator->() {
    return this;
  }

  const SdsPair* operator->() const {
    return this;
  }

  const sds first;
  const sds second;
};

};  // namespace detail

class StringMap : public DenseSet {
 public:
  StringMap(std::pmr::memory_resource* res = std::pmr::get_default_resource()) : DenseSet(res) {
  }

  ~StringMap();

  class iterator : private DenseSet::IteratorBase {
    static detail::SdsPair BreakToPair(void* obj);

   public:
    iterator() : IteratorBase() {
    }

    iterator(DenseSet* owner, bool is_end) : IteratorBase(owner, is_end) {
    }

    detail::SdsPair operator->() const {
      void* ptr = curr_entry_->GetObject();
      return BreakToPair(ptr);
    }

    detail::SdsPair operator*() const {
      void* ptr = curr_entry_->GetObject();
      return BreakToPair(ptr);
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
  };

  // Returns true if field was added
  // otherwise updates its value and returns false.
  bool AddOrSet(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX);

  bool Erase(std::string_view s1);

  bool Contains(std::string_view s1) const;
  sds Find(std::string_view key);

  void Clear();

  iterator begin() {
    return iterator{this, false};
  }

  iterator end() {
    return iterator{this, true};
  }

 private:
  uint64_t Hash(const void* obj, uint32_t cookie) const final;
  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const final;
  size_t ObjectAllocSize(const void* obj) const final;
  uint32_t ObjExpireTime(const void* obj) const final;
  void ObjDelete(void* obj, bool has_ttl) const final;
};

}  // namespace dfly
