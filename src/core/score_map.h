// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string_view>

#include "core/dense_set.h"

extern "C" {
#include "redis/sds.h"
}

namespace dfly {

namespace detail {

class SdsScorePair {
 public:
  SdsScorePair(sds k, double v) : first(k), second(v) {
  }

  SdsScorePair* operator->() {
    return this;
  }

  const SdsScorePair* operator->() const {
    return this;
  }

  const sds first;
  const double second;
};

};  // namespace detail

class ScoreMap : public DenseSet {
 public:
  ScoreMap(MemoryResource* res = PMR_NS::get_default_resource()) : DenseSet(res) {
  }

  ~ScoreMap();

  class iterator : private DenseSet::IteratorBase {
    static detail::SdsScorePair BreakToPair(void* obj);

   public:
    iterator() : IteratorBase() {
    }

    iterator(DenseSet* owner, bool is_end) : IteratorBase(owner, is_end) {
    }

    detail::SdsScorePair operator->() const {
      void* ptr = curr_entry_->GetObject();
      return BreakToPair(ptr);
    }

    detail::SdsScorePair operator*() const {
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

  // Returns pointer to the internal objest and the insertion result.
  // i.e. true if field was added, otherwise updates its value and returns false.
  std::pair<void*, bool> AddOrUpdate(std::string_view field, double value);

  // Returns true if field was added
  // false, if already exists. In that case no update is done.
  std::pair<void*, bool> AddOrSkip(std::string_view field, double value);

  bool Erase(std::string_view s1);

  bool Erase(sds s1) {
    return EraseInternal(s1, 0);
  }

  /// @brief  Returns value of the key or nullptr if key not found.
  /// @param key
  /// @return sds
  std::optional<double> Find(std::string_view key);

  // returns the internal object if found, otherwise nullptr.
  void* FindObj(sds ele) {
    return FindInternal(ele, 0);
  }

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
