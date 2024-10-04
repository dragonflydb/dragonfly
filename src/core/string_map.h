// Copyright 2022, DragonflyDB authors.  All rights reserved.
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
  StringMap(MemoryResource* res = PMR_NS::get_default_resource()) : DenseSet(res) {
  }

  ~StringMap();

  class iterator : private DenseSet::IteratorBase {
    static detail::SdsPair BreakToPair(void* obj);

   public:
    iterator() : IteratorBase() {
    }

    explicit iterator(const IteratorBase& o) : IteratorBase(o) {
    }

    iterator(DenseSet* owner) : IteratorBase(owner, false) {
    }

    detail::SdsPair operator->() const {
      void* ptr = curr_entry_->GetObject();
      return BreakToPair(ptr);
    }

    detail::SdsPair operator*() const {
      void* ptr = curr_entry_->GetObject();
      return BreakToPair(ptr);
    }

    // Try reducing memory fragmentation of the value by re-allocating. Returns true if
    // re-allocation happened.
    bool ReallocIfNeeded(float ratio);

    iterator& operator++() {
      Advance();
      return *this;
    }

    // Advances at most `n` steps, but stops at end.
    iterator& operator+=(unsigned int n) {
      for (unsigned int i = 0; i < n; ++i) {
        if (curr_entry_ == nullptr) {
          break;
        }

        Advance();
      }
      return *this;
    }

    bool operator==(const iterator& b) const {
      if (owner_ == nullptr && b.owner_ == nullptr) {  // to allow comparison with end()
        return true;
      }
      return owner_ == b.owner_ && curr_entry_ == b.curr_entry_;
    }

    bool operator!=(const iterator& b) const {
      return !(*this == b);
    }

    using IteratorBase::ExpiryTime;
    using IteratorBase::HasExpiry;
    using IteratorBase::SetExpiryTime;
  };

  // otherwise updates its value and returns false.
  bool AddOrUpdate(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX);

  // Returns true if field was added
  // false, if already exists. In that case no update is done.
  bool AddOrSkip(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX);

  bool Erase(std::string_view s1);

  bool Contains(std::string_view s1) const;

  /// @brief  Returns value of the key or an empty iterator if key not found.
  /// @param key
  /// @return sds
  iterator Find(std::string_view member) {
    return iterator{FindIt(&member, 1)};
  }

  iterator begin() {
    return iterator{this};
  }

  iterator end() {
    return iterator{};
  }

  // Returns a random key value pair.
  // Returns key only if value is a nullptr.
  std::optional<std::pair<sds, sds>> RandomPair();

  // Randomly selects count of key value pairs. The selections are unique.
  // if count is larger than the total number of key value pairs, returns
  // every pair.
  // Executes at O(n) (i.e. slow for large sets).
  void RandomPairsUnique(unsigned int count, std::vector<sds>& keys, std::vector<sds>& vals,
                         bool with_value);

  // Randomly selects count of key value pairs. The select key value pairs
  // are allowed to have duplications.
  // Executes at O(n) (i.e. slow for large sets).
  void RandomPairs(unsigned int count, std::vector<sds>& keys, std::vector<sds>& vals,
                   bool with_value);

 private:
  // Reallocate key and/or value if their pages are underutilized.
  // Returns new pointer (stays same if key utilization is enough) and if reallocation happened.
  std::pair<sds, bool> ReallocIfNeeded(void* obj, float ratio);

  uint64_t Hash(const void* obj, uint32_t cookie) const final;
  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const final;
  size_t ObjectAllocSize(const void* obj) const final;
  uint32_t ObjExpireTime(const void* obj) const final;
  void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) override;
  void ObjDelete(void* obj, bool has_ttl) const override;
  void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const final;
};

}  // namespace dfly
