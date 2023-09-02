// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

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

    // Try reducing memory fragmentation of the value by re-allocating. Returns true if
    // re-allocation happened.
    bool ReallocIfNeeded(float ratio) {
      // Unwrap all links to correctly call SetObject()
      auto* ptr = curr_entry_;
      while (ptr->IsLink())
        ptr = ptr->AsLink();

      auto* obj = ptr->GetObject();
      auto [new_obj, realloced] = static_cast<StringMap*>(owner_)->ReallocIfNeeded(obj, ratio);
      ptr->SetObject(new_obj);
      return realloced;
    }

    iterator& operator++() {
      Advance();
      return *this;
    }

    iterator& operator+=(unsigned int n) {
      for (unsigned int i = 0; i < n; ++i)
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
  bool AddOrUpdate(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX);

  // Returns true if field was added
  // false, if already exists. In that case no update is done.
  bool AddOrSkip(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX);

  bool Erase(std::string_view s1);

  bool Contains(std::string_view s1) const;

  /// @brief  Returns value of the key or nullptr if key not found.
  /// @param key
  /// @return sds
  sds Find(std::string_view key);

  void Clear();

  iterator begin() {
    return iterator{this, false};
  }

  iterator end() {
    return iterator{this, true};
  }

  // Returns a random key value pair.
  // Returns key only if value is a nullptr.
  std::pair<sds, sds> RandomPair();

  // Randomly selects count of key value pairs. The selections are unique.
  // if count is larger than the total number of key value pairs, returns
  // every pair.
  void RandomPairsUnique(unsigned int count, std::vector<sds>& keys, std::vector<sds>& vals,
                         bool with_value);

  // Randomly selects count of key value pairs. The select key value pairs
  // are allowed to have duplications.
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
  void ObjDelete(void* obj, bool has_ttl) const final;
};

}  // namespace dfly
