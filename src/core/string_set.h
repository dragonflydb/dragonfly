// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>

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

  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec);

  bool Erase(std::string_view str) {
    return EraseInternal(&str, 1);
  }

  bool Contains(std::string_view s1) const {
    return FindInternal(&s1, Hash(&s1, 1), 1) != nullptr;
  }

  std::optional<std::string> Pop();

  class iterator : private IteratorBase {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = sds;
    using pointer = sds*;
    using reference = sds&;

    explicit iterator(const IteratorBase& o) : IteratorBase(o) {
    }

    iterator() : IteratorBase() {
    }

    iterator(DenseSet* set) : IteratorBase(set, false) {
    }

    iterator& operator++() {
      Advance();
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

    value_type operator*() {
      return (value_type)curr_entry_->GetObject();
    }

    value_type operator->() {
      return (value_type)curr_entry_->GetObject();
    }

    using IteratorBase::ExpiryTime;
    using IteratorBase::HasExpiry;
    using IteratorBase::SetExpiryTime;

    // Try reducing memory fragmentation of the value by re-allocating. Returns true if
    // re-allocation happened.
    bool ReallocIfNeeded(float ratio);
  };

  iterator begin() {
    return iterator{this};
  }

  iterator end() {
    return iterator{};
  }

  uint32_t Scan(uint32_t, const std::function<void(sds)>&) const;

  iterator Find(std::string_view member) {
    return iterator{FindIt(&member, 1)};
  }

 protected:
  uint64_t Hash(const void* ptr, uint32_t cookie) const override;

  unsigned AddBatch(absl::Span<std::string_view> span, uint32_t ttl_sec);

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const override;

  size_t ObjectAllocSize(const void* s1) const override;
  uint32_t ObjExpireTime(const void* obj) const override;
  void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) override;
  void ObjDelete(void* obj, bool has_ttl) const override;
  void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const override;
  sds MakeSetSds(std::string_view src, uint32_t ttl_sec) const;

 private:
  std::pair<sds, bool> DuplicateEntryIfFragmented(void* obj, float ratio);
};

}  // end namespace dfly
