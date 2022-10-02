// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/dense_set.h"

extern "C" {
#include "redis/sds.h"
}

namespace dfly {

class StringMap : public DenseSet {
 public:
  StringMap(std::pmr::memory_resource* res = std::pmr::get_default_resource()) : DenseSet(res) {
  }

  ~StringMap();

  class iterator : private DenseSet::IteratorBase {
   public:
    iterator() : IteratorBase() {
    }

    iterator(DenseSet* owner, bool is_end) : IteratorBase(owner, is_end) {
    }
  };

  bool AddOrSet(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX);

  bool Erase(std::string_view s1);

  bool Contains(std::string_view s1) const;
  sds Find(std::string_view key);

  void Clear();

 private:
  uint64_t Hash(const void* obj, uint32_t cookie) const final;
  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const final;
  size_t ObjectAllocSize(const void* obj) const final;
  uint32_t ObjExpireTime(const void* obj) const final;
  void ObjDelete(void* obj, bool has_ttl) const final;
};

}  // namespace dfly
