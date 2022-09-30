#pragma once

#include <cstdint>
#include <functional>
#include <optional>

#include "core/dense_set.h"

extern "C" {
#include "redis/sds.h"
}

namespace dfly {

class StringSet : public DenseSet {
 public:
  bool Add(std::string_view s1);

  // Used currently by rdb_load.
  bool AddSds(sds s1);

  bool Erase(std::string_view s1);

  bool Contains(std::string_view s1) const;

  void Clear();

  std::optional<std::string> Pop();
  sds PopRaw();

  ~StringSet() {
    Clear();
  }

  StringSet(std::pmr::memory_resource* res = std::pmr::get_default_resource()) : DenseSet(res) {
  }

  iterator<sds> begin() {
    return DenseSet::begin<sds>();
  }

  iterator<sds> end() {
    return DenseSet::end<sds>();
  }

  const_iterator<sds> cbegin() const {
    return DenseSet::cbegin<sds>();
  }

  const_iterator<sds> cend() const {
    return DenseSet::cend<sds>();
  }

  uint32_t Scan(uint32_t, const std::function<void(sds)>&) const;

 protected:
  uint64_t Hash(const void* ptr, uint32_t cookie) const override;

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const override;

  size_t ObjectAllocSize(const void* s1) const override;
  void ObjDelete(void* obj) const override;
};

}  // end namespace dfly
