#include <cstdint>

#include "core/compact_object.h"
#include "core/string_set.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

uint64_t StringSet::Hash(const void* ptr) const {
  sds s = (sds)ptr;
  return CompactObj::HashCode(std::string_view{s, sdslen(s)});
}

bool StringSet::Equal(const void* ptr1, const void* ptr2) const {
  sds s1 = (sds)ptr1;
  sds s2 = (sds)ptr2;

  if (sdslen(s1) != sdslen(s2)) {
    return false;
  }
  return sdslen(s1) == 0 || memcmp(s1, s2, sdslen(s1)) == 0;
}

size_t StringSet::ObjectAllocSize(const void* s1) const {
  return zmalloc_usable_size(sdsAllocPtr((sds)s1));
}

bool StringSet::AddRaw(sds s1) {
  return DenseSet::AddInternal(s1);
}

bool StringSet::Add(std::string_view s1) {
  sds newsds = sdsnewlen(s1.data(), s1.size());
  if (!DenseSet::AddInternal(newsds)) {
    sdsfree(newsds);
    return false;
  }

  return true;
}

bool StringSet::EraseRaw(sds s1) {
  void *ret = DenseSet::EraseInternal(s1);
  if (ret == nullptr) {
    return false;
  } else {
    sdsfree((sds)ret);
    return true;
  }
}

bool StringSet::Erase(std::string_view s1) {
  sds to_erase = sdsnewlen(s1.data(), s1.size());
  bool ret = EraseRaw(to_erase);
  sdsfree(to_erase);
  return ret;
}

bool StringSet::ContainsRaw(sds s1) const {
  return DenseSet::ContainsInternal(s1);
}

bool StringSet::Contains(std::string_view s1) const {
  sds to_search = sdsnewlen(s1.data(), s1.size());
  bool ret = DenseSet::ContainsInternal(to_search);
  sdsfree(to_search);
  return ret;
}

void StringSet::Clear() {
  for (auto it = begin(); it != end(); ++it) {
    sdsfree((sds)*it);
  }

  DenseSet::Clear();
}

std::optional<std::string> StringSet::Pop() {
  sds str = (sds)DenseSet::PopInternal();

  if (str == nullptr) {
    return std::nullopt;
  }

  std::string ret{str, sdslen(str)};
  sdsfree(str);

  return ret;
}

sds StringSet::PopRaw() {
  return (sds)DenseSet::PopInternal();
}

uint32_t StringSet::Scan(uint32_t cursor, const std::function<void(const sds)>& func) const {
  return DenseSet::Scan(cursor, [func](const void* ptr) { func((sds)ptr); });
}

};  // namespace dfly