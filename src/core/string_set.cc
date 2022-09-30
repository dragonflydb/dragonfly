#include "core/string_set.h"

#include "core/compact_object.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include "base/logging.h"

using namespace std;

namespace dfly {

bool StringSet::AddSds(sds s1) {
  return AddInternal(s1);
}

bool StringSet::Add(string_view src) {
  sds newsds = sdsnewlen(src.data(), src.size());

  if (!AddInternal(newsds)) {
    sdsfree(newsds);
    return false;
  }

  return true;
}

bool StringSet::Erase(string_view str) {
  void* ret = EraseInternal(&str, 1);
  if (ret) {
  sdsfree((sds)ret);
  return true;
}

  return false;
}

bool StringSet::Contains(string_view s1) const {
  bool ret = ContainsInternal(&s1, 1);
  return ret;
}

void StringSet::Clear() {
  ClearInternal();
}

std::optional<std::string> StringSet::Pop() {
  sds str = (sds)PopInternal();

  if (str == nullptr) {
    return std::nullopt;
  }

  std::string ret{str, sdslen(str)};
  sdsfree(str);

  return ret;
}

sds StringSet::PopRaw() {
  return (sds)PopInternal();
}

uint32_t StringSet::Scan(uint32_t cursor, const std::function<void(const sds)>& func) const {
  return DenseSet::Scan(cursor, [func](const void* ptr) { func((sds)ptr); });
}

uint64_t StringSet::Hash(const void* ptr, uint32_t cookie) const {
  DCHECK_LT(cookie, 2u);

  if (cookie == 0) {
  sds s = (sds)ptr;
    return CompactObj::HashCode(string_view{s, sdslen(s)});
}

  const string_view* sv = (const string_view*)ptr;
  return CompactObj::HashCode(*sv);
}

bool StringSet::ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
  DCHECK_LT(right_cookie, 2u);

  sds s1 = (sds)left;

  if (right_cookie == 0) {
    sds s2 = (sds)right;

  if (sdslen(s1) != sdslen(s2)) {
    return false;
  }

  return sdslen(s1) == 0 || memcmp(s1, s2, sdslen(s1)) == 0;
}

  const string_view* right_sv = (const string_view*)right;
  string_view left_sv{s1, sdslen(s1)};
  return left_sv == (*right_sv);
}

size_t StringSet::ObjectAllocSize(const void* s1) const {
  return zmalloc_usable_size(sdsAllocPtr((sds)s1));
}

void StringSet::ObjDelete(void* obj) const {
  sdsfree((sds)obj);
}

};  // namespace dfly
