// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_set.h"

#include "core/compact_object.h"
#include "core/sds_utils.h"

extern "C" {
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"

using namespace std;

namespace dfly {

namespace {

inline bool MayHaveTtl(sds s) {
  char* alloc_ptr = (char*)sdsAllocPtr(s);
  return sdslen(s) + 1 + 4 <= zmalloc_usable_size(alloc_ptr);
}

sds AllocImmutableWithTtl(uint32_t len, uint32_t at) {
  sds res = AllocSdsWithSpace(len, sizeof(at));
  absl::little_endian::Store32(res + len + 1, at);

  return res;
}

}  // namespace

StringSet::~StringSet() {
  Clear();
}

bool StringSet::AddSds(sds s1) {
  return AddOrFindObj(s1, false) == nullptr;
}

bool StringSet::Add(string_view src, uint32_t ttl_sec) {
  DCHECK_GT(ttl_sec, 0u);  // ttl_sec == 0 would mean find and delete immediately

  sds newsds = nullptr;
  bool has_ttl = false;

  if (ttl_sec == UINT32_MAX) {
    newsds = sdsnewlen(src.data(), src.size());
  } else {
    uint32_t at = time_now() + ttl_sec;
    DCHECK_LT(time_now(), at);

    newsds = AllocImmutableWithTtl(src.size(), at);
    if (!src.empty())
      memcpy(newsds, src.data(), src.size());
    has_ttl = true;
  }

  if (AddOrFindObj(newsds, has_ttl) != nullptr) {
    sdsfree(newsds);
    return false;
  }

  return true;
}

bool StringSet::Erase(string_view str) {
  return EraseInternal(&str, 1);
}

bool StringSet::Contains(string_view s1) const {
  return FindInternal(&s1, 1) != nullptr;
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

uint32_t StringSet::ObjExpireTime(const void* str) const {
  sds s = (sds)str;
  DCHECK(MayHaveTtl(s));

  char* ttlptr = s + sdslen(s) + 1;
  return absl::little_endian::Load32(ttlptr);
}

void StringSet::ObjDelete(void* obj, bool has_ttl) const {
  sdsfree((sds)obj);
}

}  // namespace dfly
