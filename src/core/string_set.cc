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
  absl::little_endian::Store32(res + len + 1, at);  // Save TTL

  return res;
}

}  // namespace

StringSet::~StringSet() {
  Clear();
}

bool StringSet::Add(string_view src, uint32_t ttl_sec) {
  uint64_t hash = Hash(&src, 1);
  void* prev = FindInternal(&src, hash, 1);
  if (prev != nullptr) {
    return false;
  }

  sds newsds = MakeSetSds(src, ttl_sec);
  bool has_ttl = ttl_sec != UINT32_MAX;
  AddUnique(newsds, has_ttl, hash);
  return true;
}

void StringSet::AddMany(string_view elems[], size_t count, uint32_t ttl_sec, bool* res) {
  uint64_t hash[kMaxBatchLen];
  string_view* data = elems;
  bool has_ttl = ttl_sec != UINT32_MAX;

  while (count >= kMaxBatchLen) {
    for (unsigned i = 0; i < kMaxBatchLen; ++i) {
      hash[i] = CompactObj::HashCode(data[i]);
      Prefetch(hash[i]);
    }

    for (unsigned i = 0; i < kMaxBatchLen; ++i) {
      void* prev = FindInternal(data + i, hash[i], 1);
      if (prev != nullptr) {
        res[i] = false;
      } else {
        res[i] = true;
        sds field = MakeSetSds(data[i], ttl_sec);
        AddUnique(field, has_ttl, hash[i]);
      }
    }

    count -= kMaxBatchLen;
    data += kMaxBatchLen;
    res += kMaxBatchLen;
  }

  for (unsigned i = 0; i < count; ++i) {
    res[i] = Add(data[i], ttl_sec);
  }
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

void StringSet::ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) {
  return SdsUpdateExpireTime(obj, time_now() + ttl_sec, 0);
}

void StringSet::ObjDelete(void* obj, bool has_ttl) const {
  sdsfree((sds)obj);
}

void* StringSet::ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const {
  sds src = (sds)obj;
  string_view sv{src, sdslen(src)};
  uint32_t ttl_sec = add_ttl ? 0 : (has_ttl ? ObjExpireTime(obj) : UINT32_MAX);
  return (void*)MakeSetSds(sv, ttl_sec);
}

sds StringSet::MakeSetSds(string_view src, uint32_t ttl_sec) const {
  if (ttl_sec != UINT32_MAX) {
    uint32_t at = time_now() + ttl_sec;

    sds newsds = AllocImmutableWithTtl(src.size(), at);
    if (!src.empty())
      memcpy(newsds, src.data(), src.size());
    return newsds;
  }

  return sdsnewlen(src.data(), src.size());
}

}  // namespace dfly
