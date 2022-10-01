#include "core/string_set.h"

#include "core/compact_object.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include "base/logging.h"

using namespace std;

namespace dfly {

namespace {

inline char SdsReqType(size_t string_size) {
  if (string_size < 1 << 5)
    return SDS_TYPE_5;
  if (string_size < 1 << 8)
    return SDS_TYPE_8;
  if (string_size < 1 << 16)
    return SDS_TYPE_16;
  if (string_size < 1ll << 32)
    return SDS_TYPE_32;
  return SDS_TYPE_64;
}

inline int SdsHdrSize(char type) {
  switch (type & SDS_TYPE_MASK) {
    case SDS_TYPE_5:
      return sizeof(struct sdshdr5);
    case SDS_TYPE_8:
      return sizeof(struct sdshdr8);
    case SDS_TYPE_16:
      return sizeof(struct sdshdr16);
    case SDS_TYPE_32:
      return sizeof(struct sdshdr32);
    case SDS_TYPE_64:
      return sizeof(struct sdshdr64);
  }
  return 0;
}

sds AllocImmutableWithTtl(uint32_t len, uint32_t at) {
  size_t usable;
  char type = SdsReqType(len);
  int hdrlen = SdsHdrSize(type);

  char* ptr = (char*)zmalloc_usable(hdrlen + len + 1 + 4, &usable);
  char* s = ptr + hdrlen;
  char* fp = s - 1;

  switch (type) {
    case SDS_TYPE_5: {
      *fp = type | (len << SDS_TYPE_BITS);
      break;
    }

    case SDS_TYPE_8: {
      SDS_HDR_VAR(8, s);
      sh->len = len;
      sh->alloc = len;
      *fp = type;
      break;
    }

    case SDS_TYPE_16: {
      SDS_HDR_VAR(16, s);
      sh->len = len;
      sh->alloc = len;
      *fp = type;
      break;
    }

    case SDS_TYPE_32: {
      SDS_HDR_VAR(32, s);
      sh->len = len;
      sh->alloc = len;
      *fp = type;
      break;
    }
    case SDS_TYPE_64: {
      SDS_HDR_VAR(64, s);
      sh->len = len;
      sh->alloc = len;
      *fp = type;
      break;
    }
  }
  s[len] = '\0';
  absl::little_endian::Store32(s + len + 1, at);

  return s;
}

inline bool MayHaveTtl(sds s) {
  char* alloc_ptr = (char*)sdsAllocPtr(s);
  return sdslen(s) + 1 + 4 <= zmalloc_usable_size(alloc_ptr);
}

}  // namespace

bool StringSet::AddSds(sds s1) {
  return AddInternal(s1, false);
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

  if (!AddInternal(newsds, has_ttl)) {
    sdsfree(newsds);
    return false;
  }

  return true;
}

bool StringSet::Erase(string_view str) {
  return EraseInternal(&str, 1);
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
