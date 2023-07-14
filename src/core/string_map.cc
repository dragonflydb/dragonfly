// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_map.h"

#include <string.h>

#include <fstream>

#include "core/compact_object.h"
#include "core/sds_utils.h"
#include "glog/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace std;

namespace dfly {

namespace {

constexpr uint64_t kValTtlBit = 1ULL << 63;
constexpr uint64_t kValMask = ~kValTtlBit;

inline sds GetValue(sds key) {
  char* valptr = key + sdslen(key) + 1;
  uint64_t val = absl::little_endian::Load64(valptr);
  return (sds)(kValMask & val);
}

}  // namespace

StringMap::~StringMap() {
  Clear();
}

bool StringMap::AddOrUpdate(string_view field, string_view value, uint32_t ttl_sec) {
  // 8 additional bytes for a pointer to value.
  sds newkey;
  size_t meta_offset = field.size() + 1;
  sds sdsval = sdsnewlen(value.data(), value.size());
  uint64_t sdsval_tag = uint64_t(sdsval);

  if (ttl_sec == UINT32_MAX) {
    // The layout is:
    // key, '\0', 8-byte pointer to value
    newkey = AllocSdsWithSpace(field.size(), 8);
  } else {
    // The layout is:
    // key, '\0', 8-byte pointer to value, 4-byte absolute time.
    // the value pointer it tagged.
    newkey = AllocSdsWithSpace(field.size(), 8 + 4);
    uint32_t at = time_now() + ttl_sec;
    absl::little_endian::Store32(newkey + meta_offset + 8, at);  // skip the value pointer.
    sdsval_tag |= kValTtlBit;
  }

  if (!field.empty()) {
    memcpy(newkey, field.data(), field.size());
  }

  absl::little_endian::Store64(newkey + meta_offset, sdsval_tag);

  // Replace the whole entry.
  sds prev_entry = (sds)AddOrReplaceObj(newkey, sdsval_tag & kValTtlBit);
  if (prev_entry) {
    ObjDelete(prev_entry, false);
    return false;
  }

  return true;
}

bool StringMap::AddOrSkip(std::string_view field, std::string_view value, uint32_t ttl_sec) {
  void* obj = FindInternal(&field, 1);  // 1 - string_view

  if (obj)
    return false;

  return AddOrUpdate(field, value, ttl_sec);
}

bool StringMap::Erase(string_view key) {
  return EraseInternal(&key, 1);
}

bool StringMap::Contains(string_view field) const {
  // 1 - means it's string_view. See ObjEqual for details.
  return FindInternal(&field, 1) != nullptr;
}

void StringMap::Clear() {
  ClearInternal();
}

sds StringMap::Find(std::string_view key) {
  sds str = (sds)FindInternal(&key, 1);
  if (!str)
    return nullptr;

  return GetValue(str);
}

uint64_t StringMap::Hash(const void* obj, uint32_t cookie) const {
  DCHECK_LT(cookie, 2u);

  if (cookie == 0) {
    sds s = (sds)obj;
    return CompactObj::HashCode(string_view{s, sdslen(s)});
  }

  const string_view* sv = (const string_view*)obj;
  return CompactObj::HashCode(*sv);
}

bool StringMap::ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
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

size_t StringMap::ObjectAllocSize(const void* obj) const {
  sds s1 = (sds)obj;
  size_t res = zmalloc_usable_size(sdsAllocPtr(s1));
  sds val = GetValue(s1);
  res += zmalloc_usable_size(sdsAllocPtr(val));

  return res;
}

uint32_t StringMap::ObjExpireTime(const void* obj) const {
  sds str = (sds)obj;
  const char* valptr = str + sdslen(str) + 1;

  uint64_t val = absl::little_endian::Load64(valptr);
  DCHECK(val & kValTtlBit);
  if (val & kValTtlBit) {
    return absl::little_endian::Load32(valptr + 8);
  }

  // Should not reach.
  return UINT32_MAX;
}

void StringMap::ObjDelete(void* obj, bool has_ttl) const {
  sds s1 = (sds)obj;
  sds value = GetValue(s1);
  sdsfree(value);
  sdsfree(s1);
}

detail::SdsPair StringMap::iterator::BreakToPair(void* obj) {
  sds f = (sds)obj;
  return detail::SdsPair(f, GetValue(f));
}

}  // namespace dfly
