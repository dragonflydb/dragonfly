// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/score_map.h"

#include "base/endian.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/sds_utils.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace std;

namespace dfly {

namespace {

inline double GetValue(sds key) {
  char* valptr = key + sdslen(key) + 1;
  return absl::bit_cast<double>(absl::little_endian::Load64(valptr));
}

void* AllocateScored(string_view field, double value) {
  size_t meta_offset = field.size() + 1;

  // The layout is:
  // key, '\0', 8-byte double value
  sds newkey = AllocSdsWithSpace(field.size(), 8);

  if (!field.empty()) {
    memcpy(newkey, field.data(), field.size());
  }

  absl::little_endian::Store64(newkey + meta_offset, absl::bit_cast<uint64_t>(value));

  return newkey;
}

}  // namespace

ScoreMap::~ScoreMap() {
  Clear();
}

pair<void*, bool> ScoreMap::AddOrUpdate(string_view field, double value) {
  void* newkey = AllocateScored(field, value);

  // Replace the whole entry.
  sds prev_entry = (sds)AddOrReplaceObj(newkey, false);
  if (prev_entry) {
    ObjDelete(prev_entry, false);
    return {newkey, false};
  }

  return {newkey, true};
}

std::pair<void*, bool> ScoreMap::AddOrSkip(std::string_view field, double value) {
  uint64_t hashcode = Hash(&field, 1);
  void* obj = FindInternal(&field, hashcode, 1);  // 1 - string_view

  if (obj)
    return {obj, false};

  void* newkey = AllocateScored(field, value);
  DenseSet::AddUnique(newkey, false, hashcode);
  return {newkey, true};
}

void* ScoreMap::AddUnique(std::string_view field, double value) {
  void* newkey = AllocateScored(field, value);
  DenseSet::AddUnique(newkey, false, Hash(&field, 1));
  return newkey;
}

std::optional<double> ScoreMap::Find(std::string_view field) {
  uint64_t hashcode = Hash(&field, 1);
  sds str = (sds)FindInternal(&field, hashcode, 1);
  if (!str)
    return nullopt;

  return GetValue(str);
}

uint64_t ScoreMap::Hash(const void* obj, uint32_t cookie) const {
  DCHECK_LT(cookie, 2u);

  if (cookie == 0) {
    sds s = (sds)obj;
    return CompactObj::HashCode(string_view{s, sdslen(s)});
  }

  const string_view* sv = (const string_view*)obj;
  return CompactObj::HashCode(*sv);
}

bool ScoreMap::ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
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

size_t ScoreMap::ObjectAllocSize(const void* obj) const {
  sds s1 = (sds)obj;
  size_t res = zmalloc_usable_size(sdsAllocPtr(s1));
  return res;
}

uint32_t ScoreMap::ObjExpireTime(const void* obj) const {
  // Should not reach.
  return UINT32_MAX;
}

void ScoreMap::ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) {
  // Should not reach.
}

void ScoreMap::ObjDelete(void* obj, bool has_ttl) const {
  sds s1 = (sds)obj;
  sdsfree(s1);
}

void* ScoreMap::ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const {
  return nullptr;
}

detail::SdsScorePair ScoreMap::iterator::BreakToPair(void* obj) {
  sds f = (sds)obj;
  return detail::SdsScorePair(f, GetValue(f));
}

namespace {
// Does not Release obj. Callers must do so explicitly if a `Reallocation` happened
pair<sds, bool> DuplicateEntryIfFragmented(void* obj, float ratio) {
  sds key = (sds)obj;
  size_t key_len = sdslen(key);

  if (!zmalloc_page_is_underutilized(key, ratio))
    return {key, false};

  sds newkey = AllocSdsWithSpace(key_len, 8);
  memcpy(newkey, key, key_len + 8 + 1);

  return {newkey, true};
}

}  // namespace

bool ScoreMap::iterator::ReallocIfNeeded(float ratio, std::function<void(sds, sds)> cb) {
  auto* ptr = curr_entry_;

  if (ptr->IsLink()) {
    ptr = ptr->AsLink();
  }

  DCHECK(!ptr->IsEmpty());
  DCHECK(ptr->IsObject());

  auto* obj = ptr->GetObject();
  auto [new_obj, realloced] = DuplicateEntryIfFragmented(obj, ratio);
  if (realloced) {
    if (cb) {
      cb((sds)obj, (sds)new_obj);
    }
    sdsfree((sds)obj);
    ptr->SetObject(new_obj);
  }
  return realloced;
}

}  // namespace dfly
