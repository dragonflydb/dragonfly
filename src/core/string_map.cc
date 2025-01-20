// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_map.h"

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

constexpr uint64_t kValTtlBit = 1ULL << 63;
constexpr uint64_t kValMask = ~kValTtlBit;

inline sds GetValue(sds key) {
  char* valptr = key + sdslen(key) + 1;
  uint64_t val = absl::little_endian::Load64(valptr);
  return (sds)(kValMask & val);
}

// Returns key, tagged value pair
pair<sds, uint64_t> CreateEntry(string_view field, string_view value, uint32_t time_now,
                                uint32_t ttl_sec) {
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
    uint32_t at = time_now + ttl_sec;
    absl::little_endian::Store32(newkey + meta_offset + 8, at);  // skip the value pointer.
    sdsval_tag |= kValTtlBit;
  }

  if (!field.empty()) {
    memcpy(newkey, field.data(), field.size());
  }

  absl::little_endian::Store64(newkey + meta_offset, sdsval_tag);
  return {newkey, sdsval_tag};
}

}  // namespace

StringMap::~StringMap() {
  Clear();
}

bool StringMap::AddOrUpdate(string_view field, string_view value, uint32_t ttl_sec) {
  // 8 additional bytes for a pointer to value.
  auto [newkey, sdsval_tag] = CreateEntry(field, value, time_now(), ttl_sec);

  // Replace the whole entry.
  sds prev_entry = (sds)AddOrReplaceObj(newkey, sdsval_tag & kValTtlBit);
  if (prev_entry) {
    ObjDelete(prev_entry, false);
    return false;
  }

  return true;
}

bool StringMap::AddOrSkip(std::string_view field, std::string_view value, uint32_t ttl_sec) {
  uint64_t hashcode = Hash(&field, 1);
  void* obj = FindInternal(&field, hashcode, 1);  // 1 - string_view

  if (obj)
    return false;

  auto [newkey, sdsval_tag] = CreateEntry(field, value, time_now(), ttl_sec);
  AddUnique(newkey, sdsval_tag & kValTtlBit, hashcode);
  return true;
}

bool StringMap::Erase(string_view key) {
  return EraseInternal(&key, 1);
}

bool StringMap::Contains(string_view field) const {
  // 1 - means it's string_view. See ObjEqual for details.
  uint64_t hashcode = Hash(&field, 1);
  return FindInternal(&field, hashcode, 1) != nullptr;
}

optional<pair<sds, sds>> StringMap::RandomPair() {
  // Iteration may remove elements, and so we need to loop if we happen to reach the end
  while (true) {
    auto it = begin();

    // It may be that begin() will invalidate all elements, getting us to an Empty() state
    if (Empty()) {
      break;
    }

    it += rand() % UpperBoundSize();
    if (it != end()) {
      return std::make_pair(it->first, it->second);
    }
  }
  return nullopt;
}

void StringMap::RandomPairsUnique(unsigned int count, std::vector<sds>& keys,
                                  std::vector<sds>& vals, bool with_value) {
  unsigned int total_size = SizeSlow();
  unsigned int index = 0;
  if (count > total_size)
    count = total_size;

  auto itr = begin();
  uint32_t picked = 0, remaining = count;
  while (picked < count && itr != end()) {
    double random_double = ((double)rand()) / RAND_MAX;
    double threshold = ((double)remaining) / (total_size - index);
    if (random_double <= threshold) {
      keys.push_back(itr->first);
      if (with_value) {
        vals.push_back(itr->second);
      }
      remaining--;
      picked++;
    }
    ++itr;
    index++;
  }

  DCHECK(keys.size() == count);
  if (with_value)
    DCHECK(vals.size() == count);
}

void StringMap::RandomPairs(unsigned int count, std::vector<sds>& keys, std::vector<sds>& vals,
                            bool with_value) {
  using RandomPick = std::pair<unsigned int, unsigned int>;
  std::vector<RandomPick> picks;
  unsigned int total_size = SizeSlow();

  for (unsigned int i = 0; i < count; ++i) {
    RandomPick pick{rand() % total_size, i};
    picks.push_back(pick);
  }

  std::sort(picks.begin(), picks.end(), [](auto& x, auto& y) { return x.first < y.first; });

  unsigned int index = picks[0].first, pick_index = 0;
  auto itr = begin();
  for (unsigned int i = 0; i < index; ++i)
    ++itr;

  keys.reserve(count);
  if (with_value)
    vals.reserve(count);

  while (itr != end() && pick_index < count) {
    auto [key, val] = *itr;
    while (pick_index < count && index == picks[pick_index].first) {
      int store_order = picks[pick_index].second;
      keys[store_order] = key;
      if (with_value)
        vals[store_order] = val;
      ++pick_index;
    }
    ++index;
    ++itr;
  }
}

pair<sds, bool> StringMap::ReallocIfNeeded(void* obj, float ratio) {
  sds key = (sds)obj;
  size_t key_len = sdslen(key);

  auto* value_ptr = key + key_len + 1;
  uint64_t value_tag = absl::little_endian::Load64(value_ptr);
  sds value = (sds)(uint64_t(value_tag) & kValMask);

  bool realloced_value = false;

  // If the allocated value is underutilized, re-allocate it and update the pointer inside the key
  if (zmalloc_page_is_underutilized(value, ratio)) {
    size_t value_len = sdslen(value);
    sds new_value = sdsnewlen(value, value_len);
    memcpy(new_value, value, value_len);
    uint64_t new_value_tag = (uint64_t(new_value) & kValMask) | (value_tag & ~kValMask);
    absl::little_endian::Store64(value_ptr, new_value_tag);
    sdsfree(value);
    realloced_value = true;
  }

  if (!zmalloc_page_is_underutilized(key, ratio))
    return {key, realloced_value};

  size_t space_size = 8 /* value ptr */ + ((value_tag & kValTtlBit) ? 4 : 0) /* optional expiry */;

  sds new_key = AllocSdsWithSpace(key_len, space_size);
  memcpy(new_key, key, key_len + 1 /* \0 */ + space_size);
  sdsfree(key);

  return {new_key, true};
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

void StringMap::ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) {
  return SdsUpdateExpireTime(obj, time_now() + ttl_sec, 8);
}

void StringMap::ObjDelete(void* obj, bool has_ttl) const {
  sds s1 = (sds)obj;
  sds value = GetValue(s1);
  sdsfree(value);
  sdsfree(s1);
}

void* StringMap::ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const {
  uint32_t ttl_sec = add_ttl ? 0 : (has_ttl ? ObjExpireTime(obj) : UINT32_MAX);
  sds str = (sds)obj;
  auto pair = detail::SdsPair(str, GetValue(str));
  auto [newkey, sdsval_tag] = CreateEntry(pair->first, pair->second, time_now(), ttl_sec);

  return (void*)newkey;
}

detail::SdsPair StringMap::iterator::BreakToPair(void* obj) {
  sds f = (sds)obj;
  return detail::SdsPair(f, GetValue(f));
}

bool StringMap::iterator::ReallocIfNeeded(float ratio) {
  auto* ptr = curr_entry_;
  if (ptr->IsLink()) {
    ptr = ptr->AsLink();
  }

  DCHECK(!ptr->IsEmpty());
  DCHECK(ptr->IsObject());

  auto* obj = ptr->GetObject();
  auto [new_obj, realloced] = static_cast<StringMap*>(owner_)->ReallocIfNeeded(obj, ratio);
  ptr->SetObject(new_obj);

  return realloced;
}

}  // namespace dfly
