// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/compact_object.h"

// #define XXH_INLINE_ALL
#include <xxhash.h>

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/stream.h"
#include "redis/util.h"
#include "redis/zmalloc.h"  // for non-string objects.
#include "redis/zset.h"
}
#include <absl/strings/str_cat.h>

#include <jsoncons/json.hpp>

#include "base/flags.h"
#include "base/logging.h"
#include "base/pod_array.h"
#include "core/detail/bitpacking.h"
#include "core/string_map.h"
#include "core/string_set.h"

ABSL_FLAG(bool, use_set2, true, "If true use DenseSet for an optimized set data structure");

namespace dfly {
using namespace std;
using absl::GetFlag;
using detail::binpacked_len;
using MemoryResource = detail::RobjWrapper::MemoryResource;
namespace {

constexpr XXH64_hash_t kHashSeed = 24061983;
constexpr size_t kAlignSize = 8u;

// Approximation since does not account for listpacks.
size_t QlMAllocSize(quicklist* ql) {
  size_t res = ql->len * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  return res + ql->count * 16;  // we account for each member 16 bytes.
}

// Approximated dictionary size.
size_t DictMallocSize(dict* d) {
  size_t res = zmalloc_usable_size(d->ht_table[0]) + zmalloc_usable_size(d->ht_table[1]) +
               znallocx(sizeof(dict));

  return res + dictSize(d) * 16;  // approximation.
}

inline void FreeObjSet(unsigned encoding, void* ptr, MemoryResource* mr) {
  switch (encoding) {
    case kEncodingStrMap: {
      dictRelease((dict*)ptr);
      break;
    }
    case kEncodingStrMap2: {
      delete (StringSet*)ptr;
      break;
    }

    case kEncodingIntSet:
      zfree((void*)ptr);
      break;
    default:
      LOG(FATAL) << "Unknown set encoding type";
  }
}

size_t MallocUsedSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case kEncodingStrMap /*OBJ_ENCODING_HT*/:
      return 0;  // TODO
    case kEncodingStrMap2: {
      StringSet* ss = (StringSet*)ptr;
      return ss->ObjMallocUsed() + ss->SetMallocUsed();
    }
    case kEncodingIntSet:
      return intsetBlobLen((intset*)ptr);
  }

  LOG(DFATAL) << "Unknown set encoding type " << encoding;
  return 0;
}

size_t MallocUsedHSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case kEncodingListPack:
      return lpBytes(reinterpret_cast<uint8_t*>(ptr));
    case kEncodingStrMap2: {
      StringMap* sm = (StringMap*)ptr;
      return sm->ObjMallocUsed() + sm->SetMallocUsed();
    }
  }
  LOG(DFATAL) << "Unknown set encoding type " << encoding;
  return 0;
}

size_t MallocUsedZSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_LISTPACK:
      return lpBytes(reinterpret_cast<uint8_t*>(ptr));
    case OBJ_ENCODING_SKIPLIST: {
      zset* zs = (zset*)ptr;
      return DictMallocSize(zs->dict);
    }
  }
  LOG(DFATAL) << "Unknown set encoding type " << encoding;
  return 0;
}

size_t MallocUsedStream(unsigned encoding, void* streamv) {
  // stream* str_obj = (stream*)streamv;
  return 0;  // TODO
}

inline void FreeObjHash(unsigned encoding, void* ptr) {
  switch (encoding) {
    case kEncodingStrMap2:
      delete ((StringMap*)ptr);
      break;
    case kEncodingListPack:
      lpFree((uint8_t*)ptr);
      break;
    default:
      LOG(FATAL) << "Unknown hset encoding type " << encoding;
  }
}

inline void FreeObjZset(unsigned encoding, void* ptr) {
  zset* zs = (zset*)ptr;
  switch (encoding) {
    case OBJ_ENCODING_SKIPLIST:
      zs = (zset*)ptr;
      dictRelease(zs->dict);
      zslFree(zs->zsl);
      zfree(zs);
      break;
    case OBJ_ENCODING_LISTPACK:
      zfree(ptr);
      break;
    default:
      LOG(FATAL) << "Unknown sorted set encoding" << encoding;
  }
}

inline void FreeObjStream(void* ptr) {
  freeStream((stream*)ptr);
}

// converts 7-bit packed length back to ascii length. Note that this conversion
// is not accurate since it maps 7 bytes to 8 bytes (rounds up), while we may have
// 7 byte strings converted to 7 byte as well.
inline constexpr size_t ascii_len(size_t bin_len) {
  return (bin_len * 8) / 7;
}

inline const uint8_t* to_byte(const void* s) {
  return reinterpret_cast<const uint8_t*>(s);
}

static_assert(binpacked_len(7) == 7);
static_assert(binpacked_len(8) == 7);
static_assert(binpacked_len(15) == 14);
static_assert(binpacked_len(16) == 14);
static_assert(binpacked_len(17) == 15);
static_assert(binpacked_len(18) == 16);
static_assert(binpacked_len(19) == 17);
static_assert(binpacked_len(20) == 18);
static_assert(ascii_len(14) == 16);
static_assert(ascii_len(15) == 17);
static_assert(ascii_len(16) == 18);
static_assert(ascii_len(17) == 19);

struct TL {
  robj tmp_robj{
      .type = 0, .encoding = 0, .lru = 0, .refcount = OBJ_STATIC_REFCOUNT, .ptr = nullptr};

  MemoryResource* local_mr = PMR_NS::get_default_resource();
  size_t small_str_bytes;
  base::PODArray<uint8_t> tmp_buf;
  string tmp_str;
};

thread_local TL tl;

constexpr bool kUseSmallStrings = true;

/// TODO: Ascii encoding becomes slow for large blobs. We should factor it out into a separate
/// file and implement with SIMD instructions.
constexpr bool kUseAsciiEncoding = true;

}  // namespace

static_assert(sizeof(CompactObj) == 18);

namespace detail {

size_t RobjWrapper::MallocUsed() const {
  if (!inner_obj_)
    return 0;

  switch (type_) {
    case OBJ_STRING:
      DVLOG(2) << "Freeing string object";
      CHECK_EQ(OBJ_ENCODING_RAW, encoding_);
      return InnerObjMallocUsed();
    case OBJ_LIST:
      DCHECK_EQ(encoding_, OBJ_ENCODING_QUICKLIST);
      return QlMAllocSize((quicklist*)inner_obj_);
    case OBJ_SET:
      return MallocUsedSet(encoding_, inner_obj_);
    case OBJ_HASH:
      return MallocUsedHSet(encoding_, inner_obj_);
    case OBJ_ZSET:
      return MallocUsedZSet(encoding_, inner_obj_);
    case OBJ_STREAM:
      return MallocUsedStream(encoding_, inner_obj_);

    default:
      LOG(FATAL) << "Not supported " << type_;
  }

  return 0;
}

size_t RobjWrapper::Size() const {
  switch (type_) {
    case OBJ_STRING:
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding_);
      return sz_;
    case OBJ_LIST:
      return quicklistCount((quicklist*)inner_obj_);
    case OBJ_ZSET: {
      robj self{.type = type_,
                .encoding = encoding_,
                .lru = 0,
                .refcount = OBJ_STATIC_REFCOUNT,
                .ptr = inner_obj_};
      return zsetLength(&self);
    }
    case OBJ_SET:
      switch (encoding_) {
        case kEncodingIntSet: {
          intset* is = (intset*)inner_obj_;
          return intsetLen(is);
        }
        case kEncodingStrMap: {
          dict* d = (dict*)inner_obj_;
          return dictSize(d);
        }
        case kEncodingStrMap2: {
          StringSet* ss = (StringSet*)inner_obj_;
          return ss->Size();
        }
        default:
          LOG(FATAL) << "Unexpected encoding " << encoding_;
      };
    default:;
  }
  return 0;
}

void RobjWrapper::Free(MemoryResource* mr) {
  if (!inner_obj_)
    return;
  DVLOG(1) << "RobjWrapper::Free " << inner_obj_;

  switch (type_) {
    case OBJ_STRING:
      DVLOG(2) << "Freeing string object";
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding_);
      mr->deallocate(inner_obj_, 0, 8);  // we do not keep the allocated size.
      break;
    case OBJ_LIST:
      CHECK_EQ(encoding_, OBJ_ENCODING_QUICKLIST);
      quicklistRelease((quicklist*)inner_obj_);
      break;
    case OBJ_SET:
      FreeObjSet(encoding_, inner_obj_, mr);
      break;
    case OBJ_ZSET:
      FreeObjZset(encoding_, inner_obj_);
      break;
    case OBJ_HASH:
      FreeObjHash(encoding_, inner_obj_);
      break;
    case OBJ_MODULE:
      LOG(FATAL) << "Unsupported OBJ_MODULE type";
      break;
    case OBJ_STREAM:
      FreeObjStream(inner_obj_);
      break;
    default:
      LOG(FATAL) << "Unknown object type";
      break;
  }
  Set(nullptr, 0);
}

uint64_t RobjWrapper::HashCode() const {
  switch (type_) {
    case OBJ_STRING:
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding());
      {
        auto str = AsView();
        return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type for hashcode " << type_;
  }
  return 0;
}

bool RobjWrapper::Equal(const RobjWrapper& ow) const {
  if (ow.type_ != type_ || ow.encoding_ != encoding_)
    return false;

  if (type_ == OBJ_STRING) {
    DCHECK_EQ(OBJ_ENCODING_RAW, encoding());
    return AsView() == ow.AsView();
  }
  LOG(FATAL) << "Unsupported type " << type_;
  return false;
}

bool RobjWrapper::Equal(string_view sv) const {
  if (type() != OBJ_STRING)
    return false;

  DCHECK_EQ(OBJ_ENCODING_RAW, encoding());
  return AsView() == sv;
}

void RobjWrapper::SetString(string_view s, MemoryResource* mr) {
  type_ = OBJ_STRING;
  encoding_ = OBJ_ENCODING_RAW;

  if (s.size() > sz_) {
    size_t cur_cap = InnerObjMallocUsed();
    if (s.size() > cur_cap) {
      MakeInnerRoom(cur_cap, s.size(), mr);
    }
    memcpy(inner_obj_, s.data(), s.size());
    sz_ = s.size();
  }
}

bool RobjWrapper::DefragIfNeeded(float ratio) {
  if (type() == OBJ_STRING) {  // only applicable to strings
    if (zmalloc_page_is_underutilized(inner_obj(), ratio)) {
      return Reallocate(tl.local_mr);
    }
  }
  return false;
}

bool RobjWrapper::Reallocate(MemoryResource* mr) {
  void* old_ptr = inner_obj_;
  inner_obj_ = mr->allocate(sz_, kAlignSize);
  memcpy(inner_obj_, old_ptr, sz_);
  mr->deallocate(old_ptr, 0, kAlignSize);
  return true;
}

void RobjWrapper::Init(unsigned type, unsigned encoding, void* inner) {
  type_ = type;
  encoding_ = encoding;
  Set(inner, 0);
}

inline size_t RobjWrapper::InnerObjMallocUsed() const {
  return zmalloc_size(inner_obj_);
}

void RobjWrapper::MakeInnerRoom(size_t current_cap, size_t desired, MemoryResource* mr) {
  if (current_cap * 2 > desired) {
    if (desired < SDS_MAX_PREALLOC)
      desired *= 2;
    else
      desired += SDS_MAX_PREALLOC;
  }

  void* newp = mr->allocate(desired, kAlignSize);
  if (sz_) {
    memcpy(newp, inner_obj_, sz_);
  }

  if (current_cap) {
    mr->deallocate(inner_obj_, current_cap, kAlignSize);
  }
  inner_obj_ = newp;
}

}  // namespace detail

using namespace std;

auto CompactObj::GetStats() -> Stats {
  Stats res;
  res.small_string_bytes = tl.small_str_bytes;

  return res;
}

void CompactObj::InitThreadLocal(MemoryResource* mr) {
  tl.local_mr = mr;
  tl.tmp_buf = base::PODArray<uint8_t>{mr};
}

CompactObj::~CompactObj() {
  if (HasAllocated()) {
    Free();
  }
}

CompactObj& CompactObj::operator=(CompactObj&& o) noexcept {
  DCHECK(&o != this);

  SetMeta(o.taglen_, o.mask_);  // Frees underlying resources if needed.
  memcpy(&u_, &o.u_, sizeof(u_));

  // SetMeta deallocates the object and we only want reset it.
  o.taglen_ = 0;
  o.mask_ = 0;

  return *this;
}

size_t CompactObj::Size() const {
  size_t raw_size = 0;

  if (IsInline()) {
    raw_size = taglen_;
  } else {
    switch (taglen_) {
      case SMALL_TAG:
        raw_size = u_.small_str.size();
        break;
      case INT_TAG: {
        absl::AlphaNum an(u_.ival);
        raw_size = an.size();
        break;
      }
      case EXTERNAL_TAG:
        raw_size = u_.ext_ptr.size;
        break;
      case ROBJ_TAG:
        raw_size = u_.r_obj.Size();
        break;
      default:
        LOG(DFATAL) << "Should not reach " << int(taglen_);
    }
  }
  uint8_t encoded = (mask_ & kEncMask);
  return encoded ? DecodedLen(raw_size) : raw_size;
}

uint64_t CompactObj::HashCode() const {
  DCHECK(taglen_ != JSON_TAG) << "JSON type cannot be used for keys!";

  uint8_t encoded = (mask_ & kEncMask);
  if (IsInline()) {
    if (encoded) {
      char buf[kInlineLen * 2];
      size_t decoded_len = DecodedLen(taglen_);
      detail::ascii_unpack(to_byte(u_.inline_str), decoded_len, buf);
      return XXH3_64bits_withSeed(buf, decoded_len, kHashSeed);
    }
    return XXH3_64bits_withSeed(u_.inline_str, taglen_, kHashSeed);
  }

  if (encoded) {
    GetString(&tl.tmp_str);
    return XXH3_64bits_withSeed(tl.tmp_str.data(), tl.tmp_str.size(), kHashSeed);
  }

  switch (taglen_) {
    case SMALL_TAG:
      return u_.small_str.HashCode();
    case ROBJ_TAG:
      return u_.r_obj.HashCode();
    case INT_TAG: {
      absl::AlphaNum an(u_.ival);
      return XXH3_64bits_withSeed(an.data(), an.size(), kHashSeed);
    }
  }
  // We need hash only for keys.
  LOG(DFATAL) << "Should not reach " << int(taglen_);

  return 0;
}

uint64_t CompactObj::HashCode(string_view str) {
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}

unsigned CompactObj::ObjType() const {
  if (IsInline() || taglen_ == INT_TAG || taglen_ == SMALL_TAG || taglen_ == EXTERNAL_TAG)
    return OBJ_STRING;

  if (taglen_ == ROBJ_TAG)
    return u_.r_obj.type();

  if (taglen_ == JSON_TAG) {
    return OBJ_JSON;
  }

  LOG(FATAL) << "TBD " << int(taglen_);
  return 0;
}

unsigned CompactObj::Encoding() const {
  switch (taglen_) {
    case ROBJ_TAG:
      return u_.r_obj.encoding();
    case INT_TAG:
      return OBJ_ENCODING_INT;
    default:
      return OBJ_ENCODING_RAW;
  }
}

// Takes ownership over o.
void CompactObj::ImportRObj(robj* o) {
  CHECK(1 == o->refcount || o->refcount == OBJ_STATIC_REFCOUNT);
  CHECK_NE(o->encoding, OBJ_ENCODING_EMBSTR);  // need regular one

  SetMeta(ROBJ_TAG);

  if (o->type == OBJ_STRING) {
    std::string_view src((sds)o->ptr, sdslen((sds)o->ptr));
    u_.r_obj.SetString(src, tl.local_mr);
    decrRefCount(o);
  } else {  // Non-string objects we move as is and release Robj wrapper.
    auto type = o->type;
    auto enc = o->encoding;
    if (o->type == OBJ_SET) {
      if (o->encoding == OBJ_ENCODING_INTSET) {
        enc = kEncodingIntSet;
      } else {
        enc = GetFlag(FLAGS_use_set2) ? kEncodingStrMap2 : kEncodingStrMap;
      }
    } else if (o->type == OBJ_HASH) {
      LOG(FATAL) << "Should not reach";
    }
    u_.r_obj.Init(type, enc, o->ptr);
    if (o->refcount == 1)
      zfree(o);
  }
}

robj* CompactObj::AsRObj() const {
  CHECK_EQ(ROBJ_TAG, taglen_);

  robj* res = &tl.tmp_robj;
  unsigned enc = u_.r_obj.encoding();
  res->type = u_.r_obj.type();

  if (res->type == OBJ_SET || res->type == OBJ_HASH) {
    LOG(DFATAL) << "Should not call AsRObj for type " << res->type;
  }

  res->encoding = enc;
  res->lru = 0;  // u_.r_obj.unneeded;
  res->ptr = u_.r_obj.inner_obj();

  return res;
}

void CompactObj::InitRobj(unsigned type, unsigned encoding, void* obj) {
  DCHECK_NE(type, OBJ_STRING);
  SetMeta(ROBJ_TAG, mask_);
  u_.r_obj.Init(type, encoding, obj);
}

void CompactObj::SyncRObj() {
  robj* obj = &tl.tmp_robj;

  DCHECK_EQ(ROBJ_TAG, taglen_);
  DCHECK_EQ(u_.r_obj.type(), obj->type);
  CHECK_NE(OBJ_SET, obj->type) << "sets should be handled without robj";

  unsigned enc = obj->encoding;
  u_.r_obj.Init(obj->type, enc, obj->ptr);
}

void CompactObj::SetInt(int64_t val) {
  if (INT_TAG != taglen_) {
    SetMeta(INT_TAG, mask_ & ~kEncMask);
  }

  u_.ival = val;
}

std::optional<int64_t> CompactObj::TryGetInt() const {
  if (taglen_ != INT_TAG)
    return std::nullopt;
  int64_t val = u_.ival;
  return val;
}

auto CompactObj::GetJson() const -> JsonType* {
  if (ObjType() == OBJ_JSON) {
    return u_.json_obj.json_ptr;
  }
  return nullptr;
}

void CompactObj::SetJson(JsonType&& j) {
  if (taglen_ == JSON_TAG) {                  // already json
    DCHECK(u_.json_obj.json_ptr != nullptr);  // must be allocated
    *u_.json_obj.json_ptr = std::move(j);
  } else {
    SetMeta(JSON_TAG);
    void* ptr = tl.local_mr->allocate(sizeof(JsonType), kAlignSize);
    u_.json_obj.json_ptr = new (ptr) JsonType(std::move(j));
  }
}

void CompactObj::SetString(std::string_view str) {
  uint8_t mask = mask_ & ~kEncMask;

  // Trying auto-detection heuristics first.
  if (str.size() <= 20) {
    long long ival;
    static_assert(sizeof(long long) == 8);

    // We use redis string2ll to be compatible with Redis.
    if (string2ll(str.data(), str.size(), &ival)) {
      SetMeta(INT_TAG, mask);
      u_.ival = ival;

      return;
    }

    if (str.size() <= kInlineLen) {
      SetMeta(str.size(), mask);
      if (!str.empty())
        memcpy(u_.inline_str, str.data(), str.size());
      return;
    }
  }

  DCHECK_GT(str.size(), kInlineLen);

  string_view encoded = str;
  bool is_ascii = kUseAsciiEncoding && detail::validate_ascii_fast(str.data(), str.size());

  if (is_ascii) {
    size_t encode_len = binpacked_len(str.size());
    size_t rev_len = ascii_len(encode_len);

    if (rev_len == str.size()) {
      mask |= ASCII2_ENC_BIT;  // str hits its highest bound.
    } else {
      CHECK_EQ(str.size(), rev_len - 1) << "Bad ascii encoding for len " << str.size();

      mask |= ASCII1_ENC_BIT;
    }

    tl.tmp_buf.resize(encode_len);
    detail::ascii_pack_simd2(str.data(), str.size(), tl.tmp_buf.data());
    encoded = string_view{reinterpret_cast<char*>(tl.tmp_buf.data()), encode_len};

    if (encoded.size() <= kInlineLen) {
      SetMeta(encoded.size(), mask);
      detail::ascii_pack(str.data(), str.size(), reinterpret_cast<uint8_t*>(u_.inline_str));

      return;
    }
  }

  if (kUseSmallStrings) {
    if ((taglen_ == 0 && encoded.size() < (1 << 13))) {
      SetMeta(SMALL_TAG, mask);
      tl.small_str_bytes += u_.small_str.Assign(encoded);
      return;
    }

    if (taglen_ == SMALL_TAG && encoded.size() <= u_.small_str.size()) {
      mask_ = mask;
      tl.small_str_bytes -= u_.small_str.MallocUsed();
      tl.small_str_bytes += u_.small_str.Assign(encoded);
      return;
    }
  }

  SetMeta(ROBJ_TAG, mask);
  u_.r_obj.SetString(encoded, tl.local_mr);
}

string_view CompactObj::GetSlice(string* scratch) const {
  uint8_t is_encoded = mask_ & kEncMask;

  if (IsInline()) {
    if (is_encoded) {
      size_t decoded_len = taglen_ + 2;

      // must be this because we either shortened 17 or 18.
      DCHECK_EQ(is_encoded, ASCII2_ENC_BIT);
      DCHECK_EQ(decoded_len, ascii_len(taglen_));

      scratch->resize(decoded_len);
      detail::ascii_unpack(to_byte(u_.inline_str), decoded_len, scratch->data());
      return *scratch;
    }

    return string_view{u_.inline_str, taglen_};
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    scratch->assign(an.Piece());

    return *scratch;
  }

  if (is_encoded) {
    if (taglen_ == ROBJ_TAG) {
      CHECK_EQ(OBJ_STRING, u_.r_obj.type());
      DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
      size_t decoded_len = DecodedLen(u_.r_obj.Size());
      scratch->resize(decoded_len);
      detail::ascii_unpack_simd(to_byte(u_.r_obj.inner_obj()), decoded_len, scratch->data());
    } else if (taglen_ == SMALL_TAG) {
      size_t decoded_len = DecodedLen(u_.small_str.size());
      size_t space_left = decoded_len - u_.small_str.size();
      scratch->resize(decoded_len);
      string_view slices[2];

      unsigned num = u_.small_str.GetV(slices);
      DCHECK_EQ(2u, num);
      char* next = scratch->data() + space_left;
      memcpy(next, slices[0].data(), slices[0].size());
      next += slices[0].size();
      memcpy(next, slices[1].data(), slices[1].size());
      detail::ascii_unpack_simd(reinterpret_cast<uint8_t*>(scratch->data() + space_left),
                                decoded_len, scratch->data());
    } else {
      LOG(FATAL) << "Unsupported tag " << int(taglen_);
    }
    return *scratch;
  }

  // no encoding.
  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    return u_.r_obj.AsView();
  }

  if (taglen_ == SMALL_TAG) {
    u_.small_str.Get(scratch);
    return *scratch;
  }

  LOG(FATAL) << "Bad tag " << int(taglen_);

  return string_view{};
}

bool CompactObj::DefragIfNeeded(float ratio) {
  switch (taglen_) {
    case ROBJ_TAG:
      // currently only these objet types are supported for this operation
      if (u_.r_obj.inner_obj() != nullptr) {
        return u_.r_obj.DefragIfNeeded(ratio);
      }
      return false;
    case SMALL_TAG:
      return u_.small_str.DefragIfNeeded(ratio);
    case INT_TAG:
      // this is not relevant in this case
      return false;
    case EXTERNAL_TAG:
      return false;
    default:
      // This is the case when the object is at inline_str
      return false;
  }
}

bool CompactObj::HasAllocated() const {
  if (IsRef() || taglen_ == INT_TAG || IsInline() || taglen_ == EXTERNAL_TAG ||
      (taglen_ == ROBJ_TAG && u_.r_obj.inner_obj() == nullptr))
    return false;

  DCHECK(taglen_ == ROBJ_TAG || taglen_ == SMALL_TAG || taglen_ == JSON_TAG);
  return true;
}

void __attribute__((noinline)) CompactObj::GetString(string* res) const {
  res->resize(Size());
  GetString(res->data());
}

void CompactObj::GetString(char* dest) const {
  uint8_t is_encoded = mask_ & kEncMask;

  if (IsInline()) {
    if (is_encoded) {
      size_t decoded_len = taglen_ + 2;

      // must be this because we either shortened 17 or 18.
      DCHECK_EQ(is_encoded, ASCII2_ENC_BIT);
      DCHECK_EQ(decoded_len, ascii_len(taglen_));

      detail::ascii_unpack(to_byte(u_.inline_str), decoded_len, dest);
    } else {
      memcpy(dest, u_.inline_str, taglen_);
    }

    return;
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    memcpy(dest, an.data(), an.size());
    return;
  }

  if (is_encoded) {
    if (taglen_ == ROBJ_TAG) {
      CHECK_EQ(OBJ_STRING, u_.r_obj.type());
      DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
      size_t decoded_len = DecodedLen(u_.r_obj.Size());
      detail::ascii_unpack_simd(to_byte(u_.r_obj.inner_obj()), decoded_len, dest);
    } else if (taglen_ == SMALL_TAG) {
      size_t decoded_len = DecodedLen(u_.small_str.size());

      // we left some space on the left to allow inplace ascii unpacking.
      size_t space_left = decoded_len - u_.small_str.size();
      string_view slices[2];

      unsigned num = u_.small_str.GetV(slices);
      DCHECK_EQ(2u, num);
      char* next = dest + space_left;
      memcpy(next, slices[0].data(), slices[0].size());
      next += slices[0].size();
      memcpy(next, slices[1].data(), slices[1].size());
      detail::ascii_unpack_simd(reinterpret_cast<uint8_t*>(dest + space_left), decoded_len, dest);
    } else {
      LOG(FATAL) << "Unsupported tag " << int(taglen_);
    }
    return;
  }

  // no encoding.
  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    memcpy(dest, u_.r_obj.inner_obj(), u_.r_obj.Size());
    return;
  }

  if (taglen_ == SMALL_TAG) {
    string_view slices[2];
    unsigned num = u_.small_str.GetV(slices);
    DCHECK_EQ(2u, num);
    memcpy(dest, slices[0].data(), slices[0].size());
    dest += slices[0].size();
    memcpy(dest, slices[1].data(), slices[1].size());
    return;
  }

  LOG(FATAL) << "Bad tag " << int(taglen_);
}

void CompactObj::SetExternal(size_t offset, size_t sz) {
  SetMeta(EXTERNAL_TAG, mask_ & ~kEncMask);

  u_.ext_ptr.page_index = offset / 4096;
  u_.ext_ptr.page_offset = offset % 4096;
  u_.ext_ptr.size = sz;
}

std::pair<size_t, size_t> CompactObj::GetExternalSlice() const {
  DCHECK_EQ(EXTERNAL_TAG, taglen_);
  size_t offset = size_t(u_.ext_ptr.page_index) * 4096 + u_.ext_ptr.page_offset;
  return pair<size_t, size_t>(offset, size_t(u_.ext_ptr.size));
}

void CompactObj::Reset() {
  if (HasAllocated()) {
    Free();
  }
  taglen_ = 0;
  mask_ = 0;
}

// Frees all resources if owns.
void CompactObj::Free() {
  DCHECK(HasAllocated());

  if (taglen_ == ROBJ_TAG) {
    u_.r_obj.Free(tl.local_mr);
  } else if (taglen_ == SMALL_TAG) {
    tl.small_str_bytes -= u_.small_str.MallocUsed();
    u_.small_str.Free();
  } else if (taglen_ == JSON_TAG) {
    VLOG(1) << "Freeing JSON object";
    u_.json_obj.json_ptr->~JsonType();
    tl.local_mr->deallocate(u_.json_obj.json_ptr, kAlignSize);
  } else {
    LOG(FATAL) << "Unsupported tag " << int(taglen_);
  }

  memset(u_.inline_str, 0, kInlineLen);
}

size_t CompactObj::MallocUsed() const {
  if (!HasAllocated())
    return 0;

  if (taglen_ == ROBJ_TAG) {
    return u_.r_obj.MallocUsed();
  }

  if (taglen_ == JSON_TAG) {
    DCHECK(u_.json_obj.json_ptr != nullptr);
    return zmalloc_size(u_.json_obj.json_ptr);
  }

  if (taglen_ == SMALL_TAG) {
    return u_.small_str.MallocUsed();
  }

  LOG(DFATAL) << "should not reach";
  return 0;
}

bool CompactObj::operator==(const CompactObj& o) const {
  DCHECK(taglen_ != JSON_TAG && o.taglen_ != JSON_TAG) << "cannot use JSON type to check equal";

  uint8_t m1 = mask_ & kEncMask;
  uint8_t m2 = o.mask_ & kEncMask;
  if (m1 != m2)
    return false;

  if (taglen_ != o.taglen_)
    return false;

  if (taglen_ == ROBJ_TAG)
    return u_.r_obj.Equal(o.u_.r_obj);

  if (taglen_ == INT_TAG)
    return u_.ival == o.u_.ival;

  if (taglen_ == SMALL_TAG)
    return u_.small_str.Equal(o.u_.small_str);

  DCHECK(IsInline() && o.IsInline());

  return memcmp(u_.inline_str, o.u_.inline_str, taglen_) == 0;
}

bool CompactObj::EqualNonInline(std::string_view sv) const {
  switch (taglen_) {
    case INT_TAG: {
      absl::AlphaNum an(u_.ival);
      return sv == an.Piece();
    }

    case ROBJ_TAG:
      return u_.r_obj.Equal(sv);
    case SMALL_TAG:
      return u_.small_str.Equal(sv);
    default:
      break;
  }
  return false;
}

bool CompactObj::CmpEncoded(string_view sv) const {
  size_t encode_len = binpacked_len(sv.size());

  if (IsInline()) {
    if (encode_len != taglen_)
      return false;

    char buf[kInlineLen * 2];
    detail::ascii_unpack(to_byte(u_.inline_str), sv.size(), buf);

    return sv == string_view(buf, sv.size());
  }

  if (taglen_ == ROBJ_TAG) {
    if (u_.r_obj.type() != OBJ_STRING)
      return false;

    if (u_.r_obj.Size() != encode_len)
      return false;

    if (!detail::validate_ascii_fast(sv.data(), sv.size()))
      return false;

    return detail::compare_packed(to_byte(u_.r_obj.inner_obj()), sv.data(), sv.size());
  }

  if (taglen_ == JSON_TAG) {
    return false;  // cannot compare json with string
  }

  if (taglen_ == SMALL_TAG) {
    if (u_.small_str.size() != encode_len)
      return false;

    if (!detail::validate_ascii_fast(sv.data(), sv.size()))
      return false;

    // We need to compare an unpacked sv with 2 packed parts.
    // To compare easily ascii with binary we would need to split ascii at 8 bytes boundaries
    // so that we could pack it into complete binary bytes (8 ascii chars produce 7 bytes).
    // I choose a minimal 16 byte prefix:
    // 1. sv must be longer than 16 if we are here (at least 18 actually).
    // 2. 16 chars produce 14 byte blob that should cover the first slice (10 bytes) and 4 bytes
    //    of the second slice.
    // 3. I assume that the first slice is less than 14 bytes which is correct since small string
    //    has only 9-10 bytes in its inline prefix storage.
    DCHECK_GT(sv.size(), 16u);  // we would not be in SMALL_TAG, otherwise.

    string_view slice[2];
    unsigned num = u_.small_str.GetV(slice);
    DCHECK_EQ(2u, num);
    DCHECK_LT(slice[0].size(), 14u);

    uint8_t tmpbuf[14];
    detail::ascii_pack(sv.data(), 16, tmpbuf);

    // Compare the first slice.
    if (memcmp(slice[0].data(), tmpbuf, slice[0].size()) != 0)
      return false;

    // Compare the prefix of the second slice.
    size_t pref_len = 14 - slice[0].size();

    if (memcmp(slice[1].data(), tmpbuf + slice[0].size(), pref_len) != 0)
      return false;

    // We verified that the first 16 chars (or 14 bytes) are equal.
    // Lets verify the rest - suffix of the second slice and the suffix of sv.
    return detail::compare_packed(to_byte(slice[1].data() + pref_len), sv.data() + 16,
                                  sv.size() - 16);
  }
  LOG(FATAL) << "Unsupported tag " << int(taglen_);
  return false;
}

size_t CompactObj::DecodedLen(size_t sz) const {
  return ascii_len(sz) - ((mask_ & ASCII1_ENC_BIT) ? 1 : 0);
}

MemoryResource* CompactObj::memory_resource() {
  return tl.local_mr;
}

}  // namespace dfly
