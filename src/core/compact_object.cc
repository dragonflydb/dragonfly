// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/compact_object.h"

// #define XXH_INLINE_ALL
#include <xxhash.h>

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/util.h"
#include "redis/zmalloc.h"  // for non-string objects.
#include "redis/zset.h"
}

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "base/pod_array.h"

#if defined(__aarch64__)
#include "base/sse2neon.h"
#else
#include <emmintrin.h>
#endif

namespace dfly {
using namespace std;

namespace {

constexpr XXH64_hash_t kHashSeed = 24061983;

// Approximation since does not account for listpacks.
size_t QlMAllocSize(quicklist* ql) {
  size_t res = ql->len * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  return res + ql->count * 16;  // we account for each member 16 bytes.
}

// Approximated dictionary size.
size_t DictMallocSize(dict* d) {
  size_t res = zmalloc_usable_size(d->ht_table[0]) + zmalloc_usable_size(d->ht_table[1]) +
               znallocx(sizeof(dict));

  return res = dictSize(d) * 16;  // approximation.
}

inline void FreeObjSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_HT:
      dictRelease((dict*)ptr);
      break;
    case OBJ_ENCODING_INTSET:
      zfree((void*)ptr);
      break;
    default:
      LOG(FATAL) << "Unknown set encoding type";
  }
}

size_t MallocUsedSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_HT:
      return DictMallocSize((dict*)ptr);
    case OBJ_ENCODING_INTSET:
      return intsetBlobLen((intset*)ptr);
    default:
      LOG(FATAL) << "Unknown set encoding type " << encoding;
  }
}

size_t MallocUsedHSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_LISTPACK:
      return lpBytes(reinterpret_cast<uint8_t*>(ptr));
    case OBJ_ENCODING_HT:
      return DictMallocSize((dict*)ptr);
    default:
      LOG(FATAL) << "Unknown set encoding type " << encoding;
  }
}

inline void FreeObjHash(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_HT:
      dictRelease((dict*)ptr);
      break;
    case OBJ_ENCODING_LISTPACK:
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

// Deniel's Lemire function validate_ascii_fast() - under Apache/MIT license.
// See https://github.com/lemire/fastvalidate-utf-8/
// The function returns true (1) if all chars passed in src are
// 7-bit values (0x00..0x7F). Otherwise, it returns false (0).
bool validate_ascii_fast(const char* src, size_t len) {
  size_t i = 0;
  __m128i has_error = _mm_setzero_si128();
  if (len >= 16) {
    for (; i <= len - 16; i += 16) {
      __m128i current_bytes = _mm_loadu_si128((const __m128i*)(src + i));
      has_error = _mm_or_si128(has_error, current_bytes);
    }
  }
  int error_mask = _mm_movemask_epi8(has_error);

  char tail_has_error = 0;
  for (; i < len; i++) {
    tail_has_error |= src[i];
  }
  error_mask |= (tail_has_error & 0x80);

  return !error_mask;
}

// maps ascii len to 7-bit packed length. Each 8 bytes are converted to 7 bytes.
inline constexpr size_t binpacked_len(size_t ascii_len) {
  return (ascii_len * 7 + 7) / 8; /* rounded up */
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

  pmr::memory_resource* local_mr = pmr::get_default_resource();
  size_t small_str_bytes;
  base::PODArray<uint8_t> tmp_buf;
  string tmp_str;
};

thread_local TL tl;

constexpr bool kUseSmallStrings = true;
constexpr bool kUseAsciiEncoding = true;
}  // namespace

static_assert(sizeof(CompactObj) == 18);

namespace detail {

CompactBlob::CompactBlob(string_view s, pmr::memory_resource* mr) : ptr_(nullptr), sz(s.size()) {
  if (sz) {
    ptr_ = mr->allocate(sz);
    memcpy(ptr_, s.data(), s.size());
  }
}

void CompactBlob::Assign(string_view s, pmr::memory_resource* mr) {
  if (s.size() > sz) {
    size_t cur_cap = capacity();
    if (s.size() > cur_cap)
      MakeRoom(cur_cap, s.size(), mr);
  }
  memcpy(ptr_, s.data(), s.size());
  sz = s.size();
}

void CompactBlob::Free(pmr::memory_resource* mr) {
  mr->deallocate(ptr_, 0);  // we do not keep the allocated size.
  sz = 0;
  ptr_ = nullptr;
}

void CompactBlob::MakeRoom(size_t current_cap, size_t desired, pmr::memory_resource* mr) {
  if (current_cap * 2 > desired) {
    if (desired < SDS_MAX_PREALLOC)
      desired *= 2;
    else
      desired += SDS_MAX_PREALLOC;
  }
  void* newp = mr->allocate(desired);
  if (sz) {
    memcpy(newp, ptr_, sz);
  }
  if (current_cap) {
    mr->deallocate(ptr_, current_cap);
  }
  ptr_ = newp;
}

// here we break pmr model since we use non-pmr api of fetching usable size based on pointer.
size_t CompactBlob::capacity() const {
  return zmalloc_size(ptr_);
}

size_t RobjWrapper::MallocUsed() const {
  void* ptr = blob.ptr();
  if (!ptr)
    return 0;

  switch (type) {
    case OBJ_STRING:
      DVLOG(2) << "Freeing string object";
      CHECK_EQ(OBJ_ENCODING_RAW, encoding);
      return blob.capacity();
      break;
    case OBJ_LIST:
      CHECK_EQ(encoding, OBJ_ENCODING_QUICKLIST);
      return QlMAllocSize((quicklist*)ptr);
    case OBJ_SET:
      return MallocUsedSet(encoding, ptr);
    case OBJ_HASH:
      return MallocUsedHSet(encoding, ptr);
    default:
      LOG(FATAL) << "Not supported " << type;
  }

  return 0;
}

size_t RobjWrapper::Size() const {
  switch (type) {
    case OBJ_STRING:
      DVLOG(2) << "Freeing string object";
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding);
      return blob.size();
      break;
    default:;
  }
  return 0;
}

void RobjWrapper::Free(std::pmr::memory_resource* mr) {
  void* ptr = blob.ptr();
  if (!ptr)
    return;

  switch (type) {
    case OBJ_STRING:
      DVLOG(2) << "Freeing string object";
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding);
      blob.Free(mr);
      break;
    case OBJ_LIST:
      CHECK_EQ(encoding, OBJ_ENCODING_QUICKLIST);
      quicklistRelease((quicklist*)ptr);
      break;
    case OBJ_SET:
      FreeObjSet(encoding, ptr);
      break;
    case OBJ_ZSET:
      FreeObjZset(encoding, ptr);
      break;
    case OBJ_HASH:
      FreeObjHash(encoding, ptr);
      break;
    case OBJ_MODULE:
      LOG(FATAL) << "Unsupported OBJ_MODULE type";
      break;
    case OBJ_STREAM:
      LOG(FATAL) << "Unsupported OBJ_STREAM type";
      break;
    default:
      LOG(FATAL) << "Unknown object type";
      break;
  }
  blob.Set(nullptr, 0);
}

uint64_t RobjWrapper::HashCode() const {
  switch (type) {
    case OBJ_STRING:
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding);
      {
        auto str = blob.AsView();
        return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type for hashcode " << type;
  }
  return 0;
}

bool RobjWrapper::Equal(const RobjWrapper& ow) const {
  if (ow.type != type || ow.encoding != encoding)
    return false;

  if (type == OBJ_STRING) {
    DCHECK_EQ(OBJ_ENCODING_RAW, encoding);
    return blob.AsView() == ow.blob.AsView();
  }
  LOG(FATAL) << "Unsupported type " << type;
  return false;
}

bool RobjWrapper::Equal(std::string_view sv) const {
  if (type != OBJ_STRING)
    return false;

  DCHECK_EQ(OBJ_ENCODING_RAW, encoding);
  return blob.AsView() == sv;
}

// len must be at least 16
void ascii_pack(const char* ascii, size_t len, uint8_t* bin) {
  unsigned i = 0;
  while (len >= 8) {
    for (i = 0; i < 7; ++i) {
      *bin++ = (ascii[0] >> i) | (ascii[1] << (7 - i));
      ++ascii;
    }
    ++ascii;
    len -= 8;
  }

  for (i = 0; i < len; ++i) {
    *bin++ = (ascii[i] >> i) | (ascii[i + 1] << (7 - i));
  }
}

// unpacks 8->7 encoded blob back to ascii.
// generally, we can not unpack inplace because ascii (dest) buffer is 8/7 bigger than
// the source buffer.
// however, if binary data is positioned on the right of the ascii buffer with empty space on the
// left than we can unpack inplace.
void ascii_unpack(const uint8_t* bin, size_t ascii_len, char* ascii) {
  constexpr uint8_t kM = 0x7F;
  uint8_t p = 0;
  unsigned i = 0;

  auto step = [&] {
    uint8_t src = *bin;  // keep on stack in case we unpack inplace.
    *ascii++ = (p >> (8 - i)) | ((src << i) & kM);
    p = src;
    ++bin;
  };

  while (ascii_len >= 8) {
    for (i = 0; i < 7; ++i) {
      step();
    }
    ascii_len -= 8;
    *ascii++ = p >> 1;
  }

  for (i = 0; i < ascii_len; ++i) {
    uint8_t src = *bin;
    *ascii++ = (p >> (8 - i)) | ((src << i) & kM);
    p = src;
    ++bin;
  }
}

// compares packed and unpacked strings. packed must be of length = binpacked_len(ascii_len).
bool compare_packed(const uint8_t* packed, const char* ascii, size_t ascii_len) {
  unsigned i = 0;
  bool res = true;

  while (ascii_len >= 8) {
    for (i = 0; i < 7; ++i) {
      uint8_t conv = (ascii[0] >> i) | (ascii[1] << (7 - i));
      res &= (conv == *packed);
      ++ascii;
      ++packed;
    }

    if (!res)
      return false;
    ++ascii;
    ascii_len -= 8;
  }

  for (i = 0; i < ascii_len; ++i) {
    uint8_t b = (ascii[i] >> i) | (ascii[i + 1] << (7 - i));
    res &= (b == *packed);
    ++packed;
  }

  return res;
}

}  // namespace detail

using namespace std;

auto CompactObj::GetStats() -> Stats {
  Stats res;
  res.small_string_bytes = tl.small_str_bytes;

  return res;
}

void CompactObj::InitThreadLocal(pmr::memory_resource* mr) {
  tl.local_mr = mr;
  tl.tmp_buf = base::PODArray<uint8_t>{mr};
  SmallString::InitThreadLocal();
}

CompactObj::~CompactObj() {
  if (HasAllocated()) {
    Free();
  }
}

CompactObj& CompactObj::operator=(CompactObj&& o) noexcept {
  SetMeta(o.taglen_, o.mask_);  // Frees underlying resources if needed.
  memcpy(&u_, &o.u_, sizeof(u_));

  // SetMeta deallocates the object and we only want reset it.
  o.taglen_ = 0;
  o.mask_ = 0;

  return *this;
}

size_t CompactObj::StrSize() const {
  if (IsInline()) {
    return taglen_;
  }

  if (taglen_ == SMALL_TAG) {
    return u_.small_str.size();
  }

  if (taglen_ == ROBJ_TAG) {
    return u_.r_obj.Size();
  }

  LOG(DFATAL) << "Should not reach " << int(taglen_);
  return 0;
}

uint64_t CompactObj::HashCode() const {
  uint8_t encoded = (mask_ & kEncMask);

  if (IsInline()) {
    if (encoded) {
      char buf[kInlineLen * 2];
      detail::ascii_unpack(to_byte(u_.inline_str), taglen_, buf);
      return XXH3_64bits_withSeed(buf, DecodedLen(taglen_), kHashSeed);
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
  LOG(DFATAL) << "Should not reach " << int(taglen_);

  return 0;
}

uint64_t CompactObj::HashCode(std::string_view str) {
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}
unsigned CompactObj::ObjType() const {
  if (IsInline() || taglen_ == INT_TAG || taglen_ == SMALL_TAG)
    return OBJ_STRING;

  if (taglen_ == ROBJ_TAG)
    return u_.r_obj.type;

  LOG(FATAL) << "TBD " << int(taglen_);
  return 0;
}

unsigned CompactObj::Encoding() const {
  switch (taglen_) {
    case ROBJ_TAG:
      return u_.r_obj.encoding;
    case INT_TAG:
      return OBJ_ENCODING_INT;
    default:
      return OBJ_ENCODING_RAW;
  }
}

quicklist* CompactObj::GetQL() const {
  CHECK_EQ(taglen_, ROBJ_TAG);
  CHECK_EQ(u_.r_obj.type, OBJ_LIST);
  CHECK_EQ(u_.r_obj.encoding, OBJ_ENCODING_QUICKLIST);

  return (quicklist*)u_.r_obj.blob.ptr();
}

// Takes ownership over o.
void CompactObj::ImportRObj(robj* o) {
  CHECK(1 == o->refcount || o->refcount == OBJ_STATIC_REFCOUNT);
  CHECK_NE(o->encoding, OBJ_ENCODING_EMBSTR);  // need regular one

  SetMeta(ROBJ_TAG);

  u_.r_obj.type = o->type;
  u_.r_obj.encoding = o->encoding;
  u_.r_obj.unneeded = o->lru;

  if (o->type == OBJ_STRING) {
    std::string_view src((char*)o->ptr, sdslen((sds)o->ptr));
    u_.r_obj.blob.Assign(src, tl.local_mr);
    decrRefCount(o);
  } else {  // Non-string objects we move as is and release Robj wrapper.
    u_.r_obj.blob.Set(o->ptr, 0);
    if (o->refcount == 1)
      zfree(o);
  }
}

robj* CompactObj::AsRObj() const {
  CHECK_EQ(ROBJ_TAG, taglen_);

  robj* res = &tl.tmp_robj;
  res->encoding = u_.r_obj.encoding;
  res->type = u_.r_obj.type;
  res->lru = u_.r_obj.unneeded;
  res->ptr = u_.r_obj.blob.ptr();

  return res;
}

void CompactObj::SyncRObj() {
  robj* obj = &tl.tmp_robj;

  DCHECK_EQ(ROBJ_TAG, taglen_);
  DCHECK_EQ(u_.r_obj.type, obj->type);

  u_.r_obj.encoding = obj->encoding;
  u_.r_obj.blob.Set(obj->ptr, 0);
}

void CompactObj::SetInt(int64_t val) {
  if (INT_TAG != taglen_) {
    SetMeta(INT_TAG);
  }

  u_.ival = val;
}

std::optional<int64_t> CompactObj::TryGetInt() const {
  if (taglen_ != INT_TAG)
    return std::nullopt;
  int64_t val = u_.ival;
  return val;
}

void CompactObj::SetString(std::string_view str) {
  // Trying auto-detection heuristics first.
  if (str.size() <= 20) {  // TODO: to move OBJ_ENCODING_INT out of ROBJ logic.
    long long ival;
    static_assert(sizeof(long long) == 8);

    // We use redis string2ll to be compatible with Redis.
    if (string2ll(str.data(), str.size(), &ival)) {
      SetMeta(INT_TAG);
      u_.ival = ival;

      return;
    }

    if (str.size() <= kInlineLen) {
      SetMeta(str.size());

      memcpy(u_.inline_str, str.data(), str.size());
      return;
    }
  }

  if (str.size() <= kInlineLen) {
    SetMeta(str.size(), 0);
    return;
  }

  string_view encoded = str;
  uint8_t mask = 0;
  bool is_ascii = kUseAsciiEncoding && validate_ascii_fast(str.data(), str.size());

  if (is_ascii) {
    size_t encode_len = binpacked_len(str.size());
    size_t rev_len = ascii_len(encode_len);
    CHECK_GE(rev_len, str.size() - 1) << "Bad ascii encoding for len " << str.size();

    if (rev_len == str.size() - 1) {
      mask |= ASCII1_ENC_BIT;
    } else {
      mask |= ASCII2_ENC_BIT;
    }

    tl.tmp_buf.resize(encode_len);
    detail::ascii_pack(str.data(), str.size(), tl.tmp_buf.data());
    encoded = string_view{reinterpret_cast<char*>(tl.tmp_buf.data()), encode_len};

    if (encoded.size() <= kInlineLen) {
      SetMeta(encoded.size(), mask);
      detail::ascii_pack(str.data(), str.size(), reinterpret_cast<uint8_t*>(u_.inline_str));

      return;
    }
  }

  if (kUseSmallStrings && taglen_ == 0 && encoded.size() < (1 << 15)) {
    SetMeta(SMALL_TAG, mask);
    u_.small_str.Assign(encoded);
    tl.small_str_bytes += u_.small_str.MallocUsed();
    return;
  }

  SetMeta(ROBJ_TAG, mask);
  u_.r_obj.type = OBJ_STRING;
  u_.r_obj.encoding = OBJ_ENCODING_RAW;

  DCHECK(taglen_ == ROBJ_TAG && u_.r_obj.type == OBJ_STRING);
  CHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding);
  u_.r_obj.blob.Assign(encoded, tl.local_mr);
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
      CHECK_EQ(OBJ_STRING, u_.r_obj.type);
      DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding);
      size_t decoded_len = DecodedLen(u_.r_obj.blob.size());
      scratch->resize(decoded_len);
      detail::ascii_unpack(to_byte(u_.r_obj.blob.ptr()), decoded_len, scratch->data());
    } else if (taglen_ == SMALL_TAG) {
      size_t decoded_len = DecodedLen(u_.small_str.size());
      size_t pref_len = decoded_len - u_.small_str.size();
      scratch->resize(decoded_len);
      string_view slices[2];

      unsigned num = u_.small_str.GetV(slices);
      DCHECK_EQ(2u, num);
      char* next = scratch->data() + pref_len;
      memcpy(next, slices[0].data(), slices[0].size());
      next += slices[0].size();
      memcpy(next, slices[1].data(), slices[1].size());
      detail::ascii_unpack(reinterpret_cast<uint8_t*>(scratch->data() + pref_len), decoded_len,
                           scratch->data());
    } else {
      LOG(FATAL) << "Unsupported tag " << int(taglen_);
    }
    return *scratch;
  }

  // no encoding.
  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type);
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding);
    return u_.r_obj.blob.AsView();
  }

  if (taglen_ == SMALL_TAG) {
    u_.small_str.Get(scratch);
    return *scratch;
  }

  LOG(FATAL) << "Bad tag " << int(taglen_);

  return string_view{};
}

bool CompactObj::HasAllocated() const {
  if (IsRef() || taglen_ == INT_TAG || IsInline() ||
      (taglen_ == ROBJ_TAG && u_.r_obj.blob.ptr() == nullptr))
    return false;

  DCHECK(taglen_ == ROBJ_TAG || taglen_ == SMALL_TAG);
  return true;
}

void CompactObj::GetString(string* res) const {
  string_view slice = GetSlice(res);
  if (res->data() != slice.data()) {
    res->assign(slice);
  }
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

  if (taglen_ == SMALL_TAG) {
    return u_.small_str.MallocUsed();
  }

  LOG(FATAL) << "TBD";
  return 0;
}

bool CompactObj::operator==(const CompactObj& o) const {
  uint8_t m1 = mask_ & kEncMask;
  uint8_t m2 = mask_ & kEncMask;
  if (m1 != m2)
    return false;

  if (taglen_ == ROBJ_TAG || o.taglen_ == ROBJ_TAG) {
    if (o.taglen_ != taglen_)
      return false;
    return u_.r_obj.Equal(o.u_.r_obj);
  }

  if (taglen_ != o.taglen_)
    return false;

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
    if (u_.r_obj.type != OBJ_STRING)
      return false;

    if (u_.r_obj.blob.size() != encode_len)
      return false;

    if (!validate_ascii_fast(sv.data(), sv.size()))
      return false;

    return detail::compare_packed(to_byte(u_.r_obj.blob.ptr()), sv.data(), sv.size());
  }

  if (taglen_ == SMALL_TAG) {
    if (u_.small_str.size() != encode_len)
      return false;

    if (!validate_ascii_fast(sv.data(), sv.size()))
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

}  // namespace dfly
