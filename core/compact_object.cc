// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/compact_object.h"

// #define XXH_INLINE_ALL
// #include <xxhash.h>

extern "C" {
#include "redis/object.h"
#include "redis/util.h"
#include "redis/zmalloc.h"  // for non-string objects.
}

#include <absl/strings/str_cat.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

namespace {

size_t QlUsedSize(quicklist* ql) {
  size_t res = ql->len * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  quicklistNode* ptr = ql->head;
  while (ptr) {
    res += ptr->sz;
    ptr = ptr->next;
  }
  return res;
}

thread_local robj tmp_robj{
    .type = 0, .encoding = 0, .lru = 0, .refcount = OBJ_STATIC_REFCOUNT, .ptr = nullptr};
}  // namespace

static_assert(sizeof(CompactObj) == 18);

namespace detail {

CompactBlob::CompactBlob(std::string_view s, pmr::memory_resource* mr)
    : ptr_(nullptr), sz(s.size()) {
  if (sz) {
    ptr_ = mr->allocate(sz);
    memcpy(ptr_, s.data(), s.size());
  }
}

void CompactBlob::Assign(std::string_view s, std::pmr::memory_resource* mr) {
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

void CompactBlob::MakeRoom(size_t current_cap, size_t desired, std::pmr::memory_resource* mr) {
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
  return malloc_usable_size(ptr_);
}

#if 0
std::string_view SmallString::GetPrefix(size_t len, std::string* scratch) const {
  len = std::min<size_t>(len, size_);
  if (len <= kPrefLen) {
    return std::string_view{prefix_, len};
  }
  scratch->resize(len);
  memcpy(scratch->data(), prefix_, kPrefLen);
  Region::Ptr reg_ptr{.arena_ptr = GetSmall(), .arena_idx = arena_indx_};
  uint8_t* realp = region_allocator->Translate(reg_ptr);
  memcpy(scratch->data() + kPrefLen, realp, len - kPrefLen);

  return std::string_view{scratch->data(), len};
}
#endif

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
      return QlUsedSize((quicklist*)ptr);
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
      if (encoding == OBJ_ENCODING_RAW) {
        blob.Free(mr);
      } else {
        CHECK_EQ(OBJ_ENCODING_INT, encoding);
      }
      break;
    case OBJ_LIST:
      CHECK_EQ(encoding, OBJ_ENCODING_QUICKLIST);
      quicklistRelease((quicklist*)ptr);
      break;

    case OBJ_SET:
      LOG(FATAL) << "TBD";
      break;
    case OBJ_ZSET:
      LOG(FATAL) << "TBD";
      break;
    case OBJ_HASH:
      LOG(FATAL) << "Unsupported HASH type";
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

}  // namespace detail

using namespace std;

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

  if (taglen_ == ROBJ_TAG) {
    return u_.r_obj.Size();
  }

  LOG(DFATAL) << "Should not reach " << int(taglen_);
  return 0;
}

unsigned CompactObj::ObjType() const {
  if (IsInline() || taglen_ == INT_TAG)
    return OBJ_STRING;

  if (taglen_ == ROBJ_TAG)
    return u_.r_obj.type;

  LOG(FATAL) << "TBD " << taglen_;
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
  u_.r_obj.lru_unneeded = o->lru;

  if (o->type == OBJ_STRING) {
    std::string_view src((char*)o->ptr, sdslen((sds)o->ptr));
    u_.r_obj.blob.Assign(src, pmr::get_default_resource());
    decrRefCount(o);
  } else {  // Non-string objects we move as is and release Robj wrapper.
    u_.r_obj.blob.Set(o->ptr, 0);
    if (o->refcount == 1)
      zfree(o);
  }
}

robj* CompactObj::AsRObj() const {
  CHECK_EQ(ROBJ_TAG, taglen_);

  tmp_robj.encoding = u_.r_obj.encoding;
  tmp_robj.type = u_.r_obj.type;
  tmp_robj.lru = u_.r_obj.lru_unneeded;
  tmp_robj.ptr = u_.r_obj.blob.ptr();

  return &tmp_robj;
}

void CompactObj::SyncRObj() {
  CHECK_EQ(ROBJ_TAG, taglen_);
  CHECK_EQ(u_.r_obj.type, tmp_robj.type);

  u_.r_obj.encoding = tmp_robj.encoding;
  u_.r_obj.blob.Set(tmp_robj.ptr, 0);
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

  std::string_view input = str;

  if (str.size() <= kInlineLen) {
    SetMeta(str.size(), 0);
    return;
  }

  if (taglen_ != ROBJ_TAG || u_.r_obj.type != OBJ_STRING) {
    SetMeta(ROBJ_TAG);
    u_.r_obj.type = OBJ_STRING;
    u_.r_obj.encoding = OBJ_ENCODING_RAW;
  }

  DCHECK(taglen_ == ROBJ_TAG && u_.r_obj.type == OBJ_STRING);
  CHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding);
  u_.r_obj.blob.Assign(input, pmr::get_default_resource());
}

std::string_view CompactObj::GetSlice(std::string* scratch) const {
  if (IsInline()) {
    return std::string_view{u_.inline_str, taglen_};
  }

  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type);
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding);
    return u_.r_obj.blob.AsView();
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    scratch->assign(an.Piece());

    return *scratch;
  }

  LOG(FATAL) << "Bad tag " << int(taglen_);

  return std::string_view{};
}

bool CompactObj::HasAllocated() const {
  if (IsRef() || taglen_ == INT_TAG || IsInline() ||
      (taglen_ == ROBJ_TAG && u_.r_obj.blob.ptr() == nullptr))
    return false;

  DCHECK(taglen_ == ROBJ_TAG);
  return true;
}

void CompactObj::GetString(string* res) const {
  std::string_view slice = GetSlice(res);
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
    u_.r_obj.Free(pmr::get_default_resource());
  } else {
    LOG(FATAL) << "Bad compact object type " << int(taglen_);
  }

  memset(u_.inline_str, 0, kInlineLen);
}

size_t CompactObj::MallocUsed() const {
  if (!HasAllocated())
    return 0;

  if (taglen_ == ROBJ_TAG) {
    return u_.r_obj.MallocUsed();
  }

  LOG(FATAL) << "TBD";
  return 0;
}

bool CompactObj::operator==(const CompactObj& o) const {
  if (taglen_ == ROBJ_TAG || o.taglen_ == ROBJ_TAG) {
    if (o.taglen_ != taglen_)
      return false;
    return u_.r_obj.Equal(o.u_.r_obj);
  }

  if (taglen_ != o.taglen_)
    return false;
  if (taglen_ == INT_TAG)
    return u_.ival == o.u_.ival;
  DCHECK(IsInline() && o.IsInline());

  if (memcmp(u_.inline_str, o.u_.inline_str, taglen_) != 0)
    return false;

  return true;
}

bool CompactObj::EqualNonInline(std::string_view sv) const {
  switch (taglen_) {
    case INT_TAG: {
      absl::AlphaNum an(u_.ival);
      return sv == an.Piece();
    }
    case ROBJ_TAG:
      return u_.r_obj.Equal(sv);
    default:
      break;
  }
  return false;
}

}  // namespace dfly
