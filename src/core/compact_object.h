// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>

#ifdef __clang__
#include <experimental/memory_resource>
namespace PMR_NS = std::experimental::pmr;
#else
#include <memory_resource>
namespace PMR_NS = std::pmr;
#endif

#include <optional>

#include "core/json_object.h"
#include "core/small_string.h"

typedef struct redisObject robj;

namespace dfly {

constexpr unsigned kEncodingIntSet = 0;
constexpr unsigned kEncodingStrMap = 1;   // for set/map encodings of strings
constexpr unsigned kEncodingStrMap2 = 2;  // for set/map encodings of strings using DenseSet
constexpr unsigned kEncodingListPack = 3;

namespace detail {

// redis objects or blobs of upto 4GB size.
class RobjWrapper {
 public:
  using MemoryResource = PMR_NS::memory_resource;

  RobjWrapper() {
  }
  size_t MallocUsed() const;

  uint64_t HashCode() const;
  bool Equal(const RobjWrapper& ow) const;
  bool Equal(std::string_view sv) const;
  size_t Size() const;
  void Free(MemoryResource* mr);

  void SetString(std::string_view s, MemoryResource* mr);
  void Init(unsigned type, unsigned encoding, void* inner);

  unsigned type() const {
    return type_;
  }
  unsigned encoding() const {
    return encoding_;
  }
  void* inner_obj() const {
    return inner_obj_;
  }

  void set_inner_obj(void* ptr) {
    inner_obj_ = ptr;
  }

  std::string_view AsView() const {
    return std::string_view{reinterpret_cast<char*>(inner_obj_), sz_};
  }

  bool DefragIfNeeded(float ratio);

  // as defined in zset.h
  int ZsetAdd(double score, char* ele, int in_flags, int* out_flags, double* newscore);

 private:
  bool Reallocate(MemoryResource* mr);
  size_t InnerObjMallocUsed() const;
  void MakeInnerRoom(size_t current_cap, size_t desired, MemoryResource* mr);

  void Set(void* p, uint32_t s) {
    inner_obj_ = p;
    sz_ = s;
  }

  void* inner_obj_ = nullptr;

  // semantics depend on the type. For OBJ_STRING it's string length.
  uint32_t sz_ = 0;

  uint32_t type_ : 4;
  uint32_t encoding_ : 4;
  uint32_t : 24;

} __attribute__((packed));

}  // namespace detail

class CompactObj {
  static constexpr unsigned kInlineLen = 16;

  void operator=(const CompactObj&) = delete;
  CompactObj(const CompactObj&) = delete;

  // 0-16 is reserved for inline lengths of string type.
  enum TagEnum {
    INT_TAG = 17,
    SMALL_TAG = 18,
    ROBJ_TAG = 19,
    EXTERNAL_TAG = 20,
    JSON_TAG = 21,
  };

  enum MaskBit {
    REF_BIT = 1,
    EXPIRE_BIT = 2,  // Mark objects that have expiry timestamp assigned.
    FLAG_BIT = 4,    // Used to mark keys that have memcache flags assigned.

    // ascii encoding is not an injective function. it compresses 8 bytes to 7 but also 7 to 7.
    // therefore, in order to know the original length we introduce 2 flags that
    // correct the length upon decoding. ASCII1_ENC_BIT rounds down the decoded length,
    // while ASCII2_ENC_BIT rounds it up. See DecodedLen implementation for more info.
    ASCII1_ENC_BIT = 8,
    ASCII2_ENC_BIT = 0x10,
    IO_PENDING = 0x20,
    STICKY = 0x40,
  };

  static constexpr uint8_t kEncMask = ASCII1_ENC_BIT | ASCII2_ENC_BIT;

 public:
  using PrefixArray = std::vector<std::string_view>;
  using MemoryResource = detail::RobjWrapper::MemoryResource;

  CompactObj() {  // By default - empty string.
  }

  explicit CompactObj(robj* o) {
    ImportRObj(o);
  }

  explicit CompactObj(std::string_view str) {
    SetString(str);
  }

  CompactObj(CompactObj&& cs) noexcept {
    operator=(std::move(cs));
  };

  ~CompactObj();

  CompactObj& operator=(CompactObj&& o) noexcept;

  // Returns object size depending on the semantics.
  // For strings - returns the length of the string.
  // For containers - returns number of elements in the container.
  size_t Size() const;

  // TODO: We don't use c++ constructs (ctor, dtor, =) in objects of U,
  // because we use memcpy here.
  CompactObj AsRef() const {
    CompactObj res;
    memcpy(&res.u_, &u_, sizeof(u_));
    res.taglen_ = taglen_;
    res.mask_ = mask_ | REF_BIT;

    return res;
  }

  bool IsRef() const {
    return mask_ & REF_BIT;
  }

  std::string_view GetSlice(std::string* scratch) const;

  std::string ToString() const {
    std::string res;
    GetString(&res);
    return res;
  }

  uint64_t HashCode() const;
  static uint64_t HashCode(std::string_view str);

  bool operator==(const CompactObj& o) const;

  bool operator==(std::string_view sl) const;

  friend bool operator!=(const CompactObj& lhs, const CompactObj& rhs) {
    return !(lhs == rhs);
  }

  friend bool operator==(std::string_view sl, const CompactObj& o) {
    return o.operator==(sl);
  }

  bool HasExpire() const {
    return mask_ & EXPIRE_BIT;
  }

  void SetExpire(bool e) {
    if (e) {
      mask_ |= EXPIRE_BIT;
    } else {
      mask_ &= ~EXPIRE_BIT;
    }
  }

  bool HasFlag() const {
    return mask_ & FLAG_BIT;
  }

  void SetFlag(bool e) {
    if (e) {
      mask_ |= FLAG_BIT;
    } else {
      mask_ &= ~FLAG_BIT;
    }
  }

  bool HasIoPending() const {
    return mask_ & IO_PENDING;
  }

  bool DefragIfNeeded(float ratio);

  void SetIoPending(bool b) {
    if (b) {
      mask_ |= IO_PENDING;
    } else {
      mask_ &= ~IO_PENDING;
    }
  }

  bool IsSticky() const {
    return mask_ & STICKY;
  }

  void SetSticky(bool s) {
    if (s) {
      mask_ |= STICKY;
    } else {
      mask_ &= ~STICKY;
    }
  }

  unsigned Encoding() const;
  unsigned ObjType() const;

  void* RObjPtr() const {
    return u_.r_obj.inner_obj();
  }

  void SetRObjPtr(void* ptr) {
    u_.r_obj.Init(u_.r_obj.type(), u_.r_obj.encoding(), ptr);
  }

  // Takes ownership over o.
  void ImportRObj(robj* o);

  robj* AsRObj() const;

  // takes ownership over obj_inner.
  // type should not be OBJ_STRING.
  void InitRobj(unsigned type, unsigned encoding, void* obj_inner);

  // Syncs 'this' instance with the object that was previously returned by AsRObj().
  // Requires: AsRObj() has been called before in the same thread in fiber-atomic section.
  void SyncRObj();

  // For STR object.
  void SetInt(int64_t val);
  std::optional<int64_t> TryGetInt() const;

  // We temporary expose this function to avoid passing around robj objects.
  detail::RobjWrapper* GetRobjWrapper() {
    return &u_.r_obj;
  }

  const detail::RobjWrapper* GetRobjWrapper() const {
    return &u_.r_obj;
  }

  // For STR object.
  void SetString(std::string_view str);
  void GetString(std::string* res) const;

  // Will set this to hold OBJ_JSON, after that it is safe to call GetJson
  // NOTE: in order to avid copy which can be expensive in this case,
  // you need to move an object that created with the function JsonFromString
  // into here, no copying is allowed!
  void SetJson(JsonType&& j);

  // pre condition - the type here is OBJ_JSON and was set with SetJson
  JsonType* GetJson() const;

  // dest must have at least Size() bytes available
  void GetString(char* dest) const;

  bool IsExternal() const {
    return taglen_ == EXTERNAL_TAG;
  }

  void SetExternal(size_t offset, size_t sz);
  std::pair<size_t, size_t> GetExternalSlice() const;

  // In case this object a single blob, returns number of bytes allocated on heap
  // for that blob. Otherwise returns 0.
  size_t MallocUsed() const;

  // Resets the object to empty state (string).
  void Reset();

  bool IsInline() const {
    return taglen_ <= kInlineLen;
  }

  static constexpr unsigned InlineLen() {
    return kInlineLen;
  }

  struct Stats {
    size_t small_string_bytes = 0;
  };

  static Stats GetStats();

  static void InitThreadLocal(MemoryResource* mr);
  static MemoryResource* memory_resource();  // thread-local.

 private:
  size_t DecodedLen(size_t sz) const;

  bool EqualNonInline(std::string_view sv) const;

  // Requires: HasAllocated() - true.
  void Free();

  bool HasAllocated() const;

  bool CmpEncoded(std::string_view sv) const;

  void SetMeta(uint8_t taglen, uint8_t mask = 0) {
    if (HasAllocated()) {
      Free();
    } else {
      memset(u_.inline_str, 0, kInlineLen);
    }
    taglen_ = taglen;
    mask_ = mask;
  }

  struct ExternalPtr {
    uint32_t type : 8;
    uint32_t reserved : 24;
    uint32_t page_index;
    uint16_t page_offset;  // 0 for multi-page blobs. != 0 for small blobs.
    uint16_t reserved2;
    uint32_t size;
  } __attribute__((packed));

  struct JsonWrapper {
    JsonType* json_ptr = nullptr;
    size_t unneeded = 0;
  } __attribute__((packed));

  // My main data structure. Union of representations.
  // RobjWrapper is kInlineLen=16 bytes, so we employ SSO of that size via inline_str.
  // In case of int values, we waste 8 bytes. I am assuming it's ok and it's not the data type
  // with biggest memory usage.
  union U {
    char inline_str[kInlineLen];

    SmallString small_str;
    detail::RobjWrapper r_obj;
    JsonWrapper json_obj;
    int64_t ival __attribute__((packed));
    ExternalPtr ext_ptr;

    U() : r_obj() {
    }
  } u_;

  //
  static_assert(sizeof(u_) == 16, "");

  mutable uint8_t mask_ = 0;

  // We currently reserve 5 bits for tags and 3 bits for extending the mask. currently reserved.
  uint8_t taglen_ = 0;
};

inline bool CompactObj::operator==(std::string_view sv) const {
  if (mask_ & kEncMask)
    return CmpEncoded(sv);

  if (IsInline()) {
    return std::string_view{u_.inline_str, taglen_} == sv;
  }
  return EqualNonInline(sv);
}

class CompactObjectView {
 public:
  CompactObjectView(const CompactObj& src) : obj_(src.AsRef()) {
  }
  CompactObjectView(const CompactObjectView& o) : obj_(o.obj_.AsRef()) {
  }
  CompactObjectView(CompactObjectView&& o) = default;

  operator CompactObj() const {
    return obj_.AsRef();
  }

  const CompactObj* operator->() const {
    return &obj_;
  }

  bool operator==(const CompactObjectView& o) const {
    return obj_ == o.obj_;
  }

  uint64_t Hash() const {
    return obj_.HashCode();
  }

  CompactObjectView& operator=(const CompactObjectView& o) {
    obj_ = o.obj_.AsRef();
    return *this;
  }

  bool defined() const {
    return obj_.IsRef();
  }

  void Reset() {
    obj_.Reset();
  }

 private:
  CompactObj obj_;
};

}  // namespace dfly

#undef PMR_NS
