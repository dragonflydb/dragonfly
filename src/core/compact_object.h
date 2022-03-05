// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>

#include <memory_resource>
#include <optional>

#include "core/small_string.h"

typedef struct redisObject robj;
typedef struct quicklist quicklist;

namespace dfly {

constexpr unsigned kEncodingIntSet = 0;
constexpr unsigned kEncodingStrMap = 1;  // for set/map encodings of strings

namespace detail {

// redis objects or blobs of upto 4GB size.
class RobjWrapper {
 public:
  RobjWrapper() {
  }
  size_t MallocUsed() const;

  uint64_t HashCode() const;
  bool Equal(const RobjWrapper& ow) const;
  bool Equal(std::string_view sv) const;
  size_t Size() const;
  void Free(std::pmr::memory_resource* mr);

  void SetString(std::string_view s, std::pmr::memory_resource* mr);
  void Init(unsigned type, unsigned encoding, void* inner);

  unsigned type() const { return type_; }
  unsigned encoding() const { return encoding_; }
  quicklist* GetQL() const;
  void* inner_obj() const { return inner_obj_;}

  std::string_view AsView() const {
    return std::string_view{reinterpret_cast<char*>(inner_obj_), sz_};
  }

  bool IsMember(std::string_view key) const;

 private:
  size_t InnerObjMallocUsed() const;
  void MakeInnerRoom(size_t current_cap, size_t desired, std::pmr::memory_resource* mr);

  void Set(void* p, uint32_t s) {
    inner_obj_ = p;
    sz_ = s;
  }

  void* inner_obj_ = nullptr;

  // semantics depend on the type. For OBJ_STRING it's string length.
  uint32_t sz_ = 0;

  uint32_t type_ : 4;
  uint32_t encoding_ : 4;
  uint32_t unneeded_ : 24;

} __attribute__((packed));

// unpacks 8->7 encoded blob back to ascii.
// generally, we can not unpack inplace because ascii (dest) buffer is 8/7 bigger than
// the source buffer.
// however, if binary data is positioned on the right of the ascii buffer with empty space on the
// left than we can unpack inplace.
void ascii_unpack(const uint8_t* bin, size_t ascii_len, char* ascii);

// packs ascii string (does not verify) into binary form saving 1 bit per byte on average (12.5%).
void ascii_pack(const char* ascii, size_t len, uint8_t* bin);

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
  };

  enum MaskBit {
    REF_BIT = 1,
    EXPIRE_BIT = 2,
    FLAG_BIT = 4,

    // ascii encoding is not an injective function. it compresses 8 bytes to 7 but also 7 to 7.
    // therefore, in order to know the original length we introduce 2 flags that
    // correct the length upon decoding. ASCII1_ENC_BIT rounds down the decoded length,
    // while ASCII2_ENC_BIT rounds it up. See DecodedLen implementation for more info.
    ASCII1_ENC_BIT = 8,
    ASCII2_ENC_BIT = 0x10,
  };

  static constexpr uint8_t kEncMask = ASCII1_ENC_BIT | ASCII2_ENC_BIT;

 public:
  using PrefixArray = std::vector<std::string_view>;

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

  // Should be called only for container based objects.
  // Returns true if the container contains this key.
  bool IsMember(std::string_view key) const {
    return u_.r_obj.IsMember(key);
  }

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

  unsigned Encoding() const;
  unsigned ObjType() const;

  quicklist* GetQL() const;

  void* RObjPtr() const {
    return u_.r_obj.inner_obj();
  }

  void SetRObjPtr(void* ptr) {
    u_.r_obj.Init(u_.r_obj.type(), u_.r_obj.encoding(), ptr);
  }

  // Takes ownership over o.
  void ImportRObj(robj* o);

  robj* AsRObj() const;

  // takes ownership over obj.
  // type should not be OBJ_STRING.
  void InitRobj(unsigned type, unsigned encoding, void* obj_inner);

  // Syncs 'this' instance with the object that was previously returned by AsRObj().
  // Requires: AsRObj() has been called before in the same thread in fiber-atomic section.
  void SyncRObj();

  // For STR object.
  void SetInt(int64_t val);
  std::optional<int64_t> TryGetInt() const;

  // For STR object.
  void SetString(std::string_view str);
  void GetString(std::string* res) const;

  size_t MallocUsed() const;

  // Resets the object to empty state.
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

  static void InitThreadLocal(std::pmr::memory_resource* mr);

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

  // My main data structure. Union of representations.
  // RobjWrapper is kInlineLen=16 bytes, so we employ SSO of that size via inline_str.
  // In case of int values, we waste 8 bytes. I am assuming it's ok and it's not the data type
  // with biggest memory usage.
  union U {
    char inline_str[kInlineLen];

    SmallString small_str;
    detail::RobjWrapper r_obj;
    int64_t ival __attribute__((packed));

    U() : r_obj() {
    }
  } u_;

  //
  static_assert(sizeof(u_) == 16, "");

  // Maybe it's possible to merge those 2 together and gain another byte
  // but lets postpone it to 2023.
  mutable uint8_t mask_ = 0;
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

}  // namespace dfly
