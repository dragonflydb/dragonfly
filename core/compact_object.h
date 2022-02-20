// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>

#include <memory_resource>
#include <optional>

typedef struct redisObject robj;
typedef struct quicklist quicklist;

namespace dfly {

namespace detail {

class CompactBlob {
  void* ptr_;
  uint32_t sz;

 public:
  CompactBlob() : ptr_(nullptr), sz(0) {
  }

  explicit CompactBlob(std::string_view s, std::pmr::memory_resource* mr);

  void Assign(std::string_view s, std::pmr::memory_resource* mr);

  void Set(void* p, uint32_t s) {
    ptr_ = p;
    sz = s;
  }

  void Free(std::pmr::memory_resource* mr);

  size_t size() const {
    return sz;
  }

  size_t capacity() const;

  void* ptr() const {
    return ptr_;
  }

  std::string_view AsView() const {
    return std::string_view{reinterpret_cast<char*>(ptr_), sz};
  }

  void MakeRoom(size_t current_cap, size_t desired, std::pmr::memory_resource* mr);
} __attribute__((packed));

static_assert(sizeof(CompactBlob) == 12, "");

// redis objects or blobs of upto 4GB size.
struct RobjWrapper {
  size_t MallocUsed() const;

  uint64_t HashCode() const;
  bool Equal(const RobjWrapper& ow) const;
  bool Equal(std::string_view sv) const;
  size_t Size() const;
  void Free(std::pmr::memory_resource* mr);

  CompactBlob blob;
  static_assert(sizeof(blob) == 12);

  uint32_t type : 4;
  uint32_t encoding : 4;
  uint32_t unneeded : 24;
  RobjWrapper() {
  }
} __attribute__((packed));

}  // namespace detail

class CompactObj {
  static constexpr unsigned kInlineLen = 16;

  void operator=(const CompactObj&) = delete;
  CompactObj(const CompactObj&) = delete;

  // 0-16 is reserved for inline lengths of string type.
  enum TagEnum {
    INT_TAG = 17,
    SMALL_TAG = 18,  // TBD
    ROBJ_TAG = 19,
  };

  enum MaskBit {
    REF_BIT = 1,
    EXPIRE_BIT = 2,
    FLAG_BIT = 4,
  };

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

  size_t StrSize() const;

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

  // Takes ownership over o.
  void ImportRObj(robj* o);

  robj* AsRObj() const;

  // Syncs 'this' instance with the object that was previously returned by AsRObj().
  // Requires: AsRObj() has been called before in the same thread in fiber-atomic section.
  void SyncRObj();

  void SetInt(int64_t val);
  std::optional<int64_t> TryGetInt() const;

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

 private:
  bool EqualNonInline(std::string_view sv) const;

  // Requires: HasAllocated() - true.
  void Free();

  bool HasAllocated() const;

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
  if (IsInline()) {
    return std::string_view{u_.inline_str, taglen_} == sv;
  }
  return EqualNonInline(sv);
}

}  // namespace dfly
