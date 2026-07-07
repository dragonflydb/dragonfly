// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// PROTOTYPE: copy of StringSet rebased onto CrtpDenseSet (static dispatch, no vtable).
#pragma once

#include <absl/base/internal/endian.h>
#include <absl/types/span.h>

#include <string_view>

#include "core/compact_object.h"
#include "core/crtp_dense_set.h"
#include "core/sds_utils.h"

extern "C" {
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

namespace dfly {
namespace crtp_bench {

class CrtpStringSet : public CrtpDenseSet<CrtpStringSet> {
  using Base = CrtpDenseSet<CrtpStringSet>;
  friend Base;

 public:
  CrtpStringSet() = default;
  ~CrtpStringSet() {
    Clear();
  }

  bool Add(std::string_view src, uint32_t ttl_sec = UINT32_MAX) {
    uint64_t hash = Hash(&src, 1);
    void* prev = FindInternal(&src, hash, 1);
    if (prev != nullptr)
      return false;
    sds newsds = MakeSetSds(src, ttl_sec);
    bool has_ttl = ttl_sec != UINT32_MAX;
    AddUnique(newsds, has_ttl, hash);
    return true;
  }

  bool Erase(std::string_view str) {
    return EraseInternal(&str, 1);
  }

  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec = UINT32_MAX,
                   bool keepttl = true) {
    std::string_view views[Base::kMaxBatchLen];
    unsigned res = 0;
    if (BucketCount() < span.size()) {
      Reserve(span.size());
    }

    while (span.size() >= Base::kMaxBatchLen) {
      for (size_t i = 0; i < Base::kMaxBatchLen; i++)
        views[i] = span[i];

      span.remove_prefix(Base::kMaxBatchLen);
      res += AddBatch(absl::MakeSpan(views), ttl_sec, keepttl);
    }

    if (span.size()) {
      for (size_t i = 0; i < span.size(); i++)
        views[i] = span[i];

      res += AddBatch(absl::MakeSpan(views, span.size()), ttl_sec, keepttl);
    }
    return res;
  }

  bool Contains(std::string_view s1) const {
    return FindInternal(&s1, Hash(&s1, 1), 1) != nullptr;
  }

  class iterator : private Base::IteratorBase {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = sds;

    iterator() : IteratorBase() {
    }
    explicit iterator(const IteratorBase& o) : IteratorBase(o) {
    }
    iterator(Base* set) : IteratorBase(set, false) {
    }

    iterator& operator++() {
      Advance();
      return *this;
    }
    bool operator==(const iterator& b) const {
      if (owner_ == nullptr && b.owner_ == nullptr)
        return true;
      return owner_ == b.owner_ && curr_entry_ == b.curr_entry_;
    }
    bool operator!=(const iterator& b) const {
      return !(*this == b);
    }
    value_type operator*() {
      return (value_type)curr_entry_->GetObject();
    }
  };

  iterator begin() {
    return iterator{this};
  }
  iterator end() {
    return iterator{};
  }

  iterator Find(std::string_view member) {
    return iterator{FindIt(&member, 1)};
  }

  // Implementations called via CRTP from CrtpDenseSet (no `override`, no vtable).
  uint64_t Hash(const void* ptr, uint32_t cookie) const {
    if (cookie == 0) {
      sds s = (sds)ptr;
      return CompactObj::HashCode(std::string_view{s, sdslen(s)});
    }
    const std::string_view* sv = (const std::string_view*)ptr;
    return CompactObj::HashCode(*sv);
  }

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
    sds s1 = (sds)left;
    if (right_cookie == 0) {
      sds s2 = (sds)right;
      if (sdslen(s1) != sdslen(s2))
        return false;
      return sdslen(s1) == 0 || memcmp(s1, s2, sdslen(s1)) == 0;
    }
    const std::string_view* right_sv = (const std::string_view*)right;
    std::string_view left_sv{s1, sdslen(s1)};
    return left_sv == (*right_sv);
  }

  size_t ObjectAllocSize(const void* s1) const {
    return zmalloc_usable_size(sdsAllocPtr((sds)s1));
  }

  uint32_t ObjExpireTime(const void* str) const {
    sds s = (sds)str;
    char* ttlptr = s + sdslen(s) + 1;
    return absl::little_endian::Load32(ttlptr);
  }

  void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) {
    sds s = (sds)obj;
    char* ttlptr = s + sdslen(s) + 1;
    absl::little_endian::Store32(ttlptr, time_now() + ttl_sec);
  }

  void ObjDelete(void* obj) const {
    sdsfree((sds)obj);
  }

  void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const {
    sds src = (sds)obj;
    std::string_view sv{src, sdslen(src)};
    uint32_t ttl_sec = add_ttl ? 0 : (has_ttl ? ObjExpireTime(obj) : UINT32_MAX);
    return (void*)MakeSetSds(sv, ttl_sec);
  }

 private:
  unsigned AddBatch(absl::Span<std::string_view> span, uint32_t ttl_sec, bool keepttl) {
    uint64_t hash[Base::kMaxBatchLen];
    bool has_ttl = ttl_sec != UINT32_MAX;
    unsigned count = span.size();
    unsigned res = 0;

    for (size_t i = 0; i < count; i++) {
      hash[i] = CompactObj::HashCode(span[i]);
      Prefetch(hash[i]);
    }

    for (unsigned i = 0; i < count; ++i) {
      void* prev = FindInternal(&span[i], hash[i], 1);
      if (prev == nullptr) {
        ++res;
        sds field = MakeSetSds(span[i], ttl_sec);
        AddUnique(field, has_ttl, hash[i]);
      } else if (has_ttl && !keepttl) {
        sds field = MakeSetSds(span[i], ttl_sec);
        void* old = AddOrReplaceObj(field, true);
        ObjDelete(old);
      }
    }

    return res;
  }

  sds MakeSetSds(std::string_view src, uint32_t ttl_sec) const {
    if (ttl_sec != UINT32_MAX) {
      uint32_t at = time_now() + ttl_sec;
      sds res = AllocSdsWithSpace(src.size(), sizeof(at));
      absl::little_endian::Store32(res + src.size() + 1, at);
      if (!src.empty())
        memcpy(res, src.data(), src.size());
      return res;
    }
    return sdsnewlen(src.data(), src.size());
  }
};

// PROTOTYPE: byte-for-byte the same as CrtpStringSet, except instantiated
// against CrtpDenseSet<CrtpStringSetNoTag, false> - i.e. devirtualization
// only, with the cached-hash-tag optimization compiled out entirely (see the
// kHashTagEnabled `if constexpr` branches in CrtpDenseSet). Exists purely to
// isolate the "devirt only" rung of the benchmark ladder from "devirt +
// tagging" (CrtpStringSet) - never call both from the same binary expecting
// shared behavior beyond Add/Get/Erase semantics; this is benchmark scratch
// code, not a place to fix bugs twice.
class CrtpStringSetNoTag : public CrtpDenseSet<CrtpStringSetNoTag, false> {
  using Base = CrtpDenseSet<CrtpStringSetNoTag, false>;
  friend Base;

 public:
  CrtpStringSetNoTag() = default;
  ~CrtpStringSetNoTag() {
    Clear();
  }

  bool Add(std::string_view src, uint32_t ttl_sec = UINT32_MAX) {
    uint64_t hash = Hash(&src, 1);
    void* prev = FindInternal(&src, hash, 1);
    if (prev != nullptr)
      return false;
    sds newsds = MakeSetSds(src, ttl_sec);
    bool has_ttl = ttl_sec != UINT32_MAX;
    AddUnique(newsds, has_ttl, hash);
    return true;
  }

  bool Erase(std::string_view str) {
    return EraseInternal(&str, 1);
  }

  bool Contains(std::string_view s1) const {
    return FindInternal(&s1, Hash(&s1, 1), 1) != nullptr;
  }

  class iterator : private Base::IteratorBase {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = sds;

    iterator() : IteratorBase() {
    }
    explicit iterator(const IteratorBase& o) : IteratorBase(o) {
    }
    iterator(Base* set) : IteratorBase(set, false) {
    }

    iterator& operator++() {
      Advance();
      return *this;
    }
    bool operator==(const iterator& b) const {
      if (owner_ == nullptr && b.owner_ == nullptr)
        return true;
      return owner_ == b.owner_ && curr_entry_ == b.curr_entry_;
    }
    bool operator!=(const iterator& b) const {
      return !(*this == b);
    }
    value_type operator*() {
      return (value_type)curr_entry_->GetObject();
    }
  };

  iterator begin() {
    return iterator{this};
  }
  iterator end() {
    return iterator{};
  }

  iterator Find(std::string_view member) {
    return iterator{FindIt(&member, 1)};
  }

  // Implementations called via CRTP from CrtpDenseSet (no `override`, no vtable).
  uint64_t Hash(const void* ptr, uint32_t cookie) const {
    if (cookie == 0) {
      sds s = (sds)ptr;
      return CompactObj::HashCode(std::string_view{s, sdslen(s)});
    }
    const std::string_view* sv = (const std::string_view*)ptr;
    return CompactObj::HashCode(*sv);
  }

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
    sds s1 = (sds)left;
    if (right_cookie == 0) {
      sds s2 = (sds)right;
      if (sdslen(s1) != sdslen(s2))
        return false;
      return sdslen(s1) == 0 || memcmp(s1, s2, sdslen(s1)) == 0;
    }
    const std::string_view* right_sv = (const std::string_view*)right;
    std::string_view left_sv{s1, sdslen(s1)};
    return left_sv == (*right_sv);
  }

  size_t ObjectAllocSize(const void* s1) const {
    return zmalloc_usable_size(sdsAllocPtr((sds)s1));
  }

  uint32_t ObjExpireTime(const void* str) const {
    sds s = (sds)str;
    char* ttlptr = s + sdslen(s) + 1;
    return absl::little_endian::Load32(ttlptr);
  }

  void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) {
    sds s = (sds)obj;
    char* ttlptr = s + sdslen(s) + 1;
    absl::little_endian::Store32(ttlptr, time_now() + ttl_sec);
  }

  void ObjDelete(void* obj) const {
    sdsfree((sds)obj);
  }

  void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const {
    sds src = (sds)obj;
    std::string_view sv{src, sdslen(src)};
    uint32_t ttl_sec = add_ttl ? 0 : (has_ttl ? ObjExpireTime(obj) : UINT32_MAX);
    return (void*)MakeSetSds(sv, ttl_sec);
  }

 private:
  sds MakeSetSds(std::string_view src, uint32_t ttl_sec) const {
    if (ttl_sec != UINT32_MAX) {
      uint32_t at = time_now() + ttl_sec;
      sds res = AllocSdsWithSpace(src.size(), sizeof(at));
      absl::little_endian::Store32(res + src.size() + 1, at);
      if (!src.empty())
        memcpy(res, src.data(), src.size());
      return res;
    }
    return sdsnewlen(src.data(), src.size());
  }
};

// PROTOTYPE: same as CrtpStringSet (devirt + hash-tag, via CrtpDenseSet), but
// members are a minimal custom blob instead of a real sds - no SDS_TYPE_5/8/
// 16/32/64 header-type selection, no growth/mutation support, since a set
// member is written once and never resized. Layout: [4-byte length][key
// bytes][4-byte expiry, present only when the DensePtr's own kTtlBit says so -
// same convention the sds version uses, the caller already checked HasTtl()
// before ever calling ObjExpireTime/ObjUpdateExpireTime].
class CrtpStringSetBlob : public CrtpDenseSet<CrtpStringSetBlob> {
  using Base = CrtpDenseSet<CrtpStringSetBlob>;
  friend Base;

  // Descriptor byte, mirroring OAHEntry's kSsoBit: high bit clear -> length
  // is the low 7 bits of byte 0 (keys up to 127 bytes, header = 1 byte total).
  // High bit set -> byte 0 is just a marker, the real length is a 4-byte
  // field right after it (header = 5 bytes total). This is what actually
  // gives short keys a smaller allocation than the fixed-4-byte-length
  // version - sds's own SDS_TYPE_5 does the same kind of thing for the same
  // reason.
  static constexpr uint32_t kShortLenMax = 127;

  static uint32_t BlobLen(const void* blob) {
    uint8_t b0 = *(const uint8_t*)blob;
    if (!(b0 & 0x80))
      return b0;
    return absl::little_endian::Load32((const char*)blob + 1);
  }
  static const char* BlobData(const void* blob) {
    uint8_t b0 = *(const uint8_t*)blob;
    return (const char*)blob + ((b0 & 0x80) ? 5 : 1);
  }
  static size_t BlobHeaderSize(size_t len) {
    return len <= kShortLenMax ? 1 : 5;
  }

 public:
  CrtpStringSetBlob() = default;
  ~CrtpStringSetBlob() {
    Clear();
  }

  bool Add(std::string_view src, uint32_t ttl_sec = UINT32_MAX) {
    uint64_t hash = Hash(&src, 1);
    void* prev = FindInternal(&src, hash, 1);
    if (prev != nullptr)
      return false;
    void* blob = MakeBlob(src, ttl_sec);
    AddUnique(blob, ttl_sec != UINT32_MAX, hash);
    return true;
  }

  bool Erase(std::string_view str) {
    return EraseInternal(&str, 1);
  }

  bool Contains(std::string_view s1) const {
    return FindInternal(&s1, Hash(&s1, 1), 1) != nullptr;
  }

  class iterator : private Base::IteratorBase {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string_view;

    iterator() : IteratorBase() {
    }
    explicit iterator(const IteratorBase& o) : IteratorBase(o) {
    }
    iterator(Base* set) : IteratorBase(set, false) {
    }

    iterator& operator++() {
      Advance();
      return *this;
    }
    bool operator==(const iterator& b) const {
      if (owner_ == nullptr && b.owner_ == nullptr)
        return true;
      return owner_ == b.owner_ && curr_entry_ == b.curr_entry_;
    }
    bool operator!=(const iterator& b) const {
      return !(*this == b);
    }
    value_type operator*() {
      const void* blob = curr_entry_->GetObject();
      return {BlobData(blob), BlobLen(blob)};
    }

    using IteratorBase::ExpiryTime;
    using IteratorBase::HasExpiry;
  };

  iterator begin() {
    return iterator{this};
  }
  iterator end() {
    return iterator{};
  }

  iterator Find(std::string_view member) {
    return iterator{FindIt(&member, 1)};
  }

  // Implementations called via CRTP from CrtpDenseSet (no `override`, no vtable).
  uint64_t Hash(const void* ptr, uint32_t cookie) const {
    if (cookie == 0) {
      return CompactObj::HashCode(std::string_view{BlobData(ptr), BlobLen(ptr)});
    }
    const std::string_view* sv = (const std::string_view*)ptr;
    return CompactObj::HashCode(*sv);
  }

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
    uint32_t llen = BlobLen(left);
    if (right_cookie == 0) {
      if (llen != BlobLen(right))
        return false;
      return llen == 0 || memcmp(BlobData(left), BlobData(right), llen) == 0;
    }
    const std::string_view* right_sv = (const std::string_view*)right;
    return std::string_view{BlobData(left), llen} == *right_sv;
  }

  size_t ObjectAllocSize(const void* blob) const {
    return zmalloc_usable_size(const_cast<void*>(blob));
  }

  uint32_t ObjExpireTime(const void* blob) const {
    return absl::little_endian::Load32(BlobData(blob) + BlobLen(blob));
  }

  void ObjUpdateExpireTime(const void* blob, uint32_t ttl_sec) {
    char* ttlptr = const_cast<char*>(BlobData(blob)) + BlobLen(blob);
    absl::little_endian::Store32(ttlptr, time_now() + ttl_sec);
  }

  void ObjDelete(void* blob) const {
    zfree(blob);
  }

  void* ObjectClone(const void* blob, bool has_ttl, bool add_ttl) const {
    std::string_view sv{BlobData(blob), BlobLen(blob)};
    uint32_t ttl_sec = add_ttl ? 0 : (has_ttl ? ObjExpireTime(blob) : UINT32_MAX);
    return MakeBlob(sv, ttl_sec);
  }

 private:
  void* MakeBlob(std::string_view src, uint32_t ttl_sec) const {
    const bool has_ttl = ttl_sec != UINT32_MAX;
    const size_t hdr = BlobHeaderSize(src.size());
    const size_t total = hdr + src.size() + (has_ttl ? 4 : 0);
    char* buf = (char*)zmalloc(total);
    if (hdr == 1) {
      buf[0] = (uint8_t)src.size();  // high bit clear: length is right here
    } else {
      buf[0] = uint8_t(0x80);  // high bit set: real length follows in 4 bytes
      absl::little_endian::Store32(buf + 1, (uint32_t)src.size());
    }
    if (!src.empty())
      memcpy(buf + hdr, src.data(), src.size());
    if (has_ttl) {
      uint32_t at = time_now() + ttl_sec;
      absl::little_endian::Store32(buf + hdr + src.size(), at);
    }
    return buf;
  }
};

// PROTOTYPE: rung 5 of the ladder - byte-for-byte the same as
// CrtpStringSetBlob (devirt + tag + lean blob), except Add() is fused: a
// single FindOrEmptyAround() pass replaces the separate FindInternal
// (existence check) + AddUnique's own FindEmptyAround (insertion-slot
// search), which used to scan the same home-bucket + bid+1/bid-1 window
// twice. Mirrors OAHSet's ProbeWindow, which answers both questions in one
// masked pass. Falls back to the old two-pass path when the table is empty
// or the 3-slot window is already full (FindOrEmptyAround returns
// EntriesEnd()) - same as the slow path always taken before this rung
// existed. AddMany/AddBatch are out of scope for this rung (see context.txt
// section 3) and aren't defined here.
class CrtpStringSetBlobFused : public CrtpDenseSet<CrtpStringSetBlobFused> {
  using Base = CrtpDenseSet<CrtpStringSetBlobFused>;
  friend Base;

  static constexpr uint32_t kShortLenMax = 127;

  static uint32_t BlobLen(const void* blob) {
    uint8_t b0 = *(const uint8_t*)blob;
    if (!(b0 & 0x80))
      return b0;
    return absl::little_endian::Load32((const char*)blob + 1);
  }
  static const char* BlobData(const void* blob) {
    uint8_t b0 = *(const uint8_t*)blob;
    return (const char*)blob + ((b0 & 0x80) ? 5 : 1);
  }
  static size_t BlobHeaderSize(size_t len) {
    return len <= kShortLenMax ? 1 : 5;
  }

 public:
  CrtpStringSetBlobFused() = default;
  ~CrtpStringSetBlobFused() {
    Clear();
  }

  bool Add(std::string_view src, uint32_t ttl_sec = UINT32_MAX) {
    uint64_t hash = Hash(&src, 1);
    if (!Empty()) {
      uint32_t bid = ComputeBucketId(hash);
      // Mirrors AddUnique's own growth-timing check (it only tries
      // FindEmptyAround before growing while size_ < BucketCount(); once
      // the table is at/over that load factor it grows first). Replicated
      // here so the fused path's growth behavior matches the slow path
      // exactly, never growing earlier or later.
      if (CurrentSize() < BucketCount()) {
        auto result = FindOrEmptyAround(&src, bid, 1, uint8_t(hash));
        if (result.found != nullptr)
          return false;
        if (result.slot != EntriesEnd()) {
          void* blob = MakeBlob(src, ttl_sec);
          PlaceNew(result.slot, bid, blob, ttl_sec != UINT32_MAX, hash);
          return true;
        }
      }
    }
    // Slow path: empty table, or the bid/bid+1/bid-1 window was full (so
    // FindOrEmptyAround couldn't place without growing) - same code as
    // CrtpStringSetBlob::Add(), unchanged.
    void* prev = FindInternal(&src, hash, 1);
    if (prev != nullptr)
      return false;
    void* blob = MakeBlob(src, ttl_sec);
    AddUnique(blob, ttl_sec != UINT32_MAX, hash);
    return true;
  }

  bool Erase(std::string_view str) {
    return EraseInternal(&str, 1);
  }

  bool Contains(std::string_view s1) const {
    return FindInternal(&s1, Hash(&s1, 1), 1) != nullptr;
  }

  class iterator : private Base::IteratorBase {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string_view;

    iterator() : IteratorBase() {
    }
    explicit iterator(const IteratorBase& o) : IteratorBase(o) {
    }
    iterator(Base* set) : IteratorBase(set, false) {
    }

    iterator& operator++() {
      Advance();
      return *this;
    }
    bool operator==(const iterator& b) const {
      if (owner_ == nullptr && b.owner_ == nullptr)
        return true;
      return owner_ == b.owner_ && curr_entry_ == b.curr_entry_;
    }
    bool operator!=(const iterator& b) const {
      return !(*this == b);
    }
    value_type operator*() {
      const void* blob = curr_entry_->GetObject();
      return {BlobData(blob), BlobLen(blob)};
    }

    using IteratorBase::ExpiryTime;
    using IteratorBase::HasExpiry;
  };

  iterator begin() {
    return iterator{this};
  }
  iterator end() {
    return iterator{};
  }

  iterator Find(std::string_view member) {
    return iterator{FindIt(&member, 1)};
  }

  // Implementations called via CRTP from CrtpDenseSet (no `override`, no vtable).
  uint64_t Hash(const void* ptr, uint32_t cookie) const {
    if (cookie == 0) {
      return CompactObj::HashCode(std::string_view{BlobData(ptr), BlobLen(ptr)});
    }
    const std::string_view* sv = (const std::string_view*)ptr;
    return CompactObj::HashCode(*sv);
  }

  bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const {
    uint32_t llen = BlobLen(left);
    if (right_cookie == 0) {
      if (llen != BlobLen(right))
        return false;
      return llen == 0 || memcmp(BlobData(left), BlobData(right), llen) == 0;
    }
    const std::string_view* right_sv = (const std::string_view*)right;
    return std::string_view{BlobData(left), llen} == *right_sv;
  }

  size_t ObjectAllocSize(const void* blob) const {
    return zmalloc_usable_size(const_cast<void*>(blob));
  }

  uint32_t ObjExpireTime(const void* blob) const {
    return absl::little_endian::Load32(BlobData(blob) + BlobLen(blob));
  }

  void ObjUpdateExpireTime(const void* blob, uint32_t ttl_sec) {
    char* ttlptr = const_cast<char*>(BlobData(blob)) + BlobLen(blob);
    absl::little_endian::Store32(ttlptr, time_now() + ttl_sec);
  }

  void ObjDelete(void* blob) const {
    zfree(blob);
  }

  void* ObjectClone(const void* blob, bool has_ttl, bool add_ttl) const {
    std::string_view sv{BlobData(blob), BlobLen(blob)};
    uint32_t ttl_sec = add_ttl ? 0 : (has_ttl ? ObjExpireTime(blob) : UINT32_MAX);
    return MakeBlob(sv, ttl_sec);
  }

 private:
  void* MakeBlob(std::string_view src, uint32_t ttl_sec) const {
    const bool has_ttl = ttl_sec != UINT32_MAX;
    const size_t hdr = BlobHeaderSize(src.size());
    const size_t total = hdr + src.size() + (has_ttl ? 4 : 0);
    char* buf = (char*)zmalloc(total);
    if (hdr == 1) {
      buf[0] = (uint8_t)src.size();
    } else {
      buf[0] = uint8_t(0x80);
      absl::little_endian::Store32(buf + 1, (uint32_t)src.size());
    }
    if (!src.empty())
      memcpy(buf + hdr, src.data(), src.size());
    if (has_ttl) {
      uint32_t at = time_now() + ttl_sec;
      absl::little_endian::Store32(buf + hdr + src.size(), at);
    }
    return buf;
  }
};

}  // namespace crtp_bench
}  // namespace dfly
