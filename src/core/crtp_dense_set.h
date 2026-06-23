// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// PROTOTYPE: byte-for-byte copy of DenseSet, CRTP-ified to remove virtual dispatch.
// Used only to A/B benchmark "virtual calls" vs "static dispatch" against the original
// DenseSet/StringSet. Not for production use.
#pragma once

#include <absl/numeric/bits.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <vector>

#include "base/logging.h"
#include "core/detail/stateless_allocator.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

namespace crtp_bench {

inline thread_local absl::InsecureBitGen tl_bit_gen2;

#ifndef CRTP_PREFETCH_READ
#define CRTP_PREFETCH_READ(x) __builtin_prefetch(x, 0, 1)
#endif

// Derived must implement (non-virtual):
//   uint64_t Hash(const void* obj, uint32_t cookie) const;
//   bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const;
//   size_t ObjectAllocSize(const void* obj) const;
//   uint32_t ObjExpireTime(const void* obj) const;
//   void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec);
//   void ObjDelete(void* obj) const;
//   void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const;
template <typename Derived, bool kHashTagEnabled = true> class CrtpDenseSet {
 protected:
  struct DenseLinkKey;

  static constexpr size_t kLinkBit = 1ULL << 52;
  static constexpr size_t kDisplaceBit = 1ULL << 53;
  static constexpr size_t kDisplaceDirectionBit = 1ULL << 54;
  static constexpr size_t kTtlBit = 1ULL << 55;
  static constexpr size_t kTagMask = 4095ULL << 52;

  // Cached low-8-bits-of-hash "tag" for a quick reject before dereferencing the
  // object. Uses bits 56-63, distinct from the structural tag bits above
  // (52-55). Sourced from the *low* end of the hash, which BucketId never
  // touches (it reads from the high end: hash >> (64 - capacity_log_)), so a
  // tag computed at insert time never needs invalidating on Grow/Shrink.
  static constexpr size_t kHashTagShift = 56;
  static constexpr uint64_t kHashTagMask = 0xFFULL << kHashTagShift;

  class DensePtr {
   public:
    explicit DensePtr(void* p = nullptr) : ptr_(p) {
    }

    static DensePtr From(DenseLinkKey* o) {
      DensePtr res;
      res.ptr_ = (void*)(o->uptr() & (~kLinkBit));
      return res;
    }

    uint64_t uptr() const {
      return uint64_t(ptr_);
    }
    bool IsObject() const {
      return (uptr() & kLinkBit) == 0;
    }
    bool IsLink() const {
      return (uptr() & kLinkBit) != 0;
    }
    bool HasTtl() const {
      return (uptr() & kTtlBit) != 0;
    }
    bool IsEmpty() const {
      return ptr_ == nullptr;
    }
    void* Raw() const {
      return (void*)(uptr() & ~kTagMask);
    }
    bool IsDisplaced() const {
      return (uptr() & kDisplaceBit) == kDisplaceBit;
    }
    void SetLink(DenseLinkKey* lk) {
      ptr_ = (void*)(uintptr_t(lk) | kLinkBit);
    }
    void SetDisplaced(int direction) {
      ptr_ = (void*)(uptr() | kDisplaceBit);
      if (direction == 1) {
        ptr_ = (void*)(uptr() | kDisplaceDirectionBit);
      }
    }
    void ClearDisplaced() {
      ptr_ = (void*)(uptr() & ~(kDisplaceBit | kDisplaceDirectionBit));
    }
    int GetDisplacedDirection() const {
      return (uptr() & kDisplaceDirectionBit) == kDisplaceDirectionBit ? 1 : -1;
    }
    void SetTtl(bool b) {
      if (b)
        ptr_ = (void*)(uptr() | kTtlBit);
      else
        ptr_ = (void*)(uptr() & (~kTtlBit));
    }
    void SetHashTag(uint8_t tag) {
      ptr_ = (void*)((uptr() & ~kHashTagMask) | (uint64_t(tag) << kHashTagShift));
    }
    uint8_t GetHashTag() const {
      return uint8_t((uptr() & kHashTagMask) >> kHashTagShift);
    }
    void Reset() {
      ptr_ = nullptr;
    }
    void* GetObject() const {
      if (IsObject())
        return Raw();
      return AsLink()->Raw();
    }
    void SetObject(void* obj) {
      assert(IsObject());
      ptr_ = (void*)((uptr() & kTagMask) | (uintptr_t(obj) & ~kTagMask));
    }
    DenseLinkKey* AsLink() {
      return (DenseLinkKey*)Raw();
    }
    const DenseLinkKey* AsLink() const {
      return (const DenseLinkKey*)Raw();
    }
    DensePtr* Next() {
      if (!IsLink())
        return nullptr;
      return &AsLink()->next;
    }
    const DensePtr* Next() const {
      if (!IsLink())
        return nullptr;
      return &AsLink()->next;
    }

   private:
    void* ptr_ = nullptr;
  };

  struct DenseLinkKey : public DensePtr {
    DensePtr next;
  };

  static_assert(sizeof(DensePtr) == sizeof(uintptr_t));
  static_assert(sizeof(DenseLinkKey) == 2 * sizeof(uintptr_t));

  using DensePtrAllocator = StatelessAllocator<DensePtr>;
  using ChainVectorIterator = typename std::vector<DensePtr, DensePtrAllocator>::iterator;

  class IteratorBase {
    friend class CrtpDenseSet;

   public:
    IteratorBase(CrtpDenseSet* owner, ChainVectorIterator list_it, DensePtr* e)
        : owner_(owner), curr_list_(list_it), curr_entry_(e) {
    }

    uint32_t ExpiryTime() const {
      return curr_entry_->HasTtl() ? owner_->self()->ObjExpireTime(curr_entry_->GetObject())
                                   : UINT32_MAX;
    }

    bool HasExpiry() const {
      return curr_entry_->HasTtl();
    }

   protected:
    IteratorBase() : owner_(nullptr), curr_entry_(nullptr) {
    }

    IteratorBase(const CrtpDenseSet* owner, bool is_end)
        : owner_(const_cast<CrtpDenseSet*>(owner)), curr_entry_(nullptr) {
      curr_list_ = is_end ? owner_->entries_.end() : owner_->entries_.begin();
      if (curr_list_ == owner->entries_.end()) {
        curr_entry_ = nullptr;
        owner_ = nullptr;
      } else {
        curr_entry_ = &(*curr_list_);
        owner->ExpireIfNeeded(nullptr, curr_entry_);
        if (curr_entry_->IsEmpty())
          Advance();
      }
    }

    void Advance() {
      bool step_link = false;
      DCHECK(curr_entry_);
      if (curr_entry_->IsLink()) {
        DenseLinkKey* plink = curr_entry_->AsLink();
        if (!owner_->ExpireIfNeeded(curr_entry_, &plink->next) || curr_entry_->IsLink()) {
          curr_entry_ = &plink->next;
          step_link = true;
        }
      }
      if (!step_link) {
        DCHECK(curr_list_ != owner_->entries_.end());
        do {
          ++curr_list_;
          if (curr_list_ == owner_->entries_.end()) {
            curr_entry_ = nullptr;
            owner_ = nullptr;
            return;
          }
          owner_->ExpireIfNeeded(nullptr, &(*curr_list_));
        } while (curr_list_->IsEmpty());
        DCHECK(curr_list_ != owner_->entries_.end());
        curr_entry_ = &(*curr_list_);
      }
      DCHECK(!curr_entry_->IsEmpty());
    }

    CrtpDenseSet* owner_;
    ChainVectorIterator curr_list_;
    DensePtr* curr_entry_;
  };

 public:
  static constexpr uint32_t kMaxBatchLen = 32;

  CrtpDenseSet() {
    static_assert(sizeof(entries_) == 24);
  }

  ~CrtpDenseSet() {
    CHECK(entries_.empty());
  }

  void Clear() {
    ClearStep(0, entries_.size());
  }

  uint32_t ClearStep(uint32_t start, uint32_t count) {
    constexpr unsigned kArrLen = 32;
    ClearItem arr[kArrLen];
    unsigned len = 0;
    size_t end = std::min<size_t>(entries_.size(), start + count);
    for (size_t i = start; i < end; ++i) {
      DensePtr& ptr = entries_[i];
      if (ptr.IsEmpty())
        continue;
      auto& dest = arr[len++];
      dest.has_ttl = ptr.HasTtl();
      CRTP_PREFETCH_READ(ptr.Raw());
      if (ptr.IsObject()) {
        dest.obj = ptr.Raw();
        dest.ptr.Reset();
      } else {
        dest.ptr = ptr;
        dest.obj = nullptr;
      }
      ptr.Reset();
      if (len == kArrLen) {
        ClearBatch(kArrLen, arr);
        len = 0;
      }
    }
    ClearBatch(len, arr);
    if (size_ == 0) {
      entries_.clear();
      num_links_ = 0;
      obj_malloc_used_ = 0;
    }
    return end;
  }

  size_t UpperBoundSize() const {
    return size_;
  }
  bool Empty() const {
    return size_ == 0;
  }
  size_t BucketCount() const {
    return entries_.size();
  }
  size_t ObjMallocUsed() const {
    return obj_malloc_used_;
  }
  size_t SetMallocUsed() const {
    return entries_.capacity() * sizeof(DensePtr) + num_links_ * sizeof(DenseLinkKey);
  }

  void Reserve(size_t sz) {
    sz = std::max<size_t>(sz, kMinSize);
    sz = absl::bit_ceil(sz);
    if (sz > entries_.size()) {
      size_t prev_size = entries_.size();
      entries_.resize(sz);
      capacity_log_ = absl::bit_width(sz) - 1;
      Grow(prev_size);
    }
  }

  void set_time(uint32_t val) {
    time_now_ = val;
  }
  uint32_t time_now() const {
    return time_now_;
  }

 protected:
  Derived* self() {
    return static_cast<Derived*>(this);
  }
  const Derived* self() const {
    return static_cast<const Derived*>(this);
  }

  bool EraseInternal(void* obj, uint32_t cookie) {
    const uint64_t hash = self()->Hash(obj, cookie);
    auto [prev, found] = Find(obj, BucketId(hash), cookie, hash);
    if (found) {
      Delete(prev, found);
      return true;
    }
    return false;
  }

  void* FindInternal(const void* obj, uint64_t hashcode, uint32_t cookie) const {
    if (entries_.empty())
      return nullptr;
    uint32_t bid = BucketId(hashcode);
    DensePtr* ptr = const_cast<CrtpDenseSet*>(this)->Find(obj, bid, cookie, hashcode).second;
    return ptr ? ptr->GetObject() : nullptr;
  }

  IteratorBase FindIt(const void* ptr, uint32_t cookie) {
    if (Empty())
      return IteratorBase{};
    const uint64_t hash = self()->Hash(ptr, cookie);
    auto [bid, p, curr] = Find2(ptr, BucketId(hash), cookie, hash);
    (void)p;
    if (curr) {
      return IteratorBase(this, entries_.begin() + bid, curr);
    }
    return IteratorBase{};
  }

  void IncreaseMallocUsed(size_t delta) {
    obj_malloc_used_ += delta;
  }
  void DecreaseMallocUsed(size_t delta) {
    obj_malloc_used_ -= delta;
  }

  void* AddOrReplaceObj(void* obj, bool has_ttl) {
    uint64_t hc = self()->Hash(obj, 0);
    DensePtr* dptr = entries_.empty() ? nullptr : Find(obj, BucketId(hc), 0, hc).second;
    if (dptr) {
      dptr->SetTtl(has_ttl);
      if (dptr->IsLink())
        dptr = dptr->AsLink();
      void* res = dptr->Raw();
      const size_t res_sz = self()->ObjectAllocSize(res);
      obj_malloc_used_ -= res_sz;
      obj_malloc_used_ += self()->ObjectAllocSize(obj);
      dptr->SetObject(obj);
      if constexpr (kHashTagEnabled) {
        dptr->SetHashTag(uint8_t(hc));
      }
      return res;
    }
    AddUnique(obj, has_ttl, hc);
    return nullptr;
  }

  void AddUnique(void* obj, bool has_ttl, uint64_t hashcode) {
    if (entries_.empty()) {
      capacity_log_ = kMinSizeShift;
      entries_.resize(kMinSize);
    }
    uint32_t bucket_id = BucketId(hashcode);

    for (unsigned j = 0; j < 2; ++j) {
      ChainVectorIterator list = FindEmptyAround(bucket_id);
      if (list != entries_.end()) {
        obj_malloc_used_ += PushFront(list, obj, has_ttl, hashcode);
        if (std::distance(entries_.begin(), list) != bucket_id) {
          list->SetDisplaced(std::distance(entries_.begin() + bucket_id, list));
        }
        ++size_;
        return;
      }
      if (size_ < entries_.size())
        break;
      size_t prev_size = entries_.size();
      entries_.resize(prev_size * 2);
      ++capacity_log_;
      Grow(prev_size);
      bucket_id = BucketId(hashcode);
    }

    DensePtr to_insert(obj);
    // Freshly constructed - unlike `unlinked` below (an existing, already-
    // tagged entry being evicted further), this wraps the brand-new object
    // being inserted and has no tag yet. PushFront(DensePtr) now trusts
    // ptr.GetHashTag() instead of recomputing, so this must be set explicitly.
    if constexpr (kHashTagEnabled) {
      to_insert.SetHashTag(uint8_t(hashcode));
    }
    if (has_ttl)
      to_insert.SetTtl(true);

    while (!entries_[bucket_id].IsEmpty() && entries_[bucket_id].IsDisplaced()) {
      DensePtr unlinked = PopPtrFront(entries_.begin() + bucket_id);
      PushFront(entries_.begin() + bucket_id, to_insert);
      to_insert = unlinked;
      bucket_id -= unlinked.GetDisplacedDirection();
    }

    ChainVectorIterator list = entries_.begin() + bucket_id;
    PushFront(list, to_insert);
    obj_malloc_used_ += self()->ObjectAllocSize(obj);
    ++size_;
  }

  void Prefetch(uint64_t hash) {
    uint32_t bid = BucketId(hash);
    CRTP_PREFETCH_READ(&entries_[bid]);
  }

  // FUSION: result of a single combined pass that answers both "does this
  // key already exist" and "where would it go if not" - the two questions
  // AddUnique used to answer via two separate scans (FindInternal, then its
  // own FindEmptyAround). `found` is non-null on a duplicate. Otherwise
  // `slot` is the empty slot to insert into, or EntriesEnd() if the bid/
  // bid+1/bid-1 window was fully occupied (caller falls back to the slow
  // FindInternal + AddUnique path, same as if this were never called).
  struct FindOrSlotResult {
    void* found;
    ChainVectorIterator slot;
  };

  size_t CurrentSize() const {
    return size_;
  }
  ChainVectorIterator EntriesEnd() {
    return entries_.end();
  }
  uint32_t ComputeBucketId(uint64_t hash) const {
    return BucketId(hash);
  }

  // Single pass over the home bucket (+ its link chain) and the bid+1/bid-1
  // displacement neighbors - the same window FindEmptyAround and Find2 each
  // scan separately today. Mirrors OAHSet's ProbeWindow: one masked answer
  // covering both "did we match" and "where's empty", instead of two passes
  // over the same cache lines.
  FindOrSlotResult FindOrEmptyAround(const void* ptr, uint32_t bid, uint32_t cookie,
                                     uint8_t query_tag) {
    ExpireIfNeeded(nullptr, &entries_[bid]);
    if (entries_[bid].IsEmpty()) {
      return {nullptr, entries_.begin() + bid};
    }
    if (Equal(entries_[bid], ptr, cookie, query_tag)) {
      return {entries_[bid].GetObject(), entries_.end()};
    }
    // Duplicates chained off the home bucket (rather than displaced into a
    // neighbor slot) only show up here.
    DensePtr* prev = &entries_[bid];
    DensePtr* curr = prev->Next();
    while (curr != nullptr) {
      if (ExpireIfNeeded(prev, curr)) {
        if (!prev->IsLink())
          break;
      }
      if (Equal(*curr, ptr, cookie, query_tag))
        return {curr->GetObject(), entries_.end()};
      prev = curr;
      curr = curr->Next();
    }

    ChainVectorIterator empty_slot = entries_.end();
    if (bid + 1 < entries_.size()) {
      DensePtr& nb = entries_[bid + 1];
      ExpireIfNeeded(nullptr, &nb);
      if (nb.IsEmpty()) {
        empty_slot = entries_.begin() + bid + 1;
      } else if (nb.IsDisplaced() && nb.GetDisplacedDirection() == 1 &&
                 Equal(nb, ptr, cookie, query_tag)) {
        return {nb.GetObject(), entries_.end()};
      }
    }
    if (bid > 0) {
      DensePtr& nb = entries_[bid - 1];
      ExpireIfNeeded(nullptr, &nb);
      if (nb.IsEmpty()) {
        if (empty_slot == entries_.end())
          empty_slot = entries_.begin() + bid - 1;
      } else if (nb.IsDisplaced() && nb.GetDisplacedDirection() == -1 &&
                 Equal(nb, ptr, cookie, query_tag)) {
        return {nb.GetObject(), entries_.end()};
      }
    }
    return {nullptr, empty_slot};
  }

  // Does the PushFront + displacement-bit bookkeeping + size_ increment that
  // AddUnique's FindEmptyAround-success branch does, given a slot already
  // located by FindOrEmptyAround.
  void PlaceNew(ChainVectorIterator slot, uint32_t bid, void* obj, bool has_ttl,
                uint64_t hashcode) {
    obj_malloc_used_ += PushFront(slot, obj, has_ttl, hashcode);
    if (std::distance(entries_.begin(), slot) != static_cast<ptrdiff_t>(bid)) {
      slot->SetDisplaced(std::distance(entries_.begin() + bid, slot));
    }
    ++size_;
  }

 private:
  static constexpr size_t kMinSizeShift = 2;
  static constexpr size_t kMinSize = 1 << kMinSizeShift;
  static constexpr bool kAllowDisplacements = true;

  struct CloneItem {
    DensePtr ptr;
    void* obj = nullptr;
    bool has_ttl = false;
  };
  using ClearItem = CloneItem;

  uint32_t BucketId(uint64_t hash) const {
    return hash >> (64 - capacity_log_);
  }
  uint32_t BucketId(const void* ptr, uint32_t cookie) const {
    return BucketId(self()->Hash(ptr, cookie));
  }

  bool Equal(DensePtr dptr, const void* ptr, uint32_t cookie, uint8_t query_tag) const {
    if (dptr.IsEmpty())
      return false;
    // Quick reject on the cached hash tag before touching the object's own
    // (likely cold) heap allocation. Compiled out entirely when
    // kHashTagEnabled is false, so the no-tag rung pays zero extra cost.
    if constexpr (kHashTagEnabled) {
      if (dptr.GetHashTag() != query_tag)
        return false;
    } else {
      (void)query_tag;
    }
    return self()->ObjEqual(dptr.GetObject(), ptr, cookie);
  }

  ChainVectorIterator FindEmptyAround(uint32_t bid) {
    ExpireIfNeeded(nullptr, &entries_[bid]);
    if (entries_[bid].IsEmpty())
      return entries_.begin() + bid;
    if (!kAllowDisplacements)
      return entries_.end();
    if (bid + 1 < entries_.size()) {
      auto it = std::next(entries_.begin(), bid + 1);
      ExpireIfNeeded(nullptr, &(*it));
      if (it->IsEmpty())
        return it;
    }
    if (bid) {
      auto it = std::next(entries_.begin(), bid - 1);
      ExpireIfNeeded(nullptr, &(*it));
      if (it->IsEmpty())
        return it;
    }
    return entries_.end();
  }

  // `hashcode` is the caller's already-computed hash (it's always derived
  // from the same hash used to pick `it`'s bucket) - never recompute it here.
  size_t PushFront(ChainVectorIterator it, void* data, bool has_ttl, uint64_t hashcode) {
    const uint8_t tag = uint8_t(hashcode);
    if (it->IsEmpty()) {
      it->SetObject(data);
    } else {
      it->SetLink(NewLink(data, *it, tag));
    }
    if constexpr (kHashTagEnabled) {
      it->SetHashTag(tag);
    }
    if (has_ttl)
      it->SetTtl(true);
    return self()->ObjectAllocSize(data);
  }

  void PushFront(ChainVectorIterator it, DensePtr ptr) {
    if (it->IsEmpty()) {
      it->SetObject(ptr.GetObject());
      if (ptr.HasTtl())
        it->SetTtl(true);
      if (ptr.IsLink())
        FreeLink(ptr.AsLink());
    } else if (ptr.IsLink()) {
      *ptr.Next() = *it;
      *it = ptr;
    } else {
      it->SetLink(NewLink(ptr.Raw(), *it, ptr.GetHashTag()));
      if (ptr.HasTtl())
        it->SetTtl(true);
    }
    // `ptr` is the relocated entry's own prior state, so its tag is already
    // correct (it was set when the entry was first created and never
    // changes - tag bits come from the low end of the hash, which BucketId
    // never touches, so a resize/relocate never invalidates it). Some
    // branches above already copy it wholesale (`*it = ptr`); this just
    // makes it unconditional and explicit across all three branches, with
    // no hash recomputation needed. Compiled out when kHashTagEnabled is
    // false (ptr.GetHashTag() would just be reading always-zero bits).
    if constexpr (kHashTagEnabled) {
      it->SetHashTag(ptr.GetHashTag());
    }
  }

  DensePtr PopPtrFront(ChainVectorIterator it) {
    if (it->IsEmpty())
      return DensePtr{};
    DensePtr front = *it;
    if (it->IsObject()) {
      it->Reset();
    } else {
      DenseLinkKey* link = it->AsLink();
      *it = link->next;
    }
    return front;
  }

  // `hash` must be the same hash the caller used to derive `bid` - never
  // recomputed here, since that would be a second O(key length) hash call
  // on every single lookup/erase.
  std::pair<DensePtr*, DensePtr*> Find(const void* ptr, uint32_t bid, uint32_t cookie,
                                       uint64_t hash) {
    auto [b, p, c] = Find2(ptr, bid, cookie, hash);
    (void)b;
    return {p, c};
  }

  std::tuple<size_t, DensePtr*, DensePtr*> Find2(const void* ptr, uint32_t bid, uint32_t cookie,
                                                 uint64_t hash) {
    const uint8_t query_tag = uint8_t(hash);

    DensePtr* curr = &entries_[bid];
    ExpireIfNeeded(nullptr, curr);
    if (Equal(*curr, ptr, cookie, query_tag))
      return {bid, nullptr, curr};

    if (bid > 0) {
      curr = &entries_[bid - 1];
      if (curr->IsDisplaced() && curr->GetDisplacedDirection() == -1) {
        ExpireIfNeeded(nullptr, curr);
        if (Equal(*curr, ptr, cookie, query_tag))
          return {bid - 1, nullptr, curr};
      }
    }
    if (bid + 1 < entries_.size()) {
      curr = &entries_[bid + 1];
      if (curr->IsDisplaced() && curr->GetDisplacedDirection() == 1) {
        ExpireIfNeeded(nullptr, curr);
        if (Equal(*curr, ptr, cookie, query_tag))
          return {bid + 1, nullptr, curr};
      }
    }

    DensePtr* prev = &entries_[bid];
    curr = prev->Next();
    while (curr != nullptr) {
      if (ExpireIfNeeded(prev, curr)) {
        if (!prev->IsLink())
          break;
      }
      if (Equal(*curr, ptr, cookie, query_tag))
        return {bid, prev, curr};
      prev = curr;
      curr = curr->Next();
    }
    return {0, nullptr, nullptr};
  }

  void* Delete(DensePtr* prev, DensePtr* ptr, bool detach = false) {
    void* obj = nullptr;
    if (ptr->IsObject()) {
      obj = ptr->Raw();
      ptr->Reset();
      if (prev) {
        DenseLinkKey* plink = prev->AsLink();
        DensePtr tmp = DensePtr::From(plink);
        tmp.SetTtl(prev->HasTtl());
        FreeLink(plink);
        *prev = tmp;
      }
    } else {
      DenseLinkKey* link = ptr->AsLink();
      obj = link->Raw();
      *ptr = link->next;
      FreeLink(link);
    }
    obj_malloc_used_ -= self()->ObjectAllocSize(obj);
    --size_;
    if (detach)
      return obj;
    self()->ObjDelete(obj);
    return nullptr;
  }

  void Grow(size_t prev_size) {
    DensePtr first;
    if (entries_.front().IsDisplaced()) {
      first = PopPtrFront(entries_.begin());
    }
    for (long i = (long)prev_size - 1; i >= 0; --i) {
      DensePtr* curr = &entries_[i];
      DensePtr* prev = nullptr;
      do {
        if (ExpireIfNeeded(prev, curr)) {
          if (prev && !prev->IsLink())
            break;
        }
        if (curr->IsEmpty())
          break;
        void* ptr = curr->GetObject();
        uint32_t bid = BucketId(ptr, 0);
        if (bid == (uint32_t)i) {
          curr->ClearDisplaced();
          prev = curr;
          curr = curr->Next();
          if (curr == nullptr)
            break;
        } else {
          auto dest = entries_.begin() + bid;
          DensePtr dptr = *curr;
          if (curr->IsObject()) {
            if (prev) {
              DenseLinkKey* plink = prev->AsLink();
              DensePtr tmp = DensePtr::From(plink);
              tmp.SetTtl(prev->HasTtl());
              FreeLink(plink);
              curr = nullptr;
              *prev = tmp;
            } else {
              curr->Reset();
            }
          } else {
            *curr = *dptr.Next();
          }
          PushFront(dest, dptr);
        }
      } while (curr);
    }
    if (!first.IsEmpty()) {
      uint32_t bid = BucketId(first.GetObject(), 0);
      PushFront(entries_.begin() + bid, first);
    }
  }

  void CloneBatch(unsigned len, CloneItem* items, CrtpDenseSet* other) const {
    auto clone = [this](void* obj, bool has_ttl, CrtpDenseSet* other) {
      void* new_obj = other->self()->ObjectClone(obj, has_ttl, false);
      uint64_t hash = self()->Hash(obj, 0);
      other->AddUnique(new_obj, has_ttl, hash);
    };
    while (len) {
      unsigned dest_id = 0;
      for (unsigned i = 0; i < len; ++i) {
        auto& src = items[i];
        if (src.obj) {
          clone(src.obj, src.has_ttl, other);
          src.obj = nullptr;
        }
        if (src.ptr.IsEmpty())
          continue;
        if (src.ptr.IsObject()) {
          clone(src.ptr.Raw(), src.has_ttl, other);
        } else {
          auto& dest = items[dest_id++];
          DenseLinkKey* link = src.ptr.AsLink();
          dest.obj = link->Raw();
          dest.has_ttl = src.ptr.HasTtl();
          dest.ptr = link->next;
        }
      }
      len = dest_id;
    }
  }

  void ClearBatch(unsigned len, ClearItem* items) {
    while (len) {
      unsigned dest_id = 0;
      for (unsigned i = 0; i < len; ++i) {
        auto& src = items[i];
        if (src.obj) {
          self()->ObjDelete(src.obj);
          --size_;
          src.obj = nullptr;
        }
        if (src.ptr.IsEmpty())
          continue;
        if (src.ptr.IsObject()) {
          self()->ObjDelete(src.ptr.Raw());
          --size_;
        } else {
          auto& dest = items[dest_id++];
          DenseLinkKey* link = src.ptr.AsLink();
          dest.obj = link->Raw();
          dest.has_ttl = src.ptr.HasTtl();
          dest.ptr = link->next;
          FreeLink(link);
        }
      }
      len = dest_id;
    }
  }

 public:
  void Fill(CrtpDenseSet* other) const {
    other->Reserve(UpperBoundSize());
    constexpr unsigned kArrLen = 32;
    CloneItem arr[kArrLen];
    unsigned len = 0;
    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
      DensePtr ptr = *it;
      if (ptr.IsEmpty())
        continue;
      auto& item = arr[len++];
      item.has_ttl = ptr.HasTtl();
      if (ptr.IsObject()) {
        item.ptr.Reset();
        item.obj = ptr.Raw();
      } else {
        item.ptr = ptr;
        item.obj = nullptr;
      }
      if (len == kArrLen) {
        CloneBatch(kArrLen, arr, other);
        len = 0;
      }
    }
    CloneBatch(len, arr, other);
  }

 private:
  DenseLinkKey* NewLink(void* data, DensePtr next, uint8_t tag) {
    using LinkAllocator = StatelessAllocator<DenseLinkKey>;
    LinkAllocator la;
    DenseLinkKey* lk = la.allocate(1);
    la.construct(lk);
    lk->next = next;
    lk->SetObject(data);
    // The link's own base-DensePtr identity must carry data's tag too, not
    // just the slot that points at this link - DensePtr::From(lk) (used by
    // Delete/Grow to collapse a link back into a direct object) copies these
    // bits forward, so an unset tag here would silently corrupt the entry's
    // cached tag the next time this link is collapsed. `tag` is passed in by
    // the caller, who already has the hash in hand - recomputing it here via
    // self()->Hash(data, 0) would be a second, redundant O(key length) hash
    // call on every single insert.
    if constexpr (kHashTagEnabled) {
      lk->SetHashTag(tag);
    } else {
      (void)tag;
    }
    ++num_links_;
    return lk;
  }

  void FreeLink(DenseLinkKey* plink) {
    DensePtrAllocator::resource()->deallocate(plink, sizeof(DenseLinkKey), alignof(DenseLinkKey));
    --num_links_;
  }

  bool ExpireIfNeeded(DensePtr* prev, DensePtr* node) const {
    if (node->HasTtl()) {
      return ExpireIfNeededInternal(prev, node);
    }
    return false;
  }

  bool ExpireIfNeededInternal(DensePtr* prev, DensePtr* node) const {
    bool deleted = false;
    do {
      uint32_t obj_time = self()->ObjExpireTime(node->GetObject());
      if (obj_time > time_now_)
        break;
      const bool node_in_prev_link = prev != nullptr && node->IsObject();
      const_cast<CrtpDenseSet*>(this)->Delete(prev, node);
      deleted = true;
      if (node_in_prev_link)
        break;
    } while (node->HasTtl());
    return deleted;
  }

  std::vector<DensePtr, DensePtrAllocator> entries_;
  mutable size_t obj_malloc_used_ = 0;
  mutable uint32_t size_ = 0;
  mutable uint32_t num_links_ = 0;
  unsigned capacity_log_ = 0;
  uint32_t time_now_ = 0;
};

}  // namespace crtp_bench
}  // namespace dfly
