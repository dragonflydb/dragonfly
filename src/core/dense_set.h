// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <vector>

#include "base/pmr/memory_resource.h"

namespace dfly {

// DenseSet is a nice but over-optimized data-structure. Probably is not worth it in the first
// place but sometimes the OCD kicks in and one can not resist.
// The advantage of it over redis-dict is smaller meta-data waste.
// dictEntry is 24 bytes, i.e it uses at least 32N bytes where N is the expected length.
// dict requires to allocate dictEntry per each addition in addition to the supplied key.
// It also wastes space in case of a set because it stores a value pointer inside dictEntry.
// To summarize:
// 100% utilized dict uses N*24 + N*8 = 32N bytes not including the key space.
// for 75% utilization (1/0.75 buckets): N*1.33*8 + N*24 = 35N
//
// This class uses 8 bytes per bucket (similarly to dictEntry*) but it used it for both
// links and keys. For most cases, we remove the need for another redirection layer
// and just store the key, so no "dictEntry" allocations occur.
// For those cells that require chaining, the bucket is
// changed in run-time to represent a linked chain.
// Additional feature - in order to to reduce collisions, we insert items into
// neighbour cells but only if they are empty (not chains). This way we reduce the number of
// empty (unused) spaces at full utilization from 36% to ~21%.
// 100% utilized table requires: N*8 + 0.2N*16 = 11.2N bytes or ~20 bytes savings.
// 75% utilization: N*1.33*8 + 0.12N*16 = 13N or ~22 bytes savings per record.
// with potential replacements of hset/zset data structures.
// static_assert(sizeof(dictEntry) == 24);

class DenseSet {
  struct DenseLinkKey;
  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  static constexpr size_t kLinkBit = 1ULL << 52;
  static constexpr size_t kDisplaceBit = 1ULL << 53;
  static constexpr size_t kDisplaceDirectionBit = 1ULL << 54;
  static constexpr size_t kTtlBit = 1ULL << 55;
  static constexpr size_t kTagMask = 4095ULL << 52;  // we reserve 12 high bits.

  class DensePtr {
   public:
    explicit DensePtr(void* p = nullptr) : ptr_(p) {
    }

    // Imports the object with its metadata except the link bit that is reset.
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

    // returns 1 if the displaced node is right of the correct bucket and -1 if it is left
    int GetDisplacedDirection() const {
      return (uptr() & kDisplaceDirectionBit) == kDisplaceDirectionBit ? 1 : -1;
    }

    void SetTtl(bool b) {
      if (b)
        ptr_ = (void*)(uptr() | kTtlBit);
      else
        ptr_ = (void*)(uptr() & (~kTtlBit));
    }

    void Reset() {
      ptr_ = nullptr;
    }

    void* GetObject() const {
      if (IsObject()) {
        return Raw();
      }

      return AsLink()->Raw();
    }

    // Sets pointer but preserves tagging info
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
      if (!IsLink()) {
        return nullptr;
      }

      return &AsLink()->next;
    }

    const DensePtr* Next() const {
      if (!IsLink()) {
        return nullptr;
      }

      return &AsLink()->next;
    }

   private:
    void* ptr_ = nullptr;
  };

  struct DenseLinkKey : public DensePtr {
    DensePtr next;  // could be LinkKey* or Object *.
  };

  static_assert(sizeof(DensePtr) == sizeof(uintptr_t));
  static_assert(sizeof(DenseLinkKey) == 2 * sizeof(uintptr_t));

 protected:
  using LinkAllocator = PMR_NS::polymorphic_allocator<DenseLinkKey>;
  using DensePtrAllocator = PMR_NS::polymorphic_allocator<DensePtr>;
  using ChainVectorIterator = std::vector<DensePtr, DensePtrAllocator>::iterator;
  using ChainVectorConstIterator = std::vector<DensePtr, DensePtrAllocator>::const_iterator;

  class IteratorBase {
    friend class DenseSet;

   public:
    IteratorBase(DenseSet* owner, ChainVectorIterator list_it, DensePtr* e)
        : owner_(owner), curr_list_(list_it), curr_entry_(e) {
    }

    // returns the expiry time of the current entry or UINT32_MAX if no ttl is set.
    uint32_t ExpiryTime() const {
      return curr_entry_->HasTtl() ? owner_->ObjExpireTime(curr_entry_->GetObject()) : UINT32_MAX;
    }

    void SetExpiryTime(uint32_t ttl_sec);

    bool HasExpiry() const {
      return curr_entry_->HasTtl();
    }

   protected:
    IteratorBase() : owner_(nullptr), curr_entry_(nullptr) {
    }

    IteratorBase(const DenseSet* owner, bool is_end);

    void Advance();

    DenseSet* owner_;
    ChainVectorIterator curr_list_;
    DensePtr* curr_entry_;
  };

 public:
  using MemoryResource = PMR_NS::memory_resource;
  static constexpr uint32_t kMaxBatchLen = 32;

  explicit DenseSet(MemoryResource* mr = PMR_NS::get_default_resource());
  virtual ~DenseSet();

  void Clear() {
    ClearStep(0, entries_.size());
  }

  // Returns the next bucket index that should be cleared.
  // Returns BucketCount when all objects are erased.
  uint32_t ClearStep(uint32_t start, uint32_t count);

  // Returns the number of elements in the map. Note that it might be that some of these elements
  // have expired and can't be accessed.
  size_t UpperBoundSize() const {
    return size_;
  }

  // Returns an accurate size, post-expiration. O(n).
  size_t SizeSlow();

  bool Empty() const {
    return size_ == 0;
  }

  size_t BucketCount() const {
    return entries_.size();
  }

  size_t NumUsedBuckets() const {
    return num_used_buckets_;
  }

  size_t ObjMallocUsed() const {
    return obj_malloc_used_;
  }

  size_t SetMallocUsed() const {
    return entries_.capacity() * sizeof(DensePtr) + num_links_ * sizeof(DenseLinkKey);
  }

  using ItemCb = std::function<void(const void*)>;

  uint32_t Scan(uint32_t cursor, const ItemCb& cb) const;
  void Reserve(size_t sz);

  void Fill(DenseSet* other) const;

  // set an abstract time that allows expiry.
  void set_time(uint32_t val) {
    time_now_ = val;
  }

  uint32_t time_now() const {
    return time_now_;
  }

  bool ExpirationUsed() const {
    return expiration_used_;
  }

 protected:
  // Virtual functions to be implemented for generic data
  virtual uint64_t Hash(const void* obj, uint32_t cookie) const = 0;
  virtual bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const = 0;
  virtual size_t ObjectAllocSize(const void* obj) const = 0;
  virtual uint32_t ObjExpireTime(const void* obj) const = 0;
  virtual void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) = 0;
  virtual void ObjDelete(void* obj, bool has_ttl) const = 0;
  virtual void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const = 0;

  void CollectExpired();

  bool EraseInternal(void* obj, uint32_t cookie) {
    auto [prev, found] = Find(obj, BucketId(obj, cookie), cookie);
    if (found) {
      Delete(prev, found);
      return true;
    }
    return false;
  }

  void* FindInternal(const void* obj, uint64_t hashcode, uint32_t cookie) const;

  IteratorBase FindIt(const void* ptr, uint32_t cookie) {
    if (Empty())
      return IteratorBase{};

    auto [bid, _, curr] = Find2(ptr, BucketId(ptr, cookie), cookie);
    if (curr) {
      return IteratorBase(this, entries_.begin() + bid, curr);
    }
    return IteratorBase{};
  }

  void* PopInternal();

  void IncreaseMallocUsed(size_t delta) {
    obj_malloc_used_ += delta;
  }

  void DecreaseMallocUsed(size_t delta) {
    obj_malloc_used_ -= delta;
  }

  // Returns the previous object if it has been replaced.
  // nullptr, if obj was added.
  void* AddOrReplaceObj(void* obj, bool has_ttl);

  // Assumes that the object does not exist in the set.
  void AddUnique(void* obj, bool has_ttl, uint64_t hashcode);

  void Prefetch(uint64_t hash);

 private:
  DenseSet(const DenseSet&) = delete;
  DenseSet& operator=(DenseSet&) = delete;

  bool Equal(DensePtr dptr, const void* ptr, uint32_t cookie) const;

  struct CloneItem {
    DensePtr ptr;
    void* obj = nullptr;
    bool has_ttl = false;
  };

  void CloneBatch(unsigned len, CloneItem* items, DenseSet* other) const;

  using ClearItem = CloneItem;
  void ClearBatch(unsigned len, ClearItem* items);

  MemoryResource* mr() {
    return entries_.get_allocator().resource();
  }

  uint32_t BucketId(uint64_t hash) const {
    assert(capacity_log_ > 0);
    return hash >> (64 - capacity_log_);
  }

  uint32_t BucketId(const void* ptr, uint32_t cookie) const {
    return BucketId(Hash(ptr, cookie));
  }

  // return a ChainVectorIterator (a.k.a iterator) or end if there is an empty chain found
  ChainVectorIterator FindEmptyAround(uint32_t bid);

  // Return if bucket has no item which is not displaced and right/left bucket has no displaced item
  // belong to given bid
  bool NoItemBelongsBucket(uint32_t bid) const;
  void Grow(size_t prev_size);

  // ============ Pseudo Linked List Functions for interacting with Chains ==================
  size_t PushFront(ChainVectorIterator, void* obj, bool has_ttl);
  void PushFront(ChainVectorIterator, DensePtr);

  DensePtr PopPtrFront(ChainVectorIterator);

  // ============ Pseudo Linked List in DenseSet end ==================

  // returns (prev, item) pair. If item is root, then prev is null.
  std::pair<DensePtr*, DensePtr*> Find(const void* ptr, uint32_t bid, uint32_t cookie) {
    auto [_, p, c] = Find2(ptr, bid, cookie);
    return {p, c};
  }

  // returns bid and (prev, item) pair. If item is root, then prev is null.
  std::tuple<size_t, DensePtr*, DensePtr*> Find2(const void* ptr, uint32_t bid, uint32_t cookie);

  DenseLinkKey* NewLink(void* data, DensePtr next);

  inline void FreeLink(DenseLinkKey* plink) {
    // deallocate the link if it is no longer a link as it is now in an empty list
    mr()->deallocate(plink, sizeof(DenseLinkKey), alignof(DenseLinkKey));
    --num_links_;
  }

  // Returns true if *node was deleted.
  bool ExpireIfNeeded(DensePtr* prev, DensePtr* node) const {
    if (node->HasTtl()) {
      return ExpireIfNeededInternal(prev, node);
    }
    return false;
  }

  bool ExpireIfNeededInternal(DensePtr* prev, DensePtr* node) const;

  // Deletes the object pointed by ptr and removes it from the set.
  // If ptr is a link then it will be deleted internally.
  void Delete(DensePtr* prev, DensePtr* ptr);

  std::vector<DensePtr, DensePtrAllocator> entries_;

  mutable size_t obj_malloc_used_ = 0;
  mutable uint32_t size_ = 0;              // number of elements in the set.
  mutable uint32_t num_links_ = 0;         // number of links in the set.
  mutable uint32_t num_used_buckets_ = 0;  // number of buckets used in entries_ array.
  unsigned capacity_log_ = 0;

  uint32_t time_now_ = 0;

  mutable bool expiration_used_ = false;
};

inline void* DenseSet::FindInternal(const void* obj, uint64_t hashcode, uint32_t cookie) const {
  if (entries_.empty())
    return nullptr;

  uint32_t bid = BucketId(hashcode);
  DensePtr* ptr = const_cast<DenseSet*>(this)->Find(obj, bid, cookie).second;
  return ptr ? ptr->GetObject() : nullptr;
}

}  // namespace dfly
