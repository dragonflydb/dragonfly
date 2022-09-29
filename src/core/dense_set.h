// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory_resource>
#include <type_traits>

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
  // https://www.kernel.org/doc/html/latest/arm64/memory.html
  static constexpr size_t kLinkBit = 1ULL << 52;
  static constexpr size_t kDisplaceBit = 1ULL << 53;
  static constexpr size_t kDisplaceDirectionBit = 1ULL << 54;
  static constexpr size_t kTagMask = 4095ULL << 51;  // we reserve 12 high bits.

  struct DensePtr {
    explicit DensePtr(void* p = nullptr) : ptr_(p) {
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
    void* ptr_ = nullptr;  //
  };

  struct DenseLinkKey : public DensePtr {
    DensePtr next;  // could be LinkKey* or Object *.
  };

  static_assert(sizeof(DensePtr) == sizeof(uintptr_t));
  static_assert(sizeof(DenseLinkKey) == 2 * sizeof(uintptr_t));

  using LinkAllocator = std::pmr::polymorphic_allocator<DenseLinkKey>;
  using ChainVectorIterator = std::pmr::vector<DensePtr>::iterator;
  using ChainVectorConstIterator = std::pmr::vector<DensePtr>::const_iterator;

 public:
  explicit DenseSet(std::pmr::memory_resource* mr = std::pmr::get_default_resource());
  virtual ~DenseSet();

  size_t Size() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

  size_t BucketCount() const {
    return entries_.size();
  }

  // those that are chained to the entries stored inline in the bucket array.
  size_t NumChainEntries() const {
    return num_chain_entries_;
  }

  size_t NumUsedBuckets() const {
    return num_used_buckets_;
  }

  size_t ObjMallocUsed() const {
    return obj_malloc_used_;
  }

  size_t SetMallocUsed() const {
    return (num_chain_entries_ + entries_.capacity()) * sizeof(DensePtr);
  }

  template <typename T> class iterator {
    static_assert(std::is_pointer_v<T>, "Iterators can only return pointers");

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using pointer = value_type*;
    using reference = value_type&;

    iterator(DenseSet* set, ChainVectorIterator begin_list) : set_(set), curr_list_(begin_list) {
      if (begin_list == set->entries_.end()) {
        curr_entry_ = nullptr;
      } else {
        curr_entry_ = &*begin_list;
        // find the first non null entry
        if (curr_entry_ == nullptr || curr_entry_->IsEmpty()) {
          ++(*this);
        }
      }
    }

    iterator& operator++() {
      curr_entry_ = curr_entry_->Next();
      while (curr_list_ != set_->entries_.end() &&
             (curr_entry_ == nullptr || curr_entry_->IsEmpty())) {
        ++curr_list_;
        curr_entry_ = &*curr_list_;
      }

      return *this;
    }

    friend bool operator==(const iterator& a, const iterator& b) {
      return a.curr_list_ == b.curr_list_;
    }

    friend bool operator!=(const iterator& a, const iterator& b) {
      return !(a == b);
    }

    value_type operator*() {
      return (value_type)curr_entry_->GetObject();
    }

    value_type operator->() {
      return (value_type)curr_entry_->GetObject();
    }

   private:
    DenseSet* set_;
    ChainVectorIterator curr_list_;
    DensePtr* curr_entry_;
  };

  template <typename T> class const_iterator {
    static_assert(std::is_pointer_v<T>, "Iterators can only return pointer types");

   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = const T;
    using pointer = value_type*;
    using reference = value_type&;

    const_iterator(const DenseSet* set, ChainVectorConstIterator begin_list)
        : set_(set), curr_list_(begin_list) {
      if (begin_list == set->entries_.end()) {
        curr_entry_ = nullptr;
      } else {
        curr_entry_ = &*begin_list;
        // find the first non null entry
        if (curr_entry_ == nullptr || curr_entry_->IsEmpty()) {
          ++(*this);
        }
      }
    }

    const_iterator& operator++() {
      curr_entry_ = curr_entry_->Next();
      curr_entry_ = curr_entry_->Next();
      while (curr_list_ != set_->entries_.end() &&
             (curr_entry_ == nullptr || curr_entry_->IsEmpty())) {
        ++curr_list_;
        curr_entry_ = &*curr_list_;
      }
      return *this;
    }

    friend bool operator==(const const_iterator& a, const const_iterator& b) {
      return a.curr_list_ == b.curr_list_;
    }

    friend bool operator!=(const const_iterator& a, const const_iterator& b) {
      return !(a == b);
    }

    value_type operator*() const {
      return (value_type)curr_entry_->GetObject();
    }

    value_type operator->() const {
      return (value_type)curr_entry_->GetObject();
    }

   private:
    const DenseSet* set_;
    ChainVectorConstIterator curr_list_;
    const DensePtr* curr_entry_;
  };

  template <typename T> iterator<T> begin() {
    return iterator<T>(this, entries_.begin());
  }

  template <typename T> iterator<T> end() {
    return iterator<T>(this, entries_.end());
  }

  template <typename T> const_iterator<T> cbegin() const {
    return const_iterator<T>(this, entries_.cbegin());
  }

  template <typename T> const_iterator<T> cend() const {
    return const_iterator<T>(this, entries_.cend());
  }

  using ItemCb = std::function<void(const void*)>;

  uint32_t Scan(uint32_t cursor, const ItemCb& cb) const;
  void Reserve(size_t sz);

 protected:
  // Virtual functions to be implemented for generic data
  virtual uint64_t Hash(const void* obj) const = 0;
  virtual bool ObjEqual(const void* obj1, const void* obj2) const = 0;
  virtual size_t ObjectAllocSize(const void* obj) const = 0;

  void* EraseInternal(void* obj) {
    DensePtr* found = Find(obj);
    return found ? Delete(found) : nullptr;
  }

  bool AddInternal(void* obj);

  bool ContainsInternal(const void* obj) const {
    return Find(obj, BucketId(obj)) != nullptr;
  }

  void* PopInternal();

  // Note this does not free any dynamic allocations done by derived classes, that a DensePtr
  // in the set may point to. This function only frees the allocated DenseLinkKeys created by
  // DenseSet. All data allocated by a derived class should be freed before calling this
  void ClearInternal();

 private:
  DenseSet(const DenseSet&) = delete;
  DenseSet& operator=(DenseSet&) = delete;

  bool Equal(DensePtr dptr, const void* ptr) const;

  std::pmr::memory_resource* mr() {
    return entries_.get_allocator().resource();
  }

  uint32_t BucketId(uint64_t hash) const {
    return hash >> (64 - capacity_log_);
  }

  uint32_t BucketId(const void* ptr) const {
    return BucketId(Hash(ptr));
  }

  // return a ChainVectorIterator (a.k.a iterator) or end if there is an empty chain found
  ChainVectorIterator FindEmptyAround(uint32_t bid);
  void Grow();

  // ============ Pseudo Linked List Functions for interacting with Chains ==================
  size_t PushFront(ChainVectorIterator, void*);
  void PushFront(ChainVectorIterator, DensePtr);

  void* PopDataFront(ChainVectorIterator);
  DensePtr PopPtrFront(ChainVectorIterator);

  // Note this function will modify the iterator passed to it
  // to point to the next node in the chain
  DensePtr Unlink(DensePtr* node);

  // ============ Pseudo Linked List in DenseSet end ==================

  const DensePtr* Find(const void* ptr, uint32_t bid) const;

  const DensePtr* Find(const void* ptr) const {
    return Find(ptr, BucketId(ptr));
  }

  DensePtr* Find(const void* ptr, uint32_t bid) {
    const DensePtr* ret = const_cast<const DenseSet*>(this)->Find(ptr, bid);
    return const_cast<DensePtr*>(ret);
  }

  DensePtr* Find(const void* ptr) {
    const DensePtr* ret = const_cast<const DenseSet*>(this)->Find(ptr);
    return const_cast<DensePtr*>(ret);
  }

  DenseLinkKey* NewLink(void* data, DensePtr next);

  inline void FreeLink(DensePtr link) {
    // deallocate the link if it is no longer a link as it is now in an empty list
    mr()->deallocate(link.AsLink(), sizeof(DenseLinkKey), alignof(DenseLinkKey));
  }

  void* Delete(DensePtr* ptr);

  // We may update it during const operations due to expiry interactions.
  std::pmr::vector<DensePtr> entries_;

  size_t obj_malloc_used_ = 0;
  uint32_t size_ = 0;
  uint32_t num_chain_entries_ = 0;
  uint32_t num_used_buckets_ = 0;
  unsigned capacity_log_ = 0;
};

}  // namespace dfly
