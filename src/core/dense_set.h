// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstddef>
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
 public:
  bool Reserve(size_t sz);

 protected:
  // Virtual functions to be implemented for generic data
  virtual uint64_t Hash(const void*) const = 0;
  virtual bool Equal(const void*, const void*) const = 0;
  virtual size_t ObjectAllocSize(const void*) const = 0;
  virtual void Clear();

  bool AddInternal(void*);
  bool ContainsInternal(const void*) const;
  void* EraseInternal(void*);
  void* PopInternal();

 private:
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

    bool IsObject() const {
      return (uintptr_t(ptr_) & kLinkBit) == 0;
    }

    bool IsLink() const {
      return (uintptr_t(ptr_) & kLinkBit) == kLinkBit;
    }

    bool IsEmpty() const {
      return ptr_ == nullptr;
    }

    void* Raw() const {
      return (void*)(uintptr_t(ptr_) & ~kTagMask);
    }

    bool IsDisplaced() const {
      return (uintptr_t(ptr_) & kDisplaceBit) == kDisplaceBit;
    }

    void SetLink(DenseLinkKey* lk) {
      ptr_ = (void*)(uintptr_t(lk) | kLinkBit);
    }

    void SetDisplaced(int direction) {
      ptr_ = (void*)(uintptr_t(ptr_) | kDisplaceBit);
      if (direction == 1) {
        ptr_ = (void*)(uintptr_t(ptr_) | kDisplaceDirectionBit);
      }
    }

    void ClearDisplaced() {
      ptr_ = (void*)(uintptr_t(ptr_) & ~kDisplaceBit);
      ptr_ = (void*)(uintptr_t(ptr_) & ~kDisplaceDirectionBit);
    }

    // returns 1 if the displaced node is right of the correct bucket and -1 if it is left
    int GetDisplacedDirection() const {
      return (uintptr_t(ptr_) & kDisplaceDirectionBit) == kDisplaceDirectionBit ? 1 : -1;
    }

    void Reset() {
      ptr_ = nullptr;
    }

    void* GetObject() const {
      if (IsObject())
        return Raw();
      DenseLinkKey* lk = (DenseLinkKey*)Raw();
      return lk->Raw();
    }

    // Sets pointer but preserves tagging info
    void SetObject(void* obj) {
      ptr_ = (void*)((uintptr_t(ptr_) & kTagMask) | (uintptr_t(obj) & ~kTagMask));
    }

    DensePtr* Next() {
      if (!IsLink()) {
        return nullptr;
      }

      return &((DenseLinkKey*)Raw())->next;
    }

    const DensePtr* Next() const {
      if (!IsLink()) {
        return nullptr;
      }

      return &((DenseLinkKey*)Raw())->next;
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

  bool Equal(const DensePtr* dptr, const void* ptr) const {
    if (dptr->IsEmpty()) {
      return false;
    }

    return Equal(dptr->GetObject(), ptr);
  }

  std::pmr::memory_resource* mr() {
    return entries_.get_allocator().resource();
  }

  uint32_t BucketId(uint64_t hash) const {
    return hash >> (64 - capacity_log_);
  }

  uint32_t BucketId(const void* ptr) const {
    return BucketId(Hash(ptr));
  }

  // return a DenseSetList (a.k.a iterator) or end if there is an empty chain found
  ChainVectorIterator FindEmptyAround(uint32_t bid);
  void Grow();

  // ============ Pseudo Linked List Functions for interacting with Chains ==================
  class ChainIterator {
   public:
    ChainIterator(DensePtr* curr = nullptr) : curr_(curr) {
    }

    ChainIterator& operator++();

    DensePtr* operator*() {
      return curr_;
    }

    DensePtr* operator->() {
      return curr_;
    }

    inline bool IsNull() const noexcept {
      return curr_ == nullptr || curr_->IsEmpty();
    }

    bool operator==(const ChainIterator& iter) const {
      return curr_ == iter.curr_ || (this->IsNull() && iter.IsNull());
    }

    bool operator!=(const ChainIterator& iter) const {
      return !(*this == iter);
    }

   private:
    // keep track of the previous pointer in the needed case of unlinking / removing
    DensePtr* curr_;
  };

  class ConstChainIterator {
   public:
    ConstChainIterator(const DensePtr* curr = nullptr) : curr_(curr) {
    }

    ConstChainIterator& operator++();

    const DensePtr* operator*() const {
      return curr_;
    }

    const DensePtr* operator->() const {
      return curr_;
    }

    inline bool IsNull() const noexcept {
      return curr_ == nullptr || curr_->IsEmpty();
    }

    bool operator==(const ConstChainIterator& iter) const {
      return curr_ == iter.curr_ || (this->IsNull() && iter.IsNull());
    }

    bool operator!=(const ConstChainIterator& iter) const {
      return !(*this == iter);
    }

   private:
    const DensePtr* curr_;
  };

  ChainIterator ChainBegin(ChainVectorIterator it) {
    return it == entries_.end() || it->IsEmpty() ? ChainIterator(nullptr) : ChainIterator(&(*it));
  }

  ChainIterator ChainEnd(ChainVectorIterator it) {
    (void)it;
    return ChainIterator(nullptr);
  }

  ChainIterator ChainBegin(size_t idx) {
    return idx >= entries_.size() ? ChainBegin(entries_.end()) : ChainBegin(entries_.begin() + idx);
  }

  ChainIterator ChainEnd(size_t idx) {
    return idx >= entries_.size() ? ChainEnd(entries_.end()) : ChainEnd(entries_.begin() + idx);
  }

  ConstChainIterator ChainConstBegin(ChainVectorConstIterator it) const {
    return it >= entries_.cend() || it->IsEmpty() ? ConstChainIterator(nullptr)
                                                  : ConstChainIterator(&(*it));
  }

  ConstChainIterator ChainConstBegin(size_t idx) const {
    return idx == entries_.size() ? ChainConstBegin(entries_.cend())
                                  : ChainConstBegin(entries_.cbegin() + idx);
  }

  ConstChainIterator ChainConstEnd(ChainVectorConstIterator it) const {
    (void)it;
    return ConstChainIterator(nullptr);
  }

  ConstChainIterator ChainConstEnd(size_t idx) const {
    return idx >= entries_.size() ? ChainConstEnd(entries_.cend())
                                  : ChainConstEnd(entries_.cbegin() + idx);
  }

  bool IsEmpty(ChainVectorIterator) const;

  size_t PushFront(ChainVectorIterator, void*);
  void PushFront(ChainVectorIterator, DensePtr*);

  void* PopFront(ChainVectorIterator);

  ChainIterator Unlink(ChainIterator node, ChainIterator prev, DensePtr* unlinked);

  // Note this will only free the encapsulaing DenseLinkKey and not
  // the data it points to, this will be returned
  ChainIterator UnlinkAndFree(ChainIterator node, ChainIterator prev, void** out);

  DensePtr* Find(void*, ChainVectorIterator, DensePtr** = nullptr);

  // ============ Pseudo Linked List in DenseSet end ==================

  std::pair<ChainIterator, ChainIterator> FindAround(void* ptr, uint32_t bid);

  std::pmr::vector<DensePtr> entries_;
  size_t obj_malloc_used_ = 0;
  uint32_t size_ = 0;
  uint32_t num_chain_entries_ = 0;
  uint32_t num_used_buckets_ = 0;
  unsigned capacity_log_ = 0;

 public:
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
        curr_bucket_ = set->ChainEnd(set->entries_.end());
      } else {
        curr_bucket_ = set->ChainBegin(begin_list);
        // find the first non null entry
        if (curr_bucket_.IsNull()) {
          ++(*this);
        }
      }
    }

    iterator& operator++() {
      ++curr_bucket_;
      while (curr_list_ != set_->entries_.end() && curr_bucket_ == set_->ChainEnd(curr_list_)) {
        ++curr_list_;
        curr_bucket_ = set_->ChainBegin(curr_list_);
      }

      return *this;
    }

    friend bool operator==(const iterator& a, const iterator& b) {
      return a.curr_list_ == b.curr_list_ && a.curr_bucket_ == b.curr_bucket_;
    }

    friend bool operator!=(const iterator& a, const iterator& b) {
      return !(a == b);
    }

    value_type operator*() {
      return (value_type)curr_bucket_->GetObject();
    }

    value_type operator->() {
      return (value_type)curr_bucket_->GetObject();
    }

   private:
    DenseSet* set_;
    ChainVectorIterator curr_list_;
    ChainIterator curr_bucket_;
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
        curr_bucket_ = set->ChainConstEnd(set->entries_.end());
      } else {
        curr_bucket_ = set->ChainConstBegin(begin_list);
        // find the first non null entry
        if (curr_bucket_.IsNull()) {
          ++(*this);
        }
      }
    }

    const_iterator& operator++() {
      ++curr_bucket_;
      while (curr_list_ != set_->entries_.end() &&
             curr_bucket_ == set_->ChainConstEnd(curr_list_)) {
        ++curr_list_;
        curr_bucket_ = set_->ChainConstBegin(curr_list_);
      }

      return *this;
    }

    friend bool operator==(const const_iterator& a, const const_iterator& b) {
      return a.curr_list_ == b.curr_list_ && a.curr_bucket_ == b.curr_bucket_;
    }

    friend bool operator!=(const const_iterator& a, const const_iterator& b) {
      return !(a == b);
    }

    value_type operator*() const {
      return (value_type)curr_bucket_->GetObject();
    }

    value_type operator->() const {
      return (value_type)curr_bucket_->GetObject();
    }

   private:
    const DenseSet* set_;
    ChainVectorConstIterator curr_list_;
    ConstChainIterator curr_bucket_;
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

  DenseSet(const DenseSet&) = delete;
  explicit DenseSet(std::pmr::memory_resource* mr = std::pmr::get_default_resource());
  virtual ~DenseSet();
  DenseSet& operator=(DenseSet&) = delete;
};

}  // namespace dfly
