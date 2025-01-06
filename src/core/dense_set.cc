// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/dense_set.h"

#include <absl/numeric/bits.h>

#include <cstddef>
#include <cstdint>
#include <stack>
#include <type_traits>
#include <vector>

#include "base/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {
using namespace std;

constexpr size_t kMinSizeShift = 2;
constexpr size_t kMinSize = 1 << kMinSizeShift;
constexpr bool kAllowDisplacements = true;

#define PREFETCH_READ(x) __builtin_prefetch(x, 0, 1)

DenseSet::IteratorBase::IteratorBase(const DenseSet* owner, bool is_end)
    : owner_(const_cast<DenseSet*>(owner)), curr_entry_(nullptr) {
  curr_list_ = is_end ? owner_->entries_.end() : owner_->entries_.begin();

  // Even if `is_end` is `false`, the list can be empty.
  if (curr_list_ == owner->entries_.end()) {
    curr_entry_ = nullptr;
    owner_ = nullptr;
  } else {
    curr_entry_ = &(*curr_list_);
    owner->ExpireIfNeeded(nullptr, curr_entry_);

    // find the first non null entry
    if (curr_entry_->IsEmpty()) {
      Advance();
    }
  }
}

void DenseSet::IteratorBase::SetExpiryTime(uint32_t ttl_sec) {
  DensePtr* ptr = curr_entry_->IsLink() ? curr_entry_->AsLink() : curr_entry_;
  void* src = ptr->GetObject();
  if (!HasExpiry()) {
    void* new_obj = owner_->ObjectClone(src, false, true);
    ptr->SetObject(new_obj);

    // Important: we set the ttl bit on the wrapping pointer.
    curr_entry_->SetTtl(true);
    owner_->ObjDelete(src, false);
    src = new_obj;
  }
  owner_->ObjUpdateExpireTime(src, ttl_sec);
}

void DenseSet::IteratorBase::Advance() {
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

DenseSet::DenseSet(MemoryResource* mr) : entries_(mr) {
}

DenseSet::~DenseSet() {
  // We can not call Clear from the base class because it internally calls ObjDelete which is
  // a virtual function. Therefore, destructor of the derived classes must clean up the table.
  CHECK(entries_.empty());
}

size_t DenseSet::PushFront(DenseSet::ChainVectorIterator it, void* data, bool has_ttl) {
  // if this is an empty list assign the value to the empty placeholder pointer
  DCHECK(!it->IsDisplaced());
  if (it->IsEmpty()) {
    it->SetObject(data);
  } else {
    // otherwise make a new link and connect it to the front of the list
    it->SetLink(NewLink(data, *it));
  }

  if (has_ttl) {
    it->SetTtl(true);
    expiration_used_ = true;
  }
  return ObjectAllocSize(data);
}

void DenseSet::PushFront(DenseSet::ChainVectorIterator it, DenseSet::DensePtr ptr) {
  DVLOG(2) << "PushFront to " << distance(entries_.begin(), it) << ", "
           << ObjectAllocSize(ptr.GetObject());
  DCHECK(!it->IsDisplaced());

  if (it->IsEmpty()) {
    it->SetObject(ptr.GetObject());
    if (ptr.HasTtl()) {
      it->SetTtl(true);
      expiration_used_ = true;
    }
    if (ptr.IsLink()) {
      FreeLink(ptr.AsLink());
    }
  } else if (ptr.IsLink()) {
    // if the pointer is already a link then no allocation needed.
    *ptr.Next() = *it;
    *it = ptr;
    DCHECK(!it->AsLink()->next.IsEmpty());
  } else {
    DCHECK(ptr.IsObject());

    // allocate a new link if needed and copy the pointer to the new link
    it->SetLink(NewLink(ptr.Raw(), *it));
    if (ptr.HasTtl()) {
      it->SetTtl(true);
      expiration_used_ = true;
    }
    DCHECK(!it->AsLink()->next.IsEmpty());
  }
}

auto DenseSet::PopPtrFront(DenseSet::ChainVectorIterator it) -> DensePtr {
  if (it->IsEmpty()) {
    return DensePtr{};
  }

  DensePtr front = *it;

  // if this is an object, then it's also the only record in this chain.
  // therefore, we should just reset DensePtr.
  if (it->IsObject()) {
    it->Reset();
  } else {
    DCHECK(it->IsLink());
    DenseLinkKey* link = it->AsLink();
    *it = link->next;
  }

  return front;
}

uint32_t DenseSet::ClearStep(uint32_t start, uint32_t count) {
  constexpr unsigned kArrLen = 32;
  ClearItem arr[kArrLen];
  unsigned len = 0;

  size_t end = min<size_t>(entries_.size(), start + count);
  for (size_t i = start; i < end; ++i) {
    DensePtr& ptr = entries_[i];
    if (ptr.IsEmpty())
      continue;

    auto& dest = arr[len++];
    dest.has_ttl = ptr.HasTtl();

    PREFETCH_READ(ptr.Raw());
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
    num_used_buckets_ = 0;
    num_links_ = 0;
    expiration_used_ = false;
  }
  return end;
}

bool DenseSet::Equal(DensePtr dptr, const void* ptr, uint32_t cookie) const {
  if (dptr.IsEmpty()) {
    return false;
  }

  return ObjEqual(dptr.GetObject(), ptr, cookie);
}

void DenseSet::CloneBatch(unsigned len, CloneItem* items, DenseSet* other) const {
  // We handle a batch of items to minimize data dependencies when accessing memory for a single
  // item. We prefetch the memory for entire batch before actually reading data from any of the
  // elements.

  auto clone = [this](void* obj, bool has_ttl, DenseSet* other) {
    // The majority of the CPU is spent in this block.
    void* new_obj = other->ObjectClone(obj, has_ttl, false);
    uint64_t hash = this->Hash(obj, 0);
    other->AddUnique(new_obj, has_ttl, hash);
  };

  while (len) {
    unsigned dest_id = 0;
    // we walk "len" linked lists in parallel, and prefetch their next, obj pointers
    // before actually processing them.
    for (unsigned i = 0; i < len; ++i) {
      auto& src = items[i];
      if (src.obj) {
        clone(src.obj, src.has_ttl, other);
        src.obj = nullptr;
      }

      if (src.ptr.IsEmpty()) {
        continue;
      }

      if (src.ptr.IsObject()) {
        clone(src.ptr.Raw(), src.has_ttl, other);
      } else {
        auto& dest = items[dest_id++];
        DenseLinkKey* link = src.ptr.AsLink();
        dest.obj = link->Raw();
        DCHECK(!link->HasTtl());

        // ttl is attached to the wrapping pointer.
        dest.has_ttl = src.ptr.HasTtl();
        dest.ptr = link->next;
        PREFETCH_READ(dest.ptr.Raw());
        PREFETCH_READ(dest.obj);
      }
    }

    // update the length of the batch for the next iteration.
    len = dest_id;
  }
}

void DenseSet::ClearBatch(unsigned len, ClearItem* items) {
  while (len) {
    unsigned dest_id = 0;
    // we walk "len" linked lists in parallel, and prefetch their next, obj pointers
    // before actually processing them.
    for (unsigned i = 0; i < len; ++i) {
      auto& src = items[i];
      if (src.obj) {
        ObjDelete(src.obj, src.has_ttl);
        --size_;
        src.obj = nullptr;
      }

      if (src.ptr.IsEmpty())
        continue;

      if (src.ptr.IsObject()) {
        ObjDelete(src.ptr.Raw(), src.has_ttl);
        --size_;
      } else {
        auto& dest = items[dest_id++];
        DenseLinkKey* link = src.ptr.AsLink();
        DCHECK(!link->HasTtl());
        dest.obj = link->Raw();
        dest.has_ttl = src.ptr.HasTtl();
        dest.ptr = link->next;
        PREFETCH_READ(dest.ptr.Raw());
        PREFETCH_READ(dest.obj);
        FreeLink(link);
      }
    }

    // update the length of the batch for the next iteration.
    len = dest_id;
  }
}
bool DenseSet::NoItemBelongsBucket(uint32_t bid) const {
  auto& entries = const_cast<DenseSet*>(this)->entries_;
  DensePtr* curr = &entries[bid];
  ExpireIfNeeded(nullptr, curr);
  if (!curr->IsEmpty() && !curr->IsDisplaced()) {
    return false;
  }

  if (bid + 1 < entries_.size()) {
    DensePtr* right_bucket = &entries[bid + 1];
    ExpireIfNeeded(nullptr, right_bucket);
    if (!right_bucket->IsEmpty() && right_bucket->IsDisplaced() &&
        right_bucket->GetDisplacedDirection() == 1)
      return false;
  }

  if (bid > 0) {
    DensePtr* left_bucket = &entries[bid - 1];
    ExpireIfNeeded(nullptr, left_bucket);
    if (!left_bucket->IsEmpty() && left_bucket->IsDisplaced() &&
        left_bucket->GetDisplacedDirection() == -1)
      return false;
  }
  return true;
}

auto DenseSet::FindEmptyAround(uint32_t bid) -> ChainVectorIterator {
  ExpireIfNeeded(nullptr, &entries_[bid]);

  if (entries_[bid].IsEmpty()) {
    return entries_.begin() + bid;
  }

  if (!kAllowDisplacements) {
    return entries_.end();
  }

  if (bid + 1 < entries_.size()) {
    auto it = next(entries_.begin(), bid + 1);
    ExpireIfNeeded(nullptr, &(*it));
    if (it->IsEmpty())
      return it;
  }

  if (bid) {
    auto it = next(entries_.begin(), bid - 1);
    ExpireIfNeeded(nullptr, &(*it));
    if (it->IsEmpty())
      return it;
  }

  return entries_.end();
}

void DenseSet::Reserve(size_t sz) {
  sz = std::max<size_t>(sz, kMinSize);

  sz = absl::bit_ceil(sz);
  if (sz > entries_.size()) {
    size_t prev_size = entries_.size();
    entries_.resize(sz);
    capacity_log_ = absl::bit_width(sz) - 1;
    Grow(prev_size);
  }
}

void DenseSet::Fill(DenseSet* other) const {
  DCHECK(other->entries_.empty());

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
      PREFETCH_READ(item.obj);
    } else {
      item.ptr = ptr;
      item.obj = nullptr;
      PREFETCH_READ(item.ptr.Raw());
    }

    if (len == kArrLen) {
      CloneBatch(kArrLen, arr, other);
      len = 0;
    }
  }
  CloneBatch(len, arr, other);
}

void DenseSet::Grow(size_t prev_size) {
  DensePtr first;

  // Corner case. Usually elements are moved to higher buckets during rehashing.
  // By moving upper elements first we make sure that there are no displaced elements
  // when we move the lower elements.
  // However the (displaced) elements at bucket_id=1 can move to bucket 0, and
  // bucket 0 can host displaced elements from bucket 1. To avoid this situation, we
  // stash the displaced element from bucket 0 and move it to the correct bucket at the end.
  if (entries_.front().IsDisplaced()) {
    first = PopPtrFront(entries_.begin());
  }

  // perform rehashing of items in the array, chain by chain.
  for (long i = prev_size - 1; i >= 0; --i) {
    DensePtr* curr = &entries_[i];
    DensePtr* prev = nullptr;

    do {
      if (ExpireIfNeeded(prev, curr)) {
        // if curr has disappeared due to expiry and prev was converted from Link to a
        // regular DensePtr
        if (prev && !prev->IsLink())
          break;
      }

      if (curr->IsEmpty())
        break;
      void* ptr = curr->GetObject();

      DCHECK(ptr != nullptr && ObjectAllocSize(ptr));

      uint32_t bid = BucketId(ptr, 0);

      // if the item does not move from the current chain, ensure
      // it is not marked as displaced and move to the next item in the chain
      if (bid == i) {
        curr->ClearDisplaced();
        prev = curr;
        curr = curr->Next();
        if (curr == nullptr)
          break;
      } else {
        // if the entry is in the wrong chain remove it and
        // add it to the correct chain. This will also correct
        // displaced entries
        auto dest = entries_.begin() + bid;
        DensePtr dptr = *curr;

        if (curr->IsObject()) {
          if (prev) {
            DCHECK(prev->IsLink());

            DenseLinkKey* plink = prev->AsLink();
            DCHECK(&plink->next == curr);

            // we want to make *prev a DensePtr instead of DenseLink and we
            // want to deallocate the link.
            DensePtr tmp = DensePtr::From(plink);

            // Important to transfer the ttl flag.
            tmp.SetTtl(prev->HasTtl());
            DCHECK(ObjectAllocSize(tmp.GetObject()));

            FreeLink(plink);
            // we deallocated the link, curr is invalid now.
            curr = nullptr;
            *prev = tmp;
          } else {
            // prev == nullptr
            curr->Reset();  // reset the root placeholder.
          }
        } else {
          // !curr.IsObject
          *curr = *dptr.Next();
          DCHECK(!curr->IsEmpty());
        }

        DVLOG(2) << " Pushing to " << bid << " " << dptr.GetObject();
        DCHECK_EQ(BucketId(dptr.GetObject(), 0), bid);
        PushFront(dest, dptr);
      }
    } while (curr);
  }
  if (!first.IsEmpty()) {
    uint32_t bid = BucketId(first.GetObject(), 0);
    PushFront(entries_.begin() + bid, first);
  }
}

// Assumes that the object does not exist in the set.
void DenseSet::AddUnique(void* obj, bool has_ttl, uint64_t hashcode) {
  if (entries_.empty()) {
    capacity_log_ = kMinSizeShift;
    entries_.resize(kMinSize);
  }

  uint32_t bucket_id = BucketId(hashcode);

  DCHECK_LT(bucket_id, entries_.size());

  // Try insert into flat surface first. Also handle the grow case
  // if utilization is too high.
  for (unsigned j = 0; j < 2; ++j) {
    ChainVectorIterator list = FindEmptyAround(bucket_id);
    if (list != entries_.end()) {
      obj_malloc_used_ += PushFront(list, obj, has_ttl);
      if (std::distance(entries_.begin(), list) != bucket_id) {
        list->SetDisplaced(std::distance(entries_.begin() + bucket_id, list));
      }
      ++num_used_buckets_;
      ++size_;
      return;
    }

    if (size_ < entries_.size()) {
      break;
    }

    size_t prev_size = entries_.size();
    entries_.resize(prev_size * 2);
    ++capacity_log_;

    Grow(prev_size);
    bucket_id = BucketId(hashcode);
  }

  DCHECK(!entries_[bucket_id].IsEmpty());

  /**
   * Since the current entry is not empty, it is either a valid chain
   * or there is a displaced node here. In the latter case it is best to
   * move the displaced node to its correct bucket. However there could be
   * a displaced node there and so forth. Keep to avoid having to keep a stack
   * of displacements we can keep track of the current displaced node, add it
   * to the correct chain, and if the correct chain contains a displaced node
   * unlink it and repeat the steps
   */

  DensePtr to_insert(obj);
  if (has_ttl) {
    to_insert.SetTtl(true);
    expiration_used_ = true;
  }

  while (!entries_[bucket_id].IsEmpty() && entries_[bucket_id].IsDisplaced()) {
    DensePtr unlinked = PopPtrFront(entries_.begin() + bucket_id);

    PushFront(entries_.begin() + bucket_id, to_insert);
    to_insert = unlinked;
    bucket_id -= unlinked.GetDisplacedDirection();
  }

  DCHECK_EQ(BucketId(to_insert.GetObject(), 0), bucket_id);
  ChainVectorIterator list = entries_.begin() + bucket_id;
  PushFront(list, to_insert);
  obj_malloc_used_ += ObjectAllocSize(obj);
  DCHECK(!entries_[bucket_id].IsDisplaced());

  ++size_;
}

void DenseSet::Prefetch(uint64_t hash) {
  uint32_t bid = BucketId(hash);
  PREFETCH_READ(&entries_[bid]);
}

auto DenseSet::Find2(const void* ptr, uint32_t bid, uint32_t cookie)
    -> tuple<size_t, DensePtr*, DensePtr*> {
  DCHECK_LT(bid, entries_.size());

  DensePtr* curr = &entries_[bid];
  ExpireIfNeeded(nullptr, curr);

  if (Equal(*curr, ptr, cookie)) {
    return {bid, nullptr, curr};
  }

  // first look for displaced nodes since this is quicker than iterating a potential long chain
  if (bid > 0) {
    curr = &entries_[bid - 1];
    if (curr->IsDisplaced() && curr->GetDisplacedDirection() == -1) {
      ExpireIfNeeded(nullptr, curr);

      if (Equal(*curr, ptr, cookie)) {
        return {bid - 1, nullptr, curr};
      }
    }
  }

  if (bid + 1 < entries_.size()) {
    curr = &entries_[bid + 1];
    if (curr->IsDisplaced() && curr->GetDisplacedDirection() == 1) {
      ExpireIfNeeded(nullptr, curr);

      if (Equal(*curr, ptr, cookie)) {
        return {bid + 1, nullptr, curr};
      }
    }
  }

  // if the node is not displaced, search the correct chain
  DensePtr* prev = &entries_[bid];
  curr = prev->Next();
  while (curr != nullptr) {
    ExpireIfNeeded(prev, curr);

    if (Equal(*curr, ptr, cookie)) {
      return {bid, prev, curr};
    }
    prev = curr;
    curr = curr->Next();
  }

  // not in the Set
  return {0, nullptr, nullptr};
}

void DenseSet::Delete(DensePtr* prev, DensePtr* ptr) {
  void* obj = nullptr;

  if (ptr->IsObject()) {
    obj = ptr->Raw();
    ptr->Reset();
    if (prev == nullptr) {
      --num_used_buckets_;
    } else {
      DCHECK(prev->IsLink());

      DenseLinkKey* plink = prev->AsLink();
      DensePtr tmp = DensePtr::From(plink);
      DCHECK(ObjectAllocSize(tmp.GetObject()));

      FreeLink(plink);
      *prev = tmp;
      DCHECK(!prev->IsLink());
    }
  } else {
    DCHECK(ptr->IsLink());

    DenseLinkKey* link = ptr->AsLink();
    obj = link->Raw();
    *ptr = link->next;
    FreeLink(link);
  }

  obj_malloc_used_ -= ObjectAllocSize(obj);
  --size_;
  ObjDelete(obj, false);
}

void* DenseSet::PopInternal() {
  ChainVectorIterator bucket_iter = entries_.begin();

  // find the first non-empty chain
  do {
    while (bucket_iter != entries_.end() && bucket_iter->IsEmpty()) {
      ++bucket_iter;
    }

    // empty set
    if (bucket_iter == entries_.end()) {
      return nullptr;
    }

    ExpireIfNeeded(nullptr, &(*bucket_iter));
  } while (bucket_iter->IsEmpty());

  if (bucket_iter->IsObject()) {
    --num_used_buckets_;
  }

  // unlink the first node in the first non-empty chain
  obj_malloc_used_ -= ObjectAllocSize(bucket_iter->GetObject());

  DensePtr front = PopPtrFront(bucket_iter);
  void* ret = front.GetObject();

  if (front.IsLink()) {
    FreeLink(front.AsLink());
  }

  --size_;
  return ret;
}

void* DenseSet::AddOrReplaceObj(void* obj, bool has_ttl) {
  uint64_t hc = Hash(obj, 0);
  DensePtr* dptr = entries_.empty() ? nullptr : Find(obj, BucketId(hc), 0).second;

  if (dptr) {  // replace existing object.
    // A bit confusing design: ttl bit is located on the wrapping pointer,
    // therefore we must set ttl bit before unrapping below.
    dptr->SetTtl(has_ttl);

    if (dptr->IsLink())  // unwrap the pointer.
      dptr = dptr->AsLink();

    void* res = dptr->Raw();
    obj_malloc_used_ -= ObjectAllocSize(res);
    obj_malloc_used_ += ObjectAllocSize(obj);

    dptr->SetObject(obj);

    return res;
  }

  AddUnique(obj, has_ttl, hc);
  return nullptr;
}

/**
 * stable scanning api. has the same guarantees as redis scan command.
 * we avoid doing bit-reverse by using a different function to derive a bucket id
 * from hash values. By using msb part of hash we make it "stable" with respect to
 * rehashes. For example, with table log size 4 (size 16), entries in bucket id
 * 1110 come from hashes 1110XXXXX.... When a table grows to log size 5,
 * these entries can move either to 11100 or 11101. So if we traversed with our cursor
 * range [0000-1110], it's guaranteed that in grown table we do not need to cover again
 * [00000-11100]. Similarly with shrinkage, if a table is shrunk to log size 3,
 * keys from 1110 and 1111 will move to bucket 111. Again, it's guaranteed that we
 * covered the range [000-111] (all keys in that case).
 * Returns: next cursor or 0 if reached the end of scan.
 * cursor = 0 - initiates a new scan.
 */

uint32_t DenseSet::Scan(uint32_t cursor, const ItemCb& cb) const {
  // empty set
  if (capacity_log_ == 0) {
    return 0;
  }

  uint32_t entries_idx = cursor >> (32 - capacity_log_);

  auto& entries = const_cast<DenseSet*>(this)->entries_;

  // First find the bucket to scan, skip empty buckets.
  // A bucket is empty if the current index is empty and the data is not displaced
  // to the right or to the left.
  while (entries_idx < entries_.size() && NoItemBelongsBucket(entries_idx)) {
    ++entries_idx;
  }

  if (entries_idx == entries_.size()) {
    return 0;
  }

  DensePtr* curr = &entries[entries_idx];

  // Check home bucket
  if (!curr->IsEmpty() && !curr->IsDisplaced()) {
    // scanning add all entries in a given chain
    while (true) {
      cb(curr->GetObject());
      if (!curr->IsLink())
        break;

      DensePtr* mcurr = const_cast<DensePtr*>(curr);

      if (ExpireIfNeeded(mcurr, &mcurr->AsLink()->next) && !mcurr->IsLink()) {
        break;
      }
      curr = &curr->AsLink()->next;
    }
  }

  // Check if the bucket on the left belongs to the home bucket.
  if (entries_idx > 0) {
    DensePtr* left_bucket = &entries[entries_idx - 1];
    ExpireIfNeeded(nullptr, left_bucket);

    if (left_bucket->IsDisplaced() &&
        left_bucket->GetDisplacedDirection() == -1) {  // left of the home bucket
      cb(left_bucket->GetObject());
    }
  }

  // move to the next index for the next scan and check if we are done
  ++entries_idx;
  if (entries_idx >= entries_.size()) {
    return 0;
  }

  // Check if the bucket on the right belongs to the home bucket.
  DensePtr* right_bucket = &entries[entries_idx];
  ExpireIfNeeded(nullptr, right_bucket);

  if (right_bucket->IsDisplaced() &&
      right_bucket->GetDisplacedDirection() == 1) {  // right of the home bucket
    cb(right_bucket->GetObject());
  }

  return entries_idx << (32 - capacity_log_);
}

auto DenseSet::NewLink(void* data, DensePtr next) -> DenseLinkKey* {
  LinkAllocator la(mr());
  DenseLinkKey* lk = la.allocate(1);
  la.construct(lk);

  lk->next = next;
  lk->SetObject(data);
  ++num_links_;

  return lk;
}

bool DenseSet::ExpireIfNeededInternal(DensePtr* prev, DensePtr* node) const {
  DCHECK(node != nullptr);
  DCHECK(node->HasTtl());

  bool deleted = false;
  do {
    uint32_t obj_time = ObjExpireTime(node->GetObject());
    if (obj_time > time_now_) {
      break;
    }

    // updates the *node to next item if relevant or resets it to empty.
    const_cast<DenseSet*>(this)->Delete(prev, node);
    deleted = true;
  } while (node->HasTtl());

  return deleted;
}

void DenseSet::CollectExpired() {
  // Simply iterating over all items will remove expired
  auto it = IteratorBase(this, false);
  while (it.curr_entry_ != nullptr) {
    it.Advance();
  }
}

size_t DenseSet::SizeSlow() {
  CollectExpired();
  return size_;
}

}  // namespace dfly
