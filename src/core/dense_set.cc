// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/dense_set.h"

#include <absl/numeric/bits.h>

#include <cstddef>
#include <cstdint>
#include <stack>
#include <type_traits>
#include <vector>

#include "glog/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {
using namespace std;

constexpr size_t kMinSizeShift = 2;
constexpr size_t kMinSize = 1 << kMinSizeShift;
constexpr bool kAllowDisplacements = true;

DenseSet::DenseSet(pmr::memory_resource* mr) : entries_(mr) {
}

DenseSet::~DenseSet() {
  Clear();
}

size_t DenseSet::PushFront(DenseSet::ChainVectorIterator it, void* data) {
  // if this is an empty list assign the value to the empty placeholder pointer
  if (it->IsEmpty()) {
    it->SetObject(data);
    return ObjectAllocSize(data);
  }

  // otherwise make a new link and connect it to the front of the list
  LinkAllocator la(mr());
  DenseLinkKey* lk = la.allocate(1);
  la.construct(lk);

  lk->next = *it;
  it->SetLink(lk);
  lk->SetObject(data);

  return ObjectAllocSize(data);
}

void DenseSet::PushFront(DenseSet::ChainVectorIterator it, DenseSet::DensePtr ptr) {
  if (it->IsEmpty()) {
    it->SetObject(ptr.GetObject());
    if (ptr.IsLink()) {
      // deallocate the link if it is no longer a link as it is now in an empty list
      mr()->deallocate(ptr.Raw(), sizeof(DenseLinkKey), alignof(DenseLinkKey));
    }
  } else if (ptr.IsLink()) {
    // if the pointer is already a link then no allocation needed
    *ptr.Next() = *it;
    *it = ptr;
  } else {
    // allocate a new link if needed and copy the pointer to the new link
    LinkAllocator la(mr());
    DenseLinkKey* lk = la.allocate(1);
    la.construct(lk);

    lk->next = *it;
    lk->SetObject(ptr.GetObject());
    it->SetLink(lk);
  }
}

auto DenseSet::PopPtrFront(DenseSet::ChainVectorIterator it) -> DensePtr {
  if (it->IsEmpty()) {
    return DensePtr(nullptr);
  }

  DensePtr front = *it;

  // if this is an object, then it's also the only record in this chain.
  // therefore, we should just reset DensePtr.
  if (it->IsObject()) {
    it->Reset();
  } else {
    DCHECK(it->IsLink());
    // this will not return nullptr since it is a Link
    *it = *it->Next();
  }

  return front;
}

void* DenseSet::PopDataFront(DenseSet::ChainVectorIterator it) {
  DensePtr front = PopPtrFront(it);
  void* ret = front.GetObject();

  if (front.IsLink()) {
    DenseLinkKey* lk = (DenseLinkKey*)front.Raw();
    mr()->deallocate(lk, sizeof(DenseLinkKey), alignof(DenseLinkKey));
  }

  return ret;
}

auto DenseSet::Unlink(DenseSet::DensePtr* node) -> DensePtr {
  DensePtr ret = *node;
  if (node->IsObject()) {
    node->Reset();
  } else {
    *node = *node->Next();
  }

  return ret;
}

void* DenseSet::UnlinkAndFree(DenseSet::DensePtr* node) {
  DensePtr unlinked = Unlink(node);
  void* ret = unlinked.GetObject();

  if (unlinked.IsLink()) {
    mr()->deallocate(unlinked.Raw(), sizeof(DenseLinkKey), alignof(DenseLinkKey));
  }

  return ret;
}

void DenseSet::Clear() {
  for (auto it = entries_.begin(); it != entries_.end(); ++it) {
    while (!it->IsEmpty()) {
      PopDataFront(it);
    }
  }

  entries_.clear();
}

auto DenseSet::FindEmptyAround(uint32_t bid) -> ChainVectorIterator {
  if (entries_[bid].IsEmpty()) {
    return entries_.begin() + bid;
  }

  if (!kAllowDisplacements) {
    return entries_.end();
  }

  if (bid + 1 < entries_.size() && entries_[bid + 1].IsEmpty()) {
    return entries_.begin() + bid + 1;
  }

  if (bid && entries_[bid - 1].IsEmpty()) {
    return entries_.begin() + bid - 1;
  }

  return entries_.end();
}

bool DenseSet::Reserve(size_t sz) {
  sz = std::min<size_t>(sz, kMinSize);

  if (sz > entries_.max_size()) {
    return false;
  }

  sz = absl::bit_ceil(sz);
  capacity_log_ = absl::bit_width(sz);
  entries_.reserve(sz);

  if (entries_.capacity() != sz) {
    return false;
  }

  return true;
}

void DenseSet::Grow() {
  size_t prev_size = entries_.size();
  entries_.resize(prev_size * 2);
  ++capacity_log_;

  for (long i = prev_size - 1; i >= 0; --i) {
    DensePtr* curr = &entries_[i];

    while (curr != nullptr && !curr->IsEmpty()) {
      void* ptr = curr->GetObject();
      uint32_t bid = BucketId(ptr);

      if (bid == i) {
        curr->ClearDisplaced();
        curr = curr->Next();
      } else {
        DensePtr node = Unlink(curr);
        PushFront(entries_.begin() + bid, node);
        entries_[bid].ClearDisplaced();
      }
    }
  }
}

bool DenseSet::AddInternal(void* ptr) {
  uint64_t hc = Hash(ptr);

  if (entries_.empty()) {
    capacity_log_ = kMinSizeShift;
    entries_.resize(kMinSize);
    uint32_t bucket_id = BucketId(hc);
    auto e = entries_.begin() + bucket_id;
    obj_malloc_used_ += PushFront(e, ptr);
    ++size_;
    ++num_used_buckets_;

    return true;
  }

  // if the value is already in the set exit early
  uint32_t bucket_id = BucketId(hc);
  if (Find(ptr, bucket_id) != nullptr) {
    return false;
  }

  DCHECK_LT(bucket_id, entries_.size());

  // Try insert into flat surface first. Also handle the grow case
  // if utilization is too high.
  for (unsigned j = 0; j < 2; ++j) {
    ChainVectorIterator list = FindEmptyAround(bucket_id);
    if (list != entries_.end()) {
      obj_malloc_used_ += PushFront(list, ptr);
      if (std::distance(entries_.begin(), list) != bucket_id) {
        list->SetDisplaced(std::distance(entries_.begin() + bucket_id, list));
      }
      ++num_used_buckets_;
      ++size_;
      return true;
    }

    if (size_ < entries_.size()) {
      break;
    }

    Grow();
    bucket_id = BucketId(hc);
  }

  DCHECK(!entries_[bucket_id].IsEmpty());

  /**
   * Since the current entry is not empty, it is either a valid chain
   * or there is a displaced node here. In the latter case it is best to
   * move the displaced node to its correct bucket. However there could be
   * a displaced node there and so forth. Keep a stack of nodes that are displaced
   * until we find a displaced node which hashes to an empty bucket.
   * Then unwind the stack of pending displacements until each node is in
   * its correct bucket
   */

  DensePtr to_insert(ptr);
  while (!entries_[bucket_id].IsEmpty() && entries_[bucket_id].IsDisplaced()) {
    DensePtr unlinked = PopPtrFront(entries_.begin() + bucket_id);

    PushFront(entries_.begin() + bucket_id, to_insert);
    to_insert = unlinked;
    bucket_id -= unlinked.GetDisplacedDirection();
  }

  if (!entries_[bucket_id].IsEmpty()) {
    ++num_chain_entries_;
  }

  ChainVectorIterator list = entries_.begin() + bucket_id;
  PushFront(list, to_insert);
  obj_malloc_used_ += ObjectAllocSize(ptr);
  DCHECK(!entries_[bucket_id].IsDisplaced());

  ++size_;
  return true;
}

auto DenseSet::Find(const void* ptr, uint32_t bid) const -> const DensePtr* {
  // first look for displaced nodes since this is quicker than iterating a poential long chain
  if (bid && Equal(entries_[bid - 1], ptr)) {
    return &entries_[bid - 1];
  }

  if (bid + 1 < entries_.size() && Equal(entries_[bid + 1], ptr)) {
    return &entries_[bid + 1];
  }

  // if the node is not displaced, search the correct chain
  const DensePtr* curr = &entries_[bid];
  while (curr != nullptr) {
    if (Equal(*curr, ptr)) {
      return curr;
    }

    curr = curr->Next();
  }

  // not in the Set
  return nullptr;
}

auto DenseSet::Find(const void* ptr) const -> const DensePtr* {
  return Find(ptr, BucketId(ptr));
}

auto DenseSet::Find(const void* ptr, uint32_t bid) -> DensePtr* {
  const DensePtr* ret = const_cast<const DenseSet*>(this)->Find(ptr, bid);
  return const_cast<DensePtr*>(ret);
}

auto DenseSet::Find(const void* ptr) -> DensePtr* {
  const DensePtr* ret = const_cast<const DenseSet*>(this)->Find(ptr);
  return const_cast<DensePtr*>(ret);
}

// Same idea as FindAround but provide the const guarantee
bool DenseSet::ContainsInternal(const void* ptr) const {
  uint32_t bid = BucketId(ptr);
  return Find(ptr, bid) != nullptr;
}

void* DenseSet::EraseInternal(void* ptr) {
  uint32_t bid = BucketId(ptr);
  auto found = Find(ptr, bid);

  if (found == nullptr) {
    return nullptr;
  }

  if (found->IsLink()) {
    --num_chain_entries_;
  } else {
    DCHECK(found->IsObject());
    --num_used_buckets_;
  }

  obj_malloc_used_ -= ObjectAllocSize(ptr);
  void* ret = UnlinkAndFree(found);

  --size_;
  return ret;
}

void* DenseSet::PopInternal() {
  std::pmr::vector<DenseSet::DensePtr>::iterator bucket_iter = entries_.begin();

  // find the first non-empty chain
  while (bucket_iter != entries_.end() && bucket_iter->IsEmpty()) {
    ++bucket_iter;
  }

  // empty set
  if (bucket_iter == entries_.end()) {
    return nullptr;
  }

  if (bucket_iter->IsLink()) {
    --num_chain_entries_;
  } else {
    DCHECK(bucket_iter->IsObject());
    --num_used_buckets_;
  }

  // unlink the first node in the first non-empty chain
  obj_malloc_used_ -= ObjectAllocSize(bucket_iter->GetObject());
  void* ret = PopDataFront(bucket_iter);

  --size_;
  return ret;
}

/**
 * stable scanning api. has the same guarantees as redis scan command.
 * we avoid doing bit-reverse by using a different function to derive a bucket id
 * from hash values. By using msb part of hash we make it "stable" with respect to
 * rehashes. For example, with table log size 4 (size 16), entries in bucket id
 * 1110 come from hashes 1110XXXXX.... When a table grows to log size 5,
 * these entries can move either to 11100 or 11101. So if we traversed with our cursor
 * range [0000-1110], it's guaranteed that in grown table we do not need to cover again
 * [00000-11100]. Similarly with shrinkage, if a table is shrinked to log size 3,
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

  // skip empty entries
  while (entries_idx < entries_.size() && entries_[entries_idx].IsEmpty()) {
    ++entries_idx;
  }

  if (entries_idx >= entries_.size()) {
    return 0;
  }

  const DensePtr* curr = &entries_[entries_idx];

  // when scanning add all entries in a given chain
  while (curr != nullptr && !curr->IsEmpty()) {
    cb(curr->GetObject());
    curr = curr->Next();
  }

  // move to the next index for the next scan and check if we are done
  ++entries_idx;
  if (entries_idx >= entries_.size()) {
    return 0;
  }

  return entries_idx << (32 - capacity_log_);
}
}  // namespace dfly
