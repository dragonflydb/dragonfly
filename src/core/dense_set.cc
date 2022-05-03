// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/dense_set.h"

#include <absl/numeric/bits.h>

#include <cstddef>
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
constexpr bool kFastScan = true;

DenseSet::ChainIterator& DenseSet::ChainIterator::operator++() {
  if (curr_) {
    curr_ = curr_->Next();
  }

  return *this;
}

DenseSet::ConstChainIterator& DenseSet::ConstChainIterator::operator++() {
  if (curr_) {
    curr_ = curr_->Next();
  }

  return *this;
}

bool DenseSet::IsEmpty(DenseSet::ChainVectorIterator it) const {
  return it->IsEmpty();
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

void DenseSet::PushFront(DenseSet::ChainVectorIterator it, DenseSet::DensePtr* ptr) {
  if (it->IsEmpty()) {
    it->SetObject(ptr->GetObject());
    if (ptr->IsLink()) {
      // deallocate the link if it is no longer a link as it is now in an empty list
      mr()->deallocate(ptr->Raw(), sizeof(DenseLinkKey), alignof(DenseLinkKey));
    }
  } else if (ptr->IsLink()) {
    // if the pointer is already a link then no allocation needed
    *ptr->Next() = *it;
    *it = *ptr;
  } else {
    // allocate a new link if needed and copy the pointer to the new link
    LinkAllocator la(mr());
    DenseLinkKey* lk = la.allocate(1);
    la.construct(lk);

    lk->next = *it;
    lk->SetObject(ptr->GetObject());
    it->SetLink(lk);
  }
}

void* DenseSet::PopFront(DenseSet::ChainVectorIterator it) {
  if (IsEmpty(it)) {
    return nullptr;
  }

  void* ret = it->GetObject();

  // if this is the last object in this chain just reset the
  // DensePtr instead of deleting it
  if (it->IsObject()) {
    it->Reset();
  } else {
    DCHECK(it->IsLink());
    // set the current item in the list to the next one & deallocate the link data structure
    DenseLinkKey* lk = (DenseLinkKey*)it->Raw();
    *it = lk->next;
    mr()->deallocate(lk, sizeof(DenseLinkKey), alignof(DenseLinkKey));
  }

  return ret;
}

DenseSet::DensePtr* DenseSet::Find(void* needle, DenseSet::ChainVectorIterator list,
                                   DenseSet::DensePtr** prev_out) {
  DensePtr* prev = nullptr;
  ChainIterator it = ChainBegin(list);
  for (; it != ChainEnd(list); ++it) {
    if (Equal(*it, needle)) {
      break;
    }

    prev = *it;
  }

  if (!it->IsEmpty() && prev_out != nullptr) {
    *prev_out = prev;
  }

  return *it;
}

DenseSet::ChainIterator DenseSet::Unlink(DenseSet::ChainIterator node,
                                          DenseSet::ChainIterator prev, DenseSet::DensePtr* out) {
  DCHECK(out != nullptr);
  if (node->IsObject()) {
    // this is the only item in the list, reset it and keep the iterator here
    if (prev == nullptr) {
      // copy the pointer to the data before a reset
      *out = **node;
      node->Reset();
      return node;
    } else {
      DCHECK(prev->IsLink());
      /*
        here we can try to pull a sneaky optimization, since the caller
        may get a LinkKey or an regular object it is responsible for freeing it.
        Some use cases (i.e Grow) may be able to use a LinkKey without freeing it as they can be
        inserted into the front of a non-empty chained list. So instead of freeing them here
        and save some memory management calls
      */
      DenseLinkKey* lk = (DenseLinkKey*)prev->Raw();
      DCHECK(!lk->IsDisplaced());

      // swap the two pointers to the actual data
      void* lk_tmp = lk->GetObject();
      lk->SetObject(node->GetObject());
      node->SetObject(lk_tmp);

      // prev now contains the data unlinked so store this pointer value in the out ptr
      *out = **prev;
      // overwrite prev with the data in node which is the end node containing the original
      // previous value
      **prev = **node;

      // set the iterator to be the location of the previous value as this now contains
      // the valid node still in the linked list
      return prev;
    }
  } else {
    DCHECK(node->IsLink());
    // return the node and replace its spot with the next node in the list
    DenseLinkKey* lk = (DenseLinkKey*)node->Raw();
    *out = **node;
    **node = lk->next;
    return node;
  }
}

DenseSet::ChainIterator DenseSet::UnlinkAndFree(DenseSet::ChainIterator node,
                                                 DenseSet::ChainIterator prev, void** out) {
  DensePtr unlinked;
  ChainIterator ret = Unlink(node, prev, &unlinked);
  *out = unlinked.GetObject();

  if (unlinked.IsLink()) {
    mr()->deallocate(unlinked.Raw(), sizeof(DenseLinkKey), alignof(DenseLinkKey));
  }

  return ret;
}

DenseSet::DenseSet(pmr::memory_resource* mr) : entries_(mr) {
}

void DenseSet::Clear() {
  for (auto it = entries_.begin(); it != entries_.end(); ++it) {
    while (!it->IsEmpty()) {
      PopFront(it);
    }
  }

  entries_.clear();
}

DenseSet::~DenseSet() {
  Clear();
}

DenseSet::ChainVectorIterator DenseSet::FindEmptyAround(uint32_t bid) {
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
    ChainIterator prev = ChainEnd(i);
    ChainIterator curr = ChainBegin(i);

    while (curr != ChainEnd(i)) {
      void* ptr = curr->GetObject();
      uint32_t bid = BucketId(ptr);

      if (bid == i) {
        curr->ClearDisplaced();
        prev = curr;
        ++curr;
      } else {
        DensePtr node;
        curr = Unlink(curr, prev, &node);
        PushFront(entries_.begin() + bid, &node);
        entries_[bid].ClearDisplaced();
      }
    }

    if (prev != nullptr) {
      DCHECK(!prev->IsLink());
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
  if (FindAround(ptr, bucket_id).first != ChainEnd(bucket_id)) {
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
  std::stack<std::pair<DensePtr, ChainVectorIterator>> to_fix_displacements;
  auto curr = entries_.begin() + bucket_id;
  while (!curr->IsEmpty() && curr->IsDisplaced()) {
    DCHECK(!curr->IsLink());
    DensePtr unlinked;
    Unlink(ChainBegin(curr), nullptr, &unlinked);
    --num_used_buckets_;

    // undo the direction shift in a displaced pointer
    curr -= unlinked.GetDisplacedDirection();
    to_fix_displacements.push({unlinked, curr});
  }

  while (!to_fix_displacements.empty()) {
    auto [ptr, list] = to_fix_displacements.top();
    to_fix_displacements.pop();

    if (!list->IsEmpty()) {
      ++num_chain_entries_;
    } else {
      ++num_used_buckets_;
    }

    PushFront(list, &ptr);
  }
  if (!entries_[bucket_id].IsEmpty()) {
    ++num_chain_entries_;
  }

  ChainVectorIterator list = entries_.begin() + bucket_id;
  obj_malloc_used_ += PushFront(list, ptr);
  DCHECK(!entries_[bucket_id].IsDisplaced());

  ++size_;
  return true;
}

std::pair<DenseSet::ChainIterator, DenseSet::ChainIterator> DenseSet::FindAround(void* ptr,
                                                                                   uint32_t bid) {
  // first look for displaced nodes since this is quicker than iterating a poential long chain
  if (bid && Equal(&entries_[bid - 1], ptr)) {
    return {ChainBegin(bid - 1), ChainEnd(bid - 1)};
  }

  if (bid + 1 < entries_.size() && Equal(&entries_[bid + 1], ptr)) {
    return {ChainBegin(bid + 1), ChainEnd(bid + 1)};
  }

  // if the node is not displaced, search the correct chain
  ChainIterator prev;
  for (auto it = ChainBegin(bid); it != ChainEnd(bid); ++it) {
    if (Equal(*it, ptr)) {
      return {it, prev};
    }

    prev = it;
  }

  // not in the Set
  return {ChainEnd(bid), ChainEnd(bid)};
}

// Same idea as FindAround but provide the const guarantee
bool DenseSet::ContainsInternal(const void* ptr) const {
  uint32_t bid = BucketId(ptr);
  if (bid && Equal(&entries_[bid - 1], ptr)) {
    return true;
  }

  if (bid + 1 < entries_.size() && Equal(&entries_[bid + 1], ptr)) {
    return true;
  }

  for (auto it = ChainConstBegin(bid); it != ChainConstEnd(bid); ++it) {
    if (Equal(*it, ptr)) {
      return true;
    }
  }

  return false;
}

void* DenseSet::EraseInternal(void* ptr) {
  uint32_t bid = BucketId(ptr);
  auto [found, prev] = FindAround(ptr, bid);

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
  void* ret;
  (void)UnlinkAndFree(found, prev, &ret);

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
  void* ret;
  (void)UnlinkAndFree(&*bucket_iter, nullptr, &ret);

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
 *
 * Therefore for any given Scan cursor x which was run when capacity_log_ was m, if the
 * next Scan is run when capacity_log_ is n the current bucket to check is x << (n - m)
 *
 * When kFastScan is enabled this optimization is used.
 * To be able to make this comparison the cursor must store the capacity_log_ from when
 * Scan is run. To do this the (capacity_log_)th bit is set in the cursor, this will always
 * be the highest bit set since the number of buckets is 2**(capacity_log_) and therefore
 * the greatest index is (2 ** capacity_log_) - 1.
 *
 * When using this optimization the maximum number of buckets is 2 ** 31 instead of 2 ** 32
 */

// Extract the previous capacity_log_ from the cursor
static inline std::pair<size_t, uint32_t> cursor_to_idx(uint32_t cursor) {
  DCHECK(cursor != 0);
  size_t expected_capacity_log = 0;
  uint32_t expected_capacity_log_bitmask = 1;

  while ((cursor >> expected_capacity_log) != 1) {
    ++expected_capacity_log;
    expected_capacity_log_bitmask <<= 1;
  }

  return {expected_capacity_log, expected_capacity_log_bitmask};
}

uint32_t DenseSet::Scan(uint32_t cursor, const ItemCb& cb) const {
  uint32_t entries_idx = 0;

  // if the fast scan optimization is enabled
  if constexpr (kFastScan) {
    DCHECK(entries_.size() <= (1ULL << 31));
    // not a new scan
    if (cursor != 0) {
      // extract the previous capacity_log_ and actual bucket index from the cursor
      auto [cursor_capacity_log, cursor_bitmask] = cursor_to_idx(cursor);
      entries_idx = cursor & ~cursor_bitmask;

      // Since DenseSet does not currently support shrinking the current log2(capacity)
      // should never be less than the log2(capacity) of the cursor from a previous Scan run
      DCHECK(cursor_capacity_log <= capacity_log_);

      // if the capacity has grown significantly use the above described optmization
      // to skip as many buckets as possible
      if (cursor_capacity_log < capacity_log_) {
        entries_idx <<= capacity_log_ - cursor_capacity_log;
      }
    }
  }

  // Shrinkage is not currently supported by the DenseSet type
  DCHECK(entries_idx < 1ULL << capacity_log_);

  // skip empty entries
  while (entries_idx < entries_.size() && entries_[entries_idx].IsEmpty()) {
    ++entries_idx;
  }

  // when scanning add all entries in a given chain
  for (ConstChainIterator it = ChainConstBegin(entries_idx); it != ChainConstEnd(entries_idx);
       ++it) {
    cb(it->GetObject());
  }

  // move to the next index for the next scan and check if we are done
  ++entries_idx;
  if (entries_idx >= entries_.size()) {
    return 0;
  }

  // encode the current capacity_log_ in the cursor if needed
  if constexpr (kFastScan) {
    entries_idx |= 1 << capacity_log_;
  }

  return entries_idx;
}
}  // namespace dfly
