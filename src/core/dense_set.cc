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

#include "glog/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {
using namespace std;

constexpr size_t kMinSizeShift = 2;
constexpr size_t kMinSize = 1 << kMinSizeShift;
constexpr bool kAllowDisplacements = true;

DenseSet::IteratorBase::IteratorBase(const DenseSet* owner, bool is_end)
    : owner_(const_cast<DenseSet&>(*owner)),
      curr_entry_(nullptr) {
  curr_list_ = is_end ? owner_.entries_.end() : owner_.entries_.begin();
  if (curr_list_ != owner->entries_.end()) {
    curr_entry_ = &(*curr_list_);
    owner->ExpireIfNeeded(nullptr, curr_entry_);

    // find the first non null entry
    if (curr_entry_->IsEmpty()) {
      Advance();
    }
  }
}

void DenseSet::IteratorBase::Advance() {
  bool step_link = false;
  DCHECK(curr_entry_);

  if (curr_entry_->IsLink()) {
    DenseLinkKey* plink = curr_entry_->AsLink();
    if (!owner_.ExpireIfNeeded(curr_entry_, &plink->next) || curr_entry_->IsLink()) {
      curr_entry_ = &plink->next;
      step_link = true;
    }
  }

  if (!step_link) {
    DCHECK(curr_list_ != owner_.entries_.end());
    do {
      ++curr_list_;
      if (curr_list_ == owner_.entries_.end()) {
        curr_entry_ = nullptr;
        return;
      }
      owner_.ExpireIfNeeded(nullptr, &(*curr_list_));
    } while (curr_list_->IsEmpty());
    DCHECK(curr_list_ != owner_.entries_.end());
    curr_entry_ = &(*curr_list_);
  }
  DCHECK(!curr_entry_->IsEmpty());
}

DenseSet::DenseSet(pmr::memory_resource* mr) : entries_(mr) {
}

DenseSet::~DenseSet() {
  ClearInternal();
}

size_t DenseSet::PushFront(DenseSet::ChainVectorIterator it, void* data, bool has_ttl) {
  // if this is an empty list assign the value to the empty placeholder pointer
  if (it->IsEmpty()) {
    it->SetObject(data);
  } else {
    // otherwise make a new link and connect it to the front of the list
    it->SetLink(NewLink(data, *it));
  }

  if (has_ttl)
    it->SetTtl();
  return ObjectAllocSize(data);
}

void DenseSet::PushFront(DenseSet::ChainVectorIterator it, DenseSet::DensePtr ptr) {
  DVLOG(2) << "PushFront to " << distance(entries_.begin(), it) << ", "
           << ObjectAllocSize(ptr.GetObject());

  if (it->IsEmpty()) {
    it->SetObject(ptr.GetObject());
    if (ptr.HasTtl())
      it->SetTtl();
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
    if (ptr.HasTtl())
      it->SetTtl();
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

    // since a DenseLinkKey could be at the end of a chain and have a nullptr for next
    // avoid dereferencing a nullptr and just reset the pointer to this DenseLinkKey
    if (it->Next() == nullptr) {
      it->Reset();
    } else {
      *it = *it->Next();
    }
  }

  return front;
}

void* DenseSet::PopDataFront(DenseSet::ChainVectorIterator it) {
  DensePtr front = PopPtrFront(it);
  void* ret = front.GetObject();

  if (front.IsLink()) {
    FreeLink(front.AsLink());
  }

  return ret;
}

void DenseSet::ClearInternal() {
  for (auto it = entries_.begin(); it != entries_.end(); ++it) {
    while (!it->IsEmpty()) {
      bool has_ttl = it->HasTtl();
      void* obj = PopDataFront(it);
      ObjDelete(obj, has_ttl);
    }
  }

  entries_.clear();
}

bool DenseSet::Equal(DensePtr dptr, const void* ptr, uint32_t cookie) const {
  if (dptr.IsEmpty()) {
    return false;
  }

  return ObjEqual(dptr.GetObject(), ptr, cookie);
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
  sz = std::min<size_t>(sz, kMinSize);

  sz = absl::bit_ceil(sz);
  capacity_log_ = absl::bit_width(sz);
  entries_.reserve(sz);
}

void DenseSet::Grow() {
  size_t prev_size = entries_.size();
  entries_.resize(prev_size * 2);
  ++capacity_log_;

  // perform rehashing of items in the set
  for (long i = prev_size - 1; i >= 0; --i) {
    DensePtr* curr = &entries_[i];
    DensePtr* prev = nullptr;

    while (true) {
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
          curr->Reset();  // reset the original placeholder (.next or root)

          if (prev) {
            DCHECK(prev->IsLink());

            DenseLinkKey* plink = prev->AsLink();
            DCHECK(&plink->next == curr);

            // we want to make *prev a DensePtr instead of DenseLink and we
            // want to deallocate the link.
            DensePtr tmp = DensePtr::From(plink);
            DCHECK(ObjectAllocSize(tmp.GetObject()));

            FreeLink(plink);
            *prev = tmp;
          }

          DVLOG(2) << " Pushing to " << bid << " " << dptr.GetObject();
          PushFront(dest, dptr);

          dest->ClearDisplaced();

          break;
        }  // if IsObject

        *curr = *dptr.Next();
        DCHECK(!curr->IsEmpty());

        PushFront(dest, dptr);
        dest->ClearDisplaced();
      }
    }
  }
}

bool DenseSet::AddInternal(void* ptr, bool has_ttl) {
  uint64_t hc = Hash(ptr, 0);

  if (entries_.empty()) {
    capacity_log_ = kMinSizeShift;
    entries_.resize(kMinSize);
    uint32_t bucket_id = BucketId(hc);
    auto e = entries_.begin() + bucket_id;
    obj_malloc_used_ += PushFront(e, ptr, has_ttl);
    ++size_;
    ++num_used_buckets_;

    return true;
  }

  // if the value is already in the set exit early
  uint32_t bucket_id = BucketId(hc);
  if (Find(ptr, bucket_id, 0).second != nullptr) {
    return false;
  }

  DCHECK_LT(bucket_id, entries_.size());

  // Try insert into flat surface first. Also handle the grow case
  // if utilization is too high.
  for (unsigned j = 0; j < 2; ++j) {
    ChainVectorIterator list = FindEmptyAround(bucket_id);
    if (list != entries_.end()) {
      obj_malloc_used_ += PushFront(list, ptr, has_ttl);
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
   * a displaced node there and so forth. Keep to avoid having to keep a stack
   * of displacements we can keep track of the current displaced node, add it
   * to the correct chain, and if the correct chain contains a displaced node
   * unlink it and repeat the steps
   */

  DensePtr to_insert(ptr);
  if (has_ttl)
    to_insert.SetTtl();

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

auto DenseSet::Find(const void* ptr, uint32_t bid, uint32_t cookie) -> pair<DensePtr*, DensePtr*> {
  // could do it with zigzag decoding but this is clearer.
  int offset[] = {0, -1, 1};

  // first look for displaced nodes since this is quicker than iterating a potential long chain
  for (int j = 0; j < 3; ++j) {
    if ((bid == 0 && j == 1) || (bid + 1 == entries_.size() && j == 2))
      continue;

    DensePtr* curr = &entries_[bid + offset[j]];

    ExpireIfNeeded(nullptr, curr);
    if (Equal(*curr, ptr, cookie)) {
      return make_pair(nullptr, curr);
    }
  }

  // if the node is not displaced, search the correct chain
  DensePtr* prev = &entries_[bid];
  DensePtr* curr = prev->Next();
  while (curr != nullptr) {
    ExpireIfNeeded(prev, curr);

    if (Equal(*curr, ptr, cookie)) {
      return make_pair(prev, curr);
    }
    prev = curr;
    curr = curr->Next();
  }

  // not in the Set
  return make_pair(nullptr, nullptr);
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

      --num_chain_entries_;
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
    --num_chain_entries_;
    FreeLink(link);
  }

  obj_malloc_used_ -= ObjectAllocSize(obj);
  --size_;
  ObjDelete(obj, false);
}

void* DenseSet::PopInternal() {
  std::pmr::vector<DenseSet::DensePtr>::iterator bucket_iter = entries_.begin();

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

  // skip empty entries
  while (entries_idx < entries_.size() && entries_[entries_idx].IsEmpty()) {
    ++entries_idx;
  }

  if (entries_idx == entries_.size()) {
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

auto DenseSet::NewLink(void* data, DensePtr next) -> DenseLinkKey* {
  LinkAllocator la(mr());
  DenseLinkKey* lk = la.allocate(1);
  la.construct(lk);

  lk->next = next;
  lk->SetObject(data);
  return lk;
}

bool DenseSet::ExpireIfNeeded(DensePtr* prev, DensePtr* node) const {
  DCHECK_NOTNULL(node);

  bool deleted = false;
  while (node->HasTtl()) {
    uint32_t obj_time = ObjExpireTime(node->GetObject());
    if (obj_time > time_now_) {
      break;
    }

    // updates the node to next item if relevant.
    const_cast<DenseSet*>(this)->Delete(prev, node);
    deleted = true;
  }

  return deleted;
}

}  // namespace dfly
