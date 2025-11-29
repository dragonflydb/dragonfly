// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/small_string.h"

#include <mimalloc.h>
#include <xxhash.h>

#include <memory>

#include "base/logging.h"
#include "core/page_usage_stats.h"
#include "core/segment_allocator.h"

namespace dfly {
using namespace std;

namespace {

class XXH3_Deleter {
 public:
  void operator()(XXH3_state_t* ptr) const {
    XXH3_freeState(ptr);
  }
};

struct TL {
  unique_ptr<XXH3_state_t, XXH3_Deleter> xxh_state;
  unique_ptr<SegmentAllocator> seg_alloc;
};

thread_local TL tl;

constexpr XXH64_hash_t kHashSeed = 24061983;  // same as in compact_object.cc

}  // namespace

void SmallString::InitThreadLocal(void* heap) {
  SegmentAllocator* ns = new SegmentAllocator((mi_heap_t*)heap);

  tl.seg_alloc.reset(ns);
  tl.xxh_state.reset(XXH3_createState());
  XXH3_64bits_reset_withSeed(tl.xxh_state.get(), kHashSeed);
}

bool SmallString::CanAllocate(size_t size) {
  return size <= kMaxSize && tl.seg_alloc->CanAllocate();
}

size_t SmallString::UsedThreadLocal() {
  return tl.seg_alloc ? tl.seg_alloc->used() : 0;
}

static_assert(sizeof(SmallString) == 16);

size_t SmallString::Assign(std::string_view s) {
  DCHECK_GT(s.size(), kPrefLen);
  DCHECK(CanAllocate(s.size()));
  uint8_t* realptr = nullptr;

  // If the allocation is large enough and space efficient, we can avoid allocating
  if (s.size() >= size_ || s.size() * 2 < MallocUsed()) {
    if (size_)
      Free();
    auto [sp, rp] = tl.seg_alloc->Allocate(s.size() - kPrefLen);
    small_ptr_ = sp;
    realptr = rp;
  }

  size_ = s.size();
  memcpy(prefix_, s.data(), kPrefLen);
  memcpy(realptr, s.data() + kPrefLen, s.size() - kPrefLen);
  return mi_malloc_usable_size(realptr);
}

void SmallString::Free() {
  tl.seg_alloc->Free(small_ptr_);
  size_ = 0;
}

uint16_t SmallString::MallocUsed() const {
  return mi_malloc_usable_size(tl.seg_alloc->Translate(small_ptr_));
}

bool SmallString::Equal(std::string_view o) const {
  if (size_ != o.size())
    return false;

  if (size_ == 0)
    return true;

  if (memcmp(prefix_, o.data(), kPrefLen) != 0)
    return false;

  uint8_t* realp = tl.seg_alloc->Translate(small_ptr_);
  return memcmp(realp, o.data() + kPrefLen, size_ - kPrefLen) == 0;
}

bool SmallString::Equal(const SmallString& os) const {
  if (size_ != os.size_)
    return false;

  string_view me[2], other[2];
  Get(me);
  os.Get(other);

  return me[0] == other[0] && me[1] == other[1];
}

uint64_t SmallString::HashCode() const {
  string_view slice[2];
  Get(slice);

  XXH3_state_t* state = tl.xxh_state.get();
  XXH3_64bits_reset_withSeed(state, kHashSeed);
  XXH3_64bits_update(state, slice[0].data(), slice[0].size());
  XXH3_64bits_update(state, slice[1].data(), slice[1].size());

  return XXH3_64bits_digest(state);
}

void SmallString::Get(string_view dest[2]) const {
  DCHECK(size_);

  dest[0] = string_view{prefix_, kPrefLen};
  uint8_t* ptr = tl.seg_alloc->Translate(small_ptr_);
  dest[1] = string_view{reinterpret_cast<char*>(ptr), size_ - kPrefLen};
}

void SmallString::Get(char* out) const {
  string_view strs[2];
  Get(strs);
  memcpy(out, strs[0].data(), strs[0].size());
  memcpy(out + strs[0].size(), strs[1].data(), strs[1].size());
}

void SmallString::Get(std::string* dest) const {
  dest->resize(size_);
  Get(dest->data());
}

bool SmallString::DefragIfNeeded(PageUsage* page_usage) {
  uint8_t* cur_real_ptr = tl.seg_alloc->Translate(small_ptr_);
  if (!page_usage->IsPageForObjectUnderUtilized(tl.seg_alloc->heap(), cur_real_ptr))
    return false;

  if (!CanAllocate(size_ - kPrefLen))  // Forced
    return false;

  auto [sp, rp] = tl.seg_alloc->Allocate(size_ - kPrefLen);
  memcpy(rp, cur_real_ptr, size_ - kPrefLen);
  tl.seg_alloc->Free(small_ptr_);
  small_ptr_ = sp;

  return true;
}

}  // namespace dfly
