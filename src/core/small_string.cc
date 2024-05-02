// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/small_string.h"

#include <mimalloc.h>
#include <xxhash.h>

#include <memory>

#include "base/logging.h"
#include "core/segment_allocator.h"

extern "C" bool mi_heap_page_is_underutilized(mi_heap_t* heap, void* p, float ratio);

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

// we should use only for sizes greater than kPrefLen
size_t SmallString::Assign(std::string_view s) {
  DCHECK_GT(s.size(), kPrefLen);

  uint8_t* realptr = nullptr;

  if (size_ == 0) {
    // packed structs can not be tied here.
    auto [sp, rp] = tl.seg_alloc->Allocate(s.size() - kPrefLen);
    small_ptr_ = sp;
    realptr = rp;
    size_ = s.size();
  } else if (s.size() <= size_) {
    realptr = tl.seg_alloc->Translate(small_ptr_);

    if (s.size() < size_) {
      size_t capacity = mi_usable_size(realptr);
      if (s.size() * 2 < capacity) {
        tl.seg_alloc->Free(small_ptr_);
        auto [sp, rp] = tl.seg_alloc->Allocate(s.size() - kPrefLen);
        small_ptr_ = sp;
        realptr = rp;
      }
      size_ = s.size();
    }
  } else {
    LOG(FATAL) << "TBD: Bad usage";
  }

  memcpy(prefix_, s.data(), kPrefLen);
  memcpy(realptr, s.data() + kPrefLen, s.size() - kPrefLen);

  return mi_malloc_usable_size(realptr);
}

void SmallString::Free() {
  if (size_ <= kPrefLen)
    return;

  tl.seg_alloc->Free(small_ptr_);
  size_ = 0;
}

uint16_t SmallString::MallocUsed() const {
  if (size_ <= kPrefLen)
    return 0;
  auto* realptr = tl.seg_alloc->Translate(small_ptr_);

  return mi_malloc_usable_size(realptr);
}

bool SmallString::Equal(std::string_view o) const {
  if (size_ != o.size())
    return false;

  if (size_ == 0)
    return true;

  DCHECK_GT(size_, kPrefLen);

  if (memcmp(prefix_, o.data(), kPrefLen) != 0)
    return false;

  uint8_t* realp = tl.seg_alloc->Translate(small_ptr_);

  return memcmp(realp, o.data() + kPrefLen, size_ - kPrefLen) == 0;
}

bool SmallString::Equal(const SmallString& os) const {
  if (size_ != os.size_)
    return false;

  string_view me[2], other[2];
  unsigned n1 = GetV(me);
  unsigned n2 = os.GetV(other);

  if (n1 != n2)
    return false;

  return me[0] == other[0] && me[1] == other[1];
}

uint64_t SmallString::HashCode() const {
  DCHECK_GT(size_, kPrefLen);

  string_view slice[2];

  GetV(slice);
  XXH3_state_t* state = tl.xxh_state.get();
  XXH3_64bits_reset_withSeed(state, kHashSeed);
  XXH3_64bits_update(state, slice[0].data(), slice[0].size());
  XXH3_64bits_update(state, slice[1].data(), slice[1].size());

  return XXH3_64bits_digest(state);
}

void SmallString::Get(std::string* dest) const {
  dest->resize(size_);
  if (size_) {
    DCHECK_GT(size_, kPrefLen);
    memcpy(dest->data(), prefix_, kPrefLen);
    uint8_t* ptr = tl.seg_alloc->Translate(small_ptr_);
    memcpy(dest->data() + kPrefLen, ptr, size_ - kPrefLen);
  }
}

unsigned SmallString::GetV(string_view dest[2]) const {
  DCHECK_GT(size_, kPrefLen);
  if (size_ <= kPrefLen) {
    dest[0] = string_view{prefix_, size_};
    return 1;
  }

  dest[0] = string_view{prefix_, kPrefLen};
  uint8_t* ptr = tl.seg_alloc->Translate(small_ptr_);
  dest[1] = string_view{reinterpret_cast<char*>(ptr), size_ - kPrefLen};
  return 2;
}

bool SmallString::DefragIfNeeded(float ratio) {
  DCHECK_GT(size_, kPrefLen);
  if (size_ <= kPrefLen) {
    return false;
  }

  uint8_t* cur_real_ptr = tl.seg_alloc->Translate(small_ptr_);
  if (!mi_heap_page_is_underutilized(tl.seg_alloc->heap(), cur_real_ptr, ratio))
    return false;

  auto [sp, rp] = tl.seg_alloc->Allocate(size_ - kPrefLen);

  memcpy(rp, cur_real_ptr, size_ - kPrefLen);
  tl.seg_alloc->Free(small_ptr_);
  small_ptr_ = sp;

  return true;
}

}  // namespace dfly
