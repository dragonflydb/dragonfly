// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "core/oah_base.h"
#include "core/page_usage/page_usage_stats.h"
#include "core/ptr_vector.h"

namespace dfly {

// oah_ptr.h - the bucket-slot manager that ties the entry accessor and PtrVector together.
//
// OAHPtr is a non-owning accessor over one bucket TaggedPtr that holds EITHER a single Entry
// (OAHEntry or OAHPair) or a PtrVector collision chain. Bit 0 (kVectorBit) discriminates them: a
// vector sets it, an entry never does. OAHPtr is the only type aware of both representations and
// owns the lifetime logic (promote-to-vector, grow, clear) via the Entry and PtrVector
// Create()/Destroy() factories.
template <typename Entry> class OAHPtr {
  using TaggedPtr = oah::TaggedPtr;
  using Vector = PtrVector<TaggedPtr>;

 public:
  explicit OAHPtr(TaggedPtr& slot) : slot_(&slot) {
  }

  bool Empty() const {
    return *slot_ == 0;
  }
  operator bool() const {
    return !Empty();
  }
  bool IsVector() const {
    return (*slot_ & oah::kVectorBit) != 0;
  }

  Vector AsVector() {
    return Vector(*slot_);
  }

  // The entry at `pos`: the bucket slot itself for a single entry (pos == 0), else a vector cell.
  Entry operator[](uint32_t pos) {
    if (!IsVector()) {
      assert(pos == 0);
      return Entry(*slot_);
    }
    assert(pos < AsVector().Size());
    return Entry(AsVector()[pos]);
  }

  // 0 if empty, 1 for a single entry, else the vector capacity.
  uint32_t ElementsNum() {
    if (Empty())
      return 0;
    if (!IsVector())
      return 1;
    return AsVector().Size();
  }

  // Inserts entry `tagged_ptr` (ownership transferred), promoting a single entry to a 2-element
  // vector on the first collision and growing it afterwards. Returns the added vector allocation
  // size.
  [[nodiscard]] size_t Insert(TaggedPtr tagged_ptr) {
    if (Empty()) {
      *slot_ = tagged_ptr;
      return 0;
    }
    return InsertNonEmpty(tagged_ptr);
  }

  // Insert into a slot the caller has already proven non-empty, skipping Insert's Empty() check.
  [[nodiscard]] size_t InsertNonEmpty(TaggedPtr tagged_ptr) {
    assert(!Empty());
    if (!IsVector()) {
      TaggedPtr arr_tagged_ptr = Vector::Create(2);
      Vector arr(arr_tagged_ptr);
      arr[0] = *slot_;      // the existing single entry
      arr[1] = tagged_ptr;  // the new one
      *slot_ = arr_tagged_ptr;
      return arr.AllocSize();
    }
    Vector arr = AsVector();
    for (size_t i = 0; i < arr.Size(); ++i) {
      if (arr[i] == 0) {
        arr[i] = tagged_ptr;
        return 0;
      }
    }
    size_t prev_alloc = arr.AllocSize();
    size_t pos = arr.Size();
    arr.Grow();
    arr[pos] = tagged_ptr;
    return arr.AllocSize() - prev_alloc;
  }

  // Moves out and returns the entry's TaggedPtr at `pos`, leaving the cell empty.
  TaggedPtr Remove(uint32_t pos) {
    if (Empty()) {
      assert(pos == 0);
      return 0;
    }
    if (!IsVector()) {
      assert(pos == 0);
      TaggedPtr tagged_ptr = *slot_;
      *slot_ = 0;
      return tagged_ptr;
    }
    Vector arr = AsVector();
    assert(pos < arr.Size());
    TaggedPtr tagged_ptr = arr[pos];
    arr[pos] = 0;
    return tagged_ptr;
  }

  // Defragments fragmented buffers under this slot: the entry's string, or for a vector every inner
  // entry plus the array. Returns the cumulative usable-size delta; *realloced is set if anything
  // moved.
  int64_t ReallocIfNeeded(PageUsage* page_usage, bool* realloced) {
    *realloced = false;
    if (Empty())
      return 0;
    if (!IsVector())
      return Entry(*slot_).ReallocIfNeeded(page_usage, realloced);

    int64_t obj_alloc_delta = 0;
    Vector vec = AsVector();
    for (TaggedPtr& cell : vec) {
      Entry entry(cell);
      if (entry) {
        bool inner_moved = false;
        obj_alloc_delta += entry.ReallocIfNeeded(page_usage, &inner_moved);
        *realloced |= inner_moved;
      }
    }
    if (page_usage->IsPageForObjectUnderUtilized(Raw())) {
      vec.Realloc();
      *realloced = true;
    }
    return obj_alloc_delta;
  }

  char* Raw() const {
    return reinterpret_cast<char*>(*slot_ & ~oah::kTagMask);
  }

  // Replaces the slot's contents with the single entry `tagged_ptr`, freeing the old contents.
  void Assign(TaggedPtr tagged_ptr) {
    Clear();
    *slot_ = tagged_ptr;
  }

  // Frees the entry blob or collision vector and zeroes the slot.
  void Clear() {
    if (!*slot_)
      return;
    if (IsVector()) {
      Vector vec = AsVector();
      for (TaggedPtr cell : vec)
        Entry::Destroy(cell);
      Vector::Destroy(vec.Release());
    } else {
      Entry::Destroy(*slot_);
      *slot_ = 0;
    }
  }

 private:
  TaggedPtr* slot_;
};

}  // namespace dfly
