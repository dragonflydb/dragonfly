// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "core/extent_tree.h"
#include "server/tiering/common.h"

namespace dfly::tiering {

/**
 *
 * An external allocator inspired by mimalloc. Its goal is to maintain a state machine for
 * bookkeeping the allocations of different sizes that are backed up by a separate
 * storage. It could be a disk, SSD or another memory allocator. This class serves
 * as a state machine that either returns an offset to the backing storage or the indication
 * of the resource that is missing. The advantage of such design is that we can use it in
 * asynchronous callbacks without blocking on any IO requests.
 * The allocator uses dynamic memory internally. Should be used in a single thread.
 *
 */

namespace detail {
struct Page;

constexpr unsigned kNumFreePages = 29;

/**
 * pages classes can be SMALL, MEDIUM or LARGE. SMALL (2MB) for block sizes upto 128KB.
 * MEDIUM (16MB) for block sizes 128KB-1MB. Anything else is LARGE.
 *
 */
enum PageClass : uint16_t {
  SMALL_P = 0,
  MEDIUM_P = 1,
  LARGE_P = 2,
};

PageClass ClassFromSize(size_t size);

}  // namespace detail

class ExternalAllocator {
  ExternalAllocator(const ExternalAllocator&) = delete;
  void operator=(const ExternalAllocator&) = delete;

 public:
  static constexpr size_t kExtAlignment = 256_MB;     // 256 MB
  static constexpr size_t kMinBlockSize = kPageSize;  // 4KB

  ExternalAllocator();
  ~ExternalAllocator();

  // If a negative result - backing storage is required of size=-result. See AddStorage
  // on how to add more storage.
  // For results >= 0 Returns offset to the backing storage where we may write the data of
  // size sz.
  int64_t Malloc(size_t sz);

  void Free(size_t offset, size_t sz);

  /// Adds backing storage to the allocator. The range should not overlap with already
  /// added storage ranges.
  void AddStorage(size_t start, size_t size);

  // Similar to mi_good_size, returns the size of the underlying block as if
  // were returned by Malloc. Guaranteed that the result not less than sz.
  // No allocation is done.
  static size_t GoodSize(size_t sz);

  size_t capacity() const {
    return capacity_;
  }

  size_t allocated_bytes() const {
    return allocated_bytes_;
  }

 private:
  class SegmentDescr;
  using Page = detail::Page;

  // Returns a page if there is a segment of that class.
  // Returns NULL if no page is found.
  Page* FindPage(detail::PageClass sc);

  int64_t LargeMalloc(size_t size);
  SegmentDescr* GetNewSegment(detail::PageClass sc);
  void FreePage(Page* page, SegmentDescr* owner, size_t block_size);

  static SegmentDescr* ToSegDescr(Page*);

  SegmentDescr* sq_[2];                      // map: PageClass -> free Segment.
  Page* free_pages_[detail::kNumFreePages];  // intrusive linked lists of pages with free blocks

  // A segment for each 256MB range. To get a segment id from the offset, shift right by 28.
  std::vector<SegmentDescr*> segments_;

  ExtentTree extent_tree_;

  size_t capacity_ = 0;  // in bytes.
  size_t allocated_bytes_ = 0;
};

}  // namespace dfly::tiering
