// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/external_alloc.h"

#include <mimalloc.h>

#include <bitset>
#include <cstring>

#include "base/logging.h"

namespace dfly {
using namespace std;
using detail::PageClass;

using BinIdx = uint8_t;

namespace {

constexpr inline unsigned long long operator""_MB(unsigned long long x) {
  return x << 20U;
}

constexpr inline unsigned long long operator""_KB(unsigned long long x) {
  return x << 10U;
}

constexpr size_t kMediumObjSize = 1_MB;
constexpr size_t kSmallPageShift = 20;
constexpr size_t kMediumPageShift = 23;
constexpr size_t kSegmentAlignment = 256_MB;
constexpr size_t kSegmentDefaultSize = 256_MB;

constexpr inline size_t wsize_from_size(size_t size) {
  return (size + sizeof(uintptr_t) - 1) / sizeof(uintptr_t);
}

// TODO: we may want to round it up to the nearst 512 multiplier so that all the allocated
// blocks will be multipliers of 4kb.
constexpr size_t kBinLens[detail::kNumSizeBins] = {
    512,   512,   640,   768,   896,   1024,  1280,  1536,  1792,   2048,   2560,      3072,
    3584,  4096,  5120,  6144,  7168,  8192,  10240, 12288, 14336,  16384,  20480,     24576,
    28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072, UINT64_MAX};

static_assert(kBinLens[detail::kLargeSizeBin] == UINT64_MAX);

constexpr inline BinIdx ToBinIdx(size_t size) {
  size_t wsize = wsize_from_size(size);

  if (wsize <= 512) {
    return 1;
  }

  if (wsize > kMediumObjSize) {
    return detail::kLargeSizeBin;
  }

  // to correct rounding up of size to words that the last word will be within the range.
  --wsize;

  // find the highest bit
  uint8_t b = 63 - __builtin_clzl(wsize);
  return (b << 2) + ((wsize >> (b - 2)) & 3) - 34;
}

static_assert(ToBinIdx(4096) == 1);
static_assert(ToBinIdx(4097) == 2);
static_assert(ToBinIdx(5120) == 2);
static_assert(ToBinIdx(5121) == 3);
static_assert(ToBinIdx(6144) == 3);
static_assert(ToBinIdx(6145) == 4);

PageClass ClassFromBlockSize(size_t sz) {
  if (sz <= 128_KB)
    return PageClass::SMALL_P;
  if (sz <= 1_MB)
    return PageClass::MEDIUM_P;

  return PageClass::LARGE_P;
}

size_t ToBlockSize(BinIdx idx) {
  return kBinLens[idx] * 8;
}

unsigned NumPagesInClass(PageClass pc) {
  switch (pc) {
    case PageClass::SMALL_P:
      return kSegmentDefaultSize >> kSmallPageShift;
    case PageClass::MEDIUM_P:
      return kSegmentDefaultSize >> kMediumPageShift;
      break;
    case PageClass::LARGE_P:
      DLOG(FATAL) << "TBD";
      break;
  }
  return 0;
}

}  // namespace

/*
   block 4Kb or more, page - 1MB (256 blocks) or bigger.


   Block sizes grow exponentially - by factor ~1.25. See MI_PAGE_QUEUES_EMPTY definition
   for sizes example.
*/
namespace detail {
struct Page {
  std::bitset<256> free_blocks;  // bitmask of free blocks (32 bytes).
  uint8_t id;                    // index inside the Segment.pages array.

  // need some  mapping function to map from block_size to real_block_size given Page class.
  BinIdx block_size_bin;
  uint8_t segment_inuse : 1;  // true if segment allocated this page.
  uint8_t reserved1;

  // number of available free blocks. Note: we could get rid of this field and use
  // free_blocks.count instead.
  uint16_t available;
  uint8_t reserved2[2];

  // We can not use c'tor because we use the trick in segment where we allocate more pages
  // than SegmentDescr declares.
  void Reset(uint8_t new_id) {
    static_assert(sizeof(Page) == 40);

    memset(&id, 0, sizeof(Page) - offsetof(Page, id));
    id = new_id;
  }

  void Init(PageClass pc, BinIdx bin_id) {
    DCHECK_EQ(available, 0);
    DCHECK(segment_inuse);

    size_t page_size = 1UL << (pc == PageClass::SMALL_P ? kSmallPageShift : kMediumPageShift);

    block_size_bin = bin_id;
    available = page_size / ToBlockSize(bin_id);

    free_blocks.reset();
    for (unsigned i = 0; i < available; ++i) {
      free_blocks.set(i, true);
    }
  }
};

static_assert(sizeof(std::bitset<256>) == 32);
}  // namespace detail

//
/**
 * SegmentDescr denotes a 256MB segment on external storage -
 * holds upto 256 pages (in case of small pages).
 * Each segment has pages of the same type, but each page can host blocks of
 * differrent sizes upto maximal block size for that page class.
 * SegmentDescr points to the range within external storage space.
 * By using the page.id together with segment->page_shift and segment->offset
 * one can know where the page is located in the storage.
 * Opposite direction: by giving an offset to the file, segment_id = offset / 256MB.
 * Moreover (offset % 256MB) >> segment.page_shift gives us the page id and subsequently
 * page_start.  segment.pages[page_id].block_size gives us the block size and that in turn gives us
 * block id within the page. We can also know block_size if the originally allocated
   size is provided by using round_up function that was used to allocate the block.
 * SegmentDescr be aligned by 16KB boundaries - ToSegDescr relies on that.
 */
class ExternalAllocator::SegmentDescr {
  SegmentDescr(const SegmentDescr&) = delete;
  void operator=(const SegmentDescr&) = delete;
  friend class ExternalAllocator;

 public:
  explicit SegmentDescr(PageClass pc, size_t offs, uint16_t capacity);

  Page* FindPageSegment();

  Page* GetPage(unsigned i) {
    return pages_ + i;
  }

  size_t BlockOffset(const Page* page, unsigned blockpos) {
    return offset_ + page->id * (1 << page_shift_) + ToBlockSize(page->block_size_bin) * blockpos;
  }

  bool HasFreePages() const {
    return capacity_ > used_;
  }

  unsigned capacity() const {
    return capacity_;
  }

  unsigned used() const {
    return used_;
  }

  unsigned page_shift() const {
    return page_shift_;
  }

  PageClass page_class() const {
    return page_class_;
  }

  SegmentDescr *next, *prev;

 private:
  uint64_t offset_;
  uint16_t capacity_, used_;

  PageClass page_class_;
  uint8_t page_shift_;

  Page pages_[1];  // must be the last field. Can be 1-256 pages.
};

ExternalAllocator::SegmentDescr::SegmentDescr(PageClass pc, size_t offs, uint16_t capacity)
    : offset_(offs), capacity_(capacity), used_(0), page_class_(pc), page_shift_(kSmallPageShift) {
  next = prev = this;
  DCHECK(pc != PageClass::LARGE_P);

  if (pc == PageClass::MEDIUM_P)
    page_shift_ = kMediumPageShift;

  for (unsigned i = 0; i < capacity; ++i) {
    pages_[i].Reset(i);
  }
}

auto ExternalAllocator::SegmentDescr::FindPageSegment() -> Page* {
  for (uint32_t i = 0; i < capacity_; ++i) {
    if (!pages_[i].segment_inuse) {
      pages_[i].segment_inuse = 1;
      ++used_;
      return pages_ + i;
    }
  }

  LOG(DFATAL) << "Should not reach here";

  return nullptr;
}

static detail::Page empty_page;

ExternalAllocator::ExternalAllocator() {
  std::fill(sq_, sq_ + 3, nullptr);
  std::fill(free_pages_, free_pages_ + detail::kNumSizeBins, &empty_page);
}

ExternalAllocator::~ExternalAllocator() {
  for (auto* seg : segments_) {
    mi_free(seg);
  }
}

int64_t ExternalAllocator::Malloc(size_t sz) {
  uint8_t bin_idx = ToBinIdx(sz);
  Page* page = free_pages_[bin_idx];

  if (page->available == 0) {  // empty page.
    PageClass pc = ClassFromBlockSize(sz);
    CHECK_NE(pc, PageClass::LARGE_P) << "not supported, TBD";

    size_t seg_size = 0;
    page = FindPage(pc, &seg_size);
    if (!page)
      return -int64_t(seg_size);

    page->Init(pc, bin_idx);
    free_pages_[bin_idx] = page;
  }

  DCHECK(page->available);
  size_t pos = page->free_blocks._Find_first();
  page->free_blocks.flip(pos);
  --page->available;
  allocated_bytes_ += ToBlockSize(page->block_size_bin);

  SegmentDescr* seg = ToSegDescr(page);
  return seg->BlockOffset(page, pos);
}

void ExternalAllocator::Free(size_t offset, size_t sz) {
  size_t idx = offset / 256_MB;
  size_t delta = offset % 256_MB;
  CHECK_LT(idx, segments_.size());
  CHECK(segments_[idx]);

  SegmentDescr* seg = segments_[idx];
  unsigned page_id = delta >> seg->page_shift();
  CHECK_LT(page_id, seg->capacity());

  Page* page = seg->GetPage(page_id);
  unsigned page_size = (1 << seg->page_shift());
  unsigned block_offs = delta % page_size;
  unsigned block_size = ToBlockSize(page->block_size_bin);
  unsigned block_id = block_offs / block_size;
  unsigned blocks_num = page_size / block_size;

  CHECK_LE(sz, block_size);
  DCHECK_LT(block_id, blocks_num);
  DCHECK(!page->free_blocks[block_id]);

  page->free_blocks.set(block_id);
  ++page->available;

  DCHECK_EQ(page->available, page->free_blocks.count());
  if (page->available == blocks_num) {
    FreePage(page, seg, block_size);
  }
  allocated_bytes_ -= block_size;
}

void ExternalAllocator::AddStorage(size_t offset, size_t size) {
  CHECK_EQ(256_MB, size);
  CHECK_EQ(0u, offset % 256_MB);

  size_t idx = offset / 256_MB;

  CHECK_LE(segments_.size(), idx);
  auto [it, added] = added_segs_.emplace(offset, size);
  CHECK(added);
  if (it != added_segs_.begin()) {
    auto prev = it;
    --prev;
    CHECK_LE(prev->first + prev->second, offset);
  }
  auto next = it;
  ++next;
  if (next != added_segs_.end()) {
    CHECK_LE(offset + size, next->first);
  }

  capacity_ += size;
}

size_t ExternalAllocator::GoodSize(size_t sz) {
  uint8_t bin_idx = ToBinIdx(sz);
  return ToBlockSize(bin_idx);
}

/**
 *
  _____      _            _          __                  _   _
 |  __ \    (_)          | |        / _|                | | (_)
 | |__) | __ ___   ____ _| |_ ___  | |_ _   _ _ __   ___| |_ _  ___  _ __  ___
 |  ___/ '__| \ \ / / _` | __/ _ \ |  _| | | | '_ \ / __| __| |/ _ \| '_ \/ __|
 | |   | |  | |\ V / (_| | ||  __/ | | | |_| | | | | (__| |_| | (_) | | | \__ \
 |_|   |_|  |_| \_/ \__,_|\__\___| |_|  \__,_|_| |_|\___|\__|_|\___/|_| |_|___/

 src: https://patorjk.com/software/taag/#f=Big
 */

// private functions
auto ExternalAllocator::FindPage(PageClass pc, size_t* seg_size) -> Page* {
  DCHECK_NE(pc, PageClass::LARGE_P);

  SegmentDescr* seg = sq_[pc];
  if (seg) {
    while (true) {
      if (seg->HasFreePages()) {
        return seg->FindPageSegment();
      }

      // remove head.
      SegmentDescr* next = seg->next;
      if (next == seg->prev) {
        sq_[pc] = nullptr;
        DCHECK(next == seg);
        break;
      }

      sq_[pc] = next;
      next->prev = seg->prev;
      seg->prev->next = next;
      seg->next = seg->prev = seg;
      seg = next;
    }
  }

  if (!added_segs_.empty()) {
    unsigned num_pages = NumPagesInClass(pc);

    auto it = added_segs_.begin();
    size_t seg_idx = it->first / kSegmentAlignment;
    CHECK_LE(segments_.size(), seg_idx);

    segments_.resize(seg_idx + 1);
    void* ptr = mi_malloc_aligned(sizeof(SegmentDescr) + (num_pages - 1) * sizeof(Page), 16_KB);
    SegmentDescr* seg = new (ptr) SegmentDescr(pc, it->first, num_pages);
    segments_[seg_idx] = seg;
    added_segs_.erase(it);

    DCHECK(sq_[pc] == NULL);
    DCHECK(seg->next == seg->prev && seg == seg->next);

    sq_[pc] = seg;
    return seg->FindPageSegment();
  }

  *seg_size = kSegmentDefaultSize;
  return nullptr;
}

void ExternalAllocator::FreePage(Page* page, SegmentDescr* owner, size_t block_size) {
  // page is fully free. Return it to the segment even if it's
  // referenced via free_pages_. The allows more elasticity by potentially reassigning
  // it to other bin sizes.
  BinIdx bidx = ToBinIdx(block_size);

  // Remove fast allocation reference.
  if (free_pages_[bidx] == page) {
    free_pages_[bidx] = &empty_page;
  }

  page->segment_inuse = 0;
  page->available = 0;

  if (!owner->HasFreePages()) {
    // Segment was fully booked but now it has a free page.
    // Add it to the tail of segment queue.
    DCHECK(owner->next == owner->prev);

    auto& sq = sq_[owner->page_class()];
    if (sq == nullptr) {
      sq = owner;
    } else {
      SegmentDescr* last = sq->prev;
      last->next = owner;
      owner->prev = last;
      owner->next = sq;
      sq->prev = owner;
    }
  }
  --owner->used_;
}

inline auto ExternalAllocator::ToSegDescr(Page* page) -> SegmentDescr* {
  uintptr_t ptr = (uintptr_t)page;
  uintptr_t seg_ptr = ptr & ~uintptr_t(16_KB - 1);  // align to 16KB boundary.
  SegmentDescr* res = reinterpret_cast<SegmentDescr*>(seg_ptr);

  DCHECK(res->GetPage(page->id) == page);

  return res;
}

}  // namespace dfly
