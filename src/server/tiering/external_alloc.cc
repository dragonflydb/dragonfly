// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/server/tiering/external_alloc.h"

#include <mimalloc.h>

#include <bitset>
#include <cstring>

#include "base/logging.h"

namespace dfly::tiering {
using namespace std;
using detail::PageClass;

using BinIdx = uint8_t;

namespace {

constexpr inline size_t divup(size_t num, size_t div) {
  return (num + div - 1) / div;
}

constexpr inline size_t alignup(size_t num, size_t align) {
  size_t amask = align - 1;
  return (num + amask) & (~amask);
}

constexpr inline size_t wsize_from_size(size_t size) {
  return divup(size, sizeof(uintptr_t));
}

constexpr size_t kMinBlockSize = ExternalAllocator::kMinBlockSize;

constexpr size_t kSmallPageShift = 20;
constexpr size_t kMediumPageShift = 24;
constexpr size_t kSmallPageSize = 1UL << kSmallPageShift;    // 1MB
constexpr size_t kMediumPageSize = 1UL << kMediumPageShift;  // 16MB

// we preserve 16:1 ratio, i.e. each page can host at least 16 blocks within its class.
constexpr size_t kSmallObjMaxSize = kSmallPageSize / 16;
constexpr size_t kMediumObjMaxSize = kMediumPageSize / 16;

constexpr size_t kSegmentAlignment = 256_MB;
constexpr size_t kSegmentSize = 256_MB;

constexpr unsigned kNumBins = detail::kNumFreePages;
constexpr unsigned kLargeSizeBin = kNumBins - 1;
constexpr unsigned kMaxPagesInSegment = kSegmentSize / kSmallPageSize;
constexpr unsigned kSegDescrAlignment = 16_KB;

constexpr size_t kBinWordLens[kNumBins] = {
    512,   512 * 2, 512 * 3, 2048,  2560,  3072,  3584,   4096,   5120,      6144,
    7168,  8192,    10240,   12288, 14336, 16384, 20480,  24576,  28672,     32768,
    40960, 49152,   57344,   65536, 81920, 98304, 114688, 131072, UINT64_MAX};

static_assert(kBinWordLens[kLargeSizeBin - 1] * 8 == kMediumObjMaxSize);
static_assert(kBinWordLens[kLargeSizeBin] == UINT64_MAX);

constexpr inline BinIdx ToBinIdx(size_t size) {
  // first 4 bins are multiplies of kMinBlockSize.
  if (size < ExternalAllocator::kMinBlockSize * 4) {
    return size <= ExternalAllocator::kMinBlockSize ? 0
                                                    : (size - 1) / ExternalAllocator::kMinBlockSize;
  }

  if (size > kMediumObjMaxSize) {
    return kLargeSizeBin;
  }

  size_t wsize = wsize_from_size(size);

  // to correct rounding up of size to words that the last word will be within the range.
  --wsize;

  // find the highest bit
  uint8_t b = 63 - __builtin_clzl(wsize);
  return (b << 2) + ((wsize >> (b - 2)) & 3) - 40;
}

static_assert(ToBinIdx(kMinBlockSize) == 0);
static_assert(ToBinIdx(kMinBlockSize * 2) == 1);
static_assert(ToBinIdx(kMinBlockSize * 3) == 2);
static_assert(ToBinIdx(kMinBlockSize * 4) == 3);
static_assert(ToBinIdx(kMinBlockSize * 5) == 4);
static_assert(ToBinIdx(kMinBlockSize * 6) == 5);
static_assert(ToBinIdx(kMinBlockSize * 6 + 1) == 6);
static_assert(ToBinIdx(kMinBlockSize * 7) == 6);

size_t ToBlockSize(BinIdx idx) {
  return kBinWordLens[idx] * 8;
}

// num pages in a segment of that class.
unsigned NumPagesInSegment(PageClass pc) {
  switch (pc) {
    case PageClass::SMALL_P:
      return kSegmentSize >> kSmallPageShift;
    case PageClass::MEDIUM_P:
      return kSegmentSize >> kMediumPageShift;
      break;
    case PageClass::LARGE_P:
      return 1;
      break;
  }
  // unreachable.
  return 0;
}

template <size_t N> size_t FindFirst(const std::bitset<N>& bs) {
#ifdef _LIBCPP_VERSION
  for (size_t i = 0; i < bs.size(); ++i) {
    if (bs.test(i))
      return i;
  }
#else
  return bs._Find_first();
#endif
}

};  // namespace

/*
   block 8Kb or more, page - 2MB (256 blocks) or bigger.


   Block sizes grow exponentially - by factor ~1.25. See MI_PAGE_QUEUES_EMPTY definition
   for sizes example.
*/
namespace detail {

// Page can be exactly in either these 3 states:
// 1. unitialized - with no blocks being allocated - segment_inuse will be 0 in that case, 1
// otherwise.
// 2. Partly utilized by 1 or more blocks, with available > 0 in that case. It must be present in
// free_pages_ list then.
// 3. Fully utilized, with available==0, in that case it's not part of free_pages_ list.
struct Page {
  std::bitset<256> free_blocks;  // bitmask of free blocks (32 bytes).
  uint8_t id;                    // index inside the Segment.pages array.

  // need some mapping function to map from block_size to real_block_size given Page class.
  BinIdx bin_idx;
  uint8_t segment_inuse : 1;  // true if segment allocated this page.
  uint8_t reserved[3];

  // can be computed via free_blocks.count().
  uint16_t available;  // in number of blocks.
  Page* next_free;     // next page in the free_pages_ list

  // We can not use c'tor because we use the trick in segment where we allocate more pages
  // than SegmentDescr declares.
  void Reset(uint8_t new_id) {
    static_assert(sizeof(Page) == 48);

    memset(&id, 0, sizeof(Page) - offsetof(Page, id));
    id = new_id;
  }

  void Init(PageClass pc, BinIdx bin_id);
};

constexpr size_t kSegDescrDataSize = sizeof(Page) * kMaxPagesInSegment + 128;
static_assert(kSegDescrDataSize < kSegDescrAlignment);

void Page::Init(PageClass pc, BinIdx bin_id) {
  DCHECK_EQ(available, 0);
  DCHECK(segment_inuse);

  bin_idx = bin_id;
  if (pc == PageClass::LARGE_P) {
    available = 1;
  } else {
    size_t page_size = (pc == PageClass::SMALL_P) ? kSmallPageSize : kMediumPageSize;
    available = page_size / ToBlockSize(bin_id);
  }

  free_blocks.reset();
  for (unsigned i = 0; i < available; ++i) {
    free_blocks.set(i, true);
  }
}

PageClass ClassFromSize(size_t size) {
  if (size <= kSmallObjMaxSize)
    return PageClass::SMALL_P;
  if (size <= kMediumObjMaxSize)
    return PageClass::MEDIUM_P;

  return PageClass::LARGE_P;
}

}  // namespace detail

//
/**
 * SegmentDescr denotes a 256MB segment on external storage -
 * holds upto 256 pages (in case of small pages).
 * Each segment has pages of the same type, but each page can host blocks of
 * different sizes upto maximal block size for that page class.
 * SegmentDescr points to the range within external storage space.
 * By using the page.id together with segment->page_shift and segment->offset
 * one can know where the page is located in the storage.
 * Opposite direction: by giving an offset to the file, segment_id = offset / 256MB.
 * Moreover (offset % 256MB) >> segment.page_shift gives us the page id and subsequently
 * page_start.  segment.pages[page_id].block_size gives us the block size and that in turn gives us
 * block id within the page. We can also know block_size if the originally allocated
   size is provided by using round_up function that was used to allocate the block.
 * SegmentDescr be aligned by kSegDescrAlignment boundaries - ToSegDescr relies on that.
 */
class ExternalAllocator::SegmentDescr {
  SegmentDescr(const SegmentDescr&) = delete;
  void operator=(const SegmentDescr&) = delete;
  friend class ExternalAllocator;

 public:
  explicit SegmentDescr(PageClass pc, size_t offs, uint16_t capacity);

  Page* FindPageSegment() {
    return page_info_.FindPageSegment();
  }

  Page* GetPage(unsigned i) {
    return page_info_.pages + i;
  }

  size_t BlockOffset(const Page* page, unsigned blockpos) {
    return offset_ + page->id * (1 << page_info_.page_shift) +
           ToBlockSize(page->bin_idx) * blockpos;
  }

  bool HasFreePages() const {
    return page_info_.capacity > page_info_.used;
  }

  unsigned capacity() const {
    return page_info_.capacity;
  }

  unsigned used() const {
    return page_info_.used;
  }

  unsigned page_shift() const {
    return page_info_.page_shift;
  }

  PageClass page_class() const {
    return page_class_;
  }

  SegmentDescr *next, *prev;

  // Links seg before this.
  void LinkBefore(SegmentDescr* seg) {
    seg->next = this;
    seg->prev = prev;
    this->prev->next = seg;
    this->prev = seg;
  }

  // detaches this from the circular list.
  // returns next if the list is has more than 1 element
  // returns null otherwise.
  SegmentDescr* Detach() {
    if (next == this)
      return nullptr;

    next->prev = prev;
    prev->next = next;

    SegmentDescr* res = next;
    next = prev = this;
    return res;
  }

 private:
  uint64_t offset_;  // size_ - relevant for large segments.
  PageClass page_class_;

  struct PageInfo {
    uint16_t capacity, used;  // in number of pages.
    uint8_t page_shift;
    Page pages[0];  // must be the last field. Can be 1-256 pages.

    PageInfo(uint16_t c) : capacity(c), used(0), page_shift(0) {
    }

    auto FindPageSegment() -> Page* {
      for (uint32_t i = 0; i < capacity; ++i) {
        if (!pages[i].segment_inuse) {
          pages[i].segment_inuse = 1;
          ++used;
          return pages + i;
        }
      }

      LOG(DFATAL) << "Should not reach here";

      return nullptr;
    }
  };

  PageInfo page_info_;
};

ExternalAllocator::SegmentDescr::SegmentDescr(PageClass pc, size_t offs, uint16_t page_capacity)
    : offset_(offs), page_class_(pc), page_info_(page_capacity) {
  constexpr size_t kDescrSize = sizeof(SegmentDescr);
  (void)kDescrSize;

  next = prev = this;
  DCHECK(pc != PageClass::LARGE_P);

  if (pc == PageClass::MEDIUM_P)
    page_info_.page_shift = kMediumPageShift;
  else
    page_info_.page_shift = kSmallPageShift;

  for (unsigned i = 0; i < page_capacity; ++i) {
    page_info_.pages[i].Reset(i);
  }
}

static detail::Page empty_page;

ExternalAllocator::ExternalAllocator() {
  std::fill(sq_, sq_ + ABSL_ARRAYSIZE(sq_), nullptr);
  std::fill(free_pages_, free_pages_ + detail::kNumFreePages, &empty_page);
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
    PageClass pc = detail::ClassFromSize(sz);
    if (pc == PageClass::LARGE_P) {
      return LargeMalloc(sz);
    }

    page = FindPage(pc);
    if (!page)
      return -int64_t(kSegmentSize);

    free_pages_[bin_idx] = page;
    page->Init(pc, bin_idx);
  }

  DCHECK(page->available);
  size_t pos = FindFirst(page->free_blocks);
  page->free_blocks.flip(pos);

  if (--page->available == 0)  // Remove empty page from freelist
    free_pages_[bin_idx] = page->next_free ? page->next_free : &empty_page;

  allocated_bytes_ += ToBlockSize(page->bin_idx);
  SegmentDescr* seg = ToSegDescr(page);
  return seg->BlockOffset(page, pos);
}

void ExternalAllocator::Free(size_t offset, size_t sz) {
  if (sz > kMediumObjMaxSize) {
    size_t align_sz = alignup(sz, 4_KB);
    extent_tree_.Add(offset, align_sz);
    return;
  }

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
  unsigned block_size = ToBlockSize(page->bin_idx);
  unsigned block_id = block_offs / block_size;
  unsigned blocks_num = page_size / block_size;

  CHECK_LE(sz, block_size);
  DCHECK_LT(block_id, blocks_num);
  DCHECK(!page->free_blocks[block_id]) << offset;

  page->free_blocks.set(block_id);
  ++page->available;

  DCHECK_EQ(page->available, page->free_blocks.count());
  // If page becomes fully free, return it to segment list, otherwise if it just became non-empty,
  // then return it to free pages list
  if (page->available == blocks_num) {
    FreePage(page, seg, block_size);
  } else if (page->available == 1) {
    DCHECK_NE(page, free_pages_[page->bin_idx]);
    page->next_free = free_pages_[page->bin_idx];
    free_pages_[page->bin_idx] = page;
  }
  allocated_bytes_ -= block_size;
}

void ExternalAllocator::AddStorage(size_t start, size_t size) {
  VLOG(1) << "AddStorage " << start << "/" << size;

  extent_tree_.Add(start, size);
  capacity_ += size;
}

size_t ExternalAllocator::GoodSize(size_t sz) {
  uint8_t bin_idx = ToBinIdx(sz);
  if (bin_idx < kLargeSizeBin)
    return ToBlockSize(bin_idx);

  return alignup(sz, 4_KB);
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
auto ExternalAllocator::FindPage(PageClass pc) -> Page* {
  DCHECK_NE(pc, PageClass::LARGE_P);

  SegmentDescr* seg = sq_[pc];
  while (seg) {
    if (seg->HasFreePages()) {
      return seg->FindPageSegment();
    }

    // remove head.
    SegmentDescr* next = seg->Detach();
    sq_[pc] = next;
    seg = next;
  }

  // no pages in the existing segments. Lets search in the extent tree.
  auto op_range = extent_tree_.GetRange(kSegmentSize, kSegmentAlignment);
  if (op_range) {
    DCHECK_EQ(0u, op_range->first % kSegmentAlignment);

    unsigned num_pages = NumPagesInSegment(pc);
    size_t seg_idx = op_range->first / kSegmentAlignment;

    if (segments_.size() > seg_idx) {
      DCHECK(segments_[seg_idx] == nullptr);
    } else {
      segments_.resize(seg_idx + 1);
    }

    void* ptr =
        mi_malloc_aligned(sizeof(SegmentDescr) + num_pages * sizeof(Page), kSegDescrAlignment);
    SegmentDescr* seg = new (ptr) SegmentDescr(pc, op_range->first, num_pages);
    segments_[seg_idx] = seg;

    DCHECK(sq_[pc] == NULL);
    DCHECK(seg->next == seg->prev && seg == seg->next);

    sq_[pc] = seg;
    return seg->FindPageSegment();
  }

  return nullptr;
}

int64_t ExternalAllocator::LargeMalloc(size_t size) {
  size_t align_sz = alignup(size, 4_KB);
  auto op_range = extent_tree_.GetRange(align_sz, 4_KB);
  if (!op_range) {
    align_sz = max(align_sz, kSegmentSize);
    return -int64_t(align_sz);
  }

  return op_range->first;
}

void ExternalAllocator::FreePage(Page* page, SegmentDescr* owner, size_t block_size) {
  // page is fully free. Return it to the segment even if it's
  // referenced via free_pages_. The allows more elasticity by potentially reassigning
  // it to other bin sizes.
  BinIdx bidx = ToBinIdx(block_size);

  // Remove fast allocation reference.
  if (free_pages_[bidx] == page) {
    free_pages_[bidx] = page->next_free ? page->next_free : &empty_page;
  } else {
    for (auto* cur = free_pages_[bidx]; cur != nullptr; cur = cur->next_free) {
      if (cur->next_free == page) {
        cur->next_free = page->next_free;
        break;
      }
    }
  }

  page->segment_inuse = 0;
  page->available = 0;
  page->next_free = nullptr;

  if (!owner->HasFreePages()) {
    // Segment was fully booked but now it has a free page.
    // Add it to the tail of segment queue.
    DCHECK(owner->next == owner->prev);

    auto& sq = sq_[owner->page_class()];
    if (sq == nullptr) {
      sq = owner;
    } else {
      sq->LinkBefore(owner);
    }
  }
  --owner->page_info_.used;
}

inline auto ExternalAllocator::ToSegDescr(Page* page) -> SegmentDescr* {
  uintptr_t ptr = (uintptr_t)page;

  // find SegDescr boundary.
  uintptr_t seg_ptr = ptr & ~uintptr_t(kSegDescrAlignment - 1);
  SegmentDescr* res = reinterpret_cast<SegmentDescr*>(seg_ptr);

  DCHECK(res->GetPage(page->id) == page);

  return res;
}

}  // namespace dfly::tiering
