// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/lzfP.h"
#include "redis/zmalloc.h"
}

#include <absl/base/macros.h>
#include <absl/base/optimization.h>

#include "base/logging.h"

using namespace std;

/* Maximum size in bytes of any multi-element listpack.
 * Larger values will live in their own isolated listpacks.
 * This is used only if we're limited by record count. when we're limited by
 * size, the maximum limit is bigger, but still safe.
 * 8k is a recommended / default size limit */
#define SIZE_SAFETY_LIMIT 8192
#define sizeMeetsSafetyLimit(sz) ((sz) <= SIZE_SAFETY_LIMIT)

/* Maximum estimate of the listpack entry overhead.
 * Although in the worst case(sz < 64), we will waste 6 bytes in one
 * quicklistNode, but can avoid memory waste due to internal fragmentation
 * when the listpack exceeds the size limit by a few bytes (e.g. being 16388). */
#define SIZE_ESTIMATE_OVERHEAD 8

/* Minimum listpack size in bytes for attempting compression. */
#define MIN_COMPRESS_BYTES 48

/* Minimum size reduction in bytes to store compressed quicklistNode data.
 * This also prevents us from storing compression if the compression
 * resulted in a larger size than the original data. */
#define MIN_COMPRESS_IMPROVE 8

/* This macro is used to compress a node.
 *
 * If the 'recompress' flag of the node is true, we compress it directly without
 * checking whether it is within the range of compress depth.
 * However, it's important to ensure that the 'recompress' flag of head and tail
 * is always false, as we always assume that head and tail are not compressed.
 *
 * If the 'recompress' flag of the node is false, we check whether the node is
 * within the range of compress depth before compressing it. */
#define quicklistCompress(_node) \
  do {                           \
    if ((_node)->recompress)     \
      CompressNode((_node));     \
    else                         \
      Compress(_node);           \
  } while (0)

namespace dfly {

namespace {

enum IterDir : uint8_t { FWD = 1, REV = 0 };

/* This is for test suite development purposes only, 0 means disabled. */
static size_t packed_threshold = 0;

/* Optimization levels for size-based filling.
 * Note that the largest possible limit is 64k, so even if each record takes
 * just one byte, it still won't overflow the 16 bit count field. */
const size_t kOptLevel[] = {4096, 8192, 16384, 32768, 65536};

/* Calculate the size limit of the quicklist node based on negative 'fill'. */
size_t NodeNegFillLimit(int fill) {
  DCHECK_LT(fill, 0);

  size_t offset = (-fill) - 1;
  constexpr size_t max_level = ABSL_ARRAYSIZE(kOptLevel);
  if (offset >= max_level)
    offset = max_level - 1;
  return kOptLevel[offset];
}

const uint8_t* uint_ptr(string_view sv) {
  return reinterpret_cast<const uint8_t*>(sv.data());
}

bool IsLargeElement(size_t sz, int fill) {
  if (ABSL_PREDICT_FALSE(packed_threshold != 0))
    return sz >= packed_threshold;
  if (fill >= 0)
    return !sizeMeetsSafetyLimit(sz);
  else
    return sz > NodeNegFillLimit(fill);
}

bool NodeAllowInsert(const quicklistNode* node, const int fill, const size_t sz) {
  if (ABSL_PREDICT_FALSE(!node))
    return false;

  if (ABSL_PREDICT_FALSE(QL_NODE_IS_PLAIN(node) || IsLargeElement(sz, fill)))
    return false;

  /* Estimate how many bytes will be added to the listpack by this one entry.
   * We prefer an overestimation, which would at worse lead to a few bytes
   * below the lowest limit of 4k (see optimization_level).
   * Note: No need to check for overflow below since both `node->sz` and
   * `sz` are to be less than 1GB after the plain/large element check above. */
  size_t new_sz = node->sz + sz + SIZE_ESTIMATE_OVERHEAD;
  return !quicklistNodeExceedsLimit(fill, new_sz, node->count + 1);
}

quicklistNode* CreateNode() {
  quicklistNode* node = (quicklistNode*)zmalloc(sizeof(*node));
  node->entry = NULL;
  node->count = 0;
  node->sz = 0;
  node->next = node->prev = NULL;
  node->encoding = QUICKLIST_NODE_ENCODING_RAW;
  node->container = QUICKLIST_NODE_CONTAINER_PACKED;
  node->recompress = 0;
  node->dont_compress = 0;
  return node;
}

quicklistNode* CreateNode(int container, string_view value) {
  quicklistNode* new_node = CreateNode();
  new_node->container = container;
  new_node->sz = value.size();
  new_node->count++;

  if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
    DCHECK(!value.empty());
    new_node->entry = (uint8_t*)zmalloc(new_node->sz);
    memcpy(new_node->entry, value.data(), new_node->sz);
  } else {
    new_node->entry = lpPrepend(lpNew(0), uint_ptr(value), new_node->sz);
  }

  return new_node;
}

void NodeUpdateSz(quicklistNode* node) {
  node->sz = lpBytes((node)->entry);
}

/* Compress the listpack in 'node' and update encoding details.
 * Returns true if listpack compressed successfully.
 * Returns false if compression failed or if listpack too small to compress. */
bool CompressNode(quicklistNode* node) {
#ifdef SERVER_TEST
  node->attempted_compress = 1;
#endif
  if (node->dont_compress)
    return false;

  /* validate that the node is neither
   * tail nor head (it has prev and next)*/
  assert(node->prev && node->next);

  node->recompress = 0;
  /* Don't bother compressing small values */
  if (node->sz < MIN_COMPRESS_BYTES)
    return false;

  // ROMAN: we allocate LZF_STATE on heap, piggy-backing on the existing allocation.
  char* uptr = (char*)zmalloc(sizeof(quicklistLZF) + node->sz + sizeof(LZF_STATE));
  quicklistLZF* lzf = (quicklistLZF*)uptr;
  LZF_HSLOT* sdata = (LZF_HSLOT*)(uptr + sizeof(quicklistLZF) + node->sz);

  /* Cancel if compression fails or doesn't compress small enough */
  if (((lzf->sz = lzf_compress(node->entry, node->sz, lzf->compressed, node->sz, sdata)) == 0) ||
      lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz) {
    /* lzf_compress aborts/rejects compression if value not compressible. */
    zfree(lzf);
    return false;
  }

  lzf = (quicklistLZF*)zrealloc(lzf, sizeof(*lzf) + lzf->sz);
  zfree(node->entry);
  node->entry = (unsigned char*)lzf;
  node->encoding = QUICKLIST_NODE_ENCODING_LZF;
  return true;
}

/* Uncompress the listpack in 'node' and update encoding details.
 * Returns 1 on successful decode, 0 on failure to decode. */
bool DecompressNode(bool recompress, quicklistNode* node) {
  node->recompress = int(recompress);

  void* decompressed = zmalloc(node->sz);
  quicklistLZF* lzf = (quicklistLZF*)node->entry;
  if (lzf_decompress(lzf->compressed, lzf->sz, decompressed, node->sz) == 0) {
    /* Someone requested decompress, but we can't decompress.  Not good. */
    zfree(decompressed);
    return false;
  }
  zfree(lzf);
  node->entry = (uint8_t*)decompressed;
  node->encoding = QUICKLIST_NODE_ENCODING_RAW;
  return true;
}

/* Decompress only compressed nodes. */
void DecompressNodeIfNeeded(bool recompress, quicklistNode* node) {
  if ((node) && (node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {
    DecompressNode(recompress, node);
  }
}

}  // namespace

QList::QList() : fill_(-2), compress_(0), bookmark_count_(0) {
}

QList::QList(int fill, int compress) : fill_(fill), compress_(compress), bookmark_count_(0) {
}

QList::~QList() {
  unsigned long len;
  quicklistNode *current, *next;

  current = head_;
  len = len_;
  while (len--) {
    next = current->next;

    zfree(current->entry);
    count_ -= current->count;

    zfree(current);

    len_--;
    current = next;
  }
}

void QList::Push(string_view value, Where where) {
  /* The head and tail should never be compressed (we don't attempt to decompress them) */
  if (head_)
    DCHECK(head_->encoding != QUICKLIST_NODE_ENCODING_LZF);
  if (tail_)
    DCHECK(tail_->encoding != QUICKLIST_NODE_ENCODING_LZF);

  if (where == HEAD) {
    PushHead(value);
  } else {
    DCHECK_EQ(TAIL, where);
    PushTail(value);
  }
}

void QList::AppendListpack(unsigned char* zl) {
}

void QList::AppendPlain(unsigned char* zl) {
}

void QList::Insert(std::string_view pivot, std::string_view elem, InsertOpt opt) {
}

size_t QList::MallocUsed() const {
  // Approximation since does not account for listpacks.
  size_t res = len_ * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  return res + count_ * 16;  // we account for each member 16 bytes.
}

string QList::Peek(Where where) const {
  return {};
}

optional<string> QList::Get(long index) const {
  return nullopt;
}

void QList::Iterate(IterateFunc cb, long start, long end) const {
}

bool QList::PushHead(string_view value) {
  quicklistNode* orig = head_;
  size_t sz = value.size();
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    InsertPlainNode(head_, value, false);
    return true;
  }

  // TODO: we can deduplicate this code with PushTail once we complete the functionality
  // of this class.
  count_++;

  if (ABSL_PREDICT_TRUE(NodeAllowInsert(head_, fill_, sz))) {
    head_->entry = lpPrepend(head_->entry, uint_ptr(value), sz);
    NodeUpdateSz(head_);
  } else {
    quicklistNode* node = CreateNode();
    node->entry = lpPrepend(lpNew(0), uint_ptr(value), sz);

    NodeUpdateSz(node);
    InsertNode(head_, node, false);
  }

  head_->count++;
  return (orig != head_);
}

// Returns false if used existing head, true if new head created.
bool QList::PushTail(string_view value) {
  quicklistNode* orig = tail_;
  size_t sz = value.size();
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    InsertPlainNode(orig, value, true);
    return true;
  }

  count_++;
  if (ABSL_PREDICT_TRUE(NodeAllowInsert(orig, fill_, sz))) {
    orig->entry = lpAppend(orig->entry, uint_ptr(value), sz);
    NodeUpdateSz(orig);
  } else {
    quicklistNode* node = CreateNode();
    node->entry = lpAppend(lpNew(0), uint_ptr(value), sz);

    NodeUpdateSz(node);
    InsertNode(orig, node, true);
  }
  tail_->count++;
  return (orig != tail_);
}

void QList::InsertPlainNode(quicklistNode* old_node, string_view value, bool after) {
  quicklistNode* new_node = CreateNode(QUICKLIST_NODE_CONTAINER_PLAIN, value);
  InsertNode(old_node, new_node, after);
  count_++;
}

void QList::InsertNode(quicklistNode* old_node, quicklistNode* new_node, bool after) {
  if (after) {
    new_node->prev = old_node;
    if (old_node) {
      new_node->next = old_node->next;
      if (old_node->next)
        old_node->next->prev = new_node;
      old_node->next = new_node;
    }
    if (tail_ == old_node)
      tail_ = new_node;
  } else {
    new_node->next = old_node;
    if (old_node) {
      new_node->prev = old_node->prev;
      if (old_node->prev)
        old_node->prev->next = new_node;
      old_node->prev = new_node;
    }
    if (head_ == old_node)
      head_ = new_node;
  }
  /* If this insert creates the only element so far, initialize head/tail. */
  if (len_ == 0) {
    head_ = tail_ = new_node;
  }

  /* Update len first, so in __quicklistCompress we know exactly len */
  len_++;

  if (old_node)
    quicklistCompress(old_node);

  quicklistCompress(new_node);
}

/* Force 'quicklist' to meet compression guidelines set by compress depth.
 * The only way to guarantee interior nodes get compressed is to iterate
 * to our "interior" compress depth then compress the next node we find.
 * If compress depth is larger than the entire list, we return immediately. */
void QList::Compress(quicklistNode* node) {
  if (len_ == 0)
    return;

  /* The head and tail should never be compressed (we should not attempt to recompress them) */
  assert(head_->recompress == 0 && tail_->recompress == 0);

  /* If length is less than our compress depth (from both sides),
   * we can't compress anything. */
  if (!AllowCompression() || len_ < (unsigned int)(compress_ * 2))
    return;

  /* Iterate until we reach compress depth for both sides of the list.a
   * Note: because we do length checks at the *top* of this function,
   *       we can skip explicit null checks below. Everything exists. */
  quicklistNode* forward = head_;
  quicklistNode* reverse = tail_;
  int depth = 0;
  int in_depth = 0;
  while (depth++ < compress_) {
    DecompressNodeIfNeeded(false, forward);
    DecompressNodeIfNeeded(false, reverse);

    if (forward == node || reverse == node)
      in_depth = 1;

    /* We passed into compress depth of opposite side of the quicklist
     * so there's no need to compress anything and we can exit. */
    if (forward == reverse || forward->next == reverse)
      return;

    forward = forward->next;
    reverse = reverse->prev;
  }

  if (!in_depth)
    CompressNode(node);

  /* At this point, forward and reverse are one node beyond depth */
  CompressNode(forward);
  CompressNode(reverse);
}

auto QList::GetIterator(Where where) -> Iterator {
  Iterator it;
  it.owner_ = this;
  it.zi_ = NULL;
  if (where == HEAD) {
    it.current_ = head_;
    it.offset_ = 0;
    it.direction_ = FWD;
  } else {
    it.current_ = tail_;
    it.offset_ = -1;
    it.direction_ = REV;
  }

  return it;
}

bool QList::Iterator::Next() {
  DCHECK(current_);

  unsigned char* (*nextFn)(unsigned char*, unsigned char*) = NULL;
  int offset_update = 0;

  int plain = QL_NODE_IS_PLAIN(current_);
  if (!zi_) {
    /* If !zi, use current index. */
    DecompressNodeIfNeeded(true, current_);
    if (ABSL_PREDICT_FALSE(plain))
      zi_ = current_->entry;
    else
      zi_ = lpSeek(current_->entry, offset_);
  } else if (ABSL_PREDICT_FALSE(plain)) {
    zi_ = NULL;
  } else {
    /* else, use existing iterator offset and get prev/next as necessary. */
    if (direction_ == FWD) {
      nextFn = lpNext;
      offset_update = 1;
    } else {
      DCHECK_EQ(REV, direction_);
      nextFn = lpPrev;
      offset_update = -1;
    }
    zi_ = nextFn(current_->entry, zi_);
    offset_ += offset_update;
  }

  if (zi_)
    return true;

  // Retry again with the next node.
  owner_->Compress(current_);

  if (direction_ == FWD) {
    /* Forward traversal, Jumping to start of next node */
    current_ = current_->next;
    offset_ = 0;
  } else {
    /* Reverse traversal, Jumping to end of previous node */
    DCHECK_EQ(REV, direction_);

    current_ = current_->prev;
    offset_ = -1;
  }

  return current_ ? Next() : false;
}

auto QList::Iterator::Get() const -> Entry {
  int plain = QL_NODE_IS_PLAIN(current_);
  if (ABSL_PREDICT_FALSE(plain)) {
    char* str = reinterpret_cast<char*>(current_->entry);
    return Entry(str, current_->sz);
  }

  /* Populate value from existing listpack position */
  unsigned int sz = 0;
  long long val;
  uint8_t* ptr = lpGetValue(zi_, &sz, &val);

  return ptr ? Entry(reinterpret_cast<char*>(ptr), sz) : Entry(val);
}

}  // namespace dfly
