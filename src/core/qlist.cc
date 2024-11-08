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
#include <absl/strings/str_cat.h>

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

bool NodeAllowMerge(const quicklistNode* a, const quicklistNode* b, const int fill) {
  if (!a || !b)
    return false;

  if (ABSL_PREDICT_FALSE(QL_NODE_IS_PLAIN(a) || QL_NODE_IS_PLAIN(b)))
    return false;

  /* approximate merged listpack size (- 7 to remove one listpack
   * header/trailer, see LP_HDR_SIZE and LP_EOF) */
  unsigned int merge_sz = a->sz + b->sz - 7;
  return quicklistNodeExceedsLimit(fill, merge_sz, a->count + b->count);
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

uint8_t* LP_FromElem(string_view elem) {
  return lpPrepend(lpNew(0), uint_ptr(elem), elem.size());
}

uint8_t* LP_Insert(uint8_t* lp, string_view elem, uint8_t* pos, int lp_where) {
  return lpInsertString(lp, uint_ptr(elem), elem.size(), pos, lp_where, NULL);
}

uint8_t* LP_Append(uint8_t* lp, string_view elem) {
  return lpAppend(lp, uint_ptr(elem), elem.size());
}

uint8_t* LP_Prepend(uint8_t* lp, string_view elem) {
  return lpPrepend(lp, uint_ptr(elem), elem.size());
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
    new_node->entry = LP_FromElem(value);
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

/* Decompress only compressed nodes.
   recompress: if true, the node will be marked for recompression after decompression.
*/
void DecompressNodeIfNeeded(bool recompress, quicklistNode* node) {
  if ((node) && (node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {
    DecompressNode(recompress, node);
  }
}

void RecompressOnly(quicklistNode* node) {
  if (node->recompress && !node->dont_compress) {
    CompressNode(node);
  }
}

bool ElemCompare(const QList::Entry& entry, string_view elem) {
  if (entry.value) {
    return entry.view() == elem;
  }

  absl::AlphaNum an(entry.longval);
  return elem == an.Piece();
}

quicklistNode* SplitNode(quicklistNode* node, int offset, bool after) {
  size_t zl_sz = node->sz;

  quicklistNode* new_node = CreateNode();
  new_node->entry = (uint8_t*)zmalloc(zl_sz);

  /* Copy original listpack so we can split it */
  memcpy(new_node->entry, node->entry, zl_sz);

  /* Need positive offset for calculating extent below. */
  if (offset < 0)
    offset = node->count + offset;

  /* Ranges to be trimmed: -1 here means "continue deleting until the list ends" */
  int orig_start = after ? offset + 1 : 0;
  int orig_extent = after ? -1 : offset;
  int new_start = after ? 0 : offset;
  int new_extent = after ? offset + 1 : -1;

  node->entry = lpDeleteRange(node->entry, orig_start, orig_extent);
  node->count = lpLength(node->entry);
  NodeUpdateSz(node);

  new_node->entry = lpDeleteRange(new_node->entry, new_start, new_extent);
  new_node->count = lpLength(new_node->entry);
  NodeUpdateSz(new_node);

  return new_node;
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
  quicklistNode* node = CreateNode();
  node->entry = zl;
  node->count = lpLength(node->entry);
  node->sz = lpBytes(zl);

  InsertNode(tail_, node, AFTER);
  count_ += node->count;
}

void QList::AppendPlain(unsigned char* data, size_t sz) {
  quicklistNode* node = CreateNode();

  node->entry = data;
  node->count = 1;
  node->sz = sz;
  node->container = QUICKLIST_NODE_CONTAINER_PLAIN;

  InsertNode(tail_, node, AFTER);
  ++count_;
}

bool QList::Insert(std::string_view pivot, std::string_view elem, InsertOpt opt) {
  Iterator it = GetIterator(HEAD);

  while (it.Next()) {
    if (ElemCompare(it.Get(), pivot)) {
      Insert(it, elem, opt);
      return true;
    }
  }

  return false;
}

bool QList::Replace(long index, std::string_view elem) {
  Iterator it = GetIterator(index);
  if (it.Next()) {
    Replace(it, elem);
    return true;
  }
  return false;
}

size_t QList::MallocUsed() const {
  // Approximation since does not account for listpacks.
  size_t res = len_ * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  return res + count_ * 16;  // we account for each member 16 bytes.
}

void QList::Iterate(IterateFunc cb, long start, long end) const {
  long llen = Size();
  if (llen == 0)
    return;

  if (end < 0 || end >= long(Size()))
    end = Size() - 1;
  Iterator it = GetIterator(start);
  while (start <= end && it.Next()) {
    if (!cb(it.Get()))
      break;
    start++;
  }
}

bool QList::PushHead(string_view value) {
  quicklistNode* orig = head_;
  size_t sz = value.size();
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    InsertPlainNode(head_, value, BEFORE);
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
    node->entry = LP_FromElem(value);

    NodeUpdateSz(node);
    InsertNode(head_, node, BEFORE);
  }

  head_->count++;
  return (orig != head_);
}

// Returns false if used existing head, true if new head created.
bool QList::PushTail(string_view value) {
  quicklistNode* orig = tail_;
  size_t sz = value.size();
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    InsertPlainNode(orig, value, AFTER);
    return true;
  }

  count_++;
  if (ABSL_PREDICT_TRUE(NodeAllowInsert(orig, fill_, sz))) {
    orig->entry = lpAppend(orig->entry, uint_ptr(value), sz);
    NodeUpdateSz(orig);
  } else {
    quicklistNode* node = CreateNode();
    node->entry = LP_FromElem(value);

    NodeUpdateSz(node);
    InsertNode(orig, node, AFTER);
  }
  tail_->count++;
  return (orig != tail_);
}

void QList::InsertPlainNode(quicklistNode* old_node, string_view value, InsertOpt insert_opt) {
  quicklistNode* new_node = CreateNode(QUICKLIST_NODE_CONTAINER_PLAIN, value);
  InsertNode(old_node, new_node, insert_opt);
  count_++;
}

void QList::InsertNode(quicklistNode* old_node, quicklistNode* new_node, InsertOpt insert_opt) {
  if (insert_opt == AFTER) {
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

  /* Update len first, so in Compress we know exactly len */
  len_++;

  if (old_node)
    quicklistCompress(old_node);

  quicklistCompress(new_node);
}

void QList::Insert(Iterator it, std::string_view elem, InsertOpt insert_opt) {
  int full = 0, at_tail = 0, at_head = 0, avail_next = 0, avail_prev = 0;
  quicklistNode* node = it.current_;
  quicklistNode* new_node = NULL;
  size_t sz = elem.size();
  bool after = insert_opt == AFTER;

  if (!node) {
    /* we have no reference node, so let's create only node in the list */
    if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
      InsertPlainNode(tail_, elem, insert_opt);
      return;
    }
    new_node = CreateNode();
    new_node->entry = LP_FromElem(elem);
    InsertNode(NULL, new_node, insert_opt);
    new_node->count++;
    count_++;
    return;
  }

  /* Populate accounting flags for easier boolean checks later */
  if (!NodeAllowInsert(node, fill_, sz)) {
    full = 1;
  }

  if (after && (it.offset_ == node->count - 1 || it.offset_ == -1)) {
    at_tail = 1;
    if (NodeAllowInsert(node->next, fill_, sz)) {
      avail_next = 1;
    }
  }

  if (!after && (it.offset_ == 0 || it.offset_ == -(node->count))) {
    at_head = 1;
    if (NodeAllowInsert(node->prev, fill_, sz)) {
      avail_prev = 1;
    }
  }

  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    if (QL_NODE_IS_PLAIN(node) || (at_tail && after) || (at_head && !after)) {
      InsertPlainNode(node, elem, insert_opt);
    } else {
      DecompressNodeIfNeeded(true, node);
      new_node = SplitNode(node, it.offset_, after);
      quicklistNode* entry_node = CreateNode(QUICKLIST_NODE_CONTAINER_PLAIN, elem);
      InsertNode(node, entry_node, insert_opt);
      InsertNode(entry_node, new_node, insert_opt);
      count_++;
    }
    return;
  }

  /* Now determine where and how to insert the new element */
  if (!full && after) {
    DecompressNodeIfNeeded(true, node);
    node->entry = LP_Insert(node->entry, elem, it.zi_, LP_AFTER);
    node->count++;
    NodeUpdateSz(node);
    RecompressOnly(node);
  } else if (!full && !after) {
    DecompressNodeIfNeeded(true, node);
    node->entry = LP_Insert(node->entry, elem, it.zi_, LP_BEFORE);
    node->count++;
    NodeUpdateSz(node);
    RecompressOnly(node);
  } else if (full && at_tail && avail_next && after) {
    /* If we are: at tail, next has free space, and inserting after:
     *   - insert entry at head of next node. */
    new_node = node->next;
    DecompressNodeIfNeeded(true, new_node);
    new_node->entry = LP_Prepend(new_node->entry, elem);
    new_node->count++;
    NodeUpdateSz(new_node);
    RecompressOnly(new_node);
    RecompressOnly(node);
  } else if (full && at_head && avail_prev && !after) {
    /* If we are: at head, previous has free space, and inserting before:
     *   - insert entry at tail of previous node. */
    new_node = node->prev;
    DecompressNodeIfNeeded(true, new_node);
    new_node->entry = LP_Append(new_node->entry, elem);
    new_node->count++;
    NodeUpdateSz(new_node);
    RecompressOnly(new_node);
    RecompressOnly(node);
  } else if (full && ((at_tail && !avail_next && after) || (at_head && !avail_prev && !after))) {
    /* If we are: full, and our prev/next has no available space, then:
     *   - create new node and attach to qlist */
    new_node = CreateNode();
    new_node->entry = LP_FromElem(elem);
    new_node->count++;
    NodeUpdateSz(new_node);
    InsertNode(node, new_node, insert_opt);
  } else if (full) {
    /* else, node is full we need to split it. */
    /* covers both after and !after cases */
    DecompressNodeIfNeeded(true, node);
    new_node = SplitNode(node, it.offset_, after);
    if (after)
      new_node->entry = LP_Prepend(new_node->entry, elem);
    else
      new_node->entry = LP_Append(new_node->entry, elem);
    new_node->count++;
    NodeUpdateSz(new_node);
    InsertNode(node, new_node, insert_opt);
    MergeNodes(node);
  }

  count_++;
}

void QList::Replace(Iterator it, std::string_view elem) {
  // TODO
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

/* Attempt to merge listpacks within two nodes on either side of 'center'.
 *
 * We attempt to merge:
 *   - (center->prev->prev, center->prev)
 *   - (center->next, center->next->next)
 *   - (center->prev, center)
 *   - (center, center->next)
 *
 * Returns the new 'center' after merging.
 */
quicklistNode* QList::MergeNodes(quicklistNode* center) {
  quicklistNode *prev = NULL, *prev_prev = NULL, *next = NULL;
  quicklistNode *next_next = NULL, *target = NULL;

  if (center->prev) {
    prev = center->prev;
    if (center->prev->prev)
      prev_prev = center->prev->prev;
  }

  if (center->next) {
    next = center->next;
    if (center->next->next)
      next_next = center->next->next;
  }

  /* Try to merge prev_prev and prev */
  if (NodeAllowMerge(prev, prev_prev, fill_)) {
    ListpackMerge(prev_prev, prev);
    prev_prev = prev = NULL; /* they could have moved, invalidate them. */
  }

  /* Try to merge next and next_next */
  if (NodeAllowMerge(next, next_next, fill_)) {
    ListpackMerge(next, next_next);
    next = next_next = NULL; /* they could have moved, invalidate them. */
  }

  /* Try to merge center node and previous node */
  if (NodeAllowMerge(center, center->prev, fill_)) {
    target = ListpackMerge(center->prev, center);
    center = NULL; /* center could have been deleted, invalidate it. */
  } else {
    /* else, we didn't merge here, but target needs to be valid below. */
    target = center;
  }

  /* Use result of center merge (or original) to merge with next node. */
  if (NodeAllowMerge(target, target->next, fill_)) {
    target = ListpackMerge(target, target->next);
  }
  return target;
}

/* Given two nodes, try to merge their listpacks.
 *
 * This helps us not have a quicklist with 3 element listpacks if
 * our fill factor can handle much higher levels.
 *
 * Note: 'a' must be to the LEFT of 'b'.
 *
 * After calling this function, both 'a' and 'b' should be considered
 * unusable.  The return value from this function must be used
 * instead of re-using any of the quicklistNode input arguments.
 *
 * Returns the input node picked to merge against or NULL if
 * merging was not possible. */
quicklistNode* QList::ListpackMerge(quicklistNode* a, quicklistNode* b) {
  DecompressNodeIfNeeded(false, a);
  DecompressNodeIfNeeded(false, b);
  if ((lpMerge(&a->entry, &b->entry))) {
    /* We merged listpacks! Now remove the unused quicklistNode. */
    quicklistNode *keep = NULL, *nokeep = NULL;
    if (!a->entry) {
      nokeep = a;
      keep = b;
    } else if (!b->entry) {
      nokeep = b;
      keep = a;
    }
    keep->count = lpLength(keep->entry);
    NodeUpdateSz(keep);
    keep->recompress = 0; /* Prevent 'keep' from being recompressed if
                           * it becomes head or tail after merging. */

    nokeep->count = 0;
    DelNode(nokeep);
    quicklistCompress(keep);
    return keep;
  } else {
    /* else, the merge returned NULL and nothing changed. */
    return NULL;
  }
}

void QList::DelNode(quicklistNode* node) {
  if (node->next)
    node->next->prev = node->prev;
  if (node->prev)
    node->prev->next = node->next;

  if (node == tail_) {
    tail_ = node->prev;
  }

  if (node == head_) {
    head_ = node->next;
  }

  /* Update len first, so in Compress we know exactly len */
  len_--;
  count_ -= node->count;

  /* If we deleted a node within our compress depth, we
   * now have compressed nodes needing to be decompressed. */
  Compress(NULL);

  zfree(node->entry);
  zfree(node);
}

auto QList::GetIterator(Where where) const -> Iterator {
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

auto QList::GetIterator(long idx) const -> Iterator {
  quicklistNode* n;
  unsigned long long accum = 0;
  unsigned long long index;
  int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward */

  index = forward ? idx : (-idx) - 1;
  if (index >= count_)
    return {};

  /* Seek in the other direction if that way is shorter. */
  int seek_forward = forward;
  unsigned long long seek_index = index;
  if (index > (count_ - 1) / 2) {
    seek_forward = !forward;
    seek_index = count_ - 1 - index;
  }

  n = seek_forward ? head_ : tail_;
  while (ABSL_PREDICT_TRUE(n)) {
    if ((accum + n->count) > seek_index) {
      break;
    } else {
      accum += n->count;
      n = seek_forward ? n->next : n->prev;
    }
  }

  if (!n)
    return {};

  /* Fix accum so it looks like we seeked in the other direction. */
  if (seek_forward != forward)
    accum = count_ - n->count - accum;

  Iterator iter;
  iter.owner_ = this;
  iter.direction_ = forward ? FWD : REV;
  iter.current_ = n;

  if (forward) {
    /* forward = normal head-to-tail offset. */
    iter.offset_ = index - accum;
  } else {
    /* reverse = need negative offset for tail-to-head, so undo
     * the result of the original index = (-idx) - 1 above. */
    iter.offset_ = (-index) - 1 + accum;
  }

  return iter;
}

bool QList::Iterator::Next() {
  if (!current_)
    return false;

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
    unsigned char* (*nextFn)(unsigned char*, unsigned char*) = NULL;
    int offset_update = 0;

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
  const_cast<QList*>(owner_)->Compress(current_);

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

  DCHECK(zi_);

  /* Populate value from existing listpack position */
  unsigned int sz = 0;
  long long val;
  uint8_t* ptr = lpGetValue(zi_, &sz, &val);

  return ptr ? Entry(reinterpret_cast<char*>(ptr), sz) : Entry(val);
}

}  // namespace dfly
