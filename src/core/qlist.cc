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
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"

using namespace std;

/* Maximum size in bytes of any multi-element listpack.
 * Larger values will live in their own isolated listpacks.
 * This is used only if we're limited by record count. when we're limited by
 * size, the maximum limit is bigger, but still safe.
 * 8k is a recommended / default size limit */
#define SIZE_SAFETY_LIMIT 8192

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
      this->Compress(_node);     \
  } while (0)

namespace dfly {

namespace {

static_assert(sizeof(QList) == 32);

enum IterDir : uint8_t { FWD = 1, REV = 0 };

/* This is for test suite development purposes only, 0 means disabled. */
size_t packed_threshold = 0;

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
  static uint8_t empty = 0;
  return sv.empty() ? &empty : reinterpret_cast<const uint8_t*>(sv.data());
}

bool IsLargeElement(size_t sz, int fill) {
  if (ABSL_PREDICT_FALSE(packed_threshold != 0))
    return sz >= packed_threshold;
  if (fill >= 0)
    return sz > SIZE_SAFETY_LIMIT;
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

  // Allow merge if new node will not exceed the limit.
  return !quicklistNodeExceedsLimit(fill, merge_sz, a->count + b->count);
}

// the owner over entry is passed to the node.
quicklistNode* CreateRAW(int container, uint8_t* entry, size_t sz) {
  quicklistNode* node = (quicklistNode*)zmalloc(sizeof(*node));
  node->entry = entry;
  node->count = 1;
  node->sz = sz;
  node->next = node->prev = NULL;
  node->encoding = QUICKLIST_NODE_ENCODING_RAW;
  node->container = container;
  node->recompress = 0;
  node->dont_compress = 0;
  return node;
}

uint8_t* LP_Insert(uint8_t* lp, string_view elem, uint8_t* pos, int lp_where) {
  DCHECK(pos);
  return lpInsertString(lp, uint_ptr(elem), elem.size(), pos, lp_where, NULL);
}

uint8_t* LP_Append(uint8_t* lp, string_view elem) {
  return lpAppend(lp, uint_ptr(elem), elem.size());
}

uint8_t* LP_Prepend(uint8_t* lp, string_view elem) {
  return lpPrepend(lp, uint_ptr(elem), elem.size());
}

quicklistNode* CreateFromSV(int container, string_view value) {
  uint8_t* entry = nullptr;
  size_t sz = 0;
  if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
    DCHECK(!value.empty());
    sz = value.size();
    entry = (uint8_t*)zmalloc(sz);
    memcpy(entry, value.data(), sz);
  } else {
    entry = LP_Append(lpNew(0), value);
    sz = lpBytes(entry);
  }

  return CreateRAW(container, entry, sz);
}

// Returns the relative increase in size.
inline ssize_t NodeSetEntry(quicklistNode* node, uint8_t* entry) {
  node->entry = entry;
  size_t new_sz = lpBytes(node->entry);
  ssize_t diff = new_sz - node->sz;
  node->sz = new_sz;
  return diff;
}

/* Compress the listpack in 'node' and update encoding details.
 * Returns true if listpack compressed successfully.
 * Returns false if compression failed or if listpack too small to compress. */
bool CompressNode(quicklistNode* node) {
  DCHECK(node->encoding == QUICKLIST_NODE_ENCODING_RAW);
  DCHECK(!node->dont_compress);

  /* validate that the node is neither
   * tail nor head (it has prev and next)*/
  DCHECK(node->prev && node->next);

  node->recompress = 0;
  /* Don't bother compressing small values */
  if (node->sz < MIN_COMPRESS_BYTES)
    return false;

  // We allocate LZF_STATE on heap, piggy-backing on the existing allocation.
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

ssize_t CompressNodeIfNeeded(quicklistNode* node) {
  DCHECK(node);
  if (node->encoding == QUICKLIST_NODE_ENCODING_RAW && !node->dont_compress) {
    if (CompressNode(node))
      return ((quicklistLZF*)node->entry)->sz - node->sz;
  }
  return 0;
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
   returns by how much the size of the node has increased.
*/
ssize_t DecompressNodeIfNeeded(bool recompress, quicklistNode* node) {
  if ((node) && (node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {
    size_t compressed_sz = ((quicklistLZF*)node->entry)->sz;
    if (DecompressNode(recompress, node)) {
      return node->sz - compressed_sz;
    }
  }
  return 0;
}

ssize_t RecompressOnly(quicklistNode* node) {
  if (node->recompress && !node->dont_compress) {
    if (CompressNode(node))
      return ((quicklistLZF*)node->entry)->sz - node->sz;
  }
  return 0;
}

quicklistNode* SplitNode(quicklistNode* node, int offset, bool after, ssize_t* diff) {
  DCHECK(node->container == QUICKLIST_NODE_CONTAINER_PACKED);
  size_t zl_sz = node->sz;
  uint8_t* entry = (uint8_t*)zmalloc(zl_sz);

  memcpy(entry, node->entry, zl_sz);

  /* Need positive offset for calculating extent below. */
  if (offset < 0)
    offset = node->count + offset;

  /* Ranges to be trimmed: -1 here means "continue deleting until the list ends" */
  int orig_start = after ? offset + 1 : 0;
  int orig_extent = after ? -1 : offset;
  int new_start = after ? 0 : offset;
  int new_extent = after ? offset + 1 : -1;

  ssize_t diff_existing = NodeSetEntry(node, lpDeleteRange(node->entry, orig_start, orig_extent));
  node->count = lpLength(node->entry);

  entry = lpDeleteRange(entry, new_start, new_extent);
  quicklistNode* new_node = CreateRAW(QUICKLIST_NODE_CONTAINER_PACKED, entry, lpBytes(entry));
  new_node->count = lpLength(new_node->entry);
  *diff = diff_existing;

  return new_node;
}

}  // namespace

void QList::SetPackedThreshold(unsigned threshold) {
  packed_threshold = threshold;
}

QList::QList() : fill_(-2), compress_(0), bookmark_count_(0) {
}

QList::QList(int fill, int compress) : fill_(fill), compress_(compress), bookmark_count_(0) {
}

QList::QList(QList&& other)
    : head_(other.head_),
      count_(other.count_),
      len_(other.len_),
      fill_(other.fill_),
      compress_(other.compress_),
      bookmark_count_(other.bookmark_count_) {
  other.head_ = nullptr;
  other.len_ = other.count_ = 0;
}

QList::~QList() {
  Clear();
}

QList& QList::operator=(QList&& other) {
  if (this != &other) {
    Clear();
    head_ = other.head_;
    len_ = other.len_;
    count_ = other.count_;
    fill_ = other.fill_;
    compress_ = other.compress_;
    bookmark_count_ = other.bookmark_count_;

    other.head_ = nullptr;
    other.len_ = other.count_ = 0;
  }
  return *this;
}

void QList::Clear() {
  quicklistNode* current = head_;

  while (len_) {
    quicklistNode* next = current->next;

    zfree(current->entry);
    zfree(current);

    len_--;
    current = next;
  }
  head_ = nullptr;
  count_ = 0;
  malloc_size_ = 0;
}

void QList::Push(string_view value, Where where) {
  DVLOG(2) << "Push " << absl::CHexEscape(value) << " " << (where == HEAD ? "HEAD" : "TAIL");

  /* The head and tail should never be compressed (we don't attempt to decompress them) */
  if (head_) {
    DCHECK(head_->encoding != QUICKLIST_NODE_ENCODING_LZF);
    DCHECK(head_->prev->encoding != QUICKLIST_NODE_ENCODING_LZF);
  }

  quicklistNode* orig = head_;
  if (where == TAIL && orig) {
    orig = orig->prev;
  }

  InsertOpt opt = where == HEAD ? BEFORE : AFTER;

  size_t sz = value.size();
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    InsertPlainNode(orig, value, opt);
    return;
  }

  count_++;

  if (ABSL_PREDICT_TRUE(NodeAllowInsert(orig, fill_, sz))) {
    auto func = (where == HEAD) ? LP_Prepend : LP_Append;
    malloc_size_ += NodeSetEntry(orig, func(orig->entry, value));
    orig->count++;
    if (len_ == 1) {  // sanity check
      DCHECK_EQ(malloc_size_, orig->sz);
    }
    DCHECK(head_->prev->next == nullptr);
    return;
  }

  quicklistNode* node = CreateFromSV(QUICKLIST_NODE_CONTAINER_PACKED, value);
  InsertNode(orig, node, opt);
  DCHECK(head_->prev->next == nullptr);
}

string QList::Pop(Where where) {
  DCHECK_GT(count_, 0u);
  quicklistNode* node = head_;
  if (where == TAIL) {
    node = head_->prev;
  }

  /* The head and tail should never be compressed */
  DCHECK(node->encoding != QUICKLIST_NODE_ENCODING_LZF);
  DCHECK(head_->prev->next == nullptr);

  string res;
  if (ABSL_PREDICT_FALSE(QL_NODE_IS_PLAIN(node))) {
    // TODO: We could avoid this copy by returning the pointer of the plain node.
    // But the higher level APIs should support this.
    res.assign(reinterpret_cast<char*>(node->entry), node->sz);
    DelNode(node);
  } else {
    uint8_t* pos = where == HEAD ? lpFirst(node->entry) : lpLast(node->entry);
    unsigned int vlen;
    long long vlong;
    uint8_t* vstr = lpGetValue(pos, &vlen, &vlong);
    if (vstr) {
      res.assign(reinterpret_cast<char*>(vstr), vlen);
    } else {
      res = absl::StrCat(vlong);
    }
    DelPackedIndex(node, pos);
  }
  DCHECK(head_ == nullptr || head_->prev->next == nullptr);
  return res;
}

void QList::AppendListpack(unsigned char* zl) {
  quicklistNode* node = CreateRAW(QUICKLIST_NODE_CONTAINER_PACKED, zl, lpBytes(zl));
  node->count = lpLength(node->entry);

  InsertNode(_Tail(), node, AFTER);
  count_ += node->count;
}

void QList::AppendPlain(unsigned char* data, size_t sz) {
  quicklistNode* node = CreateRAW(QUICKLIST_NODE_CONTAINER_PLAIN, data, sz);
  InsertNode(_Tail(), node, AFTER);
  ++count_;
}

bool QList::Insert(std::string_view pivot, std::string_view elem, InsertOpt opt) {
  Iterator it = GetIterator(HEAD);

  while (it.Next()) {
    if (it.Get() == pivot) {
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

size_t QList::MallocUsed(bool slow) const {
  size_t node_size = len_ * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  if (slow) {
    for (quicklistNode* node = head_; node; node = node->next) {
      node_size += zmalloc_usable_size(node->entry);
    }
    return node_size;
  }

  return node_size + malloc_size_;
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

quicklistNode* QList::InsertPlainNode(quicklistNode* old_node, string_view value,
                                      InsertOpt insert_opt) {
  quicklistNode* new_node = CreateFromSV(QUICKLIST_NODE_CONTAINER_PLAIN, value);
  InsertNode(old_node, new_node, insert_opt);
  count_++;
  return new_node;
}

void QList::InsertNode(quicklistNode* old_node, quicklistNode* new_node, InsertOpt insert_opt) {
  if (insert_opt == AFTER) {
    new_node->prev = old_node;
    if (old_node) {
      new_node->next = old_node->next;
      if (old_node->next)
        old_node->next->prev = new_node;
      old_node->next = new_node;
      if (head_->prev == old_node)  // if old_node is tail, update the tail to the new node.
        head_->prev = new_node;
    }
  } else {
    new_node->next = old_node;
    if (old_node) {
      new_node->prev = old_node->prev;
      // if old_node is not head, link its prev to the new node.
      // head->prev is tail, so we don't need to update it.
      if (old_node != head_)
        old_node->prev->next = new_node;
      old_node->prev = new_node;
    }
    if (head_ == old_node)
      head_ = new_node;
  }

  /* If this insert creates the only element so far, initialize head/tail. */
  if (len_ == 0) {
    head_ = new_node;
    head_->prev = new_node;
  }

  /* Update len first, so in Compress we know exactly len */
  len_++;
  malloc_size_ += new_node->sz;

  if (old_node)
    quicklistCompress(old_node);

  quicklistCompress(new_node);
}

void QList::Insert(Iterator it, std::string_view elem, InsertOpt insert_opt) {
  DCHECK(it.current_);
  DCHECK(it.zi_);

  int full = 0, at_tail = 0, at_head = 0, avail_next = 0, avail_prev = 0;
  quicklistNode* node = it.current_;
  size_t sz = elem.size();

  bool after = insert_opt == AFTER;

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
      malloc_size_ += DecompressNodeIfNeeded(true, node);
      ssize_t diff_existing = 0;
      quicklistNode* new_node = SplitNode(node, it.offset_, after, &diff_existing);
      quicklistNode* entry_node = InsertPlainNode(node, elem, insert_opt);
      InsertNode(entry_node, new_node, insert_opt);
      malloc_size_ += diff_existing;
    }
    return;
  }

  /* Now determine where and how to insert the new element */
  if (!full) {
    malloc_size_ += DecompressNodeIfNeeded(true, node);
    uint8_t* new_entry = LP_Insert(node->entry, elem, it.zi_, after ? LP_AFTER : LP_BEFORE);
    malloc_size_ += NodeSetEntry(node, new_entry);
    node->count++;
    malloc_size_ += RecompressOnly(node);
  } else {
    bool insert_tail = at_tail && after;
    bool insert_head = at_head && !after;
    if (insert_tail && avail_next) {
      /* If we are: at tail, next has free space, and inserting after:
       *   - insert entry at head of next node. */
      auto* new_node = node->next;
      malloc_size_ += DecompressNodeIfNeeded(true, new_node);
      malloc_size_ += NodeSetEntry(new_node, LP_Prepend(new_node->entry, elem));
      new_node->count++;
      malloc_size_ += RecompressOnly(new_node);
      malloc_size_ += RecompressOnly(node);
    } else if (insert_head && avail_prev) {
      /* If we are: at head, previous has free space, and inserting before:
       *   - insert entry at tail of previous node. */
      auto* new_node = node->prev;
      malloc_size_ += DecompressNodeIfNeeded(true, new_node);
      malloc_size_ += NodeSetEntry(new_node, LP_Append(new_node->entry, elem));
      new_node->count++;
      malloc_size_ += RecompressOnly(new_node);
      malloc_size_ += RecompressOnly(node);
    } else if (insert_tail || insert_head) {
      /* If we are: full, and our prev/next has no available space, then:
       *   - create new node and attach to qlist */
      auto* new_node = CreateFromSV(QUICKLIST_NODE_CONTAINER_PACKED, elem);
      InsertNode(node, new_node, insert_opt);
    } else {
      /* else, node is full we need to split it. */
      /* covers both after and !after cases */
      malloc_size_ += DecompressNodeIfNeeded(true, node);
      ssize_t diff_existing = 0;
      auto* new_node = SplitNode(node, it.offset_, after, &diff_existing);
      auto func = after ? LP_Prepend : LP_Append;
      malloc_size_ += NodeSetEntry(new_node, func(new_node->entry, elem));
      new_node->count++;
      InsertNode(node, new_node, insert_opt);
      MergeNodes(node);
      malloc_size_ += diff_existing;
    }
  }
  count_++;
}

void QList::Replace(Iterator it, std::string_view elem) {
  quicklistNode* node = it.current_;
  unsigned char* newentry;
  size_t sz = elem.size();

  if (ABSL_PREDICT_TRUE(!QL_NODE_IS_PLAIN(node) && !IsLargeElement(sz, fill_) &&
                        (newentry = lpReplace(node->entry, &it.zi_, uint_ptr(elem), sz)) != NULL)) {
    malloc_size_ += NodeSetEntry(node, newentry);
    /* quicklistNext() and quicklistGetIteratorEntryAtIdx() provide an uncompressed node */
    quicklistCompress(node);
  } else if (QL_NODE_IS_PLAIN(node)) {
    if (IsLargeElement(sz, fill_)) {
      zfree(node->entry);
      uint8_t* new_entry = (uint8_t*)zmalloc(sz);
      memcpy(new_entry, elem.data(), sz);
      malloc_size_ += NodeSetEntry(node, new_entry);
      quicklistCompress(node);
    } else {
      Insert(it, elem, AFTER);
      DelNode(node);
    }
  } else { /* The node is full or data is a large element */
    quicklistNode *split_node = NULL, *new_node;
    node->dont_compress = 1; /* Prevent compression in InsertNode() */

    /* If the entry is not at the tail, split the node at the entry's offset. */
    if (it.offset_ != node->count - 1 && it.offset_ != -1) {
      ssize_t diff_existing = 0;
      split_node = SplitNode(node, it.offset_, 1, &diff_existing);
      malloc_size_ += diff_existing;
    }

    /* Create a new node and insert it after the original node.
     * If the original node was split, insert the split node after the new node. */
    new_node = CreateFromSV(IsLargeElement(sz, fill_) ? QUICKLIST_NODE_CONTAINER_PLAIN
                                                      : QUICKLIST_NODE_CONTAINER_PACKED,
                            elem);
    InsertNode(node, new_node, AFTER);
    if (split_node)
      InsertNode(new_node, split_node, AFTER);
    count_++;

    /* Delete the replaced element. */
    if (node->count == 1) {
      DelNode(node);
    } else {
      unsigned char* p = lpSeek(node->entry, -1);
      DelPackedIndex(node, p);
      node->dont_compress = 0; /* Re-enable compression */
      new_node = MergeNodes(new_node);
      /* We can't know if the current node and its sibling nodes are correctly compressed,
       * and we don't know if they are within the range of compress depth, so we need to
       * use quicklistCompress() for compression, which checks if node is within compress
       * depth before compressing. */
      quicklistCompress(new_node);
      quicklistCompress(new_node->prev);
      if (new_node->next)
        quicklistCompress(new_node->next);
    }
  }
}

/* Force 'quicklist' to meet compression guidelines set by compress depth.
 * The only way to guarantee interior nodes get compressed is to iterate
 * to our "interior" compress depth then compress the next node we find.
 * If compress depth is larger than the entire list, we return immediately. */
void QList::Compress(quicklistNode* node) {
  if (len_ == 0)
    return;

  /* The head and tail should never be compressed (we should not attempt to recompress them) */
  DCHECK(head_->recompress == 0 && head_->prev->recompress == 0);

  /* If length is less than our compress depth (from both sides),
   * we can't compress anything. */
  if (!AllowCompression() || len_ < (unsigned int)(compress_ * 2))
    return;

  /* Iterate until we reach compress depth for both sides of the list.a
   * Note: because we do length checks at the *top* of this function,
   *       we can skip explicit null checks below. Everything exists. */
  quicklistNode* forward = head_;
  quicklistNode* reverse = head_->prev;
  int depth = 0;
  int in_depth = 0;
  while (depth++ < compress_) {
    malloc_size_ += DecompressNodeIfNeeded(false, forward);
    malloc_size_ += DecompressNodeIfNeeded(false, reverse);

    if (forward == node || reverse == node)
      in_depth = 1;

    /* We passed into compress depth of opposite side of the quicklist
     * so there's no need to compress anything and we can exit. */
    if (forward == reverse || forward->next == reverse)
      return;

    forward = forward->next;
    reverse = reverse->prev;
  }

  if (!in_depth && node)
    malloc_size_ += CompressNodeIfNeeded(node);

  /* At this point, forward and reverse are one node beyond depth */
  malloc_size_ += CompressNodeIfNeeded(forward);
  malloc_size_ += CompressNodeIfNeeded(reverse);
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
  malloc_size_ += DecompressNodeIfNeeded(false, a);
  malloc_size_ += DecompressNodeIfNeeded(false, b);
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
    malloc_size_ += NodeSetEntry(keep, keep->entry);

    keep->recompress = 0; /* Prevent 'keep' from being recompressed if
                           * it becomes head or tail after merging. */

    nokeep->count = 0;
    DelNode(nokeep);
    quicklistCompress(keep);
    return keep;
  }

  /* else, the merge returned NULL and nothing changed. */
  return NULL;
}

void QList::DelNode(quicklistNode* node) {
  if (node->next)
    node->next->prev = node->prev;

  if (node == head_) {
    head_ = node->next;
  } else {
    // for non-head nodes, update prev->next to point to node->next
    // (If node==head, prev is tail and should always point to NULL).
    node->prev->next = node->next;
    if (node == head_->prev)  // tail
      head_->prev = node->prev;
  }

  /* Update len first, so in Compress we know exactly len */
  len_--;
  count_ -= node->count;
  malloc_size_ -= node->sz;

  /* If we deleted a node within our compress depth, we
   * now have compressed nodes needing to be decompressed. */
  Compress(NULL);

  zfree(node->entry);
  zfree(node);
}

/* Delete one entry from list given the node for the entry and a pointer
 * to the entry in the node.
 *
 * Note: DelPackedIndex() *requires* uncompressed nodes because you
 *       already had to get *p from an uncompressed node somewhere.
 *
 * Returns true if the entire node was deleted, false if node still exists.
 * Also updates in/out param 'p' with the next offset in the listpack. */
bool QList::DelPackedIndex(quicklistNode* node, uint8_t* p) {
  DCHECK(!QL_NODE_IS_PLAIN(node));

  if (node->count == 1) {
    DelNode(node);
    return true;
  }

  malloc_size_ += NodeSetEntry(node, lpDelete(node->entry, p, NULL));
  node->count--;
  count_--;

  return false;
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
    it.current_ = _Tail();
    it.offset_ = -1;
    it.direction_ = REV;
  }

  return it;
}

auto QList::GetIterator(long idx) const -> Iterator {
  quicklistNode* n;
  unsigned long long accum = 0;
  int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward */
  uint64_t index = forward ? idx : (-idx) - 1;
  if (index >= count_)
    return {};

  DCHECK(head_);

  /* Seek in the other direction if that way is shorter. */
  int seek_forward = forward;
  unsigned long long seek_index = index;
  if (index > (count_ - 1) / 2) {
    seek_forward = !forward;
    seek_index = count_ - 1 - index;
  }

  n = seek_forward ? head_ : head_->prev;
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

auto QList::Erase(Iterator it) -> Iterator {
  DCHECK(it.current_);

  quicklistNode* node = it.current_;
  quicklistNode* prev = node->prev;
  quicklistNode* next = node->next;

  bool deleted_node = false;
  if (QL_NODE_IS_PLAIN(node)) {
    DelNode(node);
    deleted_node = true;
  } else {
    deleted_node = DelPackedIndex(node, it.zi_);
  }

  /* after delete, the zi is now invalid for any future usage. */
  it.zi_ = NULL;

  /* If current node is deleted, we must update iterator node and offset. */
  if (deleted_node) {
    if (it.direction_ == FWD) {
      it.current_ = next;
      it.offset_ = 0;
    } else if (it.direction_ == REV) {
      it.current_ = len_ ? prev : nullptr;
      it.offset_ = -1;
    }
  }

  // Sanity, should be noop in release mode.
  if (len_ == 1) {
    DCHECK_EQ(count_, head_->count);
    DCHECK_EQ(malloc_size_, head_->sz);
  }

  /* else if (!deleted_node), no changes needed.
   * we already reset iter->zi above, and the existing iter->offset
   * doesn't move again because:
   *   - [1, 2, 3] => delete offset 1 => [1, 3]: next element still offset 1
   *   - [1, 2, 3] => delete offset 0 => [2, 3]: next element still offset 0
   *  if we deleted the last element at offset N and now
   *  length of this listpack is N-1, the next call into
   *  quicklistNext() will jump to the next node. */
  return it;
}

bool QList::Erase(const long start, unsigned count) {
  if (count == 0)
    return false;

  unsigned extent = count; /* range is inclusive of start position */

  if (start >= 0 && extent > (count_ - start)) {
    /* if requesting delete more elements than exist, limit to list size. */
    extent = count_ - start;
  } else if (start < 0 && extent > (unsigned long)(-start)) {
    /* else, if at negative offset, limit max size to rest of list. */
    extent = -start; /* c.f. LREM -29 29; just delete until end. */
  }

  Iterator it = GetIterator(start);
  quicklistNode* node = it.current_;
  long offset = it.offset_;

  /* iterate over next nodes until everything is deleted. */
  while (extent) {
    quicklistNode* next = node->next;

    unsigned long del;
    int delete_entire_node = 0;
    if (offset == 0 && extent >= node->count) {
      /* If we are deleting more than the count of this node, we
       * can just delete the entire node without listpack math. */
      delete_entire_node = 1;
      del = node->count;
    } else if (offset >= 0 && extent + offset >= node->count) {
      /* If deleting more nodes after this one, calculate delete based
       * on size of current node. */
      del = node->count - offset;
    } else if (offset < 0) {
      /* If offset is negative, we are in the first run of this loop
       * and we are deleting the entire range
       * from this start offset to end of list.  Since the Negative
       * offset is the number of elements until the tail of the list,
       * just use it directly as the deletion count. */
      del = -offset;

      /* If the positive offset is greater than the remaining extent,
       * we only delete the remaining extent, not the entire offset.
       */
      if (del > extent)
        del = extent;
    } else {
      /* else, we are deleting less than the extent of this node, so
       * use extent directly. */
      del = extent;
    }

    if (delete_entire_node || QL_NODE_IS_PLAIN(node)) {
      DelNode(node);
    } else {
      malloc_size_ += DecompressNodeIfNeeded(true, node);
      malloc_size_ += NodeSetEntry(node, lpDeleteRange(node->entry, offset, del));
      node->count -= del;
      count_ -= del;
      if (node->count == 0) {
        DelNode(node);
      } else {
        malloc_size_ += RecompressOnly(node);
      }
    }

    extent -= del;
    node = next;
    offset = 0;
  }
  return true;
}

bool QList::Entry::operator==(std::string_view sv) const {
  if (std::holds_alternative<int64_t>(value_)) {
    char buf[absl::numbers_internal::kFastToBufferSize];
    char* end = absl::numbers_internal::FastIntToBuffer(std::get<int64_t>(value_), buf);
    return sv == std::string_view(buf, end - buf);
  }
  return view() == sv;
}

bool QList::Iterator::Next() {
  if (!current_)
    return false;

  int plain = QL_NODE_IS_PLAIN(current_);
  if (!zi_) {
    /* If !zi, use current index. */
    const_cast<QList*>(owner_)->malloc_size_ += DecompressNodeIfNeeded(true, current_);
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
    offset_ = -1;
    current_ = (current_ == owner_->head_) ? nullptr : current_->prev;
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
