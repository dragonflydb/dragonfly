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
#include <zstd.h>

#include "base/logging.h"
#include "core/dict_builder.h"
#include "core/page_usage/page_usage_stats.h"

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
#define MIN_COMPRESS_BYTES 256

/* Minimum size reduction in bytes to store compressed quicklistNode data.
 * This also prevents us from storing compression if the compression
 * resulted in a larger size than the original data. */
#define MIN_COMPRESS_IMPROVE 32

#define QL_NODE_IS_PLAIN(node) ((node)->container == QUICKLIST_NODE_CONTAINER_PLAIN)

namespace dfly {

namespace {

struct ZstdDictState {
  ZSTD_CDict* cdict = nullptr;
  ZSTD_DDict* ddict = nullptr;
  ZSTD_CCtx* cctx = nullptr;  // Reused across compressions to avoid per-call alloc/free.
  ZSTD_DCtx* dctx = nullptr;  // Reused across decompressions to avoid per-call alloc/free.

  ~ZstdDictState() {
    if (cdict)
      ZSTD_freeCDict(cdict);
    if (ddict)
      ZSTD_freeDDict(ddict);
    if (cctx)
      ZSTD_freeCCtx(cctx);
    if (dctx)
      ZSTD_freeDCtx(dctx);
  }
};

thread_local ZstdDictState* tl_zstd_dict = nullptr;

static_assert(sizeof(QList) == 48);
static_assert(sizeof(QList::Node) == 40);

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

/* Calculate the size limit or length limit of the quicklist node
 * based on 'fill', and is also used to limit list listpack. */
void quicklistNodeLimit(int fill, size_t* size, unsigned int* count) {
  *size = SIZE_MAX;
  *count = UINT_MAX;

  if (fill >= 0) {
    /* Ensure that one node have at least one entry */
    *count = (fill == 0) ? 1 : fill;
  } else {
    *size = NodeNegFillLimit(fill);
  }
}

#define sizeMeetsSafetyLimit(sz) ((sz) <= SIZE_SAFETY_LIMIT)

/* Check if the limit of the quicklist node has been reached to determine if
 * insertions, merges or other operations that would increase the size of
 * the node can be performed.
 * Return 1 if exceeds the limit, otherwise 0. */
int quicklistNodeExceedsLimit(int fill, size_t new_sz, unsigned int new_count) {
  size_t sz_limit;
  unsigned int count_limit;
  quicklistNodeLimit(fill, &sz_limit, &count_limit);

  if (ABSL_PREDICT_TRUE(sz_limit != SIZE_MAX)) {
    return new_sz > sz_limit;
  } else if (count_limit != UINT_MAX) {
    /* when we reach here we know that the limit is a size limit (which is
     * safe, see comments next to optimization_level and SIZE_SAFETY_LIMIT) */
    if (!sizeMeetsSafetyLimit(new_sz))
      return 1;
    return new_count > count_limit;
  }

  ABSL_UNREACHABLE();
}

bool NodeAllowInsert(const QList::Node* node, const int fill, const size_t sz) {
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

bool NodeAllowMerge(const QList::Node* a, const QList::Node* b, const int fill) {
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
QList::Node* CreateRAW(int container, uint8_t* entry, size_t sz) {
  QList::Node* node = (QList::Node*)zmalloc(sizeof(*node));
  node->u_.entry = entry;
  node->count = 1;
  node->sz = sz;
  node->next = node->prev = NULL;
  node->encoding = QUICKLIST_NODE_ENCODING_RAW;
  node->container = container;
  node->recompress = 0;
  node->dont_compress = 0;
  node->offloaded = 0;
  node->io_pending = 0;

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

QList::Node* CreateFromSV(int container, string_view value) {
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
inline ssize_t NodeSetEntry(QList::Node* node, uint8_t* entry) {
  node->u_.entry = entry;
  size_t new_sz = lpBytes(node->u_.entry);
  ssize_t diff = new_sz - node->sz;
  node->sz = new_sz;
  return diff;
}

/* quicklistLZF is a 8+N byte struct holding 'sz' followed by 'compressed'.
 * 'sz' is byte length of 'compressed' field.
 * 'compressed' is LZF data with total (compressed) length 'sz'
 * NOTE: uncompressed length is stored in quicklistNode->sz.
 * When quicklistNode->u_.entry is compressed, node->u_.entry points to a quicklistLZF */
using quicklistLZF = struct quicklistLZF {
  size_t sz; /* LZF size in bytes*/
  char compressed[];
};

inline quicklistLZF* GetLzf(QList::Node* node) {
  DCHECK(node->encoding == QUICKLIST_NODE_ENCODING_LZF ||
         node->encoding == QLIST_NODE_ENCODING_ZSTD);
  return (quicklistLZF*)node->u_.entry;
}

bool CompressLZF(QList::Node* node) {
  // We allocate LZF_STATE on heap, piggy-backing on the existing allocation.
  char* uptr = (char*)zmalloc(sizeof(quicklistLZF) + node->sz + sizeof(LZF_STATE));
  quicklistLZF* lzf = (quicklistLZF*)uptr;
  LZF_HSLOT* sdata = (LZF_HSLOT*)(uptr + sizeof(quicklistLZF) + node->sz);

  /* Cancel if compression fails or doesn't compress small enough */
  if (((lzf->sz = lzf_compress(node->u_.entry, node->sz, lzf->compressed, node->sz, sdata)) == 0) ||
      lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz) {
    /* lzf_compress aborts/rejects compression if value not compressible. */
    DVLOG(2) << "Uncompressable " << node->sz << " vs " << lzf->sz;
    zfree(lzf);
    QList::stats.bad_compression_attempts++;
    return false;
  }
  DVLOG(2) << "Compressed " << node->sz << " to " << lzf->sz;
  QList::stats.compressed_bytes += lzf->sz;
  QList::stats.raw_compressed_bytes += node->sz;

  lzf = (quicklistLZF*)zrealloc(lzf, sizeof(*lzf) + lzf->sz);
  zfree(node->u_.entry);
  node->u_.entry = (unsigned char*)lzf;
  node->encoding = QUICKLIST_NODE_ENCODING_LZF;
  return true;
}

/* Compress the listpack in 'node' and update encoding details.
 * Returns true if listpack compressed successfully.
 * Returns false if compression failed or if listpack too small to compress. */
bool CompressRaw(QList::Node* node) {
  DCHECK(node->encoding == QUICKLIST_NODE_ENCODING_RAW);
  DCHECK(!node->dont_compress);

  /* validate that the node is neither
   * tail nor head (it has prev and next)*/
  DCHECK(node->prev && node->next);

  node->recompress = 0;
  /* Don't bother compressing small values */
  if (node->sz < MIN_COMPRESS_BYTES)
    return false;

  QList::stats.compression_attempts++;
  return CompressLZF(node);
}

ssize_t TryCompress(QList::Node* node) {
  DCHECK(node);
  if (node->encoding == QUICKLIST_NODE_ENCODING_RAW) {
    node->attempted_compress = 1;
    if (!node->dont_compress) {
      if (CompressRaw(node))
        return ssize_t(GetLzf(node)->sz) - node->sz;
    }
  }
  return 0;
}

/* Uncompress the listpack in 'node' and update encoding details.
 * Returns 1 on successful decode, 0 on failure to decode.
 * ddict is required for ZSTD-compressed nodes (encoding == QLIST_NODE_ENCODING_ZSTD). */
bool DecompressRaw(bool recompress, QList::Node* node) {
  DCHECK(node->encoding == QUICKLIST_NODE_ENCODING_LZF ||
         node->encoding == QLIST_NODE_ENCODING_ZSTD);

  node->recompress = int(recompress);

  void* decompressed = zmalloc(node->sz);
  quicklistLZF* lzf = GetLzf(node);
  QList::stats.decompression_calls++;
  QList::stats.compressed_bytes -= lzf->sz;
  QList::stats.raw_compressed_bytes -= node->sz;

  if (node->encoding == QLIST_NODE_ENCODING_ZSTD) {
    DCHECK(tl_zstd_dict && tl_zstd_dict->dctx);
    ZSTD_DCtx_reset(tl_zstd_dict->dctx, ZSTD_reset_session_only);
    size_t dsz = ZSTD_decompress_usingDDict(tl_zstd_dict->dctx, decompressed, node->sz,
                                            lzf->compressed, lzf->sz, tl_zstd_dict->ddict);
    if (ZSTD_isError(dsz) || dsz != node->sz) {
      LOG(DFATAL) << "ZSTD decompression error: " << ZSTD_getErrorName(dsz);
      zfree(decompressed);
      return false;
    }
  } else {
    if (lzf_decompress(lzf->compressed, lzf->sz, decompressed, node->sz) == 0) {
      LOG(DFATAL) << "Invalid LZF compressed data";
      /* Someone requested decompress, but we can't decompress.  Not good. */
      zfree(decompressed);
      return false;
    }
  }
  zfree(lzf);
  node->u_.entry = (uint8_t*)decompressed;
  node->encoding = QUICKLIST_NODE_ENCODING_RAW;
  return true;
}

/* Decompress only compressed nodes.
   recompress: if true, the node will be marked for recompression after decompression.
   returns by how much the size of the node has increased.
*/
ssize_t TryDecompressInternal(bool recompress, QList::Node* node) {
  if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
    size_t compressed_sz = GetLzf(node)->sz;
    if (DecompressRaw(recompress, node)) {
      return node->sz - compressed_sz;
    }
  }
  return 0;
}

// If after is true, returns a new node with elements in [offset, inf), otherwise
// returns [0, offset-1].
QList::Node* SplitNode(QList::Node* node, int offset, bool after, ssize_t* diff) {
  DCHECK(node->container == QUICKLIST_NODE_CONTAINER_PACKED);
  size_t zl_sz = node->sz;
  uint8_t* entry = (uint8_t*)zmalloc(zl_sz);

  memcpy(entry, node->u_.entry, zl_sz);

  /* Need positive offset for calculating extent below. */
  if (offset < 0)
    offset = node->count + offset;

  /* Ranges to be trimmed: -1 here means "continue deleting until the list ends" */
  int orig_start = after ? offset + 1 : 0;
  int orig_extent = after ? -1 : offset;
  int new_start = after ? 0 : offset;
  int new_extent = after ? offset + 1 : -1;

  ssize_t diff_existing =
      NodeSetEntry(node, lpDeleteRange(node->u_.entry, orig_start, orig_extent));
  node->count = lpLength(node->u_.entry);

  entry = lpDeleteRange(entry, new_start, new_extent);
  QList::Node* new_node = CreateRAW(QUICKLIST_NODE_CONTAINER_PACKED, entry, lpBytes(entry));
  new_node->count = lpLength(new_node->u_.entry);
  *diff = diff_existing;

  return new_node;
}

}  // namespace

__thread QList::Stats QList::stats;

QList::Stats& QList::Stats::operator+=(const Stats& other) {
#define ADD_FIELD(field) this->field += other.field;

  ADD_FIELD(compression_attempts);
  ADD_FIELD(bad_compression_attempts);
  ADD_FIELD(decompression_calls);
  ADD_FIELD(compressed_bytes);
  ADD_FIELD(raw_compressed_bytes);
  ADD_FIELD(interior_node_reads);
  ADD_FIELD(total_node_reads);
  ADD_FIELD(offload_requests);
  ADD_FIELD(onload_requests);
  ADD_FIELD(zstd_dict_compressions);

#undef ADD_FIELD

  return *this;
}

size_t QList::Node::GetEntrySize() const {
  switch (encoding) {
    case QUICKLIST_NODE_ENCODING_RAW:
      return sz;
    case QUICKLIST_NODE_ENCODING_LZF:
    case QLIST_NODE_ENCODING_ZSTD:
      return GetLzf(const_cast<QList::Node*>(this))->sz + sizeof(quicklistLZF);
    default: {
      LOG(DFATAL) << "Unknown encoding " << encoding;
      return 0;
    }
  }
}

size_t QList::Node::GetLZF(void** data) const {
  DCHECK(encoding == QUICKLIST_NODE_ENCODING_LZF || encoding == QLIST_NODE_ENCODING_ZSTD);
  quicklistLZF* lzf = (quicklistLZF*)u_.entry;
  *data = lzf->compressed;
  return lzf->sz;
}

void QList::Node::SetExternal(size_t offset, uint32_t size) {
  DCHECK(u_.entry);
  zfree(u_.entry);
  offloaded = 1;
  u_.ext_offset = offset;
  ext_size = size;
}

void QList::SetPackedThreshold(unsigned threshold) {
  packed_threshold = threshold;
}

size_t QList::DefragIfNeeded(PageUsage* page_usage) {
  size_t reallocated = 0;

  for (Node* curr = head_; curr; curr = curr->next) {
    // Skip offloaded or pending nodes
    if (curr->offloaded || curr->io_pending) {
      continue;
    }

    if (!page_usage->IsPageForObjectUnderUtilized(curr->u_.entry)) {
      continue;
    }

    // Data pointed to by the nodes is reallocated. The nodes themselves are not reallocated because
    // of their constant (and relatively small, ~40 bytes per object) size. Defragmentation fixes
    // fragmented memory allocation, which usually happens when variable-sized blocks of data are
    // allocated and deallocated, which is not expected with nodes.
    uint8_t* new_entry = static_cast<uint8_t*>(zmalloc(curr->sz));
    memcpy(new_entry, curr->u_.entry, curr->sz);

    uint8_t* old_entry = curr->u_.entry;
    curr->u_.entry = new_entry;

    zfree(old_entry);
    ++reallocated;
  }
  return reallocated;
}

void QList::SetTieringParams(const TieringParams& params) {
  tiering_params_ = make_unique<TieringParams>(params);
}

QList::QList(int fill, int compress)
    : fill_(fill),
      dict_learning_failed_(0),
      dict_compress_failed_(0),
      dict_bulk_finished_(0),
      compress_(compress),
      bookmark_count_(0) {
}

QList::QList(QList&& other) noexcept
    : head_(other.head_),
      count_(other.count_),
      len_(other.len_),
      fill_(other.fill_),
      dict_learning_failed_(other.dict_learning_failed_),
      dict_compress_failed_(other.dict_compress_failed_),
      dict_bulk_finished_(other.dict_bulk_finished_),
      compress_(other.compress_),
      bookmark_count_(other.bookmark_count_),
      num_offloaded_nodes_(other.num_offloaded_nodes_),
      tiering_params_(std::move(other.tiering_params_)) {
  other.head_ = nullptr;
  other.len_ = other.count_ = 0;
  other.num_offloaded_nodes_ = 0;
}

QList::~QList() {
  Clear();
}

QList& QList::operator=(QList&& other) noexcept {
  if (this != &other) {
    Clear();
    head_ = other.head_;
    len_ = other.len_;
    count_ = other.count_;
    fill_ = other.fill_;
    dict_learning_failed_ = other.dict_learning_failed_;
    dict_compress_failed_ = other.dict_compress_failed_;
    dict_bulk_finished_ = other.dict_bulk_finished_;
    compress_ = other.compress_;
    bookmark_count_ = other.bookmark_count_;
    tiering_params_ = std::move(other.tiering_params_);
    num_offloaded_nodes_ = other.num_offloaded_nodes_;
    other.head_ = nullptr;
    other.len_ = other.count_ = other.num_offloaded_nodes_ = 0;
  }
  return *this;
}

void QList::Clear() noexcept {
  Node* current = head_;

  while (len_) {
    Node* next = current->next;

    // If entry is offloaded we should skip freeing its memory.
    bool free_entry = current->offloaded == 0;
    if (current->offloaded || current->io_pending) {
      if (tiering_params_ && tiering_params_->delete_cb) {
        tiering_params_->delete_cb(current);
      }
    } else {
      if (current->encoding != QUICKLIST_NODE_ENCODING_RAW) {
        quicklistLZF* lzf = (quicklistLZF*)current->u_.entry;
        stats.compressed_bytes -= lzf->sz;
        stats.raw_compressed_bytes -= current->sz;
      }
    }

    if (free_entry) {
      zfree(current->u_.entry);
    }

    zfree(current);

    len_--;
    current = next;
  }
  head_ = nullptr;
  count_ = 0;
  malloc_size_ = 0;
  num_offloaded_nodes_ = 0;
}

void QList::Push(string_view value, Where where) {
  DVLOG(3) << "Push " << absl::CHexEscape(value) << " " << (where == HEAD ? "HEAD" : "TAIL");

  /* The head and tail should never be compressed (we don't attempt to decompress them) */
  if (head_) {
    DCHECK_EQ(head_->encoding, QUICKLIST_NODE_ENCODING_RAW);
    DCHECK_EQ(head_->prev->encoding, QUICKLIST_NODE_ENCODING_RAW);
  }

  Node* orig = head_;
  uint32_t orig_id = 0;
  if (where == TAIL && orig) {
    orig = orig->prev;
    orig_id = len_ - 1;
  }

  InsertOpt opt = where == HEAD ? BEFORE : AFTER;

  size_t sz = value.size();
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    InsertPlainNode(orig, value, orig_id, opt);
    return;
  }

  count_++;

  if (ABSL_PREDICT_TRUE(NodeAllowInsert(orig, fill_, sz))) {
    auto func = (where == HEAD) ? LP_Prepend : LP_Append;
    malloc_size_ += NodeSetEntry(orig, func(orig->u_.entry, value));
    orig->count++;
    if (len_ == 1) {  // sanity check
      DCHECK_EQ(malloc_size_, orig->sz);
    }
    DCHECK(head_->prev->next == nullptr);
    return;
  }

  Node* node = CreateFromSV(QUICKLIST_NODE_CONTAINER_PACKED, value);
  InsertNode(orig, node, orig_id, opt);
  DCHECK(head_->prev->next == nullptr);
}

string QList::Pop(Where where) {
  DCHECK_GT(count_, 0u);
  Node* node = head_;
  if (where == TAIL) {
    node = head_->prev;
  }

  /* The head and tail should never be compressed */
  DCHECK_EQ(node->encoding, QUICKLIST_NODE_ENCODING_RAW);
  DCHECK(head_->prev->next == nullptr);

  // Try onloading entry if it's offloaded.
  if (tiering_params_) {
    AccessForReads(false, node);
  }

  string res;
  if (ABSL_PREDICT_FALSE(QL_NODE_IS_PLAIN(node))) {
    // TODO: We could avoid this copy by returning the pointer of the plain node.
    // But the higher level APIs should support this.
    res.assign(reinterpret_cast<char*>(node->u_.entry), node->sz);
    DelNode(node);
  } else {
    uint8_t* pos = where == HEAD ? lpFirst(node->u_.entry) : lpLast(node->u_.entry);
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
  Node* node = CreateRAW(QUICKLIST_NODE_CONTAINER_PACKED, zl, lpBytes(zl));
  node->count = lpLength(node->u_.entry);

  InsertNode(_Tail(), node, len_ ? len_ - 1 : 0, AFTER);
  count_ += node->count;
}

void QList::AppendPlain(unsigned char* data, size_t sz) {
  Node* node = CreateRAW(QUICKLIST_NODE_CONTAINER_PLAIN, data, sz);
  InsertNode(_Tail(), node, len_ ? len_ - 1 : 0, AFTER);
  ++count_;
}

bool QList::Insert(std::string_view pivot, std::string_view elem, InsertOpt opt) {
  Iterator it = GetIterator(HEAD);

  if (it.Valid()) {
    do {
      if (it.Get() == pivot) {
        Insert(it, elem, opt);
        return true;
      }
    } while (it.Next());
  }

  return false;
}

bool QList::Replace(long index, std::string_view elem) {
  Iterator it = GetIterator(index);
  if (it.Valid()) {
    Replace(it, elem);
    return true;
  }
  return false;
}

size_t QList::MallocUsed(bool slow) const {
  size_t node_size = len_ * sizeof(Node) + znallocx(sizeof(QList));
  if (slow) {
    for (Node* node = head_; node; node = node->next) {
      // Skip offloaded or pending nodes from malloc size calculation
      if (node->offloaded || node->io_pending) {
        continue;
      }
      node_size += zmalloc_usable_size(node->u_.entry);
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
  if (it.Valid()) {
    do {
      if (start > end || !cb(it.Get()))
        break;
      start++;
    } while (it.Next());
  }
}

auto QList::InsertPlainNode(Node* old_node, string_view value, uint32_t old_node_id,
                            InsertOpt insert_opt) -> Node* {
  Node* new_node = CreateFromSV(QUICKLIST_NODE_CONTAINER_PLAIN, value);
  InsertNode(old_node, new_node, old_node_id, insert_opt);
  count_++;
  return new_node;
}

void QList::InsertNode(Node* old_node, Node* new_node, uint32_t old_node_id, InsertOpt insert_opt) {
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
  } else {  // BEFORE
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

  // Calculate final positions AFTER all linkage and len_ updates are complete.
  uint32_t new_node_id;
  if (insert_opt == AFTER && old_node) {
    new_node_id = old_node_id + 1;  // new_node inserted after, old_node position unchanged
  } else {
    new_node_id = old_node_id;  // new_node takes old_node's position
    old_node_id++;              // old_node shifts one position forward
  }

  if (old_node)
    CoolOff(old_node, old_node_id);

  CoolOff(new_node, new_node_id);
}

void QList::Insert(Iterator it, std::string_view elem, InsertOpt insert_opt) {
  DCHECK(it.current_);
  DCHECK(it.zi_);

  int full = 0, at_tail = 0, at_head = 0, avail_next = 0, avail_prev = 0;
  Node* node = it.current_;
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
  uint32_t node_id = it.node_id_;
  if (ABSL_PREDICT_FALSE(IsLargeElement(sz, fill_))) {
    if (QL_NODE_IS_PLAIN(node) || (at_tail && after) || (at_head && !after)) {
      InsertPlainNode(node, elem, node_id, insert_opt);
    } else {
      AccessForReads(true, node);
      ssize_t diff_existing = 0;
      // if after == true, the order will be node, entry_node, new_node
      // otherwise: new_node, entry_node, node.
      Node* new_node = SplitNode(node, it.offset_, after, &diff_existing);
      Node* entry_node = InsertPlainNode(node, elem, node_id, insert_opt);
      uint32_t entry_node_id = after ? node_id + 1 : node_id;
      InsertNode(entry_node, new_node, entry_node_id, insert_opt);
      malloc_size_ += diff_existing;
    }
    return;
  }

  /* Now determine where and how to insert the new element */
  if (!full) {
    AccessForReads(true, node);
    uint8_t* new_entry = LP_Insert(node->u_.entry, elem, it.zi_, after ? LP_AFTER : LP_BEFORE);
    malloc_size_ += NodeSetEntry(node, new_entry);
    node->count++;
    malloc_size_ += RecompressNode(node);
  } else {
    bool insert_tail = at_tail && after;
    bool insert_head = at_head && !after;
    if (insert_tail && avail_next) {
      /* If we are: at tail, next has free space, and inserting after:
       *   - insert entry at head of next node. */
      auto* new_node = node->next;
      AccessForReads(true, new_node);
      malloc_size_ += NodeSetEntry(new_node, LP_Prepend(new_node->u_.entry, elem));
      new_node->count++;
      malloc_size_ += RecompressNode(new_node);
      malloc_size_ += RecompressNode(node);
    } else if (insert_head && avail_prev) {
      /* If we are: at head, previous has free space, and inserting before:
       *   - insert entry at tail of previous node. */
      auto* new_node = node->prev;
      AccessForReads(true, new_node);
      malloc_size_ += NodeSetEntry(new_node, LP_Append(new_node->u_.entry, elem));
      new_node->count++;
      malloc_size_ += RecompressNode(new_node);
      malloc_size_ += RecompressNode(node);
    } else if (insert_tail || insert_head) {
      /* If we are: full, and our prev/next has no available space, then:
       *   - create new node and attach to qlist */
      auto* new_node = CreateFromSV(QUICKLIST_NODE_CONTAINER_PACKED, elem);
      InsertNode(node, new_node, node_id, insert_opt);
    } else {
      /* else, node is full we need to split it. */
      /* covers both after and !after cases */
      AccessForReads(true, node);
      ssize_t diff_existing = 0;
      auto* new_node = SplitNode(node, it.offset_, after, &diff_existing);
      auto func = after ? LP_Prepend : LP_Append;
      new_node->u_.entry = func(new_node->u_.entry, elem);
      new_node->sz = lpBytes(new_node->u_.entry);
      new_node->count++;
      InsertNode(node, new_node, node_id, insert_opt);
      MergeNodes(node);
      malloc_size_ += diff_existing;
    }
  }
  count_++;
  if (len_ == 1) {
    DCHECK_EQ(malloc_size_, head_->sz);
  }
}

void QList::Replace(Iterator it, std::string_view elem) {
  Node* node = it.current_;
  uint8_t* newentry = nullptr;
  size_t sz = elem.size();
  uint32_t node_id = it.node_id_;
  if (ABSL_PREDICT_TRUE(!QL_NODE_IS_PLAIN(node) && !IsLargeElement(sz, fill_) &&
                        (newentry = lpReplace(node->u_.entry, &it.zi_, uint_ptr(elem), sz)) !=
                            NULL)) {
    malloc_size_ += NodeSetEntry(node, newentry);
    CoolOff(node, node_id);
  } else if (QL_NODE_IS_PLAIN(node)) {
    if (IsLargeElement(sz, fill_)) {
      zfree(node->u_.entry);
      uint8_t* new_entry = (uint8_t*)zmalloc(sz);
      memcpy(new_entry, elem.data(), sz);
      malloc_size_ += NodeSetEntry(node, new_entry);
      CoolOff(node, node_id);
    } else {
      Insert(it, elem, AFTER);
      DelNode(node);
    }
  } else { /* The node is full or data is a large element */
    Node *split_node = NULL, *new_node;
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
    // The order is: node, new_node, split_node.
    InsertNode(node, new_node, node_id, AFTER);
    if (split_node)
      InsertNode(new_node, split_node, node_id + 1, AFTER);
    count_++;

    /* Delete the replaced element. */
    if (node->count == 1) {
      DelNode(node);
    } else {
      unsigned char* p = lpSeek(node->u_.entry, -1);
      DelPackedIndex(node, p);
      node->dont_compress = 0; /* Re-enable compression */
      new_node = MergeNodes(new_node);

      /* We can't know if the current node and its sibling nodes are correctly compressed,
       * and we don't know if they are within the range of compress depth, so we need to
       * use UpdateCompression() for compression, which checks if node is within compress
       * depth before compressing. */
      // TODO: node_id might be off after merges.
      CoolOff(new_node, node_id + 1);
      CoolOff(new_node->prev, node_id);
      if (new_node->next)
        CoolOff(new_node->next, node_id + 2);
    }
  }
}

void QList::CoolOff(Node* node, uint32_t node_id) {
  if (tiering_params_) {
    // Dry run for offloading decision.
    // a. Node id is withing the offloadable depth - offload it if not already offloaded.
    // b. Node id is outside the offloadable depth - but we have too many nodes that are not
    //    offloaded - take the O(n) route to traverse and offload them. The reason for having such
    //    nodes is because (a) handles node that we touch during operations.
    //    if for example we just perform lpush, then we won't touch any interior nodes, and they
    //    will never get offloaded. The good news is that once interior nodes are offloaded,
    //    we won't need to traverse them again for "trivial" access patterns unless they
    //    get accessed again. Another reason for missing offloaded nodes is that node_id can be
    //    off due to merges (can be improved in future).
    if (node_id >= tiering_params_->node_depth_threshold &&
        node_id + tiering_params_->node_depth_threshold < len_) {
      if (!node->offloaded && !node->io_pending) {
        OffloadNode(node);
      }
    } else if (num_offloaded_nodes_ * 2 + tiering_params_->node_depth_threshold * 2 < len_) {
      // We check `num_offloaded_nodes_ * 2` above to avoid frequent traversals.
      // So only when the gap between offloaded and non-offloaded nodes is large enough,
      // we do a traversal to offload more nodes.
      auto* fw = head_;
      auto* rev = head_->prev;
      uint32_t traverse_node_id = 0;

      // Traverse from both ends towards the middle as we expect more offloads towards the ends
      // due to usual access patterns of adding items via lpush/rpush.
      while (traverse_node_id <= len_ / 2 &&
             (num_offloaded_nodes_ + 2 * tiering_params_->node_depth_threshold) < len_) {
        if (traverse_node_id >= tiering_params_->node_depth_threshold) {
          if (fw->offloaded == 0 && fw->io_pending == 0) {
            OffloadNode(fw);
          }

          // Avoid offloading the same node twice when fw and rev meet in the middle.
          if (rev != fw && rev->offloaded == 0 && rev->io_pending == 0) {
            OffloadNode(rev);
          }
        }
        fw = fw->next;
        rev = rev->prev;
        traverse_node_id++;
      }
    }
  } else if (zstd_threshold_ > 0 && !AllowLZFCompression()) {
    // ZSTD dictionary compression (mutually exclusive with LZF depth-compression).
    if (dict_bulk_finished_) {
      // Steady state: compress individual nodes as they appear.
      if (!dict_compress_failed_ && tl_zstd_dict && node != head_ && node->next != nullptr) {
        CompressNodeWithDict(node);
      }
    } else if (tl_zstd_dict) {
      // Dict exists (trained by this or another instance), bulk-compress all interior nodes.
      CompressWithZstdDict();
      dict_bulk_finished_ = 1;
    } else if (!dict_learning_failed_ && malloc_size_ >= zstd_threshold_ && len_ >= 2) {
      // No dict yet, try to train one.
      TrainZstdDict();
    }
  } else {
    /* Force 'quicklist' to meet compression guidelines set by compress depth.
     * The only way to guarantee interior nodes get compressed is to iterate
     * to our "interior" compress depth then compress the next node we find.
     * If compress depth is larger than the entire list, we return immediately. */
    if (node->recompress)
      CompressRaw(node);
    else
      this->CompressByDepth(node);
  }
}

void QList::CompressByDepth(Node* node) {
  if (len_ == 0)
    return;

  // In ZSTD dict mode (LZF disabled), depth-based LZF compression doesn't apply.
  // Handle the recompress flag via dict and return.
  if (zstd_threshold_ > 0 && !AllowLZFCompression()) {
    if (node && node->recompress && tl_zstd_dict && !dict_compress_failed_ && node != head_ &&
        node->next != nullptr) {
      size_t sz = node->sz;
      if (CompressNodeWithDict(node)) {
        node->recompress = 0;
        malloc_size_ += ssize_t(((quicklistLZF*)node->u_.entry)->sz) - sz;
      }
    }
    return;
  }

  /* The head and tail should never be compressed (we should not attempt to recompress them) */
  DCHECK(head_->recompress == 0 && head_->prev->recompress == 0);

  /* If length is less than our compress depth (from both sides),
   * we can't compress anything. */
  if (!AllowLZFCompression() || len_ < (unsigned int)(compress_ * 2))
    return;

  /* Iterate until we reach compress depth for both sides of the list.a
   * Note: because we do length checks at the *top* of this function,
   *       we can skip explicit null checks below. Everything exists. */
  Node* forward = head_;
  Node* reverse = head_->prev;
  int depth = 0;
  int in_depth = 0;

  while (depth++ < compress_) {
    malloc_size_ += TryDecompressInternal(false, forward);
    malloc_size_ += TryDecompressInternal(false, reverse);

    if (forward == node || reverse == node)
      in_depth = 1;

    /* We passed into compress depth of opposite side of the quicklist
     * so there's no need to compress anything and we can exit. */
    if (forward == reverse || forward->next == reverse)
      return;

    forward = forward->next;
    reverse = reverse->prev;
  }

  if (!in_depth && node) {
    malloc_size_ += TryCompress(node);
  }
  malloc_size_ += TryCompress(forward);
  malloc_size_ += TryCompress(reverse);
}

void QList::Materialize(Node* node) {
  if (!tiering_params_ || (!node->offloaded && !node->io_pending))
    return;

  // Cancel stash in progress before loading.
  if (node->io_pending && tiering_params_->delete_cb) {
    tiering_params_->delete_cb(node);
  }

  // Load the offloaded node data back into memory.
  if (node->offloaded && tiering_params_->onload_cb) {
    stats.onload_requests++;
    tiering_params_->onload_cb(node);
  }

  DCHECK(!node->offloaded);
  DCHECK(!node->io_pending);
  DCHECK(node->u_.entry != nullptr);
}

void QList::AccessForReads(bool recompress, Node* node) {
  DCHECK(node);
  stats.total_node_reads++;

  if (tiering_params_ && (node->offloaded || node->io_pending)) {
    // If the nodes is io_pending. Just call the delete callback to cancel stashing.
    if (node->io_pending && tiering_params_->delete_cb) {
      tiering_params_->delete_cb(node);
    }
    // If the nodes is offlodaded. Load the data back to node.
    if (node->offloaded && tiering_params_->onload_cb) {
      stats.onload_requests++;
      tiering_params_->onload_cb(node);
    }
    // After onload_cb returns, the node entry must be restored and flags cleared.
    DCHECK(!node->offloaded);
    DCHECK(!node->io_pending);
    DCHECK(node->u_.entry != nullptr);
  }

  if (len_ > 2 && node != head_ && node->next != nullptr) {
    stats.interior_node_reads++;
  }
  ssize_t res = TryDecompressInternal(recompress, node);
  malloc_size_ += res;
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
auto QList::MergeNodes(Node* center) -> Node* {
  Node *prev = NULL, *prev_prev = NULL, *next = NULL;
  Node *next_next = NULL, *target = NULL;

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
auto QList::ListpackMerge(Node* a, Node* b) -> Node* {
  AccessForReads(false, a);
  AccessForReads(false, b);
  if ((lpMerge(&a->u_.entry, &b->u_.entry))) {
    /* We merged listpacks! Now remove the unused Node. */
    Node *keep = NULL, *nokeep = NULL;
    if (!a->u_.entry) {
      nokeep = a;
      keep = b;
    } else if (!b->u_.entry) {
      nokeep = b;
      keep = a;
    }
    keep->count = lpLength(keep->u_.entry);
    malloc_size_ += NodeSetEntry(keep, keep->u_.entry);

    keep->recompress = 0; /* Prevent 'keep' from being recompressed if
                           * it becomes head or tail after merging. */

    nokeep->count = 0;
    DelNode(nokeep);
    CoolOff(keep, 0);  // TODO: node_id is unknown here, so just pass 0.
    return keep;
  }

  /* else, the merge returned NULL and nothing changed. */
  return NULL;
}

void QList::DelNode(Node* node) {
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

  /* Update len first, so in CompressByDepth we know exactly len */
  len_--;
  count_ -= node->count;

  // Offloaded nodes don't have entry data, so we only update malloc_size_ for non-offloaded nodes.
  if (!node->offloaded) {
    malloc_size_ -= node->sz;
  }

  if (tiering_params_ && (node->offloaded || node->io_pending)) {
    if (tiering_params_->delete_cb) {
      tiering_params_->delete_cb(node);
    }
  }

  /* If we deleted a node within our compress depth, we
   * now have compressed nodes needing to be decompressed. */
  CompressByDepth(NULL);

  // Head and tail must always be uncompressed. A deletion may promote a
  // ZSTD-compressed interior node to head or tail.
  if (head_) {
    if (head_->IsCompressed()) {
      malloc_size_ += TryDecompressInternal(false, head_);
    }
    if (head_->prev->IsCompressed()) {
      malloc_size_ += TryDecompressInternal(false, head_->prev);
    }
  }

  if (!node->offloaded) {
    zfree(node->u_.entry);
  }

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
bool QList::DelPackedIndex(Node* node, uint8_t* p) {
  DCHECK(!QL_NODE_IS_PLAIN(node));

  if (node->count == 1) {
    DelNode(node);
    return true;
  }

  malloc_size_ += NodeSetEntry(node, lpDelete(node->u_.entry, p, NULL));
  node->count--;
  count_--;

  return false;
}

void QList::OffloadNode(Node* node) {
  DCHECK(tiering_params_ && node->offloaded == 0 && node->io_pending == 0);
  stats.offload_requests++;
  if (tiering_params_->offload_cb) {
    tiering_params_->offload_cb(node);
  }
}

void QList::InitIteratorEntry(Iterator* it) const {
  DCHECK(it->current_);
  const_cast<QList*>(this)->AccessForReads(true, it->current_);
  if (QL_NODE_IS_PLAIN(it->current_)) {
    it->zi_ = it->current_->u_.entry;
  } else {
    it->zi_ = lpSeek(it->current_->u_.entry, it->offset_);
  }
}

auto QList::GetIterator(Where where) const -> Iterator {
  Iterator it;
  it.owner_ = this;
  it.zi_ = NULL;
  if (where == HEAD) {
    it.current_ = head_;
    it.offset_ = 0;
    it.direction_ = FWD;
    it.node_id_ = 0;
  } else {
    it.current_ = _Tail();
    it.offset_ = -1;
    it.direction_ = REV;
    it.node_id_ = len_ - 1;
  }

  if (it.current_) {
    InitIteratorEntry(&it);
  }

  return it;
}

auto QList::GetIterator(long idx) const -> Iterator {
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

  Node* n = seek_forward ? head_ : head_->prev;
  unsigned node_cnt = 0;
  while (ABSL_PREDICT_TRUE(n)) {
    if ((accum + n->count) > seek_index) {
      break;
    } else {
      accum += n->count;
      n = seek_forward ? n->next : n->prev;
      node_cnt++;
    }
  }
  DCHECK(n);
  if (!n)
    return {};

  /* Fix accum so it looks like we seeked in the other direction. */
  if (seek_forward != forward)
    accum = count_ - n->count - accum;

  Iterator iter;
  iter.owner_ = this;
  iter.direction_ = forward ? FWD : REV;
  iter.current_ = n;
  iter.node_id_ = seek_forward ? node_cnt : (len_ - 1 - node_cnt);
  if (forward) {
    /* forward = normal head-to-tail offset. */
    iter.offset_ = index - accum;
  } else {
    /* reverse = need negative offset for tail-to-head, so undo
     * the result of the original index = (-idx) - 1 above. */
    iter.offset_ = (-index) - 1 + accum;
  }

  InitIteratorEntry(&iter);

  return iter;
}

auto QList::Erase(Iterator it) -> Iterator {
  DCHECK(it.current_);

  Node* node = it.current_;
  Node* prev = node->prev;
  Node* next = node->next;

  bool deleted_node = false;
  if (QL_NODE_IS_PLAIN(node)) {
    DelNode(node);
    deleted_node = true;
  } else {
    deleted_node = DelPackedIndex(node, it.zi_);
  }

  it.zi_ = NULL;  // Reset current entry pointer

  // If current node is deleted, we must update iterator node and offset.
  if (deleted_node) {
    if (it.direction_ == FWD) {
      it.current_ = next;
      it.offset_ = 0;
      it.node_id_++;
    } else if (it.direction_ == REV) {
      it.current_ = len_ ? prev : nullptr;
      it.offset_ = -1;
      it.node_id_ = it.node_id_ ? it.node_id_ - 1 : len_ - 1;
    }
  }

  if (it.current_) {
    InitIteratorEntry(&it);
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
  Node* node = it.current_;
  long offset = it.offset_;

  /* iterate over next nodes until everything is deleted. */
  while (extent) {
    Node* next = node->next;

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
      AccessForReads(true, node);
      malloc_size_ += NodeSetEntry(node, lpDeleteRange(node->u_.entry, offset, del));
      node->count -= del;
      count_ -= del;
      if (node->count == 0) {
        DelNode(node);
      } else {
        malloc_size_ += RecompressNode(node);
      }
    }

    extent -= del;
    node = next;
    offset = 0;
  }
  return true;
}

uint8_t* QList::TryExtractListpack() {
  if (len_ != 1 || QL_NODE_IS_PLAIN(head_) || !ShouldStoreAsListPack(head_->sz) ||
      head_->IsCompressed()) {
    return nullptr;
  }

  uint8_t* res = std::exchange(head_->u_.entry, nullptr);
  DelNode(head_);

  return res;
}

bool QList::Iterator::Next() {
  if (!current_)
    return false;

  int plain = QL_NODE_IS_PLAIN(current_);

  // Advance to the next element in the current node.
  if (ABSL_PREDICT_FALSE(plain)) {
    zi_ = NULL;
  } else {
    unsigned char* (*nextFn)(unsigned char*, unsigned char*) = lpNext;
    int offset_update = 1;

    if (direction_ == REV) {
      DCHECK_EQ(REV, direction_);
      nextFn = lpPrev;
      offset_update = -1;
    }
    zi_ = nextFn(current_->u_.entry, zi_);
    offset_ += offset_update;
  }

  if (zi_)
    return true;

  // Move to the next node.
  const_cast<QList*>(owner_)->CompressByDepth(current_);

  if (direction_ == FWD) {
    /* Forward traversal, Jumping to start of next node */
    current_ = current_->next;
    offset_ = 0;
    node_id_++;
  } else {
    /* Reverse traversal, Jumping to end of previous node */
    DCHECK_EQ(REV, direction_);
    offset_ = -1;
    current_ = (current_ == owner_->head_) ? nullptr : current_->prev;
    node_id_--;
  }

  if (!current_)
    return false;

  owner_->InitIteratorEntry(this);
  return zi_ != nullptr;
}

auto QList::Iterator::Get() const -> Entry {
  int plain = QL_NODE_IS_PLAIN(current_);
  if (ABSL_PREDICT_FALSE(plain)) {
    char* str = reinterpret_cast<char*>(current_->u_.entry);
    return Entry(str, current_->sz);
  }

  DCHECK(zi_);

  /* Populate value from existing listpack position */
  unsigned int sz = 0;
  long long val;
  uint8_t* ptr = lpGetValue(zi_, &sz, &val);

  return ptr ? Entry(reinterpret_cast<char*>(ptr), sz) : Entry(val);
}

bool QList::TrainZstdDict() {
  DCHECK_GE(malloc_size_, zstd_threshold_);
  DCHECK_GE(len_, 2u);

  // If the thread-local dictionary is already trained, reuse it.
  if (tl_zstd_dict) {
    return true;
  }

  // Collect raw data from all nodes for estimation and training.
  // We must decompress any already-compressed nodes first.
  vector<pair<const uint8_t*, size_t>> data_pieces;
  data_pieces.reserve(len_);

  for (Node* node = head_; node; node = node->next) {
    DCHECK_EQ(node->encoding, QUICKLIST_NODE_ENCODING_RAW);
    data_pieces.emplace_back(node->u_.entry, size_t(node->sz));
  }

  // Estimate compressibility.
  double ratio = EstimateCompressibility(data_pieces, 2);
  if (ratio > 0.6) {
    VLOG(2) << "QList data not compressible (ratio=" << ratio << ")";
    dict_learning_failed_ = 1;
    return false;
  }

  // Train dictionary.
  string dict_raw = TrainDictionary(data_pieces, 8192, 64);
  if (dict_raw.empty()) {
    dict_learning_failed_ = 1;
    return false;
  }

  auto* state = new ZstdDictState();
  state->cdict = ZSTD_createCDict(dict_raw.data(), dict_raw.size(), 1);
  state->ddict = ZSTD_createDDict(dict_raw.data(), dict_raw.size());
  state->cctx = ZSTD_createCCtx();
  state->dctx = ZSTD_createDCtx();
  if (!state->cdict || !state->ddict || !state->cctx || !state->dctx) {
    delete state;
    dict_learning_failed_ = 1;
    return false;
  }
  tl_zstd_dict = state;

  return true;
}

void QList::CompressWithZstdDict() {
  DCHECK(tl_zstd_dict);

  // Bulk-compress all interior nodes, tracking memory delta.
  bool any_compressed = false;
  bool any_attempted = false;
  for (Node* node = head_; node; node = node->next) {
    if (node == head_ || node->next == nullptr)
      continue;
    if (node->encoding == QUICKLIST_NODE_ENCODING_RAW && node->sz >= MIN_COMPRESS_BYTES)
      any_attempted = true;
    size_t prev_size = zmalloc_usable_size(node->u_.entry);
    if (CompressNodeWithDict(node)) {
      any_compressed = true;
      malloc_size_ += zmalloc_usable_size(node->u_.entry) - prev_size;
    }
  }

  // Only mark failure if we actually tried to compress nodes and all failed.
  if (any_attempted && !any_compressed) {
    dict_compress_failed_ = 1;
  }
}

bool QList::CompressNodeWithDict(Node* node) {
  DCHECK(tl_zstd_dict);

  if (node->encoding != QUICKLIST_NODE_ENCODING_RAW)
    return false;
  if (node->sz < MIN_COMPRESS_BYTES)
    return false;

  stats.compression_attempts++;

  size_t bound = ZSTD_compressBound(node->sz);
  quicklistLZF* dest = (quicklistLZF*)zmalloc(sizeof(quicklistLZF) + bound);
  ZSTD_CCtx_reset(tl_zstd_dict->cctx, ZSTD_reset_session_only);
  size_t csz = ZSTD_compress_usingCDict(tl_zstd_dict->cctx, dest->compressed, bound, node->u_.entry,
                                        node->sz, tl_zstd_dict->cdict);
  CHECK(!ZSTD_isError(csz)) << ZSTD_getErrorName(csz);

  // Reject if absolute improvement is too small or ratio is not good enough.
  // The ratio check (30% savings required) avoids storing incompressible blobs.
  if (csz + MIN_COMPRESS_IMPROVE >= node->sz || csz > node->sz * 7 / 10) {
    zfree(dest);
    stats.bad_compression_attempts++;
    return false;
  }

  dest->sz = csz;
  dest = (quicklistLZF*)zrealloc(dest, sizeof(quicklistLZF) + csz);
  stats.compressed_bytes += csz;
  stats.raw_compressed_bytes += node->sz;
  stats.zstd_dict_compressions++;

  zfree(node->u_.entry);
  node->u_.entry = (unsigned char*)dest;
  node->encoding = QLIST_NODE_ENCODING_ZSTD;
  return true;
}

ssize_t QList::RecompressNode(Node* node) {
  if (!node->recompress || node->dont_compress)
    return 0;
  if (zstd_threshold_ > 0 && !AllowLZFCompression() && tl_zstd_dict && !dict_compress_failed_ &&
      node != head_ && node->next != nullptr) {
    size_t sz = node->sz;
    if (CompressNodeWithDict(node)) {
      node->recompress = 0;
      return ssize_t(((quicklistLZF*)node->u_.entry)->sz) - sz;
    }
  } else if (CompressRaw(node)) {
    return ssize_t(GetLzf(node)->sz) - node->sz;
  }
  return 0;
}

bool QList::DecompressZstdNode(const Node* node, std::string* dest) {
  if (node->encoding != QLIST_NODE_ENCODING_ZSTD)
    return false;
  if (!tl_zstd_dict) {
    LOG(DFATAL) << "ZSTD-compressed node found but no thread-local dict during save";
    return false;
  }
  const quicklistLZF* lzf = (const quicklistLZF*)node->u_.entry;
  dest->resize(node->sz);
  ZSTD_DCtx_reset(tl_zstd_dict->dctx, ZSTD_reset_session_only);
  size_t dsz = ZSTD_decompress_usingDDict(tl_zstd_dict->dctx, dest->data(), dest->size(),
                                          lzf->compressed, lzf->sz, tl_zstd_dict->ddict);
  if (ZSTD_isError(dsz)) {
    LOG(ERROR) << "ZSTD decompression error during save: " << ZSTD_getErrorName(dsz);
    return false;
  }
  return true;
}

void QList::ShutdownThread() {
  delete tl_zstd_dict;
  tl_zstd_dict = nullptr;
}

}  // namespace dfly
