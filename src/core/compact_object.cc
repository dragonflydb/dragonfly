// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/compact_object.h"

// #define XXH_INLINE_ALL
#include <xxhash.h>

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/stream.h"
#include "redis/util.h"
#include "redis/zmalloc.h"  // for non-string objects.
}
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include "base/flags.h"
#include "base/logging.h"
#include "base/pod_array.h"
#include "core/bloom.h"
#include "core/detail/bitpacking.h"
#include "core/huff_coder.h"
#include "core/page_usage/page_usage_stats.h"
#include "core/qlist.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"

ABSL_FLAG(bool, experimental_flat_json, false, "If true uses flat json implementation.");

namespace dfly {
using namespace std;
using detail::binpacked_len;
using MemoryResource = detail::RobjWrapper::MemoryResource;

namespace {

constexpr XXH64_hash_t kHashSeed = 24061983;
constexpr size_t kAlignSize = 8u;

size_t UpdateSize(size_t size, int64_t update) {
  int64_t result = static_cast<int64_t>(size) + update;
  if (result < 0) {
    DCHECK(false) << "Can't decrease " << size << " from " << -update;
    LOG_EVERY_N(ERROR, 30) << "Can't decrease " << size << " from " << -update;
  }
  return result;
}

inline void FreeObjSet(unsigned encoding, void* ptr, MemoryResource* mr) {
  switch (encoding) {
    case kEncodingStrMap2: {
      CompactObj::DeleteMR<StringSet>(ptr);
      break;
    }

    case kEncodingIntSet:
      zfree((void*)ptr);
      break;
    default:
      LOG(FATAL) << "Unknown set encoding type";
  }
}

void FreeList(unsigned encoding, void* ptr, MemoryResource* mr) {
  CHECK_EQ(encoding, kEncodingQL2);
  CompactObj::DeleteMR<QList>(ptr);
}

size_t MallocUsedSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case kEncodingStrMap2: {
      StringSet* ss = (StringSet*)ptr;
      return ss->ObjMallocUsed() + ss->SetMallocUsed() + zmalloc_usable_size(ptr);
    }
    case kEncodingIntSet:
      return intsetBlobLen((intset*)ptr);
  }

  LOG(DFATAL) << "Unknown set encoding type " << encoding;
  return 0;
}

size_t MallocUsedHSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case kEncodingListPack:
      return zmalloc_usable_size(reinterpret_cast<uint8_t*>(ptr));
    case kEncodingStrMap2: {
      StringMap* sm = (StringMap*)ptr;
      return sm->ObjMallocUsed() + sm->SetMallocUsed() + zmalloc_usable_size(ptr);
    }
  }
  LOG(DFATAL) << "Unknown set encoding type " << encoding;
  return 0;
}

size_t MallocUsedZSet(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_LISTPACK:
      return zmalloc_usable_size(reinterpret_cast<uint8_t*>(ptr));
    case OBJ_ENCODING_SKIPLIST: {
      detail::SortedMap* ss = (detail::SortedMap*)ptr;
      return ss->MallocSize() + zmalloc_usable_size(ptr);  // DictMallocSize(zs->dict);
    }
  }
  LOG(DFATAL) << "Unknown set encoding type " << encoding;
  return 0;
}

/* This is a helper function with the goal of estimating the memory
 * size of a radix tree that is used to store Stream IDs.
 *
 * Note: to guess the size of the radix tree is not trivial, so we
 * approximate it considering 16 bytes of data overhead for each
 * key (the ID), and then adding the number of bare nodes, plus some
 * overhead due by the data and child pointers. This secret recipe
 * was obtained by checking the average radix tree created by real
 * workloads, and then adjusting the constants to get numbers that
 * more or less match the real memory usage.
 *
 * Actually the number of nodes and keys may be different depending
 * on the insertion speed and thus the ability of the radix tree
 * to compress prefixes. */
size_t streamRadixTreeMemoryUsage(rax* rax) {
  size_t size = sizeof(*rax);
  size = rax->numele * sizeof(streamID);
  size += rax->numnodes * sizeof(raxNode);
  /* Add a fixed overhead due to the aux data pointer, children, ... */
  size += rax->numnodes * sizeof(long) * 30;
  return size;
}

size_t MallocUsedStream(stream* s) {
  size_t asize = sizeof(*s);
  asize += streamRadixTreeMemoryUsage(s->rax_tree);

  /* Now we have to add the listpacks. The last listpack is often non
   * complete, so we estimate the size of the first N listpacks, and
   * use the average to compute the size of the first N-1 listpacks, and
   * finally add the real size of the last node. */
  raxIterator ri;
  raxStart(&ri, s->rax_tree);
  raxSeek(&ri, "^", NULL, 0);
  size_t lpsize = 0, samples = 0;
  while (raxNext(&ri)) {
    uint8_t* lp = (uint8_t*)ri.data;
    /* Use the allocated size, since we overprovision the node initially. */
    lpsize += zmalloc_size(lp);
    samples++;
  }
  if (s->rax_tree->numele <= samples) {
    asize += lpsize;
  } else {
    if (samples)
      lpsize /= samples; /* Compute the average. */
    asize += lpsize * (s->rax_tree->numele - 1);
    /* No need to check if seek succeeded, we enter this branch only
     * if there are a few elements in the radix tree. */
    raxSeek(&ri, "$", NULL, 0);
    raxNext(&ri);
    /* Use the allocated size, since we overprovision the node initially. */
    asize += zmalloc_size(ri.data);
  }
  raxStop(&ri);

  /* Consumer groups also have a non trivial memory overhead if there
   * are many consumers and many groups, let's count at least the
   * overhead of the pending entries in the groups and consumers
   * PELs. */
  if (s->cgroups) {
    raxStart(&ri, s->cgroups);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) {
      streamCG* cg = (streamCG*)ri.data;
      asize += sizeof(*cg);
      asize += streamRadixTreeMemoryUsage(cg->pel);
      asize += sizeof(streamNACK) * raxSize(cg->pel);

      /* For each consumer we also need to add the basic data
       * structures and the PEL memory usage. */
      raxIterator cri;
      raxStart(&cri, cg->consumers);
      raxSeek(&cri, "^", NULL, 0);
      while (raxNext(&cri)) {
        const streamConsumer* consumer = (const streamConsumer*)cri.data;
        asize += sizeof(*consumer);
        asize += sdslen(consumer->name);
        asize += streamRadixTreeMemoryUsage(consumer->pel);
        /* Don't count NACKs again, they are shared with the
         * consumer group PEL. */
      }
      raxStop(&cri);
    }
    raxStop(&ri);
  }
  return asize;
}

inline void FreeObjHash(unsigned encoding, void* ptr) {
  switch (encoding) {
    case kEncodingStrMap2:
      CompactObj::DeleteMR<StringMap>(ptr);
      break;
    case kEncodingListPack:
      lpFree((uint8_t*)ptr);
      break;
    default:
      LOG(FATAL) << "Unknown hset encoding type " << encoding;
  }
}

inline void FreeObjZset(unsigned encoding, void* ptr) {
  switch (encoding) {
    case OBJ_ENCODING_SKIPLIST:
      CompactObj::DeleteMR<detail::SortedMap>(ptr);
      break;
    case OBJ_ENCODING_LISTPACK:
      zfree(ptr);
      break;
    default:
      LOG(FATAL) << "Unknown sorted set encoding" << encoding;
  }
}

pair<void*, bool> DefragStrMap2(StringMap* sm, PageUsage* page_usage) {
  bool realloced = false;

  for (auto it = sm->begin(); it != sm->end(); ++it)
    realloced |= it.ReallocIfNeeded(page_usage);

  return {sm, realloced};
}

pair<void*, bool> DefragListPack(uint8_t* lp, PageUsage* page_usage) {
  if (!page_usage->IsPageForObjectUnderUtilized(lp))
    return {lp, false};

  size_t lp_bytes = lpBytes(lp);
  uint8_t* replacement = lpNew(lpBytes(lp));
  memcpy(replacement, lp, lp_bytes);
  lpFree(lp);

  return {replacement, true};
}

pair<void*, bool> DefragIntSet(intset* is, PageUsage* page_usage) {
  if (!page_usage->IsPageForObjectUnderUtilized(is))
    return {is, false};

  const size_t blob_len = intsetBlobLen(is);
  intset* replacement = (intset*)zmalloc(blob_len);
  memcpy(replacement, is, blob_len);

  zfree(is);
  return {replacement, true};
}

pair<void*, bool> DefragSortedMap(detail::SortedMap* sm, PageUsage* page_usage) {
  const bool reallocated = sm->DefragIfNeeded(page_usage);
  return {sm, reallocated};
}

pair<void*, bool> DefragStrSet(StringSet* ss, PageUsage* page_usage) {
  bool realloced = false;

  for (auto it = ss->begin(); it != ss->end(); ++it)
    realloced |= it.ReallocIfNeeded(page_usage);

  return {ss, realloced};
}

// Iterates over allocations of internal hash data structures and re-allocates
// them if their pages are underutilized.
// Returns pointer to new object ptr and whether any re-allocations happened.
pair<void*, bool> DefragHash(unsigned encoding, void* ptr, PageUsage* page_usage) {
  switch (encoding) {
    // Listpack is stored as a single contiguous array
    case kEncodingListPack: {
      return DefragListPack((uint8_t*)ptr, page_usage);
    }

    // StringMap supports re-allocation of it's internal nodes
    case kEncodingStrMap2: {
      return DefragStrMap2((StringMap*)ptr, page_usage);
    }

    default:
      ABSL_UNREACHABLE();
  }
}

pair<void*, bool> DefragSet(unsigned encoding, void* ptr, PageUsage* page_usage) {
  switch (encoding) {
    // Int sets have flat storage
    case kEncodingIntSet: {
      return DefragIntSet((intset*)ptr, page_usage);
    }

    case kEncodingStrMap2: {
      return DefragStrSet((StringSet*)ptr, page_usage);
    }

    default:
      ABSL_UNREACHABLE();
  }
}

pair<void*, bool> DefragZSet(unsigned encoding, void* ptr, PageUsage* page_usage) {
  switch (encoding) {
    // Listpack is stored as a single contiguous array
    case OBJ_ENCODING_LISTPACK: {
      return DefragListPack((uint8_t*)ptr, page_usage);
    }

    // SKIPLIST really means ScoreMap
    case OBJ_ENCODING_SKIPLIST: {
      return DefragSortedMap((detail::SortedMap*)ptr, page_usage);
    }

    default:
      ABSL_UNREACHABLE();
  }
}

pair<void*, bool> DefragList(unsigned /**/, void* ptr, PageUsage* page_usage) {
  auto* qlist_ptr = static_cast<QList*>(ptr);
  bool reallocated = qlist_ptr->DefragIfNeeded(page_usage);
  return {ptr, reallocated};
}

inline void FreeObjStream(void* ptr) {
  freeStream((stream*)ptr);
}

// converts 7-bit packed length back to ascii length. Note that this conversion
// is not accurate since it maps 7 bytes to 8 bytes (rounds up), while we may have
// 7 byte strings converted to 7 byte as well.
inline constexpr size_t ascii_len(size_t bin_len) {
  return (bin_len * 8) / 7;
}

inline const uint8_t* to_byte(const void* s) {
  return reinterpret_cast<const uint8_t*>(s);
}

static_assert(binpacked_len(7) == 7);
static_assert(binpacked_len(8) == 7);
static_assert(binpacked_len(15) == 14);
static_assert(binpacked_len(16) == 14);
static_assert(binpacked_len(17) == 15);
static_assert(binpacked_len(18) == 16);
static_assert(binpacked_len(19) == 17);
static_assert(binpacked_len(20) == 18);
static_assert(ascii_len(14) == 16);
static_assert(ascii_len(15) == 17);
static_assert(ascii_len(16) == 18);
static_assert(ascii_len(17) == 19);

struct Huffman {
  HuffmanEncoder encoder;
  HuffmanDecoder decoder;
};

struct TL {
  MemoryResource* local_mr = PMR_NS::get_default_resource();
  base::PODArray<uint8_t> tmp_buf;
  string tmp_str;
  size_t small_str_bytes;
  Huffman huff_keys, huff_string_values;
  uint64_t huff_encode_total = 0, huff_encode_success = 0;  // success/total metrics.

  const HuffmanDecoder& GetHuffmanDecoder(uint8_t huffman_domain) const {
    return huffman_domain == CompactObj::HUFF_KEYS ? huff_keys.decoder : huff_string_values.decoder;
  }
};

thread_local TL tl;

constexpr bool kUseAsciiEncoding = true;

}  // namespace

static_assert(sizeof(CompactObj) == 18);

namespace detail {

size_t RobjWrapper::MallocUsed(bool slow) const {
  if (!inner_obj_)
    return 0;

  switch (type_) {
    case OBJ_STRING:
      CHECK_EQ(OBJ_ENCODING_RAW, encoding_);
      return InnerObjMallocUsed();
    case OBJ_LIST:
      return ((QList*)inner_obj_)->MallocUsed(slow);
    case OBJ_SET:
      return MallocUsedSet(encoding_, inner_obj_);
    case OBJ_HASH:
      return MallocUsedHSet(encoding_, inner_obj_);
    case OBJ_ZSET:
      return MallocUsedZSet(encoding_, inner_obj_);
    case OBJ_STREAM:
      return slow ? MallocUsedStream((stream*)inner_obj_) : sz_;

    default:
      LOG(FATAL) << "Not supported " << type_;
  }

  return 0;
}

size_t RobjWrapper::Size() const {
  switch (type_) {
    case OBJ_STRING:
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding_);
      return sz_;
    case OBJ_LIST:
      return ((QList*)inner_obj_)->Size();
    case OBJ_ZSET: {
      switch (encoding_) {
        case OBJ_ENCODING_SKIPLIST: {
          SortedMap* ss = (SortedMap*)inner_obj_;
          return ss->Size();
        }
        case OBJ_ENCODING_LISTPACK:
          return lpLength((uint8_t*)inner_obj_) / 2;
        default:
          LOG(FATAL) << "Unknown sorted set encoding" << encoding_;
      }
    }
    case OBJ_SET:
      switch (encoding_) {
        case kEncodingIntSet: {
          intset* is = (intset*)inner_obj_;
          return intsetLen(is);
        }
        case kEncodingStrMap2: {
          StringSet* ss = (StringSet*)inner_obj_;
          return ss->UpperBoundSize();
        }
        default:
          LOG(FATAL) << "Unexpected encoding " << encoding_;
      };
    case OBJ_HASH:
      switch (encoding_) {
        case kEncodingListPack: {
          uint8_t* lp = (uint8_t*)inner_obj_;
          return lpLength(lp) / 2;
        } break;

        case kEncodingStrMap2: {
          StringMap* sm = (StringMap*)inner_obj_;
          return sm->UpperBoundSize();
        }
        default:
          LOG(FATAL) << "Unexpected encoding " << encoding_;
      }
    case OBJ_STREAM:
      // Size mean malloc bytes for streams
      return sz_;
    default:;
  }
  return 0;
}

void RobjWrapper::Free(MemoryResource* mr) {
  if (!inner_obj_)
    return;
  DVLOG(1) << "RobjWrapper::Free " << inner_obj_;

  switch (type_) {
    case OBJ_STRING:
      DVLOG(2) << "Freeing string object";
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding_);
      mr->deallocate(inner_obj_, 0, 8);  // we do not keep the allocated size.
      break;
    case OBJ_LIST:
      FreeList(encoding_, inner_obj_, mr);
      break;
    case OBJ_SET:
      FreeObjSet(encoding_, inner_obj_, mr);
      break;
    case OBJ_ZSET:
      FreeObjZset(encoding_, inner_obj_);
      break;
    case OBJ_HASH:
      FreeObjHash(encoding_, inner_obj_);
      break;
    case OBJ_MODULE:
      LOG(FATAL) << "Unsupported OBJ_MODULE type";
      break;
    case OBJ_STREAM:
      FreeObjStream(inner_obj_);
      break;
    default:
      LOG(FATAL) << "Unknown object type";
      break;
  }
  Set(nullptr, 0);
}

uint64_t RobjWrapper::HashCode() const {
  switch (type_) {
    case OBJ_STRING:
      DCHECK_EQ(OBJ_ENCODING_RAW, encoding());
      {
        auto str = AsView();
        return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type for hashcode " << type_;
  }
  return 0;
}

bool RobjWrapper::Equal(const RobjWrapper& ow) const {
  if (ow.type_ != type_ || ow.encoding_ != encoding_)
    return false;

  if (type_ == OBJ_STRING) {
    DCHECK_EQ(OBJ_ENCODING_RAW, encoding());
    return AsView() == ow.AsView();
  }
  LOG(FATAL) << "Unsupported type " << type_;
  return false;
}

bool RobjWrapper::Equal(string_view sv) const {
  if (type() != OBJ_STRING)
    return false;

  DCHECK_EQ(OBJ_ENCODING_RAW, encoding());
  return AsView() == sv;
}

void RobjWrapper::SetString(string_view s, MemoryResource* mr) {
  type_ = OBJ_STRING;
  encoding_ = OBJ_ENCODING_RAW;

  if (s.size() > sz_) {
    size_t cur_cap = InnerObjMallocUsed();
    if (s.size() > cur_cap) {
      MakeInnerRoom(cur_cap, s.size(), mr);
    }
    memcpy(inner_obj_, s.data(), s.size());
    sz_ = s.size();
  }
}

void RobjWrapper::ReserveString(size_t size, MemoryResource* mr) {
  CHECK_EQ(inner_obj_, nullptr);
  type_ = OBJ_STRING;
  encoding_ = OBJ_ENCODING_RAW;
  MakeInnerRoom(0, size, mr);
}

void RobjWrapper::AppendString(string_view s, MemoryResource* mr) {
  size_t cur_cap = InnerObjMallocUsed();
  CHECK(cur_cap >= sz_ + s.size()) << cur_cap << " " << sz_ << " " << s.size();
  memcpy(reinterpret_cast<uint8_t*>(inner_obj_) + sz_, s.data(), s.size());
  sz_ += s.size();
}

void RobjWrapper::SetSize(uint64_t size) {
  sz_ = size;
}

bool RobjWrapper::DefragIfNeeded(PageUsage* page_usage) {
  auto do_defrag = [this, &page_usage](auto defrag_fun) mutable {
    auto [new_ptr, realloced] = defrag_fun(encoding_, inner_obj_, page_usage);
    inner_obj_ = new_ptr;
    return realloced;
  };

  if (type() == OBJ_STRING) {
    if (page_usage->IsPageForObjectUnderUtilized(inner_obj())) {
      ReallocateString(tl.local_mr);
      return true;
    }
  } else if (type() == OBJ_HASH) {
    return do_defrag(DefragHash);
  } else if (type() == OBJ_SET) {
    return do_defrag(DefragSet);
  } else if (type() == OBJ_ZSET) {
    return do_defrag(DefragZSet);
  } else if (type() == OBJ_LIST) {
    return do_defrag(DefragList);
  }

  page_usage->RecordNotSupported();
  return false;
}

int RobjWrapper::ZsetAdd(double score, std::string_view ele, int in_flags, int* out_flags,
                         double* newscore) {
  *out_flags = 0; /* We'll return our response flags. */
  double curscore;

  /* NaN as input is an error regardless of all the other parameters. */
  if (isnan(score)) {
    *out_flags = ZADD_OUT_NAN;
    return 0;
  }

  /* Update the sorted set according to its encoding. */
  if (encoding_ == OBJ_ENCODING_LISTPACK) {
    /* Turn options into simple to check vars. */
    bool incr = (in_flags & ZADD_IN_INCR) != 0;
    bool nx = (in_flags & ZADD_IN_NX) != 0;
    bool xx = (in_flags & ZADD_IN_XX) != 0;
    bool gt = (in_flags & ZADD_IN_GT) != 0;
    bool lt = (in_flags & ZADD_IN_LT) != 0;

    uint8_t* lp = (uint8_t*)inner_obj_;
    uint8_t* eptr = ZzlFind(lp, ele, &curscore);
    if (eptr != NULL) {
      /* NX? Return, same element already exists. */
      if (nx) {
        *out_flags |= ZADD_OUT_NOP;
        return 1;
      }

      /* Prepare the score for the increment if needed. */
      if (incr) {
        score += curscore;
        if (isnan(score)) {
          *out_flags |= ZADD_OUT_NAN;
          return 0;
        }
      }

      /* GT/LT? Only update if score is greater/less than current. */
      if ((lt && score >= curscore) || (gt && score <= curscore)) {
        *out_flags |= ZADD_OUT_NOP;
        return 1;
      }

      if (newscore)
        *newscore = score;

      /* Remove and re-insert when score changed. */
      if (score != curscore) {
        lp = lpDeleteRangeWithEntry(lp, &eptr, 2);
        lp = detail::ZzlInsert(lp, ele, score);
        inner_obj_ = lp;
        *out_flags |= ZADD_OUT_UPDATED;
      }

      return 1;
    } else if (!xx) {
      unsigned zl_len = lpLength(lp) / 2;

      /* check if the element is too large or the list
       * becomes too long *before* executing zzlInsert. */
      if (zl_len >= server.zset_max_listpack_entries ||
          ele.size() > server.zset_max_listpack_value) {
        inner_obj_ = SortedMap::FromListPack(tl.local_mr, lp);
        lpFree(lp);
        encoding_ = OBJ_ENCODING_SKIPLIST;
      } else {
        lp = detail::ZzlInsert(lp, ele, score);
        inner_obj_ = lp;
        if (newscore)
          *newscore = score;
        *out_flags |= ZADD_OUT_ADDED;
        return 1;
      }
    } else {
      *out_flags |= ZADD_OUT_NOP;
      return 1;
    }
  }

  CHECK_EQ(encoding_, OBJ_ENCODING_SKIPLIST);
  SortedMap* ss = (SortedMap*)inner_obj_;
  return ss->AddElem(score, ele, in_flags, out_flags, newscore);
}

void RobjWrapper::ReallocateString(MemoryResource* mr) {
  DCHECK_EQ(type(), OBJ_STRING);
  void* old_ptr = inner_obj_;
  inner_obj_ = mr->allocate(sz_, kAlignSize);
  memcpy(inner_obj_, old_ptr, sz_);
  mr->deallocate(old_ptr, 0, kAlignSize);
}

void RobjWrapper::Init(unsigned type, unsigned encoding, void* inner) {
  type_ = type;
  encoding_ = encoding;
  Set(inner, 0);
}

inline size_t RobjWrapper::InnerObjMallocUsed() const {
  return zmalloc_size(inner_obj_);
}

void RobjWrapper::MakeInnerRoom(size_t current_cap, size_t desired, MemoryResource* mr) {
  if (current_cap * 2 > desired) {
    if (desired < SDS_MAX_PREALLOC)
      desired *= 2;
    else
      desired += SDS_MAX_PREALLOC;
  }

  void* newp = mr->allocate(desired, kAlignSize);
  if (sz_) {
    memcpy(newp, inner_obj_, sz_);
  }

  if (current_cap) {
    mr->deallocate(inner_obj_, current_cap, kAlignSize);
  }
  inner_obj_ = newp;
}

}  // namespace detail

uint32_t JsonEnconding() {
  thread_local uint32_t json_enc =
      absl::GetFlag(FLAGS_experimental_flat_json) ? kEncodingJsonFlat : kEncodingJsonCons;
  return json_enc;
}

using namespace std;

auto CompactObj::GetStatsThreadLocal() -> Stats {
  Stats res;
  res.small_string_bytes = tl.small_str_bytes;
  res.huff_encode_total = tl.huff_encode_total;
  res.huff_encode_success = tl.huff_encode_success;
  return res;
}

void CompactObj::InitThreadLocal(MemoryResource* mr) {
  tl.local_mr = mr;
  tl.tmp_buf = base::PODArray<uint8_t>{mr};
}

bool CompactObj::InitHuffmanThreadLocal(HuffmanDomain domain, std::string_view hufftable) {
  string err_msg;

  Huffman* huffman = nullptr;
  switch (domain) {
    case HUFF_KEYS:
      huffman = &tl.huff_keys;
      break;
    case HUFF_STRING_VALUES:
      huffman = &tl.huff_string_values;
      break;
  }

  // We do not allow overriding the existing huffman table once it is set.
  if (huffman->encoder.valid()) {
    return false;
  }

  if (!huffman->encoder.Load(hufftable, &err_msg)) {
    LOG(DFATAL) << "Failed to load huffman table: " << err_msg;
    return false;
  }

  if (!huffman->decoder.Load(hufftable, &err_msg)) {
    LOG(DFATAL) << "Failed to load huffman table: " << err_msg;
    return false;
  }
  return true;
}

CompactObj::~CompactObj() {
  if (HasAllocated()) {
    Free();
  }
}

CompactObj& CompactObj::operator=(CompactObj&& o) noexcept {
  DCHECK(&o != this);

  SetMeta(o.taglen_, o.mask_);  // frees own previous resources
  memcpy(&u_, &o.u_, sizeof(u_));
  huffman_domain_ = o.huffman_domain_;

  o.taglen_ = 0;  // forget all data
  o.huffman_domain_ = 0;
  o.mask_ = 0;
  return *this;
}

size_t CompactObj::Size() const {
  size_t raw_size = 0;
  uint8_t first_byte = 0;
  if (IsInline()) {
    raw_size = taglen_;
    first_byte = u_.inline_str[0];
  } else {
    switch (taglen_) {
      case SMALL_TAG:
        raw_size = u_.small_str.size();
        first_byte = u_.small_str.first_byte();
        break;
      case INT_TAG: {
        absl::AlphaNum an(u_.ival);
        raw_size = an.size();
        break;
      }
      case EXTERNAL_TAG:
        raw_size = u_.ext_ptr.serialized_size;
        first_byte = GetFirstByte();
        break;
      case ROBJ_TAG:
        raw_size = u_.r_obj.Size();
        first_byte = *(uint8_t*)u_.r_obj.inner_obj();
        break;
      case JSON_TAG:
        DCHECK_EQ(mask_bits_.encoding, NONE_ENC);
        if (JsonEnconding() == kEncodingJsonFlat) {
          raw_size = u_.json_obj.flat.json_len;
        } else {
          raw_size = u_.json_obj.cons.json_ptr->size();
        }
        break;
      case SBF_TAG:
        DCHECK_EQ(mask_bits_.encoding, NONE_ENC);
        raw_size = u_.sbf->current_size();
        break;
      default:
        LOG(DFATAL) << "Should not reach " << int(taglen_);
    }
  }
  return GetStrEncoding().DecodedSize(raw_size, first_byte);
}

uint64_t CompactObj::HashCode() const {
  DCHECK(taglen_ != JSON_TAG) << "JSON type cannot be used for keys!";

  if (mask_bits_.encoding == NONE_ENC) {
    if (IsInline()) {
      return XXH3_64bits_withSeed(u_.inline_str, taglen_, kHashSeed);
    }

    switch (taglen_) {
      case SMALL_TAG:
        return u_.small_str.HashCode();
      case ROBJ_TAG:
        return u_.r_obj.HashCode();
      case INT_TAG: {
        absl::AlphaNum an(u_.ival);
        return XXH3_64bits_withSeed(an.data(), an.size(), kHashSeed);
      }
    }
  }

  DCHECK(mask_bits_.encoding);

  if (IsInline()) {
    // Buffer must accommodate maximum decompressed size from inline storage
    // Highly compressible data can achieve ~8x compression (e.g., repeated character)
    // kInlineLen (16 bytes) compressed -> up to 128 bytes decompressed
    char buf[kInlineLen * 8];
    size_t decoded_len = GetStrEncoding().Decode(string_view{u_.inline_str, taglen_}, buf);
    return XXH3_64bits_withSeed(buf, decoded_len, kHashSeed);
  }

  string_view sv = GetSlice(&tl.tmp_str);
  return XXH3_64bits_withSeed(sv.data(), sv.size(), kHashSeed);
}

uint64_t CompactObj::HashCode(string_view str) {
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}

CompactObjType CompactObj::ObjType() const {
  if (IsInline() || taglen_ == INT_TAG || taglen_ == SMALL_TAG)
    return OBJ_STRING;

  if (taglen_ == EXTERNAL_TAG) {
    VLOG(0) << "My type is external";
    switch (static_cast<ExternalRep>(u_.ext_ptr.representation)) {
      case ExternalRep::STRING:
        VLOG(0) << "My type is a string";
        return OBJ_STRING;
      case ExternalRep::SERIALIZED_MAP:
        VLOG(0) << "Mype is a hash map";
        return OBJ_HASH;
    };
  }

  if (taglen_ == ROBJ_TAG)
    return u_.r_obj.type();

  if (taglen_ == JSON_TAG) {
    return OBJ_JSON;
  }

  if (taglen_ == SBF_TAG) {
    return OBJ_SBF;
  }

  LOG(FATAL) << "TBD " << int(taglen_);
  return kInvalidCompactObjType;
}

unsigned CompactObj::Encoding() const {
  switch (taglen_) {
    case ROBJ_TAG:
      return u_.r_obj.encoding();
    case INT_TAG:
      return OBJ_ENCODING_INT;
    default:
      return OBJ_ENCODING_RAW;
  }
}

void CompactObj::InitRobj(CompactObjType type, unsigned encoding, void* obj) {
  DCHECK_NE(type, OBJ_STRING);
  SetMeta(ROBJ_TAG, mask_);
  u_.r_obj.Init(type, encoding, obj);
}

void CompactObj::SetInt(int64_t val) {
  DCHECK(!IsExternal());

  if (INT_TAG != taglen_) {
    SetMeta(INT_TAG, mask_);
    mask_bits_.encoding = NONE_ENC;
  }

  u_.ival = val;
}

std::optional<int64_t> CompactObj::TryGetInt() const {
  if (taglen_ != INT_TAG)
    return std::nullopt;
  int64_t val = u_.ival;
  return val;
}

auto CompactObj::GetJson() const -> JsonType* {
  if (ObjType() == OBJ_JSON) {
    DCHECK_EQ(JsonEnconding(), kEncodingJsonCons);
    return u_.json_obj.cons.json_ptr;
  }
  return nullptr;
}

void CompactObj::SetJson(JsonType&& j) {
  if (taglen_ == JSON_TAG && JsonEnconding() == kEncodingJsonCons) {
    DCHECK(u_.json_obj.cons.json_ptr != nullptr);  // must be allocated
    u_.json_obj.cons.json_ptr->swap(j);
    DCHECK(jsoncons::is_trivial_storage(u_.json_obj.cons.json_ptr->storage_kind()) ||
           u_.json_obj.cons.json_ptr->get_allocator().resource() == tl.local_mr);

    // We do not set bytes_used as this is needed. Consider the two following cases:
    // 1. old json contains 50 bytes. The delta for new one is 50, so the total bytes
    // the new json occupies is 100.
    // 2. old json contains 100 bytes. The delta for new one is -50, so the total bytes
    // the new json occupies is 50.
    // Both of the cases are covered in SetJsonSize and JsonMemTracker. See below.
    return;
  }

  SetMeta(JSON_TAG);
  u_.json_obj.cons.json_ptr = AllocateMR<JsonType>(std::move(j));

  // With trivial storage json_ptr->get_allocator() throws an exception.
  DCHECK(jsoncons::is_trivial_storage(u_.json_obj.cons.json_ptr->storage_kind()) ||
         u_.json_obj.cons.json_ptr->get_allocator().resource() == tl.local_mr);
  u_.json_obj.cons.bytes_used = 0;
}

void CompactObj::SetJsonSize(int64_t size) {
  if (taglen_ == JSON_TAG && JsonEnconding() == kEncodingJsonCons) {
    // JSON.SET or if mem hasn't changed from a JSON op then we just update.
    u_.json_obj.cons.bytes_used = UpdateSize(u_.json_obj.cons.bytes_used, size);
  }
}

void CompactObj::AddStreamSize(int64_t size) {
  if (size < 0) {
    // We might have a negative size. For example, if we remove a consumer,
    // the tracker will report a negative net (since we deallocated),
    // so the object now consumes less memory than it did before. This DCHECK
    // is for fanity and to catch any potential issues with our tracking approach.
    DCHECK(static_cast<int64_t>(u_.r_obj.Size()) >= size);
  }
  u_.r_obj.SetSize((u_.r_obj.Size() + size));
}

void CompactObj::SetJson(const uint8_t* buf, size_t len) {
  SetMeta(JSON_TAG);
  u_.json_obj.flat.flat_ptr = (uint8_t*)tl.local_mr->allocate(len, kAlignSize);
  memcpy(u_.json_obj.flat.flat_ptr, buf, len);
  u_.json_obj.flat.json_len = len;
}

void CompactObj::SetSBF(uint64_t initial_capacity, double fp_prob, double grow_factor) {
  if (taglen_ == SBF_TAG) {  // already json
    *u_.sbf = SBF(initial_capacity, fp_prob, grow_factor, tl.local_mr);
  } else {
    SetMeta(SBF_TAG);
    u_.sbf = AllocateMR<SBF>(initial_capacity, fp_prob, grow_factor, tl.local_mr);
  }
}

SBF* CompactObj::GetSBF() const {
  DCHECK_EQ(SBF_TAG, taglen_);
  return u_.sbf;
}

void CompactObj::SetString(std::string_view str, bool is_key) {
  CHECK(!IsExternal());
  mask_bits_.encoding = NONE_ENC;

  // Trying auto-detection heuristics first.
  if (str.size() <= 20) {
    long long ival;
    static_assert(sizeof(long long) == 8);

    // We use redis string2ll to be compatible with Redis.
    if (string2ll(str.data(), str.size(), &ival)) {
      SetMeta(INT_TAG, mask_);
      u_.ival = ival;

      return;
    }

    if (str.size() <= kInlineLen) {
      SetMeta(str.size(), mask_);
      if (!str.empty())
        memcpy(u_.inline_str, str.data(), str.size());
      return;
    }
  }

  EncodeString(str, is_key);
}

void CompactObj::ReserveString(size_t size) {
  mask_bits_.encoding = NONE_ENC;
  SetMeta(ROBJ_TAG, mask_);

  u_.r_obj.ReserveString(size, tl.local_mr);
}

void CompactObj::AppendString(std::string_view str) {
  u_.r_obj.AppendString(str, tl.local_mr);
}

string_view CompactObj::GetSlice(string* scratch) const {
  CHECK(!IsExternal());

  if (mask_bits_.encoding) {
    GetString(scratch);
    return *scratch;
  }

  if (IsInline()) {
    return string_view{u_.inline_str, taglen_};
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    scratch->assign(an.Piece());

    return *scratch;
  }

  // no encoding.
  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    return u_.r_obj.AsView();
  }

  if (taglen_ == SMALL_TAG) {
    u_.small_str.Get(scratch);
    return *scratch;
  }

  LOG(FATAL) << "Bad tag " << int(taglen_);

  return string_view{};
}

bool CompactObj::DefragIfNeeded(PageUsage* page_usage) {
  switch (taglen_) {
    case ROBJ_TAG:
      // currently only these object types are supported for this operation
      if (u_.r_obj.inner_obj() != nullptr) {
        return u_.r_obj.DefragIfNeeded(page_usage);
      }
      return false;
    case SMALL_TAG:
      return u_.small_str.DefragIfNeeded(page_usage);
    case JSON_TAG:
      return u_.json_obj.DefragIfNeeded(page_usage);
    case INT_TAG:
      page_usage->RecordNotRequired();
      // this is not relevant in this case
      return false;
    case EXTERNAL_TAG:
      page_usage->RecordNotRequired();
      return false;
    default:
      page_usage->RecordNotRequired();
      // This is the case when the object is at inline_str
      return false;
  }
}

bool CompactObj::HasAllocated() const {
  if (IsRef() || taglen_ == INT_TAG || IsInline() || taglen_ == EXTERNAL_TAG ||
      (taglen_ == ROBJ_TAG && u_.r_obj.inner_obj() == nullptr))
    return false;

  DCHECK(taglen_ == ROBJ_TAG || taglen_ == SMALL_TAG || taglen_ == JSON_TAG || taglen_ == SBF_TAG);
  return true;
}

bool CompactObj::TagAllowsEmptyValue() const {
  const auto type = ObjType();
  return type == OBJ_JSON || type == OBJ_STREAM || type == OBJ_STRING || type == OBJ_SBF ||
         type == OBJ_SET;
}

void __attribute__((noinline)) CompactObj::GetString(string* res) const {
  res->resize(Size());
  GetString(res->data());
}

void CompactObj::GetString(char* dest) const {
  CHECK(!IsExternal());

  if (IsInline()) {
    GetStrEncoding().Decode({u_.inline_str, taglen_}, dest);
    return;
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    memcpy(dest, an.data(), an.size());
    return;
  }

  if (mask_bits_.encoding) {
    StrEncoding str_encoding = GetStrEncoding();
    string_view decode_blob;

    if (taglen_ == ROBJ_TAG) {
      CHECK_EQ(OBJ_STRING, u_.r_obj.type());
      DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
      decode_blob = {(const char*)u_.r_obj.inner_obj(), u_.r_obj.Size()};
    } else {
      CHECK_EQ(SMALL_TAG, taglen_);
      auto& ss = u_.small_str;

      char* copy_dest;
      if (str_encoding.enc_ == HUFFMAN_ENC) {
        tl.tmp_buf.resize(ss.size());
        copy_dest = reinterpret_cast<char*>(tl.tmp_buf.data());
      } else {
        // Write to rightmost location of dest buffer to leave some bytes for inline unpacking
        size_t decoded_len = str_encoding.DecodedSize(ss.size(), ss.first_byte());
        copy_dest = dest + (decoded_len - ss.size());
      }

      ss.Get(copy_dest);
      decode_blob = {copy_dest, ss.size()};
    }

    str_encoding.Decode(decode_blob, dest);
    return;
  }

  // no encoding.
  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    memcpy(dest, u_.r_obj.inner_obj(), u_.r_obj.Size());
    return;
  }

  if (taglen_ == SMALL_TAG)
    return u_.small_str.Get(dest);

  LOG(FATAL) << "Bad tag " << int(taglen_);
}

void CompactObj::SetExternal(size_t offset, uint32_t sz, ExternalRep rep) {
  uint8_t first_byte = 0;
  if (mask_bits_.encoding == HUFFMAN_ENC) {
    CHECK(rep == ExternalRep::STRING);
    first_byte = GetFirstByte();
  }
  SetMeta(EXTERNAL_TAG, mask_);

  u_.ext_ptr.is_cool = 0;
  u_.ext_ptr.representation = static_cast<uint8_t>(rep);
  u_.ext_ptr.first_byte = first_byte;
  u_.ext_ptr.page_offset = offset % 4096;
  u_.ext_ptr.serialized_size = sz;
  u_.ext_ptr.offload.page_index = offset / 4096;
}

CompactObj::ExternalRep CompactObj::GetExternalRep() const {
  DCHECK(IsExternal());
  return static_cast<CompactObj::ExternalRep>(u_.ext_ptr.representation);
}

void CompactObj::SetCool(size_t offset, uint32_t sz, ExternalRep rep,
                         detail::TieredColdRecord* record) {
  // We copy the mask of the "cooled" referenced object because it contains the encoding info.
  SetMeta(EXTERNAL_TAG, record->value.mask_);

  u_.ext_ptr.is_cool = 1;
  u_.ext_ptr.representation = static_cast<uint8_t>(rep);
  u_.ext_ptr.page_offset = offset % 4096;
  u_.ext_ptr.serialized_size = sz;
  u_.ext_ptr.cool_record = record;
}

auto CompactObj::GetCool() const -> CoolItem {
  DCHECK(IsExternal() && u_.ext_ptr.is_cool);

  CoolItem res;
  res.page_offset = u_.ext_ptr.page_offset;
  res.serialized_size = u_.ext_ptr.serialized_size;
  res.record = u_.ext_ptr.cool_record;
  return res;
}

void CompactObj::Freeze(size_t offset, size_t sz) {
  SetExternal(offset, sz, GetExternalRep());
}

std::pair<size_t, size_t> CompactObj::GetExternalSlice() const {
  DCHECK_EQ(EXTERNAL_TAG, taglen_);
  auto& ext = u_.ext_ptr;
  size_t offset = ext.page_offset;
  offset += size_t(ext.is_cool ? ext.cool_record->page_index : ext.offload.page_index) * 4096;
  return {offset, size_t(u_.ext_ptr.serialized_size)};
}

void CompactObj::Materialize(std::string_view blob, bool is_raw) {
  CHECK(IsExternal()) << int(taglen_);
  DCHECK_EQ(u_.ext_ptr.representation, static_cast<uint8_t>(ExternalRep::STRING));
  DCHECK_GT(blob.size(), kInlineLen);  // There are no mutable commands that shrink strings

  if (is_raw) {
    if (SmallString::CanAllocate(blob.size())) {
      SetMeta(SMALL_TAG, mask_);
      tl.small_str_bytes += u_.small_str.Assign(blob);
    } else {
      SetMeta(ROBJ_TAG, mask_);
      u_.r_obj.SetString(blob, tl.local_mr);
    }
  } else {
    mask_bits_.encoding = NONE_ENC;  // reset encoding
    EncodeString(blob, false);
  }
}

void CompactObj::Reset() {
  if (HasAllocated()) {
    Free();
  }
  taglen_ = 0;
  huffman_domain_ = 0;
  mask_ = 0;
}

uint8_t CompactObj::GetFirstByte() const {
  DCHECK_EQ(ObjType(), OBJ_STRING);

  if (IsInline()) {
    return u_.inline_str[0];
  }

  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    return *(uint8_t*)u_.r_obj.inner_obj();
  }

  if (taglen_ == SMALL_TAG) {
    return u_.small_str.first_byte();
  }

  if (taglen_ == EXTERNAL_TAG) {
    if (u_.ext_ptr.is_cool) {
      const CompactObj& cooled_obj = u_.ext_ptr.cool_record->value;
      return cooled_obj.GetFirstByte();
    }
    return u_.ext_ptr.first_byte;
  }

  LOG(DFATAL) << "Bad tag " << int(taglen_);
  return 0;
}

// Frees all resources if owns.
void CompactObj::Free() {
  DCHECK(HasAllocated());

  if (taglen_ == ROBJ_TAG) {
    u_.r_obj.Free(tl.local_mr);
  } else if (taglen_ == SMALL_TAG) {
    tl.small_str_bytes -= u_.small_str.MallocUsed();
    u_.small_str.Free();
  } else if (taglen_ == JSON_TAG) {
    DVLOG(1) << "Freeing JSON object";
    if (JsonEnconding() == kEncodingJsonCons) {
      DeleteMR<JsonType>(u_.json_obj.cons.json_ptr);
    } else {
      tl.local_mr->deallocate(u_.json_obj.flat.flat_ptr, u_.json_obj.flat.json_len, kAlignSize);
    }
  } else if (taglen_ == SBF_TAG) {
    DeleteMR<SBF>(u_.sbf);
  } else {
    LOG(FATAL) << "Unsupported tag " << int(taglen_);
  }

  memset(u_.inline_str, 0, kInlineLen);
}

size_t CompactObj::MallocUsed(bool slow) const {
  if (!HasAllocated())
    return 0;

  if (taglen_ == ROBJ_TAG) {
    return u_.r_obj.MallocUsed(slow);
  }

  if (taglen_ == JSON_TAG) {
    // TODO fix this once we fully support flat json
    // This is here because accessing a union field that is not active
    // is UB.
    if (JsonEnconding() == kEncodingJsonFlat) {
      return 0;
    }
    return u_.json_obj.cons.bytes_used;
  }

  if (taglen_ == SMALL_TAG) {
    return u_.small_str.MallocUsed();
  }

  if (taglen_ == SBF_TAG) {
    return u_.sbf->MallocUsed();
  }
  LOG(DFATAL) << "should not reach";
  return 0;
}

bool CompactObj::operator==(const CompactObj& o) const {
  DCHECK(taglen_ != JSON_TAG && o.taglen_ != JSON_TAG) << "cannot use JSON type to check equal";

  uint8_t m1 = mask_bits_.encoding;
  uint8_t m2 = o.mask_bits_.encoding;
  if (m1 != m2)
    return false;

  if (taglen_ != o.taglen_)
    return false;

  if (taglen_ == ROBJ_TAG)
    return u_.r_obj.Equal(o.u_.r_obj);

  if (taglen_ == INT_TAG)
    return u_.ival == o.u_.ival;

  if (taglen_ == SMALL_TAG)
    return u_.small_str.Equal(o.u_.small_str);

  DCHECK(IsInline() && o.IsInline());

  return memcmp(u_.inline_str, o.u_.inline_str, taglen_) == 0;
}

bool CompactObj::CmpNonInline(std::string_view sv) const {
  DCHECK_GT(taglen_, kInlineLen);
  switch (taglen_) {
    case INT_TAG:
      return absl::AlphaNum(u_.ival).Piece() == sv;
    case ROBJ_TAG:
      return u_.r_obj.Equal(sv);
    case SMALL_TAG:
      return u_.small_str.Equal(sv);
    default:
      break;
  }
  return false;
}

bool CompactObj::CmpEncoded(string_view sv) const {
  DCHECK(mask_bits_.encoding);

  if (mask_bits_.encoding == HUFFMAN_ENC) {
    size_t sz = Size();
    if (sv.size() != sz)
      return false;

    if (IsInline()) {
      // Buffer must accommodate maximum decompressed size from inline storage (~8x compression)
      constexpr size_t kMaxHuffLen = kInlineLen * 8;
      if (sz <= kMaxHuffLen) {
        char buf[kMaxHuffLen];
        const auto& decoder = tl.GetHuffmanDecoder(huffman_domain_);
        CHECK(decoder.Decode({u_.inline_str + 1, size_t(taglen_ - 1)}, sz, buf));
        return sv == string_view(buf, sz);
      }
    }
    tl.tmp_str.resize(sz);
    GetString(tl.tmp_str.data());
    return sv == tl.tmp_str;
  }

  size_t encode_len = binpacked_len(sv.size());
  if (IsInline()) {
    if (encode_len != taglen_)
      return false;

    char buf[kInlineLen * 2];
    detail::ascii_unpack(to_byte(u_.inline_str), sv.size(), buf);

    return sv == string_view(buf, sv.size());
  }

  if (taglen_ == ROBJ_TAG) {
    if (u_.r_obj.type() != OBJ_STRING)
      return false;

    if (u_.r_obj.Size() != encode_len)
      return false;

    if (!detail::validate_ascii_fast(sv.data(), sv.size()))
      return false;

    return detail::compare_packed(to_byte(u_.r_obj.inner_obj()), sv.data(), sv.size());
  }

  if (taglen_ == JSON_TAG) {
    return false;  // cannot compare json with string
  }

  if (taglen_ == SMALL_TAG) {
    if (u_.small_str.size() != encode_len)
      return false;

    if (!detail::validate_ascii_fast(sv.data(), sv.size()))
      return false;

    // We need to compare an unpacked sv with 2 packed parts.
    // To compare easily ascii with binary we would need to split ascii at 8 bytes boundaries
    // so that we could pack it into complete binary bytes (8 ascii chars produce 7 bytes).
    // I choose a minimal 16 byte prefix:
    // 1. sv must be longer than 16 if we are here (at least 18 actually).
    // 2. 16 chars produce 14 byte blob that should cover the first slice (10 bytes) and 4 bytes
    //    of the second slice.
    // 3. I assume that the first slice is less than 14 bytes which is correct since small string
    //    has only 9-10 bytes in its inline prefix storage.
    DCHECK_GT(sv.size(), 16u);  // we would not be in SMALL_TAG, otherwise.

    string_view slice[2];
    u_.small_str.Get(slice);
    DCHECK_LT(slice[0].size(), 14u);

    uint8_t tmpbuf[14];
    detail::ascii_pack(sv.data(), 16, tmpbuf);

    // Compare the first slice.
    if (memcmp(slice[0].data(), tmpbuf, slice[0].size()) != 0)
      return false;

    // Compare the prefix of the second slice.
    size_t pref_len = 14 - slice[0].size();

    if (memcmp(slice[1].data(), tmpbuf + slice[0].size(), pref_len) != 0)
      return false;

    // We verified that the first 16 chars (or 14 bytes) are equal.
    // Lets verify the rest - suffix of the second slice and the suffix of sv.
    return detail::compare_packed(to_byte(slice[1].data() + pref_len), sv.data() + 16,
                                  sv.size() - 16);
  }
  LOG(FATAL) << "Unsupported tag " << int(taglen_);
  return false;
}

void CompactObj::EncodeString(string_view str, bool is_key) {
  DCHECK_GT(str.size(), kInlineLen);
  DCHECK_EQ(NONE_ENC, mask_bits_.encoding);

  string_view encoded = str;
  bool huff_encoded = false;

  // We chose such length that we can store the decoded length delta into 1 byte.
  // The maximum huffman compression is 1/8, so 288 / 8 = 36.
  // 288 - 36 = 252, which is smaller than 256.
  // TODO: introduce variable length huffman length.
  constexpr unsigned kMaxHuffLen = 288;

  // For sizes 17, 18 we would like to test ascii encoding first as it's more efficient.
  // And if it succeeds we can squash into the inline buffer.
  bool is_ascii =
      kUseAsciiEncoding && str.size() < 19 && detail::validate_ascii_fast(str.data(), str.size());

  // if !is_ascii, we try huffman encoding next.
  if (!is_ascii && str.size() <= kMaxHuffLen) {
    auto& huffman = is_key ? tl.huff_keys : tl.huff_string_values;
    if (huffman.encoder.valid()) {
      unsigned dest_len = huffman.encoder.CompressedBound(str.size());
      // 1 byte for storing the size delta.
      tl.tmp_buf.resize(1 + dest_len);
      string err_msg;
      ++tl.huff_encode_total;
      bool res = huffman.encoder.Encode(str, tl.tmp_buf.data() + 1, &dest_len, &err_msg);
      if (res) {
        // we accept huffman encoding only if it is:
        // 1. smaller than the original string by 20%
        // 2. allows us to store the encoded string in the inline buffer
        if (dest_len && (dest_len < kInlineLen || (dest_len + dest_len / 5) < str.size())) {
          huff_encoded = true;
          tl.huff_encode_success++;
          encoded = string_view{reinterpret_cast<char*>(tl.tmp_buf.data()), dest_len + 1};
          unsigned delta = str.size() - dest_len;
          DCHECK_LT(delta, 256u);
          tl.tmp_buf[0] = static_cast<uint8_t>(delta);
          mask_bits_.encoding = HUFFMAN_ENC;
          huffman_domain_ = is_key ? HUFF_KEYS : HUFF_STRING_VALUES;
          if (encoded.size() <= kInlineLen) {
            SetMeta(encoded.size(), mask_);
            memcpy(u_.inline_str, tl.tmp_buf.data(), encoded.size());
            return;
          }
        }
      } else {
        // Should not happen, means we have an internal buf.
        LOG(DFATAL) << "Failed to encode string with huffman: " << err_msg;
      }
    }
  }

  // Finally we try ascii encoding for longer strings if we have not encoded them with huffman.
  if (kUseAsciiEncoding && !is_ascii && str.size() >= 19 && !huff_encoded) {
    is_ascii = detail::validate_ascii_fast(str.data(), str.size());
  }

  if (is_ascii) {
    size_t encode_len = binpacked_len(str.size());
    size_t rev_len = ascii_len(encode_len);

    if (rev_len == str.size()) {
      mask_bits_.encoding = ASCII2_ENC;  // str hits its highest bound.
    } else {
      CHECK_EQ(str.size(), rev_len - 1) << "Bad ascii encoding for len " << str.size();
      mask_bits_.encoding = ASCII1_ENC;  // str is shorter than its highest bound.
    }

    tl.tmp_buf.resize(encode_len);
    detail::ascii_pack_simd2(str.data(), str.size(), tl.tmp_buf.data());
    encoded = string_view{reinterpret_cast<char*>(tl.tmp_buf.data()), encode_len};

    if (encoded.size() <= kInlineLen) {
      SetMeta(encoded.size(), mask_);
      detail::ascii_pack(str.data(), str.size(), reinterpret_cast<uint8_t*>(u_.inline_str));

      return;
    }
  }

  DCHECK_GT(encoded.size(), kInlineLen);

  if (SmallString::CanAllocate(encoded.size())) {
    if (taglen_ == SMALL_TAG)
      tl.small_str_bytes -= u_.small_str.MallocUsed();
    else
      SetMeta(SMALL_TAG, mask_);

    tl.small_str_bytes += u_.small_str.Assign(encoded);
    return;
  }

  SetMeta(ROBJ_TAG, mask_);
  u_.r_obj.SetString(encoded, tl.local_mr);
}

StringOrView CompactObj::GetRawString() const {
  DCHECK(!IsExternal());

  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    return StringOrView::FromView(u_.r_obj.AsView());
  }

  if (taglen_ == SMALL_TAG) {
    string tmp;
    u_.small_str.Get(&tmp);
    return StringOrView::FromString(std::move(tmp));
  }

  LOG(FATAL) << "Unsupported tag for GetRawString(): " << int(taglen_);
  return {};
}

MemoryResource* CompactObj::memory_resource() {
  return tl.local_mr;
}

bool CompactObj::JsonConsT::DefragIfNeeded(PageUsage* page_usage) {
  if (JsonType* old = json_ptr; ShouldDefragment(page_usage)) {
    json_ptr = AllocateMR<JsonType>(DeepCopyJSON(old));
    DeleteMR<JsonType>(old);
    return true;
  }
  return false;
}

bool CompactObj::JsonConsT::ShouldDefragment(PageUsage* page_usage) const {
  bool should_defragment = false;
  json_ptr->compute_memory_size([&page_usage, &should_defragment](const void* p) {
    should_defragment |= page_usage->IsPageForObjectUnderUtilized(const_cast<void*>(p));
    return 0;
  });
  return should_defragment;
}

bool CompactObj::FlatJsonT::DefragIfNeeded(PageUsage* page_usage) {
  if (uint8_t* old = flat_ptr; page_usage->IsPageForObjectUnderUtilized(old)) {
    const uint32_t size = json_len;
    flat_ptr = static_cast<uint8_t*>(tl.local_mr->allocate(size, kAlignSize));
    memcpy(flat_ptr, old, size);
    tl.local_mr->deallocate(old, size, kAlignSize);
    return true;
  }

  return false;
}

bool CompactObj::JsonWrapper::DefragIfNeeded(PageUsage* page_usage) {
  if (JsonEnconding() == kEncodingJsonCons) {
    return cons.DefragIfNeeded(page_usage);
  }

  return flat.DefragIfNeeded(page_usage);
}

constexpr std::pair<CompactObjType, std::string_view> kObjTypeToString[] =
    {
        {OBJ_STRING, "string"sv},  {OBJ_LIST, "list"sv},    {OBJ_SET, "set"sv},
        {OBJ_ZSET, "zset"sv},      {OBJ_HASH, "hash"sv},    {OBJ_STREAM, "stream"sv},
        {OBJ_KEY, "key"sv},  // pseudo-type used for memory tracking
        {OBJ_JSON, "ReJSON-RL"sv}, {OBJ_SBF, "MBbloom--"sv}};

std::string_view ObjTypeToString(CompactObjType type) {
  for (auto& p : kObjTypeToString) {
    if (type == p.first) {
      return p.second;
    }
  }

  LOG(DFATAL) << "Unsupported type " << type;
  return "Invalid type"sv;
}

CompactObjType ObjTypeFromString(std::string_view sv) {
  for (auto& p : kObjTypeToString) {
    if (absl::EqualsIgnoreCase(sv, p.second)) {
      return p.first;
    }
  }
  return kInvalidCompactObjType;
}

size_t CompactObj::StrEncoding::DecodedSize(string_view blob) const {
  return DecodedSize(blob.size(), blob[0]);
}

size_t CompactObj::StrEncoding::DecodedSize(size_t blob_size, uint8_t first_byte) const {
  switch (enc_) {
    case NONE_ENC:
      return blob_size;
    case ASCII1_ENC:
    case ASCII2_ENC:
      return ascii_len(blob_size) - (enc_ == ASCII1_ENC);
    case HUFFMAN_ENC:
      return blob_size + int(first_byte) - 1;
  };
  return 0;
}

size_t CompactObj::StrEncoding::Decode(std::string_view blob, char* dest) const {
  if (blob.empty())
    return 0;
  size_t decoded_len = DecodedSize(blob);
  switch (enc_) {
    case NONE_ENC:
      memcpy(dest, blob.data(), blob.size());
      break;
    case ASCII1_ENC:
    case ASCII2_ENC:
      detail::ascii_unpack(reinterpret_cast<const uint8_t*>(blob.data()), decoded_len, dest);
      break;
    case HUFFMAN_ENC: {
      const auto& decoder = tl.GetHuffmanDecoder(is_key_);
      decoder.Decode(blob.substr(1), decoded_len, dest);
      break;
    }
  };
  return decoded_len;
}

StringOrView CompactObj::StrEncoding::Decode(std::string_view blob) const {
  switch (enc_) {
    case NONE_ENC:
      return StringOrView::FromView(blob);
    default: {
      string out;
      out.resize(DecodedSize(blob));
      Decode(blob, out.data());
      return StringOrView::FromString(std::move(out));
    }
  }
  return {};
}

}  // namespace dfly
