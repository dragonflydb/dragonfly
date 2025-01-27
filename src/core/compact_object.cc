// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/compact_object.h"

// #define XXH_INLINE_ALL
#include <xxhash.h>

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/quicklist.h"
#include "redis/redis_aux.h"
#include "redis/stream.h"
#include "redis/util.h"
#include "redis/zmalloc.h"  // for non-string objects.
#include "redis/zset.h"
}
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include "base/flags.h"
#include "base/logging.h"
#include "base/pod_array.h"
#include "core/bloom.h"
#include "core/detail/bitpacking.h"
#include "core/qlist.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"

ABSL_FLAG(bool, experimental_flat_json, false, "If true uses flat json implementation.");

namespace dfly {
using namespace std;
using absl::GetFlag;
using detail::binpacked_len;
using MemoryResource = detail::RobjWrapper::MemoryResource;

namespace {

constexpr XXH64_hash_t kHashSeed = 24061983;
constexpr size_t kAlignSize = 8u;

// Approximation since does not account for listpacks.
size_t QlMAllocSize(quicklist* ql, bool slow) {
  size_t node_size = ql->len * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  if (slow) {
    for (quicklistNode* node = ql->head; node; node = node->next) {
      node_size += zmalloc_usable_size(node->entry);
    }
    return node_size;
  }
  return node_size + ql->count * 16;  // we account for each member 16 bytes.
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
  switch (encoding) {
    case OBJ_ENCODING_QUICKLIST:
      quicklistRelease((quicklist*)ptr);
      break;
    case kEncodingQL2:
      CompactObj::DeleteMR<QList>(ptr);
      break;
    default:
      LOG(FATAL) << "Unknown list encoding type";
  }
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

pair<void*, bool> DefragStrMap2(StringMap* sm, float ratio) {
  bool realloced = false;

  for (auto it = sm->begin(); it != sm->end(); ++it)
    realloced |= it.ReallocIfNeeded(ratio);

  return {sm, realloced};
}

pair<void*, bool> DefragListPack(uint8_t* lp, float ratio) {
  if (!zmalloc_page_is_underutilized(lp, ratio))
    return {lp, false};

  size_t lp_bytes = lpBytes(lp);
  uint8_t* replacement = lpNew(lpBytes(lp));
  memcpy(replacement, lp, lp_bytes);
  lpFree(lp);

  return {replacement, true};
}

pair<void*, bool> DefragIntSet(intset* is, float ratio) {
  if (!zmalloc_page_is_underutilized(is, ratio))
    return {is, false};

  const size_t blob_len = intsetBlobLen(is);
  intset* replacement = (intset*)zmalloc(blob_len);
  memcpy(replacement, is, blob_len);

  zfree(is);
  return {replacement, true};
}

pair<void*, bool> DefragSortedMap(detail::SortedMap* sm, float ratio) {
  const bool reallocated = sm->DefragIfNeeded(ratio);
  return {sm, reallocated};
}

pair<void*, bool> DefragStrSet(StringSet* ss, float ratio) {
  bool realloced = false;

  for (auto it = ss->begin(); it != ss->end(); ++it)
    realloced |= it.ReallocIfNeeded(ratio);

  return {ss, realloced};
}

// Iterates over allocations of internal hash data structures and re-allocates
// them if their pages are underutilized.
// Returns pointer to new object ptr and whether any re-allocations happened.
pair<void*, bool> DefragHash(unsigned encoding, void* ptr, float ratio) {
  switch (encoding) {
    // Listpack is stored as a single contiguous array
    case kEncodingListPack: {
      return DefragListPack((uint8_t*)ptr, ratio);
    }

    // StringMap supports re-allocation of it's internal nodes
    case kEncodingStrMap2: {
      return DefragStrMap2((StringMap*)ptr, ratio);
    }

    default:
      ABSL_UNREACHABLE();
  }
}

pair<void*, bool> DefragSet(unsigned encoding, void* ptr, float ratio) {
  switch (encoding) {
    // Int sets have flat storage
    case kEncodingIntSet: {
      return DefragIntSet((intset*)ptr, ratio);
    }

    case kEncodingStrMap2: {
      return DefragStrSet((StringSet*)ptr, ratio);
    }

    default:
      ABSL_UNREACHABLE();
  }
}

pair<void*, bool> DefragZSet(unsigned encoding, void* ptr, float ratio) {
  switch (encoding) {
    // Listpack is stored as a single contiguous array
    case OBJ_ENCODING_LISTPACK: {
      return DefragListPack((uint8_t*)ptr, ratio);
    }

    // SKIPLIST really means ScoreMap
    case OBJ_ENCODING_SKIPLIST: {
      return DefragSortedMap((detail::SortedMap*)ptr, ratio);
    }

    default:
      ABSL_UNREACHABLE();
  }
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

struct TL {
  MemoryResource* local_mr = PMR_NS::get_default_resource();
  size_t small_str_bytes;
  base::PODArray<uint8_t> tmp_buf;
  string tmp_str;
};

thread_local TL tl;

constexpr bool kUseSmallStrings = true;

/// TODO: Ascii encoding becomes slow for large blobs. We should factor it out into a separate
/// file and implement with SIMD instructions.
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
      if (encoding_ == OBJ_ENCODING_QUICKLIST)
        return QlMAllocSize((quicklist*)inner_obj_, slow);
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
      if (encoding_ == OBJ_ENCODING_QUICKLIST)
        return quicklistCount((quicklist*)inner_obj_);
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

void RobjWrapper::SetSize(uint64_t size) {
  sz_ = size;
}

bool RobjWrapper::DefragIfNeeded(float ratio) {
  auto do_defrag = [this, ratio](auto defrag_fun) mutable {
    auto [new_ptr, realloced] = defrag_fun(encoding_, inner_obj_, ratio);
    inner_obj_ = new_ptr;
    return realloced;
  };

  if (type() == OBJ_STRING) {
    if (zmalloc_page_is_underutilized(inner_obj(), ratio)) {
      ReallocateString(tl.local_mr);
      return true;
    }
  } else if (type() == OBJ_HASH) {
    return do_defrag(DefragHash);
  } else if (type() == OBJ_SET) {
    return do_defrag(DefragSet);
  } else if (type() == OBJ_ZSET) {
    return do_defrag(DefragZSet);
  }
  return false;
}

int RobjWrapper::ZsetAdd(double score, sds ele, int in_flags, int* out_flags, double* newscore) {
  // copied from zsetAdd for listpack only.
  /* Turn options into simple to check vars. */
  bool incr = (in_flags & ZADD_IN_INCR) != 0;
  bool nx = (in_flags & ZADD_IN_NX) != 0;
  bool xx = (in_flags & ZADD_IN_XX) != 0;
  bool gt = (in_flags & ZADD_IN_GT) != 0;
  bool lt = (in_flags & ZADD_IN_LT) != 0;
  *out_flags = 0; /* We'll return our response flags. */
  double curscore;

  /* NaN as input is an error regardless of all the other parameters. */
  if (isnan(score)) {
    *out_flags = ZADD_OUT_NAN;
    return 0;
  }

  /* Update the sorted set according to its encoding. */
  if (encoding_ == OBJ_ENCODING_LISTPACK) {
    unsigned char* eptr;
    uint8_t* lp = (uint8_t*)inner_obj_;

    if ((eptr = zzlFind(lp, ele, &curscore)) != NULL) {
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
          sdslen(ele) > server.zset_max_listpack_value) {
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
  return ss->Add(score, ele, in_flags, out_flags, newscore);
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
  static thread_local uint32_t json_enc =
      absl::GetFlag(FLAGS_experimental_flat_json) ? kEncodingJsonFlat : kEncodingJsonCons;
  return json_enc;
}

using namespace std;

auto CompactObj::GetStats() -> Stats {
  Stats res;
  res.small_string_bytes = tl.small_str_bytes;

  return res;
}

void CompactObj::InitThreadLocal(MemoryResource* mr) {
  tl.local_mr = mr;
  tl.tmp_buf = base::PODArray<uint8_t>{mr};
}

CompactObj::~CompactObj() {
  if (HasAllocated()) {
    Free();
  }
}

CompactObj& CompactObj::operator=(CompactObj&& o) noexcept {
  DCHECK(&o != this);

  SetMeta(o.taglen_, o.mask_);  // Frees underlying resources if needed.
  memcpy(&u_, &o.u_, sizeof(u_));

  // SetMeta deallocates the object and we only want reset it.
  o.taglen_ = 0;
  o.mask_ = 0;

  return *this;
}

size_t CompactObj::Size() const {
  size_t raw_size = 0;

  if (IsInline()) {
    raw_size = taglen_;
  } else {
    switch (taglen_) {
      case SMALL_TAG:
        raw_size = u_.small_str.size();
        break;
      case INT_TAG: {
        absl::AlphaNum an(u_.ival);
        raw_size = an.size();
        break;
      }
      case EXTERNAL_TAG:
        raw_size = u_.ext_ptr.serialized_size;
        break;
      case ROBJ_TAG:
        raw_size = u_.r_obj.Size();
        break;
      case JSON_TAG:
        if (JsonEnconding() == kEncodingJsonFlat) {
          raw_size = u_.json_obj.flat.json_len;
        } else {
          raw_size = u_.json_obj.cons.json_ptr->size();
        }
        break;
      case SBF_TAG:
        raw_size = u_.sbf->current_size();
        break;
      default:
        LOG(DFATAL) << "Should not reach " << int(taglen_);
    }
  }
  uint8_t encoded = (mask_ & kEncMask);
  return encoded ? DecodedLen(raw_size) : raw_size;
}

uint64_t CompactObj::HashCode() const {
  DCHECK(taglen_ != JSON_TAG) << "JSON type cannot be used for keys!";

  uint8_t encoded = (mask_ & kEncMask);
  if (IsInline()) {
    if (encoded) {
      char buf[kInlineLen * 2];
      size_t decoded_len = DecodedLen(taglen_);
      detail::ascii_unpack(to_byte(u_.inline_str), decoded_len, buf);
      return XXH3_64bits_withSeed(buf, decoded_len, kHashSeed);
    }
    return XXH3_64bits_withSeed(u_.inline_str, taglen_, kHashSeed);
  }

  if (encoded) {
    string_view sv = GetSlice(&tl.tmp_str);
    return XXH3_64bits_withSeed(sv.data(), sv.size(), kHashSeed);
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
  // We need hash only for keys.
  LOG(DFATAL) << "Should not reach " << int(taglen_);

  return 0;
}

uint64_t CompactObj::HashCode(string_view str) {
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}

CompactObjType CompactObj::ObjType() const {
  if (IsInline() || taglen_ == INT_TAG || taglen_ == SMALL_TAG || taglen_ == EXTERNAL_TAG)
    return OBJ_STRING;

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
  if (INT_TAG != taglen_) {
    SetMeta(INT_TAG, mask_ & ~kEncMask);
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
    if (size < 0) {
      DCHECK(static_cast<int64_t>(u_.json_obj.cons.bytes_used) >= size);
    }
    u_.json_obj.cons.bytes_used += size;
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

void CompactObj::SetString(std::string_view str) {
  uint8_t mask = mask_ & ~kEncMask;
  CHECK(!IsExternal());
  // Trying auto-detection heuristics first.
  if (str.size() <= 20) {
    long long ival;
    static_assert(sizeof(long long) == 8);

    // We use redis string2ll to be compatible with Redis.
    if (string2ll(str.data(), str.size(), &ival)) {
      SetMeta(INT_TAG, mask);
      u_.ival = ival;

      return;
    }

    if (str.size() <= kInlineLen) {
      SetMeta(str.size(), mask);
      if (!str.empty())
        memcpy(u_.inline_str, str.data(), str.size());
      return;
    }
  }

  EncodeString(str);
}

string_view CompactObj::GetSlice(string* scratch) const {
  CHECK(!IsExternal());
  uint8_t is_encoded = mask_ & kEncMask;

  if (IsInline()) {
    if (is_encoded) {
      size_t decoded_len = taglen_ + 2;

      // must be this because we either shortened 17 or 18.
      DCHECK_EQ(is_encoded, ASCII2_ENC_BIT);
      DCHECK_EQ(decoded_len, ascii_len(taglen_));

      scratch->resize(decoded_len);
      detail::ascii_unpack(to_byte(u_.inline_str), decoded_len, scratch->data());
      return *scratch;
    }

    return string_view{u_.inline_str, taglen_};
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    scratch->assign(an.Piece());

    return *scratch;
  }

  if (is_encoded) {
    if (taglen_ == ROBJ_TAG) {
      CHECK_EQ(OBJ_STRING, u_.r_obj.type());
      DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
      size_t decoded_len = DecodedLen(u_.r_obj.Size());
      scratch->resize(decoded_len);
      detail::ascii_unpack_simd(to_byte(u_.r_obj.inner_obj()), decoded_len, scratch->data());
    } else if (taglen_ == SMALL_TAG) {
      size_t decoded_len = DecodedLen(u_.small_str.size());
      scratch->resize(decoded_len);
      string_view slices[2];

      unsigned num = u_.small_str.GetV(slices);
      DCHECK_EQ(2u, num);
      std::string tmp(decoded_len, ' ');
      char* next = tmp.data();
      memcpy(next, slices[0].data(), slices[0].size());
      next += slices[0].size();
      memcpy(next, slices[1].data(), slices[1].size());
      detail::ascii_unpack_simd(reinterpret_cast<uint8_t*>(tmp.data()), decoded_len,
                                scratch->data());
    } else {
      LOG(FATAL) << "Unsupported tag " << int(taglen_);
    }
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

bool CompactObj::DefragIfNeeded(float ratio) {
  switch (taglen_) {
    case ROBJ_TAG:
      // currently only these objet types are supported for this operation
      if (u_.r_obj.inner_obj() != nullptr) {
        return u_.r_obj.DefragIfNeeded(ratio);
      }
      return false;
    case SMALL_TAG:
      return u_.small_str.DefragIfNeeded(ratio);
    case INT_TAG:
      // this is not relevant in this case
      return false;
    case EXTERNAL_TAG:
      return false;
    default:
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
  uint8_t is_encoded = mask_ & kEncMask;

  if (IsInline()) {
    if (is_encoded) {
      size_t decoded_len = taglen_ + 2;

      // must be this because we either shortened 17 or 18.
      DCHECK_EQ(is_encoded, ASCII2_ENC_BIT);
      DCHECK_EQ(decoded_len, ascii_len(taglen_));

      detail::ascii_unpack(to_byte(u_.inline_str), decoded_len, dest);
    } else {
      memcpy(dest, u_.inline_str, taglen_);
    }

    return;
  }

  if (taglen_ == INT_TAG) {
    absl::AlphaNum an(u_.ival);
    memcpy(dest, an.data(), an.size());
    return;
  }

  if (is_encoded) {
    if (taglen_ == ROBJ_TAG) {
      CHECK_EQ(OBJ_STRING, u_.r_obj.type());
      DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
      size_t decoded_len = DecodedLen(u_.r_obj.Size());
      detail::ascii_unpack_simd(to_byte(u_.r_obj.inner_obj()), decoded_len, dest);
    } else if (taglen_ == SMALL_TAG) {
      size_t decoded_len = DecodedLen(u_.small_str.size());

      string_view slices[2];
      unsigned num = u_.small_str.GetV(slices);
      DCHECK_EQ(2u, num);
      std::string tmp(decoded_len, ' ');
      char* next = tmp.data();
      memcpy(next, slices[0].data(), slices[0].size());
      next += slices[0].size();
      memcpy(next, slices[1].data(), slices[1].size());
      detail::ascii_unpack_simd(reinterpret_cast<uint8_t*>(tmp.data()), decoded_len, dest);
    } else {
      LOG(FATAL) << "Unsupported tag " << int(taglen_);
    }
    return;
  }

  // no encoding.
  if (taglen_ == ROBJ_TAG) {
    CHECK_EQ(OBJ_STRING, u_.r_obj.type());
    DCHECK_EQ(OBJ_ENCODING_RAW, u_.r_obj.encoding());
    memcpy(dest, u_.r_obj.inner_obj(), u_.r_obj.Size());
    return;
  }

  if (taglen_ == SMALL_TAG) {
    string_view slices[2];
    unsigned num = u_.small_str.GetV(slices);
    DCHECK_EQ(2u, num);
    memcpy(dest, slices[0].data(), slices[0].size());
    dest += slices[0].size();
    memcpy(dest, slices[1].data(), slices[1].size());
    return;
  }

  LOG(FATAL) << "Bad tag " << int(taglen_);
}

void CompactObj::SetExternal(size_t offset, uint32_t sz) {
  SetMeta(EXTERNAL_TAG, mask_);

  u_.ext_ptr.is_cool = 0;
  u_.ext_ptr.page_offset = offset % 4096;
  u_.ext_ptr.serialized_size = sz;
  u_.ext_ptr.offload.page_index = offset / 4096;
}

void CompactObj::SetCool(size_t offset, uint32_t sz, detail::TieredColdRecord* record) {
  // We copy the mask of the "cooled" referenced object because it contains the encoding info.
  SetMeta(EXTERNAL_TAG, record->value.mask_);

  u_.ext_ptr.is_cool = 1;
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

void CompactObj::ImportExternal(const CompactObj& src) {
  DCHECK(src.IsExternal());
  SetMeta(EXTERNAL_TAG, src.mask_ & kEncMask);
  u_.ext_ptr = src.u_.ext_ptr;
}

std::pair<size_t, size_t> CompactObj::GetExternalSlice() const {
  DCHECK_EQ(EXTERNAL_TAG, taglen_);
  auto& ext = u_.ext_ptr;
  size_t offset = ext.page_offset;
  offset += size_t(ext.is_cool ? ext.cool_record->page_index : ext.offload.page_index) * 4096;
  return pair<size_t, size_t>(offset, size_t(u_.ext_ptr.serialized_size));
}

void CompactObj::Materialize(std::string_view blob, bool is_raw) {
  CHECK(IsExternal()) << int(taglen_);

  DCHECK_GT(blob.size(), kInlineLen);

  if (is_raw) {
    uint8_t mask = mask_;

    if (kUseSmallStrings && SmallString::CanAllocate(blob.size())) {
      SetMeta(SMALL_TAG, mask);
      tl.small_str_bytes += u_.small_str.Assign(blob);
    } else {
      SetMeta(ROBJ_TAG, mask);
      u_.r_obj.SetString(blob, tl.local_mr);
    }
  } else {
    EncodeString(blob);
  }
}

void CompactObj::Reset() {
  if (HasAllocated()) {
    Free();
  }
  taglen_ = 0;
  mask_ = 0;
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

  uint8_t m1 = mask_ & kEncMask;
  uint8_t m2 = o.mask_ & kEncMask;
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

bool CompactObj::EqualNonInline(std::string_view sv) const {
  switch (taglen_) {
    case INT_TAG: {
      absl::AlphaNum an(u_.ival);
      return sv == an.Piece();
    }

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
    unsigned num = u_.small_str.GetV(slice);
    DCHECK_EQ(2u, num);
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

void CompactObj::EncodeString(string_view str) {
  DCHECK_GT(str.size(), kInlineLen);

  uint8_t mask = mask_ & ~kEncMask;
  string_view encoded = str;
  bool is_ascii = kUseAsciiEncoding && detail::validate_ascii_fast(str.data(), str.size());

  if (is_ascii) {
    size_t encode_len = binpacked_len(str.size());
    size_t rev_len = ascii_len(encode_len);

    if (rev_len == str.size()) {
      mask |= ASCII2_ENC_BIT;  // str hits its highest bound.
    } else {
      CHECK_EQ(str.size(), rev_len - 1) << "Bad ascii encoding for len " << str.size();

      mask |= ASCII1_ENC_BIT;
    }

    tl.tmp_buf.resize(encode_len);
    detail::ascii_pack_simd2(str.data(), str.size(), tl.tmp_buf.data());
    encoded = string_view{reinterpret_cast<char*>(tl.tmp_buf.data()), encode_len};

    if (encoded.size() <= kInlineLen) {
      SetMeta(encoded.size(), mask);
      detail::ascii_pack(str.data(), str.size(), reinterpret_cast<uint8_t*>(u_.inline_str));

      return;
    }
  }

  if (kUseSmallStrings && SmallString::CanAllocate(encoded.size())) {
    if (taglen_ == 0) {
      SetMeta(SMALL_TAG, mask);
      tl.small_str_bytes += u_.small_str.Assign(encoded);
      return;
    }

    if (taglen_ == SMALL_TAG && encoded.size() <= u_.small_str.size()) {
      mask_ = mask;
      tl.small_str_bytes -= u_.small_str.MallocUsed();
      tl.small_str_bytes += u_.small_str.Assign(encoded);
      return;
    }
  }

  SetMeta(ROBJ_TAG, mask);
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

  LOG(FATAL) << "Unsupported tag for GetRawString(): " << taglen_;
  return {};
}

size_t CompactObj::DecodedLen(size_t sz) const {
  return ascii_len(sz) - ((mask_ & ASCII1_ENC_BIT) ? 1 : 0);
}

MemoryResource* CompactObj::memory_resource() {
  return tl.local_mr;
}

constexpr std::pair<CompactObjType, std::string_view> kObjTypeToString[8] = {
    {OBJ_STRING, "string"sv},  {OBJ_LIST, "list"sv},    {OBJ_SET, "set"sv},
    {OBJ_ZSET, "zset"sv},      {OBJ_HASH, "hash"sv},    {OBJ_STREAM, "stream"sv},
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

std::optional<CompactObjType> ObjTypeFromString(std::string_view sv) {
  for (auto& p : kObjTypeToString) {
    if (absl::EqualsIgnoreCase(sv, p.second)) {
      return p.first;
    }
  }
  return std::nullopt;
}

}  // namespace dfly
