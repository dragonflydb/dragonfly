// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load.h"

#include "absl/strings/escaping.h"

extern "C" {

#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/lzfP.h" /* LZF compression library */
#include "redis/rdb.h"
#include "redis/stream.h"
#include "redis/util.h"
#include "redis/ziplist.h"
#include "redis/zmalloc.h"
#include "redis/zset.h"
}
#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <lz4frame.h>
#include <zstd.h>

#include <cstring>
#include <jsoncons/json.hpp>

#include "base/endian.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/json_object.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/hset_family.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_extensions.h"
#include "server/script_mgr.h"
#include "server/serializer_commons.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "strings/human_readable.h"

ABSL_DECLARE_FLAG(int32_t, list_max_listpack_size);
ABSL_DECLARE_FLAG(int32_t, list_compress_depth);
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(bool, use_set2);

namespace dfly {

using namespace std;
using base::IoBuf;
using nonstd::make_unexpected;
using namespace util;
using absl::GetFlag;
using rdb::errc;

namespace {

constexpr size_t kYieldPeriod = 50000;
constexpr size_t kMaxBlobLen = 1ULL << 16;
constexpr char kErrCat[] = "dragonfly.rdbload";

inline void YieldIfNeeded(size_t i) {
  if (i % kYieldPeriod == 0) {
    ThisFiber::Yield();
  }
}

class error_category : public std::error_category {
 public:
  const char* name() const noexcept final {
    return kErrCat;
  }

  string message(int ev) const final;

  error_condition default_error_condition(int ev) const noexcept final;

  bool equivalent(int ev, const error_condition& condition) const noexcept final {
    return condition.value() == ev && &condition.category() == this;
  }

  bool equivalent(const error_code& error, int ev) const noexcept final {
    return error.value() == ev && &error.category() == this;
  }
};

string error_category::message(int ev) const {
  switch (ev) {
    case errc::wrong_signature:
      return "Wrong signature while trying to load from rdb file";
    default:
      return absl::StrCat("Internal error when loading RDB file ", ev);
      break;
  }
}

error_condition error_category::default_error_condition(int ev) const noexcept {
  return error_condition{ev, *this};
}

error_category rdb_category;

inline error_code RdbError(errc ev) {
  return error_code{ev, rdb_category};
}

inline auto Unexpected(errc ev) {
  return make_unexpected(RdbError(ev));
}

static const error_code kOk;

struct ZiplistCbArgs {
  long count;
  dict* fields;
  unsigned char** lp;
};

/* callback for hashZiplistConvertAndValidateIntegrity.
 * Check that the ziplist doesn't have duplicate hash field names.
 * The ziplist element pointed by 'p' will be converted and stored into listpack. */
int ziplistPairsEntryConvertAndValidate(unsigned char* p, unsigned int head_count, void* userdata) {
  unsigned char* str;
  unsigned int slen;
  long long vll;

  ZiplistCbArgs* data = (ZiplistCbArgs*)userdata;

  if (data->fields == NULL) {
    data->fields = dictCreate(&hashDictType);
    dictExpand(data->fields, head_count / 2);
  }

  if (!ziplistGet(p, &str, &slen, &vll))
    return 0;

  /* Even records are field names, add to dict and check that's not a dup */
  if (((data->count) & 1) == 0) {
    sds field = str ? sdsnewlen(str, slen) : sdsfromlonglong(vll);
    if (dictAdd(data->fields, field, NULL) != DICT_OK) {
      /* Duplicate, return an error */
      sdsfree(field);
      return 0;
    }
  }

  if (str) {
    *(data->lp) = lpAppend(*(data->lp), (unsigned char*)str, slen);
  } else {
    *(data->lp) = lpAppendInteger(*(data->lp), vll);
  }

  (data->count)++;
  return 1;
}

/* callback for ziplistValidateIntegrity.
 * The ziplist element pointed by 'p' will be converted and stored into listpack. */
int ziplistEntryConvertAndValidate(unsigned char* p, unsigned int head_count, void* userdata) {
  unsigned char* str;
  unsigned int slen;
  long long vll;
  unsigned char** lp = (unsigned char**)userdata;

  if (!ziplistGet(p, &str, &slen, &vll))
    return 0;

  if (str)
    *lp = lpAppend(*lp, (unsigned char*)str, slen);
  else
    *lp = lpAppendInteger(*lp, vll);

  return 1;
}

/* Validate the integrity of the data structure while converting it to
 * listpack and storing it at 'lp'.
 * The function is safe to call on non-validated ziplists, it returns 0
 * when encounter an integrity validation issue. */
int ziplistPairsConvertAndValidateIntegrity(const uint8_t* zl, size_t size, unsigned char** lp) {
  /* Keep track of the field names to locate duplicate ones */
  ZiplistCbArgs data = {0, NULL, lp};

  int ret = ziplistValidateIntegrity(const_cast<uint8_t*>(zl), size, 1,
                                     ziplistPairsEntryConvertAndValidate, &data);

  /* make sure we have an even number of records. */
  if (data.count & 1)
    ret = 0;

  if (data.fields)
    dictRelease(data.fields);
  return ret;
}

bool resizeStringSet(robj* set, size_t size, bool use_set2) {
  if (use_set2) {
    ((dfly::StringSet*)set->ptr)->Reserve(size);
    return true;
  } else {
    return dictTryExpand((dict*)set->ptr, size) != DICT_OK;
  }
}

}  // namespace

class DecompressImpl {
 public:
  DecompressImpl() : uncompressed_mem_buf_{16_KB} {
  }
  virtual ~DecompressImpl() {
  }
  virtual io::Result<base::IoBuf*> Decompress(std::string_view str) = 0;

 protected:
  base::IoBuf uncompressed_mem_buf_;
};

class ZstdDecompress : public DecompressImpl {
 public:
  ZstdDecompress() {
    dctx_ = ZSTD_createDCtx();
  }
  ~ZstdDecompress() {
    ZSTD_freeDCtx(dctx_);
  }

  io::Result<base::IoBuf*> Decompress(std::string_view str);

 private:
  ZSTD_DCtx* dctx_;
};

io::Result<base::IoBuf*> ZstdDecompress::Decompress(std::string_view str) {
  // Prepare membuf memory to uncompressed string.
  auto uncomp_size = ZSTD_getFrameContentSize(str.data(), str.size());
  if (uncomp_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    LOG(ERROR) << "Zstd compression missing frame content size";
    return Unexpected(errc::invalid_encoding);
  }
  if (uncomp_size == ZSTD_CONTENTSIZE_ERROR) {
    LOG(ERROR) << "Invalid ZSTD compressed string";
    return Unexpected(errc::invalid_encoding);
  }

  uncompressed_mem_buf_.Reserve(uncomp_size + 1);

  // Uncompress string to membuf
  IoBuf::Bytes dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < uncomp_size) {
    return Unexpected(errc::out_of_memory);
  }
  size_t const d_size =
      ZSTD_decompressDCtx(dctx_, dest.data(), dest.size(), str.data(), str.size());
  if (d_size == 0 || d_size != uncomp_size) {
    LOG(ERROR) << "Invalid ZSTD compressed string";
    return Unexpected(errc::rdb_file_corrupted);
  }
  uncompressed_mem_buf_.CommitWrite(d_size);

  // Add opcode of compressed blob end to membuf.
  dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < 1) {
    return Unexpected(errc::out_of_memory);
  }
  dest[0] = RDB_OPCODE_COMPRESSED_BLOB_END;
  uncompressed_mem_buf_.CommitWrite(1);

  return &uncompressed_mem_buf_;
}

class Lz4Decompress : public DecompressImpl {
 public:
  Lz4Decompress() {
    auto result = LZ4F_createDecompressionContext(&dctx_, LZ4F_VERSION);
    CHECK(!LZ4F_isError(result));
  }
  ~Lz4Decompress() {
    auto result = LZ4F_freeDecompressionContext(dctx_);
    CHECK(!LZ4F_isError(result));
  }

  io::Result<base::IoBuf*> Decompress(std::string_view str);

 private:
  LZ4F_dctx* dctx_;
};

io::Result<base::IoBuf*> Lz4Decompress::Decompress(std::string_view data) {
  LZ4F_frameInfo_t frame_info;
  size_t frame_size = data.size();

  // Get content size from frame data
  size_t consumed = frame_size;  // The nb of bytes consumed from data will be written into consumed
  size_t res = LZ4F_getFrameInfo(dctx_, &frame_info, data.data(), &consumed);
  if (LZ4F_isError(res)) {
    return make_unexpected(error_code{int(res), generic_category()});
  }
  if (frame_info.contentSize == 0) {
    LOG(ERROR) << "Missing frame content size";
    return Unexpected(errc::rdb_file_corrupted);
  }

  // reserve place for uncompressed data and end opcode
  size_t reserve = frame_info.contentSize + 1;
  uncompressed_mem_buf_.Reserve(reserve);
  IoBuf::Bytes dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < reserve) {
    return Unexpected(errc::out_of_memory);
  }

  // Uncompress data to membuf
  string_view src = data.substr(consumed);
  size_t src_size = src.size();

  size_t ret = 1;
  while (ret != 0) {
    IoBuf::Bytes dest = uncompressed_mem_buf_.AppendBuffer();
    size_t dest_capacity = dest.size();

    // It will read up to src_size bytes from src,
    // and decompress data into dest, of capacity dest_capacity
    // The nb of bytes consumed from src will be written into src_size
    // The nb of bytes decompressed into dest will be written into dest_capacity
    ret = LZ4F_decompress(dctx_, dest.data(), &dest_capacity, src.data(), &src_size, nullptr);
    if (LZ4F_isError(ret)) {
      return make_unexpected(error_code{int(ret), generic_category()});
    }
    consumed += src_size;

    uncompressed_mem_buf_.CommitWrite(dest_capacity);
    src = src.substr(src_size);
    src_size = src.size();
  }
  if (consumed != frame_size) {
    return Unexpected(errc::rdb_file_corrupted);
  }
  if (uncompressed_mem_buf_.InputLen() != frame_info.contentSize) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  // Add opcode of compressed blob end to membuf.
  dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < 1) {
    return Unexpected(errc::out_of_memory);
  }
  dest[0] = RDB_OPCODE_COMPRESSED_BLOB_END;
  uncompressed_mem_buf_.CommitWrite(1);

  return &uncompressed_mem_buf_;
}

class RdbLoaderBase::OpaqueObjLoader {
 public:
  OpaqueObjLoader(int rdb_type, PrimeValue* pv) : rdb_type_(rdb_type), pv_(pv) {
  }

  void operator()(robj* o) {
    pv_->ImportRObj(o);
  }

  void operator()(long long val) {
    pv_->SetInt(val);
  }

  void operator()(const base::PODArray<char>& str);
  void operator()(const LzfString& lzfstr);
  void operator()(const unique_ptr<LoadTrace>& ptr);
  void operator()(const JsonType& jt);

  std::error_code ec() const {
    return ec_;
  }

 private:
  void CreateSet(const LoadTrace* ltrace);
  void CreateHMap(const LoadTrace* ltrace);
  void CreateList(const LoadTrace* ltrace);
  void CreateZSet(const LoadTrace* ltrace);
  void CreateStream(const LoadTrace* ltrace);

  void HandleBlob(string_view blob);

  sds ToSds(const RdbVariant& obj);
  string_view ToSV(const RdbVariant& obj);

  template <typename F> static void Iterate(const LoadTrace& ltrace, F&& f) {
    unsigned cnt = 0;
    for (const auto& seg : ltrace.arr) {
      for (const auto& blob : seg) {
        if (!f(blob)) {
          return;
        }
        YieldIfNeeded(++cnt);
      }
    }
  }

  std::error_code ec_;
  int rdb_type_;
  base::PODArray<char> tset_blob_;
  PrimeValue* pv_;
};

RdbLoaderBase::RdbLoaderBase() : origin_mem_buf_{16_KB} {
  mem_buf_ = &origin_mem_buf_;
}

RdbLoaderBase::~RdbLoaderBase() {
}

void RdbLoaderBase::OpaqueObjLoader::operator()(const base::PODArray<char>& str) {
  string_view sv(str.data(), str.size());
  HandleBlob(sv);
}

void RdbLoaderBase::OpaqueObjLoader::operator()(const LzfString& lzfstr) {
  string tmp(lzfstr.uncompressed_len, '\0');
  if (lzf_decompress(lzfstr.compressed_blob.data(), lzfstr.compressed_blob.size(), tmp.data(),
                     tmp.size()) == 0) {
    LOG(ERROR) << "Invalid LZF compressed string";
    ec_ = RdbError(errc::rdb_file_corrupted);
    return;
  }
  HandleBlob(tmp);
}

void RdbLoaderBase::OpaqueObjLoader::operator()(const unique_ptr<LoadTrace>& ptr) {
  switch (rdb_type_) {
    case RDB_TYPE_SET:
      CreateSet(ptr.get());
      break;
    case RDB_TYPE_HASH:
      CreateHMap(ptr.get());
      break;
    case RDB_TYPE_LIST_QUICKLIST:
    case RDB_TYPE_LIST_QUICKLIST_2:
      CreateList(ptr.get());
      break;
    case RDB_TYPE_ZSET:
    case RDB_TYPE_ZSET_2:
      CreateZSet(ptr.get());
      break;
    case RDB_TYPE_STREAM_LISTPACKS:
      CreateStream(ptr.get());
      break;
    default:
      LOG(FATAL) << "Unsupported rdb type " << rdb_type_;
  }
}

void RdbLoaderBase::OpaqueObjLoader::operator()(const JsonType& json) {
  pv_->SetJson(JsonType{json});
}

void RdbLoaderBase::OpaqueObjLoader::CreateSet(const LoadTrace* ltrace) {
  size_t len = ltrace->blob_count();

  bool is_intset = true;
  if (len <= SetFamily::MaxIntsetEntries()) {
    Iterate(*ltrace, [&](const LoadBlob& blob) {
      if (!holds_alternative<long long>(blob.rdb_var)) {
        is_intset = false;
        return false;
      }
      return true;
    });
  } else {
    /* Use a regular set when there are too many entries. */
    is_intset = false;
  }

  robj* res = nullptr;
  sds sdsele = nullptr;

  auto cleanup = absl::MakeCleanup([&] {
    if (sdsele)
      sdsfree(sdsele);
    decrRefCount(res);
  });

  if (is_intset) {
    res = createIntsetObject();
    long long llval;
    Iterate(*ltrace, [&](const LoadBlob& blob) {
      llval = get<long long>(blob.rdb_var);
      uint8_t success;
      res->ptr = intsetAdd((intset*)res->ptr, llval, &success);
      if (!success) {
        LOG(ERROR) << "Duplicate set members detected";
        ec_ = RdbError(errc::duplicate_key);
        return false;
      }
      return true;
    });
  } else {
    bool use_set2 = GetFlag(FLAGS_use_set2);
    if (use_set2) {
      StringSet* set = new StringSet{CompactObj::memory_resource()};
      res = createObject(OBJ_SET, set);
      res->encoding = OBJ_ENCODING_HT;
    } else {
      res = createSetObject();
    }

    // TODO: to move this logic to set_family similarly to ConvertToStrSet.

    /* It's faster to expand the dict to the right size asap in order
     * to avoid rehashing */
    if (len > DICT_HT_INITIAL_SIZE && !resizeStringSet(res, len, use_set2)) {
      LOG(ERROR) << "OOM in dictTryExpand " << len;
      ec_ = RdbError(errc::out_of_memory);
      return;
    }

    if (use_set2) {
      Iterate(*ltrace, [&](const LoadBlob& blob) {
        sdsele = ToSds(blob.rdb_var);
        if (!sdsele)
          return false;

        if (!((StringSet*)res->ptr)->AddSds(sdsele)) {
          LOG(ERROR) << "Duplicate set members detected";
          ec_ = RdbError(errc::duplicate_key);
          return false;
        }
        return true;
      });
    } else {
      Iterate(*ltrace, [&](const LoadBlob& blob) {
        sdsele = ToSds(blob.rdb_var);
        if (!sdsele)
          return false;

        if (dictAdd((dict*)res->ptr, sdsele, NULL) != DICT_OK) {
          LOG(ERROR) << "Duplicate set members detected";
          ec_ = RdbError(errc::duplicate_key);
          return false;
        }
        return true;
      });
    }
  }

  if (ec_)
    return;
  pv_->ImportRObj(res);
  std::move(cleanup).Cancel();
}

void RdbLoaderBase::OpaqueObjLoader::CreateHMap(const LoadTrace* ltrace) {
  size_t len = ltrace->blob_count() / 2;

  /* Too many entries? Use a hash table right from the start. */
  bool keep_lp = (len <= 64);

  size_t lp_size = 0;
  if (keep_lp) {
    Iterate(*ltrace, [&](const LoadBlob& blob) {
      size_t str_len = StrLen(blob.rdb_var);
      lp_size += str_len;

      if (str_len > server.max_map_field_len) {
        keep_lp = false;
        return false;
      }
      return true;
    });
  }

  if (keep_lp) {
    uint8_t* lp = lpNew(lp_size);

    for (const auto& seg : ltrace->arr) {
      CHECK(seg.size() % 2 == 0);
      for (size_t i = 0; i < seg.size(); i += 2) {
        /* Add pair to listpack */
        string_view sv = ToSV(seg[i].rdb_var);
        lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());

        sv = ToSV(seg[i + 1].rdb_var);
        lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
      }
    }

    if (ec_) {
      lpFree(lp);
      return;
    }

    lp = lpShrinkToFit(lp);
    pv_->InitRobj(OBJ_HASH, kEncodingListPack, lp);
  } else {
    StringMap* string_map = new StringMap;

    auto cleanup = absl::MakeCleanup([&] { delete string_map; });
    std::string key;
    string_map->Reserve(len);
    for (const auto& seg : ltrace->arr) {
      for (size_t i = 0; i < seg.size(); i += 2) {
        // ToSV may reference an internal buffer, therefore we can use only before the
        // next call to ToSV. To workaround, copy the key locally.
        key = ToSV(seg[i].rdb_var);
        string_view val = ToSV(seg[i + 1].rdb_var);

        if (ec_)
          return;

        if (!string_map->AddOrSkip(key, val)) {
          LOG(ERROR) << "Duplicate hash fields detected for field " << key;
          ec_ = RdbError(errc::rdb_file_corrupted);
          return;
        }
      }
    }
    pv_->InitRobj(OBJ_HASH, kEncodingStrMap2, string_map);
    std::move(cleanup).Cancel();
  }
}

void RdbLoaderBase::OpaqueObjLoader::CreateList(const LoadTrace* ltrace) {
  quicklist* ql =
      quicklistNew(GetFlag(FLAGS_list_max_listpack_size), GetFlag(FLAGS_list_compress_depth));
  auto cleanup = absl::Cleanup([&] { quicklistRelease(ql); });

  Iterate(*ltrace, [&](const LoadBlob& blob) {
    unsigned container = blob.encoding;
    string_view sv = ToSV(blob.rdb_var);

    if (ec_)
      return false;

    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
      quicklistAppendPlainNode(ql, (uint8_t*)sv.data(), sv.size());
      return true;
    }

    uint8_t* lp = nullptr;

    if (rdb_type_ == RDB_TYPE_LIST_QUICKLIST_2) {
      uint8_t* src = (uint8_t*)sv.data();
      if (!lpValidateIntegrity(src, sv.size(), 0, nullptr, nullptr)) {
        LOG(ERROR) << "Listpack integrity check failed.";
        ec_ = RdbError(errc::rdb_file_corrupted);
        return false;
      }

      if (lpLength(src) == 0) {
        return true;
      }

      lp = (uint8_t*)zmalloc(sv.size());
      ::memcpy(lp, src, sv.size());
    } else {
      lp = lpNew(sv.size());
      if (!ziplistValidateIntegrity((uint8_t*)sv.data(), sv.size(), 1,
                                    ziplistEntryConvertAndValidate, &lp)) {
        LOG(ERROR) << "Ziplist integrity check failed.";
        zfree(lp);
        ec_ = RdbError(errc::rdb_file_corrupted);
        return false;
      }

      /* Silently skip empty ziplists, if we'll end up with empty quicklist we'll fail later. */
      if (lpLength(lp) == 0) {
        zfree(lp);
        return true;
      }

      lp = lpShrinkToFit(lp);
    }

    quicklistAppendListpack(ql, lp);
    return true;
  });

  if (ec_)
    return;
  if (quicklistCount(ql) == 0) {
    ec_ = RdbError(errc::empty_key);
    return;
  }

  robj* res = createObject(OBJ_LIST, ql);
  res->encoding = OBJ_ENCODING_QUICKLIST;
  std::move(cleanup).Cancel();

  pv_->ImportRObj(res);
}

void RdbLoaderBase::OpaqueObjLoader::CreateZSet(const LoadTrace* ltrace) {
  size_t zsetlen = ltrace->blob_count();
  detail::SortedMap* zs = new detail::SortedMap(CompactObj::memory_resource());
  unsigned encoding = OBJ_ENCODING_SKIPLIST;
  auto cleanup = absl::MakeCleanup([&] { delete zs; });

  if (zsetlen > DICT_HT_INITIAL_SIZE && !zs->Reserve(zsetlen)) {
    LOG(ERROR) << "OOM in dictTryExpand " << zsetlen;
    ec_ = RdbError(errc::out_of_memory);
    return;
  }

  size_t maxelelen = 0, totelelen = 0;

  Iterate(*ltrace, [&](const LoadBlob& blob) {
    sds sdsele = ToSds(blob.rdb_var);
    if (!sdsele)
      return false;

    double score = blob.score;

    /* Don't care about integer-encoded strings. */
    if (sdslen(sdsele) > maxelelen)
      maxelelen = sdslen(sdsele);
    totelelen += sdslen(sdsele);

    if (!zs->Insert(score, sdsele)) {
      LOG(ERROR) << "Duplicate zset fields detected";
      sdsfree(sdsele);
      ec_ = RdbError(errc::rdb_file_corrupted);
      return false;
    }

    return true;
  });

  if (ec_)
    return;

  void* inner = zs;
  if (zs->Size() <= server.zset_max_listpack_entries &&
      maxelelen <= server.zset_max_listpack_value && lpSafeToAdd(NULL, totelelen)) {
    encoding = OBJ_ENCODING_LISTPACK;
    inner = zs->ToListPack();
    delete zs;
  }

  std::move(cleanup).Cancel();

  pv_->InitRobj(OBJ_ZSET, encoding, inner);
}

void RdbLoaderBase::OpaqueObjLoader::CreateStream(const LoadTrace* ltrace) {
  CHECK(ltrace->stream_trace);

  robj* res = createStreamObject();
  stream* s = (stream*)res->ptr;

  auto cleanup = absl::Cleanup([&] { decrRefCount(res); });

  for (const auto& seg : ltrace->arr) {
    for (size_t i = 0; i < seg.size(); i += 2) {
      string_view nodekey = ToSV(seg[i].rdb_var);
      string_view data = ToSV(seg[i + 1].rdb_var);

      uint8_t* lp = (uint8_t*)data.data();

      if (!streamValidateListpackIntegrity(lp, data.size(), 0)) {
        LOG(ERROR) << "Stream listpack integrity check failed.";
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }
      unsigned char* first = lpFirst(lp);
      if (first == NULL) {
        /* Serialized listpacks should never be empty, since on
         * deletion we should remove the radix tree key if the
         * resulting listpack is empty. */
        LOG(ERROR) << "Empty listpack inside stream";
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }
      uint8_t* copy_lp = (uint8_t*)zmalloc(data.size());
      ::memcpy(copy_lp, lp, data.size());
      /* Insert the key in the radix tree. */
      int retval =
          raxTryInsert(s->rax_tree, (unsigned char*)nodekey.data(), nodekey.size(), copy_lp, NULL);
      if (!retval) {
        zfree(copy_lp);
        LOG(ERROR) << "Listpack re-added with existing key";
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }
    }
  }
  s->length = ltrace->stream_trace->stream_len;
  s->last_id.ms = ltrace->stream_trace->ms;
  s->last_id.seq = ltrace->stream_trace->seq;

  for (const auto& cg : ltrace->stream_trace->cgroup) {
    string_view cgname = ToSV(cg.name);
    streamID cg_id;
    cg_id.ms = cg.ms;
    cg_id.seq = cg.seq;

    streamCG* cgroup = streamCreateCG(s, cgname.data(), cgname.size(), &cg_id, 0);
    if (cgroup == NULL) {
      LOG(ERROR) << "Duplicated consumer group name " << cgname;
      ec_ = RdbError(errc::duplicate_key);
      return;
    }

    for (const auto& pel : cg.pel_arr) {
      streamNACK* nack = streamCreateNACK(NULL);
      nack->delivery_time = pel.delivery_time;
      nack->delivery_count = pel.delivery_count;

      if (!raxTryInsert(cgroup->pel, const_cast<uint8_t*>(pel.rawid.data()), pel.rawid.size(), nack,
                        NULL)) {
        LOG(ERROR) << "Duplicated global PEL entry loading stream consumer group";
        ec_ = RdbError(errc::duplicate_key);
        streamFreeNACK(nack);
        return;
      }
    }

    for (const auto& cons : cg.cons_arr) {
      sds cname = ToSds(cons.name);

      streamConsumer* consumer =
          streamCreateConsumer(cgroup, cname, NULL, 0, SCC_NO_NOTIFY | SCC_NO_DIRTIFY);
      sdsfree(cname);
      if (!consumer) {
        LOG(ERROR) << "Duplicate stream consumer detected.";
        ec_ = RdbError(errc::duplicate_key);
        return;
      }
      consumer->seen_time = cons.seen_time;

      /* Create the PEL (pending entries list) about entries owned by this specific
       * consumer. */
      for (const auto& rawid : cons.nack_arr) {
        uint8_t* ptr = const_cast<uint8_t*>(rawid.data());
        streamNACK* nack = (streamNACK*)raxFind(cgroup->pel, ptr, rawid.size());
        if (nack == raxNotFound) {
          LOG(ERROR) << "Consumer entry not found in group global PEL";
          ec_ = RdbError(errc::rdb_file_corrupted);
          return;
        }

        /* Set the NACK consumer, that was left to NULL when
         * loading the global PEL. Then set the same shared
         * NACK structure also in the consumer-specific PEL. */
        nack->consumer = consumer;
        if (!raxTryInsert(consumer->pel, ptr, rawid.size(), nack, NULL)) {
          LOG(ERROR) << "Duplicated consumer PEL entry loading a stream consumer group";
          streamFreeNACK(nack);
          ec_ = RdbError(errc::duplicate_key);
          return;
        }
      }
    }
  }

  std::move(cleanup).Cancel();
  pv_->ImportRObj(res);
}

void RdbLoaderBase::OpaqueObjLoader::HandleBlob(string_view blob) {
  if (rdb_type_ == RDB_TYPE_STRING) {
    pv_->SetString(blob);
    return;
  }

  if (rdb_type_ == RDB_TYPE_SET_INTSET) {
    if (!intsetValidateIntegrity((const uint8_t*)blob.data(), blob.size(), 0)) {
      LOG(ERROR) << "Intset integrity check failed.";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }

    const intset* is = (const intset*)blob.data();

    unsigned len = intsetLen(is);
    robj* res = nullptr;

    if (len > SetFamily::MaxIntsetEntries()) {
      res = createSetObject();
      if (!SetFamily::ConvertToStrSet(is, len, res)) {
        LOG(ERROR) << "OOM in ConvertToStrSet " << len;
        decrRefCount(res);
        ec_ = RdbError(errc::out_of_memory);
        return;
      }
    } else {
      intset* mine = (intset*)zmalloc(blob.size());
      ::memcpy(mine, blob.data(), blob.size());
      res = createObject(OBJ_SET, mine);
      res->encoding = OBJ_ENCODING_INTSET;
    }
    pv_->ImportRObj(res);
  } else if (rdb_type_ == RDB_TYPE_SET_LISTPACK) {
    if (!lpValidateIntegrity((uint8_t*)blob.data(), blob.size(), 0, nullptr, nullptr)) {
      LOG(ERROR) << "ListPack integrity check failed.";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }
    unsigned char* lp = (unsigned char*)blob.data();
    auto iterate_and_apply_f = [lp](auto f) {
      for (unsigned char* cur = lpFirst(lp); cur != nullptr; cur = lpNext(lp, cur)) {
        unsigned int slen = 0;
        long long lval = 0;
        unsigned char* res = lpGetValue(cur, &slen, &lval);
        f(res, slen, lval);
      }
    };
    const bool use_set2 = GetFlag(FLAGS_use_set2);
    robj* res = nullptr;
    if (use_set2) {
      StringSet* set = new StringSet{CompactObj::memory_resource()};
      res = createObject(OBJ_SET, set);
      res->encoding = OBJ_ENCODING_HT;
      auto f = [this, res](unsigned char* val, unsigned int slen, long long lval) {
        sds sdsele = (val) ? sdsnewlen(val, slen) : sdsfromlonglong(lval);
        if (!((StringSet*)res->ptr)->AddSds(sdsele)) {
          LOG(ERROR) << "Error adding to member set2";
          ec_ = RdbError(errc::duplicate_key);
        }
      };
      iterate_and_apply_f(f);
    } else {
      res = createSetObject();
      auto f = [this, res](unsigned char* val, unsigned int slen, long long lval) {
        sds sdsele = (val) ? sdsnewlen(val, slen) : sdsfromlonglong(lval);
        if (!dictAdd((dict*)res->ptr, sdsele, nullptr)) {
          LOG(ERROR) << "Error adding to member set";
          ec_ = RdbError(errc::duplicate_key);
        }
      };
      iterate_and_apply_f(f);
    }
    if (ec_) {
      decrRefCount(res);
      return;
    }
    pv_->ImportRObj(res);
  } else if (rdb_type_ == RDB_TYPE_HASH_ZIPLIST || rdb_type_ == RDB_TYPE_HASH_LISTPACK) {
    unsigned char* lp = lpNew(blob.size());
    switch (rdb_type_) {
      case RDB_TYPE_HASH_ZIPLIST:
        if (!ziplistPairsConvertAndValidateIntegrity((const uint8_t*)blob.data(), blob.size(),
                                                     &lp)) {
          LOG(ERROR) << "Zset ziplist integrity check failed.";
          zfree(lp);
          ec_ = RdbError(errc::rdb_file_corrupted);
          return;
        }
        break;
      case RDB_TYPE_HASH_LISTPACK:
        if (!lpValidateIntegrity((uint8_t*)blob.data(), blob.size(), 0, nullptr, nullptr)) {
          LOG(ERROR) << "ListPack integrity check failed.";
          zfree(lp);
          ec_ = RdbError(errc::rdb_file_corrupted);
          return;
        }
        std::memcpy(lp, blob.data(), blob.size());
        break;
    }

    if (lpLength(lp) == 0) {
      lpFree(lp);
      ec_ = RdbError(errc::empty_key);
      return;
    }

    if (lpBytes(lp) > server.max_listpack_map_bytes) {
      StringMap* sm = HSetFamily::ConvertToStrMap(lp);
      lpFree(lp);
      pv_->InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
    } else {
      lp = lpShrinkToFit(lp);
      pv_->InitRobj(OBJ_HASH, kEncodingListPack, lp);
    }
    return;
  } else if (rdb_type_ == RDB_TYPE_ZSET_ZIPLIST) {
    unsigned char* lp = lpNew(blob.size());
    if (!ziplistPairsConvertAndValidateIntegrity((uint8_t*)blob.data(), blob.size(), &lp)) {
      LOG(ERROR) << "Zset ziplist integrity check failed.";
      zfree(lp);
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }

    if (lpLength(lp) == 0) {
      lpFree(lp);
      ec_ = RdbError(errc::empty_key);
      return;
    }

    unsigned encoding = OBJ_ENCODING_LISTPACK;
    void* inner;
    if (lpBytes(lp) >= server.max_listpack_map_bytes) {
      inner = detail::SortedMap::FromListPack(CompactObj::memory_resource(), lp).release();
      lpFree(lp);
      encoding = OBJ_ENCODING_SKIPLIST;
    } else {
      lp = lpShrinkToFit(lp);
      inner = lp;
    }
    pv_->InitRobj(OBJ_ZSET, encoding, inner);
    return;
  } else if (rdb_type_ == RDB_TYPE_ZSET_LISTPACK) {
    if (!lpValidateIntegrity((uint8_t*)blob.data(), blob.size(), 0, nullptr, nullptr)) {
      LOG(ERROR) << "ListPack integrity check failed.";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }
    unsigned char* src_lp = (unsigned char*)blob.data();
    unsigned long long bytes = lpBytes(src_lp);
    unsigned char* lp = (uint8_t*)zmalloc(bytes);
    std::memcpy(lp, src_lp, bytes);
    pv_->InitRobj(OBJ_ZSET, OBJ_ENCODING_LISTPACK, lp);
  } else {
    LOG(FATAL) << "Unsupported rdb type " << rdb_type_;
  }
}

sds RdbLoaderBase::OpaqueObjLoader::ToSds(const RdbVariant& obj) {
  if (holds_alternative<long long>(obj)) {
    return sdsfromlonglong(get<long long>(obj));
  }

  const base::PODArray<char>* ch_arr = get_if<base::PODArray<char>>(&obj);
  if (ch_arr) {
    return sdsnewlen(ch_arr->data(), ch_arr->size());
  }

  const LzfString* lzf = get_if<LzfString>(&obj);
  if (lzf) {
    sds res = sdsnewlen(NULL, lzf->uncompressed_len);
    if (lzf_decompress(lzf->compressed_blob.data(), lzf->compressed_blob.size(), res,
                       lzf->uncompressed_len) == 0) {
      LOG(ERROR) << "Invalid LZF compressed string";
      ec_ = RdbError(errc::rdb_file_corrupted);
      sdsfree(res);

      return nullptr;
    }
    return res;
  }

  LOG(FATAL) << "Unexpected variant";
  return nullptr;
}

string_view RdbLoaderBase::OpaqueObjLoader::ToSV(const RdbVariant& obj) {
  if (holds_alternative<long long>(obj)) {
    tset_blob_.resize(32);
    auto val = get<long long>(obj);
    char* next = absl::numbers_internal::FastIntToBuffer(val, tset_blob_.data());
    return string_view{tset_blob_.data(), size_t(next - tset_blob_.data())};
  }

  const base::PODArray<char>* ch_arr = get_if<base::PODArray<char>>(&obj);
  if (ch_arr) {
    // pass non-null pointer to avoid UB with lp API.
    return ch_arr->empty() ? ""sv : string_view{ch_arr->data(), ch_arr->size()};
  }

  const LzfString* lzf = get_if<LzfString>(&obj);
  if (lzf) {
    tset_blob_.resize(lzf->uncompressed_len);
    if (lzf_decompress(lzf->compressed_blob.data(), lzf->compressed_blob.size(), tset_blob_.data(),
                       lzf->uncompressed_len) == 0) {
      LOG(ERROR) << "Invalid LZF compressed string";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return string_view{tset_blob_.data(), 0};
    }
    return string_view{tset_blob_.data(), tset_blob_.size()};
  }

  LOG(FATAL) << "Unexpected variant";
  return string_view{};
}

std::error_code RdbLoaderBase::FetchBuf(size_t size, void* dest) {
  if (size == 0)
    return kOk;

  uint8_t* next = (uint8_t*)dest;
  size_t bytes_read;

  size_t to_copy = std::min(mem_buf_->InputLen(), size);
  DVLOG(2) << "Copying " << to_copy << " bytes";

  ::memcpy(next, mem_buf_->InputBuffer().data(), to_copy);
  mem_buf_->ConsumeInput(to_copy);
  size -= to_copy;
  if (size == 0)
    return kOk;

  next += to_copy;

  if (size + bytes_read_ > source_limit_) {
    LOG(ERROR) << "Out of bound read " << size + bytes_read_ << " vs " << source_limit_;

    return RdbError(errc::rdb_file_corrupted);
  }

  if (size > 512) {  // Worth reading directly into next.
    io::MutableBytes mb{next, size};

    SET_OR_RETURN(src_->Read(mb), bytes_read);
    if (bytes_read < size)
      return RdbError(errc::rdb_file_corrupted);

    bytes_read_ += bytes_read;
    DCHECK_LE(bytes_read_, source_limit_);

    return kOk;
  }

  io::MutableBytes mb = mem_buf_->AppendBuffer();

  // Must be because mem_buf_ is be empty.
  DCHECK_GT(mb.size(), size);

  if (bytes_read_ + mb.size() > source_limit_) {
    mb = mb.subspan(0, source_limit_ - bytes_read_);
  }

  SET_OR_RETURN(src_->ReadAtLeast(mb, size), bytes_read);

  if (bytes_read < size)
    return RdbError(errc::rdb_file_corrupted);
  bytes_read_ += bytes_read;

  DCHECK_LE(bytes_read_, source_limit_);

  mem_buf_->CommitWrite(bytes_read);
  ::memcpy(next, mem_buf_->InputBuffer().data(), size);
  mem_buf_->ConsumeInput(size);

  return kOk;
}

size_t RdbLoaderBase::StrLen(const RdbVariant& tset) {
  const base::PODArray<char>* arr = get_if<base::PODArray<char>>(&tset);
  if (arr)
    return arr->size();

  if (holds_alternative<long long>(tset)) {
    auto val = get<long long>(tset);
    char buf[32];
    char* next = absl::numbers_internal::FastIntToBuffer(val, buf);
    return (next - buf);
  }

  const LzfString* lzf = get_if<LzfString>(&tset);
  if (lzf)
    return lzf->uncompressed_len;

  LOG(DFATAL) << "should not reach";
  return 0;
}

auto RdbLoaderBase::FetchGenericString() -> io::Result<string> {
  bool isencoded;
  size_t len;

  SET_OR_UNEXPECT(LoadLen(&isencoded), len);

  if (isencoded) {
    switch (len) {
      case RDB_ENC_INT8:
      case RDB_ENC_INT16:
      case RDB_ENC_INT32:
        return FetchIntegerObject(len);
      case RDB_ENC_LZF:
        return FetchLzfStringObject();
      default:
        LOG(ERROR) << "Unknown RDB string encoding len " << len;
        return Unexpected(errc::rdb_file_corrupted);
    }
  }

  string res;

  if (len > 0) {
    res.resize(len);
    error_code ec = FetchBuf(len, res.data());
    if (ec) {
      return make_unexpected(ec);
    }
  }

  return res;
}

auto RdbLoaderBase::FetchLzfStringObject() -> io::Result<string> {
  bool zerocopy_decompress = true;

  const uint8_t* cbuf = NULL;
  uint64_t clen, len;

  SET_OR_UNEXPECT(LoadLen(NULL), clen);
  SET_OR_UNEXPECT(LoadLen(NULL), len);

  if (len > 1ULL << 29 || len <= clen || clen == 0) {
    LOG(ERROR) << "Bad compressed string";
    return Unexpected(rdb::rdb_file_corrupted);
  }

  if (mem_buf_->InputLen() >= clen) {
    cbuf = mem_buf_->InputBuffer().data();
  } else {
    compr_buf_.resize(clen);
    zerocopy_decompress = false;

    /* Load the compressed representation and uncompress it to target. */
    error_code ec = FetchBuf(clen, compr_buf_.data());
    if (ec) {
      return make_unexpected(ec);
    }
    cbuf = compr_buf_.data();
  }

  string res(len, 0);

  if (lzf_decompress(cbuf, clen, res.data(), len) == 0) {
    LOG(ERROR) << "Invalid LZF compressed string";
    return Unexpected(errc::rdb_file_corrupted);
  }

  // FetchBuf consumes the input but if we have not went through that path
  // we need to consume now.
  if (zerocopy_decompress)
    mem_buf_->ConsumeInput(clen);

  return res;
}

auto RdbLoaderBase::FetchIntegerObject(int enctype) -> io::Result<string> {
  io::Result<long long> val = ReadIntObj(enctype);

  if (!val.has_value()) {
    return val.get_unexpected();
  }

  char buf[32];
  absl::numbers_internal::FastIntToBuffer(*val, buf);

  return string(buf);
}

io::Result<double> RdbLoaderBase::FetchBinaryDouble() {
  union {
    uint64_t val;
    double d;
  } u;

  static_assert(sizeof(u) == sizeof(uint64_t));
  auto ec = EnsureRead(8);
  if (ec)
    return make_unexpected(ec);

  uint8_t buf[8];
  mem_buf_->ReadAndConsume(8, buf);
  u.val = base::LE::LoadT<uint64_t>(buf);
  return u.d;
}

io::Result<double> RdbLoaderBase::FetchDouble() {
  uint8_t len;

  SET_OR_UNEXPECT(FetchInt<uint8_t>(), len);
  constexpr double kInf = std::numeric_limits<double>::infinity();
  switch (len) {
    case 255:
      return -kInf;
    case 254:
      return kInf;
    case 253:
      return std::numeric_limits<double>::quiet_NaN();
    default:;
  }
  char buf[256];
  error_code ec = FetchBuf(len, buf);
  if (ec)
    return make_unexpected(ec);
  buf[len] = '\0';
  double val;
  if (sscanf(buf, "%lg", &val) != 1)
    return Unexpected(errc::rdb_file_corrupted);
  return val;
}

auto RdbLoaderBase::ReadKey() -> io::Result<string> {
  return FetchGenericString();
}

error_code RdbLoaderBase::ReadObj(int rdbtype, OpaqueObj* dest) {
  io::Result<OpaqueObj> iores;

  switch (rdbtype) {
    case RDB_TYPE_STRING: {
      dest->rdb_type = RDB_TYPE_STRING;

      /* Read string value */
      return ReadStringObj(&dest->obj);
    }
    case RDB_TYPE_SET:
      iores = ReadSet();
      break;
    case RDB_TYPE_SET_INTSET:
      iores = ReadIntSet();
      break;
    case RDB_TYPE_HASH_ZIPLIST:
    case RDB_TYPE_HASH_LISTPACK:
    case RDB_TYPE_ZSET_LISTPACK:
      iores = ReadGeneric(rdbtype);
      break;
    case RDB_TYPE_HASH:
      iores = ReadHMap();
      break;
    case RDB_TYPE_ZSET:
    case RDB_TYPE_ZSET_2:
      iores = ReadZSet(rdbtype);
      break;
    case RDB_TYPE_ZSET_ZIPLIST:
      iores = ReadZSetZL();
      break;
    case RDB_TYPE_LIST_QUICKLIST:
    case RDB_TYPE_LIST_QUICKLIST_2:
      iores = ReadListQuicklist(rdbtype);
      break;
    case RDB_TYPE_STREAM_LISTPACKS:
      iores = ReadStreams();
      break;
    case RDB_TYPE_JSON:
    case RDB_TYPE_SET_LISTPACK:
      // We need to deal with protocol versions 9 and older because in these
      // RDB_TYPE_JSON == 20. On newer versions > 9 we bumped up RDB_TYPE_JSON to 30
      // because it overlapped with the new type RDB_TYPE_SET_LISTPACK
      if (rdb_version_ < 10 && rdbtype == RDB_TYPE_JSON_OLD) {
        iores = ReadJson();
        break;
      }
      if (rdbtype == RDB_TYPE_JSON) {
        iores = ReadJson();
        break;
      }

      iores = ReadGeneric(rdbtype);
      break;
    default:
      LOG(ERROR) << "Unsupported rdb type " << rdbtype;

      return RdbError(errc::invalid_encoding);
  }

  if (!iores)
    return iores.error();
  *dest = std::move(*iores);
  return error_code{};
}

error_code RdbLoaderBase::ReadStringObj(RdbVariant* dest) {
  bool isencoded;
  size_t len;

  SET_OR_RETURN(LoadLen(&isencoded), len);

  if (isencoded) {
    switch (len) {
      case RDB_ENC_INT8:
      case RDB_ENC_INT16:
      case RDB_ENC_INT32: {
        io::Result<long long> io_int = ReadIntObj(len);
        if (!io_int)
          return io_int.error();
        dest->emplace<long long>(*io_int);
        return error_code{};
      }
      case RDB_ENC_LZF: {
        io::Result<LzfString> lzf = ReadLzf();
        if (!lzf)
          return lzf.error();

        dest->emplace<LzfString>(std::move(lzf.value()));
        return error_code{};
      }
      default:
        LOG(ERROR) << "Unknown RDB string encoding " << len;
        return RdbError(errc::rdb_file_corrupted);
    }
  }

  auto& blob = dest->emplace<base::PODArray<char>>();
  blob.resize(len);
  error_code ec = FetchBuf(len, blob.data());
  return ec;
}

io::Result<long long> RdbLoaderBase::ReadIntObj(int enctype) {
  long long val;

  if (enctype == RDB_ENC_INT8) {
    SET_OR_UNEXPECT(FetchInt<int8_t>(), val);
  } else if (enctype == RDB_ENC_INT16) {
    SET_OR_UNEXPECT(FetchInt<int16_t>(), val);
  } else if (enctype == RDB_ENC_INT32) {
    SET_OR_UNEXPECT(FetchInt<int32_t>(), val);
  } else {
    return Unexpected(errc::invalid_encoding);
  }
  return val;
}

auto RdbLoaderBase::ReadLzf() -> io::Result<LzfString> {
  uint64_t clen;
  LzfString res;

  SET_OR_UNEXPECT(LoadLen(NULL), clen);
  SET_OR_UNEXPECT(LoadLen(NULL), res.uncompressed_len);

  if (res.uncompressed_len > 1ULL << 29) {
    LOG(ERROR) << "Uncompressed length is too big " << res.uncompressed_len;
    return Unexpected(errc::rdb_file_corrupted);
  }

  res.compressed_blob.resize(clen);
  /* Load the compressed representation and uncompress it to target. */
  error_code ec = FetchBuf(clen, res.compressed_blob.data());
  if (ec) {
    return make_unexpected(ec);
  }

  return res;
}

auto RdbLoaderBase::ReadSet() -> io::Result<OpaqueObj> {
  size_t len;
  SET_OR_UNEXPECT(LoadLen(NULL), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  load_trace->arr.resize((len + kMaxBlobLen - 1) / kMaxBlobLen);
  for (size_t i = 0; i < load_trace->arr.size(); i++) {
    size_t n = std::min(len - i * kMaxBlobLen, kMaxBlobLen);
    load_trace->arr[i].resize(n);
    for (size_t j = 0; j < n; j++) {
      error_code ec = ReadStringObj(&load_trace->arr[i][j].rdb_var);
      if (ec) {
        return make_unexpected(ec);
      }
    }
  }

  return OpaqueObj{std::move(load_trace), RDB_TYPE_SET};
}

auto RdbLoaderBase::ReadIntSet() -> io::Result<OpaqueObj> {
  RdbVariant obj;
  error_code ec = ReadStringObj(&obj);
  if (ec) {
    return make_unexpected(ec);
  }

  const LzfString* lzf = get_if<LzfString>(&obj);
  const base::PODArray<char>* arr = get_if<base::PODArray<char>>(&obj);

  if (lzf) {
    if (lzf->uncompressed_len == 0 || lzf->compressed_blob.empty())
      return Unexpected(errc::rdb_file_corrupted);
  } else if (arr) {
    if (arr->empty())
      return Unexpected(errc::rdb_file_corrupted);
  } else {
    return Unexpected(errc::rdb_file_corrupted);
  }

  return OpaqueObj{std::move(obj), RDB_TYPE_SET_INTSET};
}

auto RdbLoaderBase::ReadGeneric(int rdbtype) -> io::Result<OpaqueObj> {
  RdbVariant str_obj;
  error_code ec = ReadStringObj(&str_obj);
  if (ec)
    return make_unexpected(ec);

  if (StrLen(str_obj) == 0) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  return OpaqueObj{std::move(str_obj), rdbtype};
}

auto RdbLoaderBase::ReadHMap() -> io::Result<OpaqueObj> {
  size_t len;
  SET_OR_UNEXPECT(LoadLen(nullptr), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);

  len *= 2;
  load_trace->arr.resize((len + kMaxBlobLen - 1) / kMaxBlobLen);
  for (size_t i = 0; i < load_trace->arr.size(); ++i) {
    size_t n = std::min<size_t>(len, kMaxBlobLen);
    load_trace->arr[i].resize(n);
    for (size_t j = 0; j < n; ++j) {
      error_code ec = ReadStringObj(&load_trace->arr[i][j].rdb_var);
      if (ec)
        return make_unexpected(ec);
    }
    len -= n;
  }

  return OpaqueObj{std::move(load_trace), RDB_TYPE_HASH};
}

auto RdbLoaderBase::ReadZSet(int rdbtype) -> io::Result<OpaqueObj> {
  /* Read sorted set value. */
  uint64_t zsetlen;
  SET_OR_UNEXPECT(LoadLen(nullptr), zsetlen);

  if (zsetlen == 0)
    return Unexpected(errc::empty_key);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  load_trace->arr.resize((zsetlen + kMaxBlobLen - 1) / kMaxBlobLen);

  double score;

  for (size_t i = 0; i < load_trace->arr.size(); ++i) {
    size_t n = std::min<size_t>(zsetlen, kMaxBlobLen);
    load_trace->arr[i].resize(n);
    for (size_t j = 0; j < n; ++j) {
      error_code ec = ReadStringObj(&load_trace->arr[i][j].rdb_var);
      if (ec)
        return make_unexpected(ec);
      if (rdbtype == RDB_TYPE_ZSET_2) {
        SET_OR_UNEXPECT(FetchBinaryDouble(), score);
      } else {
        SET_OR_UNEXPECT(FetchDouble(), score);
      }

      if (isnan(score)) {
        LOG(ERROR) << "Zset with NAN score detected";
        return Unexpected(errc::rdb_file_corrupted);
      }
      load_trace->arr[i][j].score = score;
    }
    zsetlen -= n;
  }

  return OpaqueObj{std::move(load_trace), rdbtype};
}

auto RdbLoaderBase::ReadZSetZL() -> io::Result<OpaqueObj> {
  RdbVariant str_obj;
  error_code ec = ReadStringObj(&str_obj);
  if (ec)
    return make_unexpected(ec);

  if (StrLen(str_obj) == 0) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  return OpaqueObj{std::move(str_obj), RDB_TYPE_ZSET_ZIPLIST};
}

auto RdbLoaderBase::ReadListQuicklist(int rdbtype) -> io::Result<OpaqueObj> {
  uint64_t len;
  SET_OR_UNEXPECT(LoadLen(nullptr), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  load_trace->arr.resize((len + kMaxBlobLen - 1) / kMaxBlobLen);

  for (size_t i = 0; i < load_trace->arr.size(); ++i) {
    size_t n = std::min<size_t>(len, kMaxBlobLen);
    load_trace->arr[i].resize(n);
    for (size_t j = 0; j < n; ++j) {
      uint64_t container = QUICKLIST_NODE_CONTAINER_PACKED;
      if (rdbtype == RDB_TYPE_LIST_QUICKLIST_2) {
        SET_OR_UNEXPECT(LoadLen(nullptr), container);

        if (container != QUICKLIST_NODE_CONTAINER_PACKED &&
            container != QUICKLIST_NODE_CONTAINER_PLAIN) {
          LOG(ERROR) << "Quicklist integrity check failed.";
          return Unexpected(errc::rdb_file_corrupted);
        }
      }

      RdbVariant var;
      error_code ec = ReadStringObj(&var);
      if (ec)
        return make_unexpected(ec);

      if (StrLen(var) == 0) {
        return Unexpected(errc::rdb_file_corrupted);
      }
      load_trace->arr[i][j].rdb_var = std::move(var);
      load_trace->arr[i][j].encoding = container;
    }
    len -= n;
  }

  return OpaqueObj{std::move(load_trace), rdbtype};
}

auto RdbLoaderBase::ReadStreams() -> io::Result<OpaqueObj> {
  uint64_t listpacks;
  SET_OR_UNEXPECT(LoadLen(nullptr), listpacks);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  load_trace->arr.emplace_back();
  auto& blob_arr = load_trace->arr.back();
  blob_arr.resize(listpacks * 2);

  error_code ec;
  for (size_t i = 0; i < listpacks; ++i) {
    /* Get the master ID, the one we'll use as key of the radix tree
     * node: the entries inside the listpack itself are delta-encoded
     * relatively to this ID. */
    RdbVariant stream_id, blob;
    ec = ReadStringObj(&stream_id);
    if (ec)
      return make_unexpected(ec);
    if (StrLen(stream_id) != sizeof(streamID)) {
      LOG(ERROR) << "Stream node key entry is not the size of a stream ID";

      return Unexpected(errc::rdb_file_corrupted);
    }

    ec = ReadStringObj(&blob);
    if (ec)
      return make_unexpected(ec);
    if (StrLen(blob) == 0) {
      LOG(ERROR) << "Stream listpacks loading failed";
      return Unexpected(errc::rdb_file_corrupted);
    }

    blob_arr[2 * i].rdb_var = std::move(stream_id);
    blob_arr[2 * i + 1].rdb_var = std::move(blob);
  }

  load_trace->stream_trace.reset(new StreamTrace);

  /* Load total number of items inside the stream. */
  SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->stream_len);

  /* Load the last entry ID. */
  SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->ms);
  SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->seq);

  /* Consumer groups loading */
  uint64_t cgroups_count;
  SET_OR_UNEXPECT(LoadLen(nullptr), cgroups_count);
  load_trace->stream_trace->cgroup.resize(cgroups_count);

  for (size_t i = 0; i < cgroups_count; ++i) {
    auto& cgroup = load_trace->stream_trace->cgroup[i];
    /* Get the consumer group name and ID. We can then create the
     * consumer group ASAP and populate its structure as
     * we read more data. */

    // sds cgname;
    RdbVariant cgname;
    ec = ReadStringObj(&cgname);
    if (ec)
      return make_unexpected(ec);
    cgroup.name = std::move(cgname);

    SET_OR_UNEXPECT(LoadLen(nullptr), cgroup.ms);
    SET_OR_UNEXPECT(LoadLen(nullptr), cgroup.seq);

    /* Load the global PEL for this consumer group, however we'll
     * not yet populate the NACK structures with the message
     * owner, since consumers for this group and their messages will
     * be read as a next step. So for now leave them not resolved
     * and later populate it. */
    uint64_t pel_size;
    SET_OR_UNEXPECT(LoadLen(nullptr), pel_size);

    cgroup.pel_arr.resize(pel_size);

    for (size_t j = 0; j < pel_size; ++j) {
      auto& pel = cgroup.pel_arr[j];
      error_code ec = FetchBuf(pel.rawid.size(), pel.rawid.data());
      if (ec) {
        LOG(ERROR) << "Stream PEL ID loading failed.";
        return make_unexpected(ec);
      }

      // streamNACK* nack = streamCreateNACK(NULL);
      // auto cleanup2 = absl::Cleanup([&] { streamFreeNACK(nack); });

      SET_OR_UNEXPECT(FetchInt<int64_t>(), pel.delivery_time);
      SET_OR_UNEXPECT(LoadLen(nullptr), pel.delivery_count);

      /*if (!raxTryInsert(cgroup->pel, rawid, sizeof(rawid), nack, NULL)) {
        LOG(ERROR) << "Duplicated global PEL entry loading stream consumer group";
        return Unexpected(errc::duplicate_key);
      }
      std::move(cleanup2).Cancel();*/
    }

    /* Now that we loaded our global PEL, we need to load the
     * consumers and their local PELs. */
    uint64_t consumers_num;
    SET_OR_UNEXPECT(LoadLen(nullptr), consumers_num);
    cgroup.cons_arr.resize(consumers_num);

    for (size_t j = 0; j < consumers_num; ++j) {
      auto& consumer = cgroup.cons_arr[j];
      ec = ReadStringObj(&consumer.name);
      if (ec)
        return make_unexpected(ec);

      SET_OR_UNEXPECT(FetchInt<int64_t>(), consumer.seen_time);

      /* Load the PEL about entries owned by this specific
       * consumer. */
      SET_OR_UNEXPECT(LoadLen(nullptr), pel_size);
      consumer.nack_arr.resize(pel_size);
      for (size_t k = 0; k < pel_size; ++k) {
        auto& nack = consumer.nack_arr[k];
        // unsigned char rawid[sizeof(streamID)];
        error_code ec = FetchBuf(nack.size(), nack.data());
        if (ec) {
          LOG(ERROR) << "Stream PEL ID loading failed.";
          return make_unexpected(ec);
        }
        /*streamNACK* nack = (streamNACK*)raxFind(cgroup->pel, rawid, sizeof(rawid));
        if (nack == raxNotFound) {
          LOG(ERROR) << "Consumer entry not found in group global PEL";
          return Unexpected(errc::rdb_file_corrupted);
        }*/

        /* Set the NACK consumer, that was left to NULL when
         * loading the global PEL. Then set the same shared
         * NACK structure also in the consumer-specific PEL. */
        /*
        nack->consumer = consumer;
        if (!raxTryInsert(consumer->pel, rawid, sizeof(rawid), nack, NULL)) {
          LOG(ERROR) << "Duplicated consumer PEL entry loading a stream consumer group";
          streamFreeNACK(nack);
          return Unexpected(errc::duplicate_key);
        }*/
      }
    }  // while (consumers_num)
  }    // while (cgroup_num)

  return OpaqueObj{std::move(load_trace), RDB_TYPE_STREAM_LISTPACKS};
}

auto RdbLoaderBase::ReadJson() -> io::Result<OpaqueObj> {
  string json_str;
  SET_OR_UNEXPECT(FetchGenericString(), json_str);

  auto json = JsonFromString(json_str);
  if (!json)
    return Unexpected(errc::bad_json_string);

  return OpaqueObj{std::move(*json), RDB_TYPE_JSON};
}

template <typename T> io::Result<T> RdbLoaderBase::FetchInt() {
  auto ec = EnsureRead(sizeof(T));
  if (ec)
    return make_unexpected(ec);

  char buf[16];
  mem_buf_->ReadAndConsume(sizeof(T), buf);

  return base::LE::LoadT<std::make_unsigned_t<T>>(buf);
}

io::Result<uint8_t> RdbLoaderBase::FetchType() {
  return FetchInt<uint8_t>();
}

// -------------- RdbLoader   ----------------------------

struct RdbLoader::ObjSettings {
  long long now;           // current epoch time in ms.
  int64_t expiretime = 0;  // expire epoch time in ms

  bool has_expired = false;

  void Reset() {
    expiretime = 0;
    has_expired = false;
  }

  void SetExpire(int64_t val) {
    expiretime = val;
    has_expired = (val <= now);
  }

  ObjSettings() = default;
};

RdbLoader::RdbLoader(Service* service)
    : service_{service}, script_mgr_{service == nullptr ? nullptr : service->script_mgr()} {
  shard_buf_.reset(new ItemsBuf[shard_set->size()]);
}

RdbLoader::~RdbLoader() {
  while (true) {
    Item* item = item_queue_.Pop();
    if (item == nullptr)
      break;
    delete item;
  }
}

error_code RdbLoader::Load(io::Source* src) {
  CHECK(!src_ && src);

  absl::Time start = absl::Now();
  src_ = src;

  IoBuf::Bytes bytes = mem_buf_->AppendBuffer();
  io::Result<size_t> read_sz = src_->ReadAtLeast(bytes, 9);
  if (!read_sz)
    return read_sz.error();

  bytes_read_ = *read_sz;
  if (bytes_read_ < 9) {
    return RdbError(errc::wrong_signature);
  }

  mem_buf_->CommitWrite(bytes_read_);

  {
    auto cb = mem_buf_->InputBuffer();

    if (memcmp(cb.data(), "REDIS", 5) != 0) {
      VLOG(1) << "Bad header: " << absl::CHexEscape(facade::ToSV(cb));
      return RdbError(errc::wrong_signature);
    }

    char buf[64] = {0};
    ::memcpy(buf, cb.data() + 5, 4);

    rdb_version_ = atoi(buf);
    if (rdb_version_ < 5 || rdb_version_ > RDB_VERSION) {  // We accept starting from 5.
      LOG(ERROR) << "RDB Version " << rdb_version_ << " is not supported";
      return RdbError(errc::bad_version);
    }

    mem_buf_->ConsumeInput(9);
  }

  int type;

  /* Key-specific attributes, set by opcodes before the key type. */
  ObjSettings settings;
  settings.now = mstime();
  size_t keys_loaded = 0;

  auto cleanup = absl::Cleanup([&] { FinishLoad(start, &keys_loaded); });

  while (!stop_early_.load(memory_order_relaxed)) {
    /* Read type. */
    SET_OR_RETURN(FetchType(), type);

    DVLOG(2) << "Opcode type: " << type;

    /* Handle special types. */
    if (type == RDB_OPCODE_EXPIRETIME) {
      LOG(ERROR) << "opcode RDB_OPCODE_EXPIRETIME not supported";

      return RdbError(errc::invalid_encoding);
    }

    if (type == RDB_OPCODE_EXPIRETIME_MS) {
      int64_t val;
      /* EXPIRETIME_MS: milliseconds precision expire times introduced
       * with RDB v3. Like EXPIRETIME but no with more precision. */
      SET_OR_RETURN(FetchInt<int64_t>(), val);
      settings.SetExpire(val);
      continue; /* Read next opcode. */
    }

    if (type == RDB_OPCODE_FREQ) {
      /* FREQ: LFU frequency. */
      FetchInt<uint8_t>();  // IGNORE
      continue;             /* Read next opcode. */
    }

    if (type == RDB_OPCODE_IDLE) {
      /* IDLE: LRU idle time. */
      uint64_t idle;
      SET_OR_RETURN(LoadLen(nullptr), idle);  // ignore
      (void)idle;
      continue; /* Read next opcode. */
    }

    if (type == RDB_OPCODE_EOF) {
      /* EOF: End of file, exit the main loop. */
      break;
    }

    if (type == RDB_OPCODE_FULLSYNC_END) {
      VLOG(1) << "Read RDB_OPCODE_FULLSYNC_END";
      RETURN_ON_ERR(EnsureRead(8));
      mem_buf_->ConsumeInput(8);  // ignore 8 bytes
      if (full_sync_cut_cb)
        full_sync_cut_cb();
      continue;
    }

    if (type == RDB_OPCODE_JOURNAL_OFFSET) {
      VLOG(1) << "Read RDB_OPCODE_JOURNAL_OFFSET";
      uint64_t journal_offset;
      SET_OR_RETURN(FetchInt<uint64_t>(), journal_offset);
      VLOG(1) << "Got offset " << journal_offset;
      journal_offset_ = journal_offset;
      continue;
    }

    if (type == RDB_OPCODE_SELECTDB) {
      unsigned dbid = 0;

      /* SELECTDB: Select the specified database. */
      SET_OR_RETURN(LoadLen(nullptr), dbid);

      if (dbid > GetFlag(FLAGS_dbnum)) {
        LOG(WARNING) << "database id " << dbid << " exceeds dbnum limit. Try increasing the flag.";

        return RdbError(errc::bad_db_index);
      }

      VLOG(2) << "Select DB: " << dbid;
      for (unsigned i = 0; i < shard_set->size(); ++i) {
        // we should flush pending items before switching dbid.
        FlushShardAsync(i);

        // Active database if not existed before.
        shard_set->Add(i, [dbid] { EngineShard::tlocal()->db_slice().ActivateDb(dbid); });
      }

      cur_db_index_ = dbid;
      continue; /* Read next opcode. */
    }

    if (type == RDB_OPCODE_RESIZEDB) {
      /* RESIZEDB: Hint about the size of the keys in the currently
       * selected data base, in order to avoid useless rehashing. */
      uint64_t db_size, expires_size;
      SET_OR_RETURN(LoadLen(nullptr), db_size);
      SET_OR_RETURN(LoadLen(nullptr), expires_size);

      ResizeDb(db_size, expires_size);
      continue; /* Read next opcode. */
    }

    if (type == RDB_OPCODE_AUX) {
      RETURN_ON_ERR(HandleAux());
      continue; /* Read type again. */
    }

    if (type == RDB_OPCODE_MODULE_AUX) {
      LOG(ERROR) << "Modules are not supported";
      return RdbError(errc::feature_not_supported);
    }

    if (type == RDB_OPCODE_COMPRESSED_ZSTD_BLOB_START ||
        type == RDB_OPCODE_COMPRESSED_LZ4_BLOB_START) {
      RETURN_ON_ERR(HandleCompressedBlob(type));
      continue;
    }

    if (type == RDB_OPCODE_COMPRESSED_BLOB_END) {
      RETURN_ON_ERR(HandleCompressedBlobFinish());
      continue;
    }

    if (type == RDB_OPCODE_JOURNAL_BLOB) {
      // We should flush all changes on the current db before applying incremental changes.
      for (unsigned i = 0; i < shard_set->size(); ++i) {
        FlushShardAsync(i);
      }
      RETURN_ON_ERR(HandleJournalBlob(service_));
      continue;
    }

    if (!rdbIsObjectTypeDF(type)) {
      return RdbError(errc::invalid_rdb_type);
    }

    ++keys_loaded;
    RETURN_ON_ERR(LoadKeyValPair(type, &settings));
    settings.Reset();
  }  // main load loop

  DVLOG(1) << "RdbLoad loop finished";

  if (stop_early_) {
    return *ec_;
  }

  /* Verify the checksum if RDB version is >= 5 */
  RETURN_ON_ERR(VerifyChecksum());

  return kOk;
}

void RdbLoader::FinishLoad(absl::Time start_time, size_t* keys_loaded) {
  BlockingCounter bc(shard_set->size());
  for (unsigned i = 0; i < shard_set->size(); ++i) {
    // Flush the remaining items.
    FlushShardAsync(i);

    // Send sentinel callbacks to ensure that all previous messages have been processed.
    shard_set->Add(i, [bc]() mutable { bc.Dec(); });
  }
  bc.Wait();  // wait for sentinels to report.

  absl::Duration dur = absl::Now() - start_time;
  load_time_ = double(absl::ToInt64Milliseconds(dur)) / 1000;
  keys_loaded_ = *keys_loaded;
}

std::error_code RdbLoaderBase::EnsureRead(size_t min_sz) {
  // In the flow of reading compressed data, we store the uncompressed data to in uncompressed
  // buffer. When parsing entries we call ensure read with 9 bytes to read the length of
  // key/value. If the key/value is very small (less than 9 bytes) the remainded data in
  // uncompressed buffer might contain less than 9 bytes. We need to make sure that we dont read
  // from sink to the uncompressed buffer and therefor in this flow we return here.
  if (mem_buf_ != &origin_mem_buf_)
    return std::error_code{};
  if (mem_buf_->InputLen() >= min_sz)
    return std::error_code{};
  return EnsureReadInternal(min_sz);
}

error_code RdbLoaderBase::EnsureReadInternal(size_t min_sz) {
  DCHECK_LT(mem_buf_->InputLen(), min_sz);

  auto out_buf = mem_buf_->AppendBuffer();
  CHECK_GT(out_buf.size(), min_sz);

  // If limit was applied we do not want to read more than needed
  // important when reading from sockets.
  if (bytes_read_ + out_buf.size() > source_limit_) {
    out_buf = out_buf.subspan(0, source_limit_ - bytes_read_);
  }

  io::Result<size_t> res = src_->ReadAtLeast(out_buf, min_sz);
  if (!res)
    return res.error();

  if (*res < min_sz)
    return RdbError(errc::rdb_file_corrupted);

  bytes_read_ += *res;

  DCHECK_LE(bytes_read_, source_limit_);
  mem_buf_->CommitWrite(*res);

  return kOk;
}

io::Result<uint64_t> RdbLoaderBase::LoadLen(bool* is_encoded) {
  if (is_encoded)
    *is_encoded = false;

  // Every RDB file with rdbver >= 5 has 8-bytes checksum at the end,
  // so we can ensure we have 9 bytes to read up until that point.
  error_code ec = EnsureRead(9);
  if (ec)
    return make_unexpected(ec);

  // Read integer meta info.
  auto bytes = mem_buf_->InputBuffer();
  PackedUIntMeta meta{bytes[0]};
  bytes.remove_prefix(1);

  // Read integer.
  uint64_t res;
  SET_OR_UNEXPECT(ReadPackedUInt(meta, bytes), res);

  if (meta.Type() == RDB_ENCVAL && is_encoded)
    *is_encoded = true;

  mem_buf_->ConsumeInput(1 + meta.ByteSize());

  return res;
}

void RdbLoaderBase::AllocateDecompressOnce(int op_type) {
  if (decompress_impl_) {
    return;
  }
  if (op_type == RDB_OPCODE_COMPRESSED_ZSTD_BLOB_START) {
    decompress_impl_.reset(new ZstdDecompress());
  } else if (op_type == RDB_OPCODE_COMPRESSED_LZ4_BLOB_START) {
    decompress_impl_.reset(new Lz4Decompress());
  } else {
    CHECK(false) << "Decompressor allocation should not be done";
  }
}

error_code RdbLoaderBase::HandleCompressedBlob(int op_type) {
  AllocateDecompressOnce(op_type);
  // Fetch uncompress blob
  string res;
  SET_OR_RETURN(FetchGenericString(), res);

  // Decompress blob and switch membuf pointer
  // Last type in the compressed blob is RDB_OPCODE_COMPRESSED_BLOB_END
  // in which we will switch back to the origin membuf (HandleCompressedBlobFinish)
  SET_OR_RETURN(decompress_impl_->Decompress(res), mem_buf_);

  return kOk;
}

error_code RdbLoaderBase::HandleCompressedBlobFinish() {
  CHECK_NE(&origin_mem_buf_, mem_buf_);
  CHECK_EQ(mem_buf_->InputLen(), size_t(0));
  mem_buf_ = &origin_mem_buf_;
  return kOk;
}

error_code RdbLoaderBase::HandleJournalBlob(Service* service) {
  // Read the number of entries in the journal blob.
  size_t num_entries;
  bool _encoded;
  SET_OR_RETURN(LoadLen(&_encoded), num_entries);

  // Read the journal blob.
  string journal_blob;
  SET_OR_RETURN(FetchGenericString(), journal_blob);

  io::BytesSource bs{io::Buffer(journal_blob)};
  journal_reader_.SetSource(&bs);

  // Parse and exectue in loop.
  size_t done = 0;
  JournalExecutor ex{service};
  while (done < num_entries) {
    journal::ParsedEntry entry{};
    SET_OR_RETURN(journal_reader_.ReadEntry(), entry);
    done++;

    // EXEC entries are just for preserving atomicity of transactions. We don't create
    // transactions and we don't care about atomicity when we're loading an RDB, so skip them.
    // Currently rdb_save also filters those records out, but we filter them additionally here
    // for better forward compatibility if we decide to change that.
    if (entry.opcode == journal::Op::EXEC)
      continue;

    if (entry.cmd.cmd_args.empty())
      return RdbError(errc::rdb_file_corrupted);

    if (absl::EqualsIgnoreCase(facade::ToSV(entry.cmd.cmd_args[0]), "FLUSHALL") ||
        absl::EqualsIgnoreCase(facade::ToSV(entry.cmd.cmd_args[0]), "FLUSHDB")) {
      // Applying a flush* operation in the middle of a load can cause out-of-sync deletions of
      // data that should not be deleted, see https://github.com/dragonflydb/dragonfly/issues/1231
      // By returning an error we are effectively restarting the replication.
      return RdbError(errc::unsupported_operation);
    }

    DVLOG(2) << "Executing item: " << entry.ToString();
    ex.Execute(entry.dbid, entry.cmd);
  }

  return std::error_code{};
}

error_code RdbLoader::HandleAux() {
  /* AUX: generic string-string fields. Use to add state to RDB
   * which is backward compatible. Implementations of RDB loading
   * are required to skip AUX fields they don't understand.
   *
   * An AUX field is composed of two strings: key and value. */
  string auxkey, auxval;

  SET_OR_RETURN(FetchGenericString(), auxkey);
  SET_OR_RETURN(FetchGenericString(), auxval);

  if (!auxkey.empty() && auxkey[0] == '%') {
    /* All the fields with a name staring with '%' are considered
     * information fields and are logged at startup with a log
     * level of NOTICE. */
    LOG(INFO) << "RDB '" << auxkey << "': " << auxval;
  } else if (auxkey == "repl-stream-db") {
    // TODO
  } else if (auxkey == "repl-id") {
    // TODO
  } else if (auxkey == "repl-offset") {
    // TODO
  } else if (auxkey == "lua") {
    LoadScriptFromAux(move(auxval));
  } else if (auxkey == "redis-ver") {
    VLOG(1) << "Loading RDB produced by version " << auxval;
  } else if (auxkey == "ctime") {
    int64_t ctime;
    if (absl::SimpleAtoi(auxval, &ctime)) {
      time_t age = time(NULL) - ctime;
      if (age < 0)
        age = 0;
      VLOG(1) << "RDB age " << strings::HumanReadableElapsedTime(age);
    }
  } else if (auxkey == "used-mem") {
    long long usedmem;
    if (absl::SimpleAtoi(auxval, &usedmem)) {
      VLOG(1) << "RDB memory usage when created " << strings::HumanReadableNumBytes(usedmem);
    }
  } else if (auxkey == "aof-preamble") {
    long long haspreamble;
    if (absl::SimpleAtoi(auxval, &haspreamble) && haspreamble) {
      VLOG(1) << "RDB has an AOF tail";
    }
  } else if (auxkey == "redis-bits") {
    /* Just ignored. */
  } else if (auxkey == "search-index") {
    LoadSearchIndexDefFromAux(move(auxval));
  } else {
    /* We ignore fields we don't understand, as by AUX field
     * contract. */
    LOG(WARNING) << "Unrecognized RDB AUX field: '" << auxkey << "'";
  }

  return kOk;
}

error_code RdbLoader::VerifyChecksum() {
  uint64_t expected;

  SET_OR_RETURN(FetchInt<uint64_t>(), expected);

  io::Bytes cur_buf = mem_buf_->InputBuffer();

  VLOG(1) << "VerifyChecksum: input buffer len " << cur_buf.size() << ", expected " << expected;

  return kOk;
}

void RdbLoader::FlushShardAsync(ShardId sid) {
  auto& out_buf = shard_buf_[sid];
  if (out_buf.empty())
    return;

  auto cb = [indx = this->cur_db_index_, this, ib = std::move(out_buf)] {
    this->LoadItemsBuffer(indx, ib);
  };

  shard_set->Add(sid, std::move(cb));
}

std::error_code RdbLoaderBase::FromOpaque(const OpaqueObj& opaque, CompactObj* pv) {
  OpaqueObjLoader visitor(opaque.rdb_type, pv);
  std::visit(visitor, opaque.obj);

  return visitor.ec();
}

void RdbLoader::LoadItemsBuffer(DbIndex db_ind, const ItemsBuf& ib) {
  DbSlice& db_slice = EngineShard::tlocal()->db_slice();
  DbContext db_cntx{.db_index = db_ind, .time_now_ms = GetCurrentTimeMs()};

  for (const auto* item : ib) {
    PrimeValue pv;
    if (ec_ = FromOpaque(item->val, &pv); ec_) {
      LOG(ERROR) << "Could not load value for key '" << item->key << "' in DB " << db_ind;
      stop_early_ = true;
      break;
    }

    if (item->expire_ms > 0 && db_cntx.time_now_ms >= item->expire_ms)
      continue;

    try {
      auto [it, added] = db_slice.AddOrUpdate(db_cntx, item->key, std::move(pv), item->expire_ms);
      if (!added) {
        LOG(WARNING) << "RDB has duplicated key '" << item->key << "' in DB " << db_ind;
      }
    } catch (const std::bad_alloc&) {
      LOG(ERROR) << "OOM failed to add key '" << item->key << "' in DB " << db_ind;
      ec_ = RdbError(errc::out_of_memory);
      stop_early_ = true;
      break;
    }
  }

  for (auto* item : ib) {
    item_queue_.Push(item);
  }
}

void RdbLoader::ResizeDb(size_t key_num, size_t expire_num) {
  DCHECK_LT(key_num, 1U << 31);
  DCHECK_LT(expire_num, 1U << 31);
  // Note: To reserve space, it's necessary to allocate space at the shard level. We might
  // load with different number of shards which makes database resizing unfeasible.
}

error_code RdbLoader::LoadKeyValPair(int type, ObjSettings* settings) {
  // We return the item in LoadItemsBuffer.
  Item* item = item_queue_.Pop();

  if (item == nullptr) {
    item = new Item;
  }
  auto cleanup = absl::Cleanup([item] { delete item; });

  // Read key
  SET_OR_RETURN(ReadKey(), item->key);

  // Read value
  error_code ec = ReadObj(type, &item->val);
  if (ec) {
    VLOG(1) << "ReadObj error " << ec << " for key " << item->key;
    return ec;
  }

  /* Check if the key already expired. This function is used when loading
   * an RDB file from disk, either at startup, or when an RDB was
   * received from the master. In the latter case, the master is
   * responsible for key expiry. If we would expire keys here, the
   * snapshot taken by the master may not be reflected on the slave.
   * Similarly if the RDB is the preamble of an AOF file, we want to
   * load all the keys as they are, since the log of operations later
   * assume to work in an exact keyspace state. */

  if (ServerState::tlocal()->is_master && settings->has_expired) {
    VLOG(2) << "Expire key: " << item->key;
    return kOk;
  }

  ShardId sid = Shard(item->key, shard_set->size());
  item->expire_ms = settings->expiretime;

  auto& out_buf = shard_buf_[sid];

  out_buf.emplace_back(item);
  std::move(cleanup).Cancel();

  constexpr size_t kBufSize = 128;
  if (out_buf.size() >= kBufSize) {
    FlushShardAsync(sid);
  }

  return kOk;
}

void RdbLoader::LoadScriptFromAux(string&& body) {
  ServerState* ss = ServerState::tlocal();
  auto interpreter = ss->BorrowInterpreter();
  absl::Cleanup clean = [ss, interpreter]() { ss->ReturnInterpreter(interpreter); };

  if (script_mgr_) {
    auto res = script_mgr_->Insert(body, interpreter);
    if (!res)
      LOG(ERROR) << "Error compiling script";
  }
}

void RdbLoader::LoadSearchIndexDefFromAux(string&& def) {
  facade::CapturingReplyBuilder crb{};
  ConnectionContext cntx{nullptr, nullptr, &crb};
  cntx.journal_emulated = true;
  cntx.skip_acl_validation = true;

  absl::Cleanup cntx_clean = [&cntx] { cntx.Inject(nullptr); };

  uint32_t consumed = 0;
  facade::RespVec resp_vec;
  facade::RedisParser parser;

  def += "\r\n";  // RESP terminator
  absl::Span<uint8_t> buffer{reinterpret_cast<uint8_t*>(def.data()), def.size()};
  auto res = parser.Parse(buffer, &consumed, &resp_vec);

  if (res != facade::RedisParser::Result::OK) {
    LOG(ERROR) << "Bad index definition: " << def;
    return;
  }

  CmdArgVec arg_vec;
  facade::RespExpr::VecToArgList(resp_vec, &arg_vec);

  string ft_create = "FT.CREATE";
  arg_vec.insert(arg_vec.begin(), MutableSlice{ft_create.data(), ft_create.size()});

  service_->DispatchCommand(absl::MakeSpan(arg_vec), &cntx);

  auto response = crb.Take();
  if (auto err = facade::CapturingReplyBuilder::GetError(response); err) {
    LOG(ERROR) << "Bad index definition: " << def << " " << err->first;
  }
}

}  // namespace dfly
