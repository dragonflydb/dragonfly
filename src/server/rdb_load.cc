// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load.h"

#include "absl/strings/escaping.h"
#include "server/tiered_storage.h"

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/lzfP.h" /* LZF compression library */
#include "redis/quicklist.h"
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

#include <cstring>

#include "base/endian.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/bloom.h"
#include "core/json/json_object.h"
#include "core/qlist.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "server/cluster/cluster_family.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/hset_family.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_extensions.h"
#include "server/script_mgr.h"
#include "server/search/doc_index.h"
#include "server/serializer_commons.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "server/stream_family.h"
#include "server/transaction.h"
#include "strings/human_readable.h"

ABSL_DECLARE_FLAG(int32_t, list_max_listpack_size);
ABSL_DECLARE_FLAG(int32_t, list_compress_depth);
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(bool, list_experimental_v2);
namespace dfly {

using namespace std;
using base::IoBuf;
using nonstd::make_unexpected;
using namespace util;
using absl::GetFlag;
using rdb::errc;
using namespace tiering::literals;

namespace {

// Maximum length of each LoadTrace segment.
//
// Note kMaxBlobLen must be a multiple of 6 to avoid truncating elements
// containing 2 or 3 items.
constexpr size_t kMaxBlobLen = 4092;

inline auto Unexpected(errc ev) {
  return make_unexpected(RdbError(ev));
}

static const error_code kOk;

struct ZiplistCbArgs {
  long count = 0;
  absl::flat_hash_set<string_view> fields;
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

  if (data->fields.empty()) {
    data->fields.reserve(head_count / 2);
  }

  if (!ziplistGet(p, &str, &slen, &vll))
    return 0;

  /* Even records are field names, add to dict and check that's not a dup */
  if (((data->count) & 1) == 0) {
    sds field = str ? sdsnewlen(str, slen) : sdsfromlonglong(vll);
    auto [_, inserted] = data->fields.emplace(field, sdslen(field));
    if (!inserted) {
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
  ZiplistCbArgs data;
  data.lp = lp;

  int ret = ziplistValidateIntegrity(const_cast<uint8_t*>(zl), size, 1,
                                     ziplistPairsEntryConvertAndValidate, &data);

  /* make sure we have an even number of records. */
  if (data.count & 1)
    ret = 0;

  for (auto field : data.fields) {
    sdsfree((sds)field.data());
  }
  return ret;
}

string ModuleTypeName(uint64_t module_id) {
  static const char ModuleNameSet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz"
      "0123456789-_";

  char name[10];

  name[9] = '\0';
  char* p = name + 8;
  module_id >>= 10;
  for (int j = 0; j < 9; j++) {
    *p-- = ModuleNameSet[module_id & 63];
    module_id >>= 6;
  }

  return string{name};
}

bool RdbTypeAllowedEmpty(int type) {
  return type == RDB_TYPE_STRING || type == RDB_TYPE_JSON || type == RDB_TYPE_SBF ||
         type == RDB_TYPE_STREAM_LISTPACKS || type == RDB_TYPE_SET_WITH_EXPIRY ||
         type == RDB_TYPE_HASH_WITH_EXPIRY;
}

}  // namespace

class RdbLoaderBase::OpaqueObjLoader {
 public:
  OpaqueObjLoader(int rdb_type, PrimeValue* pv, LoadConfig config)
      : rdb_type_(rdb_type), pv_(pv), config_(config) {
  }

  void operator()(long long val) {
    pv_->SetInt(val);
  }

  void operator()(const base::PODArray<char>& str);
  void operator()(const LzfString& lzfstr);
  void operator()(const unique_ptr<LoadTrace>& ptr);
  void operator()(const RdbSBF& src);

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

  // Returns whether pv_ has the given object type and encoding. If not ec_
  // is set to the error.
  bool EnsureObjEncoding(CompactObjType type, unsigned encoding);

  template <typename F> static void Iterate(const LoadTrace& ltrace, F&& f) {
    for (const auto& blob : ltrace.arr) {
      if (!f(blob)) {
        return;
      }
    }
  }

  std::error_code ec_;
  int rdb_type_;
  base::PODArray<char> tset_blob_;
  PrimeValue* pv_;
  LoadConfig config_;
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
    case RDB_TYPE_SET_WITH_EXPIRY:
      CreateSet(ptr.get());
      break;
    case RDB_TYPE_HASH:
    case RDB_TYPE_HASH_WITH_EXPIRY:
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
    case RDB_TYPE_STREAM_LISTPACKS_2:
    case RDB_TYPE_STREAM_LISTPACKS_3:
      CreateStream(ptr.get());
      break;
    default:
      LOG(FATAL) << "Unsupported rdb type " << rdb_type_;
  }
}

void RdbLoaderBase::OpaqueObjLoader::operator()(const RdbSBF& src) {
  SBF* sbf =
      CompactObj::AllocateMR<SBF>(src.grow_factor, src.fp_prob, src.max_capacity, src.prev_size,
                                  src.current_size, CompactObj::memory_resource());
  for (unsigned i = 0; i < src.filters.size(); ++i) {
    sbf->AddFilter(src.filters[i].blob, src.filters[i].hash_cnt);
  }
  pv_->SetSBF(sbf);
}

void RdbLoaderBase::OpaqueObjLoader::CreateSet(const LoadTrace* ltrace) {
  size_t len = ltrace->arr.size();

  bool is_intset = true;
  if (!config_.streamed && rdb_type_ == RDB_TYPE_HASH &&
      ltrace->arr.size() <= SetFamily::MaxIntsetEntries()) {
    Iterate(*ltrace, [&](const LoadBlob& blob) {
      if (!holds_alternative<long long>(blob.rdb_var)) {
        is_intset = false;
        return false;
      }
      return true;
    });
  } else {
    /* Use a regular set when there are too many entries, or when the
     * set is being streamed. */
    is_intset = false;
  }

  sds sdsele = nullptr;
  void* inner_obj = nullptr;

  auto cleanup = absl::MakeCleanup([&] {
    if (sdsele)
      sdsfree(sdsele);
    if (inner_obj) {
      if (is_intset) {
        zfree(inner_obj);
      } else {
        CompactObj::DeleteMR<StringSet>(inner_obj);
      }
    }
  });

  if (is_intset) {
    inner_obj = intsetNew();

    long long llval;
    Iterate(*ltrace, [&](const LoadBlob& blob) {
      llval = get<long long>(blob.rdb_var);
      uint8_t success;
      inner_obj = intsetAdd((intset*)inner_obj, llval, &success);
      if (!success) {
        LOG(ERROR) << "Duplicate set members detected";
        ec_ = RdbError(errc::duplicate_key);
        return false;
      }
      return true;
    });
  } else {
    StringSet* set;
    if (config_.append) {
      // Note we always use StringSet when the object is being streamed.
      if (!EnsureObjEncoding(OBJ_SET, kEncodingStrMap2)) {
        return;
      }
      set = static_cast<StringSet*>(pv_->RObjPtr());
    } else {
      set = CompactObj::AllocateMR<StringSet>();
      set->set_time(MemberTimeSeconds(GetCurrentTimeMs()));
      inner_obj = set;

      // Expand the set up front to avoid rehashing.
      set->Reserve((config_.reserve > len) ? config_.reserve : len);
    }

    size_t increment = 1;
    if (rdb_type_ == RDB_TYPE_SET_WITH_EXPIRY) {
      increment = 2;
    }

    for (size_t i = 0; i < ltrace->arr.size(); i += increment) {
      string_view element = ToSV(ltrace->arr[i].rdb_var);

      uint32_t ttl_sec = UINT32_MAX;
      if (increment == 2) {
        int64_t ttl_time = -1;
        string_view ttl_str = ToSV(ltrace->arr[i + 1].rdb_var);
        if (!absl::SimpleAtoi(ttl_str, &ttl_time)) {
          LOG(ERROR) << "Can't parse set TTL " << ttl_str;
          ec_ = RdbError(errc::rdb_file_corrupted);
          return;
        }

        if (ttl_time != -1) {
          if (ttl_time < set->time_now()) {
            continue;
          }

          ttl_sec = ttl_time - set->time_now();
        }
      }
      if (!set->Add(element, ttl_sec)) {
        LOG(ERROR) << "Duplicate set members detected";
        ec_ = RdbError(errc::duplicate_key);
        return;
      }
    }
  }

  if (ec_)
    return;

  if (!config_.append) {
    pv_->InitRobj(OBJ_SET, is_intset ? kEncodingIntSet : kEncodingStrMap2, inner_obj);
  }
  std::move(cleanup).Cancel();
}

void RdbLoaderBase::OpaqueObjLoader::CreateHMap(const LoadTrace* ltrace) {
  size_t increment = 2;
  if (rdb_type_ == RDB_TYPE_HASH_WITH_EXPIRY)
    increment = 3;

  size_t len = ltrace->arr.size() / increment;

  /* Too many entries? Use a hash table right from the start. */
  bool keep_lp = !config_.streamed && (len <= 64) && (rdb_type_ != RDB_TYPE_HASH_WITH_EXPIRY);

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

    CHECK(ltrace->arr.size() % 2 == 0);
    for (size_t i = 0; i < ltrace->arr.size(); i += 2) {
      /* Add pair to listpack */
      string_view sv = ToSV(ltrace->arr[i].rdb_var);
      lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());

      sv = ToSV(ltrace->arr[i + 1].rdb_var);
      lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    }

    if (ec_) {
      lpFree(lp);
      return;
    }

    lp = lpShrinkToFit(lp);
    pv_->InitRobj(OBJ_HASH, kEncodingListPack, lp);
  } else {
    StringMap* string_map;
    if (config_.append) {
      // Note we always use StringMap when the object is being streamed.
      if (!EnsureObjEncoding(OBJ_HASH, kEncodingStrMap2)) {
        return;
      }

      string_map = static_cast<StringMap*>(pv_->RObjPtr());
    } else {
      string_map = CompactObj::AllocateMR<StringMap>();
      string_map->set_time(MemberTimeSeconds(GetCurrentTimeMs()));

      // Expand the map up front to avoid rehashing.
      string_map->Reserve((config_.reserve > len) ? config_.reserve : len);
    }

    auto cleanup = absl::MakeCleanup([&] {
      if (!config_.append) {
        CompactObj::DeleteMR<StringMap>(string_map);
      }
    });
    std::string key;
    for (size_t i = 0; i < ltrace->arr.size(); i += increment) {
      // ToSV may reference an internal buffer, therefore we can use only before the
      // next call to ToSV. To workaround, copy the key locally.
      key = ToSV(ltrace->arr[i].rdb_var);
      string_view val = ToSV(ltrace->arr[i + 1].rdb_var);

      if (ec_)
        return;

      uint32_t ttl_sec = UINT32_MAX;
      if (increment == 3) {
        int64_t ttl_time = -1;
        string_view ttl_str = ToSV(ltrace->arr[i + 2].rdb_var);
        if (!absl::SimpleAtoi(ttl_str, &ttl_time)) {
          LOG(ERROR) << "Can't parse hashmap TTL for " << key << ", ttl='" << ttl_str
                     << "', val=" << val;
          ec_ = RdbError(errc::rdb_file_corrupted);
          return;
        }

        if (ttl_time != -1) {
          if (ttl_time < string_map->time_now()) {
            continue;
          }

          ttl_sec = ttl_time - string_map->time_now();
        }
      }

      if (!string_map->AddOrSkip(key, val, ttl_sec)) {
        LOG(ERROR) << "Duplicate hash fields detected for field " << key;
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }
    }
    if (!config_.append) {
      pv_->InitRobj(OBJ_HASH, kEncodingStrMap2, string_map);
    }
    std::move(cleanup).Cancel();
  }
}

void RdbLoaderBase::OpaqueObjLoader::CreateList(const LoadTrace* ltrace) {
  quicklist* ql = nullptr;
  QList* qlv2 = nullptr;
  if (config_.append) {
    if (pv_->ObjType() != OBJ_LIST) {
      ec_ = RdbError(errc::invalid_rdb_type);
      return;
    }
    if (pv_->Encoding() == OBJ_ENCODING_QUICKLIST) {
      ql = static_cast<quicklist*>(pv_->RObjPtr());
    } else {
      DCHECK_EQ(pv_->Encoding(), kEncodingQL2);
      qlv2 = static_cast<QList*>(pv_->RObjPtr());
    }
  } else {
    if (absl::GetFlag(FLAGS_list_experimental_v2)) {
      qlv2 = CompactObj::AllocateMR<QList>(GetFlag(FLAGS_list_max_listpack_size),
                                           GetFlag(FLAGS_list_compress_depth));
    } else {
      ql = quicklistNew(GetFlag(FLAGS_list_max_listpack_size), GetFlag(FLAGS_list_compress_depth));
    }
  }

  auto cleanup = absl::Cleanup([&] {
    if (!config_.append) {
      if (ql)
        quicklistRelease(ql);
      else
        CompactObj::DeleteMR<QList>(qlv2);
    }
  });

  Iterate(*ltrace, [&](const LoadBlob& blob) {
    unsigned container = blob.encoding;
    string_view sv = ToSV(blob.rdb_var);

    if (ec_)
      return false;

    uint8_t* lp = nullptr;
    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
      lp = (uint8_t*)zmalloc(sv.size());
      ::memcpy(lp, (uint8_t*)sv.data(), sv.size());
      if (ql)
        quicklistAppendPlainNode(ql, lp, sv.size());
      else
        qlv2->AppendPlain(lp, sv.size());

      return true;
    }

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
        LOG(ERROR) << "Ziplist integrity check failed: " << sv.size();
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

    if (ql)
      quicklistAppendListpack(ql, lp);
    else
      qlv2->AppendListpack(lp);
    return true;
  });

  if (ec_)
    return;
  if ((ql && quicklistCount(ql) == 0) || (qlv2 && qlv2->Size() == 0)) {
    ec_ = RdbError(errc::empty_key);
    return;
  }

  std::move(cleanup).Cancel();

  if (!config_.append) {
    if (ql)
      pv_->InitRobj(OBJ_LIST, OBJ_ENCODING_QUICKLIST, ql);
    else
      pv_->InitRobj(OBJ_LIST, kEncodingQL2, qlv2);
  }
}

void RdbLoaderBase::OpaqueObjLoader::CreateZSet(const LoadTrace* ltrace) {
  size_t zsetlen = ltrace->arr.size();

  unsigned encoding = OBJ_ENCODING_SKIPLIST;
  detail::SortedMap* zs;
  if (config_.append) {
    // Note we always use SortedMap when the object is being streamed.
    if (!EnsureObjEncoding(OBJ_ZSET, OBJ_ENCODING_SKIPLIST)) {
      return;
    }

    zs = static_cast<detail::SortedMap*>(pv_->RObjPtr());
  } else {
    zs = CompactObj::AllocateMR<detail::SortedMap>();

    size_t reserve = (config_.reserve > zsetlen) ? config_.reserve : zsetlen;
    if (reserve > 2 && !zs->Reserve(reserve)) {
      LOG(ERROR) << "OOM in dictTryExpand " << zsetlen;
      ec_ = RdbError(errc::out_of_memory);
      return;
    }
  }

  auto cleanup = absl::MakeCleanup([&] {
    if (!config_.append) {
      CompactObj::DeleteMR<detail::SortedMap>(zs);
    }
  });

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
  if (!config_.streamed && zs->Size() <= server.zset_max_listpack_entries &&
      maxelelen <= server.zset_max_listpack_value && lpSafeToAdd(NULL, totelelen)) {
    encoding = OBJ_ENCODING_LISTPACK;
    inner = zs->ToListPack();
    CompactObj::DeleteMR<detail::SortedMap>(zs);
  }

  std::move(cleanup).Cancel();

  if (!config_.append) {
    pv_->InitRobj(OBJ_ZSET, encoding, inner);
  }
}

void RdbLoaderBase::OpaqueObjLoader::CreateStream(const LoadTrace* ltrace) {
  stream* s;
  StreamMemTracker mem_tracker;
  if (config_.append) {
    if (!EnsureObjEncoding(OBJ_STREAM, OBJ_ENCODING_STREAM)) {
      return;
    }

    s = static_cast<stream*>(pv_->RObjPtr());
  } else {
    s = streamNew();
  }

  auto cleanup = absl::Cleanup([&] {
    if (config_.append) {
      freeStream(s);
    }
  });

  for (size_t i = 0; i < ltrace->arr.size(); i += 2) {
    string_view nodekey = ToSV(ltrace->arr[i].rdb_var);
    string_view data = ToSV(ltrace->arr[i + 1].rdb_var);

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

  // We only load the stream metadata and consumer groups (stream_trace) on
  // the final read (when reading the stream in increments). Therefore if
  // stream_trace is null add the partial stream, then stream_trace will be
  // loaded later.
  if (!ltrace->stream_trace) {
    if (!config_.append) {
      pv_->InitRobj(OBJ_STREAM, OBJ_ENCODING_STREAM, s);
    }
    std::move(cleanup).Cancel();
    return;
  }

  s->length = ltrace->stream_trace->stream_len;
  CopyStreamId(ltrace->stream_trace->last_id, &s->last_id);
  CopyStreamId(ltrace->stream_trace->first_id, &s->first_id);
  CopyStreamId(ltrace->stream_trace->max_deleted_entry_id, &s->max_deleted_entry_id);
  s->entries_added = ltrace->stream_trace->entries_added;

  if (rdb_type_ == RDB_TYPE_STREAM_LISTPACKS) {
    /* Since the rax is already loaded, we can find the first entry's
     * ID. */
    streamGetEdgeID(s, 1, 1, &s->first_id);
  }

  for (const auto& cg : ltrace->stream_trace->cgroup) {
    string_view cgname = ToSV(cg.name);
    streamID cg_id;
    cg_id.ms = cg.ms;
    cg_id.seq = cg.seq;

    uint64_t entries_read = cg.entries_read;
    if (rdb_type_ == RDB_TYPE_STREAM_LISTPACKS) {
      entries_read = streamEstimateDistanceFromFirstEverEntry(s, &cg_id);
    }

    streamCG* cgroup = streamCreateCG(s, cgname.data(), cgname.size(), &cg_id, entries_read);
    if (cgroup == NULL) {
      LOG(ERROR) << "Duplicated consumer group name " << cgname;
      ec_ = RdbError(errc::duplicate_key);
      return;
    }

    for (const auto& pel : cg.pel_arr) {
      streamNACK* nack = reinterpret_cast<streamNACK*>(zmalloc(sizeof(*nack)));
      nack->delivery_time = pel.delivery_time;
      nack->delivery_count = pel.delivery_count;
      nack->consumer = nullptr;

      if (!raxTryInsert(cgroup->pel, const_cast<uint8_t*>(pel.rawid.data()), pel.rawid.size(), nack,
                        NULL)) {
        LOG(ERROR) << "Duplicated global PEL entry loading stream consumer group";
        ec_ = RdbError(errc::duplicate_key);
        streamFreeNACK(nack);
        return;
      }
    }

    for (const auto& cons : cg.cons_arr) {
      streamConsumer* consumer = StreamCreateConsumer(cgroup, ToSV(cons.name), cons.seen_time,
                                                      SCC_NO_NOTIFY | SCC_NO_DIRTIFY);
      if (!consumer) {
        LOG(ERROR) << "Duplicate stream consumer detected.";
        ec_ = RdbError(errc::duplicate_key);
        return;
      }

      consumer->active_time = cons.active_time;
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
  if (!config_.append) {
    pv_->InitRobj(OBJ_STREAM, OBJ_ENCODING_STREAM, s);
  }
  mem_tracker.UpdateStreamSize(*pv_);
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

    if (len > SetFamily::MaxIntsetEntries()) {
      StringSet* set = SetFamily::ConvertToStrSet(is, len);

      if (!set) {
        LOG(ERROR) << "OOM in ConvertToStrSet " << len;
        ec_ = RdbError(errc::out_of_memory);
        return;
      }
      pv_->InitRobj(OBJ_SET, kEncodingStrMap2, set);
    } else {
      intset* mine = (intset*)zmalloc(blob.size());
      ::memcpy(mine, blob.data(), blob.size());
      pv_->InitRobj(OBJ_SET, kEncodingIntSet, mine);
    }
  } else if (rdb_type_ == RDB_TYPE_SET_LISTPACK) {
    if (!lpValidateIntegrity((uint8_t*)blob.data(), blob.size(), 0, nullptr, nullptr)) {
      LOG(ERROR) << "ListPack integrity check failed.";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }

    unsigned char* lp = (unsigned char*)blob.data();
    StringSet* set = CompactObj::AllocateMR<StringSet>();
    for (unsigned char* cur = lpFirst(lp); cur != nullptr; cur = lpNext(lp, cur)) {
      unsigned char field_buf[LP_INTBUF_SIZE];
      string_view elem = container_utils::LpGetView(cur, field_buf);
      if (!set->Add(elem)) {
        LOG(ERROR) << "Duplicate member " << elem;
        ec_ = RdbError(errc::duplicate_key);
        break;
      }
    }
    if (ec_) {
      CompactObj::DeleteMR<StringSet>(set);
      return;
    }
    pv_->InitRobj(OBJ_SET, kEncodingStrMap2, set);
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
      inner = detail::SortedMap::FromListPack(CompactObj::memory_resource(), lp);
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
  } else if (rdb_type_ == RDB_TYPE_JSON) {
    size_t start_size = static_cast<MiMemoryResource*>(CompactObj::memory_resource())->used();
    {
      auto json = JsonFromString(blob, CompactObj::memory_resource());
      if (!json) {
        ec_ = RdbError(errc::bad_json_string);
      }
      pv_->SetJson(std::move(*json));
    }
    size_t end_size = static_cast<MiMemoryResource*>(CompactObj::memory_resource())->used();
    DCHECK(end_size > start_size);
    pv_->SetJsonSize(end_size - start_size);
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

bool RdbLoaderBase::OpaqueObjLoader::EnsureObjEncoding(CompactObjType type, unsigned encoding) {
  if (pv_->ObjType() != type) {
    LOG(DFATAL) << "Invalid RDB type " << pv_->ObjType() << "; expected " << type;
    ec_ = RdbError(errc::invalid_rdb_type);
    return false;
  }
  if (pv_->Encoding() != encoding) {
    LOG(DFATAL) << "Invalid encoding " << pv_->Encoding() << "; expected " << encoding;
    ec_ = RdbError(errc::invalid_encoding);
    return false;
  }

  return true;
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
    case RDB_TYPE_SET_WITH_EXPIRY:
      iores = ReadSet(rdbtype);
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
    case RDB_TYPE_HASH_WITH_EXPIRY:
      iores = ReadHMap(rdbtype);
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
    case RDB_TYPE_STREAM_LISTPACKS_2:
    case RDB_TYPE_STREAM_LISTPACKS_3:
      iores = ReadStreams(rdbtype);
      break;
    case RDB_TYPE_JSON:
      iores = ReadJson();
      break;
    case RDB_TYPE_SET_LISTPACK:
      // We need to deal with protocol versions 9 and older because in these
      // RDB_TYPE_JSON == 20. On newer versions > 9 we bumped up RDB_TYPE_JSON to 30
      // because it overlapped with the new type RDB_TYPE_SET_LISTPACK
      if (rdb_version_ < 10) {
        // consider it RDB_TYPE_JSON_OLD (20)
        iores = ReadJson();
      } else {
        iores = ReadGeneric(rdbtype);
      }
      break;
    case RDB_TYPE_MODULE_2:
      iores = ReadRedisJson();
      break;
    case RDB_TYPE_SBF:
      iores = ReadSBF();
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

auto RdbLoaderBase::ReadSet(int rdbtype) -> io::Result<OpaqueObj> {
  size_t len;
  if (pending_read_.remaining > 0) {
    len = pending_read_.remaining;
  } else {
    SET_OR_UNEXPECT(LoadLen(NULL), len);
    if (rdbtype == RDB_TYPE_SET_WITH_EXPIRY) {
      len *= 2;
    }
    pending_read_.reserve = len;
  }

  // Limit each read to kMaxBlobLen elements.
  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  size_t n = std::min(len, kMaxBlobLen);
  load_trace->arr.resize(n);
  for (size_t i = 0; i < n; i++) {
    error_code ec = ReadStringObj(&load_trace->arr[i].rdb_var);
    if (ec) {
      return make_unexpected(ec);
    }
  }

  // If there are still unread elements, cache the number of remaining
  // elements, or clear if the full object has been read.
  if (len > n) {
    pending_read_.remaining = len - n;
  } else if (pending_read_.remaining > 0) {
    pending_read_.remaining = 0;
  }

  return OpaqueObj{std::move(load_trace), rdbtype};
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

auto RdbLoaderBase::ReadHMap(int rdbtype) -> io::Result<OpaqueObj> {
  size_t len;
  if (pending_read_.remaining > 0) {
    len = pending_read_.remaining;
  } else {
    SET_OR_UNEXPECT(LoadLen(NULL), len);

    if (rdbtype == RDB_TYPE_HASH) {
      len *= 2;
    } else {
      DCHECK_EQ(rdbtype, RDB_TYPE_HASH_WITH_EXPIRY);
      len *= 3;
    }

    pending_read_.reserve = len;
  }

  // Limit each read to kMaxBlobLen elements.
  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  size_t n = std::min<size_t>(len, kMaxBlobLen);
  load_trace->arr.resize(n);
  for (size_t i = 0; i < n; ++i) {
    error_code ec = ReadStringObj(&load_trace->arr[i].rdb_var);
    if (ec)
      return make_unexpected(ec);
  }

  // If there are still unread elements, cache the number of remaining
  // elements, or clear if the full object has been read.
  if (len > n) {
    pending_read_.remaining = len - n;
  } else if (pending_read_.remaining > 0) {
    pending_read_.remaining = 0;
  }

  return OpaqueObj{std::move(load_trace), rdbtype};
}

auto RdbLoaderBase::ReadZSet(int rdbtype) -> io::Result<OpaqueObj> {
  uint64_t zsetlen;
  if (pending_read_.remaining > 0) {
    zsetlen = pending_read_.remaining;
  } else {
    SET_OR_UNEXPECT(LoadLen(nullptr), zsetlen);
    pending_read_.reserve = zsetlen;
  }

  if (zsetlen == 0)
    return Unexpected(errc::empty_key);

  double score;

  // Limit each read to kMaxBlobLen elements.
  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  size_t n = std::min<size_t>(zsetlen, kMaxBlobLen);
  load_trace->arr.resize(n);
  for (size_t i = 0; i < n; ++i) {
    error_code ec = ReadStringObj(&load_trace->arr[i].rdb_var);
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
    load_trace->arr[i].score = score;
  }

  // If there are still unread elements, cache the number of remaining
  // elements, or clear if the full object has been read.
  if (zsetlen > n) {
    pending_read_.remaining = zsetlen - n;
  } else if (pending_read_.remaining > 0) {
    pending_read_.remaining = 0;
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
  size_t len;
  if (pending_read_.remaining > 0) {
    len = pending_read_.remaining;
  } else {
    SET_OR_UNEXPECT(LoadLen(NULL), len);
    pending_read_.reserve = len;
  }

  if (len == 0)
    return Unexpected(errc::empty_key);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  // Lists pack multiple entries into each list node (8Kb by default),
  // therefore using a smaller segment length than kMaxBlobLen.
  size_t n = std::min<size_t>(len, 512);
  load_trace->arr.resize(n);
  for (size_t i = 0; i < n; ++i) {
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
    load_trace->arr[i].rdb_var = std::move(var);
    load_trace->arr[i].encoding = container;
  }

  // If there are still unread elements, cache the number of remaining
  // elements, or clear if the full object has been read.
  if (len > n) {
    pending_read_.remaining = len - n;
  } else if (pending_read_.remaining > 0) {
    pending_read_.remaining = 0;
  }

  return OpaqueObj{std::move(load_trace), rdbtype};
}

auto RdbLoaderBase::ReadStreams(int rdbtype) -> io::Result<OpaqueObj> {
  size_t listpacks;
  if (pending_read_.remaining > 0) {
    listpacks = pending_read_.remaining;
  } else {
    SET_OR_UNEXPECT(LoadLen(NULL), listpacks);
  }

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  // Streams pack multiple entries into each stream node (4Kb or 100
  // entries), therefore using a smaller segment length than kMaxBlobLen.
  size_t n = std::min<size_t>(listpacks, 512);
  load_trace->arr.resize(n * 2);

  error_code ec;
  for (size_t i = 0; i < n; ++i) {
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

    load_trace->arr[2 * i].rdb_var = std::move(stream_id);
    load_trace->arr[2 * i + 1].rdb_var = std::move(blob);
  }

  // If there are still unread elements, cache the number of remaining
  // elements, or clear if the full object has been read.
  //
  // We only load the stream metadata and consumer groups in the final read,
  // so if there are still unread elements return the partial stream.
  if (listpacks > n) {
    pending_read_.remaining = listpacks - n;
    return OpaqueObj{std::move(load_trace), rdbtype};
  }

  pending_read_.remaining = 0;

  // Load stream metadata.
  load_trace->stream_trace.reset(new StreamTrace);

  /* Load total number of items inside the stream. */
  SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->stream_len);

  /* Load the last entry ID. */
  SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->last_id.ms);
  SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->last_id.seq);

  if (rdbtype >= RDB_TYPE_STREAM_LISTPACKS_2) {
    /* Load the first entry ID. */
    SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->first_id.ms);
    SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->first_id.seq);

    /* Load the maximal deleted entry ID. */
    SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->max_deleted_entry_id.ms);
    SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->max_deleted_entry_id.seq);

    /* Load the offset. */
    SET_OR_UNEXPECT(LoadLen(nullptr), load_trace->stream_trace->entries_added);
  } else {
    /* During migration the offset can be initialized to the stream's
     * length. At this point, we also don't care about tombstones
     * because CG offsets will be later initialized as well. */
    load_trace->stream_trace->entries_added = load_trace->stream_trace->stream_len;
  }

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

    cgroup.entries_read = 0;
    if (rdbtype >= RDB_TYPE_STREAM_LISTPACKS_2) {
      SET_OR_UNEXPECT(LoadLen(nullptr), cgroup.entries_read);
    }

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

      SET_OR_UNEXPECT(FetchInt<int64_t>(), pel.delivery_time);
      SET_OR_UNEXPECT(LoadLen(nullptr), pel.delivery_count);
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

      if (rdbtype >= RDB_TYPE_STREAM_LISTPACKS_3) {
        SET_OR_UNEXPECT(FetchInt<int64_t>(), consumer.active_time);
      } else {
        /* That's the best estimate we got */
        consumer.active_time = consumer.seen_time;
      }

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

auto RdbLoaderBase::ReadRedisJson() -> io::Result<OpaqueObj> {
  auto json_magic_number = LoadLen(nullptr);
  if (!json_magic_number) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  constexpr string_view kJsonModule = "ReJSON-RL"sv;
  string module_name = ModuleTypeName(*json_magic_number);
  if (module_name != kJsonModule) {
    LOG(ERROR) << "Unsupported module: " << module_name;
    return Unexpected(errc::unsupported_operation);
  }

  int encver = *json_magic_number & 1023;
  if (encver != 3) {
    LOG(ERROR) << "Unsupported ReJSON version: " << encver;
    return Unexpected(errc::unsupported_operation);
  }

  auto opcode = FetchInt<uint8_t>();
  if (!opcode || *opcode != RDB_MODULE_OPCODE_STRING) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  RdbVariant dest;
  error_code ec = ReadStringObj(&dest);
  if (ec) {
    return make_unexpected(ec);
  }

  opcode = FetchInt<uint8_t>();
  if (!opcode || *opcode != RDB_MODULE_OPCODE_EOF) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  return OpaqueObj{std::move(dest), RDB_TYPE_JSON};
}

auto RdbLoaderBase::ReadJson() -> io::Result<OpaqueObj> {
  RdbVariant dest;
  error_code ec = ReadStringObj(&dest);
  if (ec)
    return make_unexpected(ec);

  return OpaqueObj{std::move(dest), RDB_TYPE_JSON};
}

auto RdbLoaderBase::ReadSBF() -> io::Result<OpaqueObj> {
  RdbSBF res;
  uint64_t options;
  SET_OR_UNEXPECT(LoadLen(nullptr), options);
  if (options != 0)
    return Unexpected(errc::rdb_file_corrupted);
  SET_OR_UNEXPECT(FetchBinaryDouble(), res.grow_factor);
  SET_OR_UNEXPECT(FetchBinaryDouble(), res.fp_prob);
  if (res.fp_prob <= 0 || res.fp_prob > 0.5) {
    return Unexpected(errc::rdb_file_corrupted);
  }
  SET_OR_UNEXPECT(LoadLen(nullptr), res.prev_size);
  SET_OR_UNEXPECT(LoadLen(nullptr), res.current_size);
  SET_OR_UNEXPECT(LoadLen(nullptr), res.max_capacity);

  unsigned num_filters = 0;
  SET_OR_UNEXPECT(LoadLen(nullptr), num_filters);
  auto is_power2 = [](size_t n) { return (n & (n - 1)) == 0; };

  for (unsigned i = 0; i < num_filters; ++i) {
    unsigned hash_cnt;
    string filter_data;
    SET_OR_UNEXPECT(LoadLen(nullptr), hash_cnt);
    SET_OR_UNEXPECT(FetchGenericString(), filter_data);
    size_t bit_len = filter_data.size() * 8;
    if (!is_power2(bit_len)) {  // must be power of two
      return Unexpected(errc::rdb_file_corrupted);
    }
    res.filters.emplace_back(hash_cnt, std::move(filter_data));
  }
  return OpaqueObj{std::move(res), RDB_TYPE_SBF};
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
  uint32_t mc_flags = 0;

  bool has_expired = false;

  bool is_sticky = false;
  bool has_mc_flags = false;

  void Reset() {
    expiretime = 0;
    has_expired = false;
    is_sticky = false;
    has_mc_flags = false;
  }

  void SetExpire(int64_t val) {
    expiretime = val;
    has_expired = (val <= now);
  }

  void SetMCFlags(uint32_t flags) {
    has_mc_flags = true;
    mc_flags = flags;
  }

  ObjSettings() = default;
};

RdbLoader::RdbLoader(Service* service)
    : service_{service},
      script_mgr_{service == nullptr ? nullptr : service->script_mgr()},
      shard_buf_{shard_set->size()} {
}

RdbLoader::~RdbLoader() {
  while (true) {
    Item* item = item_queue_.Pop();
    if (item == nullptr)
      break;
    delete item;
  }

  // Decommit local memory.
  // We create an RdbLoader for each thread, so each one will Decommit for itself after
  // full sync ends (since we explicitly reset the RdbLoader).
  auto* tlocal = ServerState::tlocal();
  tlocal->DecommitMemory(ServerState::kAllMemory);
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
  settings.now = GetCurrentTimeMs();
  size_t keys_loaded = 0;

  auto cleanup = absl::Cleanup([&] { FinishLoad(start, &keys_loaded); });

  shard_set->AwaitRunningOnShardQueue([](EngineShard* es) {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetLoadInProgress(true);
  });
  while (!stop_early_.load(memory_order_relaxed)) {
    if (pause_) {
      ThisFiber::SleepFor(100ms);
      continue;
    }

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

    if (type == RDB_OPCODE_DF_MASK) {
      uint32_t mask;
      SET_OR_RETURN(FetchInt<uint32_t>(), mask);
      settings.is_sticky = mask & DF_MASK_FLAG_STICKY;
      settings.has_mc_flags = mask & DF_MASK_FLAG_MC_FLAGS;
      if (settings.has_mc_flags) {
        SET_OR_RETURN(FetchInt<uint32_t>(), settings.mc_flags);
      }
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

      if (full_sync_cut_cb) {
        FlushAllShards();  // Flush as the handler awakes post load handlers
        full_sync_cut_cb();
      }
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
        shard_set->Add(
            i, [dbid] { namespaces->GetDefaultNamespace().GetCurrentDbSlice().ActivateDb(dbid); });
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
      uint64_t module_id;
      SET_OR_RETURN(LoadLen(nullptr), module_id);
      string module_name = ModuleTypeName(module_id);

      LOG(WARNING) << "WARNING: Skipping data for module " << module_name;
      RETURN_ON_ERR(SkipModuleData());
      continue;
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
      FlushAllShards();  // Always flush before applying incremental on top
      RETURN_ON_ERR(HandleJournalBlob(service_));
      continue;
    }

    if (type == RDB_OPCODE_SLOT_INFO) {
      [[maybe_unused]] uint64_t slot_id;
      SET_OR_RETURN(LoadLen(nullptr), slot_id);
      [[maybe_unused]] uint64_t slot_size;
      SET_OR_RETURN(LoadLen(nullptr), slot_size);
      [[maybe_unused]] uint64_t expires_slot_size;
      SET_OR_RETURN(LoadLen(nullptr), expires_slot_size);
      continue;
    }

    if (!rdbIsObjectTypeDF(type)) {
      return RdbError(errc::invalid_rdb_type);
    }

    ++keys_loaded;
    int64_t start = absl::GetCurrentTimeNanos();
    RETURN_ON_ERR(LoadKeyValPair(type, &settings));
    int delta_ms = (absl::GetCurrentTimeNanos() - start) / 1000'000;
    LOG_IF(INFO, delta_ms > 1000) << "Took " << delta_ms << " ms to load rdb_type " << type;

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
    shard_set->Add(i, [bc]() mutable {
      namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetLoadInProgress(false);
      bc->Dec();
    });
  }
  bc->Wait();  // wait for sentinels to report.

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

error_code RdbLoaderBase::EnsureReadInternal(size_t min_to_read) {
  // We need to include what we already read inside Input buffer. Otherwise we might expect to read
  // more than the minimum
  const size_t min_sz = min_to_read - mem_buf_->InputLen();

  auto out_buf = mem_buf_->AppendBuffer();
  CHECK_GT(out_buf.size(), min_sz);

  // If limit was applied we do not want to read more than needed
  // important when reading from sockets.
  if (bytes_read_ + out_buf.size() > source_limit_) {
    out_buf = out_buf.subspan(0, source_limit_ - bytes_read_);
  }

  io::Result<size_t> res = src_->ReadAtLeast(out_buf, min_sz);
  if (!res) {
    VLOG(1) << "Error reading from source: " << res.error() << " " << min_sz << " bytes";
    return res.error();
  }
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

error_code RdbLoaderBase::AllocateDecompressOnce(int op_type) {
  if (decompress_impl_) {
    return {};
  }

  if (op_type == RDB_OPCODE_COMPRESSED_ZSTD_BLOB_START) {
    decompress_impl_ = detail::DecompressImpl::CreateZstd();
  } else if (op_type == RDB_OPCODE_COMPRESSED_LZ4_BLOB_START) {
    decompress_impl_ = detail::DecompressImpl::CreateLZ4();
  } else {
    return RdbError(errc::unsupported_operation);
  }
  return {};
}

error_code RdbLoaderBase::SkipModuleData() {
  uint64_t opcode;
  SET_OR_RETURN(LoadLen(nullptr), opcode);  // ignore field 'when_opcode'
  if (opcode != RDB_MODULE_OPCODE_UINT)
    return RdbError(errc::rdb_file_corrupted);
  SET_OR_RETURN(LoadLen(nullptr), opcode);  // ignore field 'when'

  while (true) {
    SET_OR_RETURN(LoadLen(nullptr), opcode);

    switch (opcode) {
      case RDB_MODULE_OPCODE_EOF:
        return kOk;  // Module data end

      case RDB_MODULE_OPCODE_SINT:
      case RDB_MODULE_OPCODE_UINT: {
        [[maybe_unused]] uint64_t _;
        SET_OR_RETURN(LoadLen(nullptr), _);
        break;
      }

      case RDB_MODULE_OPCODE_STRING: {
        RdbVariant dest;
        error_code ec = ReadStringObj(&dest);
        if (ec) {
          return ec;
        }
        break;
      }

      case RDB_MODULE_OPCODE_DOUBLE: {
        [[maybe_unused]] double _;
        SET_OR_RETURN(FetchBinaryDouble(), _);
        break;
      }

      default:
        // TODO: handle RDB_MODULE_OPCODE_FLOAT
        LOG(ERROR) << "Unsupported module section: " << opcode;
        return RdbError(errc::rdb_file_corrupted);
    }
  }
}

error_code RdbLoaderBase::HandleCompressedBlob(int op_type) {
  RETURN_ON_ERR(AllocateDecompressOnce(op_type));

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
    LoadScriptFromAux(std::move(auxval));
  } else if (auxkey == "redis-ver") {
    VLOG(1) << "Loading RDB produced by Redis version " << auxval;
  } else if (auxkey == "df-ver") {
    VLOG(1) << "Loading RDB produced by Dragonfly version " << auxval;
  } else if (auxkey == "ctime") {
    int64_t ctime;
    if (absl::SimpleAtoi(auxval, &ctime)) {
      time_t age = time(NULL) - ctime;
      if (age < 0)
        age = 0;
      VLOG(1) << "RDB age " << strings::HumanReadableElapsedTime(age);
    }
  } else if (auxkey == "used-mem") {
    int64_t usedmem;
    if (absl::SimpleAtoi(auxval, &usedmem)) {
      VLOG(1) << "RDB memory usage when created " << strings::HumanReadableNumBytes(usedmem);
      if (usedmem > ssize_t(max_memory_limit)) {
        if (IsClusterEnabled()) {
          LOG(INFO) << "Attempting to load a snapshot of size " << usedmem
                    << ", despite memory limit of " << max_memory_limit;
        } else {
          LOG(WARNING) << "Could not load snapshot - its used memory is " << usedmem
                       << " but the limit is " << max_memory_limit;
          return RdbError(errc::out_of_memory);
        }
      }
    }
  } else if (auxkey == "aof-preamble") {
    long long haspreamble;
    if (absl::SimpleAtoi(auxval, &haspreamble) && haspreamble) {
      VLOG(1) << "RDB has an AOF tail";
    }
  } else if (auxkey == "redis-bits") {
    /* Just ignored. */
  } else if (auxkey == "search-index") {
    LoadSearchIndexDefFromAux(std::move(auxval));
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

    // Block, if tiered storage is active, but can't keep up
    while (EngineShard::tlocal()->ShouldThrottleForTiering()) {
      this->blocked_shards_.fetch_add(1, memory_order_relaxed);  // stop adding items to shard queue
      ThisFiber::SleepFor(100us);
      this->blocked_shards_.fetch_sub(1, memory_order_relaxed);
    }
  };

  while (blocked_shards_.load(memory_order_relaxed) > 0)
    ThisFiber::SleepFor(100us);
  bool preempted = shard_set->Add(sid, std::move(cb));
  VLOG_IF(2, preempted) << "FlushShardAsync was throttled";
}

void RdbLoader::FlushAllShards() {
  for (ShardId i = 0; i < shard_set->size(); i++)
    FlushShardAsync(i);
}

std::error_code RdbLoaderBase::FromOpaque(const OpaqueObj& opaque, LoadConfig config,
                                          CompactObj* pv) {
  OpaqueObjLoader visitor(opaque.rdb_type, pv, config);
  std::visit(visitor, opaque.obj);

  return visitor.ec();
}

void RdbLoaderBase::CopyStreamId(const StreamID& src, struct streamID* dest) {
  dest->ms = src.ms;
  dest->seq = src.seq;
}

void RdbLoader::LoadItemsBuffer(DbIndex db_ind, const ItemsBuf& ib) {
  EngineShard* es = EngineShard::tlocal();
  DbContext db_cntx{&namespaces->GetDefaultNamespace(), db_ind, GetCurrentTimeMs()};
  DbSlice& db_slice = db_cntx.GetDbSlice(es->shard_id());

  auto error_msg = [](const auto* item, auto db_ind) {
    return absl::StrCat("Found empty key: ", item->key, " in DB ", db_ind, " rdb_type ",
                        item->val.rdb_type);
  };

  for (const auto* item : ib) {
    PrimeValue pv;
    PrimeValue* pv_ptr = &pv;

    // If we're appending the item to an existing key, first load the
    // object.
    if (item->load_config.append) {
      auto res = db_slice.FindMutable(db_cntx, item->key);
      if (!IsValid(res.it)) {
        // If the item has expired we may not find the key. Note if the key
        // is found, but expired since we started loading, we still append to
        // avoid an inconsistent state where only part of the key is loaded.
        if (item->expire_ms == 0 || db_cntx.time_now_ms < item->expire_ms) {
          LOG(ERROR) << "Count not to find append key '" << item->key << "' in DB " << db_ind;
        }
        continue;
      }
      pv_ptr = &res.it->second;
    }

    if (ec_ = FromOpaque(item->val, item->load_config, pv_ptr); ec_) {
      if ((*ec_).value() == errc::empty_key) {
        auto error = error_msg(item, db_ind);
        if (RdbTypeAllowedEmpty(item->val.rdb_type)) {
          LOG(WARNING) << error;
        } else {
          LOG(ERROR) << error;
        }
        continue;
      }
      LOG(ERROR) << "Could not load value for key '" << item->key << "' in DB " << db_ind;
      stop_early_ = true;
      break;
    }
    if (item->load_config.append) {
      continue;
    }
    // We need this extra check because we don't return empty_key
    if (!pv.TagAllowsEmptyValue() && pv.Size() == 0) {
      LOG(WARNING) << error_msg(item, db_ind);
      continue;
    }

    if (item->expire_ms > 0 && db_cntx.time_now_ms >= item->expire_ms) {
      VLOG(2) << "Expire key on load: " << item->key;
      continue;
    }

    auto op_res = db_slice.AddOrUpdate(db_cntx, item->key, std::move(pv), item->expire_ms);
    if (!op_res) {
      LOG(ERROR) << "OOM failed to add key '" << item->key << "' in DB " << db_ind;
      ec_ = RdbError(errc::out_of_memory);
      stop_early_ = true;
      break;
    }

    auto& res = *op_res;
    res.it->first.SetSticky(item->is_sticky);
    if (item->has_mc_flags) {
      res.it->second.SetFlag(true);
      db_slice.SetMCFlag(db_cntx.db_index, res.it->first.AsRef(), item->mc_flags);
    }

    if (!override_existing_keys_ && !res.is_new) {
      LOG(WARNING) << "RDB has duplicated key '" << item->key << "' in DB " << db_ind;
    }

    if (auto* ts = es->tiered_storage(); ts)
      ts->TryStash(db_cntx.db_index, item->key, &res.it->second);
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

// Loads the next key/val pair.
//
// Huge objects may be loaded in parts, where only a subset of elements are
// loaded at a time. This reduces the memory required to load huge objects and
// prevents LoadItemsBuffer blocking. (Note so far only RDB_TYPE_SET and
// RDB_TYPE_SET_WITH_EXPIRY support partial reads).
error_code RdbLoader::LoadKeyValPair(int type, ObjSettings* settings) {
  std::string key;
  SET_OR_RETURN(ReadKey(), key);

  bool streamed = false;
  do {
    // If there is a cached Item in the free pool, take it, otherwise allocate
    // a new Item (LoadItemsBuffer returns free items).
    Item* item = item_queue_.Pop();
    if (item == nullptr) {
      item = new Item;
    }
    // Delete the item if we fail to load the key/val pair.
    auto cleanup = absl::Cleanup([item] { delete item; });

    item->load_config.append = pending_read_.remaining > 0;

    error_code ec = ReadObj(type, &item->val);
    if (ec) {
      VLOG(1) << "ReadObj error " << ec << " for key " << key;
      return ec;
    }

    // If the key can be discarded, we must still continue to read the
    // object from the RDB so we can read the next key.
    if (ShouldDiscardKey(key, settings)) {
      continue;
    }

    if (pending_read_.remaining > 0) {
      item->key = key;
      streamed = true;
    } else {
      // Avoid copying the key if this is the last read of the object.
      item->key = std::move(key);
    }

    item->load_config.streamed = streamed;
    item->load_config.reserve = pending_read_.reserve;
    // Clear 'reserve' as we must only set when the object is first
    // initialized.
    pending_read_.reserve = 0;

    item->is_sticky = settings->is_sticky;
    item->has_mc_flags = settings->has_mc_flags;
    item->mc_flags = settings->mc_flags;

    ShardId sid = Shard(item->key, shard_set->size());
    item->expire_ms = settings->expiretime;

    auto& out_buf = shard_buf_[sid];

    out_buf.emplace_back(std::move(item));
    std::move(cleanup).Cancel();

    constexpr size_t kBufSize = 64;
    if (out_buf.size() >= kBufSize) {
      // Despite being async, this function can block if the shard queue is full.
      FlushShardAsync(sid);
    }
  } while (pending_read_.remaining > 0);

  return kOk;
}

bool RdbLoader::ShouldDiscardKey(std::string_view key, ObjSettings* settings) const {
  if (!load_unowned_slots_ && IsClusterEnabled()) {
    const cluster::ClusterConfig* cluster_config = cluster::ClusterFamily::cluster_config();
    if (cluster_config != nullptr && !cluster_config->IsMySlot(key)) {
      return true;
    }
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
    VLOG(2) << "Expire key on read: " << key;
    return true;
  }

  return false;
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
  ConnectionContext cntx{nullptr, nullptr};
  cntx.is_replicating = true;
  cntx.journal_emulated = true;
  cntx.skip_acl_validation = true;
  cntx.ns = &namespaces->GetDefaultNamespace();

  uint32_t consumed = 0;
  facade::RespVec resp_vec;
  facade::RedisParser parser;

  def += "\r\n";  // RESP terminator
  io::MutableBytes buffer{reinterpret_cast<uint8_t*>(def.data()), def.size()};
  auto res = parser.Parse(buffer, &consumed, &resp_vec);

  if (res != facade::RedisParser::Result::OK) {
    LOG(ERROR) << "Bad index definition: " << def;
    return;
  }

  // Prepend FT.CREATE to index definiton
  CmdArgVec arg_vec;
  facade::RespExpr::VecToArgList(resp_vec, &arg_vec);
  string ft_create = "FT.CREATE";
  arg_vec.insert(arg_vec.begin(), MutableSlice{ft_create.data(), ft_create.size()});

  service_->DispatchCommand(absl::MakeSpan(arg_vec), &crb, &cntx);

  auto response = crb.Take();
  if (auto err = facade::CapturingReplyBuilder::TryExtractError(response); err) {
    LOG(ERROR) << "Bad index definition: " << def << " " << err->first;
  }
}

void RdbLoader::PerformPostLoad(Service* service) {
  const CommandId* cmd = service->FindCmd("FT.CREATE");
  if (cmd == nullptr)  // On MacOS we don't include search so FT.CREATE won't exist.
    return;

  // Rebuild all search indices as only their definitions are extracted from the snapshot
  shard_set->AwaitRunningOnShardQueue([](EngineShard* es) {
    es->search_indices()->RebuildAllIndices(
        OpArgs{es, nullptr, DbContext{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()}});
  });
}

}  // namespace dfly
