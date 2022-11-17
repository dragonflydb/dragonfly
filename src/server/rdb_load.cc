// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load.h"

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
#include <absl/strings/str_cat.h>

#include "base/endian.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/string_set.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/hset_family.h"
#include "server/rdb_extensions.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "strings/human_readable.h"

ABSL_DECLARE_FLAG(int32_t, list_max_listpack_size);
ABSL_DECLARE_FLAG(int32_t, list_compress_depth);
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(bool, use_set2);

#define SET_OR_RETURN(expr, dest) \
  do {                            \
    auto exp_val = (expr);        \
    if (!exp_val)                 \
      return exp_val.error();     \
    dest = exp_val.value();       \
  } while (0)

#define SET_OR_UNEXPECT(expr, dest)            \
  {                                            \
    auto exp_res = (expr);                     \
    if (!exp_res)                              \
      return make_unexpected(exp_res.error()); \
    dest = std::move(exp_res.value());         \
  }

namespace dfly {

using namespace std;
using base::IoBuf;
using nonstd::make_unexpected;
using namespace util;
using absl::GetFlag;
using rdb::errc;

namespace {
class error_category : public std::error_category {
 public:
  const char* name() const noexcept final {
    return "dragonfly.rdbload";
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

  std::error_code ec_;
  int rdb_type_;
  base::PODArray<char> tset_blob_;
  PrimeValue* pv_;
};

RdbLoaderBase::RdbLoaderBase() : mem_buf_{16_KB} {
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

void RdbLoaderBase::OpaqueObjLoader::CreateSet(const LoadTrace* ltrace) {
  size_t len = ltrace->arr.size();

  bool is_intset = true;
  if (len <= SetFamily::MaxIntsetEntries()) {
    for (size_t i = 0; i < len; i++) {
      if (!holds_alternative<long long>(ltrace->arr[i].rdb_var)) {
        is_intset = false;
        break;
      }
    }
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
    for (size_t i = 0; i < len; i++) {
      llval = get<long long>(ltrace->arr[i].rdb_var);
      uint8_t success;
      res->ptr = intsetAdd((intset*)res->ptr, llval, &success);
      if (!success) {
        LOG(ERROR) << "Duplicate set members detected";
        ec_ = RdbError(errc::duplicate_key);
        return;
      }
    }
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
      for (size_t i = 0; i < len; i++) {
        sdsele = ToSds(ltrace->arr[i].rdb_var);
        if (!sdsele)
          return;

        if (!((StringSet*)res->ptr)->AddSds(sdsele)) {
          LOG(ERROR) << "Duplicate set members detected";
          ec_ = RdbError(errc::duplicate_key);
          return;
        }
      }
    } else {
      for (size_t i = 0; i < len; i++) {
        sdsele = ToSds(ltrace->arr[i].rdb_var);
        if (!sdsele)
          return;

        if (dictAdd((dict*)res->ptr, sdsele, NULL) != DICT_OK) {
          LOG(ERROR) << "Duplicate set members detected";
          ec_ = RdbError(errc::duplicate_key);
          return;
        }
      }
    }
  }

  pv_->ImportRObj(res);
  std::move(cleanup).Cancel();
}

void RdbLoaderBase::OpaqueObjLoader::CreateHMap(const LoadTrace* ltrace) {
  size_t len = ltrace->arr.size() / 2;

  /* Too many entries? Use a hash table right from the start. */
  bool keep_lp = (len <= server.hash_max_listpack_entries);

  size_t lp_size = 0;
  if (keep_lp) {
    for (const auto& str : ltrace->arr) {
      size_t str_len = StrLen(str.rdb_var);
      lp_size += str_len;

      if (str_len > server.hash_max_listpack_value) {
        keep_lp = false;
        break;
      }
    }
  }

  robj* res = nullptr;

  if (keep_lp) {
    uint8_t* lp = lpNew(lp_size);

    for (size_t i = 0; i < len; ++i) {
      /* Add pair to listpack */
      string_view sv = ToSV(ltrace->arr[i * 2].rdb_var);
      lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());

      sv = ToSV(ltrace->arr[i * 2 + 1].rdb_var);
      lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    }

    if (ec_) {
      lpFree(lp);
      return;
    }

    lp = lpShrinkToFit(lp);
    res = createObject(OBJ_HASH, lp);
    res->encoding = OBJ_ENCODING_LISTPACK;
  } else {
    dict* hmap = dictCreate(&hashDictType);

    auto cleanup = absl::MakeCleanup([&] { dictRelease(hmap); });

    if (len > DICT_HT_INITIAL_SIZE) {
      if (dictTryExpand(hmap, len) != DICT_OK) {
        LOG(ERROR) << "OOM in dictTryExpand " << len;
        ec_ = RdbError(errc::out_of_memory);
        return;
      }
    }

    for (size_t i = 0; i < len; ++i) {
      sds key = ToSds(ltrace->arr[i * 2].rdb_var);
      sds val = ToSds(ltrace->arr[i * 2 + 1].rdb_var);

      if (!key || !val)
        return;

      /* Add pair to hash table */
      int ret = dictAdd(hmap, key, val);
      if (ret == DICT_ERR) {
        LOG(ERROR) << "Duplicate hash fields detected";
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }
    }

    res = createObject(OBJ_HASH, hmap);
    res->encoding = OBJ_ENCODING_HT;
    std::move(cleanup).Cancel();
  }

  DCHECK(res);
  pv_->ImportRObj(res);
}

void RdbLoaderBase::OpaqueObjLoader::CreateList(const LoadTrace* ltrace) {
  quicklist* ql =
      quicklistNew(GetFlag(FLAGS_list_max_listpack_size), GetFlag(FLAGS_list_compress_depth));
  auto cleanup = absl::Cleanup([&] { quicklistRelease(ql); });

  for (size_t i = 0; i < ltrace->arr.size(); ++i) {
    unsigned container = ltrace->arr[i].encoding;
    string_view sv = ToSV(ltrace->arr[i].rdb_var);

    if (ec_)
      return;

    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
      quicklistAppendPlainNode(ql, (uint8_t*)sv.data(), sv.size());
      continue;
    }

    uint8_t* lp = nullptr;

    if (rdb_type_ == RDB_TYPE_LIST_QUICKLIST_2) {
      uint8_t* src = (uint8_t*)sv.data();
      if (!lpValidateIntegrity(src, sv.size(), 0, NULL, NULL)) {
        LOG(ERROR) << "Listpack integrity check failed.";
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }

      if (lpLength(src) == 0) {
        continue;
      }

      lp = (uint8_t*)zmalloc(sv.size());
      memcpy(lp, src, sv.size());
    } else {
      lp = lpNew(sv.size());
      if (!ziplistValidateIntegrity((uint8_t*)sv.data(), sv.size(), 1,
                                    ziplistEntryConvertAndValidate, &lp)) {
        LOG(ERROR) << "Ziplist integrity check failed.";
        zfree(lp);
        ec_ = RdbError(errc::rdb_file_corrupted);
        return;
      }

      /* Silently skip empty ziplists, if we'll end up with empty quicklist we'll fail later. */
      if (lpLength(lp) == 0) {
        zfree(lp);
        continue;
      }

      lp = lpShrinkToFit(lp);
    }

    quicklistAppendListpack(ql, lp);
  }

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
  robj* res = createZsetObject();
  zset* zs = (zset*)res->ptr;

  auto cleanup = absl::Cleanup([&] { decrRefCount(res); });

  size_t zsetlen = ltrace->arr.size();
  if (zsetlen > DICT_HT_INITIAL_SIZE && dictTryExpand(zs->dict, zsetlen) != DICT_OK) {
    LOG(ERROR) << "OOM in dictTryExpand " << zsetlen;
    ec_ = RdbError(errc::out_of_memory);
    return;
  }

  size_t maxelelen = 0, totelelen = 0;

  for (size_t i = 0; i < zsetlen; ++i) {
    sds sdsele = ToSds(ltrace->arr[i].rdb_var);
    if (!sdsele)
      return;

    double score = ltrace->arr[i].score;

    /* Don't care about integer-encoded strings. */
    if (sdslen(sdsele) > maxelelen)
      maxelelen = sdslen(sdsele);
    totelelen += sdslen(sdsele);

    zskiplistNode* znode = zslInsert(zs->zsl, score, sdsele);
    int ret = dictAdd(zs->dict, sdsele, &znode->score);
    if (ret != DICT_OK) {
      LOG(ERROR) << "Duplicate zset fields detected";
      sdsfree(sdsele);
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }
  }

  /* Convert *after* loading, since sorted sets are not stored ordered. */
  if (zsetLength(res) <= server.zset_max_listpack_entries &&
      maxelelen <= server.zset_max_listpack_value && lpSafeToAdd(NULL, totelelen)) {
    zsetConvert(res, OBJ_ENCODING_LISTPACK);
  }

  std::move(cleanup).Cancel();

  pv_->ImportRObj(res);
}

void RdbLoaderBase::OpaqueObjLoader::CreateStream(const LoadTrace* ltrace) {
  CHECK(ltrace->stream_trace);

  robj* res = createStreamObject();
  stream* s = (stream*)res->ptr;

  auto cleanup = absl::Cleanup([&] { decrRefCount(res); });

  size_t lpcnt = ltrace->arr.size() / 2;

  for (size_t i = 0; i < lpcnt; ++i) {
    string_view nodekey = ToSV(ltrace->arr[i * 2].rdb_var);
    string_view data = ToSV(ltrace->arr[i * 2 + 1].rdb_var);

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
    memcpy(copy_lp, lp, data.size());
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

  robj* res = nullptr;
  if (rdb_type_ == RDB_TYPE_SET_INTSET) {
    if (!intsetValidateIntegrity((const uint8_t*)blob.data(), blob.size(), 0)) {
      LOG(ERROR) << "Intset integrity check failed.";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return;
    }

    const intset* is = (const intset*)blob.data();

    unsigned len = intsetLen(is);
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
      memcpy(mine, blob.data(), blob.size());
      res = createObject(OBJ_SET, mine);
      res->encoding = OBJ_ENCODING_INTSET;
    }
  } else if (rdb_type_ == RDB_TYPE_HASH_ZIPLIST) {
    unsigned char* lp = lpNew(blob.size());
    if (!ziplistPairsConvertAndValidateIntegrity((const uint8_t*)blob.data(), blob.size(), &lp)) {
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

    res = createObject(OBJ_HASH, lp);
    res->encoding = OBJ_ENCODING_LISTPACK;

    if (lpBytes(lp) > HSetFamily::MaxListPackLen())
      hashTypeConvert(res, OBJ_ENCODING_HT);
    else
      res->ptr = lpShrinkToFit((uint8_t*)res->ptr);
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

    res = createObject(OBJ_ZSET, lp);
    res->encoding = OBJ_ENCODING_LISTPACK;

    if (lpBytes(lp) > server.zset_max_listpack_entries)
      zsetConvert(res, OBJ_ENCODING_SKIPLIST);
    else
      res->ptr = lpShrinkToFit(lp);
  } else {
    LOG(FATAL) << "Unsupported rdb type " << rdb_type_;
  }

  pv_->ImportRObj(res);
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
    return string_view(ch_arr->data(), ch_arr->size());
  }

  const LzfString* lzf = get_if<LzfString>(&obj);
  if (lzf) {
    tset_blob_.resize(lzf->uncompressed_len);
    if (lzf_decompress(lzf->compressed_blob.data(), lzf->compressed_blob.size(), tset_blob_.data(),
                       lzf->uncompressed_len) == 0) {
      LOG(ERROR) << "Invalid LZF compressed string";
      ec_ = RdbError(errc::rdb_file_corrupted);
      return string_view{};
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

  size_t to_copy = std::min(mem_buf_.InputLen(), size);
  DVLOG(2) << "Copying " << to_copy << " bytes";

  ::memcpy(next, mem_buf_.InputBuffer().data(), to_copy);
  mem_buf_.ConsumeInput(to_copy);
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

  io::MutableBytes mb = mem_buf_.AppendBuffer();

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

  mem_buf_.CommitWrite(bytes_read);
  ::memcpy(next, mem_buf_.InputBuffer().data(), size);
  mem_buf_.ConsumeInput(size);

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

  if (mem_buf_.InputLen() >= clen) {
    cbuf = mem_buf_.InputBuffer().data();
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
    mem_buf_.ConsumeInput(clen);

  return res;
}

auto RdbLoaderBase::FetchIntegerObject(int enctype) -> io::Result<string> {
  long long val;

  if (enctype == RDB_ENC_INT8) {
    SET_OR_UNEXPECT(FetchInt<int8_t>(), val);
  } else if (enctype == RDB_ENC_INT16) {
    SET_OR_UNEXPECT(FetchInt<uint16_t>(), val);
  } else if (enctype == RDB_ENC_INT32) {
    SET_OR_UNEXPECT(FetchInt<uint32_t>(), val);
  } else {
    return Unexpected(errc::invalid_encoding);
  }

  char buf[32];
  absl::numbers_internal::FastIntToBuffer(val, buf);

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
  mem_buf_.ReadAndConsume(8, buf);
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

auto RdbLoaderBase::ReadObj(int rdbtype) -> io::Result<OpaqueObj> {
  switch (rdbtype) {
    case RDB_TYPE_STRING: {
      /* Read string value */
      auto fetch = ReadStringObj();
      if (!fetch)
        return make_unexpected(fetch.error());
      return OpaqueObj{std::move(*fetch), RDB_TYPE_STRING};
    }
    case RDB_TYPE_SET:
      return ReadSet();
    case RDB_TYPE_SET_INTSET:
      return ReadIntSet();
    case RDB_TYPE_HASH_ZIPLIST:
      return ReadHZiplist();
    case RDB_TYPE_HASH:
      return ReadHMap();
    case RDB_TYPE_ZSET:
    case RDB_TYPE_ZSET_2:
      return ReadZSet(rdbtype);
    case RDB_TYPE_ZSET_ZIPLIST:
      return ReadZSetZL();
    case RDB_TYPE_LIST_QUICKLIST:
    case RDB_TYPE_LIST_QUICKLIST_2:
      return ReadListQuicklist(rdbtype);
    case RDB_TYPE_STREAM_LISTPACKS:
      return ReadStreams();
      break;
  }

  LOG(ERROR) << "Unsupported rdb type " << rdbtype;

  return Unexpected(errc::invalid_encoding);
}

auto RdbLoaderBase::ReadStringObj() -> io::Result<RdbVariant> {
  bool isencoded;
  size_t len;

  SET_OR_UNEXPECT(LoadLen(&isencoded), len);

  if (isencoded) {
    switch (len) {
      case RDB_ENC_INT8:
      case RDB_ENC_INT16:
      case RDB_ENC_INT32:
        return ReadIntObj(len);
      case RDB_ENC_LZF:
        return ReadLzf();
      default:
        LOG(ERROR) << "Unknown RDB string encoding " << len;
        return Unexpected(errc::rdb_file_corrupted);
    }
  }

  base::PODArray<char> blob;
  blob.resize(len);
  error_code ec = FetchBuf(len, blob.data());
  if (ec) {
    return make_unexpected(ec);
  }

  return blob;
}

io::Result<long long> RdbLoaderBase::ReadIntObj(int enctype) {
  long long val;

  if (enctype == RDB_ENC_INT8) {
    SET_OR_UNEXPECT(FetchInt<int8_t>(), val);
  } else if (enctype == RDB_ENC_INT16) {
    SET_OR_UNEXPECT(FetchInt<uint16_t>(), val);
  } else if (enctype == RDB_ENC_INT32) {
    SET_OR_UNEXPECT(FetchInt<uint32_t>(), val);
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

  unique_ptr<LoadTrace> res(new LoadTrace);
  res->arr.resize(len);
  for (size_t i = 0; i < len; i++) {
    io::Result<RdbVariant> fetch = ReadStringObj();
    if (!fetch) {
      return make_unexpected(fetch.error());
    }
    res->arr[i].rdb_var = std::move(fetch.value());
  }

  return OpaqueObj{std::move(res), RDB_TYPE_SET};
}

auto RdbLoaderBase::ReadIntSet() -> io::Result<OpaqueObj> {
  io::Result<RdbVariant> fetch = ReadStringObj();
  if (!fetch) {
    return make_unexpected(fetch.error());
  }

  const LzfString* lzf = get_if<LzfString>(&fetch.value());
  const base::PODArray<char>* arr = get_if<base::PODArray<char>>(&fetch.value());

  if (lzf) {
    if (lzf->uncompressed_len == 0 || lzf->compressed_blob.empty())
      return Unexpected(errc::rdb_file_corrupted);
  } else if (arr) {
    if (arr->empty())
      return Unexpected(errc::rdb_file_corrupted);
  } else {
    return Unexpected(errc::rdb_file_corrupted);
  }

  return OpaqueObj{std::move(*fetch), RDB_TYPE_SET_INTSET};
}

auto RdbLoaderBase::ReadHZiplist() -> io::Result<OpaqueObj> {
  RdbVariant str_obj;
  SET_OR_UNEXPECT(ReadStringObj(), str_obj);

  if (StrLen(str_obj) == 0) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  return OpaqueObj{std::move(str_obj), RDB_TYPE_HASH_ZIPLIST};
}

auto RdbLoaderBase::ReadHMap() -> io::Result<OpaqueObj> {
  uint64_t len;
  SET_OR_UNEXPECT(LoadLen(nullptr), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  load_trace->arr.resize(len * 2);

  for (size_t i = 0; i < load_trace->arr.size(); ++i) {
    SET_OR_UNEXPECT(ReadStringObj(), load_trace->arr[i].rdb_var);
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
  load_trace->arr.resize(zsetlen);

  double score;

  for (size_t i = 0; i < load_trace->arr.size(); ++i) {
    SET_OR_UNEXPECT(ReadStringObj(), load_trace->arr[i].rdb_var);
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

  return OpaqueObj{std::move(load_trace), rdbtype};
}

auto RdbLoaderBase::ReadZSetZL() -> io::Result<OpaqueObj> {
  RdbVariant str_obj;
  SET_OR_UNEXPECT(ReadStringObj(), str_obj);

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
  load_trace->arr.resize(len);

  for (size_t i = 0; i < len; ++i) {
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
    SET_OR_UNEXPECT(ReadStringObj(), var);
    if (StrLen(var) == 0) {
      return Unexpected(errc::rdb_file_corrupted);
    }
    load_trace->arr[i].rdb_var = std::move(var);
    load_trace->arr[i].encoding = container;
  }

  return OpaqueObj{std::move(load_trace), rdbtype};
}

auto RdbLoaderBase::ReadStreams() -> io::Result<OpaqueObj> {
  uint64_t listpacks;
  SET_OR_UNEXPECT(LoadLen(nullptr), listpacks);

  unique_ptr<LoadTrace> load_trace(new LoadTrace);
  load_trace->arr.resize(listpacks * 2);

  for (size_t i = 0; i < listpacks; ++i) {
    /* Get the master ID, the one we'll use as key of the radix tree
     * node: the entries inside the listpack itself are delta-encoded
     * relatively to this ID. */
    RdbVariant stream_id, blob;
    SET_OR_UNEXPECT(ReadStringObj(), stream_id);

    if (StrLen(stream_id) != sizeof(streamID)) {
      LOG(ERROR) << "Stream node key entry is not the size of a stream ID";

      return Unexpected(errc::rdb_file_corrupted);
    }

    SET_OR_UNEXPECT(ReadStringObj(), blob);

    if (StrLen(blob) == 0) {
      LOG(ERROR) << "Stream listpacks loading failed";
      return Unexpected(errc::rdb_file_corrupted);
    }

    load_trace->arr[2 * i].rdb_var = std::move(stream_id);
    load_trace->arr[2 * i + 1].rdb_var = std::move(blob);
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
    SET_OR_UNEXPECT(ReadStringObj(), cgname);
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
      SET_OR_UNEXPECT(ReadStringObj(), consumer.name);

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

template <typename T> io::Result<T> RdbLoaderBase::FetchInt() {
  auto ec = EnsureRead(sizeof(T));
  if (ec)
    return make_unexpected(ec);

  char buf[16];
  mem_buf_.ReadAndConsume(sizeof(T), buf);

  return base::LE::LoadT<std::make_unsigned_t<T>>(buf);
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

RdbLoader::RdbLoader(ScriptMgr* script_mgr) : script_mgr_(script_mgr) {
  shard_buf_.reset(new ItemsBuf[shard_set->size()]);
}

RdbLoader::~RdbLoader() {
}

error_code RdbLoader::Load(io::Source* src) {
  CHECK(!src_ && src);

  absl::Time start = absl::Now();
  src_ = src;

  IoBuf::Bytes bytes = mem_buf_.AppendBuffer();
  io::Result<size_t> read_sz = src_->ReadAtLeast(bytes, 9);
  if (!read_sz)
    return read_sz.error();

  bytes_read_ = *read_sz;
  if (bytes_read_ < 9) {
    return RdbError(errc::wrong_signature);
  }

  mem_buf_.CommitWrite(bytes_read_);

  {
    auto cb = mem_buf_.InputBuffer();

    if (memcmp(cb.data(), "REDIS", 5) != 0) {
      return RdbError(errc::wrong_signature);
    }

    char buf[64] = {0};
    ::memcpy(buf, cb.data() + 5, 4);

    int rdbver = atoi(buf);
    if (rdbver < 5 || rdbver > RDB_VERSION) {  // We accept starting from 5.
      LOG(ERROR) << "RDB Version " << rdbver << " is not supported";
      return RdbError(errc::bad_version);
    }

    mem_buf_.ConsumeInput(9);
  }

  int type;

  /* Key-specific attributes, set by opcodes before the key type. */
  ObjSettings settings;
  settings.now = mstime();
  size_t keys_loaded = 0;

  while (!stop_early_.load(memory_order_relaxed)) {
    /* Read type. */
    SET_OR_RETURN(FetchType(), type);

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
      if (full_sync_cut_cb)
        full_sync_cut_cb();
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

      VLOG(1) << "Select DB: " << dbid;
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

    if (!rdbIsObjectType(type)) {
      return RdbError(errc::invalid_rdb_type);
    }

    ++keys_loaded;
    RETURN_ON_ERR(LoadKeyValPair(type, &settings));
    settings.Reset();
  }  // main load loop

  if (stop_early_) {
    lock_guard lk(mu_);
    return ec_;
  }

  /* Verify the checksum if RDB version is >= 5 */
  RETURN_ON_ERR(VerifyChecksum());

  fibers_ext::BlockingCounter bc(shard_set->size());
  for (unsigned i = 0; i < shard_set->size(); ++i) {
    // Flush the remaining items.
    FlushShardAsync(i);

    // Send sentinel callbacks to ensure that all previous messages have been processed.
    shard_set->Add(i, [bc]() mutable { bc.Dec(); });
  }
  bc.Wait();  // wait for sentinels to report.

  absl::Duration dur = absl::Now() - start;
  load_time_ = double(absl::ToInt64Milliseconds(dur)) / 1000;
  keys_loaded_ = keys_loaded;

  return kOk;
}

error_code RdbLoaderBase::EnsureReadInternal(size_t min_sz) {
  DCHECK_LT(mem_buf_.InputLen(), min_sz);

  auto out_buf = mem_buf_.AppendBuffer();
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
  mem_buf_.CommitWrite(*res);

  return kOk;
}

auto RdbLoaderBase::LoadLen(bool* is_encoded) -> io::Result<uint64_t> {
  if (is_encoded)
    *is_encoded = false;

  // Every RDB file with rdbver >= 5 has 8-bytes checksum at the end,
  // so we can ensure we have 9 bytes to read up until that point.
  error_code ec = EnsureRead(9);
  if (ec)
    return make_unexpected(ec);

  uint64_t res = 0;
  uint8_t first = mem_buf_.InputBuffer()[0];
  int type = (first & 0xC0) >> 6;
  mem_buf_.ConsumeInput(1);
  if (type == RDB_ENCVAL) {
    /* Read a 6 bit encoding type. */
    if (is_encoded)
      *is_encoded = true;
    res = first & 0x3F;
  } else if (type == RDB_6BITLEN) {
    /* Read a 6 bit len. */
    res = first & 0x3F;
  } else if (type == RDB_14BITLEN) {
    res = ((first & 0x3F) << 8) | mem_buf_.InputBuffer()[0];
    mem_buf_.ConsumeInput(1);
  } else if (first == RDB_32BITLEN) {
    /* Read a 32 bit len. */
    res = absl::big_endian::Load32(mem_buf_.InputBuffer().data());
    mem_buf_.ConsumeInput(4);
  } else if (first == RDB_64BITLEN) {
    /* Read a 64 bit len. */
    res = absl::big_endian::Load64(mem_buf_.InputBuffer().data());
    mem_buf_.ConsumeInput(8);
  } else {
    LOG(ERROR) << "Bad length encoding " << type << " in rdbLoadLen()";
    return Unexpected(errc::rdb_file_corrupted);
  }

  return res;
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
    ServerState* ss = ServerState::tlocal();
    Interpreter& script = ss->GetInterpreter();
    string_view body{auxval};
    string result;
    Interpreter::AddResult add_result = script.AddFunction(body, &result);
    if (add_result == Interpreter::ADD_OK) {
      if (script_mgr_)
        script_mgr_->InsertFunction(result, body);
    } else if (add_result == Interpreter::COMPILE_ERR) {
      LOG(ERROR) << "Error when compiling lua scripts";
    }
  } else if (auxkey == "redis-ver") {
    LOG(INFO) << "Loading RDB produced by version " << auxval;
  } else if (auxkey == "ctime") {
    int64_t ctime;
    if (absl::SimpleAtoi(auxval, &ctime)) {
      time_t age = time(NULL) - ctime;
      if (age < 0)
        age = 0;
      LOG(INFO) << "RDB age " << strings::HumanReadableElapsedTime(age);
    }
  } else if (auxkey == "used-mem") {
    long long usedmem;
    if (absl::SimpleAtoi(auxval, &usedmem)) {
      LOG(INFO) << "RDB memory usage when created " << strings::HumanReadableNumBytes(usedmem);
    }
  } else if (auxkey == "aof-preamble") {
    long long haspreamble;
    if (absl::SimpleAtoi(auxval, &haspreamble) && haspreamble) {
      LOG(INFO) << "RDB has an AOF tail";
    }
  } else if (auxkey == "redis-bits") {
    /* Just ignored. */
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

  io::Bytes cur_buf = mem_buf_.InputBuffer();

  VLOG(1) << "VerifyChecksum: input buffer len " << cur_buf.size() << ", expected " << expected;

  return kOk;
}

void RdbLoader::FlushShardAsync(ShardId sid) {
  auto& out_buf = shard_buf_[sid];
  if (out_buf.empty())
    return;

  ItemsBuf* ib = new ItemsBuf{std::move(out_buf)};
  auto cb = [indx = this->cur_db_index_, ib, this] {
    this->LoadItemsBuffer(indx, *ib);
    delete ib;
  };

  shard_set->Add(sid, std::move(cb));
}

std::error_code RdbLoaderBase::Visit(const Item& item, CompactObj* pv) {
  std::string_view key{item.key};
  OpaqueObjLoader visitor(item.val.rdb_type, pv);
  std::visit(visitor, item.val.obj);
  return visitor.ec();
}

void RdbLoader::LoadItemsBuffer(DbIndex db_ind, const ItemsBuf& ib) {
  DbSlice& db_slice = EngineShard::tlocal()->db_slice();
  DbContext db_cntx{.db_index = db_ind, .time_now_ms = GetCurrentTimeMs()};

  for (const auto& item : ib) {
    PrimeValue pv;
    if (auto ec = Visit(item, &pv); ec) {
      lock_guard lk(mu_);
      ec_ = ec;
      stop_early_ = true;
      break;
    }

    if (item.expire_ms > 0 && db_cntx.time_now_ms >= item.expire_ms)
      continue;

    auto [it, added] = db_slice.AddOrUpdate(db_cntx, item.key, std::move(pv), item.expire_ms);
    if (!added) {
      LOG(WARNING) << "RDB has duplicated key '" << item.key << "' in DB " << db_ind;
    }
  }
}

void RdbLoader::ResizeDb(size_t key_num, size_t expire_num) {
  DCHECK_LT(key_num, 1U << 31);
  DCHECK_LT(expire_num, 1U << 31);
}

error_code RdbLoader::LoadKeyValPair(int type, ObjSettings* settings) {
  /* Read key */
  string key;
  OpaqueObj val;

  // We free key in LoadItemsBuffer.
  SET_OR_RETURN(ReadKey(), key);

  io::Result<OpaqueObj> io_res = ReadObj(type);

  if (!io_res) {
    VLOG(1) << "ReadObj error " << io_res.error() << " for key " << key;
    return io_res.error();
  }

  val = std::move(io_res.value());

  /* Check if the key already expired. This function is used when loading
   * an RDB file from disk, either at startup, or when an RDB was
   * received from the master. In the latter case, the master is
   * responsible for key expiry. If we would expire keys here, the
   * snapshot taken by the master may not be reflected on the slave.
   * Similarly if the RDB is the preamble of an AOF file, we want to
   * load all the keys as they are, since the log of operations later
   * assume to work in an exact keyspace state. */
  // TODO: check rdbflags&RDBFLAGS_AOF_PREAMBLE logic in rdb.c
  bool should_expire = settings->has_expired;  // TODO: to implement
  if (should_expire) {
    // decrRefCount(val);
  } else {
    ShardId sid = Shard(key, shard_set->size());
    uint64_t expire_at_ms = settings->expiretime;

    auto& out_buf = shard_buf_[sid];

    out_buf.emplace_back(Item{std::move(key), std::move(val), expire_at_ms});

    constexpr size_t kBufSize = 128;
    if (out_buf.size() >= kBufSize) {
      FlushShardAsync(sid);
    }
  }

  return kOk;
}

}  // namespace dfly
