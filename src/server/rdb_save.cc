// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_save.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <queue>

extern "C" {
#include "redis/crc64.h"
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/quicklist.h"
#include "redis/rdb.h"
#include "redis/stream.h"
#include "redis/util.h"
#include "redis/ziplist.h"
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "core/bloom.h"
#include "core/json/json_object.h"
#include "core/qlist.h"
#include "core/size_tracking_channel.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/namespaces.h"
#include "server/rdb_extensions.h"
#include "server/search/doc_index.h"
#include "server/serializer_commons.h"
#include "server/snapshot.h"
#include "server/tiering/common.h"
#include "util/fibers/simple_channel.h"

ABSL_FLAG(dfly::CompressionMode, compression_mode, dfly::CompressionMode::MULTI_ENTRY_LZ4,
          "set 0 for no compression,"
          "set 1 for single entry lzf compression,"
          "set 2 for multi entry zstd compression on df snapshot and single entry on rdb snapshot,"
          "set 3 for multi entry lz4 compression on df snapshot and single entry on rdb snapshot");

// TODO: to retire both flags in v1.27 (Jan 2025)
ABSL_FLAG(bool, list_rdb_encode_v2, true,
          "V2 rdb encoding of list uses listpack encoding format, compatible with redis 7. V1 rdb "
          "enconding of list uses ziplist encoding compatible with redis 6");

ABSL_FLAG(bool, stream_rdb_encode_v2, false,
          "V2 uses format, compatible with redis 7.2 and Dragonfly v1.26+, while v1 format "
          "is compatible with redis 6");

namespace dfly {

using namespace std;
using base::IoBuf;
using io::Bytes;

using namespace tiering::literals;

namespace {

/* Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. */
unsigned EncodeInteger(long long value, uint8_t* enc) {
  if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
    enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT8;
    enc[1] = value & 0xFF;
    return 2;
  }

  if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
    enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT16;
    enc[1] = value & 0xFF;
    enc[2] = (value >> 8) & 0xFF;
    return 3;
  }

  constexpr long long k31 = (1LL << 31);
  if (value >= -k31 && value <= k31 - 1) {
    enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT32;
    enc[1] = value & 0xFF;
    enc[2] = (value >> 8) & 0xFF;
    enc[3] = (value >> 16) & 0xFF;
    enc[4] = (value >> 24) & 0xFF;
    return 5;
  }

  return 0;
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
unsigned TryIntegerEncoding(string_view input, uint8_t* dest) {
  long long value;

  /* Check if it's possible to encode this value as a number */
  if (!absl::SimpleAtoi(input, &value))
    return 0;
  absl::AlphaNum alpha(value);

  /* If the number converted back into a string is not identical
   * then it's not possible to encode the string as integer */
  if (alpha.size() != input.size() || alpha.Piece() != input)
    return 0;

  return EncodeInteger(value, dest);
}

constexpr size_t kBufLen = 64_KB;
constexpr size_t kAmask = 4_KB - 1;
constexpr uint32_t kChannelLen = 2;

}  // namespace

bool AbslParseFlag(std::string_view in, dfly::CompressionMode* flag, std::string* err) {
  if (in == "0" || in == "NONE") {
    *flag = dfly::CompressionMode::NONE;
    return true;
  }
  if (in == "1" || in == "SINGLE_ENTRY") {
    *flag = dfly::CompressionMode::SINGLE_ENTRY;
    return true;
  }
  if (in == "2" || in == "MULTI_ENTRY_ZSTD") {
    *flag = dfly::CompressionMode::MULTI_ENTRY_ZSTD;
    return true;
  }
  if (in == "3" || in == "MULTI_ENTRY_LZ4") {
    *flag = dfly::CompressionMode::MULTI_ENTRY_LZ4;
    return true;
  }

  *err = absl::StrCat("Unknown value ", in, " for compression_mode flag");
  return false;
}

std::string AbslUnparseFlag(dfly::CompressionMode flag) {
  switch (flag) {
    case dfly::CompressionMode::NONE:
      return "NONE";
    case dfly::CompressionMode::SINGLE_ENTRY:
      return "SINGLE_ENTRY";
    case dfly::CompressionMode::MULTI_ENTRY_ZSTD:
      return "MULTI_ENTRY_ZSTD";
    case dfly::CompressionMode::MULTI_ENTRY_LZ4:
      return "MULTI_ENTRY_LZ4";
  }
  DCHECK(false) << "Unknown compression_mode flag value " << int(flag);
  return "NONE";
}

dfly::CompressionMode GetDefaultCompressionMode() {
  return absl::GetFlag(FLAGS_compression_mode);
}

uint8_t RdbObjectType(const PrimeValue& pv) {
  unsigned type = pv.ObjType();
  unsigned compact_enc = pv.Encoding();
  switch (type) {
    case OBJ_STRING:
      return RDB_TYPE_STRING;
    case OBJ_LIST:
      if (compact_enc == OBJ_ENCODING_QUICKLIST || compact_enc == kEncodingQL2) {
        return absl::GetFlag(FLAGS_list_rdb_encode_v2) ? RDB_TYPE_LIST_QUICKLIST_2
                                                       : RDB_TYPE_LIST_QUICKLIST;
      }
      break;
    case OBJ_SET:
      if (compact_enc == kEncodingIntSet)
        return RDB_TYPE_SET_INTSET;
      else if (compact_enc == kEncodingStrMap2) {
        if (((StringSet*)pv.RObjPtr())->ExpirationUsed())
          return RDB_TYPE_SET_WITH_EXPIRY;
        else
          return RDB_TYPE_SET;
      }
      break;
    case OBJ_ZSET:
      if (compact_enc == OBJ_ENCODING_LISTPACK)
        return RDB_TYPE_ZSET_ZIPLIST;  // we save using the old ziplist encoding.
      else if (compact_enc == OBJ_ENCODING_SKIPLIST)
        return RDB_TYPE_ZSET_2;
      break;
    case OBJ_HASH:
      if (compact_enc == kEncodingListPack)
        return RDB_TYPE_HASH_ZIPLIST;
      else if (compact_enc == kEncodingStrMap2) {
        if (((StringMap*)pv.RObjPtr())->ExpirationUsed())
          return RDB_TYPE_HASH_WITH_EXPIRY;  // Incompatible with Redis
        else
          return RDB_TYPE_HASH;
      }
      break;
    case OBJ_STREAM:
      return absl::GetFlag(FLAGS_stream_rdb_encode_v2) ? RDB_TYPE_STREAM_LISTPACKS_3
                                                       : RDB_TYPE_STREAM_LISTPACKS;
    case OBJ_MODULE:
      return RDB_TYPE_MODULE_2;
    case OBJ_JSON:
      return RDB_TYPE_JSON;
    case OBJ_SBF:
      return RDB_TYPE_SBF;
  }
  LOG(FATAL) << "Unknown encoding " << compact_enc << " for type " << type;
  return 0; /* avoid warning */
}

SerializerBase::SerializerBase(CompressionMode compression_mode)
    : compression_mode_(compression_mode), mem_buf_{4_KB}, tmp_buf_(nullptr) {
}

RdbSerializer::RdbSerializer(CompressionMode compression_mode,
                             std::function<void(size_t, SerializerBase::FlushState)> flush_fun)
    : SerializerBase(compression_mode), flush_fun_(std::move(flush_fun)) {
}

RdbSerializer::~RdbSerializer() {
  VLOG(2) << "compression mode: " << uint32_t(compression_mode_);
  if (compression_stats_) {
    VLOG(2) << "compression not effective: " << compression_stats_->compression_no_effective;
    VLOG(2) << "small string none compression applied: " << compression_stats_->small_str_count;
    VLOG(2) << "compression failed: " << compression_stats_->compression_failed;
    VLOG(2) << "compressed blobs:" << compression_stats_->compressed_blobs;
  }
}

std::error_code RdbSerializer::SaveValue(const PrimeValue& pv) {
  std::error_code ec;
  if (pv.ObjType() == OBJ_STRING) {
    auto opt_int = pv.TryGetInt();
    if (opt_int) {
      ec = SaveLongLongAsString(*opt_int);
    } else {
      ec = SaveString(pv.GetSlice(&tmp_str_));
    }
  } else {
    ec = SaveObject(pv);
  }
  return ec;
}

error_code RdbSerializer::SelectDb(uint32_t dbid) {
  if (dbid == last_entry_db_index_) {
    return error_code{};
  }
  last_entry_db_index_ = dbid;
  uint8_t buf[16];
  buf[0] = RDB_OPCODE_SELECTDB;
  unsigned enclen = WritePackedUInt(dbid, io::MutableBytes{buf}.subspan(1));
  return WriteRaw(Bytes{buf, enclen + 1});
}

// Called by snapshot
io::Result<uint8_t> RdbSerializer::SaveEntry(const PrimeKey& pk, const PrimeValue& pv,
                                             uint64_t expire_ms, uint32_t mc_flags, DbIndex dbid) {
  if (!pv.TagAllowsEmptyValue() && pv.Size() == 0) {
    string_view key = pk.GetSlice(&tmp_str_);
    LOG(DFATAL) << "SaveEntry skipped empty PrimeValue with key: " << key << " with tag "
                << pv.Tag();
    return 0;
  }

  DVLOG(3) << "Selecting " << dbid << " previous: " << last_entry_db_index_;
  SelectDb(dbid);

  /* Save the expire time */
  if (expire_ms > 0) {
    uint8_t buf[16] = {RDB_OPCODE_EXPIRETIME_MS};
    absl::little_endian::Store64(buf + 1, expire_ms);
    if (auto ec = WriteRaw(Bytes{buf, 9}); ec)
      return make_unexpected(ec);
  }

  /* Save the key poperties */
  uint32_t df_mask_flags = pk.IsSticky() ? DF_MASK_FLAG_STICKY : 0;
  df_mask_flags |= pv.HasFlag() ? DF_MASK_FLAG_MC_FLAGS : 0;
  if (df_mask_flags != 0) {
    uint8_t buf[9] = {RDB_OPCODE_DF_MASK};
    absl::little_endian::Store32(buf + 1, df_mask_flags);
    size_t buf_size = 5;
    if (df_mask_flags & DF_MASK_FLAG_MC_FLAGS) {
      absl::little_endian::Store32(buf + buf_size, mc_flags);
      buf_size += 4;
    }
    if (auto ec = WriteRaw(Bytes{buf, buf_size}); ec)
      return make_unexpected(ec);
  }

  uint8_t rdb_type = RdbObjectType(pv);

  string_view key = pk.GetSlice(&tmp_str_);
  DVLOG(3) << ((void*)this) << ": Saving key/val start " << key << " in dbid=" << dbid;

  if (auto ec = WriteOpcode(rdb_type); ec)
    return make_unexpected(ec);

  if (auto ec = SaveString(key); ec)
    return make_unexpected(ec);

  if (auto ec = SaveValue(pv); ec) {
    LOG(ERROR) << "Problems saving value for key " << key << " in dbid=" << dbid;
    return make_unexpected(ec);
  }

  return rdb_type;
}

error_code RdbSerializer::SaveObject(const PrimeValue& pv) {
  unsigned obj_type = pv.ObjType();
  CHECK_NE(obj_type, OBJ_STRING);

  if (obj_type == OBJ_LIST) {
    return SaveListObject(pv);
  }

  if (obj_type == OBJ_SET) {
    return SaveSetObject(pv);
  }

  if (obj_type == OBJ_HASH) {
    return SaveHSetObject(pv);
  }

  if (obj_type == OBJ_ZSET) {
    return SaveZSetObject(pv);
  }

  if (obj_type == OBJ_STREAM) {
    return SaveStreamObject(pv);
  }

  if (obj_type == OBJ_JSON) {
    return SaveJsonObject(pv);
  }

  if (obj_type == OBJ_SBF) {
    return SaveSBFObject(pv);
  }

  LOG(ERROR) << "Not implemented " << obj_type;
  return make_error_code(errc::function_not_supported);
}

error_code RdbSerializer::SaveListObject(const PrimeValue& pv) {
  /* Save a list value */
  size_t len = 0;
  const quicklistNode* node = nullptr;

  if (pv.Encoding() == OBJ_ENCODING_QUICKLIST) {
    const quicklist* ql = reinterpret_cast<const quicklist*>(pv.RObjPtr());
    node = ql->head;
    DVLOG(2) << "Saving list of length " << ql->len;
    len = ql->len;
  } else {
    DCHECK_EQ(pv.Encoding(), kEncodingQL2);
    QList* ql = reinterpret_cast<QList*>(pv.RObjPtr());
    node = ql->Head();
    len = ql->node_count();
  }
  RETURN_ON_ERR(SaveLen(len));

  while (node) {
    DVLOG(3) << "QL node (encoding/container/sz): " << node->encoding << "/" << node->container
             << "/" << node->sz;

    if (absl::GetFlag(FLAGS_list_rdb_encode_v2)) {
      // Use listpack encoding
      SaveLen(node->container);
      if (quicklistNodeIsCompressed(node)) {
        void* data;
        size_t compress_len = quicklistGetLzf(node, &data);

        RETURN_ON_ERR(SaveLzfBlob(Bytes{reinterpret_cast<uint8_t*>(data), compress_len}, node->sz));
      } else {
        RETURN_ON_ERR(SaveString(node->entry, node->sz));
        FlushState flush_state = FlushState::kFlushMidEntry;
        if (node->next == nullptr)
          flush_state = FlushState::kFlushEndEntry;
        FlushIfNeeded(flush_state);
      }
    } else {
      // Use ziplist encoding
      if (QL_NODE_IS_PLAIN(node)) {
        RETURN_ON_ERR(SavePlainNodeAsZiplist(node));
      } else {
        // listpack node
        uint8_t* lp = node->entry;
        uint8_t* decompressed = NULL;

        if (quicklistNodeIsCompressed(node)) {
          void* data;
          size_t compress_len = quicklistGetLzf(node, &data);
          decompressed = (uint8_t*)zmalloc(node->sz);

          if (lzf_decompress(data, compress_len, decompressed, node->sz) == 0) {
            /* Someone requested decompress, but we can't decompress.  Not good. */
            zfree(decompressed);
            return make_error_code(errc::illegal_byte_sequence);
          }
          lp = decompressed;
        }

        auto cleanup = absl::MakeCleanup([=] {
          if (decompressed)
            zfree(decompressed);
        });
        RETURN_ON_ERR(SaveListPackAsZiplist(lp));
      }
    }
    node = node->next;
  }
  return error_code{};
}

error_code RdbSerializer::SaveSetObject(const PrimeValue& obj) {
  if (obj.Encoding() == kEncodingStrMap2) {
    StringSet* set = (StringSet*)obj.RObjPtr();

    RETURN_ON_ERR(SaveLen(set->SizeSlow()));
    for (auto it = set->begin(); it != set->end();) {
      RETURN_ON_ERR(SaveString(string_view{*it, sdslen(*it)}));
      if (set->ExpirationUsed()) {
        int64_t expiry = -1;
        if (it.HasExpiry())
          expiry = it.ExpiryTime();
        RETURN_ON_ERR(SaveLongLongAsString(expiry));
      }
      ++it;
      FlushState flush_state = FlushState::kFlushMidEntry;
      if (it == set->end())
        flush_state = FlushState::kFlushEndEntry;
      FlushIfNeeded(flush_state);
    }
  } else {
    CHECK_EQ(obj.Encoding(), kEncodingIntSet);
    intset* is = (intset*)obj.RObjPtr();
    size_t len = intsetBlobLen(is);

    RETURN_ON_ERR(SaveString(string_view{(char*)is, len}));
  }

  return error_code{};
}

error_code RdbSerializer::SaveHSetObject(const PrimeValue& pv) {
  DCHECK_EQ(OBJ_HASH, pv.ObjType());

  if (pv.Encoding() == kEncodingStrMap2) {
    StringMap* string_map = (StringMap*)pv.RObjPtr();

    RETURN_ON_ERR(SaveLen(string_map->SizeSlow()));

    for (auto it = string_map->begin(); it != string_map->end();) {
      const auto& [k, v] = *it;
      RETURN_ON_ERR(SaveString(string_view{k, sdslen(k)}));
      RETURN_ON_ERR(SaveString(string_view{v, sdslen(v)}));
      if (string_map->ExpirationUsed()) {
        int64_t expiry = -1;
        if (it.HasExpiry())
          expiry = it.ExpiryTime();
        RETURN_ON_ERR(SaveLongLongAsString(expiry));
      }
      ++it;
      FlushState flush_state = FlushState::kFlushMidEntry;
      if (it == string_map->end())
        flush_state = FlushState::kFlushEndEntry;
      FlushIfNeeded(flush_state);
    }
  } else {
    CHECK_EQ(kEncodingListPack, pv.Encoding());

    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    RETURN_ON_ERR(SaveListPackAsZiplist(lp));
  }

  return error_code{};
}

error_code RdbSerializer::SaveZSetObject(const PrimeValue& pv) {
  DCHECK_EQ(OBJ_ZSET, pv.ObjType());
  const detail::RobjWrapper* robj_wrapper = pv.GetRobjWrapper();
  if (pv.Encoding() == OBJ_ENCODING_SKIPLIST) {
    detail::SortedMap* zs = (detail::SortedMap*)robj_wrapper->inner_obj();

    RETURN_ON_ERR(SaveLen(zs->Size()));
    std::error_code ec;

    /* We save the skiplist elements from the greatest to the smallest
     * (that's trivial since the elements are already ordered in the
     * skiplist): this improves the load process, since the next loaded
     * element will always be the smaller, so adding to the skiplist
     * will always immediately stop at the head, making the insertion
     * O(1) instead of O(log(N)). */
    const size_t total = zs->Size();
    size_t count = 0;
    zs->Iterate(0, total, true, [&](sds ele, double score) mutable {
      ec = SaveString(string_view{ele, sdslen(ele)});
      if (ec)
        return false;
      ec = SaveBinaryDouble(score);
      if (ec)
        return false;
      ++count;
      FlushState flush_state = FlushState::kFlushMidEntry;
      if (count == total)
        flush_state = FlushState::kFlushEndEntry;

      FlushIfNeeded(flush_state);
      return true;
    });
  } else {
    CHECK_EQ(pv.Encoding(), unsigned(OBJ_ENCODING_LISTPACK)) << "Unknown zset encoding";
    uint8_t* lp = (uint8_t*)robj_wrapper->inner_obj();
    RETURN_ON_ERR(SaveListPackAsZiplist(lp));
  }

  return error_code{};
}

error_code RdbSerializer::SaveStreamObject(const PrimeValue& pv) {
  /* Store how many listpacks we have inside the radix tree. */
  stream* s = (stream*)pv.RObjPtr();
  rax* rax = s->rax_tree;

  const size_t rax_size = raxSize(rax);

  RETURN_ON_ERR(SaveLen(rax_size));

  /* Serialize all the listpacks inside the radix tree as they are,
   * when loading back, we'll use the first entry of each listpack
   * to insert it back into the radix tree. */
  raxIterator ri;
  raxStart(&ri, rax);
  raxSeek(&ri, "^", NULL, 0);

  auto stop_listpacks_rax = absl::MakeCleanup([&] { raxStop(&ri); });

  for (size_t i = 0; raxNext(&ri); i++) {
    uint8_t* lp = (uint8_t*)ri.data;
    size_t lp_bytes = lpBytes(lp);

    RETURN_ON_ERR(SaveString((uint8_t*)ri.key, ri.key_len));
    RETURN_ON_ERR(SaveString(lp, lp_bytes));

    const FlushState flush_state =
        (i + 1 < rax_size) ? FlushState::kFlushMidEntry : FlushState::kFlushEndEntry;
    FlushIfNeeded(flush_state);
  }

  std::move(stop_listpacks_rax).Invoke();

  /* Save the number of elements inside the stream. We cannot obtain
   * this easily later, since our macro nodes should be checked for
   * number of items: not a great CPU / space tradeoff. */

  RETURN_ON_ERR(SaveLen(s->length));

  /* Save the last entry ID. */
  RETURN_ON_ERR(SaveLen(s->last_id.ms));
  RETURN_ON_ERR(SaveLen(s->last_id.seq));

  uint8_t rdb_type = RdbObjectType(pv);

  // 'first_id', 'max_deleted_entry_id' and 'entries_added' are added
  // in RDB_TYPE_STREAM_LISTPACKS_2
  if (rdb_type >= RDB_TYPE_STREAM_LISTPACKS_2) {
    /* Save the first entry ID. */
    RETURN_ON_ERR(SaveLen(s->first_id.ms));
    RETURN_ON_ERR(SaveLen(s->first_id.seq));

    /* Save the maximal tombstone ID. */
    RETURN_ON_ERR(SaveLen(s->max_deleted_entry_id.ms));
    RETURN_ON_ERR(SaveLen(s->max_deleted_entry_id.seq));

    /* Save the offset. */
    RETURN_ON_ERR(SaveLen(s->entries_added));
  }
  /* The consumer groups and their clients are part of the stream
   * type, so serialize every consumer group. */

  /* Save the number of groups. */
  size_t num_cgroups = s->cgroups ? raxSize(s->cgroups) : 0;
  RETURN_ON_ERR(SaveLen(num_cgroups));

  if (num_cgroups) {
    /* Serialize each consumer group. */
    raxStart(&ri, s->cgroups);
    raxSeek(&ri, "^", NULL, 0);

    auto stop_cgroups_rax = absl::MakeCleanup([&] { raxStop(&ri); });

    while (raxNext(&ri)) {
      streamCG* cg = (streamCG*)ri.data;

      /* Save the group name. */
      RETURN_ON_ERR(SaveString((uint8_t*)ri.key, ri.key_len));

      /* Last ID. */
      RETURN_ON_ERR(SaveLen(cg->last_id.ms));

      RETURN_ON_ERR(SaveLen(cg->last_id.seq));

      if (rdb_type >= RDB_TYPE_STREAM_LISTPACKS_2) {
        /* Save the group's logical reads counter. */
        RETURN_ON_ERR(SaveLen(cg->entries_read));
      }

      /* Save the global PEL. */
      RETURN_ON_ERR(SaveStreamPEL(cg->pel, true));

      /* Save the consumers of this group. */
      RETURN_ON_ERR(SaveStreamConsumers(rdb_type >= RDB_TYPE_STREAM_LISTPACKS_3, cg));
    }
  }

  return error_code{};
}

error_code RdbSerializer::SaveJsonObject(const PrimeValue& pv) {
  auto json_string = pv.GetJson()->to_string();
  return SaveString(json_string);
}

std::error_code RdbSerializer::SaveSBFObject(const PrimeValue& pv) {
  SBF* sbf = pv.GetSBF();

  // options to allow format mutations in the future.
  RETURN_ON_ERR(SaveLen(0));  // options - reserved
  RETURN_ON_ERR(SaveBinaryDouble(sbf->grow_factor()));
  RETURN_ON_ERR(SaveBinaryDouble(sbf->fp_probability()));
  RETURN_ON_ERR(SaveLen(sbf->prev_size()));
  RETURN_ON_ERR(SaveLen(sbf->current_size()));
  RETURN_ON_ERR(SaveLen(sbf->max_capacity()));
  RETURN_ON_ERR(SaveLen(sbf->num_filters()));

  for (unsigned i = 0; i < sbf->num_filters(); ++i) {
    RETURN_ON_ERR(SaveLen(sbf->hashfunc_cnt(i)));

    string_view blob = sbf->data(i);
    RETURN_ON_ERR(SaveString(blob));
    FlushState flush_state = FlushState::kFlushMidEntry;
    if ((i + 1) == sbf->num_filters())
      flush_state = FlushState::kFlushEndEntry;

    FlushIfNeeded(flush_state);
  }

  return {};
}

/* Save a long long value as either an encoded string or a string. */
error_code RdbSerializer::SaveLongLongAsString(int64_t value) {
  uint8_t buf[32];
  unsigned enclen = EncodeInteger(value, buf);
  if (enclen > 0) {
    return WriteRaw(Bytes{buf, enclen});
  }

  /* Encode as string */
  enclen = ll2string((char*)buf, 32, value);
  DCHECK_LT(enclen, 32u);

  RETURN_ON_ERR(SaveLen(enclen));
  return WriteRaw(Bytes{buf, enclen});
}

/* Saves a double for RDB 8 or greater, where IE754 binary64 format is assumed.
 * We just make sure the integer is always stored in little endian, otherwise
 * the value is copied verbatim from memory to disk.
 *
 * Return -1 on error, the size of the serialized value on success. */
error_code RdbSerializer::SaveBinaryDouble(double val) {
  static_assert(sizeof(val) == 8);
  const uint64_t* src = reinterpret_cast<const uint64_t*>(&val);
  uint8_t buf[8];
  absl::little_endian::Store64(buf, *src);

  return WriteRaw(Bytes{buf, sizeof(buf)});
}

error_code RdbSerializer::SaveListPackAsZiplist(uint8_t* lp) {
  uint8_t* lpfield = lpFirst(lp);
  int64_t entry_len;
  uint8_t* entry;
  uint8_t buf[32];
  uint8_t* zl = ziplistNew();

  while (lpfield) {
    entry = lpGet(lpfield, &entry_len, buf);
    zl = ziplistPush(zl, entry, entry_len, ZIPLIST_TAIL);
    lpfield = lpNext(lp, lpfield);
  }
  size_t ziplen = ziplistBlobLen(zl);
  error_code ec = SaveString(string_view{reinterpret_cast<char*>(zl), ziplen});
  zfree(zl);

  return ec;
}

error_code RdbSerializer::SavePlainNodeAsZiplist(const quicklistNode* node) {
  uint8_t* zl = ziplistNew();
  zl = ziplistPush(zl, node->entry, node->sz, ZIPLIST_TAIL);

  size_t ziplen = ziplistBlobLen(zl);
  error_code ec = SaveString(string_view{reinterpret_cast<char*>(zl), ziplen});
  zfree(zl);

  return ec;
}

error_code RdbSerializer::SaveStreamPEL(rax* pel, bool nacks) {
  /* Number of entries in the PEL. */

  RETURN_ON_ERR(SaveLen(raxSize(pel)));

  /* Save each entry. */
  raxIterator ri;
  raxStart(&ri, pel);
  raxSeek(&ri, "^", NULL, 0);
  auto cleanup = absl::MakeCleanup([&] { raxStop(&ri); });

  while (raxNext(&ri)) {
    /* We store IDs in raw form as 128 big big endian numbers, like
     * they are inside the radix tree key. */
    RETURN_ON_ERR(WriteRaw(Bytes{ri.key, sizeof(streamID)}));

    if (nacks) {
      streamNACK* nack = (streamNACK*)ri.data;
      uint8_t buf[8];
      absl::little_endian::Store64(buf, nack->delivery_time);
      RETURN_ON_ERR(WriteRaw(buf));
      RETURN_ON_ERR(SaveLen(nack->delivery_count));

      /* We don't save the consumer name: we'll save the pending IDs
       * for each consumer in the consumer PEL, and resolve the consumer
       * at loading time. */
    }
  }

  return error_code{};
}

error_code RdbSerializer::SaveStreamConsumers(bool save_active, streamCG* cg) {
  /* Number of consumers in this consumer group. */

  RETURN_ON_ERR(SaveLen(raxSize(cg->consumers)));

  /* Save each consumer. */
  raxIterator ri;
  raxStart(&ri, cg->consumers);
  raxSeek(&ri, "^", NULL, 0);
  auto cleanup = absl::MakeCleanup([&] { raxStop(&ri); });
  uint8_t buf[8];

  while (raxNext(&ri)) {
    streamConsumer* consumer = (streamConsumer*)ri.data;

    /* Consumer name. */
    RETURN_ON_ERR(SaveString(ri.key, ri.key_len));

    /* seen time. */
    absl::little_endian::Store64(buf, consumer->seen_time);
    RETURN_ON_ERR(WriteRaw(buf));

    if (save_active) {
      /* Active time. */
      absl::little_endian::Store64(buf, consumer->active_time);
      RETURN_ON_ERR(WriteRaw(buf));
    }
    /* Consumer PEL, without the ACKs (see last parameter of the function
     * passed with value of 0), at loading time we'll lookup the ID
     * in the consumer group global PEL and will put a reference in the
     * consumer local PEL. */

    RETURN_ON_ERR(SaveStreamPEL(consumer->pel, false));
  }

  return error_code{};
}

error_code RdbSerializer::SendEofAndChecksum() {
  VLOG(2) << "SendEof";
  /* EOF opcode */
  RETURN_ON_ERR(WriteOpcode(RDB_OPCODE_EOF));

  /* CRC64 checksum. It will be zero if checksum computation is disabled, the
   * loading code skips the check in this case. */
  uint8_t buf[8];
  uint64_t chksum = 0;

  absl::little_endian::Store64(buf, chksum);
  return WriteRaw(buf);
}

error_code RdbSerializer::SendJournalOffset(uint64_t journal_offset) {
  VLOG(2) << "SendJournalOffset";
  RETURN_ON_ERR(WriteOpcode(RDB_OPCODE_JOURNAL_OFFSET));
  uint8_t buf[sizeof(uint64_t)];
  absl::little_endian::Store64(buf, journal_offset);
  return WriteRaw(buf);
}

error_code SerializerBase::SendFullSyncCut() {
  VLOG(2) << "SendFullSyncCut";
  RETURN_ON_ERR(WriteOpcode(RDB_OPCODE_FULLSYNC_END));

  // RDB_OPCODE_FULLSYNC_END followed by 8 bytes of 0.
  // The reason for this is that some opcodes require to have at least 8 bytes of data
  // in the read buffer when consuming the rdb data, and since RDB_OPCODE_FULLSYNC_END is one of
  // the last opcodes sent to replica, we respect this requirement by sending a blob of 8 bytes.
  uint8_t buf[8] = {0};
  return WriteRaw(buf);
}

std::error_code SerializerBase::WriteOpcode(uint8_t opcode) {
  return WriteRaw(::io::Bytes{&opcode, 1});
}

size_t SerializerBase::GetBufferCapacity() const {
  return mem_buf_.Capacity();
}

size_t SerializerBase::GetTempBufferSize() const {
  return tmp_buf_.size();
}

error_code SerializerBase::WriteRaw(const io::Bytes& buf) {
  mem_buf_.Reserve(mem_buf_.InputLen() + buf.size());
  IoBuf::Bytes dest = mem_buf_.AppendBuffer();
  memcpy(dest.data(), buf.data(), buf.size());
  mem_buf_.CommitWrite(buf.size());
  return error_code{};
}

error_code SerializerBase::FlushToSink(io::Sink* sink, SerializerBase::FlushState flush_state) {
  auto bytes = PrepareFlush(flush_state);
  if (bytes.empty())
    return error_code{};

  DVLOG(2) << "FlushToSink " << bytes.size() << " bytes";

  // interrupt point.
  RETURN_ON_ERR(sink->Write(bytes));
  mem_buf_.ConsumeInput(bytes.size());

  return error_code{};
}

error_code RdbSerializer::FlushToSink(io::Sink* s, SerializerBase::FlushState flush_state) {
  RETURN_ON_ERR(SerializerBase::FlushToSink(s, flush_state));

  // After every flush we should write the DB index again because the blobs in the channel are
  // interleaved and multiple savers can correspond to a single writer (in case of single file rdb
  // snapshot)
  last_entry_db_index_ = kInvalidDbId;

  return {};
}

namespace {
using VersionBuffer = std::array<char, sizeof(uint16_t)>;
using CrcBuffer = std::array<char, sizeof(uint64_t)>;

VersionBuffer MakeRdbVersion() {
  VersionBuffer buf;
  buf[0] = RDB_SER_VERSION & 0xff;
  buf[1] = (RDB_SER_VERSION >> 8) & 0xff;
  return buf;
}

CrcBuffer MakeCheckSum(std::string_view dump_res) {
  uint64_t chksum = crc64(0, reinterpret_cast<const uint8_t*>(dump_res.data()), dump_res.size());
  CrcBuffer buf;
  absl::little_endian::Store64(buf.data(), chksum);
  return buf;
}

void AppendFooter(io::StringSink* dump_res) {
  auto to_bytes = [](const auto& buf) {
    return io::Bytes(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
  };

  /* Write the footer, this is how it looks like:
   * ----------------+---------------------+---------------+
   * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
   * ----------------+---------------------+---------------+
   * RDB version and CRC are both in little endian.
   */
  const auto ver = MakeRdbVersion();
  dump_res->Write(to_bytes(ver));
  const auto crc = MakeCheckSum(dump_res->str());
  dump_res->Write(to_bytes(crc));
}
}  // namespace

void SerializerBase::DumpObject(const CompactObj& obj, io::StringSink* out) {
  CompressionMode compression_mode = GetDefaultCompressionMode();
  if (compression_mode != CompressionMode::NONE) {
    compression_mode = CompressionMode::SINGLE_ENTRY;
  }
  RdbSerializer serializer(compression_mode);

  // According to Redis code we need to
  // 1. Save the value itself - without the key
  // 2. Save footer: this include the RDB version and the CRC value for the message
  auto type = RdbObjectType(obj);
  DVLOG(2) << "We are going to dump object type: " << int(type);

  std::error_code ec = serializer.WriteOpcode(type);
  CHECK(!ec);
  ec = serializer.SaveValue(obj);
  CHECK(!ec);  // make sure that fully was successful
  ec = serializer.FlushToSink(out, SerializerBase::FlushState::kFlushMidEntry);
  CHECK(!ec);         // make sure that fully was successful
  AppendFooter(out);  // version and crc
  CHECK_GT(out->str().size(), 10u);
}

size_t SerializerBase::SerializedLen() const {
  return mem_buf_.InputLen();
}

io::Bytes SerializerBase::PrepareFlush(SerializerBase::FlushState flush_state) {
  size_t sz = mem_buf_.InputLen();
  if (sz == 0)
    return mem_buf_.InputBuffer();

  bool is_last_chunk = flush_state == FlushState::kFlushEndEntry;
  VLOG(2) << "PrepareFlush:" << is_last_chunk << " " << number_of_chunks_;
  if (is_last_chunk && number_of_chunks_ == 0) {
    if (compression_mode_ == CompressionMode::MULTI_ENTRY_ZSTD ||
        compression_mode_ == CompressionMode::MULTI_ENTRY_LZ4) {
      CompressBlob();
    }
  }

  number_of_chunks_ = is_last_chunk ? 0 : (number_of_chunks_ + 1);

  return mem_buf_.InputBuffer();
}

error_code SerializerBase::WriteJournalEntry(std::string_view serialized_entry) {
  VLOG(2) << "WriteJournalEntry";
  RETURN_ON_ERR(WriteOpcode(RDB_OPCODE_JOURNAL_BLOB));
  RETURN_ON_ERR(SaveLen(1));
  RETURN_ON_ERR(SaveString(serialized_entry));
  return error_code{};
}

error_code SerializerBase::SaveString(string_view val) {
  /* Try integer encoding */
  if (val.size() <= 11) {
    uint8_t buf[16];

    unsigned enclen = TryIntegerEncoding(val, buf);
    if (enclen > 0) {
      return WriteRaw(Bytes{buf, unsigned(enclen)});
    }
  }

  /* Try LZF compression - under 20 bytes it's unable to compress even
   * aaaaaaaaaaaaaaaaaa so skip it */
  size_t len = val.size();
  if ((compression_mode_ == CompressionMode::SINGLE_ENTRY) && (len > 20)) {
    size_t comprlen, outlen = len;
    tmp_buf_.resize(outlen + 1);

    // Due to stack constraints im fibers we can not allow large arrays on stack.
    // Therefore I am lazily allocating it on heap. It's not fixed in quicklist.
    if (!lzf_) {
      lzf_.reset(new LZF_HSLOT[1 << HLOG]);
    }

    /* We require at least 8 bytes compression for this to be worth it */
    comprlen = lzf_compress(val.data(), len, tmp_buf_.data(), outlen, lzf_.get());
    if (comprlen > 0 && comprlen < len - 8 && comprlen < size_t(len * 0.85)) {
      return SaveLzfBlob(Bytes{tmp_buf_.data(), comprlen}, len);
    }
  }

  /* Store verbatim */
  RETURN_ON_ERR(SaveLen(len));
  if (len > 0) {
    Bytes b{reinterpret_cast<const uint8_t*>(val.data()), val.size()};
    RETURN_ON_ERR(WriteRaw(b));
  }
  return error_code{};
}

error_code SerializerBase::SaveLen(size_t len) {
  uint8_t buf[16];
  unsigned enclen = WritePackedUInt(len, buf);
  return WriteRaw(Bytes{buf, enclen});
}

error_code SerializerBase::SaveLzfBlob(const io::Bytes& src, size_t uncompressed_len) {
  /* Data compressed! Let's save it on disk */
  uint8_t opcode = (RDB_ENCVAL << 6) | RDB_ENC_LZF;
  RETURN_ON_ERR(WriteOpcode(opcode));
  RETURN_ON_ERR(SaveLen(src.size()));
  RETURN_ON_ERR(SaveLen(uncompressed_len));
  RETURN_ON_ERR(WriteRaw(src));

  return error_code{};
}

AlignedBuffer::AlignedBuffer(size_t cap, ::io::Sink* upstream)
    : capacity_(cap), upstream_(upstream) {
  aligned_buf_ = (char*)mi_malloc_aligned(kBufLen, 4_KB);
}

AlignedBuffer::~AlignedBuffer() {
  mi_free(aligned_buf_);
}

io::Result<size_t> AlignedBuffer::WriteSome(const iovec* v, uint32_t len) {
  size_t total_len = 0;
  uint32_t vindx = 0;

  for (; vindx < len; ++vindx) {
    auto item = v[vindx];
    total_len += item.iov_len;

    while (buf_offs_ + item.iov_len > capacity_) {
      size_t to_write = capacity_ - buf_offs_;
      memcpy(aligned_buf_ + buf_offs_, item.iov_base, to_write);
      iovec ivec{.iov_base = aligned_buf_, .iov_len = capacity_};
      error_code ec = upstream_->Write(&ivec, 1);
      if (ec)
        return nonstd::make_unexpected(ec);

      item.iov_len -= to_write;
      item.iov_base = reinterpret_cast<char*>(item.iov_base) + to_write;
      buf_offs_ = 0;
    }

    DCHECK_GT(item.iov_len, 0u);
    memcpy(aligned_buf_ + buf_offs_, item.iov_base, item.iov_len);
    buf_offs_ += item.iov_len;
  }

  return total_len;
}

// Note that it may write more than AlignedBuffer has at this point since it rounds up the length
// to the nearest page boundary.
error_code AlignedBuffer::Flush() {
  size_t len = (buf_offs_ + kAmask) & (~kAmask);
  if (len == 0)
    return error_code{};

  iovec ivec{.iov_base = aligned_buf_, .iov_len = len};
  buf_offs_ = 0;

  return upstream_->Write(&ivec, 1);
}

class RdbSaver::Impl final : public SliceSnapshot::SnapshotDataConsumerInterface {
 private:
  void CleanShardSnapshots();

 public:
  // We pass K=sz to say how many producers are pushing data in order to maintain
  // correct closing semantics - channel is closing when K producers marked it as closed.
  Impl(bool align_writes, unsigned producers_len, CompressionMode compression_mode,
       SaveMode save_mode, io::Sink* sink);

  ~Impl();

  void StartSnapshotting(bool stream_journal, Context* cntx, EngineShard* shard);
  void StartIncrementalSnapshotting(LSN start_lsn, Context* cntx, EngineShard* shard);

  void StopSnapshotting(EngineShard* shard);
  void WaitForSnapshottingFinish(EngineShard* shard);

  // Pushes snapshot data. Called from SliceSnapshot
  void ConsumeData(std::string data, Context* cntx) override;
  // Finalizes the snapshot writing. Called from SliceSnapshot
  void Finalize() override;

  // used only for legacy rdb save flows.
  error_code ConsumeChannel(const Cancellation* cll);

  void FillFreqMap(RdbTypeFreqMap* dest) const;

  error_code SaveAuxFieldStrStr(string_view key, string_view val);

  void CancelInShard(EngineShard* shard);

  size_t GetTotalBuffersSize() const;

  RdbSaver::SnapshotStats GetCurrentSnapshotProgress() const;

  error_code FlushSerializer();

  error_code FlushSink() {
    return aligned_buf_ ? aligned_buf_->Flush() : error_code{};
  }

  size_t Size() const {
    return shard_snapshots_.size();
  }

  RdbSerializer* serializer() {
    return &meta_serializer_;
  }

  int64_t last_write_ts() const {
    return last_write_time_ns_;
  }

 private:
  error_code WriteRecord(io::Bytes src);

  unique_ptr<SliceSnapshot>& GetSnapshot(EngineShard* shard);

  io::Sink* sink_;
  int64_t last_write_time_ns_ = -1;  // last write call.
  vector<unique_ptr<SliceSnapshot>> shard_snapshots_;
  // used for serializing non-body components in the calling fiber.
  RdbSerializer meta_serializer_;
  using RecordChannel = SizeTrackingChannel<string, base::mpmc_bounded_queue<string>>;
  std::optional<RecordChannel> channel_;
  std::optional<AlignedBuffer> aligned_buf_;

  // Single entry compression is compatible with redis rdb snapshot
  // Multi entry compression is available only on df snapshot, this will
  // make snapshot size smaller and opreation faster.
  CompressionMode compression_mode_;
  SaveMode save_mode_;
};

// We pass K=sz to say how many producers are pushing data in order to maintain
// correct closing semantics - channel is closing when K producers marked it as closed.
RdbSaver::Impl::Impl(bool align_writes, unsigned producers_len, CompressionMode compression_mode,
                     SaveMode sm, io::Sink* sink)
    : sink_(sink),
      shard_snapshots_(producers_len),
      meta_serializer_(CompressionMode::NONE),  // Note: I think there is not need for compression
                                                // at all in meta serializer
      compression_mode_(compression_mode) {
  if (align_writes) {
    aligned_buf_.emplace(kBufLen, sink);
    sink_ = &aligned_buf_.value();
  }
  if (sm == SaveMode::RDB) {
    channel_.emplace(kChannelLen, producers_len);
  }
  save_mode_ = sm;
}

void RdbSaver::Impl::CleanShardSnapshots() {
  if (shard_snapshots_.empty()) {
    return;
  }

  auto cb = [this](ShardId sid) {
    // Destroy SliceSnapshot in target thread, as it registers itself in a thread local set.
    shard_snapshots_[sid].reset();
  };

  if (shard_snapshots_.size() == 1) {
    cb(0);
  } else {
    shard_set->RunBlockingInParallel([&](EngineShard* es) { cb(es->shard_id()); });
  }
}

RdbSaver::Impl::~Impl() {
  CleanShardSnapshots();
}

error_code RdbSaver::Impl::SaveAuxFieldStrStr(string_view key, string_view val) {
  auto& ser = meta_serializer_;
  RETURN_ON_ERR(ser.WriteOpcode(RDB_OPCODE_AUX));
  RETURN_ON_ERR(ser.SaveString(key));
  RETURN_ON_ERR(ser.SaveString(val));

  return error_code{};
}

error_code RdbSaver::Impl::ConsumeChannel(const Cancellation* cll) {
  error_code io_error;
  string record;

  auto& stats = ServerState::tlocal()->stats;
  DCHECK(channel_.has_value());
  // we can not exit on io-error since we spawn fibers that push data.
  // TODO: we may signal them to stop processing and exit asap in case of the error.
  while (channel_->Pop(record)) {
    if (io_error || cll->IsCancelled())
      continue;

    do {
      if (cll->IsCancelled())
        continue;

      auto start = absl::GetCurrentTimeNanos();
      io_error = WriteRecord(io::Buffer(record));
      if (io_error) {
        break;  // from the inner TryPop loop.
      }

      auto delta_usec = (absl::GetCurrentTimeNanos() - start) / 1'000;
      stats.rdb_save_usec += delta_usec;
      stats.rdb_save_count++;
    } while ((channel_->TryPop(record)));
  }  // while (channel_.Pop())

  for (auto& ptr : shard_snapshots_) {
    ptr->WaitSnapshotting();
  }
  VLOG(1) << "ConsumeChannel finished " << io_error;

  DCHECK(!channel_->TryPop(record));

  return io_error;
}

error_code RdbSaver::Impl::WriteRecord(io::Bytes src) {
  // For huge values, we break them up into chunks of upto several MBs to send in a single call,
  // so we could be more responsive.
  error_code ec;
  size_t start_size = src.size();
  last_write_time_ns_ = absl::GetCurrentTimeNanos();
  do {
    io::Bytes part = src.subspan(0, 8_MB);
    src.remove_prefix(part.size());

    ec = sink_->Write(part);

    int64_t now = absl::GetCurrentTimeNanos();
    unsigned delta_ms = (now - last_write_time_ns_) / 1000'000;
    last_write_time_ns_ = now;

    // Log extreme timings into the log for visibility.
    LOG_IF(INFO, delta_ms > 1000) << "Channel write took " << delta_ms << " ms while writing "
                                  << part.size() << "/" << start_size;
    if (ec) {
      LOG(INFO) << "Error writing to rdb sink " << ec.message();
      break;
    }
  } while (!src.empty());
  last_write_time_ns_ = -1;
  return ec;
}

void RdbSaver::Impl::StartSnapshotting(bool stream_journal, Context* cntx, EngineShard* shard) {
  auto& s = GetSnapshot(shard);
  auto& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());

  s = std::make_unique<SliceSnapshot>(compression_mode_, &db_slice, this, cntx);

  const auto allow_flush = (save_mode_ != SaveMode::RDB) ? SliceSnapshot::SnapshotFlush::kAllow
                                                         : SliceSnapshot::SnapshotFlush::kDisallow;

  s->Start(stream_journal, allow_flush);
}

void RdbSaver::Impl::StartIncrementalSnapshotting(LSN start_lsn, Context* cntx,
                                                  EngineShard* shard) {
  auto& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
  auto& s = GetSnapshot(shard);

  s = std::make_unique<SliceSnapshot>(compression_mode_, &db_slice, this, cntx);

  s->StartIncremental(start_lsn);
}

// called on save flow
void RdbSaver::Impl::WaitForSnapshottingFinish(EngineShard* shard) {
  auto& snapshot = GetSnapshot(shard);
  CHECK(snapshot);
  snapshot->WaitSnapshotting();
}

void RdbSaver::Impl::ConsumeData(std::string data, Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }
  if (channel_) {  // Rdb write to channel
    channel_->Push(std::move(data));
  } else {  // Write directly to socket
    auto ec = WriteRecord(io::Buffer(data));
    if (ec) {
      cntx->ReportError(ec);
    }
  }
}

void RdbSaver::Impl::Finalize() {
  if (channel_) {
    channel_->StartClosing();
  }
}

// called from replication flow
void RdbSaver::Impl::StopSnapshotting(EngineShard* shard) {
  auto& snapshot = GetSnapshot(shard);
  CHECK(snapshot);
  snapshot->FinalizeJournalStream(false);
}

void RdbSaver::Impl::CancelInShard(EngineShard* shard) {
  auto& snapshot = GetSnapshot(shard);
  if (snapshot) {  // Cancel can be called before snapshotting started.
    snapshot->FinalizeJournalStream(true);
  }
}

// This function is called from connection thread when info command is invoked.
// All accessed variableds must be thread safe, as they are fetched not from the rdb saver thread.
size_t RdbSaver::Impl::GetTotalBuffersSize() const {
  std::atomic<size_t> channel_bytes{0};
  std::atomic<size_t> serializer_bytes{0};

  auto cb = [this, &channel_bytes, &serializer_bytes](ShardId sid) {
    auto& snapshot = shard_snapshots_[sid];
    // before create a snapshot we save header so shard_snapshots_ are vector of nullptr until we
    // start snapshots saving
    if (!snapshot)
      return;
    if (channel_.has_value())
      channel_bytes.fetch_add(channel_->GetSize(), memory_order_relaxed);
    serializer_bytes.store(snapshot->GetBufferCapacity() + snapshot->GetTempBuffersSize(),
                           memory_order_relaxed);
  };

  if (shard_snapshots_.size() == 1) {
    cb(0);
  } else {
    shard_set->RunBriefInParallel([&](EngineShard* es) { cb(es->shard_id()); });
  }

  VLOG(2) << "channel_bytes:" << channel_bytes.load(memory_order_relaxed)
          << " serializer_bytes: " << serializer_bytes.load(memory_order_relaxed);
  return channel_bytes.load(memory_order_relaxed) + serializer_bytes.load(memory_order_relaxed);
}

RdbSaver::SnapshotStats RdbSaver::Impl::GetCurrentSnapshotProgress() const {
  std::vector<RdbSaver::SnapshotStats> results(shard_snapshots_.size());

  auto cb = [this, &results](ShardId sid) {
    auto& snapshot = shard_snapshots_[sid];
    // before create a snapshot we save header so shard_snapshots_ are vector of nullptr until we
    // start snapshots saving
    if (!snapshot)
      return;
    results[sid] = snapshot->GetCurrentSnapshotProgress();
  };

  if (shard_snapshots_.size() == 1) {
    cb(0);
    return results[0];
  }

  shard_set->RunBriefInParallel([&](EngineShard* es) { cb(es->shard_id()); });
  RdbSaver::SnapshotStats init{0, 0};
  return std::accumulate(
      results.begin(), results.end(), init, [](auto init, auto pr) -> RdbSaver::SnapshotStats {
        return {init.current_keys + pr.current_keys, init.total_keys + pr.total_keys};
      });
}

error_code RdbSaver::Impl::FlushSerializer() {
  last_write_time_ns_ = absl::GetCurrentTimeNanos();
  auto ec = serializer()->FlushToSink(sink_, SerializerBase::FlushState::kFlushMidEntry);
  last_write_time_ns_ = -1;
  return ec;
}

RdbSaver::GlobalData RdbSaver::GetGlobalData(const Service* service) {
  StringVec script_bodies, search_indices;

  {
    auto scripts = service->script_mgr()->GetAll();
    script_bodies.reserve(scripts.size());
    for (auto& [sha, data] : scripts)
      script_bodies.push_back(std::move(data.body));
  }

  {
    shard_set->Await(0, [&] {
      auto* indices = EngineShard::tlocal()->search_indices();
      for (auto index_name : indices->GetIndexNames()) {
        auto index_info = indices->GetIndex(index_name)->GetInfo();
        search_indices.emplace_back(
            absl::StrCat(index_name, " ", index_info.BuildRestoreCommand()));
      }
    });
  }

  return RdbSaver::GlobalData{std::move(script_bodies), std::move(search_indices)};
}

void RdbSaver::Impl::FillFreqMap(RdbTypeFreqMap* dest) const {
  for (auto& ptr : shard_snapshots_) {
    const RdbTypeFreqMap& src_map = ptr->freq_map();
    for (const auto& k_v : src_map)
      (*dest)[k_v.first] += k_v.second;
  }
}

unique_ptr<SliceSnapshot>& RdbSaver::Impl::GetSnapshot(EngineShard* shard) {
  // For single shard configuration, we maintain only one snapshot,
  // so we do not have to map it via shard_id.
  unsigned sid = shard_snapshots_.size() == 1 ? 0 : shard->shard_id();
  CHECK(sid < shard_snapshots_.size());
  return shard_snapshots_[sid];
}

RdbSaver::RdbSaver(::io::Sink* sink, SaveMode save_mode, bool align_writes) {
  CHECK_NOTNULL(sink);
  CompressionMode compression_mode = GetDefaultCompressionMode();
  int producer_count = 0;
  switch (save_mode) {
    case SaveMode::SUMMARY:
      producer_count = 0;
      if (compression_mode >= CompressionMode::SINGLE_ENTRY) {
        compression_mode_ = CompressionMode::SINGLE_ENTRY;
      } else {
        compression_mode_ = CompressionMode::NONE;
      }
      break;
    case SaveMode::SINGLE_SHARD:
    case SaveMode::SINGLE_SHARD_WITH_SUMMARY:
      producer_count = 1;
      compression_mode_ = compression_mode;
      break;
    case SaveMode::RDB:
      producer_count = shard_set->size();
      if (compression_mode >= CompressionMode::SINGLE_ENTRY) {
        compression_mode_ = CompressionMode::SINGLE_ENTRY;
      } else {
        compression_mode_ = CompressionMode::NONE;
      }
      break;
  }
  VLOG(1) << "Rdb save using compression mode:" << uint32_t(compression_mode_);
  impl_.reset(new Impl(align_writes, producer_count, compression_mode_, save_mode, sink));
  save_mode_ = save_mode;
}

RdbSaver::~RdbSaver() {
  // Decommit local memory.
  // We create an RdbSaver for each thread, so each one will Decommit for itself.
  auto* tlocal = ServerState::tlocal();
  tlocal->DecommitMemory(ServerState::kAllMemory);
}

void RdbSaver::StartSnapshotInShard(bool stream_journal, Context* cntx, EngineShard* shard) {
  impl_->StartSnapshotting(stream_journal, cntx, shard);
}

void RdbSaver::StartIncrementalSnapshotInShard(LSN start_lsn, Context* cntx, EngineShard* shard) {
  impl_->StartIncrementalSnapshotting(start_lsn, cntx, shard);
}

error_code RdbSaver::WaitSnapshotInShard(EngineShard* shard) {
  impl_->WaitForSnapshottingFinish(shard);
  return SaveEpilog();
}

error_code RdbSaver::StopFullSyncInShard(EngineShard* shard) {
  impl_->StopSnapshotting(shard);
  return SaveEpilog();
}

error_code RdbSaver::SaveHeader(const GlobalData& glob_state) {
  char magic[16];
  // We should use RDB_VERSION here from rdb.h when we ditch redis 6 support
  // For now we serialize to an older version.
  size_t sz = absl::SNPrintF(magic, sizeof(magic), "REDIS%04d", RDB_SER_VERSION);
  CHECK_EQ(9u, sz);

  RETURN_ON_ERR(impl_->serializer()->WriteRaw(Bytes{reinterpret_cast<uint8_t*>(magic), sz}));
  RETURN_ON_ERR(SaveAux(std::move(glob_state)));
  RETURN_ON_ERR(impl_->FlushSerializer());
  return error_code{};
}

error_code RdbSaver::SaveBody(const Context& cntx) {
  RETURN_ON_ERR(impl_->FlushSerializer());

  if (save_mode_ == SaveMode::RDB) {
    VLOG(1) << "SaveBody , snapshots count: " << impl_->Size();
    error_code io_error = impl_->ConsumeChannel(cntx.GetCancellation());
    if (io_error) {
      return io_error;
    }
    if (cntx.GetError()) {
      return cntx.GetError();
    }
  } else {
    DCHECK(save_mode_ == SaveMode::SUMMARY);
  }

  return SaveEpilog();
}

void RdbSaver::FillFreqMap(RdbTypeFreqMap* freq_map) {
  freq_map->clear();
  impl_->FillFreqMap(freq_map);
}

error_code RdbSaver::SaveAux(const GlobalData& glob_state) {
  static_assert(sizeof(void*) == 8, "");

  error_code ec;

  /* Add a few fields about the state when the RDB was created. */
  RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("redis-ver", REDIS_VERSION));
  RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("df-ver", GetVersion()));
  RETURN_ON_ERR(SaveAuxFieldStrInt("redis-bits", 64));

  RETURN_ON_ERR(SaveAuxFieldStrInt("ctime", time(NULL)));
  auto used_mem = used_mem_current.load(memory_order_relaxed);
  VLOG(1) << "Used memory during save: " << used_mem;
  RETURN_ON_ERR(SaveAuxFieldStrInt("used-mem", used_mem));
  RETURN_ON_ERR(SaveAuxFieldStrInt("aof-preamble", 0));

  // Save lua scripts only in rdb or summary file
  DCHECK(save_mode_ != SaveMode::SINGLE_SHARD || glob_state.lua_scripts.empty());
  for (const string& s : glob_state.lua_scripts)
    RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("lua", s));

  if (save_mode_ == SaveMode::RDB) {
    if (!glob_state.search_indices.empty())
      LOG(WARNING) << "Dragonfly search index data is incompatible with the RDB format";
  } else {
    // Search index definitions are not tied to shards and are saved in the summary file
    DCHECK(save_mode_ != SaveMode::SINGLE_SHARD || glob_state.search_indices.empty());
    for (const string& s : glob_state.search_indices)
      RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("search-index", s));
  }

  // TODO: "repl-stream-db", "repl-id", "repl-offset"
  return error_code{};
}

error_code RdbSaver::SaveEpilog() {
  RETURN_ON_ERR(impl_->serializer()->SendEofAndChecksum());

  RETURN_ON_ERR(impl_->FlushSerializer());

  return impl_->FlushSink();
}

error_code RdbSaver::SaveAuxFieldStrInt(string_view key, int64_t val) {
  char buf[LONG_STR_SIZE];
  int vlen = ll2string(buf, sizeof(buf), val);
  return impl_->SaveAuxFieldStrStr(key, string_view(buf, vlen));
}

void RdbSaver::CancelInShard(EngineShard* shard) {
  impl_->CancelInShard(shard);
}

size_t RdbSaver::GetTotalBuffersSize() const {
  return impl_->GetTotalBuffersSize();
}

RdbSaver::SnapshotStats RdbSaver::GetCurrentSnapshotProgress() const {
  return impl_->GetCurrentSnapshotProgress();
}

int64_t RdbSaver::GetLastWriteTime() const {
  return impl_->last_write_ts();
}

void SerializerBase::AllocateCompressorOnce() {
  if (compressor_impl_) {
    return;
  }
  if (compression_mode_ == CompressionMode::MULTI_ENTRY_ZSTD) {
    compressor_impl_ = detail::CompressorImpl::CreateZstd();
  } else if (compression_mode_ == CompressionMode::MULTI_ENTRY_LZ4) {
    compressor_impl_ = detail::CompressorImpl::CreateLZ4();
  } else {
    LOG(FATAL) << "Invalid compression mode " << unsigned(compression_mode_);
  }
}

void SerializerBase::CompressBlob() {
  if (!compression_stats_) {
    compression_stats_.emplace(CompressionStats{});
  }
  Bytes blob_to_compress = mem_buf_.InputBuffer();
  VLOG(2) << "CompressBlob size " << blob_to_compress.size();
  size_t blob_size = blob_to_compress.size();
  if (blob_size < kMinStrSizeToCompress) {
    ++compression_stats_->small_str_count;
    return;
  }

  AllocateCompressorOnce();

  // Compress the data. We copy compressed data once into the internal buffer of compressor_impl_
  // and then we copy it again into the mem_buf_.
  //
  // TODO: it is possible to avoid double copying here by changing the compressor interface,
  // so that the compressor will accept the output buffer and return the final size. This requires
  // exposing the additional compress bound interface as well.
  io::Result<io::Bytes> res = compressor_impl_->Compress(blob_to_compress);
  if (!res) {
    ++compression_stats_->compression_failed;
    return;
  }

  Bytes compressed_blob = *res;
  if (compressed_blob.length() > blob_size * kMinCompressionReductionPrecentage) {
    ++compression_stats_->compression_no_effective;
    return;
  }

  // Clear membuf and write the compressed blob to it
  mem_buf_.ConsumeInput(blob_size);
  mem_buf_.Reserve(compressed_blob.length() + 1 + 9);  // reserve space for blob + opcode + len

  // First write opcode for compressed string
  auto dest = mem_buf_.AppendBuffer();
  uint8_t opcode = compression_mode_ == CompressionMode::MULTI_ENTRY_ZSTD
                       ? RDB_OPCODE_COMPRESSED_ZSTD_BLOB_START
                       : RDB_OPCODE_COMPRESSED_LZ4_BLOB_START;
  dest[0] = opcode;
  mem_buf_.CommitWrite(1);

  // Write encoded compressed blob len
  dest = mem_buf_.AppendBuffer();
  unsigned enclen = WritePackedUInt(compressed_blob.length(), dest);
  mem_buf_.CommitWrite(enclen);

  // Write compressed blob
  dest = mem_buf_.AppendBuffer();
  memcpy(dest.data(), compressed_blob.data(), compressed_blob.length());
  mem_buf_.CommitWrite(compressed_blob.length());
  ++compression_stats_->compressed_blobs;
  auto& stats = ServerState::tlocal()->stats;
  ++stats.compressed_blobs;
}

size_t RdbSerializer::GetTempBufferSize() const {
  return SerializerBase::GetTempBufferSize() + tmp_str_.size();
}

void RdbSerializer::FlushIfNeeded(SerializerBase::FlushState flush_state) {
  if (flush_fun_) {
    flush_fun_(SerializedLen(), flush_state);
  }
}

}  // namespace dfly
