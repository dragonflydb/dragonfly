// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_save.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/rdb.h"
#include "redis/util.h"
#include "redis/ziplist.h"
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/snapshot.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

using namespace std;
using base::IoBuf;
using io::Bytes;
using nonstd::make_unexpected;

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

/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. buf must be at least 9 bytes.
 * */

inline unsigned SerializeLen(uint64_t len, uint8_t* buf) {
  if (len < (1 << 6)) {
    /* Save a 6 bit len */
    buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
    return 1;
  }
  if (len < (1 << 14)) {
    /* Save a 14 bit len */
    buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
    buf[1] = len & 0xFF;
    return 2;
  }

  if (len <= UINT32_MAX) {
    /* Save a 32 bit len */
    buf[0] = RDB_32BITLEN;
    absl::big_endian::Store32(buf + 1, len);
    return 1 + 4;
  }

  /* Save a 64 bit len */
  buf[0] = RDB_64BITLEN;
  absl::big_endian::Store64(buf + 1, len);
  return 1 + 8;
}

uint8_t RdbObjectType(unsigned type, unsigned encoding) {
  switch (type) {
    case OBJ_STRING:
      return RDB_TYPE_STRING;
    case OBJ_LIST:
      if (encoding == OBJ_ENCODING_QUICKLIST)
        return RDB_TYPE_LIST_QUICKLIST;
      break;
    case OBJ_SET:
      if (encoding == kEncodingIntSet)
        return RDB_TYPE_SET_INTSET;
      else if (encoding == kEncodingStrMap)
        return RDB_TYPE_SET;
      break;
    case OBJ_ZSET:
      if (encoding == OBJ_ENCODING_LISTPACK)
        return RDB_TYPE_ZSET_ZIPLIST;  // we save using the old ziplist encoding.
      else if (encoding == OBJ_ENCODING_SKIPLIST)
        return RDB_TYPE_ZSET_2;
      break;
    case OBJ_HASH:
      if (encoding == OBJ_ENCODING_LISTPACK)
        return RDB_TYPE_HASH_ZIPLIST;
      else if (encoding == OBJ_ENCODING_HT)
        return RDB_TYPE_HASH;
      break;
    case OBJ_STREAM:
      return RDB_TYPE_STREAM_LISTPACKS;
    case OBJ_MODULE:
      return RDB_TYPE_MODULE_2;
  }
  LOG(FATAL) << "Unknown encoding " << encoding << " for type " << type;
  return 0; /* avoid warning */
}

}  // namespace

RdbSerializer::RdbSerializer(io::Sink* s) : sink_(s), mem_buf_{4_KB}, tmp_buf_(nullptr) {
}

RdbSerializer::~RdbSerializer() {
}

// Called by snapshot
error_code RdbSerializer::SaveEntry(PrimeIterator it, uint64_t expire_ms) {
  uint8_t buf[16];

  /* Save the expire time */
  if (expire_ms > 0) {
    buf[0] = RDB_OPCODE_EXPIRETIME_MS;
    absl::little_endian::Store64(buf + 1, expire_ms);
    RETURN_ON_ERR(WriteRaw(Bytes{buf, 9}));
  }

  const PrimeKey& pk = it->first;
  const PrimeValue& pv = it->second;

  string_view key = pk.GetSlice(&tmp_str_);
  unsigned obj_type = pv.ObjType();
  unsigned encoding = pv.Encoding();
  uint8_t rdb_type = RdbObjectType(obj_type, encoding);

  DVLOG(2) << "Saving keyval start " << key;

  ++type_freq_map_[rdb_type];
  RETURN_ON_ERR(WriteOpcode(rdb_type));

  RETURN_ON_ERR(SaveString(key));

  if (obj_type == OBJ_STRING) {
    auto opt_int = pv.TryGetInt();
    if (opt_int) {
      return SaveLongLongAsString(*opt_int);
    }
    return SaveString(pv.GetSlice(&tmp_str_));
  }

  return SaveObject(pv);
}

error_code RdbSerializer::SaveObject(const PrimeValue& pv) {
  unsigned obj_type = pv.ObjType();
  CHECK_NE(obj_type, OBJ_STRING);

  if (obj_type == OBJ_LIST) {
    return SaveListObject(pv.AsRObj());
  }

  if (obj_type == OBJ_SET) {
    return SaveSetObject(pv);
  }

  if (obj_type == OBJ_HASH) {
    return SaveHSetObject(pv.AsRObj());
  }

  if (obj_type == OBJ_ZSET) {
    return SaveZSetObject(pv.AsRObj());
  }

  LOG(ERROR) << "Not implemented " << obj_type;
  return make_error_code(errc::function_not_supported);
}

error_code RdbSerializer::SaveListObject(const robj* obj) {
  /* Save a list value */
  DCHECK_EQ(OBJ_ENCODING_QUICKLIST, obj->encoding);
  const quicklist* ql = reinterpret_cast<const quicklist*>(obj->ptr);
  quicklistNode* node = ql->head;
  DVLOG(1) << "Saving list of length " << ql->len;
  RETURN_ON_ERR(SaveLen(ql->len));

  while (node) {
    DVLOG(2) << "QL node (encoding/container/sz): " << node->encoding << "/" << node->container
             << "/" << node->sz;
    if (QL_NODE_IS_PLAIN(node)) {
      if (quicklistNodeIsCompressed(node)) {
        void* data;
        size_t compress_len = quicklistGetLzf(node, &data);

        RETURN_ON_ERR(SaveLzfBlob(Bytes{reinterpret_cast<uint8_t*>(data), compress_len}, node->sz));
      } else {
        RETURN_ON_ERR(SaveString(node->entry, node->sz));
      }
    } else {
      // listpack
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
    node = node->next;
  }
  return error_code{};
}

error_code RdbSerializer::SaveSetObject(const PrimeValue& obj) {
  if (obj.Encoding() == kEncodingStrMap) {
    dict* set = (dict*)obj.RObjPtr();

    RETURN_ON_ERR(SaveLen(dictSize(set)));

    dictIterator* di = dictGetIterator(set);
    dictEntry* de;
    auto cleanup = absl::MakeCleanup([di] { dictReleaseIterator(di); });

    while ((de = dictNext(di)) != NULL) {
      sds ele = (sds)de->key;

      RETURN_ON_ERR(SaveString(string_view{ele, sdslen(ele)}));
    }
  } else {
    CHECK_EQ(obj.Encoding(), kEncodingIntSet);
    intset* is = (intset*)obj.RObjPtr();
    size_t len = intsetBlobLen(is);

    RETURN_ON_ERR(SaveString(string_view{(char*)is, len}));
  }

  return error_code{};
}

error_code RdbSerializer::SaveHSetObject(const robj* obj) {
  DCHECK_EQ(OBJ_HASH, obj->type);
  if (obj->encoding == OBJ_ENCODING_HT) {
    dict* set = (dict*)obj->ptr;

    RETURN_ON_ERR(SaveLen(dictSize(set)));

    dictIterator* di = dictGetIterator(set);
    dictEntry* de;
    auto cleanup = absl::MakeCleanup([di] { dictReleaseIterator(di); });

    while ((de = dictNext(di)) != NULL) {
      sds key = (sds)de->key;
      sds value = (sds)de->v.val;

      RETURN_ON_ERR(SaveString(string_view{key, sdslen(key)}));
      RETURN_ON_ERR(SaveString(string_view{value, sdslen(value)}));
    }
  } else {
    CHECK_EQ(unsigned(OBJ_ENCODING_LISTPACK), obj->encoding);

    uint8_t* lp = (uint8_t*)obj->ptr;
    size_t lplen = lpLength(lp);
    CHECK(lplen > 0 && lplen % 2 == 0);  // has (key,value) pairs.

    RETURN_ON_ERR(SaveListPackAsZiplist(lp));
  }

  return error_code{};
}

error_code RdbSerializer::SaveZSetObject(const robj* obj) {
  DCHECK_EQ(OBJ_ZSET, obj->type);
  if (obj->encoding == OBJ_ENCODING_SKIPLIST) {
    zset* zs = (zset*)obj->ptr;
    zskiplist* zsl = zs->zsl;

    RETURN_ON_ERR(SaveLen(zsl->length));

    /* We save the skiplist elements from the greatest to the smallest
     * (that's trivial since the elements are already ordered in the
     * skiplist): this improves the load process, since the next loaded
     * element will always be the smaller, so adding to the skiplist
     * will always immediately stop at the head, making the insertion
     * O(1) instead of O(log(N)). */
    zskiplistNode* zn = zsl->tail;
    while (zn != NULL) {
      RETURN_ON_ERR(SaveString(string_view{zn->ele, sdslen(zn->ele)}));
      RETURN_ON_ERR(SaveBinaryDouble(zn->score));
      zn = zn->backward;
    }
  } else {
    CHECK_EQ(obj->encoding, unsigned(OBJ_ENCODING_LISTPACK)) << "Unknown zset encoding";
    uint8_t* lp = (uint8_t*)obj->ptr;
    RETURN_ON_ERR(SaveListPackAsZiplist(lp));
  }

  return error_code{};
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

// TODO: if buf is large enough, it makes sense to write both mem_buf and buf
// directly to sink_.
error_code RdbSerializer::WriteRaw(const io::Bytes& buf) {
  IoBuf::Bytes dest = mem_buf_.AppendBuffer();
  if (dest.size() >= buf.size()) {
    memcpy(dest.data(), buf.data(), buf.size());
    mem_buf_.CommitWrite(buf.size());
    return error_code{};
  }

  io::Bytes ib = mem_buf_.InputBuffer();

  if (ib.empty()) {
    RETURN_ON_ERR(sink_->Write(buf));
  } else {
    iovec v[2] = {{.iov_base = const_cast<uint8_t*>(ib.data()), .iov_len = ib.size()},
                  {.iov_base = const_cast<uint8_t*>(buf.data()), .iov_len = buf.size()}};
    RETURN_ON_ERR(sink_->Write(v, ABSL_ARRAYSIZE(v)));
    mem_buf_.ConsumeInput(ib.size());
  }

  return error_code{};
}

error_code RdbSerializer::FlushMem() {
  size_t sz = mem_buf_.InputLen();
  if (sz == 0)
    return error_code{};

  DVLOG(2) << "FlushMem " << sz << " bytes";

  // interrupt point.
  RETURN_ON_ERR(sink_->Write(mem_buf_.InputBuffer()));
  mem_buf_.ConsumeInput(sz);
  return error_code{};
}

error_code RdbSerializer::SaveString(string_view val) {
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
  if (server.rdb_compression && len > 20) {
    size_t comprlen, outlen = len;
    tmp_buf_.resize(outlen + 1);

    // Due to stack constrainsts im fibers we can not allow large arrays on stack.
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

error_code RdbSerializer::SaveLen(size_t len) {
  uint8_t buf[16];
  unsigned enclen = SerializeLen(len, buf);
  return WriteRaw(Bytes{buf, enclen});
}

error_code RdbSerializer::SaveLzfBlob(const io::Bytes& src, size_t uncompressed_len) {
  /* Data compressed! Let's save it on disk */
  uint8_t opcode = (RDB_ENCVAL << 6) | RDB_ENC_LZF;
  RETURN_ON_ERR(WriteOpcode(opcode));
  RETURN_ON_ERR(SaveLen(src.size()));
  RETURN_ON_ERR(SaveLen(uncompressed_len));
  RETURN_ON_ERR(WriteRaw(src));

  return error_code{};
}

struct RdbSaver::Impl {
  // used for serializing non-body components in the calling fiber.
  RdbSerializer serializer;
  SliceSnapshot::StringChannel channel;
  vector<unique_ptr<SliceSnapshot>> shard_snapshots;

  // We pass K=sz to say how many producers are pushing data in order to maintain
  // correct closing semantics - channel is closing when K producers marked it as closed.
  Impl(unsigned sz) : channel{128, sz}, shard_snapshots(sz) {
  }
};

RdbSaver::RdbSaver(EngineShardSet* ess, ::io::Sink* sink) : ess_(ess), sink_(sink) {
  CHECK_NOTNULL(sink_);

  impl_.reset(new Impl(ess->size()));
  impl_->serializer.set_sink(sink_);
}

RdbSaver::~RdbSaver() {
}

std::error_code RdbSaver::SaveHeader() {
  char magic[16];
  size_t sz = absl::SNPrintF(magic, sizeof(magic), "REDIS%04d", RDB_VERSION);
  CHECK_EQ(9u, sz);

  RETURN_ON_ERR(impl_->serializer.WriteRaw(Bytes{reinterpret_cast<uint8_t*>(magic), sz}));
  RETURN_ON_ERR(SaveAux());

  return error_code{};
}

error_code RdbSaver::SaveBody(RdbTypeFreqMap* freq_map) {
  RETURN_ON_ERR(impl_->serializer.FlushMem());
  VLOG(1) << "SaveBody";

  size_t num_written = 0;
  string val;
  vector<string> vals;
  vector<iovec> ivec;

  auto& channel = impl_->channel;
  while (channel.Pop(val)) {
    vals.emplace_back(std::move(val));
    while (channel.TryPop(val)) {
      vals.emplace_back(std::move(val));
    }
    ivec.resize(vals.size());
    for (size_t i = 0; i < ivec.size(); ++i) {
      ivec[i].iov_base = vals[i].data();
      ivec[i].iov_len = vals[i].size();
    }
    RETURN_ON_ERR(sink_->Write(ivec.data(), ivec.size()));
    num_written += vals.size();
    vals.clear();
  }

  for (auto& ptr : impl_->shard_snapshots) {
    ptr->Join();
  }

  VLOG(1) << "Blobs written " << num_written;

  RETURN_ON_ERR(SaveEpilog());

  if (freq_map) {
    freq_map->clear();
    for (auto& ptr : impl_->shard_snapshots) {
      const RdbTypeFreqMap& src_map = ptr->serializer()->type_freq_map();
      for (const auto& k_v : src_map)
        (*freq_map)[k_v.first] += k_v.second;
    }
  }
  return error_code{};
}

void RdbSaver::StartSnapshotInShard(EngineShard* shard) {
  auto pair = shard->db_slice().GetTables(0);
  auto s = make_unique<SliceSnapshot>(pair.first, pair.second, &impl_->channel);

  s->Start(&shard->db_slice());
  impl_->shard_snapshots[shard->shard_id()] = move(s);
}

error_code RdbSaver::SaveAux() {
  static_assert(sizeof(void*) == 8, "");

  int aof_preamble = false;
  error_code ec;

  /* Add a few fields about the state when the RDB was created. */
  RETURN_ON_ERR(SaveAuxFieldStrStr("redis-ver", REDIS_VERSION));
  RETURN_ON_ERR(SaveAuxFieldStrInt("redis-bits", 64));

  RETURN_ON_ERR(SaveAuxFieldStrInt("ctime", time(NULL)));

  // TODO: to implement used-mem caching.
  RETURN_ON_ERR(SaveAuxFieldStrInt("used-mem", used_mem_current.load(memory_order_relaxed)));

  RETURN_ON_ERR(SaveAuxFieldStrInt("aof-preamble", aof_preamble));

  // TODO: "repl-stream-db", "repl-id", "repl-offset"
  return error_code{};
}

error_code RdbSaver::SaveEpilog() {
  uint8_t buf[8];
  uint64_t chksum;

  auto& ser = impl_->serializer;

  /* EOF opcode */
  RETURN_ON_ERR(ser.WriteOpcode(RDB_OPCODE_EOF));

  /* CRC64 checksum. It will be zero if checksum computation is disabled, the
   * loading code skips the check in this case. */
  chksum = 0;

  absl::little_endian::Store64(buf, chksum);
  RETURN_ON_ERR(ser.WriteRaw(buf));

  return ser.FlushMem();
}

error_code RdbSaver::SaveAuxFieldStrStr(string_view key, string_view val) {
  auto& ser = impl_->serializer;
  RETURN_ON_ERR(ser.WriteOpcode(RDB_OPCODE_AUX));
  RETURN_ON_ERR(ser.SaveString(key));
  RETURN_ON_ERR(ser.SaveString(val));

  return error_code{};
}

error_code RdbSaver::SaveAuxFieldStrInt(string_view key, int64_t val) {
  char buf[LONG_STR_SIZE];
  int vlen = ll2string(buf, sizeof(buf), val);
  return SaveAuxFieldStrStr(key, string_view(buf, vlen));
}

}  // namespace dfly
