// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_save.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include "core/string_set.h"

extern "C" {
#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/rdb.h"
#include "redis/stream.h"
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
      else if (encoding == kEncodingStrMap || encoding == kEncodingStrMap2)
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

constexpr size_t kBufLen = 64_KB;
constexpr size_t kAmask = 4_KB - 1;

}  // namespace

RdbSerializer::RdbSerializer(io::Sink* s) : sink_(s), mem_buf_{4_KB}, tmp_buf_(nullptr) {
}

RdbSerializer::~RdbSerializer() {
}

error_code RdbSerializer::SelectDb(uint32_t dbid) {
  uint8_t buf[16];
  buf[0] = RDB_OPCODE_SELECTDB;
  unsigned enclen = SerializeLen(dbid, buf + 1);
  return WriteRaw(Bytes{buf, enclen + 1});
}

// Called by snapshot
io::Result<uint8_t> RdbSerializer::SaveEntry(const PrimeKey& pk, const PrimeValue& pv,
                                             uint64_t expire_ms) {
  uint8_t buf[16];
  error_code ec;
  /* Save the expire time */
  if (expire_ms > 0) {
    buf[0] = RDB_OPCODE_EXPIRETIME_MS;
    absl::little_endian::Store64(buf + 1, expire_ms);
    ec = WriteRaw(Bytes{buf, 9});
    if (ec)
      return make_unexpected(ec);
  }

  string_view key = pk.GetSlice(&tmp_str_);
  unsigned obj_type = pv.ObjType();
  unsigned encoding = pv.Encoding();
  uint8_t rdb_type = RdbObjectType(obj_type, encoding);

  DVLOG(3) << "Saving keyval start " << key;

  ec = WriteOpcode(rdb_type);
  if (ec)
    return make_unexpected(ec);

  ec = SaveString(key);
  if (ec)
    return make_unexpected(ec);

  if (obj_type == OBJ_STRING) {
    auto opt_int = pv.TryGetInt();
    if (opt_int) {
      ec = SaveLongLongAsString(*opt_int);
    } else {
      ec = SaveString(pv.GetSlice(&tmp_str_));
    }
  } else {
    ec = SaveObject(pv);
  }

  if (ec)
    return make_unexpected(ec);
  return rdb_type;
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

  if (obj_type == OBJ_STREAM) {
    return SaveStreamObject(pv.AsRObj());
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
    DVLOG(3) << "QL node (encoding/container/sz): " << node->encoding << "/" << node->container
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
  } else if (obj.Encoding() == kEncodingStrMap2) {
    StringSet* set = (StringSet*)obj.RObjPtr();

    RETURN_ON_ERR(SaveLen(set->Size()));

    for (sds ele : *set) {
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

error_code RdbSerializer::SaveStreamObject(const robj* obj) {
  /* Store how many listpacks we have inside the radix tree. */
  stream* s = (stream*)obj->ptr;
  rax* rax = s->rax_tree;

  RETURN_ON_ERR(SaveLen(raxSize(rax)));

  /* Serialize all the listpacks inside the radix tree as they are,
   * when loading back, we'll use the first entry of each listpack
   * to insert it back into the radix tree. */
  raxIterator ri;
  raxStart(&ri, rax);
  raxSeek(&ri, "^", NULL, 0);
  while (raxNext(&ri)) {
    uint8_t* lp = (uint8_t*)ri.data;
    size_t lp_bytes = lpBytes(lp);
    error_code ec = SaveString((uint8_t*)ri.key, ri.key_len);
    if (ec) {
      raxStop(&ri);
      return ec;
    }

    ec = SaveString(lp, lp_bytes);
    if (ec) {
      raxStop(&ri);
      return ec;
    }
  }
  raxStop(&ri);

  /* Save the number of elements inside the stream. We cannot obtain
   * this easily later, since our macro nodes should be checked for
   * number of items: not a great CPU / space tradeoff. */

  RETURN_ON_ERR(SaveLen(s->length));

  /* Save the last entry ID. */
  RETURN_ON_ERR(SaveLen(s->last_id.ms));
  RETURN_ON_ERR(SaveLen(s->last_id.seq));

  /* The consumer groups and their clients are part of the stream
   * type, so serialize every consumer group. */

  /* Save the number of groups. */
  size_t num_cgroups = s->cgroups ? raxSize(s->cgroups) : 0;
  RETURN_ON_ERR(SaveLen(num_cgroups));

  if (num_cgroups) {
    /* Serialize each consumer group. */
    raxStart(&ri, s->cgroups);
    raxSeek(&ri, "^", NULL, 0);

    auto cleanup = absl::MakeCleanup([&] { raxStop(&ri); });

    while (raxNext(&ri)) {
      streamCG* cg = (streamCG*)ri.data;

      /* Save the group name. */
      RETURN_ON_ERR(SaveString((uint8_t*)ri.key, ri.key_len));

      /* Last ID. */
      RETURN_ON_ERR(SaveLen(s->last_id.ms));

      RETURN_ON_ERR(SaveLen(s->last_id.seq));

      /* Save the global PEL. */
      RETURN_ON_ERR(SaveStreamPEL(cg->pel, true));

      /* Save the consumers of this group. */

      RETURN_ON_ERR(SaveStreamConsumers(cg));
    }
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

error_code RdbSerializer::SaveStreamConsumers(streamCG* cg) {
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

    /* Last seen time. */
    absl::little_endian::Store64(buf, consumer->seen_time);
    RETURN_ON_ERR(WriteRaw(buf));

    /* Consumer PEL, without the ACKs (see last parameter of the function
     * passed with value of 0), at loading time we'll lookup the ID
     * in the consumer group global PEL and will put a reference in the
     * consumer local PEL. */

    RETURN_ON_ERR(SaveStreamPEL(consumer->pel, false));
  }

  return error_code{};
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
    return sink_->Write(buf);
  }
  // else
  iovec v[2] = {{.iov_base = const_cast<uint8_t*>(ib.data()), .iov_len = ib.size()},
                {.iov_base = const_cast<uint8_t*>(buf.data()), .iov_len = buf.size()}};
  RETURN_ON_ERR(sink_->Write(v, ABSL_ARRAYSIZE(v)));
  mem_buf_.ConsumeInput(ib.size());

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

io::Result<size_t> LinuxWriteWrapper::WriteSome(const iovec* v, uint32_t len) {
  io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
  if (res) {
    offset_ += *res;
  }

  return res;
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
  iovec ivec{.iov_base = aligned_buf_, .iov_len = len};
  buf_offs_ = 0;

  return upstream_->Write(&ivec, 1);
}

class RdbSaver::Impl {
 public:
  // We pass K=sz to say how many producers are pushing data in order to maintain
  // correct closing semantics - channel is closing when K producers marked it as closed.
  Impl(bool align_writes, unsigned producers_len, io::Sink* sink);

  error_code SaveAuxFieldStrStr(string_view key, string_view val);

  RdbSerializer* serializer() {
    return &meta_serializer_;
  }

  // Drain record channel and write to sink.
  error_code ConsumeChannel();

  // Start filling record channel with entries from shard.
  void StartSnapshotting(bool include_journal_changes, EngineShard* shard);

  error_code Flush() {
    if (aligned_buf_)
      return aligned_buf_->Flush();

    return error_code{};
  }

  size_t Size() const {
    return shard_snapshots_.size();
  }

  void FillFreqMap(RdbTypeFreqMap* dest) const;

 private:
  io::Sink* sink_;
  // used for serializing non-body components in the calling fiber.
  RdbSerializer meta_serializer_;
  vector<unique_ptr<SliceSnapshot>> shard_snapshots_;
  SliceSnapshot::RecordChannel channel_;
  std::optional<AlignedBuffer> aligned_buf_;
};

// We pass K=sz to say how many producers are pushing data in order to maintain
// correct closing semantics - channel is closing when K producers marked it as closed.
RdbSaver::Impl::Impl(bool align_writes, unsigned producers_len, io::Sink* sink)
    : sink_(sink), meta_serializer_(sink),
      shard_snapshots_(producers_len), channel_{128, producers_len} {
  if (align_writes) {
    aligned_buf_.emplace(kBufLen, sink);
    meta_serializer_.set_sink(&aligned_buf_.value());
  }

  DCHECK(producers_len > 0 || channel_.IsClosing());
}

error_code RdbSaver::Impl::SaveAuxFieldStrStr(string_view key, string_view val) {
  auto& ser = meta_serializer_;
  RETURN_ON_ERR(ser.WriteOpcode(RDB_OPCODE_AUX));
  RETURN_ON_ERR(ser.SaveString(key));
  RETURN_ON_ERR(ser.SaveString(val));

  return error_code{};
}

error_code RdbSaver::Impl::ConsumeChannel() {
  error_code io_error;

  uint8_t buf[16];
  size_t channel_bytes = 0;
  SliceSnapshot::DbRecord record;
  DbIndex last_db_index = kInvalidDbId;

  buf[0] = RDB_OPCODE_SELECTDB;

  // we can not exit on io-error since we spawn fibers that push data.
  // TODO: we may signal them to stop processing and exit asap in case of the error.
  auto& channel = channel_;
  while (channel.Pop(record)) {
    if (io_error)
      continue;

    do {
      if (record.db_index != last_db_index) {
        unsigned enclen = SerializeLen(record.db_index, buf + 1);
        string_view str{(char*)buf, enclen + 1};

        if (aligned_buf_) {
          io_error = aligned_buf_->Write(str);
        } else {
          io_error = sink_->Write(io::Buffer(str));
        }
        if (io_error)
          break;
        last_db_index = record.db_index;
      }

      DVLOG(2) << "Pulled " << record.id;
      channel_bytes += record.value.size();

      if (aligned_buf_) {
        io_error = aligned_buf_->Write(record.value);
      } else {
        io_error = sink_->Write(io::Buffer(record.value));
      }
      record.value.clear();
    } while (!io_error && channel.TryPop(record));
  }  // while (channel.pop)

  size_t pushed_bytes = 0;
  for (auto& ptr : shard_snapshots_) {
    ptr->Join();
    pushed_bytes += ptr->channel_bytes();
  }

  DCHECK(!channel.TryPop(record));

  VLOG(1) << "Channel pulled bytes: " << channel_bytes << " pushed bytes: " << pushed_bytes;

  return io_error;
}

void RdbSaver::Impl::StartSnapshotting(bool include_journal_changes, EngineShard* shard) {
  auto s = make_unique<SliceSnapshot>(&shard->db_slice(), &channel_);

  s->Start(include_journal_changes);

  // For single shard configuration, we maintain only one snapshot,
  // so we do not have to map it via shard_id.
  unsigned sid = shard_snapshots_.size() == 1 ? 0 : shard->shard_id();
  shard_snapshots_[sid] = move(s);
}

void RdbSaver::Impl::FillFreqMap(RdbTypeFreqMap* dest) const {
  for (auto& ptr : shard_snapshots_) {
    const RdbTypeFreqMap& src_map = ptr->freq_map();
    for (const auto& k_v : src_map)
      (*dest)[k_v.first] += k_v.second;
  }
}

RdbSaver::RdbSaver(::io::Sink* sink, SaveMode save_mode, bool align_writes) {
  CHECK_NOTNULL(sink);

  int producer_count = 0;
  switch (save_mode) {
    case SaveMode::SUMMARY:
      producer_count = 0;
      break;
    case SaveMode::SINGLE_SHARD:
      producer_count = 1;
      break;
    case SaveMode::RDB:
      producer_count = shard_set->size();
      break;
  }

  impl_.reset(new Impl(align_writes, producer_count, sink));
  save_mode_ = save_mode;
}

RdbSaver::~RdbSaver() {
}

error_code RdbSaver::SaveHeader(const StringVec& lua_scripts) {
  char magic[16];
  size_t sz = absl::SNPrintF(magic, sizeof(magic), "REDIS%04d", RDB_VERSION);
  CHECK_EQ(9u, sz);

  RETURN_ON_ERR(impl_->serializer()->WriteRaw(Bytes{reinterpret_cast<uint8_t*>(magic), sz}));
  RETURN_ON_ERR(SaveAux(lua_scripts));

  return error_code{};
}

error_code RdbSaver::SaveBody(RdbTypeFreqMap* freq_map) {
  RETURN_ON_ERR(impl_->serializer()->FlushMem());

  VLOG(1) << "SaveBody , snapshots count: " << impl_->Size();

  error_code io_error = impl_->ConsumeChannel();
  if (io_error) {
    VLOG(1) << "io error " << io_error;
    return io_error;
  }

  RETURN_ON_ERR(SaveEpilog());

  if (freq_map) {
    freq_map->clear();
    impl_->FillFreqMap(freq_map);
  }

  return error_code{};
}

void RdbSaver::StartSnapshotInShard(bool include_journal_changes, EngineShard* shard) {
  impl_->StartSnapshotting(include_journal_changes, shard);
}

error_code RdbSaver::SaveAux(const StringVec& lua_scripts) {
  static_assert(sizeof(void*) == 8, "");

  int aof_preamble = false;
  error_code ec;

  /* Add a few fields about the state when the RDB was created. */
  RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("redis-ver", REDIS_VERSION));
  RETURN_ON_ERR(SaveAuxFieldStrInt("redis-bits", 64));

  RETURN_ON_ERR(SaveAuxFieldStrInt("ctime", time(NULL)));

  RETURN_ON_ERR(SaveAuxFieldStrInt("used-mem", used_mem_current.load(memory_order_relaxed)));

  RETURN_ON_ERR(SaveAuxFieldStrInt("aof-preamble", aof_preamble));

  // Save lua scripts only in rdb or summary file
  DCHECK(save_mode_ != SaveMode::SINGLE_SHARD || lua_scripts.empty());
  for (const string& s : lua_scripts) {
    RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("lua", s));
  }

  // TODO: "repl-stream-db", "repl-id", "repl-offset"
  return error_code{};
}

error_code RdbSaver::SaveEpilog() {
  uint8_t buf[8];
  uint64_t chksum;

  auto& ser = *impl_->serializer();

  /* EOF opcode */
  RETURN_ON_ERR(ser.WriteOpcode(RDB_OPCODE_EOF));

  /* CRC64 checksum. It will be zero if checksum computation is disabled, the
   * loading code skips the check in this case. */
  chksum = 0;

  absl::little_endian::Store64(buf, chksum);
  RETURN_ON_ERR(ser.WriteRaw(buf));

  RETURN_ON_ERR(ser.FlushMem());

  return impl_->Flush();
}

error_code RdbSaver::SaveAuxFieldStrInt(string_view key, int64_t val) {
  char buf[LONG_STR_SIZE];
  int vlen = ll2string(buf, sizeof(buf), val);
  return impl_->SaveAuxFieldStrStr(key, string_view(buf, vlen));
}

}  // namespace dfly
