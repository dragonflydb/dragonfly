// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_save.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

extern "C" {
#include "redis/rdb.h"
#include "redis/util.h"
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
using facade::operator""_KB;

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

uint8_t RdbObjectType(const robj* o) {
  switch (o->type) {
    case OBJ_STRING:
      return RDB_TYPE_STRING;
    case OBJ_LIST:
      if (o->encoding == OBJ_ENCODING_QUICKLIST)
        return RDB_TYPE_LIST_QUICKLIST;
      LOG(FATAL) << ("Unknown list encoding");
      break;
    case OBJ_SET:
      if (o->encoding == OBJ_ENCODING_INTSET)
        return RDB_TYPE_SET_INTSET;
      else if (o->encoding == OBJ_ENCODING_HT)
        return RDB_TYPE_SET;
      LOG(FATAL) << ("Unknown set encoding");
      break;
    case OBJ_ZSET:
      if (o->encoding == OBJ_ENCODING_ZIPLIST)
        return RDB_TYPE_ZSET_ZIPLIST;
      else if (o->encoding == OBJ_ENCODING_SKIPLIST)
        return RDB_TYPE_ZSET_2;
      LOG(FATAL) << ("Unknown sorted set encoding");
      break;
    case OBJ_HASH:
      if (o->encoding == OBJ_ENCODING_ZIPLIST)
        return RDB_TYPE_HASH_ZIPLIST;
      else if (o->encoding == OBJ_ENCODING_HT)
        return RDB_TYPE_HASH;
      LOG(FATAL) << ("Unknown hash encoding");
      break;
    case OBJ_STREAM:
      return RDB_TYPE_STREAM_LISTPACKS;
    case OBJ_MODULE:
      return RDB_TYPE_MODULE_2;
    default:
      LOG(FATAL) << ("Unknown object type");
  }
  return 0; /* avoid warning */
}

}  // namespace

RdbSerializer::RdbSerializer(io::Sink* s) : sink_(s), mem_buf_{4_KB}, tmp_buf_(nullptr) {
}

RdbSerializer::~RdbSerializer() {
}

error_code RdbSerializer::SaveKeyVal(string_view key, const robj* val, uint64_t expire_ms) {
  uint8_t buf[16];

  /* Save the expire time */
  if (expire_ms > 0) {
    buf[0] = RDB_OPCODE_EXPIRETIME_MS;
    absl::little_endian::Store64(buf + 1, expire_ms);
    RETURN_ON_ERR(WriteRaw(Bytes{buf, 9}));
  }

  uint8_t rdb_type = RdbObjectType(val);
  RETURN_ON_ERR(WriteOpcode(rdb_type));

  RETURN_ON_ERR(SaveString(key));

  return SaveObject(val);
}

error_code RdbSerializer::SaveKeyVal(string_view key, string_view value, uint64_t expire_ms) {
  uint8_t buf[16];

  /* Save the expire time */
  if (expire_ms > 0) {
    buf[0] = RDB_OPCODE_EXPIRETIME_MS;
    absl::little_endian::Store64(buf + 1, expire_ms);
    RETURN_ON_ERR(WriteRaw(Bytes{buf, 9}));
  }

  DVLOG(2) << "Saving keyval start " << key;

  RETURN_ON_ERR(WriteOpcode(RDB_TYPE_STRING));

  RETURN_ON_ERR(SaveString(key));

  RETURN_ON_ERR(SaveString(value));

  return error_code{};
}

error_code RdbSerializer::SaveObject(const robj* o) {
  if (o->type == OBJ_STRING) {
    /* Save a string value */
    return SaveStringObject(o);
  }

  if (o->type == OBJ_LIST) {
    /* Save a list value */
    DCHECK_EQ(OBJ_ENCODING_QUICKLIST, o->encoding);
    const quicklist* ql = reinterpret_cast<const quicklist*>(o->ptr);
    quicklistNode* node = ql->head;
    DVLOG(1) << "Saving list of length " << ql->len;
    RETURN_ON_ERR(SaveLen(ql->len));

    while (node) {
      if (quicklistNodeIsCompressed(node)) {
        void* data;
        size_t compress_len = quicklistGetLzf(node, &data);
        RETURN_ON_ERR(SaveLzfBlob(Bytes{reinterpret_cast<uint8_t*>(data), compress_len}, node->sz));
      } else {
        RETURN_ON_ERR(SaveString(node->entry, node->sz));
      }
      node = node->next;
    }
    return error_code{};
  }

  LOG(FATAL) << "Not implemented " << o->type;
  return error_code{};
}

error_code RdbSerializer::SaveStringObject(const robj* obj) {
  /* Avoid to decode the object, then encode it again, if the
   * object is already integer encoded. */
  if (obj->encoding == OBJ_ENCODING_INT) {
    return SaveLongLongAsString(long(obj->ptr));
  }

  CHECK(sdsEncodedObject(obj));
  sds s = reinterpret_cast<sds>(obj->ptr);

  return SaveString(std::string_view{s, sdslen(s)});
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

error_code RdbSaver::SaveBody() {
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
  RETURN_ON_ERR(SaveAuxFieldStrInt("used-mem", 666666666));

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
