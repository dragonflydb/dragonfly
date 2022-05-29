// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load.h"

extern "C" {

#include "redis/intset.h"
#include "redis/listpack.h"
#include "redis/lzfP.h" /* LZF compression library */
#include "redis/rdb.h"
#include "redis/util.h"
#include "redis/ziplist.h"
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include "base/endian.h"
#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/hset_family.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "strings/human_readable.h"

DECLARE_int32(list_max_listpack_size);
DECLARE_int32(list_compress_depth);
DECLARE_uint32(dbnum);

namespace dfly {

using namespace std;
using base::IoBuf;
using nonstd::make_unexpected;
using namespace util;
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
int ziplistPairsConvertAndValidateIntegrity(unsigned char* zl, size_t size, unsigned char** lp) {
  /* Keep track of the field names to locate duplicate ones */
  ZiplistCbArgs data = {0, NULL, lp};

  int ret = ziplistValidateIntegrity(zl, size, 1, ziplistPairsEntryConvertAndValidate, &data);

  /* make sure we have an even number of records. */
  if (data.count & 1)
    ret = 0;

  if (data.fields)
    dictRelease(data.fields);
  return ret;
}

}  // namespace

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

RdbLoader::RdbLoader(ScriptMgr* script_mgr)
    : script_mgr_(script_mgr), mem_buf_{16_KB} {
  shard_buf_.reset(new ItemsBuf[shard_set->size()]);
}

RdbLoader::~RdbLoader() {
}

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
    dest = exp_res.value();                    \
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
      return RdbError(errc::bad_version);
    }

    mem_buf_.ConsumeInput(9);
  }

  int type;

  /* Key-specific attributes, set by opcodes before the key type. */
  ObjSettings settings;
  settings.now = mstime();
  size_t keys_loaded = 0;

  while (1) {
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

    if (type == RDB_OPCODE_SELECTDB) {
      unsigned dbid = 0;

      /* SELECTDB: Select the specified database. */
      SET_OR_RETURN(LoadLen(nullptr), dbid);

      if (dbid > FLAGS_dbnum) {
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
  double seconds = double(absl::ToInt64Milliseconds(dur)) / 1000;
  LOG(INFO) << "Done loading RDB, keys loaded: " << keys_loaded;
  LOG(INFO) << "Loading finished after " << strings::HumanReadableElapsedTime(seconds);

  return kOk;
}

error_code RdbLoader::EnsureReadInternal(size_t min_sz) {
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

auto RdbLoader::LoadLen(bool* is_encoded) -> io::Result<uint64_t> {
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

std::error_code RdbLoader::FetchBuf(size_t size, void* dest) {
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

error_code RdbLoader::HandleAux() {
  /* AUX: generic string-string fields. Use to add state to RDB
   * which is backward compatible. Implementations of RDB loading
   * are required to skip AUX fields they don't understand.
   *
   * An AUX field is composed of two strings: key and value. */
  robj *auxkey, *auxval;

  auto exp = FetchGenericString(RDB_LOAD_NONE);
  if (!exp)
    return exp.error();
  auxkey = (robj*)exp->first;
  exp = FetchGenericString(RDB_LOAD_NONE);
  if (!exp) {
    decrRefCount(auxkey);
    return exp.error();
  }

  auxval = (robj*)exp->first;
  char* auxkey_sds = (sds)auxkey->ptr;
  char* auxval_sds = (sds)auxval->ptr;

  if (auxkey_sds[0] == '%') {
    /* All the fields with a name staring with '%' are considered
     * information fields and are logged at startup with a log
     * level of NOTICE. */
    LOG(INFO) << "RDB '" << auxkey_sds << "': " << auxval_sds;
  } else if (!strcasecmp(auxkey_sds, "repl-stream-db")) {
    // TODO
  } else if (!strcasecmp(auxkey_sds, "repl-id")) {
    // TODO
  } else if (!strcasecmp(auxkey_sds, "repl-offset")) {
    // TODO
  } else if (!strcasecmp(auxkey_sds, "lua")) {
    ServerState* ss = ServerState::tlocal();
    Interpreter& script = ss->GetInterpreter();
    string_view body{auxval_sds, strlen(auxval_sds)};
    string result;
    Interpreter::AddResult add_result = script.AddFunction(body, &result);
    if (add_result == Interpreter::ADD_OK) {
      if (script_mgr_)
        script_mgr_->InsertFunction(result, body);
    } else if (add_result == Interpreter::COMPILE_ERR) {
      LOG(ERROR) << "Error when compiling lua scripts";
    }
  } else if (!strcasecmp(auxkey_sds, "redis-ver")) {
    LOG(INFO) << "Loading RDB produced by version " << auxval_sds;
  } else if (!strcasecmp(auxkey_sds, "ctime")) {
    time_t age = time(NULL) - strtol(auxval_sds, NULL, 10);
    if (age < 0)
      age = 0;
    LOG(INFO) << "RDB age " << strings::HumanReadableElapsedTime(age);
  } else if (!strcasecmp(auxkey_sds, "used-mem")) {
    long long usedmem = strtoll(auxval_sds, NULL, 10);
    LOG(INFO) << "RDB memory usage when created " << strings::HumanReadableNumBytes(usedmem);
  } else if (!strcasecmp(auxkey_sds, "aof-preamble")) {
    long long haspreamble = strtoll(auxval_sds, NULL, 10);
    if (haspreamble)
      LOG(INFO) << "RDB has an AOF tail";
  } else if (!strcasecmp(auxkey_sds, "redis-bits")) {
    /* Just ignored. */
  } else {
    /* We ignore fields we don't understand, as by AUX field
     * contract. */
    LOG(WARNING) << "Unrecognized RDB AUX field: '" << auxkey_sds << "'";
  }

  decrRefCount(auxkey);
  decrRefCount(auxval);

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

  auto cb = [indx = this->cur_db_index_, vec = std::move(out_buf)] { LoadItemsBuffer(indx, vec); };
  shard_set->Add(sid, std::move(cb));
}

void RdbLoader::LoadItemsBuffer(DbIndex db_ind, const ItemsBuf& ib) {
  DbSlice& db_slice = EngineShard::tlocal()->db_slice();
  for (const auto& item : ib) {
    std::string_view key{item.key, sdslen(item.key)};
    auto [it, added] = db_slice.AddOrFind(db_ind, key, PrimeValue{item.val}, item.expire_ms);

    if (!added) {
      LOG(WARNING) << "RDB has duplicated key '" << key << "' in DB " << db_ind;
    }

    sdsfree(item.key);
  }
}

auto RdbLoader::FetchGenericString(int flags) -> io::Result<OpaqueBuf> {
  bool isencoded;
  size_t len;

  SET_OR_UNEXPECT(LoadLen(&isencoded), len);

  if (isencoded) {
    switch (len) {
      case RDB_ENC_INT8:
      case RDB_ENC_INT16:
      case RDB_ENC_INT32:
        return FetchIntegerObject(len, flags, NULL);
      case RDB_ENC_LZF:
        return FetchLzfStringObject(flags);
      default:
        LOG(ERROR) << "Unknown RDB string encoding len " << len;
        return Unexpected(errc::rdb_file_corrupted);
    }
  }

  bool encode = (flags & RDB_LOAD_ENC) != 0;
  bool plain = (flags & RDB_LOAD_PLAIN) != 0;
  bool sds = (flags & RDB_LOAD_SDS) != 0;

  if (plain || sds) {
    if (plain && len == 0) {
      return make_pair(nullptr, 0);
    }

    char* buf = plain ? (char*)zmalloc(len) : sdsnewlen(SDS_NOINIT, len);
    error_code ec = FetchBuf(len, buf);
    if (ec) {
      if (plain)
        zfree(buf);
      else
        sdsfree(buf);
      return make_unexpected(ec);
    }

    return make_pair(buf, len);
  }

  robj* o = encode ? createStringObject(SDS_NOINIT, len) : createRawStringObject(SDS_NOINIT, len);
  error_code ec = FetchBuf(len, o->ptr);
  if (ec) {
    decrRefCount(o);
    return make_unexpected(ec);
  }
  return make_pair(o, len);
}

auto RdbLoader::FetchLzfStringObject(int flags) -> io::Result<OpaqueBuf> {
  bool zerocopy_decompress = true;

  const uint8_t* cbuf = NULL;
  char* val = NULL;

  uint64_t clen, len;

  SET_OR_UNEXPECT(LoadLen(NULL), clen);
  SET_OR_UNEXPECT(LoadLen(NULL), len);

  CHECK_LE(len, 1ULL << 29);

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

  bool plain = (flags & RDB_LOAD_PLAIN) != 0;
  bool sds = (flags & RDB_LOAD_SDS) != 0;
  DCHECK_EQ(false, plain && sds);

  /* Allocate our target according to the uncompressed size. */
  if (plain) {
    val = (char*)zmalloc(len);
  } else {
    val = sdsnewlen(SDS_NOINIT, len);
  }

  if (lzf_decompress(cbuf, clen, val, len) == 0) {
    LOG(ERROR) << "Invalid LZF compressed string";
    return Unexpected(errc::rdb_file_corrupted);
  }

  // FetchBuf consumes the input but if we have not went through that path
  // we need to consume now.
  if (zerocopy_decompress)
    mem_buf_.ConsumeInput(clen);

  if (plain || sds) {
    return make_pair(val, len);
  }

  return make_pair(createObject(OBJ_STRING, val), len);
}

auto RdbLoader::FetchIntegerObject(int enctype, int flags, size_t* lenptr)
    -> io::Result<OpaqueBuf> {
  bool plain = (flags & RDB_LOAD_PLAIN) != 0;
  bool sds = (flags & RDB_LOAD_SDS) != 0;
  bool encode = (flags & RDB_LOAD_ENC) != 0;
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

  if (plain || sds) {
    char buf[LONG_STR_SIZE], *p;
    int len = ll2string(buf, sizeof(buf), val);
    if (lenptr)
      *lenptr = len;
    p = plain ? (char*)zmalloc(len) : sdsnewlen(SDS_NOINIT, len);
    memcpy(p, buf, len);
    return make_pair(p, len);
  }

  robj* o = encode ? createStringObjectFromLongLongForValue(val)
                   : createObject(OBJ_STRING, sdsfromlonglong(val));
  return make_pair(o, 16);
}

io::Result<double> RdbLoader::FetchBinaryDouble() {
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

io::Result<double> RdbLoader::FetchDouble() {
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

auto RdbLoader::ReadKey() -> io::Result<sds> {
  auto res = FetchGenericString(RDB_LOAD_SDS);
  if (res) {
    sds k = (sds)res->first;
    DVLOG(2) << "Read " << std::string_view(k, sdslen(k));
    return k;
  }
  return res.get_unexpected();
}

io::Result<robj*> RdbLoader::ReadObj(int rdbtype) {
  io::Result<robj*> res_obj = nullptr;
  io::Result<OpaqueBuf> fetch_res;

  switch (rdbtype) {
    case RDB_TYPE_STRING:
      /* Read string value */
      fetch_res = FetchGenericString(RDB_LOAD_NONE);
      if (!fetch_res)
        return fetch_res.get_unexpected();
      res_obj = (robj*)fetch_res->first;
      break;
    case RDB_TYPE_SET:
      res_obj = ReadSet();
      break;
    case RDB_TYPE_SET_INTSET:
      res_obj = ReadIntSet();
      break;
    case RDB_TYPE_HASH_ZIPLIST:
      res_obj = ReadHZiplist();
      break;
    case RDB_TYPE_HASH:
      res_obj = ReadHSet();
      break;
    case RDB_TYPE_ZSET:
    case RDB_TYPE_ZSET_2:
      res_obj = ReadZSet(rdbtype);
      break;
    case RDB_TYPE_ZSET_ZIPLIST:
      res_obj = ReadZSetZL();
      break;
    case RDB_TYPE_LIST_QUICKLIST:
      res_obj = ReadListQuicklist(rdbtype);
      break;
    default:
      LOG(ERROR) << "Unsupported rdb type " << rdbtype;
      return Unexpected(errc::invalid_encoding);
  }

  return res_obj;
}

io::Result<robj*> RdbLoader::ReadSet() {
  size_t len;
  SET_OR_UNEXPECT(LoadLen(NULL), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  robj* res = nullptr;
  sds sdsele = nullptr;

  auto cleanup = absl::MakeCleanup([&] {
    if (sdsele)
      sdsfree(sdsele);
    decrRefCount(res);
  });

  /* Use a regular set when there are too many entries. */
  if (len > SetFamily::MaxIntsetEntries()) {
    res = createSetObject();
    /* It's faster to expand the dict to the right size asap in order
     * to avoid rehashing */
    if (len > DICT_HT_INITIAL_SIZE && dictTryExpand((dict*)res->ptr, len) != DICT_OK) {
      LOG(ERROR) << "OOM in dictTryExpand " << len;
      return Unexpected(errc::out_of_memory);
    }
  } else {
    // TODO: why do we bother creating intset if it was recorded as non intset?
    res = createIntsetObject();
  }

  /* Load every single element of the set */
  for (size_t i = 0; i < len; i++) {
    long long llval;
    io::Result<OpaqueBuf> fetch = FetchGenericString(RDB_LOAD_SDS);
    if (!fetch) {
      return make_unexpected(fetch.error());
    }
    sdsele = (sds)fetch->first;

    if (res->encoding == OBJ_ENCODING_INTSET) {
      /* Fetch integer value from element. */
      if (isSdsRepresentableAsLongLong(sdsele, &llval) == C_OK) {
        uint8_t success;
        res->ptr = intsetAdd((intset*)res->ptr, llval, &success);
        if (!success) {
          LOG(ERROR) << "Duplicate set members detected";
          return Unexpected(errc::duplicate_key);
        }
      } else {
        dict* ds = dictCreate(&setDictType);
        if (dictTryExpand((dict*)res->ptr, len) != DICT_OK) {
          dictRelease(ds);
          LOG(ERROR) << "OOM in dictTryExpand " << len;
          return Unexpected(errc::out_of_memory);
        }
        SetFamily::ConvertTo((intset*)res->ptr, ds);
        zfree(res->ptr);
        res->ptr = ds;
        res->encoding = OBJ_ENCODING_HT;
      }
    }

    /* This will also be called when the set was just converted
     * to a regular hash table encoded set. */
    if (res->encoding == OBJ_ENCODING_HT) {
      if (dictAdd((dict*)res->ptr, sdsele, NULL) != DICT_OK) {
        LOG(ERROR) << "Duplicate set members detected";
        return Unexpected(errc::duplicate_key);
      }
    } else {
      sdsfree(sdsele);
    }
  }

  std::move(cleanup).Cancel();

  return res;
}

::io::Result<robj*> RdbLoader::ReadIntSet() {
  OpaqueBuf fetch;
  SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_PLAIN), fetch);

  if (fetch.second == 0) {
    return Unexpected(errc::rdb_file_corrupted);
  }
  DCHECK(fetch.first);

  if (!intsetValidateIntegrity((uint8_t*)fetch.first, fetch.second, 0)) {
    LOG(ERROR) << "Intset integrity check failed.";
    zfree(fetch.first);
    return Unexpected(errc::rdb_file_corrupted);
  }

  intset* is = (intset*)fetch.first;
  robj* res;
  unsigned len = intsetLen(is);
  if (len > SetFamily::MaxIntsetEntries()) {
    res = createSetObject();
    if (len > DICT_HT_INITIAL_SIZE && dictTryExpand((dict*)res->ptr, len) != DICT_OK) {
      LOG(ERROR) << "OOM in dictTryExpand " << len;
      decrRefCount(res);
      return Unexpected(errc::out_of_memory);
    }
    SetFamily::ConvertTo(is, (dict*)res->ptr);
    zfree(is);
  } else {
    res = createObject(OBJ_SET, is);
    res->encoding = OBJ_ENCODING_INTSET;
  }
  return res;
}

io::Result<robj*> RdbLoader::ReadHZiplist() {
  OpaqueBuf fetch;
  SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_PLAIN), fetch);

  if (fetch.second == 0) {
    return Unexpected(errc::rdb_file_corrupted);
  }
  DCHECK(fetch.first);

  unsigned char* lp = lpNew(fetch.second);
  if (!ziplistPairsConvertAndValidateIntegrity((uint8_t*)fetch.first, fetch.second, &lp)) {
    LOG(ERROR) << "Zset ziplist integrity check failed.";
    zfree(lp);
    zfree(fetch.first);
    return Unexpected(errc::rdb_file_corrupted);
  }

  zfree(fetch.first);

  if (lpLength(lp) == 0) {
    lpFree(lp);

    return Unexpected(errc::empty_key);
  }

  robj* res = createObject(OBJ_HASH, lp);
  res->encoding = OBJ_ENCODING_LISTPACK;

  if (lpBytes(lp) > HSetFamily::MaxListPackLen())
    hashTypeConvert(res, OBJ_ENCODING_HT);
  else
    res->ptr = lpShrinkToFit((uint8_t*)res->ptr);

  return res;
}

io::Result<robj*> RdbLoader::ReadHSet() {
  uint64_t len;
  SET_OR_UNEXPECT(LoadLen(nullptr), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  sds field = nullptr;
  sds value = nullptr;
  robj* res = createHashObject();

  /* Too many entries? Use a hash table right from the start. */
  if (len > server.hash_max_listpack_entries)
    hashTypeConvert(res, OBJ_ENCODING_HT);

  auto cleanup = absl::Cleanup([&] {
    decrRefCount(res);
    if (field)
      sdsfree(field);
    if (value)
      sdsfree(value);
  });

  /* Load every field and value into the ziplist */
  while (res->encoding == OBJ_ENCODING_LISTPACK && len > 0) {
    len--;

    OpaqueBuf ofield, ovalue;
    SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_SDS), ofield);
    field = (sds)ofield.first;

    SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_SDS), ovalue);
    value = (sds)ovalue.first;

    /* Convert to hash table if size threshold is exceeded */
    if (sdslen(field) > server.hash_max_listpack_value ||
        sdslen(value) > server.hash_max_listpack_value ||
        !lpSafeToAdd((uint8_t*)res->ptr, sdslen(field) + sdslen(value))) {
      hashTypeConvert(res, OBJ_ENCODING_HT);
      int ret = dictAdd((dict*)res->ptr, field, value);
      if (ret == DICT_ERR) {
        return Unexpected(errc::rdb_file_corrupted);
      }
      break;
    }

    /* Add pair to listpack */
    res->ptr = lpAppend((uint8_t*)res->ptr, (uint8_t*)field, sdslen(field));
    res->ptr = lpAppend((uint8_t*)res->ptr, (uint8_t*)value, sdslen(value));

    sdsfree(field);
    sdsfree(value);
    field = value = nullptr;
  }

  if (res->encoding == OBJ_ENCODING_HT) {
    if (len > DICT_HT_INITIAL_SIZE) {
      if (dictTryExpand((dict*)res->ptr, len) != DICT_OK) {
        LOG(ERROR) << "OOM in dictTryExpand " << len;
        return Unexpected(errc::out_of_memory);
      }
    }

    /* Load remaining fields and values into the hash table */
    while (len > 0) {
      len--;

      OpaqueBuf ofield, ovalue;
      SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_SDS), ofield);
      field = (sds)ofield.first;

      SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_SDS), ovalue);
      value = (sds)ovalue.first;

      /* Add pair to hash table */
      int ret = dictAdd((dict*)res->ptr, field, value);
      if (ret == DICT_ERR) {
        LOG(ERROR) << "Duplicate hash fields detected";
        return Unexpected(errc::rdb_file_corrupted);
      }
    }
  }
  DCHECK_EQ(0u, len);

  std::move(cleanup).Cancel();
  return res;
}

io::Result<robj*> RdbLoader::ReadZSet(int rdbtype) {
  /* Read sorted set value. */
  uint64_t zsetlen;
  SET_OR_UNEXPECT(LoadLen(nullptr), zsetlen);

  if (zsetlen == 0)
    return Unexpected(errc::empty_key);

  robj* res = createZsetObject();
  zset* zs = (zset*)res->ptr;
  sds sdsele = nullptr;

  auto cleanup = absl::Cleanup([&] {
    decrRefCount(res);
    if (sdsele)
      sdsfree(sdsele);
  });

  if (zsetlen > DICT_HT_INITIAL_SIZE && dictTryExpand(zs->dict, zsetlen) != DICT_OK) {
    LOG(ERROR) << "OOM in dictTryExpand " << zsetlen;
    return Unexpected(errc::out_of_memory);
  }

  size_t maxelelen = 0, totelelen = 0;

  /* Load every single element of the sorted set. */
  while (zsetlen--) {
    double score;
    zskiplistNode* znode;

    OpaqueBuf fetch;
    SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_SDS), fetch);

    sdsele = (sds)fetch.first;

    if (rdbtype == RDB_TYPE_ZSET_2) {
      SET_OR_UNEXPECT(FetchBinaryDouble(), score);
    } else {
      SET_OR_UNEXPECT(FetchDouble(), score);
    }

    if (isnan(score)) {
      LOG(ERROR) << "Zset with NAN score detected";
      return Unexpected(errc::rdb_file_corrupted);
    }

    /* Don't care about integer-encoded strings. */
    if (sdslen(sdsele) > maxelelen)
      maxelelen = sdslen(sdsele);
    totelelen += sdslen(sdsele);

    znode = zslInsert(zs->zsl, score, sdsele);
    int ret = dictAdd(zs->dict, sdsele, &znode->score);
    sdsele = nullptr;

    if (ret != DICT_OK) {
      LOG(ERROR) << "Duplicate zset fields detected";
      return Unexpected(errc::rdb_file_corrupted);
    }
  }

  /* Convert *after* loading, since sorted sets are not stored ordered. */
  if (zsetLength(res) <= server.zset_max_listpack_entries &&
      maxelelen <= server.zset_max_listpack_value && lpSafeToAdd(NULL, totelelen)) {
    zsetConvert(res, OBJ_ENCODING_LISTPACK);
  }

  std::move(cleanup).Cancel();

  return res;
}

io::Result<robj*> RdbLoader::ReadZSetZL() {
  OpaqueBuf fetch;
  SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_PLAIN), fetch);

  if (fetch.second == 0) {
    return Unexpected(errc::rdb_file_corrupted);
  }
  DCHECK(fetch.first);

  unsigned char* lp = lpNew(fetch.second);
  if (!ziplistPairsConvertAndValidateIntegrity((uint8_t*)fetch.first, fetch.second, &lp)) {
    LOG(ERROR) << "Zset ziplist integrity check failed.";
    zfree(lp);
    zfree(fetch.first);
    return Unexpected(errc::rdb_file_corrupted);
  }

  zfree(fetch.first);

  if (lpLength(lp) == 0) {
    lpFree(lp);

    return Unexpected(errc::empty_key);
  }

  robj* res = createObject(OBJ_ZSET, lp);
  res->encoding = OBJ_ENCODING_LISTPACK;

  if (lpBytes(lp) > server.zset_max_listpack_entries)
    zsetConvert(res, OBJ_ENCODING_SKIPLIST);
  else
    res->ptr = lpShrinkToFit(lp);

  return res;
}

io::Result<robj*> RdbLoader::ReadListQuicklist(int rdbtype) {
  uint64_t len;
  SET_OR_UNEXPECT(LoadLen(nullptr), len);

  if (len == 0)
    return Unexpected(errc::empty_key);

  quicklist* ql = quicklistNew(FLAGS_list_max_listpack_size, FLAGS_list_compress_depth);
  uint64_t container = QUICKLIST_NODE_CONTAINER_PACKED;

  auto cleanup = absl::Cleanup([&] { quicklistRelease(ql); });

  while (len--) {
    uint8_t* lp = nullptr;

    if (rdbtype == RDB_TYPE_LIST_QUICKLIST_2) {
      SET_OR_UNEXPECT(LoadLen(nullptr), container);

      if (container != QUICKLIST_NODE_CONTAINER_PACKED &&
          container != QUICKLIST_NODE_CONTAINER_PLAIN) {
        LOG(ERROR) << "Quicklist integrity check failed.";
        return Unexpected(errc::rdb_file_corrupted);
      }
    }

    OpaqueBuf data;
    SET_OR_UNEXPECT(FetchGenericString(RDB_LOAD_PLAIN), data);
    if (data.second == 0) {
      return Unexpected(errc::rdb_file_corrupted);
    }

    if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
      quicklistAppendPlainNode(ql, (uint8_t*)data.first, data.second);
      continue;
    }

    if (rdbtype == RDB_TYPE_LIST_QUICKLIST_2) {
      lp = (uint8_t*)data.first;
      if (!lpValidateIntegrity(lp, data.second, 0, NULL, NULL)) {
        LOG(ERROR) << "Listpack integrity check failed.";
        zfree(lp);
        return Unexpected(errc::rdb_file_corrupted);
      }
    } else {
      lp = lpNew(data.second);
      if (!ziplistValidateIntegrity((uint8_t*)data.first, data.second, 1,
                                    ziplistEntryConvertAndValidate, &lp)) {
        LOG(ERROR) << "Ziplist integrity check failed.";
        zfree(data.first);
        zfree(lp);
        return Unexpected(errc::rdb_file_corrupted);
      }
      zfree(data.first);
      lp = lpShrinkToFit(lp);
    }

    /* Silently skip empty ziplists, if we'll end up with empty quicklist we'll fail later. */
    if (lpLength(lp) == 0) {
      zfree(lp);
      continue;
    }
    quicklistAppendListpack(ql, lp);
  }  // while

  if (quicklistCount(ql) == 0) {
    return Unexpected(errc::empty_key);
  }

  std::move(cleanup).Cancel();

  robj* res = createObject(OBJ_LIST, ql);
  res->encoding = OBJ_ENCODING_QUICKLIST;

  return res;
}

void RdbLoader::ResizeDb(size_t key_num, size_t expire_num) {
  DCHECK_LT(key_num, 1U << 31);
  DCHECK_LT(expire_num, 1U << 31);
}

error_code RdbLoader::LoadKeyValPair(int type, ObjSettings* settings) {
  /* Read key */
  sds key;
  robj* val;

  SET_OR_RETURN(ReadKey(), key);

  auto key_cleanup = absl::MakeCleanup([key] { sdsfree(key); });

  SET_OR_RETURN(ReadObj(type), val);

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
    decrRefCount(val);
  } else {
    std::move(key_cleanup).Cancel();

    std::string_view str_key(key, sdslen(key));
    ShardId sid = Shard(str_key, shard_set->size());
    uint64_t expire_at_ms = settings->expiretime;

    auto& out_buf = shard_buf_[sid];
    out_buf.emplace_back(Item{key, val, expire_at_ms});

    constexpr size_t kBufSize = 128;
    if (out_buf.size() >= kBufSize) {
      FlushShardAsync(sid);
    }
  }

  return kOk;
}

template <typename T> io::Result<T> RdbLoader::FetchInt() {
  auto ec = EnsureRead(sizeof(T));
  if (ec)
    return make_unexpected(ec);

  char buf[16];
  mem_buf_.ReadAndConsume(sizeof(T), buf);

  return base::LE::LoadT<std::make_unsigned_t<T>>(buf);
}

}  // namespace dfly
