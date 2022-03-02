// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load.h"

extern "C" {

#include "redis/lzfP.h" /* LZF compression library */
#include "redis/rdb.h"
#include "redis/zmalloc.h"
#include "redis/util.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include "base/endian.h"
#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "strings/human_readable.h"

namespace dfly {

using namespace std;
using base::IoBuf;
using nonstd::make_unexpected;
using namespace util;
using rdb::errc;
using facade::operator""_KB;

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

static error_category rdb_category;

inline error_code RdbError(int ev) {
  return error_code{ev, rdb_category};
}

static const error_code kOk;

RdbLoader::RdbLoader() : mem_buf_{16_KB} {
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

  while (1) {
    /* Read type. */
    auto expected_type = FetchType();
    if (!expected_type)
      return expected_type.error();
    type = expected_type.value();

    /* Handle special types. */
    if (type == RDB_OPCODE_EXPIRETIME) {
      LOG(FATAL) << "TBD";
      continue; /* Read next opcode. */
    }

    if (type == RDB_OPCODE_EXPIRETIME_MS) {
      LOG(FATAL) << "TBD";
      continue; /* Read next opcode. */
    }

    if (type == RDB_OPCODE_FREQ) {
      /* FREQ: LFU frequency. */
      FetchInt<uint8_t>();  // IGNORE
      continue;             /* Read next opcode. */
    }

    if (type == RDB_OPCODE_IDLE) {
      /* IDLE: LRU idle time. */
      LOG(FATAL) << "TBD";
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

      if (dbid > kMaxDbId) {
        return RdbError(errc::bad_db_index);
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
      return RdbError(errc::module_not_supported);
    }

    if (!rdbIsObjectType(type)) {
      return RdbError(errc::invalid_rdb_type);
    }
    RETURN_ON_ERR(LoadKeyValPair(type));
  }  // main load loop

  /* Verify the checksum if RDB version is >= 5 */
  RETURN_ON_ERR(VerifyChecksum());

  absl::Duration dur = absl::Now() - start;
  double seconds = double(absl::ToInt64Milliseconds(dur)) / 1000;
  LOG(INFO) << "Loading finished after " << strings::HumanReadableElapsedTime(seconds);

  return kOk;
}

error_code RdbLoader::EnsureReadInternal(size_t min_sz) {
  DCHECK_LT(mem_buf_.InputLen(), min_sz);

  auto out_buf = mem_buf_.AppendBuffer();
  CHECK_GT(out_buf.size(), min_sz);

  io::Result<size_t> res = src_->ReadAtLeast(out_buf, min_sz);
  if (!res)
    return res.error();

  if (*res < min_sz)
    return RdbError(errc::rdb_file_corrupted);

  bytes_read_ += *res;
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
    LOG(FATAL) << "Unknown length encoding " << type << " in rdbLoadLen()";
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

  if (size > 512) {  // Worth reading directly into next.
    io::MutableBytes mb{next, size};

    SET_OR_RETURN(src_->Read(mb), bytes_read);
    if (bytes_read < size)
      return RdbError(errc::rdb_file_corrupted);

    bytes_read_ += bytes_read;

    return kOk;
  }

  io::MutableBytes mb = mem_buf_.AppendBuffer();

  // Must be because mem_buf_ is be empty.
  DCHECK_GT(mb.size(), size);

  SET_OR_RETURN(src_->ReadAtLeast(mb, size), bytes_read);

  if (bytes_read < size)
    return RdbError(errc::rdb_file_corrupted);
  bytes_read_ += bytes_read;

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
  if (((char*)auxkey->ptr)[0] == '%') {
    /* All the fields with a name staring with '%' are considered
     * information fields and are logged at startup with a log
     * level of NOTICE. */
    LOG(INFO) << "RDB '" << (char*)auxkey->ptr << "': " << (char*)auxval->ptr;
  } else if (!strcasecmp((char*)auxkey->ptr, "repl-stream-db")) {
    // TODO
  } else if (!strcasecmp((char*)auxkey->ptr, "repl-id")) {
    // TODO
  } else if (!strcasecmp((char*)auxkey->ptr, "repl-offset")) {
    // TODO
  } else if (!strcasecmp((char*)auxkey->ptr, "lua")) {
    LOG(FATAL) << "Lua scripts are not supported";
  } else if (!strcasecmp((char*)auxkey->ptr, "redis-ver")) {
    LOG(INFO) << "Loading RDB produced by version " << (char*)auxval->ptr;
  } else if (!strcasecmp((char*)auxkey->ptr, "ctime")) {
    time_t age = time(NULL) - strtol((char*)auxval->ptr, NULL, 10);
    if (age < 0)
      age = 0;
    LOG(INFO) << "RDB age " << strings::HumanReadableElapsedTime(age);
  } else if (!strcasecmp((char*)auxkey->ptr, "used-mem")) {
    long long usedmem = strtoll((char*)auxval->ptr, NULL, 10);
    LOG(INFO) << "RDB memory usage when created " << strings::HumanReadableNumBytes(usedmem);
  } else if (!strcasecmp((char*)auxkey->ptr, "aof-preamble")) {
    long long haspreamble = strtoll((char*)auxval->ptr, NULL, 10);
    if (haspreamble)
      LOG(INFO) << "RDB has an AOF tail";
  } else if (!strcasecmp((char*)auxkey->ptr, "redis-bits")) {
    /* Just ignored. */
  } else {
    /* We ignore fields we don't understand, as by AUX field
     * contract. */
    LOG(WARNING) << "Unrecognized RDB AUX field: '" << (char*)auxkey->ptr << "'";
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
        LOG(FATAL) << "TBD";
      default:
        LOG(FATAL) << "Unknown RDB string encoding type " << len;
    }
  }

  bool encode = (flags & RDB_LOAD_ENC) != 0;
  bool plain = (flags & RDB_LOAD_PLAIN) != 0;
  bool sds = (flags & RDB_LOAD_SDS) != 0;

  if (plain || sds) {
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
    return make_unexpected(RdbError(errc::invalid_encoding));
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

auto RdbLoader::ReadKey() -> io::Result<sds> {
  auto res = FetchGenericString(RDB_LOAD_SDS);
  if (res) {
    sds k = (sds)res->first;
    DVLOG(2) << "Read " << std::string_view(k, sdslen(k));
    return k;
  }
  return res.get_unexpected();
}

auto RdbLoader::ReadObj(int rdbtype) -> io::Result<robj*> {
  if (rdbtype == RDB_TYPE_STRING) {
    /* Read string value */
    auto res = FetchGenericString(0);
    if (!res)
      return res.get_unexpected();
    return (robj*)res->first;
  }

  LOG(FATAL) << "TBD " << rdbtype;
  return NULL;
}

void RdbLoader::ResizeDb(size_t key_num, size_t expire_num) {
  DCHECK_LT(key_num, 1U << 31);
  DCHECK_LT(expire_num, 1U << 31);
}

error_code RdbLoader::LoadKeyValPair(int type) {
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
  bool should_expire = false;  // TODO: to implement
  if (should_expire) {
    decrRefCount(val);
  } else {
    std::move(key_cleanup).Cancel();

    // TODO: we should handle the duplicates.
    if (false) {
      LOG(WARNING) << "RDB has duplicated key '" << std::string_view(key, sdslen(key)) << "' in DB "
                   << cur_db_index_;
      return RdbError(errc::duplicate_key);
    }

    /* call key space notification on key loaded for modules only */
    // moduleNotifyKeyspaceEvent(NOTIFY_LOADED, "loaded", &keyobj, db->id);
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
