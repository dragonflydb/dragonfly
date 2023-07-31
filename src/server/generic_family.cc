// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

extern "C" {
#include "redis/crc64.h"
#include "redis/object.h"
#include "redis/util.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "redis/rdb.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/rdb_extensions.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/transaction.h"
#include "util/varz.h"

ABSL_FLAG(uint32_t, dbnum, 16, "Number of databases");
ABSL_FLAG(uint32_t, keys_output_limit, 8192, "Maximum number of keys output by keys command");
ABSL_DECLARE_FLAG(int, compression_mode);

namespace dfly {
using namespace std;
using namespace facade;

namespace {

using VersionBuffer = std::array<char, sizeof(uint16_t)>;
using CrcBuffer = std::array<char, sizeof(uint64_t)>;
constexpr size_t DUMP_FOOTER_SIZE = sizeof(uint64_t) + sizeof(uint16_t);  // version number and crc

int64_t CalculateExpirationTime(bool seconds, bool absolute, int64_t ts, int64_t now_msec) {
  int64_t msec = seconds ? ts * 1000 : ts;
  int64_t rel_msec = absolute ? msec - now_msec : msec;
  return rel_msec;
}

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

void AppendFooter(std::string* dump_res) {
  /* Write the footer, this is how it looks like:
   * ----------------+---------------------+---------------+
   * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
   * ----------------+---------------------+---------------+
   * RDB version and CRC are both in little endian.
   */
  const auto ver = MakeRdbVersion();
  dump_res->append(ver.data(), ver.size());
  const auto crc = MakeCheckSum(*dump_res);
  dump_res->append(crc.data(), crc.size());
}

bool VerifyFooter(std::string_view msg, int* rdb_version) {
  if (msg.size() <= DUMP_FOOTER_SIZE) {
    LOG(WARNING) << "got restore payload that is too short - " << msg.size();
    return false;
  }
  const uint8_t* footer =
      reinterpret_cast<const uint8_t*>(msg.data()) + (msg.size() - DUMP_FOOTER_SIZE);
  uint16_t version = (*(footer + 1) << 8 | (*footer));
  *rdb_version = version;
  if (version > RDB_VERSION) {
    LOG(WARNING) << "got restore payload with illegal version - supporting version up to "
                 << RDB_VERSION << " got version " << version;
    return false;
  }
  uint64_t expected_cs =
      crc64(0, reinterpret_cast<const uint8_t*>(msg.data()), msg.size() - sizeof(uint64_t));
  uint64_t actual_cs = absl::little_endian::Load64(footer + sizeof(version));
  if (actual_cs != expected_cs) {
    LOG(WARNING) << "CRC check failed for restore command, expecting: " << expected_cs << " got "
                 << actual_cs;
    return false;
  }
  return true;
}

class InMemSource : public ::io::Source {
 public:
  InMemSource(std::string_view buf) : buf_(buf) {
  }

  ::io::Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

 protected:
  std::string_view buf_;
  off_t offs_ = 0;
};

::io::Result<size_t> InMemSource::ReadSome(const iovec* v, uint32_t len) {
  ssize_t read_total = 0;
  while (size_t(offs_) < buf_.size() && len > 0) {
    size_t read_sz = min(buf_.size() - offs_, v->iov_len);
    memcpy(v->iov_base, buf_.data() + offs_, read_sz);
    read_total += read_sz;
    offs_ += read_sz;

    ++v;
    --len;
  }

  return read_total;
}

class RdbRestoreValue : protected RdbLoaderBase {
 public:
  RdbRestoreValue(int rdb_version) {
    rdb_version_ = rdb_version;
  }

  bool Add(std::string_view payload, std::string_view key, DbSlice& db_slice, DbIndex index,
           uint64_t expire_ms);

 private:
  std::optional<OpaqueObj> Parse(std::string_view payload);
};

std::optional<RdbLoaderBase::OpaqueObj> RdbRestoreValue::Parse(std::string_view payload) {
  InMemSource source(payload);
  src_ = &source;
  if (io::Result<uint8_t> type_id = FetchType(); type_id && rdbIsObjectTypeDF(type_id.value())) {
    OpaqueObj obj;
    error_code ec = ReadObj(type_id.value(), &obj);  // load the type from the input stream
    if (ec) {
      LOG(ERROR) << "failed to load data for type id " << (unsigned int)type_id.value();
      return std::nullopt;
    }

    return std::optional<OpaqueObj>(std::move(obj));
  } else {
    LOG(ERROR) << "failed to load type id from the input stream or type id is invalid";
    return std::nullopt;
  }
}

bool RdbRestoreValue::Add(std::string_view data, std::string_view key, DbSlice& db_slice,
                          DbIndex index, uint64_t expire_ms) {
  auto opaque_res = Parse(data);
  if (!opaque_res) {
    return false;
  }

  PrimeValue pv;
  if (auto ec = FromOpaque(*opaque_res, &pv); ec) {
    // we failed - report and exit
    LOG(WARNING) << "error while trying to save data: " << ec;
    return false;
  }

  DbContext context{.db_index = index, .time_now_ms = GetCurrentTimeMs()};
  auto [it, added] = db_slice.AddOrSkip(context, key, std::move(pv), expire_ms);
  return added;
}

class RestoreArgs {
  static constexpr int64_t NO_EXPIRATION = 0;

  int64_t expiration_ = NO_EXPIRATION;
  bool abs_time_ = false;
  bool replace_ = false;  // if true, over-ride existing key

 public:
  constexpr bool Replace() const {
    return replace_;
  }

  constexpr int64_t ExpirationTime() const {
    return expiration_;
  }

  [[nodiscard]] constexpr bool Expired() const {
    return ExpirationTime() < 0;
  }

  [[nodiscard]] constexpr bool HasExpiration() const {
    return expiration_ != NO_EXPIRATION;
  }

  [[nodiscard]] bool UpdateExpiration(int64_t now_msec);

  static OpResult<RestoreArgs> TryFrom(const CmdArgList& args);
};

[[nodiscard]] bool RestoreArgs::UpdateExpiration(int64_t now_msec) {
  if (HasExpiration()) {
    auto new_ttl = CalculateExpirationTime(!abs_time_, abs_time_, expiration_, now_msec);
    if (new_ttl > kMaxExpireDeadlineSec * 1000) {
      return false;
    }
    expiration_ = new_ttl;
    if (new_ttl > 0) {
      expiration_ += now_msec;
    }
  }
  return true;
}

// The structure that we are expecting is:
// args[0] == "key"
// args[1] == "ttl"
// args[2] == serialized value (list of chars that are used for the actual restore).
// args[3] .. args[n]: optional arguments that can be [REPLACE] [ABSTTL] [IDLETIME seconds]
//            [FREQ frequency], in any order
OpResult<RestoreArgs> RestoreArgs::TryFrom(const CmdArgList& args) {
  RestoreArgs out_args;
  std::string_view cur_arg = ArgS(args, 1);  // extract ttl
  if (!absl::SimpleAtoi(cur_arg, &out_args.expiration_) || (out_args.expiration_ < 0)) {
    return OpStatus::INVALID_INT;
  }

  // the 3rd arg is the serialized value, so we are starting from one pass it
  // Note that all these are actually optional
  // note about the redis doc for this command: https://redis.io/commands/restore/
  // the IDLETIME and FREQ are not required, but to make this the same as in redis
  // we would parse them and ensure that they are correct, maybe later they will be used
  int64_t idle_time = 0;

  for (size_t i = 3; i < args.size(); ++i) {
    ToUpper(&args[i]);
    cur_arg = ArgS(args, i);
    bool additional = args.size() - i - 1 >= 1;
    if (cur_arg == "REPLACE") {
      out_args.replace_ = true;
    } else if (cur_arg == "ABSTTL") {
      out_args.abs_time_ = true;
    } else if (cur_arg == "IDLETIME" && additional) {
      ++i;
      cur_arg = ArgS(args, i);
      if (!absl::SimpleAtoi(cur_arg, &idle_time)) {
        return OpStatus::INVALID_INT;
      }
      if (idle_time < 0) {
        return OpStatus::SYNTAX_ERR;
      }
    } else if (cur_arg == "FREQ" && additional) {
      ++i;
      cur_arg = ArgS(args, i);
      int freq = 0;
      if (!absl::SimpleAtoi(cur_arg, &freq)) {
        return OpStatus::INVALID_INT;
      }
      if (freq < 0 || freq > 255) {
        return OpStatus::OUT_OF_RANGE;  // need to translate in this case
      }
    } else {
      LOG(WARNING) << "Got unknown command line option for restore '" << cur_arg << "'";
      return OpStatus::SYNTAX_ERR;
    }
  }
  return out_args;
}

OpStatus OpPersist(const OpArgs& op_args, string_view key);

class Renamer {
 public:
  Renamer(ShardId source_id) : src_sid_(source_id) {
  }

  void Find(Transaction* t);

  OpResult<void> status() const {
    return status_;
  };

  void Finalize(Transaction* t, bool skip_exist_dest);

 private:
  OpStatus MoveSrc(Transaction* t, EngineShard* es);
  OpStatus UpdateDest(Transaction* t, EngineShard* es);

  ShardId src_sid_;

  struct FindResult {
    string_view key;
    PrimeValue ref_val;
    uint64_t expire_ts;
    bool sticky;
    bool found = false;
  };

  PrimeValue pv_;
  string str_val_;

  FindResult src_res_, dest_res_;  // index 0 for source, 1 for destination
  OpResult<void> status_;
};

void Renamer::Find(Transaction* t) {
  auto cb = [this](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    CHECK_EQ(1u, args.size());

    FindResult* res = (shard->shard_id() == src_sid_) ? &src_res_ : &dest_res_;

    res->key = args.front();
    auto& db_slice = EngineShard::tlocal()->db_slice();
    auto [it, exp_it] = db_slice.FindExt(t->GetDbContext(), res->key);

    res->found = IsValid(it);
    if (IsValid(it)) {
      res->ref_val = it->second.AsRef();
      res->expire_ts = db_slice.ExpireTime(exp_it);
      res->sticky = it->first.IsSticky();
    }
    return OpStatus::OK;
  };

  t->Execute(move(cb), false);
};

void Renamer::Finalize(Transaction* t, bool skip_exist_dest) {
  if (!src_res_.found) {
    status_ = OpStatus::KEY_NOTFOUND;
    t->Conclude();
    return;
  }

  if (dest_res_.found && skip_exist_dest) {
    status_ = OpStatus::KEY_EXISTS;
    t->Conclude();
    return;
  }

  DCHECK(src_res_.ref_val.IsRef());

  // Src key exist and we need to override the destination.
  // Alternatively, we could apply an optimistic algorithm and move src at Find step.
  // We would need to restore the state in case of cleanups.
  t->Execute([&](Transaction* t, EngineShard* shard) { return MoveSrc(t, shard); }, false);
  t->Execute([&](Transaction* t, EngineShard* shard) { return UpdateDest(t, shard); }, true);
}

OpStatus Renamer::MoveSrc(Transaction* t, EngineShard* es) {
  if (es->shard_id() == src_sid_) {  // Handle source key.
    // TODO: to call PreUpdate/PostUpdate.
    auto it = es->db_slice().FindExt(t->GetDbContext(), src_res_.key).first;
    CHECK(IsValid(it));

    // We distinguish because of the SmallString that is pinned to its thread by design,
    // thus can not be accessed via another thread.
    // Therefore, we copy it to standard string in its thread.
    if (it->second.ObjType() == OBJ_STRING) {
      it->second.GetString(&str_val_);
    } else {
      bool has_expire = it->second.HasExpire();
      pv_ = std::move(it->second);
      it->second.SetExpire(has_expire);
    }

    CHECK(es->db_slice().Del(t->GetDbIndex(), it));  // delete the entry with empty value in it.
    if (es->journal()) {
      RecordJournal(t->GetOpArgs(es), "DEL", ArgSlice{src_res_.key}, 2);
    }
  }

  return OpStatus::OK;
}

OpStatus Renamer::UpdateDest(Transaction* t, EngineShard* es) {
  if (es->shard_id() != src_sid_) {
    auto& db_slice = es->db_slice();
    string_view dest_key = dest_res_.key;
    PrimeIterator dest_it = db_slice.FindExt(t->GetDbContext(), dest_key).first;
    bool is_prior_list = false;

    if (IsValid(dest_it)) {
      bool has_expire = dest_it->second.HasExpire();
      is_prior_list = dest_it->second.ObjType() == OBJ_LIST;

      if (src_res_.ref_val.ObjType() == OBJ_STRING) {
        dest_it->second.SetString(str_val_);
      } else {
        dest_it->second = std::move(pv_);
      }
      dest_it->second.SetExpire(has_expire);  // preserve expire flag.
      db_slice.UpdateExpire(t->GetDbIndex(), dest_it, src_res_.expire_ts);
    } else {
      if (src_res_.ref_val.ObjType() == OBJ_STRING) {
        pv_.SetString(str_val_);
      }
      dest_it = db_slice.AddNew(t->GetDbContext(), dest_key, std::move(pv_), src_res_.expire_ts);
    }

    dest_it->first.SetSticky(src_res_.sticky);

    if (!is_prior_list && dest_it->second.ObjType() == OBJ_LIST && es->blocking_controller()) {
      es->blocking_controller()->AwakeWatched(t->GetDbIndex(), dest_key);
    }
    if (es->journal()) {
      OpArgs op_args = t->GetOpArgs(es);
      string scratch;
      // todo insert under multi exec
      RecordJournal(op_args, "SET"sv, ArgSlice{dest_key, dest_it->second.GetSlice(&scratch)}, 2,
                    true);
      if (dest_it->first.IsSticky()) {
        RecordJournal(op_args, "STICK"sv, ArgSlice{dest_key}, 2, true);
      }
      if (dest_it->second.HasExpire()) {
        auto time = absl::StrCat(src_res_.expire_ts);
        RecordJournal(op_args, "PEXPIREAT"sv, ArgSlice{dest_key, time}, 2, true);
      }
      RecordJournalFinish(op_args, 2);
    }
  }

  return OpStatus::OK;
}

OpStatus OpPersist(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(op_args.db_cntx, key);

  if (!IsValid(it)) {
    return OpStatus::KEY_NOTFOUND;
  } else {
    if (IsValid(expire_it)) {
      // The SKIPPED not really used, just placeholder for error
      return db_slice.UpdateExpire(op_args.db_cntx.db_index, it, 0) ? OpStatus::OK
                                                                    : OpStatus::SKIPPED;
    }
    return OpStatus::SKIPPED;  // fall though - key does not have expiry
  }
}

OpResult<std::string> OpDump(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(op_args.db_cntx, key);

  if (IsValid(it)) {
    DVLOG(1) << "Dump: key '" << key << "' successfully found, going to dump it";
    std::unique_ptr<::io::StringSink> sink = std::make_unique<::io::StringSink>();
    int compression_mode = absl::GetFlag(FLAGS_compression_mode);
    CompressionMode serializer_compression_mode =
        compression_mode == 0 ? CompressionMode::NONE : CompressionMode::SINGLE_ENTRY;
    RdbSerializer serializer(serializer_compression_mode);

    // According to Redis code we need to
    // 1. Save the value itself - without the key
    // 2. Save footer: this include the RDB version and the CRC value for the message
    unsigned obj_type = it->second.ObjType();
    unsigned encoding = it->second.Encoding();
    auto type = RdbObjectType(obj_type, encoding);
    DVLOG(1) << "We are going to dump object type: " << type;
    std::error_code ec = serializer.WriteOpcode(type);
    CHECK(!ec);
    ec = serializer.SaveValue(it->second);
    CHECK(!ec);  // make sure that fully was successful
    ec = serializer.FlushToSink(sink.get());
    CHECK(!ec);  // make sure that fully was successful
    std::string dump_payload(sink->str());
    AppendFooter(&dump_payload);  // version and crc
    CHECK_GT(dump_payload.size(), 10u);
    return dump_payload;
  }
  // fallback
  DVLOG(1) << "Dump: '" << key << "' Not found";
  return OpStatus::KEY_NOTFOUND;
}

OpResult<bool> OnRestore(const OpArgs& op_args, std::string_view key, std::string_view payload,
                         RestoreArgs restore_args, int rdb_version) {
  if (!restore_args.UpdateExpiration(op_args.db_cntx.time_now_ms)) {
    return OpStatus::OUT_OF_RANGE;
  }

  auto& db_slice = op_args.shard->db_slice();
  // The redis impl (see cluster.c function restoreCommand), remove the old key if
  // the replace option is set, so lets do the same here
  auto [from_it, from_expire] = db_slice.FindExt(op_args.db_cntx, key);
  if (restore_args.Replace()) {
    if (IsValid(from_it)) {
      VLOG(1) << "restore command is running with replace, found old key '" << key
              << "' and removing it";
      CHECK(db_slice.Del(op_args.db_cntx.db_index, from_it));
    }
  } else {
    // we are not allowed to replace it, so make sure it doesn't exist
    if (IsValid(from_it)) {
      return OpStatus::KEY_EXISTS;
    }
  }

  if (restore_args.Expired()) {
    VLOG(1) << "the new key '" << key << "' already expired, will not save the value";
    return true;
  }

  RdbRestoreValue loader(rdb_version);

  return loader.Add(payload, key, db_slice, op_args.db_cntx.db_index,
                    restore_args.ExpirationTime());
}

bool ScanCb(const OpArgs& op_args, PrimeIterator it, const ScanOpts& opts, StringVec* res) {
  auto& db_slice = op_args.shard->db_slice();
  if (it->second.HasExpire()) {
    it = db_slice.ExpireIfNeeded(op_args.db_cntx, it).first;
  }

  if (!IsValid(it))
    return false;

  bool matches = opts.type_filter.empty() || ObjTypeName(it->second.ObjType()) == opts.type_filter;

  if (!matches)
    return false;

  if (opts.bucket_id != UINT_MAX && opts.bucket_id != it.bucket_id()) {
    return false;
  }

  string str = it->first.ToString();
  if (!opts.Matches(str)) {
    return false;
  }
  res->push_back(std::move(str));

  return true;
}

void OpScan(const OpArgs& op_args, const ScanOpts& scan_opts, uint64_t* cursor, StringVec* vec) {
  auto& db_slice = op_args.shard->db_slice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));

  unsigned cnt = 0;

  VLOG(1) << "PrimeTable " << db_slice.shard_id() << "/" << op_args.db_cntx.db_index << " has "
          << db_slice.DbSize(op_args.db_cntx.db_index);

  PrimeTable::Cursor cur = *cursor;
  auto [prime_table, expire_table] = db_slice.GetTables(op_args.db_cntx.db_index);
  do {
    cur = prime_table->Traverse(
        cur, [&](PrimeIterator it) { cnt += ScanCb(op_args, it, scan_opts, vec); });
  } while (cur && cnt < scan_opts.limit);

  VLOG(1) << "OpScan " << db_slice.shard_id() << " cursor: " << cur.value();
  *cursor = cur.value();
}

uint64_t ScanGeneric(uint64_t cursor, const ScanOpts& scan_opts, StringVec* keys,
                     ConnectionContext* cntx) {
  ShardId sid = cursor % 1024;

  EngineShardSet* ess = shard_set;
  unsigned shard_count = ess->size();

  // Dash table returns a cursor with its right byte empty. We will use it
  // for encoding shard index. For now scan has a limitation of 255 shards.
  CHECK_LT(shard_count, 1024u);

  if (sid >= shard_count) {  // protection
    return 0;
  }

  cursor >>= 10;
  DbContext db_cntx{.db_index = cntx->conn_state.db_index, .time_now_ms = GetCurrentTimeMs()};

  do {
    ess->Await(sid, [&] {
      OpArgs op_args{EngineShard::tlocal(), 0, db_cntx};
      OpScan(op_args, scan_opts, &cursor, keys);
    });
    if (cursor == 0) {
      ++sid;
      if (unsigned(sid) == shard_count)
        break;
    }
  } while (keys->size() < scan_opts.limit);

  if (sid < shard_count) {
    cursor = (cursor << 10) | sid;
  } else {
    DCHECK_EQ(0u, cursor);
  }

  return cursor;
}

OpStatus OpExpire(const OpArgs& op_args, string_view key, const DbSlice::ExpireParams& params) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(op_args.db_cntx, key);
  if (!IsValid(it)) {
    return OpStatus::KEY_NOTFOUND;
  }

  auto res = db_slice.UpdateExpire(op_args.db_cntx, it, expire_it, params);

  // If the value was deleted, replicate as DEL.
  // Else, replicate as PEXPIREAT with exact time.
  if (op_args.shard->journal() && res.ok()) {
    if (res.value() == -1) {
      RecordJournal(op_args, "DEL"sv, ArgSlice{key});
    } else {
      auto time = absl::StrCat(res.value());
      // Note: Don't forget to change this when adding arguments to expire commands.
      RecordJournal(op_args, "PEXPIREAT"sv, ArgSlice{key, time});
    }
  }

  return res.status();
}

}  // namespace

void GenericFamily::Init(util::ProactorPool* pp) {
}

void GenericFamily::Shutdown() {
}

void GenericFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Del " << ArgS(args, 0);

  atomic_uint32_t result{0};
  bool is_mc = cntx->protocol() == Protocol::MEMCACHE;

  auto cb = [&result](const Transaction* t, EngineShard* shard) {
    ArgSlice args = t->GetShardArgs(shard->shard_id());
    auto res = OpDel(t->GetOpArgs(shard), args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  DVLOG(2) << "Del ts " << transaction->txid();

  uint32_t del_cnt = result.load(memory_order_relaxed);
  if (is_mc) {
    using facade::MCReplyBuilder;
    MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(cntx->reply_builder());

    if (del_cnt == 0) {
      mc_builder->SendNotFound();
    } else {
      mc_builder->SendSimpleString("DELETED");
    }
  } else {
    (*cntx)->SendLong(del_cnt);
  }
}

void GenericFamily::Ping(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    return (*cntx)->SendError(facade::WrongNumArgsError("ping"), kSyntaxErrType);
  }

  // We synchronously block here until the engine sends us the payload and notifies that
  // the I/O operation has been processed.
  if (args.size() == 0) {
    return (*cntx)->SendSimpleString("PONG");
  } else {
    string_view arg = ArgS(args, 0);
    DVLOG(2) << "Ping " << arg;

    return (*cntx)->SendBulkString(arg);
  }
}

void GenericFamily::Exists(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Exists " << ArgS(args, 0);

  atomic_uint32_t result{0};

  auto cb = [&result](Transaction* t, EngineShard* shard) {
    ArgSlice args = t->GetShardArgs(shard->shard_id());
    auto res = OpExists(t->GetOpArgs(shard), args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  return (*cntx)->SendLong(result.load(memory_order_acquire));
}

void GenericFamily::Persist(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpPersist(t->GetOpArgs(shard), key); };

  OpStatus status = cntx->transaction->ScheduleSingleHop(move(cb));
  if (status == OpStatus::OK)
    (*cntx)->SendLong(1);
  else
    (*cntx)->SendLong(0);
}

void GenericFamily::Expire(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view sec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  if (int_arg > kMaxExpireDeadlineSec || int_arg < -kMaxExpireDeadlineSec) {
    return (*cntx)->SendError(InvalidExpireTime(cntx->cid->name()));
  }

  int_arg = std::max(int_arg, -1L);
  DbSlice::ExpireParams params{.value = int_arg};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };

  OpStatus status = cntx->transaction->ScheduleSingleHop(move(cb));
  (*cntx)->SendLong(status == OpStatus::OK);
}

void GenericFamily::ExpireAt(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view sec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  int_arg = std::max(int_arg, 0L);
  DbSlice::ExpireParams params{.value = int_arg, .absolute = true};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return (*cntx)->SendError(kExpiryOutOfRange);
  } else {
    (*cntx)->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::Keys(CmdArgList args, ConnectionContext* cntx) {
  string_view pattern(ArgS(args, 0));
  uint64_t cursor = 0;

  StringVec keys;

  ScanOpts scan_opts;
  scan_opts.pattern = pattern;
  scan_opts.limit = 512;
  auto output_limit = absl::GetFlag(FLAGS_keys_output_limit);

  do {
    cursor = ScanGeneric(cursor, scan_opts, &keys, cntx);
  } while (cursor != 0 && keys.size() < output_limit);

  (*cntx)->StartArray(keys.size());
  for (const auto& k : keys) {
    (*cntx)->SendBulkString(k);
  }
}

void GenericFamily::PexpireAt(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view msec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(msec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  int_arg = std::max(int_arg, 0L);
  DbSlice::ExpireParams params{.value = int_arg, .absolute = true, .unit = TimeUnit::MSEC};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return (*cntx)->SendError(kExpiryOutOfRange);
  } else {
    (*cntx)->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::Pexpire(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view msec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(msec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  int_arg = std::max(int_arg, 0L);
  DbSlice::ExpireParams params{.value = int_arg, .unit = TimeUnit::MSEC};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return (*cntx)->SendError(kExpiryOutOfRange);
  } else {
    (*cntx)->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::Stick(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Stick " << ArgS(args, 0);

  atomic_uint32_t result{0};

  auto cb = [&result](const Transaction* t, EngineShard* shard) {
    ArgSlice args = t->GetShardArgs(shard->shard_id());
    auto res = OpStick(t->GetOpArgs(shard), args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  DVLOG(2) << "Stick ts " << transaction->txid();

  uint32_t match_cnt = result.load(memory_order_relaxed);
  (*cntx)->SendLong(match_cnt);
}

// Used to conditionally store double score
struct SortEntryScore {
  double score;
};

// SortEntry stores all data required for sorting
template <bool ALPHA>
struct SortEntry
    // Store score only if we need it
    : public std::conditional_t<ALPHA, std::tuple<>, SortEntryScore> {
  std::string key;

  bool Parse(std::string&& item) {
    if constexpr (!ALPHA) {
      if (!absl::SimpleAtod(item, &this->score))
        return false;
    }
    key = std::move(item);
    return true;
  }

  bool Parse(int64_t item) {
    if constexpr (!ALPHA) {
      this->score = item;
    }
    key = absl::StrCat(item);
    return true;
  }

  std::conditional_t<ALPHA, const std::string&, double> Cmp() const {
    if constexpr (ALPHA) {
      return key;
    } else {
      return this->score;
    }
  }
};

// std::variant of all possible vectors of SortEntries
using SortEntryList = std::variant<
    // Used when sorting by double values
    std::vector<SortEntry<false>>,
    // Used when sorting by string values
    std::vector<SortEntry<true>>>;

// Create SortEntryList based on runtime arguments
SortEntryList MakeSortEntryList(bool alpha) {
  if (alpha)
    return SortEntryList{std::vector<SortEntry<true>>{}};
  else
    return SortEntryList{std::vector<SortEntry<false>>{}};
}

// Iterate over container with generic function that accepts strings and ints
template <typename F> bool Iterate(const PrimeValue& pv, F&& func) {
  auto cb = [&func](container_utils::ContainerEntry ce) {
    if (ce.value)
      return func(ce.ToString());
    else
      return func(ce.longval);
  };

  switch (pv.ObjType()) {
    case OBJ_LIST:
      return container_utils::IterateList(pv, cb);
    case OBJ_SET:
      return container_utils::IterateSet(pv, cb);
    case OBJ_ZSET:
      return container_utils::IterateSortedSet(
          pv.GetRobjWrapper(),
          [&cb](container_utils::ContainerEntry ce, double) { return cb(ce); });
    default:
      return false;
  }
}

// Create a SortEntryList from given key
OpResultTyped<SortEntryList> OpFetchSortEntries(const OpArgs& op_args, std::string_view key,
                                                bool alpha) {
  using namespace container_utils;

  auto [it, _] = op_args.shard->db_slice().FindExt(op_args.db_cntx, key);
  if (!IsValid(it) || !IsContainer(it->second)) {
    return OpStatus::KEY_NOTFOUND;
  }

  auto result = MakeSortEntryList(alpha);
  bool success = std::visit(
      [&pv = it->second](auto& entries) {
        entries.reserve(pv.Size());
        return Iterate(pv, [&entries](auto&& val) {
          return entries.emplace_back().Parse(std::forward<decltype(val)>(val));
        });
      },
      result);
  auto res = OpResultTyped{std::move(result)};
  res.setType(it->second.ObjType());
  return success ? res : OpStatus::WRONG_TYPE;
}

void GenericFamily::Sort(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  bool alpha = false;
  bool reversed = false;
  std::optional<std::pair<size_t, size_t>> bounds;

  for (size_t i = 1; i < args.size(); i++) {
    ToUpper(&args[i]);

    std::string_view arg = ArgS(args, i);
    if (arg == "ALPHA") {
      alpha = true;
    } else if (arg == "DESC") {
      reversed = true;
    } else if (arg == "LIMIT") {
      int offset, limit;
      if (i + 2 >= args.size()) {
        return (*cntx)->SendError(kSyntaxErr);
      }
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &offset) ||
          !absl::SimpleAtoi(ArgS(args, i + 2), &limit)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
      bounds = {offset, limit};
      i += 2;
    }
  }

  OpResultTyped<SortEntryList> fetch_result =
      cntx->transaction->ScheduleSingleHopT([&](Transaction* t, EngineShard* shard) {
        return OpFetchSortEntries(t->GetOpArgs(shard), key, alpha);
      });

  if (fetch_result.status() == OpStatus::WRONG_TYPE)
    return (*cntx)->SendError("One or more scores can't be converted into double");

  if (!fetch_result.ok())
    return (*cntx)->SendEmptyArray();

  auto result_type = fetch_result.type();
  auto sort_call = [cntx, bounds, reversed, result_type](auto& entries) {
    if (bounds) {
      auto sort_it = entries.begin() + std::min(bounds->first + bounds->second, entries.size());
      std::partial_sort(entries.begin(), sort_it, entries.end(),
                        [reversed](const auto& lhs, const auto& rhs) {
                          return bool(lhs.Cmp() < rhs.Cmp()) ^ reversed;
                        });
    } else {
      std::sort(entries.begin(), entries.end(), [reversed](const auto& lhs, const auto& rhs) {
        return bool(lhs.Cmp() < rhs.Cmp()) ^ reversed;
      });
    }

    auto start_it = entries.begin();
    auto end_it = entries.end();
    if (bounds) {
      start_it += std::min(bounds->first, entries.size());
      end_it = entries.begin() + std::min(bounds->first + bounds->second, entries.size());
    }

    bool is_set = (result_type == OBJ_SET || result_type == OBJ_ZSET);
    (*cntx)->StartCollection(std::distance(start_it, end_it),
                             is_set ? RedisReplyBuilder::SET : RedisReplyBuilder::ARRAY);

    for (auto it = start_it; it != end_it; ++it) {
      (*cntx)->SendBulkString(it->key);
    }
  };
  std::visit(std::move(sort_call), fetch_result.value());
}

void GenericFamily::Restore(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view serialized_value = ArgS(args, 2);
  int rdb_version = 0;
  if (!VerifyFooter(serialized_value, &rdb_version)) {
    return (*cntx)->SendError("ERR DUMP payload version or checksum are wrong");
  }

  OpResult<RestoreArgs> restore_args = RestoreArgs::TryFrom(args);
  if (!restore_args) {
    if (restore_args.status() == OpStatus::OUT_OF_RANGE) {
      return (*cntx)->SendError("Invalid IDLETIME value, must be >= 0");
    } else {
      return (*cntx)->SendError(restore_args.status());
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OnRestore(t->GetOpArgs(shard), key, serialized_value, restore_args.value(), rdb_version);
  };

  OpResult<bool> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    if (result.value()) {
      return (*cntx)->SendOk();
    } else {
      return (*cntx)->SendError("Bad data format");
    }
  } else {
    switch (result.status()) {
      case OpStatus::KEY_EXISTS:
        return (*cntx)->SendError("BUSYKEY: key name already exists.");
      case OpStatus::WRONG_TYPE:
        return (*cntx)->SendError("Bad data format");
      default:
        return (*cntx)->SendError(result.status());
    }
  }
}

void GenericFamily::Move(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view target_db_sv = ArgS(args, 1);
  int64_t target_db;

  if (!absl::SimpleAtoi(target_db_sv, &target_db)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  if (target_db < 0 || target_db >= absl::GetFlag(FLAGS_dbnum)) {
    return (*cntx)->SendError(kDbIndOutOfRangeErr);
  }

  if (target_db == cntx->db_index()) {
    return (*cntx)->SendError("source and destination objects are the same");
  }

  OpStatus res = OpStatus::SKIPPED;
  ShardId target_shard = Shard(key, shard_set->size());
  auto cb = [&](Transaction* t, EngineShard* shard) {
    // MOVE runs as a global transaction and is therefore scheduled on every shard.
    if (target_shard == shard->shard_id()) {
      auto op_args = t->GetOpArgs(shard);
      res = OpMove(op_args, key, target_db);
      // MOVE runs as global command but we want to write the
      // command to only one journal.
      if (op_args.shard->journal()) {
        RecordJournal(op_args, "MOVE"sv, ArgSlice{key, target_db_sv});
      }
    }
    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(std::move(cb));
  // Exactly one shard will call OpMove.
  DCHECK(res != OpStatus::SKIPPED);
  (*cntx)->SendLong(res == OpStatus::OK);
}

void GenericFamily::Rename(CmdArgList args, ConnectionContext* cntx) {
  OpResult<void> st = RenameGeneric(args, false, cntx);
  (*cntx)->SendError(st.status());
}

void GenericFamily::RenameNx(CmdArgList args, ConnectionContext* cntx) {
  OpResult<void> st = RenameGeneric(args, true, cntx);
  OpStatus status = st.status();
  if (status == OpStatus::OK) {
    (*cntx)->SendLong(1);
  } else if (status == OpStatus::KEY_EXISTS) {
    (*cntx)->SendLong(0);
  } else {
    (*cntx)->SendError(status);
  }
}

void GenericFamily::Ttl(CmdArgList args, ConnectionContext* cntx) {
  TtlGeneric(args, cntx, TimeUnit::SEC);
}

void GenericFamily::Pttl(CmdArgList args, ConnectionContext* cntx) {
  TtlGeneric(args, cntx, TimeUnit::MSEC);
}

void GenericFamily::TtlGeneric(CmdArgList args, ConnectionContext* cntx, TimeUnit unit) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpTtl(t, shard, key); };
  OpResult<uint64_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    long ttl = (unit == TimeUnit::SEC) ? (result.value() + 500) / 1000 : result.value();
    (*cntx)->SendLong(ttl);
    return;
  }

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      (*cntx)->SendLong(-2);
      break;
    default:
      LOG_IF(ERROR, result.status() != OpStatus::SKIPPED)
          << "Unexpected status " << result.status();
      (*cntx)->SendLong(-1);
      break;
  }
}

void GenericFamily::Select(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  int64_t index;
  if (!absl::SimpleAtoi(key, &index)) {
    return (*cntx)->SendError(kInvalidDbIndErr);
  }
  if (ClusterConfig::IsEnabled() && index != 0) {
    return (*cntx)->SendError("SELECT is not allowed in cluster mode");
  }
  if (index < 0 || index >= absl::GetFlag(FLAGS_dbnum)) {
    return (*cntx)->SendError(kDbIndOutOfRangeErr);
  }
  cntx->conn_state.db_index = index;
  auto cb = [index](EngineShard* shard) {
    shard->db_slice().ActivateDb(index);
    return OpStatus::OK;
  };
  shard_set->RunBriefInParallel(std::move(cb));

  return (*cntx)->SendOk();
}

void GenericFamily::Dump(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  DVLOG(1) << "Dumping before ::ScheduleSingleHopT " << key;
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpDump(t->GetOpArgs(shard), key); };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));
  if (result) {
    DVLOG(1) << "Dump " << trans->DebugId() << ": " << key << ", dump size "
             << result.value().size();
    (*cntx)->SendBulkString(*result);
  } else {
    DVLOG(1) << "Dump failed: " << result.DebugFormat() << key << " nil";
    (*cntx)->SendNull();
  }
}

void GenericFamily::Type(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<int> {
    auto it = shard->db_slice().FindExt(t->GetDbContext(), key).first;
    if (!it.is_done()) {
      return it->second.ObjType();
    } else {
      return OpStatus::KEY_NOTFOUND;
    }
  };
  OpResult<int> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (!result) {
    (*cntx)->SendSimpleString("none");
  } else {
    (*cntx)->SendSimpleString(ObjTypeName(result.value()));
  }
}

void GenericFamily::Time(CmdArgList args, ConnectionContext* cntx) {
  uint64_t now_usec;
  if (cntx->transaction) {
    now_usec = cntx->transaction->GetDbContext().time_now_ms * 1000;
  } else {
    now_usec = absl::GetCurrentTimeNanos() / 1000;
  }

  (*cntx)->StartArray(2);
  (*cntx)->SendLong(now_usec / 1000000);
  (*cntx)->SendLong(now_usec % 1000000);
}

OpResult<void> GenericFamily::RenameGeneric(CmdArgList args, bool skip_exist_dest,
                                            ConnectionContext* cntx) {
  string_view key[2] = {ArgS(args, 0), ArgS(args, 1)};

  Transaction* transaction = cntx->transaction;

  if (transaction->GetUniqueShardCnt() == 1) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto ec = OpRen(t->GetOpArgs(shard), key[0], key[1], skip_exist_dest);
      // Incase of uniqe shard count we can use rename command in replica.
      t->RenableAutoJournal();
      return ec;
    };
    OpResult<void> result = transaction->ScheduleSingleHopT(std::move(cb));

    return result;
  }

  transaction->Schedule();
  unsigned shard_count = shard_set->size();
  Renamer renamer{Shard(key[0], shard_count)};

  // Phase 1 -> Fetch  keys from both shards.
  // Phase 2 -> If everything is ok, clone the source object, delete the destination object, and
  //            set its ptr to cloned one. we also copy the expiration data of the source key.
  renamer.Find(transaction);
  renamer.Finalize(transaction, skip_exist_dest);

  return renamer.status();
}

void GenericFamily::Echo(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  return (*cntx)->SendBulkString(key);
}

void GenericFamily::Scan(CmdArgList args, ConnectionContext* cntx) {
  string_view token = ArgS(args, 0);
  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(1));
  if (!ops) {
    DVLOG(1) << "Scan invalid args - return " << ops << " to the user";
    return (*cntx)->SendError(ops.status());
  }

  ScanOpts scan_op = ops.value();

  StringVec keys;
  cursor = ScanGeneric(cursor, scan_op, &keys, cntx);

  (*cntx)->StartArray(2);
  (*cntx)->SendBulkString(absl::StrCat(cursor));
  (*cntx)->StartArray(keys.size());
  for (const auto& k : keys) {
    (*cntx)->SendBulkString(k);
  }
}

OpResult<uint64_t> GenericFamily::OpTtl(Transaction* t, EngineShard* shard, string_view key) {
  auto& db_slice = shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(t->GetDbContext(), key);
  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (!IsValid(expire_it))
    return OpStatus::SKIPPED;

  int64_t ttl_ms = db_slice.ExpireTime(expire_it) - t->GetDbContext().time_now_ms;
  DCHECK_GT(ttl_ms, 0);  // Otherwise FindExt would return null.
  return ttl_ms;
}

OpResult<uint32_t> GenericFamily::OpDel(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Del: " << keys[0];
  auto& db_slice = op_args.shard->db_slice();

  uint32_t res = 0;

  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto fres = db_slice.FindExt(op_args.db_cntx, keys[i]);
    if (!IsValid(fres.first))
      continue;
    res += int(db_slice.Del(op_args.db_cntx.db_index, fres.first));
  }

  return res;
}

OpResult<uint32_t> GenericFamily::OpExists(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Exists: " << keys[0];
  auto& db_slice = op_args.shard->db_slice();
  uint32_t res = 0;

  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto find_res = db_slice.FindExt(op_args.db_cntx, keys[i]);
    res += IsValid(find_res.first);
  }
  return res;
}

OpResult<void> GenericFamily::OpRen(const OpArgs& op_args, string_view from_key, string_view to_key,
                                    bool skip_exists) {
  auto* es = op_args.shard;
  auto& db_slice = es->db_slice();
  auto [from_it, from_expire] = db_slice.FindExt(op_args.db_cntx, from_key);
  if (!IsValid(from_it))
    return OpStatus::KEY_NOTFOUND;

  if (from_key == to_key)
    return OpStatus::OK;

  bool is_prior_list = false;
  auto [to_it, to_expire] = db_slice.FindExt(op_args.db_cntx, to_key);
  if (IsValid(to_it)) {
    if (skip_exists)
      return OpStatus::KEY_EXISTS;

    is_prior_list = (to_it->second.ObjType() == OBJ_LIST);
  }

  bool sticky = from_it->first.IsSticky();
  uint64_t exp_ts = db_slice.ExpireTime(from_expire);

  // we keep the value we want to move.
  PrimeValue from_obj = std::move(from_it->second);

  // Restore the expire flag on 'from' so we could delete it from expire table.
  from_it->second.SetExpire(IsValid(from_expire));

  if (IsValid(to_it)) {
    to_it->second = std::move(from_obj);
    to_it->second.SetExpire(IsValid(to_expire));  // keep the expire flag on 'to'.

    // It is guaranteed that UpdateExpire() call does not erase the element because then
    // from_it would be invalid. Therefore, UpdateExpire does not invalidate any iterators,
    // therefore we can delete 'from_it'.
    db_slice.UpdateExpire(op_args.db_cntx.db_index, to_it, exp_ts);
    CHECK(db_slice.Del(op_args.db_cntx.db_index, from_it));
  } else {
    // Here we first delete from_it because AddNew below could invalidate from_it.
    // On the other hand, AddNew does not rely on the iterators - this is why we keep
    // the value in `from_obj`.
    CHECK(db_slice.Del(op_args.db_cntx.db_index, from_it));
    to_it = db_slice.AddNew(op_args.db_cntx, to_key, std::move(from_obj), exp_ts);
  }

  to_it->first.SetSticky(sticky);

  if (!is_prior_list && to_it->second.ObjType() == OBJ_LIST && es->blocking_controller()) {
    es->blocking_controller()->AwakeWatched(op_args.db_cntx.db_index, to_key);
  }
  return OpStatus::OK;
}

OpResult<uint32_t> GenericFamily::OpStick(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Stick: " << keys[0];

  auto& db_slice = op_args.shard->db_slice();

  uint32_t res = 0;
  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto [it, _] = db_slice.FindExt(op_args.db_cntx, keys[i]);
    if (IsValid(it) && !it->first.IsSticky()) {
      it->first.SetSticky(true);
      ++res;
    }
  }

  return res;
}

// OpMove touches multiple databases (op_args.db_idx, target_db), so it assumes it runs
// as a global transaction.
// TODO: Allow running OpMove without a global transaction.
OpStatus GenericFamily::OpMove(const OpArgs& op_args, string_view key, DbIndex target_db) {
  auto& db_slice = op_args.shard->db_slice();

  // Fetch value at key in current db.
  auto [from_it, from_expire] = db_slice.FindExt(op_args.db_cntx, key);
  if (!IsValid(from_it))
    return OpStatus::KEY_NOTFOUND;

  // Fetch value at key in target db.
  DbContext target_cntx = op_args.db_cntx;
  target_cntx.db_index = target_db;
  auto [to_it, _] = db_slice.FindExt(target_cntx, key);
  if (IsValid(to_it))
    return OpStatus::KEY_EXISTS;

  // Ensure target database exists.
  db_slice.ActivateDb(target_db);

  bool sticky = from_it->first.IsSticky();
  uint64_t exp_ts = db_slice.ExpireTime(from_expire);
  PrimeValue from_obj = std::move(from_it->second);

  // Restore expire flag after std::move.
  from_it->second.SetExpire(IsValid(from_expire));

  CHECK(db_slice.Del(op_args.db_cntx.db_index, from_it));
  to_it = db_slice.AddNew(target_cntx, key, std::move(from_obj), exp_ts);
  to_it->first.SetSticky(sticky);

  if (to_it->second.ObjType() == OBJ_LIST && op_args.shard->blocking_controller()) {
    op_args.shard->blocking_controller()->AwakeWatched(target_db, key);
  }

  return OpStatus::OK;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&GenericFamily::x)

void GenericFamily::Register(CommandRegistry* registry) {
  constexpr auto kSelectOpts = CO::LOADING | CO::FAST | CO::NOSCRIPT;

  *registry << CI{"DEL", CO::WRITE, -2, 1, -1, 1}.HFUNC(Del)
            /* Redis compatibility:
             * We don't allow PING during loading since in Redis PING is used as
             * failure detection, and a loading server is considered to be
             * not available. */
            << CI{"PING", CO::FAST, -1, 0, 0, 0}.HFUNC(Ping)
            << CI{"ECHO", CO::LOADING | CO::FAST, 2, 0, 0, 0}.HFUNC(Echo)
            << CI{"EXISTS", CO::READONLY | CO::FAST, -2, 1, -1, 1}.HFUNC(Exists)
            << CI{"TOUCH", CO::READONLY | CO::FAST, -2, 1, -1, 1}.HFUNC(Exists)
            << CI{"EXPIRE", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, 3, 1, 1, 1}.HFUNC(Expire)
            << CI{"EXPIREAT", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, 3, 1, 1, 1}.HFUNC(ExpireAt)
            << CI{"PERSIST", CO::WRITE | CO::FAST, 2, 1, 1, 1}.HFUNC(Persist)
            << CI{"KEYS", CO::READONLY, 2, 0, 0, 0}.HFUNC(Keys)
            << CI{"PEXPIREAT", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, 3, 1, 1, 1}.HFUNC(
                   PexpireAt)
            << CI{"PEXPIRE", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, 3, 1, 1, 1}.HFUNC(Pexpire)
            << CI{"RENAME", CO::WRITE | CO::NO_AUTOJOURNAL, 3, 1, 2, 1}.HFUNC(Rename)
            << CI{"RENAMENX", CO::WRITE | CO::NO_AUTOJOURNAL, 3, 1, 2, 1}.HFUNC(RenameNx)
            << CI{"SELECT", kSelectOpts, 2, 0, 0, 0}.HFUNC(Select)
            << CI{"SCAN", CO::READONLY | CO::FAST | CO::LOADING, -2, 0, 0, 0}.HFUNC(Scan)
            << CI{"TTL", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Ttl)
            << CI{"PTTL", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Pttl)
            << CI{"TIME", CO::LOADING | CO::FAST, 1, 0, 0, 0}.HFUNC(Time)
            << CI{"TYPE", CO::READONLY | CO::FAST | CO::LOADING, 2, 1, 1, 1}.HFUNC(Type)
            << CI{"DUMP", CO::READONLY, 2, 1, 1, 1}.HFUNC(Dump)
            << CI{"UNLINK", CO::WRITE, -2, 1, -1, 1}.HFUNC(Del)
            << CI{"STICK", CO::WRITE, -2, 1, -1, 1}.HFUNC(Stick)
            << CI{"SORT", CO::READONLY, -2, 1, 1, 1}.HFUNC(Sort)
            << CI{"MOVE", CO::WRITE | CO::GLOBAL_TRANS | CO::NO_AUTOJOURNAL, 3, 1, 1, 1}.HFUNC(Move)
            << CI{"RESTORE", CO::WRITE, -4, 1, 1, 1}.HFUNC(Restore);
}

}  // namespace dfly
