// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

#include <boost/operators.hpp>
#include <optional>

#include "facade/cmd_arg_parser.h"
#include "facade/reply_builder.h"

extern "C" {
#include "redis/crc64.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "redis/rdb.h"
#include "server/acl/acl_commands_def.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/hset_family.h"
#include "server/journal/journal.h"
#include "server/rdb_extensions.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/search/doc_index.h"
#include "server/set_family.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "util/fibers/future.h"
#include "util/varz.h"

ABSL_FLAG(uint32_t, dbnum, 16, "Number of databases");
ABSL_FLAG(uint32_t, keys_output_limit, 8192, "Maximum number of keys output by keys command");
ABSL_FLAG(bool, unlink_experimental_async, true, "If true, runs unlink command asynchronously.");

namespace dfly {
using namespace std;
using namespace facade;

namespace {

constexpr uint32_t kMaxTtl = (1UL << 26);
constexpr size_t DUMP_FOOTER_SIZE = sizeof(uint64_t) + sizeof(uint16_t);  // version number and crc

std::optional<RdbVersion> GetRdbVersion(std::string_view msg) {
  if (msg.size() <= DUMP_FOOTER_SIZE) {
    LOG(WARNING) << "got restore payload that is too short - " << msg.size();
    return std::nullopt;
  }

  // The footer looks like this: version (2 bytes) | crc64 (8 bytes)
  const std::uint8_t* footer =
      reinterpret_cast<const std::uint8_t*>(msg.data()) + (msg.size() - DUMP_FOOTER_SIZE);
  const RdbVersion version = (*(footer + 1) << 8 | (*footer));

  if (version > RDB_VERSION) {
    LOG(WARNING) << "got restore payload with illegal version - supporting version up to "
                 << RDB_VERSION << " got version " << version;
    return std::nullopt;
  }

  // Compute expected crc64 based on the actual data upto the expected crc64 field.
  uint64_t actual_cs =
      crc64(0, reinterpret_cast<const uint8_t*>(msg.data()), msg.size() - sizeof(uint64_t));
  uint64_t expected_cs = absl::little_endian::Load64(footer + 2);  // skip the version

  if (actual_cs != expected_cs) {
    LOG(WARNING) << "CRC check failed for restore command, expecting: " << expected_cs << " got "
                 << actual_cs;
    return std::nullopt;
  }

  return version;
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
    size_t read_sz = min<size_t>(buf_.size() - offs_, v->iov_len);
    memcpy(v->iov_base, buf_.data() + offs_, read_sz);
    read_total += read_sz;
    offs_ += read_sz;

    ++v;
    --len;
  }

  return read_total;
}

class RestoreArgs {
 private:
  static constexpr int64_t NO_EXPIRATION = 0;

  int64_t expiration_ = NO_EXPIRATION;
  bool abs_time_ = false;
  bool replace_ = false;  // if true, over-ride existing key
  bool sticky_ = false;

 public:
  RestoreArgs() = default;

  RestoreArgs(int64_t expiration, bool abs_time, bool replace)
      : expiration_(expiration), abs_time_(abs_time), replace_(replace) {
  }

  bool Replace() const {
    return replace_;
  }

  bool Sticky() const {
    return sticky_;
  }

  void SetSticky(bool sticky) {
    sticky_ = sticky;
  }

  uint64_t ExpirationTime() const {
    DCHECK_GE(expiration_, 0);
    return expiration_;
  }

  bool Expired() const {
    return expiration_ < 0;
  }

  bool HasExpiration() const {
    return expiration_ != NO_EXPIRATION;
  }

  [[nodiscard]] bool UpdateExpiration(int64_t now_msec);

  static OpResult<RestoreArgs> TryFrom(const CmdArgList& args);
};

class RdbRestoreValue : protected RdbLoaderBase {
 public:
  RdbRestoreValue(RdbVersion rdb_version) {
    rdb_version_ = rdb_version;
  }

  OpResult<DbSlice::ItAndUpdater> Add(string_view key, string_view payload, const DbContext& cntx,
                                      const RestoreArgs& args, DbSlice* db_slice);

 private:
  std::optional<OpaqueObj> Parse(io::Source* source);
  int rdb_type_ = -1;
};

std::optional<RdbLoaderBase::OpaqueObj> RdbRestoreValue::Parse(io::Source* source) {
  src_ = source;
  if (pending_read_.remaining == 0) {
    io::Result<uint8_t> type_id = FetchType();
    if (type_id && rdbIsObjectTypeDF(type_id.value())) {
      rdb_type_ = *type_id;
    }
  }

  if (rdb_type_ == -1) {
    LOG(ERROR) << "failed to load type id from the input stream or type id is invalid";
    return std::nullopt;
  }

  OpaqueObj obj;
  error_code ec = ReadObj(rdb_type_, &obj);  // load the type from the input stream
  if (ec) {
    LOG(ERROR) << "failed to load data for type id " << rdb_type_;
    return std::nullopt;
  }

  return std::optional<OpaqueObj>(std::move(obj));
}

OpResult<DbSlice::ItAndUpdater> RdbRestoreValue::Add(string_view key, string_view data,
                                                     const DbContext& cntx, const RestoreArgs& args,
                                                     DbSlice* db_slice) {
  InMemSource data_src(data);
  PrimeValue pv;
  bool first_parse = true;
  do {
    auto opaque_res = Parse(&data_src);
    if (!opaque_res) {
      return OpStatus::INVALID_VALUE;
    }

    LoadConfig config;
    if (first_parse) {
      first_parse = false;
    } else {
      config.append = true;
    }
    if (pending_read_.remaining > 0) {
      config.streamed = true;
    }
    config.reserve = pending_read_.reserve;

    if (auto ec = FromOpaque(*opaque_res, config, &pv); ec) {
      // we failed - report and exit
      LOG(WARNING) << "error while trying to read data: " << ec;
      return OpStatus::INVALID_VALUE;
    }
  } while (pending_read_.remaining > 0);

  auto res = db_slice->AddOrUpdate(cntx, key, std::move(pv), args.ExpirationTime());
  if (res) {
    res->it->first.SetSticky(args.Sticky());
    db_slice->shard_owner()->search_indices()->AddDoc(key, cntx, res->it->second);
  }
  return res;
}

[[nodiscard]] bool RestoreArgs::UpdateExpiration(int64_t now_msec) {
  if (HasExpiration()) {
    int64_t ttl = abs_time_ ? expiration_ - now_msec : expiration_;
    if (ttl > kMaxExpireDeadlineMs)
      ttl = kMaxExpireDeadlineMs;

    expiration_ = ttl < 0 ? -1 : ttl + now_msec;
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
  string cur_arg{ArgS(args, 1)};  // extract ttl
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
    cur_arg = absl::AsciiStrToUpper(ArgS(args, i));
    bool additional = args.size() - i - 1 >= 1;
    if (cur_arg == "REPLACE") {
      out_args.replace_ = true;
    } else if (cur_arg == "ABSTTL") {
      out_args.abs_time_ = true;
    } else if (cur_arg == "STICK") {
      out_args.sticky_ = true;
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
  Renamer(Transaction* t, std::string_view src_key, std::string_view dest_key, unsigned shard_count,
          bool do_copy = false)
      : transaction_(t),
        src_key_(src_key),
        dest_key_(dest_key),
        src_sid_(Shard(src_key, shard_count)),
        dest_sid_(Shard(dest_key, shard_count)),
        do_copy_(do_copy) {
  }

  ErrorReply Rename(bool destination_should_not_exist);

 private:
  void FetchData();
  void FinalizeRename();

  bool KeyExists(Transaction* t, EngineShard* shard, std::string_view key) const;
  void SerializeSrc(Transaction* t, EngineShard* shard);

  OpStatus DelSrc(Transaction* t, EngineShard* shard);
  OpStatus DeserializeDest(Transaction* t, EngineShard* shard);

  struct SerializedValue {
    std::string value;
    std::optional<RdbVersion> version;
    int64_t expire_ts;
    bool sticky;
  };

 private:
  Transaction* const transaction_;

  const std::string_view src_key_;
  const std::string_view dest_key_;
  const ShardId src_sid_;
  const ShardId dest_sid_;

  bool src_found_ = false;
  bool dest_found_ = false;
  bool do_copy_ = false;

  SerializedValue serialized_value_;
};

ErrorReply Renamer::Rename(bool destination_should_not_exist) {
  FetchData();

  if (!src_found_) {
    transaction_->Conclude();
    return OpStatus::KEY_NOTFOUND;
  }

  if (!serialized_value_.version) {
    transaction_->Conclude();
    return ErrorReply{kInvalidDumpValueErr};
  }

  if (dest_found_ && destination_should_not_exist) {
    transaction_->Conclude();
    return OpStatus::KEY_EXISTS;
  }

  FinalizeRename();
  return OpStatus::OK;
}

void Renamer::FetchData() {
  auto cb = [this](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    DCHECK(1 == args.Size() || do_copy_);

    const ShardId shard_id = shard->shard_id();

    if (shard_id == src_sid_) {
      SerializeSrc(t, shard);
    }

    if (shard_id == dest_sid_) {
      dest_found_ = KeyExists(t, shard, dest_key_);
    }

    return OpStatus::OK;
  };

  transaction_->Execute(std::move(cb), false);
}

void Renamer::FinalizeRename() {
  auto cb = [this](Transaction* t, EngineShard* shard) {
    const ShardId shard_id = shard->shard_id();

    if (!do_copy_ && shard_id == src_sid_) {
      return DelSrc(t, shard);
    }

    if (shard_id == dest_sid_) {
      return DeserializeDest(t, shard);
    }

    return OpStatus::OK;
  };

  transaction_->Execute(std::move(cb), true);
}

bool Renamer::KeyExists(Transaction* t, EngineShard* shard, std::string_view key) const {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto it = db_slice.FindReadOnly(t->GetDbContext(), key).it;
  return IsValid(it);
}

void Renamer::SerializeSrc(Transaction* t, EngineShard* shard) {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto [it, exp_it] = db_slice.FindReadOnly(t->GetDbContext(), src_key_);

  src_found_ = IsValid(it);
  if (!src_found_) {
    return;
  }

  DVLOG(1) << "Rename: key '" << src_key_ << "' successfully found, going to dump it";

  io::StringSink sink;
  SerializerBase::DumpObject(it->second, &sink);

  auto rdb_version = GetRdbVersion(sink.str());
  serialized_value_ = {std::move(sink).str(), rdb_version, db_slice.ExpireTime(exp_it),
                       it->first.IsSticky()};
}

OpStatus Renamer::DelSrc(Transaction* t, EngineShard* shard) {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto res = db_slice.FindMutable(t->GetDbContext(), src_key_);
  auto& it = res.it;

  CHECK(IsValid(it));

  DVLOG(1) << "Rename: removing the key '" << src_key_;

  res.post_updater.Run();
  db_slice.Del(t->GetDbContext(), it);
  if (shard->journal()) {
    RecordJournal(t->GetOpArgs(shard), "DEL"sv, ArgSlice{src_key_}, 2);
  }

  return OpStatus::OK;
}

OpStatus Renamer::DeserializeDest(Transaction* t, EngineShard* shard) {
  OpArgs op_args = t->GetOpArgs(shard);
  RestoreArgs restore_args{serialized_value_.expire_ts, true, true};

  if (!restore_args.UpdateExpiration(op_args.db_cntx.time_now_ms)) {
    return OpStatus::OUT_OF_RANGE;
  }

  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto dest_res = db_slice.FindMutable(op_args.db_cntx, dest_key_);

  if (dest_found_) {
    DVLOG(1) << "Rename: deleting the destiny key '" << dest_key_;
    dest_res.post_updater.Run();
    db_slice.Del(op_args.db_cntx, dest_res.it);
  }

  if (restore_args.Expired()) {
    VLOG(1) << "Rename: the new key '" << dest_key_ << "' already expired, will not save the value";

    if (dest_found_ && shard->journal()) {  // We need to delete old dest_key_ from replica
      RecordJournal(op_args, "DEL"sv, ArgSlice{dest_key_}, 2);
    }

    return OpStatus::OK;
  }

  restore_args.SetSticky(serialized_value_.sticky);

  RdbRestoreValue loader(serialized_value_.version.value());
  auto add_res =
      loader.Add(dest_key_, serialized_value_.value, op_args.db_cntx, restore_args, &db_slice);

  if (!add_res)
    return add_res.status();

  LOG_IF(DFATAL, !add_res->is_new)
      << "Unexpected override for key " << dest_key_ << " " << dest_found_;
  auto bc = op_args.db_cntx.ns->GetBlockingController(op_args.shard->shard_id());
  if (bc) {
    bc->AwakeWatched(t->GetDbIndex(), dest_key_);
  }

  if (shard->journal()) {
    auto expire_str = absl::StrCat(serialized_value_.expire_ts);

    absl::InlinedVector<std::string_view, 6> args(
        {dest_key_, expire_str, serialized_value_.value, "REPLACE"sv, "ABSTTL"sv});
    if (serialized_value_.sticky) {
      args.push_back("STICK"sv);
    }

    RecordJournal(op_args, "RESTORE"sv, args, 2);
  }

  return OpStatus::OK;
}

OpStatus OpPersist(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto res = db_slice.FindMutable(op_args.db_cntx, key);

  if (!IsValid(res.it)) {
    return OpStatus::KEY_NOTFOUND;
  } else {
    if (IsValid(res.exp_it)) {
      // The SKIPPED not really used, just placeholder for error
      return db_slice.UpdateExpire(op_args.db_cntx.db_index, res.it, 0) ? OpStatus::OK
                                                                        : OpStatus::SKIPPED;
    }
    return OpStatus::SKIPPED;  // fall though - key does not have expiry
  }
}

OpResult<std::string> OpDump(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto [it, expire_it] = db_slice.FindReadOnly(op_args.db_cntx, key);

  if (IsValid(it)) {
    DVLOG(1) << "Dump: key '" << key << "' successfully found, going to dump it";
    io::StringSink sink;
    const PrimeValue& pv = it->second;

    if (pv.IsExternal() && !pv.IsCool()) {
      util::fb2::Future<string> future =
          op_args.shard->tiered_storage()->Read(op_args.db_cntx.db_index, key, pv);

      CompactObj co(future.Get());
      SerializerBase::DumpObject(co, &sink);
    } else {
      SerializerBase::DumpObject(it->second, &sink);
    }

    return std::move(sink).str();
  }

  // fallback
  DVLOG(1) << "Dump: '" << key << "' Not found";
  return OpStatus::KEY_NOTFOUND;
}

OpStatus OpRestore(const OpArgs& op_args, std::string_view key, std::string_view payload,
                   RestoreArgs restore_args, RdbVersion rdb_version) {
  if (!restore_args.UpdateExpiration(op_args.db_cntx.time_now_ms)) {
    return OpStatus::OUT_OF_RANGE;
  }

  auto& db_slice = op_args.GetDbSlice();
  bool found_prev = false;

  // The redis impl (see cluster.c function restoreCommand), remove the old key if
  // the replace option is set, so lets do the same here
  {
    auto res = db_slice.FindMutable(op_args.db_cntx, key);
    if (IsValid(res.it)) {
      found_prev = true;
      if (restore_args.Replace()) {
        VLOG(1) << "restore command is running with replace, found old key '" << key
                << "' and removing it";
        res.post_updater.Run();
        db_slice.Del(op_args.db_cntx, res.it);
      } else {
        // we are not allowed to replace it.
        return OpStatus::KEY_EXISTS;
      }
    }
  }

  if (restore_args.Expired()) {
    VLOG(1) << "the new key '" << key << "' already expired, will not save the value";
    return OpStatus::OK;
  }

  RdbRestoreValue loader(rdb_version);
  auto add_res = loader.Add(key, payload, op_args.db_cntx, restore_args, &db_slice);
  LOG_IF(DFATAL, add_res && !add_res->is_new)
      << "Unexpected override for key " << key << ", found previous " << found_prev
      << " override: " << restore_args.Replace()
      << ", type: " << ObjTypeToString(add_res->it->second.ObjType());

  return add_res.status();
}

bool ScanCb(const OpArgs& op_args, PrimeIterator prime_it, const ScanOpts& opts, StringVec* res) {
  auto& db_slice = op_args.GetDbSlice();

  DbSlice::Iterator it = DbSlice::Iterator::FromPrime(prime_it);
  if (prime_it->second.HasExpire()) {
    it = db_slice.ExpireIfNeeded(op_args.db_cntx, it).it;
    if (!IsValid(it))
      return false;
  }

  bool matches = !opts.type_filter || it->second.ObjType() == opts.type_filter;
  if (opts.mask.has_value()) {
    if (opts.mask == ScanOpts::Mask::Volatile) {
      matches &= it->second.HasExpire();
    } else if (opts.mask == ScanOpts::Mask::Permanent) {
      matches &= !it->second.HasExpire();
    } else if (opts.mask == ScanOpts::Mask::Accessed) {
      matches &= it->first.WasTouched();
    } else if (opts.mask == ScanOpts::Mask::Untouched) {
      matches &= !it->first.WasTouched();
    }
  }
  if (!matches)
    return false;

  if (opts.min_malloc_size > 0 && it->second.MallocUsed() < opts.min_malloc_size) {
    return false;
  }

  if (opts.bucket_id != UINT_MAX && opts.bucket_id != it.GetInnerIt().bucket_id()) {
    return false;
  }

  if (!opts.Matches(it.key())) {
    return false;
  }
  res->emplace_back(it.key());

  return true;
}

void OpScan(const OpArgs& op_args, const ScanOpts& scan_opts, uint64_t* cursor, StringVec* vec) {
  auto& db_slice = op_args.GetDbSlice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));

  // ScanCb can preempt due to journaling expired entries and we need to make sure that
  // we enter the callback in a timing when journaling will not cause preemptions. Otherwise,
  // the bucket might change as we Traverse and yield.
  db_slice.GetLatch()->Wait();

  // Disable flush journal changes to prevent preemtion in traverse.
  journal::JournalFlushGuard journal_flush_guard(op_args.shard->journal());
  unsigned cnt = 0;

  VLOG(1) << "PrimeTable " << db_slice.shard_id() << "/" << op_args.db_cntx.db_index << " has "
          << db_slice.DbSize(op_args.db_cntx.db_index);

  PrimeTable::Cursor cur = *cursor;
  auto [prime_table, expire_table] = db_slice.GetTables(op_args.db_cntx.db_index);

  const auto start = absl::Now();
  // Don't allow it to monopolize cpu time.
  const absl::Duration timeout = absl::Milliseconds(10);

  do {
    cur = prime_table->Traverse(
        cur, [&](PrimeIterator it) { cnt += ScanCb(op_args, it, scan_opts, vec); });
  } while (cur && cnt < scan_opts.limit && (absl::Now() - start) < timeout);

  VLOG(1) << "OpScan " << db_slice.shard_id() << " cursor: " << cur.value();
  *cursor = cur.value();
}

uint64_t ScanGeneric(uint64_t cursor, const ScanOpts& scan_opts, StringVec* keys,
                     ConnectionContext* cntx) {
  ShardId sid = cursor % 1024;

  EngineShardSet* ess = shard_set;
  unsigned shard_count = ess->size();
  constexpr uint64_t kMaxScanTimeMs = 100;

  // Dash table returns a cursor with its right byte empty. We will use it
  // for encoding shard index. For now scan has a limitation of 255 shards.
  CHECK_LT(shard_count, 1024u);

  if (sid >= shard_count) {  // protection
    return 0;
  }

  cursor >>= 10;
  DbContext db_cntx{cntx->ns, cntx->conn_state.db_index, GetCurrentTimeMs()};

  do {
    auto cb = [&] {
      OpArgs op_args{EngineShard::tlocal(), nullptr, db_cntx};
      OpScan(op_args, scan_opts, &cursor, keys);
    };

    // Avoid deadlocking, if called from shard queue script
    if (EngineShard::tlocal() && EngineShard::tlocal()->shard_id() == sid)
      cb();
    else
      ess->Await(sid, cb);

    if (cursor == 0) {
      ++sid;
      if (unsigned(sid) == shard_count)
        break;
    }

    // Break after kMaxScanTimeMs.
    uint64_t time_now_ms = GetCurrentTimeMs();
    if (time_now_ms > db_cntx.time_now_ms + kMaxScanTimeMs) {
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
  auto& db_slice = op_args.GetDbSlice();
  auto find_res = db_slice.FindMutable(op_args.db_cntx, key);
  if (!IsValid(find_res.it)) {
    return OpStatus::KEY_NOTFOUND;
  }

  find_res.post_updater.Run();
  auto res = db_slice.UpdateExpire(op_args.db_cntx, find_res.it, find_res.exp_it, params);

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

OpResult<vector<long>> OpFieldExpire(const OpArgs& op_args, string_view key, uint32_t ttl_sec,
                                     CmdArgList values) {
  auto& db_slice = op_args.GetDbSlice();
  auto [it, expire_it, auto_updater, is_new] = db_slice.FindMutable(op_args.db_cntx, key);

  if (!IsValid(it) || (it->second.ObjType() != OBJ_SET && it->second.ObjType() != OBJ_HASH)) {
    std::vector<long> res(values.size(), -2);
    return res;
  }

  PrimeValue* pv = &it->second;
  if (pv->ObjType() == OBJ_SET) {
    return SetFamily::SetFieldsExpireTime(op_args, ttl_sec, values, pv);
  } else {
    return HSetFamily::SetFieldsExpireTime(op_args, ttl_sec, key, values, pv);
  }
}

// returns -2 if the key was not found, -3 if the field was not found,
// -1 if ttl on the field was not found.
OpResult<long> OpFieldTtl(Transaction* t, EngineShard* shard, string_view key, string_view field) {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  const DbContext& db_cntx = t->GetDbContext();
  auto [it, expire_it] = db_slice.FindReadOnly(db_cntx, key);
  if (!IsValid(it))
    return -2;

  if (it->second.ObjType() != OBJ_SET && it->second.ObjType() != OBJ_HASH)
    return OpStatus::WRONG_TYPE;

  int32_t res = -1;
  if (it->second.ObjType() == OBJ_SET) {
    res = SetFamily::FieldExpireTime(db_cntx, it->second, field);
  } else {
    DCHECK_EQ(OBJ_HASH, it->second.ObjType());
    res = HSetFamily::FieldExpireTime(db_cntx, it->second, field);
  }
  return res <= 0 ? res : int32_t(res - MemberTimeSeconds(db_cntx.time_now_ms));
}

OpResult<uint32_t> OpStick(const OpArgs& op_args, const ShardArgs& keys) {
  DVLOG(1) << "Stick: " << keys.Front();

  auto& db_slice = op_args.GetDbSlice();

  uint32_t res = 0;
  for (string_view key : keys) {
    auto find_res = db_slice.FindMutable(op_args.db_cntx, key);
    if (IsValid(find_res.it) && !find_res.it->first.IsSticky()) {
      find_res.it->first.SetSticky(true);
      ++res;
    }
  }

  return res;
}

OpResult<uint64_t> OpExpireTime(Transaction* t, EngineShard* shard, string_view key) {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto [it, expire_it] = db_slice.FindReadOnly(t->GetDbContext(), key);
  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (!IsValid(expire_it))
    return OpStatus::SKIPPED;

  int64_t ttl_ms = db_slice.ExpireTime(expire_it);
  DCHECK_GT(ttl_ms, 0);  // Otherwise FindReadOnly would return null.
  return ttl_ms;
}

// OpMove touches multiple databases (op_args.db_idx, target_db), so it assumes it runs
// as a global transaction.
// TODO: Allow running OpMove without a global transaction.
OpStatus OpMove(const OpArgs& op_args, string_view key, DbIndex target_db) {
  auto& db_slice = op_args.GetDbSlice();

  // Fetch value at key in current db.
  auto from_res = db_slice.FindMutable(op_args.db_cntx, key);
  if (!IsValid(from_res.it))
    return OpStatus::KEY_NOTFOUND;

  // Ensure target database exists.
  db_slice.ActivateDb(target_db);

  // Fetch value at key in target db.
  DbContext target_cntx = op_args.db_cntx;
  target_cntx.db_index = target_db;
  auto to_res = db_slice.FindReadOnly(target_cntx, key);
  if (IsValid(to_res.it))
    return OpStatus::KEY_EXISTS;

  bool sticky = from_res.it->first.IsSticky();
  uint64_t exp_ts = db_slice.ExpireTime(from_res.exp_it);
  from_res.post_updater.Run();
  PrimeValue from_obj = std::move(from_res.it->second);

  // Restore expire flag after std::move.
  from_res.it->second.SetExpire(IsValid(from_res.exp_it));

  db_slice.Del(op_args.db_cntx, from_res.it);
  auto op_result = db_slice.AddNew(target_cntx, key, std::move(from_obj), exp_ts);
  RETURN_ON_BAD_STATUS(op_result);
  auto& add_res = *op_result;
  add_res.it->first.SetSticky(sticky);

  auto bc = op_args.db_cntx.ns->GetBlockingController(op_args.shard->shard_id());
  if (add_res.it->second.ObjType() == OBJ_LIST && bc) {
    bc->AwakeWatched(target_db, key);
  }

  return OpStatus::OK;
}

OpResult<void> OpRen(const OpArgs& op_args, string_view from_key, string_view to_key,
                     bool destination_should_not_exist) {
  auto* es = op_args.shard;
  auto& db_slice = op_args.GetDbSlice();
  auto from_res = db_slice.FindMutable(op_args.db_cntx, from_key);
  if (!IsValid(from_res.it))
    return OpStatus::KEY_NOTFOUND;

  if (from_key == to_key)
    return destination_should_not_exist ? OpStatus::KEY_EXISTS : OpStatus::OK;

  bool is_prior_list = false;
  auto to_res = db_slice.FindMutable(op_args.db_cntx, to_key);
  if (IsValid(to_res.it)) {
    if (destination_should_not_exist)
      return OpStatus::KEY_EXISTS;

    op_args.shard->search_indices()->RemoveDoc(to_key, op_args.db_cntx, to_res.it->second);
    is_prior_list = (to_res.it->second.ObjType() == OBJ_LIST);
  }

  // Delete the "from" document from the search index before deleting from the database
  op_args.shard->search_indices()->RemoveDoc(from_key, op_args.db_cntx, from_res.it->second);

  bool sticky = from_res.it->first.IsSticky();
  uint64_t exp_ts = db_slice.ExpireTime(from_res.exp_it);

  // we keep the value we want to move.
  PrimeValue from_obj = std::move(from_res.it->second);

  // Restore the expire flag on 'from' so we could delete it from expire table.
  from_res.it->second.SetExpire(IsValid(from_res.exp_it));

  if (IsValid(to_res.it)) {
    to_res.it->second = std::move(from_obj);
    to_res.it->second.SetExpire(IsValid(to_res.exp_it));  // keep the expire flag on 'to'.

    // It is guaranteed that UpdateExpire() call does not erase the element because then
    // from_it would be invalid. Therefore, UpdateExpire does not invalidate any iterators,
    // therefore we can delete 'from_it'.
    db_slice.UpdateExpire(op_args.db_cntx.db_index, to_res.it, exp_ts);
    to_res.it->first.SetSticky(sticky);
    to_res.post_updater.Run();

    from_res.post_updater.Run();
    db_slice.Del(op_args.db_cntx, from_res.it);
  } else {
    // Here we first delete from_it because AddNew below could invalidate from_it.
    // On the other hand, AddNew does not rely on the iterators - this is why we keep
    // the value in `from_obj`.
    from_res.post_updater.Run();
    db_slice.Del(op_args.db_cntx, from_res.it);
    auto op_result = db_slice.AddNew(op_args.db_cntx, to_key, std::move(from_obj), exp_ts);
    RETURN_ON_BAD_STATUS(op_result);
    to_res = std::move(*op_result);
    to_res.it->first.SetSticky(sticky);
  }

  op_args.shard->search_indices()->AddDoc(to_key, op_args.db_cntx, to_res.it->second);

  auto bc = op_args.db_cntx.ns->GetBlockingController(es->shard_id());
  if (!is_prior_list && to_res.it->second.ObjType() == OBJ_LIST && bc) {
    bc->AwakeWatched(op_args.db_cntx.db_index, to_key);
  }
  return OpStatus::OK;
}

OpResult<uint64_t> OpTtl(Transaction* t, EngineShard* shard, string_view key) {
  auto opExpireTimeResult = OpExpireTime(t, shard, key);

  if (opExpireTimeResult) {
    int64_t ttl_ms = opExpireTimeResult.value() - t->GetDbContext().time_now_ms;
    DCHECK_GT(ttl_ms, 0);  // Otherwise FindReadOnly would return null.
    return ttl_ms;
  } else {
    return opExpireTimeResult;
  }
}

ErrorReply RenameGeneric(CmdArgList args, bool destination_should_not_exist, Transaction* tx) {
  string_view key[2] = {ArgS(args, 0), ArgS(args, 1)};

  if (tx->GetUniqueShardCnt() == 1) {
    tx->ReviveAutoJournal();  // Safe to use RENAME with single shard
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return OpRen(t->GetOpArgs(shard), key[0], key[1], destination_should_not_exist);
    };
    OpResult<void> result = tx->ScheduleSingleHopT(std::move(cb));

    return result.status();
  }

  Renamer renamer{tx, key[0], key[1], shard_set->size()};
  return renamer.Rename(destination_should_not_exist);
}

void ExpireTimeGeneric(CmdArgList args, TimeUnit unit, Transaction* tx, SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpExpireTime(t, shard, key); };
  OpResult<uint64_t> result = tx->ScheduleSingleHopT(std::move(cb));

  if (result) {
    long ttl = (unit == TimeUnit::SEC) ? (result.value() + 500) / 1000 : result.value();
    builder->SendLong(ttl);
    return;
  }

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      builder->SendLong(-2);
      break;
    default:
      LOG_IF(ERROR, result.status() != OpStatus::SKIPPED)
          << "Unexpected status " << result.status();
      builder->SendLong(-1);
      break;
  }
}

void TtlGeneric(CmdArgList args, TimeUnit unit, Transaction* tx, SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpTtl(t, shard, key); };
  OpResult<uint64_t> result = tx->ScheduleSingleHopT(std::move(cb));

  if (result) {
    long ttl = (unit == TimeUnit::SEC) ? (result.value() + 500) / 1000 : result.value();
    builder->SendLong(ttl);
    return;
  }

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      builder->SendLong(-2);
      break;
    default:
      LOG_IF(ERROR, result.status() != OpStatus::SKIPPED)
          << "Unexpected status " << result.status();
      builder->SendLong(-1);
      break;
  }
}

std::optional<int32_t> ParseExpireOptionsOrReply(const CmdArgList args, SinkReplyBuilder* builder) {
  int32_t flags = ExpireFlags::EXPIRE_ALWAYS;
  for (auto& arg : args) {
    string arg_sv = absl::AsciiStrToUpper(ToSV(arg));
    if (arg_sv == "NX") {
      flags |= ExpireFlags::EXPIRE_NX;
    } else if (arg_sv == "XX") {
      flags |= ExpireFlags::EXPIRE_XX;
    } else if (arg_sv == "GT") {
      flags |= ExpireFlags::EXPIRE_GT;
    } else if (arg_sv == "LT") {
      flags |= ExpireFlags::EXPIRE_LT;
    } else {
      builder->SendError(absl::StrCat("Unsupported option: ", arg_sv));
      return nullopt;
    }
  }
  if ((flags & ExpireFlags::EXPIRE_NX) && (flags & ~ExpireFlags::EXPIRE_NX)) {
    builder->SendError("NX and XX, GT or LT options at the same time are not compatible");
    return nullopt;
  }
  if ((flags & ExpireFlags::EXPIRE_GT) && (flags & ExpireFlags::EXPIRE_LT)) {
    builder->SendError("GT and LT options at the same time are not compatible");
    return nullopt;
  }
  return flags;
}

void DeleteGeneric(CmdArgList args, const CommandContext& cmd_cntx, bool async) {
  atomic_uint32_t result{0};
  auto* builder = cmd_cntx.rb;
  bool is_mc = (builder->GetProtocol() == Protocol::MEMCACHE);

  auto cb = [&](const Transaction* t, EngineShard* shard) {
    ShardArgs args = t->GetShardArgs(shard->shard_id());
    auto res = GenericFamily::OpDel(t->GetOpArgs(shard), args, async);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  DVLOG(2) << "Del ts " << cmd_cntx.tx->txid();

  uint32_t del_cnt = result.load(memory_order_relaxed);
  if (is_mc) {
    using facade::MCReplyBuilder;
    MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(builder);

    if (del_cnt == 0) {
      mc_builder->SendNotFound();
    } else {
      mc_builder->SendDeleted();
    }
  } else {
    builder->SendLong(del_cnt);
  }
}

}  // namespace

OpResult<uint32_t> GenericFamily::OpDel(const OpArgs& op_args, const ShardArgs& keys, bool async) {
  DVLOG(1) << "Del: " << keys.Front() << " async: " << async;
  auto& db_slice = op_args.GetDbSlice();

  uint32_t res = 0;

  for (string_view key : keys) {
    auto it = db_slice.FindMutable(op_args.db_cntx, key).it;  // post_updater will run immediately
    if (!IsValid(it))
      continue;

    if (async)
      it->first.SetAsyncDelete();

    db_slice.Del(op_args.db_cntx, it);
    ++res;
  }

  return res;
}

void GenericFamily::Del(CmdArgList args, const CommandContext& cmd_cntx) {
  DeleteGeneric(args, cmd_cntx, false);
}

void GenericFamily::Unlink(CmdArgList args, const CommandContext& cmd_cntx) {
  bool async = absl::GetFlag(FLAGS_unlink_experimental_async);
  DeleteGeneric(args, cmd_cntx, async);
}

void GenericFamily::Ping(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (args.size() > 1) {
    return rb->SendError(facade::WrongNumArgsError("ping"), kSyntaxErrType);
  }

  string_view msg;

  // If a client in the subscribe state and in resp2 mode, it returns an array for some reason.
  if (cmd_cntx.conn_cntx->conn_state.subscribe_info && !rb->IsResp3()) {
    if (args.size() == 1) {
      msg = ArgS(args, 0);
    }

    string_view resp[2] = {"pong", msg};
    return rb->SendBulkStrArr(resp);
  }

  if (args.size() == 0) {
    return rb->SendSimpleString("PONG");
  }

  msg = ArgS(args, 0);
  DVLOG(2) << "Ping " << msg;

  return rb->SendBulkString(msg);
}

void GenericFamily::Exists(CmdArgList args, const CommandContext& cmd_cntx) {
  VLOG(1) << "Exists " << ArgS(args, 0);

  atomic_uint32_t result{0};

  auto cb = [&result](Transaction* t, EngineShard* shard) {
    ShardArgs args = t->GetShardArgs(shard->shard_id());
    auto res = OpExists(t->GetOpArgs(shard), args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  return cmd_cntx.rb->SendLong(result.load(memory_order_acquire));
}

void GenericFamily::Persist(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpPersist(t->GetOpArgs(shard), key); };

  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  cmd_cntx.rb->SendLong(status == OpStatus::OK);
}

void GenericFamily::Expire(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view sec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  int_arg = std::max<int64_t>(int_arg, -1);

  // silently cap the expire time to kMaxExpireDeadlineSec which is more than 8 years.
  if (int_arg > kMaxExpireDeadlineSec) {
    int_arg = kMaxExpireDeadlineSec;
  }

  auto expire_options = ParseExpireOptionsOrReply(args.subspan(2), cmd_cntx.rb);
  if (!expire_options) {
    return;
  }
  DbSlice::ExpireParams params{.value = int_arg, .expire_options = expire_options.value()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };

  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  cmd_cntx.rb->SendLong(status == OpStatus::OK);
}

void GenericFamily::ExpireAt(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view sec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  int_arg = std::max<int64_t>(int_arg, 0L);
  auto expire_options = ParseExpireOptionsOrReply(args.subspan(2), cmd_cntx.rb);
  if (!expire_options) {
    return;
  }
  DbSlice::ExpireParams params{
      .value = int_arg, .absolute = true, .expire_options = expire_options.value()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };
  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return cmd_cntx.rb->SendError(kExpiryOutOfRange);
  }

  cmd_cntx.rb->SendLong(status == OpStatus::OK);
}

void GenericFamily::Keys(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view pattern(ArgS(args, 0));
  uint64_t cursor = 0;

  StringVec keys;

  ScanOpts scan_opts;
  if (pattern != "*") {
    scan_opts.matcher.reset(new GlobMatcher{pattern, true});
  }

  scan_opts.limit = 512;
  auto output_limit = absl::GetFlag(FLAGS_keys_output_limit);

  do {
    cursor = ScanGeneric(cursor, scan_opts, &keys, cmd_cntx.conn_cntx);
  } while (cursor != 0 && keys.size() < output_limit);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(keys.size());
  for (const auto& k : keys) {
    rb->SendBulkString(k);
  }
}

void GenericFamily::PexpireAt(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view msec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(msec, &int_arg)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  int_arg = std::max<int64_t>(int_arg, 0L);
  auto expire_options = ParseExpireOptionsOrReply(args.subspan(2), cmd_cntx.rb);
  if (!expire_options) {
    return;
  }
  DbSlice::ExpireParams params{.value = int_arg,
                               .unit = TimeUnit::MSEC,
                               .absolute = true,
                               .expire_options = expire_options.value()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };
  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return cmd_cntx.rb->SendError(kExpiryOutOfRange);
  } else {
    cmd_cntx.rb->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::Pexpire(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view msec = ArgS(args, 1);
  int64_t int_arg;

  if (!absl::SimpleAtoi(msec, &int_arg)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }
  int_arg = std::max<int64_t>(int_arg, -1);

  // to be more compatible with redis, we silently cap the expire time to kMaxExpireDeadlineSec
  if (int_arg > kMaxExpireDeadlineMs) {
    int_arg = kMaxExpireDeadlineMs;
  }

  auto expire_options = ParseExpireOptionsOrReply(args.subspan(2), cmd_cntx.rb);
  if (!expire_options) {
    return;
  }
  DbSlice::ExpireParams params{
      .value = int_arg, .unit = TimeUnit::MSEC, .expire_options = expire_options.value()};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(t->GetOpArgs(shard), key, params);
  };
  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return cmd_cntx.rb->SendError(kExpiryOutOfRange);
  }
  cmd_cntx.rb->SendLong(status == OpStatus::OK);
}

void GenericFamily::Stick(CmdArgList args, const CommandContext& cmd_cntx) {
  Transaction* transaction = cmd_cntx.tx;
  VLOG(1) << "Stick " << ArgS(args, 0);

  atomic_uint32_t result{0};

  auto cb = [&result](const Transaction* t, EngineShard* shard) {
    ShardArgs args = t->GetShardArgs(shard->shard_id());
    auto res = OpStick(t->GetOpArgs(shard), args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  DVLOG(2) << "Stick ts " << transaction->txid();

  uint32_t match_cnt = result.load(memory_order_relaxed);
  cmd_cntx.rb->SendLong(match_cnt);
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
      if (!absl::SimpleAtod(item, &this->score)) {
        if (!item.empty()) {
          return false;
        }
        this->score = 0;
      }
      if (std::isnan(this->score)) {
        return false;
      }
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

  static bool less(const SortEntry& l, const SortEntry& r) {
    if constexpr (!ALPHA) {
      if (l.score < r.score) {
        return true;
      } else if (r.score < l.score) {
        return false;
      }
      // to prevent unstrict order we compare values lexicographically
    }
    return l.key < r.key;
  }

  static bool greater(const SortEntry& l, const SortEntry& r) {
    return less(r, l);
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

  auto it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key).it;
  if (!IsValid(it)) {
    return OpStatus::KEY_NOTFOUND;
  }
  if (!IsContainer(it->second)) {
    return OpStatus::WRONG_TYPE;
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
  return success ? res : OpStatus::INVALID_NUMERIC_RESULT;
}

void GenericFamily::Sort(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  bool alpha = false;
  bool reversed = false;
  std::optional<std::pair<size_t, size_t>> bounds;
  auto* builder = cmd_cntx.rb;
  for (size_t i = 1; i < args.size(); i++) {
    string arg = absl::AsciiStrToUpper(ArgS(args, i));
    if (arg == "ALPHA") {
      alpha = true;
    } else if (arg == "DESC") {
      reversed = true;
    } else if (arg == "ASC") {
      reversed = false;
    } else if (arg == "LIMIT") {
      int offset, limit;
      if (i + 2 >= args.size()) {
        return builder->SendError(kSyntaxErr);
      }
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &offset) ||
          !absl::SimpleAtoi(ArgS(args, i + 2), &limit)) {
        return builder->SendError(kInvalidIntErr);
      }
      bounds = {offset, limit};
      i += 2;
    } else {
      LOG_EVERY_T(ERROR, 1) << "Unsupported option " << arg;
      return builder->SendError(kSyntaxErr, kSyntaxErrType);
    }
  }

  OpResultTyped<SortEntryList> fetch_result =
      cmd_cntx.tx->ScheduleSingleHopT([&](Transaction* t, EngineShard* shard) {
        return OpFetchSortEntries(t->GetOpArgs(shard), key, alpha);
      });

  if (fetch_result == OpStatus::WRONG_TYPE)
    return builder->SendError(fetch_result.status());

  if (fetch_result.status() == OpStatus::INVALID_NUMERIC_RESULT)
    return builder->SendError("One or more scores can't be converted into double");

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (!fetch_result.ok())
    return rb->SendEmptyArray();

  auto result_type = fetch_result.type();
  auto sort_call = [builder, bounds, reversed, result_type](auto& entries) {
    using value_t = typename std::decay_t<decltype(entries)>::value_type;
    auto cmp = reversed ? &value_t::greater : &value_t::less;
    if (bounds) {
      auto sort_it = entries.begin() + std::min(bounds->first + bounds->second, entries.size());
      std::partial_sort(entries.begin(), sort_it, entries.end(), cmp);
    } else {
      std::sort(entries.begin(), entries.end(), cmp);
    }

    auto start_it = entries.begin();
    auto end_it = entries.end();
    if (bounds) {
      start_it += std::min(bounds->first, entries.size());
      end_it = entries.begin() + std::min(bounds->first + bounds->second, entries.size());
    }

    bool is_set = (result_type == OBJ_SET || result_type == OBJ_ZSET);
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    rb->StartCollection(std::distance(start_it, end_it),
                        is_set ? RedisReplyBuilder::SET : RedisReplyBuilder::ARRAY);

    for (auto it = start_it; it != end_it; ++it) {
      rb->SendBulkString(it->key);
    }
  };

  std::visit(std::move(sort_call), fetch_result.value());
}

void GenericFamily::Restore(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view serialized_value = ArgS(args, 2);

  auto rdb_version = GetRdbVersion(serialized_value);
  auto* builder = cmd_cntx.rb;
  if (!rdb_version) {
    return builder->SendError(kInvalidDumpValueErr);
  }

  OpResult<RestoreArgs> restore_args = RestoreArgs::TryFrom(args);
  if (!restore_args) {
    if (restore_args.status() == OpStatus::OUT_OF_RANGE) {
      return builder->SendError("Invalid IDLETIME value, must be >= 0");
    } else {
      return builder->SendError(restore_args.status());
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRestore(t->GetOpArgs(shard), key, serialized_value, restore_args.value(),
                     rdb_version.value());
  };

  OpStatus result = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  switch (result) {
    case OpStatus::OK:
      return builder->SendOk();
    case OpStatus::KEY_EXISTS:
      return builder->SendError("-BUSYKEY Target key name already exists.");
    case OpStatus::INVALID_VALUE:
      return builder->SendError("Bad data format");
    default:
      return builder->SendError(result);
  }
}

void GenericFamily::FieldExpire(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view ttl_str = parser.Next();
  uint32_t ttl_sec;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (!absl::SimpleAtoi(ttl_str, &ttl_sec) || ttl_sec == 0 || ttl_sec > kMaxTtl) {
    return rb->SendError(kInvalidIntErr);
  }
  CmdArgList fields = parser.Tail();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpFieldExpire(t->GetOpArgs(shard), key, ttl_sec, fields);
  };

  OpResult<vector<long>> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  if (result) {
    rb->StartArray(result->size());
    const auto& array = result.value();
    for (const auto& v : array) {
      rb->SendLong(v);
    }
  } else {
    rb->SendError(result.status());
  }
}

// Returns -2 if key not found, WRONG_TYPE if key is not a set or hash
// -1 if the field does not have associated TTL on it, and -3 if field is not found.
void GenericFamily::FieldTtl(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpFieldTtl(t, shard, key, field); };

  OpResult<long> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  if (result) {
    cmd_cntx.rb->SendLong(*result);
    return;
  }

  cmd_cntx.rb->SendError(result.status());
}

void GenericFamily::Move(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view target_db_sv = ArgS(args, 1);
  int32_t target_db;
  auto* builder = cmd_cntx.rb;
  if (!absl::SimpleAtoi(target_db_sv, &target_db)) {
    return builder->SendError(kInvalidIntErr);
  }

  if (target_db < 0 || uint32_t(target_db) >= absl::GetFlag(FLAGS_dbnum)) {
    return builder->SendError(kDbIndOutOfRangeErr);
  }

  if (target_db == cmd_cntx.tx->GetDbIndex()) {
    return builder->SendError("source and destination objects are the same");
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

  cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  // Exactly one shard will call OpMove.
  DCHECK(res != OpStatus::SKIPPED);
  builder->SendLong(res == OpStatus::OK);
}

void GenericFamily::Rename(CmdArgList args, const CommandContext& cmd_cntx) {
  auto reply = RenameGeneric(args, false, cmd_cntx.tx);
  cmd_cntx.rb->SendError(reply);
}

void GenericFamily::RenameNx(CmdArgList args, const CommandContext& cmd_cntx) {
  auto reply = RenameGeneric(args, true, cmd_cntx.tx);
  auto* builder = cmd_cntx.rb;
  if (!reply.status) {
    builder->SendError(reply);
    return;
  }

  OpStatus st = reply.status.value();
  if (st == OpStatus::OK) {
    builder->SendLong(1);
  } else if (st == OpStatus::KEY_EXISTS) {
    builder->SendLong(0);
  } else {
    builder->SendError(reply);
  }
}

void GenericFamily::Copy(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser(args);
  auto [k1, k2] = parser.Next<std::string_view, std::string_view>();
  bool replace = parser.Check("REPLACE");

  if (!parser.Finalize()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  if (k1 == k2) {
    cmd_cntx.rb->SendError("source and destination objects are the same");
    return;
  }

  Renamer renamer(cmd_cntx.tx, k1, k2, shard_set->size(), true);
  cmd_cntx.rb->SendError(renamer.Rename(!replace));
}

void GenericFamily::ExpireTime(CmdArgList args, const CommandContext& cmd_cntx) {
  ExpireTimeGeneric(args, TimeUnit::SEC, cmd_cntx.tx, cmd_cntx.rb);
}

void GenericFamily::PExpireTime(CmdArgList args, const CommandContext& cmd_cntx) {
  ExpireTimeGeneric(args, TimeUnit::MSEC, cmd_cntx.tx, cmd_cntx.rb);
}

void GenericFamily::Ttl(CmdArgList args, const CommandContext& cmd_cntx) {
  TtlGeneric(args, TimeUnit::SEC, cmd_cntx.tx, cmd_cntx.rb);
}

void GenericFamily::Pttl(CmdArgList args, const CommandContext& cmd_cntx) {
  TtlGeneric(args, TimeUnit::MSEC, cmd_cntx.tx, cmd_cntx.rb);
}

void GenericFamily::Select(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  int64_t index;
  auto* builder = cmd_cntx.rb;
  if (!absl::SimpleAtoi(key, &index)) {
    return builder->SendError(kInvalidDbIndErr);
  }
  if (IsClusterEnabled() && index != 0) {
    return builder->SendError("SELECT is not allowed in cluster mode");
  }
  if (index < 0 || index >= absl::GetFlag(FLAGS_dbnum)) {
    return builder->SendError(kDbIndOutOfRangeErr);
  }
  auto* cntx = cmd_cntx.conn_cntx;
  if (cntx->conn_state.db_index == index) {
    // accept a noop.
    return builder->SendOk();
  }

  if (cntx->conn_state.exec_info.IsRunning()) {
    return builder->SendError("SELECT is not allowed in a transaction");
  }

  cntx->conn_state.db_index = index;
  auto cb = [ns = cntx->ns, index](EngineShard* shard) {
    auto& db_slice = ns->GetDbSlice(shard->shard_id());
    db_slice.ActivateDb(index);
    return OpStatus::OK;
  };
  shard_set->RunBriefInParallel(std::move(cb));

  return builder->SendOk();
}

void GenericFamily::Dump(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  DVLOG(1) << "Dumping before ::ScheduleSingleHopT " << key;
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpDump(t->GetOpArgs(shard), key); };
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  OpResult<string> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  if (result) {
    DVLOG(1) << "Dump " << cmd_cntx.tx->DebugId() << ": " << key << ", dump size "
             << result.value().size();
    rb->SendBulkString(*result);
  } else {
    rb->SendNull();
  }
}

void GenericFamily::Type(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<CompactObjType> {
    auto& db_slice = t->GetDbSlice(shard->shard_id());
    auto it = db_slice.FindReadOnly(t->GetDbContext(), key).it;
    if (!it.is_done()) {
      return it->second.ObjType();
    } else {
      return OpStatus::KEY_NOTFOUND;
    }
  };
  OpResult<CompactObjType> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (!result) {
    cmd_cntx.rb->SendSimpleString("none");
  } else {
    cmd_cntx.rb->SendSimpleString(ObjTypeToString(result.value()));
  }
}

void GenericFamily::Time(CmdArgList args, const CommandContext& cmd_cntx) {
  uint64_t now_usec;
  if (cmd_cntx.tx) {
    now_usec = cmd_cntx.tx->GetDbContext().time_now_ms * 1000;
  } else {
    now_usec = absl::GetCurrentTimeNanos() / 1000;
  }

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(2);
  rb->SendLong(now_usec / 1000000);
  rb->SendLong(now_usec % 1000000);
}

void GenericFamily::Echo(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  return rb->SendBulkString(key);
}

// SCAN cursor [MATCH <glob>] [TYPE <type>] [COUNT <count>] [BUCKET <bucket_id>]
// [ATTR <mask>] [MLCGE <len>]
void GenericFamily::Scan(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view token = ArgS(args, 0);
  uint64_t cursor = 0;
  auto* builder = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (!absl::SimpleAtoi(token, &cursor)) {
    if (absl::EqualsIgnoreCase(token, "HELP")) {
      string_view help_arr[] = {
          "SCAN cursor [MATCH <glob>] [TYPE <type>] [COUNT <count>] [ATTR <mask>] [MINMSZ <len>]",
          "    MATCH <glob> - pattern to match keys against",
          "    TYPE <type> - type of values to match",
          "    COUNT <count> - number of keys to return",
          "    ATTR <v|p|a|u> - filter by attributes: v - volatile (ttl), ",
          "    p - persistent (no ttl), a - accessed since creation, u - untouched",
          "    MINMSZ <len> - keeps keys with values, whose allocated size is greater or equal to",
          "        the specified length",
      };
      return builder->SendSimpleStrArr(help_arr);
    }
    return builder->SendError("invalid cursor");
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(1));
  if (!ops) {
    DVLOG(1) << "Scan invalid args - return " << ops << " to the user";
    return builder->SendError(ops.status());
  }

  const ScanOpts& scan_op = ops.value();

  StringVec keys;
  cursor = ScanGeneric(cursor, scan_op, &keys, cmd_cntx.conn_cntx);

  builder->StartArray(2);
  builder->SendBulkString(absl::StrCat(cursor));
  builder->StartArray(keys.size());
  for (const auto& k : keys) {
    builder->SendBulkString(k);
  }
}

OpResult<uint32_t> GenericFamily::OpExists(const OpArgs& op_args, const ShardArgs& keys) {
  DVLOG(1) << "Exists: " << keys.Front();
  auto& db_slice = op_args.GetDbSlice();
  uint32_t res = 0;

  for (string_view key : keys) {
    auto find_res = db_slice.FindReadOnly(op_args.db_cntx, key);
    res += IsValid(find_res.it);
  }
  return res;
}

void GenericFamily::RandomKey(CmdArgList args, const CommandContext& cmd_cntx) {
  const static size_t kMaxAttempts = 3;

  absl::BitGen bitgen;
  atomic_size_t candidates_counter{0};
  auto* cntx = cmd_cntx.conn_cntx;
  DbContext db_cntx{cntx->ns, cntx->conn_state.db_index, GetCurrentTimeMs()};
  ScanOpts scan_opts;
  scan_opts.limit = 3;  // number of entries per shard
  std::vector<StringVec> candidates_collection(shard_set->size());

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) {
        auto [prime_table, expire_table] =
            cntx->ns->GetDbSlice(shard->shard_id()).GetTables(db_cntx.db_index);
        if (prime_table->size() == 0) {
          return;
        }

        StringVec* candidates = &candidates_collection[shard->shard_id()];

        for (size_t i = 0; i <= kMaxAttempts; ++i) {
          if (!candidates->empty()) {
            break;
          }
          uint64_t cursor = 0;  // scans from the start of the shard after reaching kMaxAttemps
          if (i < kMaxAttempts) {
            cursor = prime_table->GetRandomCursor(&bitgen).value();
          }
          OpScan({shard, 0u, db_cntx}, scan_opts, &cursor, candidates);
        }

        candidates_counter.fetch_add(candidates->size(), memory_order_relaxed);
      },
      [&](ShardId) { return true; });

  auto candidates_count = candidates_counter.load(memory_order_relaxed);
  std::optional<string> random_key = std::nullopt;
  auto random_idx = absl::Uniform<size_t>(bitgen, 0, candidates_count);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  for (const auto& candidate : candidates_collection) {
    if (random_idx >= candidate.size()) {
      random_idx -= candidate.size();
    } else {
      return rb->SendBulkString(candidate[random_idx]);
    }
  }
  rb->SendNull();
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&GenericFamily::x)

namespace acl {

constexpr uint32_t kDel = KEYSPACE | WRITE | SLOW;
constexpr uint32_t kPing = FAST | CONNECTION;
constexpr uint32_t kEcho = FAST | CONNECTION;
constexpr uint32_t kExists = KEYSPACE | READ | FAST;
constexpr uint32_t kTouch = KEYSPACE | READ | FAST;
constexpr uint32_t kExpire = KEYSPACE | WRITE | FAST;
constexpr uint32_t kExpireAt = KEYSPACE | WRITE | FAST;
constexpr uint32_t kPersist = KEYSPACE | WRITE | FAST;
constexpr uint32_t kKeys = KEYSPACE | READ | SLOW | DANGEROUS;
constexpr uint32_t kPExpireAt = KEYSPACE | WRITE | FAST;
constexpr uint32_t kPExpire = KEYSPACE | WRITE | FAST;
constexpr uint32_t kRename = KEYSPACE | WRITE | SLOW;
constexpr uint32_t kCopy = KEYSPACE | WRITE | SLOW;
constexpr uint32_t kRenamNX = KEYSPACE | WRITE | FAST;
constexpr uint32_t kSelect = FAST | CONNECTION;
constexpr uint32_t kScan = KEYSPACE | READ | SLOW;
constexpr uint32_t kTTL = KEYSPACE | READ | FAST;
constexpr uint32_t kPTTL = KEYSPACE | READ | FAST;
constexpr uint32_t kFieldTtl = KEYSPACE | READ | FAST;
constexpr uint32_t kTime = FAST;
constexpr uint32_t kType = KEYSPACE | READ | FAST;
constexpr uint32_t kDump = KEYSPACE | READ | SLOW;
constexpr uint32_t kUnlink = KEYSPACE | WRITE | FAST;
constexpr uint32_t kStick = KEYSPACE | WRITE | FAST;
constexpr uint32_t kSort = WRITE | SET | SORTEDSET | LIST | SLOW | DANGEROUS;
constexpr uint32_t kMove = KEYSPACE | WRITE | FAST;
constexpr uint32_t kRestore = KEYSPACE | WRITE | SLOW | DANGEROUS;
constexpr uint32_t kExpireTime = KEYSPACE | READ | FAST;
constexpr uint32_t kPExpireTime = KEYSPACE | READ | FAST;
constexpr uint32_t kFieldExpire = WRITE | HASH | SET | FAST;
}  // namespace acl

void GenericFamily::Register(CommandRegistry* registry) {
  constexpr auto kSelectOpts = CO::LOADING | CO::FAST | CO::NOSCRIPT;
  registry->StartFamily();
  *registry
      << CI{"DEL", CO::WRITE, -2, 1, -1, acl::kDel}.HFUNC(Del)
      /* Redis compatibility:
       * We don't allow PING during loading since in Redis PING is used as
       * failure detection, and a loading server is considered to be
       * not available. */
      << CI{"PING", CO::FAST, -1, 0, 0, acl::kPing}.HFUNC(Ping)
      << CI{"ECHO", CO::LOADING | CO::FAST, 2, 0, 0, acl::kEcho}.HFUNC(Echo)
      << CI{"EXISTS", CO::READONLY | CO::FAST, -2, 1, -1, acl::kExists}.HFUNC(Exists)
      << CI{"TOUCH", CO::READONLY | CO::FAST, -2, 1, -1, acl::kTouch}.HFUNC(Exists)
      << CI{"EXPIRE", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, -3, 1, 1, acl::kExpire}.HFUNC(
             Expire)
      << CI{"EXPIREAT", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, -3, 1, 1, acl::kExpireAt}.HFUNC(
             ExpireAt)
      << CI{"PERSIST", CO::WRITE | CO::FAST, 2, 1, 1, acl::kPersist}.HFUNC(Persist)
      << CI{"KEYS", CO::READONLY, 2, 0, 0, acl::kKeys}.HFUNC(Keys)
      << CI{"PEXPIREAT", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, -3, 1, 1, acl::kPExpireAt}
             .HFUNC(PexpireAt)
      << CI{"PEXPIRE", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, -3, 1, 1, acl::kPExpire}.HFUNC(
             Pexpire)
      << CI{"FIELDEXPIRE", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, acl::kFieldExpire}.HFUNC(
             FieldExpire)
      << CI{"RENAME", CO::WRITE | CO::NO_AUTOJOURNAL, 3, 1, 2, acl::kRename}.HFUNC(Rename)
      << CI{"COPY", CO::WRITE | CO::NO_AUTOJOURNAL, -3, 1, 2, acl::kCopy}.HFUNC(Copy)
      << CI{"RENAMENX", CO::WRITE | CO::NO_AUTOJOURNAL, 3, 1, 2, acl::kRenamNX}.HFUNC(RenameNx)
      << CI{"SELECT", kSelectOpts, 2, 0, 0, acl::kSelect}.HFUNC(Select)
      << CI{"SCAN", CO::READONLY | CO::FAST | CO::LOADING, -2, 0, 0, acl::kScan}.HFUNC(Scan)
      << CI{"TTL", CO::READONLY | CO::FAST, 2, 1, 1, acl::kTTL}.HFUNC(Ttl)
      << CI{"PTTL", CO::READONLY | CO::FAST, 2, 1, 1, acl::kPTTL}.HFUNC(Pttl)
      << CI{"FIELDTTL", CO::READONLY | CO::FAST, 3, 1, 1, acl::kFieldTtl}.HFUNC(FieldTtl)
      << CI{"TIME", CO::LOADING | CO::FAST, 1, 0, 0, acl::kTime}.HFUNC(Time)
      << CI{"TYPE", CO::READONLY | CO::FAST | CO::LOADING, 2, 1, 1, acl::kType}.HFUNC(Type)
      << CI{"DUMP", CO::READONLY, 2, 1, 1, acl::kDump}.HFUNC(Dump)
      << CI{"UNLINK", CO::WRITE, -2, 1, -1, acl::kUnlink}.HFUNC(Unlink)
      << CI{"STICK", CO::WRITE, -2, 1, -1, acl::kStick}.HFUNC(Stick)
      << CI{"SORT", CO::READONLY, -2, 1, 1, acl::kSort}.HFUNC(Sort)
      << CI{"MOVE", CO::WRITE | CO::GLOBAL_TRANS | CO::NO_AUTOJOURNAL, 3, 1, 1, acl::kMove}.HFUNC(
             Move)
      << CI{"RESTORE", CO::WRITE, -4, 1, 1, acl::kRestore}.HFUNC(Restore)
      << CI{"RANDOMKEY", CO::READONLY, 1, 0, 0, 0}.HFUNC(RandomKey)
      << CI{"EXPIRETIME", CO::READONLY | CO::FAST, 2, 1, 1, acl::kExpireTime}.HFUNC(ExpireTime)
      << CI{"PEXPIRETIME", CO::READONLY | CO::FAST, 2, 1, 1, acl::kPExpireTime}.HFUNC(PExpireTime);
}

}  // namespace dfly
