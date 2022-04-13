// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

extern "C" {
#include "redis/object.h"
#include "redis/util.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"
#include "util/varz.h"

DEFINE_uint32(dbnum, 16, "Number of databases");

namespace dfly {
using namespace std;
using facade::kExpiryOutOfRange;
using facade::Protocol;

namespace {

class Renamer {
 public:
  Renamer(DbIndex dind, ShardId source_id) : db_indx_(dind), src_sid_(source_id) {
  }

  void Find(Transaction* t);

  OpResult<void> status() const {
    return status_;
  };

  void Finalize(Transaction* t, bool skip_exist_dest);

 private:
  OpStatus MoveSrc(Transaction* t, EngineShard* es);
  OpStatus UpdateDest(Transaction* t, EngineShard* es);

  DbIndex db_indx_;
  ShardId src_sid_;

  struct FindResult {
    string_view key;
    PrimeValue ref_val;
    uint64_t expire_ts;
    bool found = false;
  };

  PrimeValue pv_;
  string str_val_;

  FindResult src_res_, dest_res_;  // index 0 for source, 1 for destination
  OpResult<void> status_;
};

void Renamer::Find(Transaction* t) {
  auto cb = [this](Transaction* t, EngineShard* shard) {
    auto args = t->ShardArgsInShard(shard->shard_id());
    CHECK_EQ(1u, args.size());

    FindResult* res = (shard->shard_id() == src_sid_) ? &src_res_ : &dest_res_;

    res->key = args.front();
    auto& db_slice = EngineShard::tlocal()->db_slice();
    auto [it, exp_it] = db_slice.FindExt(db_indx_, res->key);

    res->found = IsValid(it);
    if (IsValid(it)) {
      res->ref_val = it->second.AsRef();
      res->expire_ts = db_slice.ExpireTime(exp_it);
    }
    return OpStatus::OK;
  };

  t->Execute(move(cb), false);
};

void Renamer::Finalize(Transaction* t, bool skip_exist_dest) {
  auto cleanup = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };

  if (!src_res_.found) {
    status_ = OpStatus::KEY_NOTFOUND;

    t->Execute(move(cleanup), true);
    return;
  }

  if (dest_res_.found && skip_exist_dest) {
    status_ = OpStatus::KEY_EXISTS;

    t->Execute(move(cleanup), true);
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
    auto it = es->db_slice().FindExt(db_indx_, src_res_.key).first;
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
    CHECK(es->db_slice().Del(db_indx_, it));  // delete the entry with empty value in it.
  }

  return OpStatus::OK;
}

OpStatus Renamer::UpdateDest(Transaction* t, EngineShard* es) {
  if (es->shard_id() != src_sid_) {
    auto& db_slice = es->db_slice();
    string_view dest_key = dest_res_.key;
    PrimeIterator dest_it = db_slice.FindExt(db_indx_, dest_key).first;
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
      db_slice.Expire(db_indx_, dest_it, src_res_.expire_ts);
    } else {
      if (src_res_.ref_val.ObjType() == OBJ_STRING) {
        pv_.SetString(str_val_);
      }
      dest_it = db_slice.AddNew(db_indx_, dest_key, std::move(pv_), src_res_.expire_ts);
    }

    if (!is_prior_list && dest_it->second.ObjType() == OBJ_LIST) {
      es->AwakeWatched(db_indx_, dest_key);
    }
  }

  return OpStatus::OK;
}

const char* ObjTypeName(int type) {
  switch (type) {
    case OBJ_STRING:
      return "string";
    case OBJ_LIST:
      return "list";
    case OBJ_SET:
      return "set";
    case OBJ_ZSET:
      return "zset";
    case OBJ_HASH:
      return "hash";
    case OBJ_STREAM:
      return "stream";
    default:
      LOG(ERROR) << "Unsupported type " << type;
  }
  return "invalid";
};
}  // namespace

void GenericFamily::Init(util::ProactorPool* pp) {
}

void GenericFamily::Shutdown() {
}

void GenericFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Del " << ArgS(args, 1);

  atomic_uint32_t result{0};
  bool is_mc = cntx->protocol() == Protocol::MEMCACHE;

  auto cb = [&result](const Transaction* t, EngineShard* shard) {
    ArgSlice args = t->ShardArgsInShard(shard->shard_id());
    auto res = OpDel(OpArgs{shard, t->db_index()}, args);
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
  if (args.size() > 2) {
    return (*cntx)->SendError(facade::WrongNumArgsError("ping"), kSyntaxErr);
  }

  // We synchronously block here until the engine sends us the payload and notifies that
  // the I/O operation has been processed.
  if (args.size() == 1) {
    return (*cntx)->SendSimpleString("PONG");
  } else {
    string_view arg = ArgS(args, 1);
    DVLOG(2) << "Ping " << arg;

    return (*cntx)->SendBulkString(arg);
  }
}

void GenericFamily::Exists(CmdArgList args, ConnectionContext* cntx) {
  Transaction* transaction = cntx->transaction;
  VLOG(1) << "Exists " << ArgS(args, 1);

  atomic_uint32_t result{0};

  auto cb = [&result](Transaction* t, EngineShard* shard) {
    ArgSlice args = t->ShardArgsInShard(shard->shard_id());
    auto res = OpExists(OpArgs{shard, t->db_index()}, args);
    result.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  OpStatus status = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  return (*cntx)->SendLong(result.load(memory_order_release));
}

void GenericFamily::Expire(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view sec = ArgS(args, 2);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  int_arg = std::max(int_arg, -1L);
  ExpireParams params{.ts = int_arg};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(OpArgs{shard, t->db_index()}, key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(move(cb));
  if (status == OpStatus::OUT_OF_RANGE) {
    return (*cntx)->SendError(kExpiryOutOfRange);
  } else {
    (*cntx)->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::ExpireAt(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view sec = ArgS(args, 2);
  int64_t int_arg;

  if (!absl::SimpleAtoi(sec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  int_arg = std::max(int_arg, 0L);
  ExpireParams params{.ts = int_arg, .absolute = true};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(OpArgs{shard, t->db_index()}, key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return (*cntx)->SendError(kExpiryOutOfRange);
  } else {
    (*cntx)->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::PexpireAt(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view msec = ArgS(args, 2);
  int64_t int_arg;

  if (!absl::SimpleAtoi(msec, &int_arg)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  int_arg = std::max(int_arg, 0L);
  ExpireParams params{.ts = int_arg, .absolute = true, .unit = MSEC};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExpire(OpArgs{shard, t->db_index()}, key, params);
  };
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OUT_OF_RANGE) {
    return (*cntx)->SendError(kExpiryOutOfRange);
  } else {
    (*cntx)->SendLong(status == OpStatus::OK);
  }
}

void GenericFamily::Rename(CmdArgList args, ConnectionContext* cntx) {
  OpResult<void> st = RenameGeneric(args, false, cntx);
  (*cntx)->SendError(st.status());
}

void GenericFamily::Ttl(CmdArgList args, ConnectionContext* cntx) {
  TtlGeneric(args, cntx, TimeUnit::SEC);
}

void GenericFamily::Pttl(CmdArgList args, ConnectionContext* cntx) {
  TtlGeneric(args, cntx, TimeUnit::MSEC);
}

void GenericFamily::TtlGeneric(CmdArgList args, ConnectionContext* cntx, TimeUnit unit) {
  string_view key = ArgS(args, 1);

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
  string_view key = ArgS(args, 1);
  int64_t index;
  if (!absl::SimpleAtoi(key, &index)) {
    return (*cntx)->SendError(kInvalidDbIndErr);
  }
  if (index < 0 || index >= FLAGS_dbnum) {
    return (*cntx)->SendError(kDbIndOutOfRangeErr);
  }
  cntx->conn_state.db_index = index;
  auto cb = [index](EngineShard* shard) {
    shard->db_slice().ActivateDb(index);
    return OpStatus::OK;
  };
  cntx->shard_set->RunBriefInParallel(std::move(cb));

  return (*cntx)->SendOk();
}

void GenericFamily::Type(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<int> {
    auto it = shard->db_slice().FindExt(t->db_index(), key).first;
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

OpResult<void> GenericFamily::RenameGeneric(CmdArgList args, bool skip_exist_dest,
                                            ConnectionContext* cntx) {
  string_view key[2] = {ArgS(args, 1), ArgS(args, 2)};

  Transaction* transaction = cntx->transaction;

  if (transaction->unique_shard_cnt() == 1) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return OpRen(OpArgs{shard, t->db_index()}, key[0], key[1], skip_exist_dest);
    };
    OpResult<void> result = transaction->ScheduleSingleHopT(std::move(cb));

    return result;
  }

  transaction->Schedule();
  unsigned shard_count = transaction->shard_set()->size();
  Renamer renamer{transaction->db_index(), Shard(key[0], shard_count)};

  // Phase 1 -> Fetch  keys from both shards.
  // Phase 2 -> If everything is ok, clone the source object, delete the destination object, and
  //            set its ptr to cloned one. we also copy the expiration data of the source key.
  renamer.Find(transaction);
  renamer.Finalize(transaction, skip_exist_dest);

  return renamer.status();
}

void GenericFamily::Echo(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  return (*cntx)->SendBulkString(key);
}

void GenericFamily::Scan(CmdArgList args, ConnectionContext* cntx) {
  string_view token = ArgS(args, 1);
  uint64_t cursor = 0;
  EngineShardSet* ess = cntx->shard_set;
  unsigned shard_count = ess->size();
  uint32_t limit = 10;
  string_view pattern, type_filter;

  // Dash table returns a cursor with its right byte empty. We will use it
  // for encoding shard index. For now scan has a limitation of 255 shards.
  CHECK_LT(shard_count, 1024u);

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  for (unsigned i = 2; i < args.size(); i += 2) {
    if (i + 1 == args.size()) {
      return (*cntx)->SendError(kSyntaxErr);
    }

    ToUpper(&args[i]);

    string_view opt = ArgS(args, i);
    if (opt == "COUNT") {
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &limit)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
      if (limit == 0)
        limit = 1;
      else if (limit > 4096)
        limit = 4096;
    } else if (opt == "MATCH") {
      pattern = ArgS(args, i + 1);
      if (pattern == "*")
        pattern = string_view{};
    } else if (opt == "TYPE") {
      ToLower(&args[i + 1]);
      type_filter = ArgS(args, i + 1);
    } else {
      return (*cntx)->SendError(kSyntaxErr);
    }
  }

  ShardId sid = cursor % 1024;
  if (sid >= shard_count) {
    return (*cntx)->SendError("invalid cursor");
  }

  cursor >>= 10;

  vector<string> keys;
  do {
    ess->Await(sid, [&] {
      OpArgs op_args{EngineShard::tlocal(), cntx->conn_state.db_index};
      OpScan(op_args, pattern, type_filter, limit, &cursor, &keys);
    });
    if (cursor == 0) {
      ++sid;
      if (unsigned(sid) == shard_count)
        break;
    }
  } while (keys.size() < limit);

  if (sid < shard_count) {
    cursor = (cursor << 10) | sid;
  } else {
    DCHECK_EQ(0u, cursor);
  }

  (*cntx)->StartArray(2);
  (*cntx)->SendSimpleString(absl::StrCat(cursor));
  (*cntx)->StartArray(keys.size());
  for (const auto& k : keys) {
    (*cntx)->SendBulkString(k);
  }
}

OpStatus GenericFamily::OpExpire(const OpArgs& op_args, string_view key,
                                 const ExpireParams& params) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(op_args.db_ind, key);
  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  int64_t msec = (params.unit == TimeUnit::SEC) ? params.ts * 1000 : params.ts;
  int64_t now_msec = db_slice.Now();
  int64_t rel_msec = params.absolute ? msec - now_msec : msec;

  if (rel_msec > kMaxExpireDeadlineSec * 1000) {
    return OpStatus::OUT_OF_RANGE;
  }

  if (rel_msec <= 0) {
    CHECK(db_slice.Del(op_args.db_ind, it));
  } else if (IsValid(expire_it)) {
    expire_it->second = db_slice.FromAbsoluteTime(now_msec + rel_msec);
  } else {
    db_slice.Expire(op_args.db_ind, it, rel_msec + now_msec);
  }

  return OpStatus::OK;
}

OpResult<uint64_t> GenericFamily::OpTtl(Transaction* t, EngineShard* shard, string_view key) {
  auto& db_slice = shard->db_slice();
  auto [it, expire_it] = db_slice.FindExt(t->db_index(), key);
  if (!IsValid(it))
    return OpStatus::KEY_NOTFOUND;

  if (!IsValid(expire_it))
    return OpStatus::SKIPPED;

  int64_t ttl_ms = db_slice.ExpireTime(expire_it) - db_slice.Now();
  DCHECK_GT(ttl_ms, 0);  // Otherwise FindExt would return null.
  return ttl_ms;
}

OpResult<uint32_t> GenericFamily::OpDel(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Del: " << keys[0];
  auto& db_slice = op_args.shard->db_slice();

  uint32_t res = 0;

  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto fres = db_slice.FindExt(op_args.db_ind, keys[i]);
    if (!IsValid(fres.first))
      continue;
    res += int(db_slice.Del(op_args.db_ind, fres.first));
  }

  return res;
}

OpResult<uint32_t> GenericFamily::OpExists(const OpArgs& op_args, ArgSlice keys) {
  DVLOG(1) << "Exists: " << keys[0];
  auto& db_slice = op_args.shard->db_slice();
  uint32_t res = 0;

  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto find_res = db_slice.FindExt(op_args.db_ind, keys[i]);
    res += IsValid(find_res.first);
  }
  return res;
}

OpResult<void> GenericFamily::OpRen(const OpArgs& op_args, string_view from_key, string_view to_key,
                                    bool skip_exists) {
  auto& db_slice = op_args.shard->db_slice();
  auto [from_it, from_expire] = db_slice.FindExt(op_args.db_ind, from_key);
  if (!IsValid(from_it))
    return OpStatus::KEY_NOTFOUND;

  bool is_prior_list = false;
  auto [to_it, to_expire] = db_slice.FindExt(op_args.db_ind, to_key);
  if (IsValid(to_it)) {
    if (skip_exists)
      return OpStatus::KEY_EXISTS;

    is_prior_list = (to_it->second.ObjType() == OBJ_LIST);
  }

  uint64_t exp_ts = db_slice.ExpireTime(from_expire);

  // we keep the value we want to move.
  PrimeValue from_obj = std::move(from_it->second);

  // Restore the expire flag on 'from' so we could delete it from expire table.
  from_it->second.SetExpire(IsValid(from_expire));

  if (IsValid(to_it)) {
    to_it->second = std::move(from_obj);
    to_it->second.SetExpire(IsValid(to_expire));  // keep the expire flag on 'to'.

    // It is guaranteed that Expire() call does not erase the element because then
    // from_it would be invalid. Therefore, it Expire does not invalidate any iterators and
    // we can delete via from_it.
    db_slice.Expire(op_args.db_ind, to_it, exp_ts);
    CHECK(db_slice.Del(op_args.db_ind, from_it));
  } else {
    // Here we first delete from_it because AddNew below could invalidate from_it.
    // On the other hand, AddNew does not rely on the iterators - this is why we keep
    // the value in `from_obj`.
    CHECK(db_slice.Del(op_args.db_ind, from_it));
    to_it = db_slice.AddNew(op_args.db_ind, to_key, std::move(from_obj), exp_ts);
  }

  if (!is_prior_list && to_it->second.ObjType() == OBJ_LIST) {
    op_args.shard->AwakeWatched(op_args.db_ind, to_key);
  }
  return OpStatus::OK;
}

void GenericFamily::OpScan(const OpArgs& op_args, string_view pattern, string_view type_filter,
                           size_t limit, uint64_t* cursor, StringVec* vec) {
  auto& db_slice = op_args.shard->db_slice();
  DCHECK(db_slice.IsDbValid(op_args.db_ind));

  unsigned cnt = 0;

  VLOG(1) << "PrimeTable " << db_slice.shard_id() << "/" << op_args.db_ind << " has "
          << db_slice.DbSize(op_args.db_ind);

  uint64_t cur = *cursor;
  auto [prime_table, expire_table] = db_slice.GetTables(op_args.db_ind);
  do {
    cur = prime_table->Traverse(
        cur, [&](PrimeIterator it) { cnt += ScanCb(op_args, it, pattern, type_filter, vec); });
  } while (cur && cnt < limit);

  VLOG(1) << "OpScan " << db_slice.shard_id() << " cursor: " << cur;
  *cursor = cur;
}

bool GenericFamily::ScanCb(const OpArgs& op_args, PrimeIterator it, string_view pattern,
                           string_view type_filter, StringVec* res) {
  auto& db_slice = op_args.shard->db_slice();
  if (it->second.HasExpire()) {
    it = db_slice.ExpireIfNeeded(op_args.db_ind, it).first;
  }

  if (!IsValid(it))
    return false;

  bool matches = type_filter.empty() || ObjTypeName(it->second.ObjType()) == type_filter;

  if (!matches)
    return false;

  if (pattern.empty()) {
    res->push_back(it->first.ToString());
  } else {
    string str = it->first.ToString();
    if (stringmatchlen(pattern.data(), pattern.size(), str.data(), str.size(), 0) != 1)
      return false;

    res->push_back(std::move(str));
  }

  return true;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&GenericFamily::x)

void GenericFamily::Register(CommandRegistry* registry) {
  constexpr auto kSelectOpts = CO::LOADING | CO::FAST | CO::NOSCRIPT;

  *registry << CI{"DEL", CO::WRITE, -2, 1, -1, 1}.HFUNC(Del)
            /* Redis compaitibility:
             * We don't allow PING during loading since in Redis PING is used as
             * failure detection, and a loading server is considered to be
             * not available. */
            << CI{"PING", CO::FAST, -1, 0, 0, 0}.HFUNC(Ping)
            << CI{"ECHO", CO::LOADING | CO::FAST, 2, 0, 0, 0}.HFUNC(Echo)
            << CI{"EXISTS", CO::READONLY | CO::FAST, -2, 1, -1, 1}.HFUNC(Exists)
            << CI{"EXPIRE", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Expire)
            << CI{"EXPIREAT", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(ExpireAt)
            << CI{"PEXPIREAT", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(PexpireAt)
            << CI{"RENAME", CO::WRITE, 3, 1, 2, 1}.HFUNC(Rename)
            << CI{"SELECT", kSelectOpts, 2, 0, 0, 0}.HFUNC(Select)
            << CI{"SCAN", CO::READONLY | CO::FAST, -2, 0, 0, 0}.HFUNC(Scan)
            << CI{"TTL", CO::READONLY | CO::FAST | CO::RANDOM, 2, 1, 1, 1}.HFUNC(Ttl)
            << CI{"PTTL", CO::READONLY | CO::FAST | CO::RANDOM, 2, 1, 1, 1}.HFUNC(Pttl)
            << CI{"TYPE", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(Type)
            << CI{"UNLINK", CO::WRITE, -2, 1, -1, 1}.HFUNC(Del);
}

}  // namespace dfly
