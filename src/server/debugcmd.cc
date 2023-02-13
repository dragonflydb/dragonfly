// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/debugcmd.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>

#include <boost/fiber/operations.hpp>
#include <filesystem>

#include "base/flags.h"
#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "server/server_state.h"
#include "server/string_family.h"
#include "server/transaction.h"
#include "util/fiber_sched_algo.h"
#include "util/fibers/fiber.h"

using namespace std;

ABSL_DECLARE_FLAG(string, dir);
ABSL_DECLARE_FLAG(string, dbfilename);

namespace dfly {

using namespace util;
using boost::intrusive_ptr;
using boost::fibers::fiber;
using namespace facade;
namespace fs = std::filesystem;
using absl::GetFlag;
using absl::StrAppend;

struct PopulateBatch {
  DbIndex dbid;
  uint64_t index[32];
  uint64_t sz = 0;

  PopulateBatch(DbIndex id) : dbid(id) {
  }
};

struct ObjInfo {
  unsigned encoding;
  unsigned bucket_id = 0;
  unsigned slot_id = 0;

  enum LockStatus { NONE, S, X } lock_status = NONE;

  int64_t ttl = INT64_MAX;
  optional<uint32_t> external_len;

  bool has_sec_precision = false;
  bool found = false;
};

void DoPopulateBatch(std::string_view prefix, size_t val_size, bool random_value_str,
                     const SetCmd::SetParams& params, const PopulateBatch& batch) {
  DbContext db_cntx{batch.dbid, 0};
  OpArgs op_args(EngineShard::tlocal(), 0, db_cntx);
  SetCmd sg(op_args, false);

  absl::InsecureBitGen gen;
  for (unsigned i = 0; i < batch.sz; ++i) {
    string key = absl::StrCat(prefix, ":", batch.index[i]);
    string val;

    if (random_value_str) {
      val = GetRandomHex(gen, val_size);
    } else {
      val = absl::StrCat("value:", batch.index[i]);
      if (val.size() < val_size) {
        val.resize(val_size, 'x');
      }
    }

    sg.Set(params, key, val);
  }
}

DebugCmd::DebugCmd(ServerFamily* owner, ConnectionContext* cntx) : sf_(*owner), cntx_(cntx) {
}

void DebugCmd::Run(CmdArgList args) {
  string_view subcmd = ArgS(args, 1);
  if (subcmd == "HELP") {
    string_view help_arr[] = {
        "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "OBJECT <key>",
        "    Show low-level info about `key` and associated value.",
        "LOAD <filename>",
        "RELOAD [option ...]",
        "    Save the RDB on disk and reload it back to memory. Valid <option> values:",
        "    * NOSAVE: the database will be loaded from an existing RDB file.",
        "    Examples:",
        "    * DEBUG RELOAD NOSAVE: replace the current database with the contents of an",
        "      existing RDB file.",
        "REPLICA PAUSE/RESUME",
        "    Stops replica from reconnecting to master, or resumes",
        "REPLICA OFFSET",
        "    Get number of journal commands executed for each replica flow",
        "WATCHED",
        "    Shows the watched keys as a result of BLPOP and similar operations.",
        "POPULATE <count> [<prefix>] [<size>] [RAND]",
        "    Create <count> string keys named key:<num> with value value:<num>.",
        "    If <prefix> is specified then it is used instead of the 'key' prefix.",
        "    If <size> is specified then X character is concatenated multiple times to value:<num>",
        "    to meet value size.",
        "    If RAND is specified than value will be set to random hex string in specified size.",
        "HELP",
        "    Prints this help.",
    };
    return (*cntx_)->SendSimpleStrArr(help_arr, ABSL_ARRAYSIZE(help_arr));
  }

  VLOG(1) << "subcmd " << subcmd;

  if (subcmd == "POPULATE") {
    return Populate(args);
  }

  if (subcmd == "RELOAD") {
    return Reload(args);
  }

  if (subcmd == "REPLICA" && args.size() == 3) {
    return Replica(args);
  }

  if (subcmd == "WATCHED") {
    return Watched();
  }

  if (subcmd == "LOAD" && args.size() == 3) {
    return Load(ArgS(args, 2));
  }

  if (subcmd == "OBJECT" && args.size() == 3) {
    string_view key = ArgS(args, 2);
    return Inspect(key);
  }

  string reply = UnknownSubCmd(subcmd, "DEBUG");
  return (*cntx_)->SendError(reply, kSyntaxErrType);
}

void DebugCmd::Reload(CmdArgList args) {
  bool save = true;

  for (size_t i = 2; i < args.size(); ++i) {
    ToUpper(&args[i]);
    string_view opt = ArgS(args, i);
    VLOG(1) << "opt " << opt;

    if (opt == "NOSAVE") {
      save = false;
    } else {
      return (*cntx_)->SendError("DEBUG RELOAD only supports the NOSAVE options.");
    }
  }

  if (save) {
    string err_details;
    const CommandId* cid = sf_.service().FindCmd("SAVE");
    CHECK_NOTNULL(cid);
    intrusive_ptr<Transaction> trans(new Transaction{cid});
    trans->InitByArgs(0, {});
    VLOG(1) << "Performing save";

    GenericError ec = sf_.DoSave(false, trans.get());
    if (ec) {
      return (*cntx_)->SendError(ec.Format());
    }
  }

  string last_save_file = sf_.GetLastSaveInfo()->file_name;
  Load(last_save_file);
}

void DebugCmd::Replica(CmdArgList args) {
  args.remove_prefix(2);
  ToUpper(&args[0]);
  string_view opt = ArgS(args, 0);

  if (opt == "PAUSE" || opt == "RESUME") {
    sf_.PauseReplication(opt == "PAUSE");
    return (*cntx_)->SendOk();
  } else if (opt == "OFFSET") {
    const auto& offset_info = sf_.GetReplicaOffsetInfo();
    if (offset_info) {
      (*cntx_)->StartArray(2);
      (*cntx_)->SendBulkString(offset_info.value().sync_id);
      (*cntx_)->StartArray(offset_info.value().flow_offsets.size());
      for (uint64_t offset : offset_info.value().flow_offsets) {
        (*cntx_)->SendLong(offset);
      }
      return;
    } else {
      return (*cntx_)->SendError("I am master");
    }
  }
  return (*cntx_)->SendError(UnknownSubCmd("replica", "DEBUG"));
}

void DebugCmd::Load(string_view filename) {
  GlobalState new_state = sf_.service().SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  if (new_state != GlobalState::LOADING) {
    LOG(WARNING) << GlobalStateName(new_state) << " in progress, ignored";
    return;
  }

  absl::Cleanup rev_state = [this] {
    sf_.service().SwitchState(GlobalState::SAVING, GlobalState::ACTIVE);
  };

  const CommandId* cid = sf_.service().FindCmd("FLUSHALL");
  intrusive_ptr<Transaction> flush_trans(new Transaction{cid});
  flush_trans->InitByArgs(0, {});
  VLOG(1) << "Performing flush";
  error_code ec = sf_.Drakarys(flush_trans.get(), DbSlice::kDbAll);
  if (ec) {
    LOG(ERROR) << "Error flushing db " << ec.message();
  }

  fs::path path(filename);

  if (filename.empty()) {
    fs::path dir_path(GetFlag(FLAGS_dir));
    string filename = GetFlag(FLAGS_dbfilename);
    dir_path.append(filename);
    path = dir_path;
  }

  auto fut_ec = sf_.Load(path.generic_string());
  if (fut_ec.valid()) {
    ec = fut_ec.get();
    if (ec) {
      LOG(INFO) << "Could not load file " << ec.message();
      return (*cntx_)->SendError(ec.message());
    }
  }

  (*cntx_)->SendOk();
}

void DebugCmd::Populate(CmdArgList args) {
  if (args.size() < 3 || args.size() > 6) {
    return (*cntx_)->SendError(UnknownSubCmd("populate", "DEBUG"));
  }

  uint64_t total_count = 0;
  if (!absl::SimpleAtoi(ArgS(args, 2), &total_count))
    return (*cntx_)->SendError(kUintErr);
  std::string_view prefix{"key"};

  if (args.size() > 3) {
    prefix = ArgS(args, 3);
  }
  uint32_t val_size = 0;
  if (args.size() > 4) {
    std::string_view str = ArgS(args, 4);
    if (!absl::SimpleAtoi(str, &val_size))
      return (*cntx_)->SendError(kUintErr);
  }

  bool populate_random_values = false;
  if (args.size() > 5) {
    ToUpper(&args[5]);
    std::string_view str = ArgS(args, 5);
    if (str != "RAND") {
      return (*cntx_)->SendError(kSyntaxErr);
    }
    populate_random_values = true;
  }

  ProactorPool& pp = sf_.service().proactor_pool();
  size_t runners_count = pp.size();
  vector<pair<uint64_t, uint64_t>> ranges(runners_count - 1);
  uint64_t batch_size = total_count / runners_count;
  size_t from = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    ranges[i].first = from;
    ranges[i].second = batch_size;
    from += batch_size;
  }
  ranges.emplace_back(from, total_count - from);

  vector<fibers_ext::Fiber> fb_arr(ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    auto range = ranges[i];

    // whatever we do, we should not capture i by reference.
    fb_arr[i] = pp.at(i)->LaunchFiber([range, prefix, val_size, populate_random_values, this] {
      this->PopulateRangeFiber(range.first, range.second, prefix, val_size, populate_random_values);
    });
  }
  for (auto& fb : fb_arr)
    fb.Join();

  (*cntx_)->SendOk();
}

void DebugCmd::PopulateRangeFiber(uint64_t from, uint64_t len, std::string_view prefix,
                                  unsigned value_len, bool populate_random_values) {
  FiberProps::SetName("populate_range");
  VLOG(1) << "PopulateRange: " << from << "-" << (from + len - 1);

  string key = absl::StrCat(prefix, ":");
  size_t prefsize = key.size();
  DbIndex db_indx = cntx_->db_index();
  EngineShardSet& ess = *shard_set;
  std::vector<PopulateBatch> ps(ess.size(), PopulateBatch{db_indx});
  SetCmd::SetParams params;

  for (uint64_t i = from; i < from + len; ++i) {
    StrAppend(&key, i);
    ShardId sid = Shard(key, ess.size());
    key.resize(prefsize);  // shrink back

    auto& shard_batch = ps[sid];
    shard_batch.index[shard_batch.sz++] = i;
    if (shard_batch.sz == 32) {  // TODO what is this 32?
      ess.Add(sid, [=] {
        DoPopulateBatch(prefix, value_len, populate_random_values, params, shard_batch);
        if (i % 50 == 0) {
          fibers_ext::Yield();
        }
      });

      // we capture shard_batch by value so we can override it here.
      shard_batch.sz = 0;
    }
  }

  ess.RunBlockingInParallel([&](EngineShard* shard) {
    DoPopulateBatch(prefix, value_len, populate_random_values, params, ps[shard->shard_id()]);
  });
}

void DebugCmd::Inspect(string_view key) {
  EngineShardSet& ess = *shard_set;
  ShardId sid = Shard(key, ess.size());

  auto cb = [&]() -> ObjInfo {
    auto& db_slice = EngineShard::tlocal()->db_slice();
    auto [pt, exp_t] = db_slice.GetTables(cntx_->db_index());

    PrimeIterator it = pt->Find(key);
    ObjInfo oinfo;
    if (IsValid(it)) {
      const PrimeValue& pv = it->second;

      oinfo.found = true;
      oinfo.encoding = pv.Encoding();
      oinfo.bucket_id = it.bucket_id();
      oinfo.slot_id = it.slot_id();
      if (pv.IsExternal()) {
        oinfo.external_len.emplace(pv.GetExternalSlice().second);
      }

      if (pv.HasExpire()) {
        ExpireIterator exp_it = exp_t->Find(it->first);
        CHECK(!exp_it.is_done());

        time_t exp_time = db_slice.ExpireTime(exp_it);
        oinfo.ttl = exp_time - GetCurrentTimeMs();
        oinfo.has_sec_precision = exp_it->second.is_second_precision();
      }
    }

    KeyLockArgs lock_args;
    lock_args.args = ArgSlice{&key, 1};
    lock_args.db_index = cntx_->db_index();
    if (!db_slice.CheckLock(IntentLock::EXCLUSIVE, lock_args)) {
      oinfo.lock_status =
          db_slice.CheckLock(IntentLock::SHARED, lock_args) ? ObjInfo::S : ObjInfo::X;
    }

    return oinfo;
  };

  ObjInfo res = ess.Await(sid, cb);
  string resp;

  if (!res.found) {
    (*cntx_)->SendError(kKeyNotFoundErr);
    return;
  }

  StrAppend(&resp, "encoding:", strEncoding(res.encoding), " bucket_id:", res.bucket_id);
  StrAppend(&resp, " slot:", res.slot_id, " shard:", sid);

  if (res.ttl != INT64_MAX) {
    StrAppend(&resp, " ttl:", res.ttl, res.has_sec_precision ? "s" : "ms");
  }

  if (res.external_len) {
    StrAppend(&resp, " spill_len:", *res.external_len);
  }

  if (res.lock_status != ObjInfo::NONE) {
    StrAppend(&resp, " lock:", res.lock_status == ObjInfo::X ? "x" : "s");
  }
  (*cntx_)->SendSimpleString(resp);
}

void DebugCmd::Watched() {
  vector<string> watched_keys;
  boost::fibers::mutex mu;

  auto cb = [&](EngineShard* shard) {
    auto* bc = shard->blocking_controller();
    if (bc) {
      auto keys = bc->GetWatchedKeys(cntx_->db_index());

      lock_guard lk(mu);
      watched_keys.insert(watched_keys.end(), keys.begin(), keys.end());
    }
  };

  shard_set->RunBlockingInParallel(cb);
  (*cntx_)->SendStringArr(watched_keys);
}

}  // namespace dfly
