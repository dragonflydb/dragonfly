// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>  // for master_id_ generation.
#include <absl/strings/match.h>

#include <filesystem>

extern "C" {
#include "redis/redis_aux.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/debugcmd.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/rdb_save.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/uring/uring_file.h"

DEFINE_string(dir, "", "working directory");
DEFINE_string(dbfilename, "", "the filename to save/load the DB");

DECLARE_uint32(port);

namespace dfly {

using namespace std;
using namespace util;
namespace fibers = ::boost::fibers;

namespace fs = std::filesystem;

namespace {

using EngineFunc = void (ServerFamily::*)(CmdArgList args, ConnectionContext* cntx);

inline CommandId::Handler HandlerFunc(ServerFamily* se, EngineFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (se->*f)(args, cntx); };
}

using CI = CommandId;

// Create a direc
error_code CreateDirs(fs::path dir_path) {
  error_code ec;
  fs::file_status dir_status = fs::status(dir_path, ec);
  if (ec == errc::no_such_file_or_directory) {
    fs::create_directories(dir_path, ec);
    if (!ec)
      dir_status = fs::status(dir_path, ec);
  }
  return ec;
}
}  // namespace

ServerFamily::ServerFamily(Service* engine)
    : engine_(*engine), pp_(engine->proactor_pool()), ess_(engine->shard_set()) {
  last_save_.store(time(NULL), memory_order_release);
}

ServerFamily::~ServerFamily() {
}

void ServerFamily::Init(util::AcceptServer* acceptor) {
  CHECK(acceptor_ == nullptr);
  acceptor_ = acceptor;
}

void ServerFamily::Shutdown() {
  VLOG(1) << "ServerFamily::Shutdown";
}

void ServerFamily::DbSize(CmdArgList args, ConnectionContext* cntx) {
  atomic_ulong num_keys{0};

  ess_.RunBriefInParallel(
      [&](EngineShard* shard) {
        auto db_size = shard->db_slice().DbSize(cntx->conn_state.db_index);
        num_keys.fetch_add(db_size, memory_order_relaxed);
      },
      [](ShardId) { return true; });

  return cntx->SendLong(num_keys.load(memory_order_relaxed));
}

void ServerFamily::FlushDb(CmdArgList args, ConnectionContext* cntx) {
  DCHECK(cntx->transaction);
  Transaction* transaction = cntx->transaction;
  transaction->Schedule();  // TODO: to convert to ScheduleSingleHop ?

  transaction->Execute(
      [](Transaction* t, EngineShard* shard) {
        shard->db_slice().FlushDb(t->db_index());
        return OpStatus::OK;
      },
      true);

  cntx->SendOk();
}

void ServerFamily::FlushAll(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    cntx->SendError(kSyntaxErr);
    return;
  }

  DCHECK(cntx->transaction);
  Transaction* transaction = cntx->transaction;
  transaction->Schedule();

  transaction->Execute(
      [](Transaction* t, EngineShard* shard) {
        shard->db_slice().FlushDb(DbSlice::kDbAll);
        return OpStatus::OK;
      },
      true);

  cntx->SendOk();
}

void ServerFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);

  DebugCmd dbg_cmd{&ess_, cntx};

  return dbg_cmd.Run(args);
}

void ServerFamily::Save(CmdArgList args, ConnectionContext* cntx) {
  static unsigned fl_index = 1;

  auto [current, switched] = global_state_.Next(GlobalState::SAVING);
  if (!switched) {
    string error = absl::StrCat(GlobalState::Name(current), " - can not save database");
    return cntx->SendError(error);
  }

  absl::Cleanup rev_state = [this] {
    global_state_.Clear();
  };

  fs::path dir_path(FLAGS_dir);
  error_code ec;

  if (!dir_path.empty()) {
    ec = CreateDirs(dir_path);
    if (ec)
      return cntx->SendError(absl::StrCat("create dir ", ec.message()));
  }

  string filename = FLAGS_dbfilename.empty() ? "dump_save.rdb" : FLAGS_dbfilename;
  fs::path path = dir_path;
  path.append(filename);
  path.concat(absl::StrCat("_", fl_index++));
  VLOG(1) << "Saving to " << path;

  auto res = uring::OpenWrite(path.generic_string());
  if (!res) {
    cntx->SendError(res.error().message());
    return;
  }

  unique_ptr<::io::WriteFile> wf(*res);
  auto start = absl::Now();

  RdbSaver saver{&ess_, wf.get()};

  ec = saver.SaveHeader();
  if (!ec) {
    auto cb = [&saver](Transaction* t, EngineShard* shard) {
      saver.StartSnapshotInShard(shard);
      return OpStatus::OK;
    };
    cntx->transaction->ScheduleSingleHop(std::move(cb));

    // perform snapshot serialization, block the current fiber until it completes.
    ec = saver.SaveBody();
  }

  if (ec) {
    cntx->SendError(res.error().message());
    return;
  }

  absl::Duration dur = absl::Now() - start;
  double seconds = double(absl::ToInt64Milliseconds(dur)) / 1000;
  LOG(INFO) << "Saving " << path << " finished after "
            << strings::HumanReadableElapsedTime(seconds);

  auto close_ec = wf->Close();

  if (!ec)
    ec = close_ec;

  if (ec) {
    cntx->SendError(ec.message());
  } else {
    last_save_.store(time(NULL), memory_order_release);
    cntx->SendOk();
  }
}

Metrics ServerFamily::GetMetrics() const {
  Metrics result;

  fibers::mutex mu;

  auto cb = [&](EngineShard* shard) {
    auto local_stats = shard->db_slice().GetStats();
    lock_guard<fibers::mutex> lk(mu);

    result.db += local_stats.db;
    result.events += local_stats.events;
    result.conn_stats += *ServerState::tl_connection_stats();
  };

  ess_.RunBlockingInParallel(std::move(cb));

  return result;
}

void ServerFamily::Info(CmdArgList args, ConnectionContext* cntx) {
  const char kInfo1[] =
      R"(# Server
redis_version:6.2.0
redis_mode:standalone
arch_bits:64
multiplexing_api:iouring
atomicvar_api:atomic-builtin
tcp_port:)";

  string info = absl::StrCat(kInfo1, FLAGS_port, "\n");

  Metrics m = GetMetrics();
  absl::StrAppend(&info, "\n# Memory\n");
  absl::StrAppend(&info, "object_used_memory:", m.db.obj_memory_usage, "\n");
  absl::StrAppend(&info, "table_used_memory:", m.db.table_mem_usage, "\n");
  absl::StrAppend(&info, "used_memory_human:",
                  strings::HumanReadableNumBytes(m.db.table_mem_usage + m.db.obj_memory_usage),
                  "\n");

  absl::StrAppend(&info, "\n# Stats\n");
  absl::StrAppend(&info, "total_commands_processed:", m.conn_stats.command_cnt, "\n");
  absl::StrAppend(&info, "total_pipelined_commands:", m.conn_stats.pipelined_cmd_cnt, "\n");
  absl::StrAppend(&info, "total_reads_processed:", m.conn_stats.io_reads_cnt, "\n");

  absl::StrAppend(&info, "\n# Clients\n");
  absl::StrAppend(&info, "connected_clients:", m.conn_stats.num_conns, "\n");
  absl::StrAppend(&info, "client_read_buf_capacity:", m.conn_stats.read_buf_capacity, "\n");
  cntx->SendBulkString(info);
}

void ServerFamily::LastSave(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendLong(last_save_.load(memory_order_relaxed));
}

void ServerFamily::_Shutdown(CmdArgList args, ConnectionContext* cntx) {
  CHECK_NOTNULL(acceptor_)->Stop();
  cntx->SendOk();
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ServerFamily::x))

void ServerFamily::Register(CommandRegistry* registry) {
  *registry << CI{"DBSIZE", CO::READONLY | CO::FAST | CO::LOADING, 1, 0, 0, 0}.HFUNC(DbSize)
            << CI{"DEBUG", CO::RANDOM | CO::READONLY, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"FLUSHDB", CO::WRITE | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(FlushDb)
            << CI{"FLUSHALL", CO::WRITE | CO::GLOBAL_TRANS, -1, 0, 0, 0}.HFUNC(FlushAll)
            << CI{"INFO", CO::LOADING | CO::STALE, -1, 0, 0, 0}.HFUNC(Info)
            << CI{"LASTSAVE", CO::LOADING | CO::STALE | CO::RANDOM | CO::FAST, 1, 0, 0, 0}.HFUNC(
                   LastSave)
            << CI{"SAVE", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(Save)
            << CI{"SHUTDOWN", CO::ADMIN | CO::NOSCRIPT | CO::LOADING | CO::STALE, 1, 0, 0, 0}.HFUNC(
                   _Shutdown);
}

}  // namespace dfly
