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
#include "server/server_state.h"
#include "server/transaction.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"

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

}  // namespace

ServerFamily::ServerFamily(Service* engine)
    : engine_(*engine), pp_(engine->proactor_pool()), ess_(engine->shard_set()) {
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

Metrics ServerFamily::GetMetrics() const {
  Metrics result;

  fibers::mutex mu;

  auto cb = [&](EngineShard* shard) {
    auto local_stats = shard->db_slice().GetStats();
    lock_guard<fibers::mutex> lk(mu);

    result.db += local_stats.db;
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
            << CI{"SHUTDOWN", CO::ADMIN | CO::NOSCRIPT | CO::LOADING | CO::STALE, 1, 0, 0, 0}.HFUNC(
                   _Shutdown);
}

}  // namespace dfly
