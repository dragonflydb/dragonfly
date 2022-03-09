// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>  // for master_id_ generation.
#include <absl/strings/match.h>
#include <malloc.h>
#include <sys/resource.h>

#include <filesystem>

extern "C" {
#include "redis/redis_aux.h"
}

#include "base/logging.h"
#include "io/proc_reader.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/debugcmd.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/rdb_save.h"
#include "server/replica.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/uring/uring_file.h"

DEFINE_string(dir, "", "working directory");
DEFINE_string(dbfilename, "", "the filename to save/load the DB");
DEFINE_string(requirepass, "", "password for AUTH authentication");

DECLARE_uint32(port);

namespace dfly {

using namespace std;
using namespace util;
namespace fibers = ::boost::fibers;
namespace fs = std::filesystem;
using facade::MCReplyBuilder;
using strings::HumanReadableNumBytes;

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

ServerFamily::ServerFamily(Service* service) : service_(*service), ess_(service->shard_set()) {
  start_time_ = time(NULL);
  last_save_.store(start_time_, memory_order_release);
  script_mgr_.reset(new ScriptMgr(&service->shard_set()));
}

ServerFamily::~ServerFamily() {
}

void ServerFamily::Init(util::AcceptServer* acceptor) {
  CHECK(acceptor_ == nullptr);
  acceptor_ = acceptor;
}

void ServerFamily::Shutdown() {
  VLOG(1) << "ServerFamily::Shutdown";

  service_.proactor_pool().GetNextProactor()->Await([this] {
    unique_lock lk(replica_of_mu_);
    if (replica_) {
      replica_->Stop();
    }
  });
}

void ServerFamily::StatsMC(std::string_view section, facade::ConnectionContext* cntx) {
  if (!section.empty()) {
    return cntx->reply_builder()->SendError("");
  }
  string info;

#define ADD_LINE(name, val) absl::StrAppend(&info, "STAT " #name " ", val, "\r\n")

  time_t now = time(NULL);
  size_t uptime = now - start_time_;
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);

  auto dbl_time = [](const timeval& tv) -> double {
    return tv.tv_sec + double(tv.tv_usec) / 1000000.0;
  };

  double utime = dbl_time(ru.ru_utime);
  double systime = dbl_time(ru.ru_stime);

  Metrics m = GetMetrics();

  ADD_LINE(pid, getpid());
  ADD_LINE(uptime, uptime);
  ADD_LINE(time, now);
  ADD_LINE(version, "1.6.12");
  ADD_LINE(libevent, "iouring");
  ADD_LINE(pointer_size, sizeof(void*));
  ADD_LINE(rusage_user, utime);
  ADD_LINE(rusage_system, systime);
  ADD_LINE(max_connections, -1);
  ADD_LINE(curr_connections, m.conn_stats.num_conns);
  ADD_LINE(total_connections, -1);
  ADD_LINE(rejected_connections, -1);
  ADD_LINE(bytes_read, m.conn_stats.io_read_bytes);
  ADD_LINE(bytes_written, m.conn_stats.io_write_bytes);
  ADD_LINE(limit_maxbytes, -1);

  absl::StrAppend(&info, "END\r\n");

  MCReplyBuilder* builder = static_cast<MCReplyBuilder*>(cntx->reply_builder());
  builder->SendDirect(info);

#undef ADD_LINE
}

void ServerFamily::DbSize(CmdArgList args, ConnectionContext* cntx) {
  atomic_ulong num_keys{0};

  ess_.RunBriefInParallel(
      [&](EngineShard* shard) {
        auto db_size = shard->db_slice().DbSize(cntx->conn_state.db_index);
        num_keys.fetch_add(db_size, memory_order_relaxed);
      },
      [](ShardId) { return true; });

  return (*cntx)->SendLong(num_keys.load(memory_order_relaxed));
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

  cntx->reply_builder()->SendOk();
}

void ServerFamily::FlushAll(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    (*cntx)->SendError(kSyntaxErr);
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

  (*cntx)->SendOk();
}

void ServerFamily::Auth(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 3) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  if (args.size() == 3) {
    return (*cntx)->SendError("ACL is not supported yet");
  }

  if (!cntx->req_auth) {
    return (*cntx)->SendError(
        "AUTH <password> called without any password configured for the "
        "default user. Are you sure your configuration is correct?");
  }

  string_view pass = ArgS(args, 1);
  if (pass == FLAGS_requirepass) {
    cntx->authenticated = true;
    (*cntx)->SendOk();
  } else {
    (*cntx)->SendError(facade::kAuthRejected);
  }
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
    return (*cntx)->SendError(error);
  }

  absl::Cleanup rev_state = [this] { global_state_.Clear(); };

  fs::path dir_path(FLAGS_dir);
  error_code ec;

  if (!dir_path.empty()) {
    ec = CreateDirs(dir_path);
    if (ec)
      return (*cntx)->SendError(absl::StrCat("create dir ", ec.message()));
  }

  string filename = FLAGS_dbfilename.empty() ? "dump_save.rdb" : FLAGS_dbfilename;
  fs::path path = dir_path;
  path.append(filename);
  path.concat(absl::StrCat("_", fl_index++));
  VLOG(1) << "Saving to " << path;

  auto res = uring::OpenWrite(path.generic_string());
  if (!res) {
    (*cntx)->SendError(res.error().message());
    return;
  }

  auto& pool = service_.proactor_pool();
  pool.Await([](auto*) { ServerState::tlocal()->set_gstate(GlobalState::SAVING); });

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
    (*cntx)->SendError(res.error().message());
    return;
  }

  pool.Await([](auto*) { ServerState::tlocal()->set_gstate(GlobalState::IDLE); });
  CHECK_EQ(GlobalState::SAVING, global_state_.Clear());

  absl::Duration dur = absl::Now() - start;
  double seconds = double(absl::ToInt64Milliseconds(dur)) / 1000;
  LOG(INFO) << "Saving " << path << " finished after "
            << strings::HumanReadableElapsedTime(seconds);

  auto close_ec = wf->Close();

  if (!ec)
    ec = close_ec;

  if (ec) {
    (*cntx)->SendError(ec.message());
  } else {
    last_save_.store(time(NULL), memory_order_release);
    (*cntx)->SendOk();
  }
}

Metrics ServerFamily::GetMetrics() const {
  Metrics result;

  fibers::mutex mu;

  auto cb = [&](ProactorBase* pb) {
    EngineShard* shard = EngineShard::tlocal();
    ServerState* ss = ServerState::tlocal();

    lock_guard<fibers::mutex> lk(mu);

    result.conn_stats += ss->connection_stats;
    result.qps += uint64_t(ss->MovingSum6());

    if (shard) {
      auto db_stats = shard->db_slice().GetStats();
      result.db += db_stats.db;
      result.events += db_stats.events;

      EngineShard::Stats shard_stats = shard->stats();
      result.heap_comitted_bytes += shard_stats.heap_comitted_bytes;
      result.heap_used_bytes += shard_stats.heap_used_bytes;
    }
  };

  service_.proactor_pool().AwaitFiberOnAll(std::move(cb));
  result.qps /= 6;

  return result;
}

void ServerFamily::Info(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 2) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  string_view section;

  if (args.size() == 2) {
    ToUpper(&args[1]);
    section = ArgS(args, 1);
  }

  string info;

  const char kInfo1[] =
      R"(# Server
redis_version:1.9.9
redis_mode:standalone
arch_bits:64
multiplexing_api:iouring
tcp_port:)";

  auto should_enter = [&](string_view name, bool hidden = false) {
    bool res = (!hidden && section.empty()) || section == "ALL" || section == name;
    if (res && !info.empty())
      info.push_back('\n');

    return res;
  };

  if (should_enter("SERVER")) {
    absl::StrAppend(&info, kInfo1, FLAGS_port, "\n");

    size_t uptime = time(NULL) - start_time_;
    absl::StrAppend(&info, "uptime_in_seconds:", uptime, "\n");
    absl::StrAppend(&info, "uptime_in_days:", uptime / (3600 * 24), "\n");
  }

  Metrics m = GetMetrics();
  auto sdata_res = io::ReadStatusInfo();

  if (should_enter("CLIENTS")) {
    absl::StrAppend(&info, "# Clients\n");
    absl::StrAppend(&info, "connected_clients:", m.conn_stats.num_conns, "\n");
    absl::StrAppend(&info, "client_read_buf_capacity:", m.conn_stats.read_buf_capacity, "\n");
    absl::StrAppend(&info, "blocked_clients:", 0, "\n");
  }

  if (should_enter("MEMORY")) {
    absl::StrAppend(&info, "# Memory\n");

    absl::StrAppend(&info, "used_memory:", m.heap_used_bytes, "\n");
    absl::StrAppend(&info, "used_memory_human:", HumanReadableNumBytes(m.heap_used_bytes), "\n");
    absl::StrAppend(&info, "comitted_memory:", m.heap_comitted_bytes, "\n");

    if (sdata_res.has_value()) {
      absl::StrAppend(&info, "used_memory_rss:", sdata_res->vm_rss, "\n");
      absl::StrAppend(&info, "used_memory_rss_human:", HumanReadableNumBytes(sdata_res->vm_rss),
                      "\n");
    } else {
      LOG(ERROR) << "Error fetching /proc/self/status stats";
    }

    // TBD: should be the max of all seen used_memory values.
    absl::StrAppend(&info, "used_memory_peak:", -1, "\n");

    // Blob - all these cases where the key/objects are represented by a single blob allocated on
    // heap. For example, strings or intsets. members of lists, sets, zsets etc
    // are not accounted for to avoid complex computations. In some cases, when number of members is
    // known we approximate their allocations by taking 16 bytes per member.
    absl::StrAppend(&info, "blob_used_memory:", m.db.obj_memory_usage, "\n");
    absl::StrAppend(&info, "table_used_memory:", m.db.table_mem_usage, "\n");
    absl::StrAppend(&info, "num_buckets:", m.db.bucket_count, "\n");
    absl::StrAppend(&info, "num_entries:", m.db.key_count, "\n");
    absl::StrAppend(&info, "inline_keys:", m.db.inline_keys, "\n");
    absl::StrAppend(&info, "small_string_bytes:", m.db.small_string_bytes, "\n");
    absl::StrAppend(&info, "listpack_blobs:", m.db.listpack_blob_cnt, "\n");
    absl::StrAppend(&info, "listpack_bytes:", m.db.listpack_bytes, "\n");
  }

  if (should_enter("STATS")) {
    absl::StrAppend(&info, "# Stats\n");
    absl::StrAppend(&info, "instantaneous_ops_per_sec:", m.qps, "\n");
    absl::StrAppend(&info, "total_commands_processed:", m.conn_stats.command_cnt, "\n");
    absl::StrAppend(&info, "total_pipelined_commands:", m.conn_stats.pipelined_cmd_cnt, "\n");
    absl::StrAppend(&info, "total_net_input_bytes:", m.conn_stats.io_read_bytes, "\n");
    absl::StrAppend(&info, "total_net_output_bytes:", m.conn_stats.io_write_bytes, "\n");
    absl::StrAppend(&info, "instantaneous_input_kbps:", -1, "\n");
    absl::StrAppend(&info, "instantaneous_output_kbps:", -1, "\n");
    absl::StrAppend(&info, "rejected_connections:", -1, "\n");
    absl::StrAppend(&info, "expired_keys:", m.events.expired_keys, "\n");
    absl::StrAppend(&info, "keyspace_hits:", -1, "\n");
    absl::StrAppend(&info, "keyspace_misses:", -1, "\n");
    absl::StrAppend(&info, "total_reads_processed:", m.conn_stats.io_read_cnt, "\n");
    absl::StrAppend(&info, "total_writes_processed:", m.conn_stats.io_write_cnt, "\n");
  }

  if (should_enter("REPLICATION")) {
    absl::StrAppend(&info, "# Replication\n");

    ServerState& etl = *ServerState::tlocal();

    if (etl.is_master) {
      absl::StrAppend(&info, "role:master\n");
      absl::StrAppend(&info, "connected_slaves:", m.conn_stats.num_replicas, "\n");
    } else {
      absl::StrAppend(&info, "role:slave\n");

      // it's safe to access replica_ because replica_ is created before etl.is_master set to false
      // and cleared after etl.is_master is set to true. And since the code here that checks for
      // is_master and copies shared_ptr is atomic, it1 should be correct.
      auto replica_ptr = replica_;
      Replica::Info rinfo = replica_ptr->GetInfo();
      absl::StrAppend(&info, "master_host:", rinfo.host, "\n");
      absl::StrAppend(&info, "master_port:", rinfo.port, "\n");

      const char* link = rinfo.master_link_established ? "up" : "down";
      absl::StrAppend(&info, "master_link_status:", link, "\n");
      absl::StrAppend(&info, "master_last_io_seconds_ago:", rinfo.master_last_io_sec, "\n");
      absl::StrAppend(&info, "master_sync_in_progress:", rinfo.sync_in_progress, "\n");
    }
  }

  if (should_enter("COMMANDSTATS", true)) {
    absl::StrAppend(&info, "# Commandstats\n");
    auto unknown_cmd = service_.UknownCmdMap();

    for (const auto& k_v : unknown_cmd) {
      absl::StrAppend(&info, "unknown_", k_v.first, ":", k_v.second, "\n");
    }

    for (const auto& k_v : m.conn_stats.cmd_count) {
      absl::StrAppend(&info, "cmd_", k_v.first, ":", k_v.second, "\n");
    }
  }

  if (should_enter("ERRORSTATS", true)) {
    absl::StrAppend(&info, "# Errorstats\n");
    for (const auto& k_v : m.conn_stats.err_count) {
      absl::StrAppend(&info, k_v.first, ":", k_v.second, "\n");
    }
  }

  if (should_enter("KEYSPACE")) {
    absl::StrAppend(&info, "# Keyspace\n");
    absl::StrAppend(&info, "db0:keys=xxx,expires=yyy,avg_ttl=zzz\n");  // TODO
  }

  (*cntx)->SendBulkString(info);
}

void ServerFamily::ReplicaOf(CmdArgList args, ConnectionContext* cntx) {
  std::string_view host = ArgS(args, 1);
  std::string_view port_s = ArgS(args, 2);
  auto& pool = service_.proactor_pool();

  if (absl::EqualsIgnoreCase(host, "no") && absl::EqualsIgnoreCase(port_s, "one")) {
    // use this lock as critical section to prevent concurrent replicaof commands running.
    unique_lock lk(replica_of_mu_);

    // Switch to primary mode.
    if (!ServerState::tlocal()->is_master) {
      auto repl_ptr = replica_;
      CHECK(repl_ptr);

      pool.AwaitFiberOnAll(
          [&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = true; });
      replica_->Stop();
      replica_.reset();
    }

    return (*cntx)->SendOk();
  }

  uint32_t port;

  if (!absl::SimpleAtoi(port_s, &port) || port < 1 || port > 65535) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto new_replica = make_shared<Replica>(string(host), port, &service_);

  unique_lock lk(replica_of_mu_);
  if (replica_) {
    replica_->Stop();  // NOTE: consider introducing update API flow.
  }

  replica_.swap(new_replica);

  // Flushing all the data after we marked this instance as replica.
  Transaction* transaction = cntx->transaction;
  transaction->Schedule();

  auto cb = [](Transaction* t, EngineShard* shard) {
    shard->db_slice().FlushDb(DbSlice::kDbAll);
    return OpStatus::OK;
  };
  transaction->Execute(std::move(cb), true);

  // Replica sends response in either case. No need to send response in this function.
  // It's a bit confusing but simpler.
  if (!replica_->Run(cntx)) {
    replica_.reset();
  }

  bool is_master = !replica_;
  pool.AwaitFiberOnAll(
      [&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = is_master; });
}

void ServerFamily::Role(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendDirect("*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");
}

void ServerFamily::Script(CmdArgList args, ConnectionContext* cntx) {
  args.remove_prefix(1);
  ToUpper(&args.front());

  script_mgr_->Run(std::move(args), cntx);
}

void ServerFamily::Sync(CmdArgList args, ConnectionContext* cntx) {
  SyncGeneric("", 0, cntx);
}

void ServerFamily::Psync(CmdArgList args, ConnectionContext* cntx) {
  SyncGeneric("?", 0, cntx);  // full sync, ignore the request.
}
void ServerFamily::LastSave(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(last_save_.load(memory_order_relaxed));
}

void ServerFamily::_Shutdown(CmdArgList args, ConnectionContext* cntx) {
  CHECK_NOTNULL(acceptor_)->Stop();
  (*cntx)->SendOk();
}

void ServerFamily::SyncGeneric(std::string_view repl_master_id, uint64_t offs,
                               ConnectionContext* cntx) {
  if (cntx->async_dispatch) {
    // SYNC is a special command that should not be sent in batch with other commands.
    // It should be the last command since afterwards the server just dumps the replication data.
    (*cntx)->SendError("Can not sync in pipeline mode");
    return;
  }

  cntx->replica_conn = true;
  ServerState::tl_connection_stats()->num_replicas += 1;
  // TBD.
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ServerFamily::x))

void ServerFamily::Register(CommandRegistry* registry) {
  constexpr auto kReplicaOpts = CO::ADMIN | CO::GLOBAL_TRANS;
  *registry << CI{"AUTH", CO::NOSCRIPT | CO::FAST | CO::LOADING, -2, 0, 0, 0}.HFUNC(Auth)
            << CI{"BGSAVE", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(Save)
            << CI{"DBSIZE", CO::READONLY | CO::FAST | CO::LOADING, 1, 0, 0, 0}.HFUNC(DbSize)
            << CI{"DEBUG", CO::RANDOM | CO::ADMIN | CO::LOADING, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"FLUSHDB", CO::WRITE | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(FlushDb)
            << CI{"FLUSHALL", CO::WRITE | CO::GLOBAL_TRANS, -1, 0, 0, 0}.HFUNC(FlushAll)
            << CI{"INFO", CO::LOADING, -1, 0, 0, 0}.HFUNC(Info)
            << CI{"LASTSAVE", CO::LOADING | CO::RANDOM | CO::FAST, 1, 0, 0, 0}.HFUNC(LastSave)
            << CI{"SAVE", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(Save)
            << CI{"SHUTDOWN", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0}.HFUNC(_Shutdown)
            << CI{"SLAVEOF", kReplicaOpts, 3, 0, 0, 0}.HFUNC(ReplicaOf)
            << CI{"REPLICAOF", kReplicaOpts, 3, 0, 0, 0}.HFUNC(ReplicaOf)
            << CI{"ROLE", CO::LOADING | CO::FAST | CO::NOSCRIPT, 1, 0, 0, 0}.HFUNC(Role)
            << CI{"SYNC", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(Sync)
            << CI{"PSYNC", CO::ADMIN | CO::GLOBAL_TRANS, 3, 0, 0, 0}.HFUNC(Psync)
            << CI{"SCRIPT", CO::NOSCRIPT, -2, 0, 0, 0}.HFUNC(Script);
}

}  // namespace dfly
