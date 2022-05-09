// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>  // for master_id_ generation.
#include <absl/strings/match.h>
#include <mimalloc-types.h>
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
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/uring/uring_file.h"

DEFINE_string(dir, "", "working directory");
DEFINE_string(dbfilename, "", "the filename to save/load the DB");
DEFINE_string(requirepass, "", "password for AUTH authentication");

DECLARE_uint32(port);

extern "C" mi_stats_t _mi_stats_main;

namespace dfly {

using namespace std;
using namespace util;
namespace fibers = ::boost::fibers;
namespace fs = std::filesystem;
using absl::StrCat;
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
  last_save_ = start_time_;
  script_mgr_.reset(new ScriptMgr(&service->shard_set()));
}

ServerFamily::~ServerFamily() {
}

void ServerFamily::Init(util::AcceptServer* acceptor) {
  CHECK(acceptor_ == nullptr);
  acceptor_ = acceptor;

  pb_task_ = ess_.pool()->GetNextProactor();
  auto cache_cb = [] {
    uint64_t sum = 0;
    const auto& stats = EngineShardSet::GetCachedStats();
    for (const auto& s : stats)
      sum += s.used_memory.load(memory_order_relaxed);

    used_mem_current.store(sum, memory_order_relaxed);

    // Single writer, so no races.
    if (sum > used_mem_peak.load(memory_order_relaxed))
      used_mem_peak.store(sum, memory_order_relaxed);
  };

  task_10ms_ = pb_task_->AwaitBrief([&] { return pb_task_->AddPeriodic(10, cache_cb); });

  fs::path data_path = fs::current_path();

  if (!FLAGS_dir.empty()) {
    data_path = FLAGS_dir;

    error_code ec;

    data_path = fs::canonical(data_path, ec);
  }

  LOG(INFO) << "Data directory is " << data_path;
}

void ServerFamily::Shutdown() {
  VLOG(1) << "ServerFamily::Shutdown";

  pb_task_->Await([this] {
    pb_task_->CancelPeriodic(task_10ms_);
    task_10ms_ = 0;

    unique_lock lk(replicaof_mu_);
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
  builder->SendRaw(info);

#undef ADD_LINE
}

error_code ServerFamily::DoSave(Transaction* trans, string* err_details) {
  static unsigned fl_index = 1;

  auto [current, switched] = global_state_.Next(GlobalState::SAVING);
  if (!switched) {
    *err_details = StrCat(GlobalState::Name(current), " - can not save database");
    return make_error_code(errc::operation_in_progress);
  }

  absl::Cleanup rev_state = [this] { global_state_.Clear(); };

  fs::path dir_path(FLAGS_dir);
  error_code ec;

  if (!dir_path.empty()) {
    ec = CreateDirs(dir_path);
    if (ec) {
      *err_details = "create-dir ";
      return ec;
    }
  }

  string filename = FLAGS_dbfilename.empty() ? "dump_save.rdb" : FLAGS_dbfilename;
  fs::path path = dir_path;
  path.append(filename);
  path.concat(StrCat("_", fl_index++));
  VLOG(1) << "Saving to " << path;

  auto res = uring::OpenWrite(path.generic_string());
  if (!res) {
    return res.error();
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
    trans->ScheduleSingleHop(std::move(cb));

    // perform snapshot serialization, block the current fiber until it completes.
    RdbTypeFreqMap freq_map;
    ec = saver.SaveBody(&freq_map);

    // TODO: needs protection from reads.
    last_save_freq_map_.clear();
    for (const auto& k_v : freq_map) {
      last_save_freq_map_.push_back(k_v);
    }
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

  if (!ec) {
    lock_guard lk(save_mu_);
    last_save_ = time(NULL);
    last_save_file_ = path.generic_string();
  }

  return ec;
}

error_code ServerFamily::DoFlush(Transaction* transaction, DbIndex db_ind) {
  transaction->Schedule();  // TODO: to convert to ScheduleSingleHop ?

  transaction->Execute(
      [db_ind](Transaction* t, EngineShard* shard) {
        shard->db_slice().FlushDb(db_ind);
        return OpStatus::OK;
      },
      true);

  return error_code{};
}

string ServerFamily::LastSaveFile() const {
  lock_guard lk(save_mu_);
  return last_save_file_;
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
  DoFlush(cntx->transaction, cntx->transaction->db_index());
  cntx->reply_builder()->SendOk();
}

void ServerFamily::FlushAll(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  DCHECK(cntx->transaction);
  DoFlush(cntx->transaction, DbSlice::kDbAll);
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

void ServerFamily::Client(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "SETNAME") {
    return (*cntx)->SendOk();
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  (*cntx)->SendError(kSyntaxErr);
}

void ServerFamily::Config(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "SET") {
    return (*cntx)->SendOk();
  } else if (sub_cmd == "GET" && args.size() == 3) {
    string_view param = ArgS(args, 2);
    string_view res[2] = {param, "tbd"};

    return (*cntx)->SendStringArr(res);
  } else if (sub_cmd == "RESETSTAT") {
    ess_.pool()->Await([](auto*) {
      auto* stats = ServerState::tl_connection_stats();
      stats->cmd_count_map.clear();
      stats->err_count_map.clear();
      stats->command_cnt = 0;
      stats->async_writes_cnt = 0;
    });
    return (*cntx)->SendOk();
  } else {
    string err = StrCat("Unknown subcommand or wrong number of arguments for '", sub_cmd,
                        "'. Try CONFIG HELP.");
    return (*cntx)->SendError(err, kSyntaxErr);
  }
}

void ServerFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);

  DebugCmd dbg_cmd{this, cntx};

  return dbg_cmd.Run(args);
}

void ServerFamily::Memory(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);
  if (sub_cmd == "USAGE") {
    return (*cntx)->SendLong(1);
  }

  string err = StrCat("Unknown subcommand or wrong number of arguments for '", sub_cmd,
                      "'. Try MEMORY HELP.");
  return (*cntx)->SendError(err, kSyntaxErr);
}

void ServerFamily::Save(CmdArgList args, ConnectionContext* cntx) {
  string err_detail;

  error_code ec = DoSave(cntx->transaction, &err_detail);

  if (ec) {
    (*cntx)->SendError(absl::StrCat(err_detail, ec.message()));
  } else {
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

      result.heap_used_bytes += shard->UsedMemory();
      if (shard->tiered_storage()) {
        result.tiered_stats += shard->tiered_storage()->GetStats();
      }
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
      info.append("\r\n");

    return res;
  };

  auto append = [&info](absl::AlphaNum a1, absl::AlphaNum a2) {
    absl::StrAppend(&info, a1, a2, "\r\n");
  };

#define ADD_HEADER(x) absl::StrAppend(&info, x "\r\n")

  if (should_enter("SERVER")) {
    append(kInfo1, FLAGS_port);

    size_t uptime = time(NULL) - start_time_;
    append("uptime_in_seconds:", uptime);
    append("uptime_in_days:", uptime / (3600 * 24));
  }

  Metrics m = GetMetrics();
  auto sdata_res = io::ReadStatusInfo();

  if (should_enter("CLIENTS")) {
    ADD_HEADER("# Clients");
    append("connected_clients:", m.conn_stats.num_conns);
    append("client_read_buf_capacity:", m.conn_stats.read_buf_capacity);
    append("blocked_clients:", m.conn_stats.num_blocked_clients);
  }

  if (should_enter("MEMORY")) {
    ADD_HEADER("# Memory");

    append("used_memory:", m.heap_used_bytes);
    append("used_memory_human:", HumanReadableNumBytes(m.heap_used_bytes));
    append("used_memory_peak:", used_mem_peak.load(memory_order_relaxed));

    append("comitted_memory:", _mi_stats_main.committed.current);

    if (sdata_res.has_value()) {
      append("used_memory_rss:", sdata_res->vm_rss);
      append("used_memory_rss_human:", HumanReadableNumBytes(sdata_res->vm_rss));
    } else {
      LOG(ERROR) << "Error fetching /proc/self/status stats";
    }

    // Blob - all these cases where the key/objects are represented by a single blob allocated on
    // heap. For example, strings or intsets. members of lists, sets, zsets etc
    // are not accounted for to avoid complex computations. In some cases, when number of members
    // is known we approximate their allocations by taking 16 bytes per member.
    append("blob_used_memory:", m.db.obj_memory_usage);
    append("table_used_memory:", m.db.table_mem_usage);
    append("num_buckets:", m.db.bucket_count);
    append("num_entries:", m.db.key_count);
    append("inline_keys:", m.db.inline_keys);
    append("small_string_bytes:", m.db.small_string_bytes);
    append("strval_bytes:", m.db.strval_memory_usage);
    append("listpack_blobs:", m.db.listpack_blob_cnt);
    append("listpack_bytes:", m.db.listpack_bytes);
  }

  if (should_enter("STATS")) {
    ADD_HEADER("# Stats");

    append("instantaneous_ops_per_sec:", m.qps);
    append("total_commands_processed:", m.conn_stats.command_cnt);
    append("total_pipelined_commands:", m.conn_stats.pipelined_cmd_cnt);
    append("total_net_input_bytes:", m.conn_stats.io_read_bytes);
    append("total_net_output_bytes:", m.conn_stats.io_write_bytes);
    append("instantaneous_input_kbps:", -1);
    append("instantaneous_output_kbps:", -1);
    append("rejected_connections:", -1);
    append("expired_keys:", m.events.expired_keys);
    append("gc_keys:", m.events.garbage_collected);
    append("keyspace_hits:", -1);
    append("keyspace_misses:", -1);
    append("total_reads_processed:", m.conn_stats.io_read_cnt);
    append("total_writes_processed:", m.conn_stats.io_write_cnt);
    append("async_writes_count:", m.conn_stats.async_writes_cnt);
  }

  if (should_enter("TIERED", true)) {
    ADD_HEADER("# TIERED");
    append("external_entries:", m.db.external_entries);
    append("external_bytes:", m.db.external_size);
    append("external_reads:", m.tiered_stats.external_reads);
    append("external_writes:", m.tiered_stats.external_writes);
    append("external_reserved:", m.tiered_stats.storage_reserved);
    append("external_capacity:", m.tiered_stats.storage_capacity);
  }

  if (should_enter("REPLICATION")) {
    ADD_HEADER("# Replication");

    ServerState& etl = *ServerState::tlocal();

    if (etl.is_master) {
      append("role:", "master");
      append("connected_slaves:", m.conn_stats.num_replicas);
    } else {
      append("role:", "slave");

      // it's safe to access replica_ because replica_ is created before etl.is_master set to
      // false and cleared after etl.is_master is set to true. And since the code here that checks
      // for is_master and copies shared_ptr is atomic, it1 should be correct.
      auto replica_ptr = replica_;
      Replica::Info rinfo = replica_ptr->GetInfo();
      append("master_host:", rinfo.host);
      append("master_port:", rinfo.port);

      const char* link = rinfo.master_link_established ? "up" : "down";
      append("master_link_status:", link);
      append("master_last_io_seconds_ago:", rinfo.master_last_io_sec);
      append("master_sync_in_progress:", rinfo.sync_in_progress);
    }
  }

  if (should_enter("COMMANDSTATS", true)) {
    ADD_HEADER("# Commandstats");

    auto unknown_cmd = service_.UknownCmdMap();

    for (const auto& k_v : unknown_cmd) {
      append(StrCat("unknown_", k_v.first, ":"), k_v.second);
    }

    for (const auto& k_v : m.conn_stats.cmd_count_map) {
      append(StrCat("cmd_", k_v.first, ":"), k_v.second);
    }
  }

  if (should_enter("ERRORSTATS", true)) {
    ADD_HEADER("# Errorstats");
    for (const auto& k_v : m.conn_stats.err_count_map) {
      append(StrCat(k_v.first, ":"), k_v.second);
    }
  }

  if (should_enter("KEYSPACE")) {
    ADD_HEADER("# Keyspace");
    append("db0:", "keys=xxx,expires=yyy,avg_ttl=zzz");  // TODO
  }

  (*cntx)->SendBulkString(info);
}

void ServerFamily::Hello(CmdArgList args, ConnectionContext* cntx) {
  return (*cntx)->SendOk();
}

void ServerFamily::ReplicaOf(CmdArgList args, ConnectionContext* cntx) {
  std::string_view host = ArgS(args, 1);
  std::string_view port_s = ArgS(args, 2);
  auto& pool = service_.proactor_pool();

  if (absl::EqualsIgnoreCase(host, "no") && absl::EqualsIgnoreCase(port_s, "one")) {
    // use this lock as critical section to prevent concurrent replicaof commands running.
    unique_lock lk(replicaof_mu_);

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

  unique_lock lk(replicaof_mu_);
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
  (*cntx)->SendRaw("*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");
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
  lock_guard lk(save_mu_);
  (*cntx)->SendLong(last_save_);
}

void ServerFamily::Latency(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "LATEST") {
    return (*cntx)->StartArray(0);
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  (*cntx)->SendError(kSyntaxErr);
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
  constexpr auto kMemOpts = CO::LOADING | CO::READONLY | CO::FAST | CO::NOSCRIPT;

  *registry << CI{"AUTH", CO::NOSCRIPT | CO::FAST | CO::LOADING, -2, 0, 0, 0}.HFUNC(Auth)
            << CI{"BGSAVE", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(Save)
            << CI{"CLIENT", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, 0}.HFUNC(Client)
            << CI{"CONFIG", CO::ADMIN, -2, 0, 0, 0}.HFUNC(Config)
            << CI{"DBSIZE", CO::READONLY | CO::FAST | CO::LOADING, 1, 0, 0, 0}.HFUNC(DbSize)
            << CI{"DEBUG", CO::RANDOM | CO::ADMIN | CO::LOADING, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"FLUSHDB", CO::WRITE | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(FlushDb)
            << CI{"FLUSHALL", CO::WRITE | CO::GLOBAL_TRANS, -1, 0, 0, 0}.HFUNC(FlushAll)
            << CI{"INFO", CO::LOADING, -1, 0, 0, 0}.HFUNC(Info)
            << CI{"HELLO", CO::LOADING, -1, 0, 0, 0}.HFUNC(Hello)
            << CI{"LASTSAVE", CO::LOADING | CO::RANDOM | CO::FAST, 1, 0, 0, 0}.HFUNC(LastSave)
            << CI{"LATENCY", CO::NOSCRIPT | CO::LOADING | CO::RANDOM | CO::FAST, -2, 0, 0, 0}.HFUNC(
                   Latency)
            << CI{"MEMORY", kMemOpts, -2, 0, 0, 0}.HFUNC(Memory)
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
