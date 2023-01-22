// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>  // for master_id_ generation.
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>

extern "C" {
#include "redis/redis_aux.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "io/file_util.h"
#include "io/proc_reader.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/debugcmd.h"
#include "server/dflycmd.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/main_service.h"
#include "server/memory_cmd.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/replica.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "server/version.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/fibers/fiber_file.h"
#include "util/uring/uring_file.h"

using namespace std;

ABSL_FLAG(string, dir, "", "working directory");
ABSL_FLAG(string, dbfilename, "dump", "the filename to save/load the DB");
ABSL_FLAG(string, requirepass, "", "password for AUTH authentication");
ABSL_FLAG(string, save_schedule, "",
          "glob spec for the UTC time to save a snapshot which matches HH:MM 24h time");

ABSL_DECLARE_FLAG(uint32_t, port);
ABSL_DECLARE_FLAG(bool, cache_mode);
ABSL_DECLARE_FLAG(uint32_t, hz);

namespace dfly {

namespace fibers = ::boost::fibers;
namespace fs = std::filesystem;
namespace uring = util::uring;

using absl::GetFlag;
using absl::StrCat;
using namespace facade;
using strings::HumanReadableNumBytes;
using util::ProactorBase;
using util::fibers_ext::FiberQueueThreadPool;
using util::http::StringResponse;

namespace {

const auto kRdbWriteFlags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_DIRECT;

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

string UnknownCmd(string cmd, CmdArgList args) {
  return absl::StrCat("unknown command '", cmd, "' with args beginning with: ",
                      StrJoin(args.begin(), args.end(), ", ", CmdArgListFormatter()));
}

string InferLoadFile(fs::path data_dir) {
  const auto& dbname = GetFlag(FLAGS_dbfilename);
  if (dbname.empty())
    return string{};

  fs::path fl_path = data_dir.append(dbname);
  if (fs::exists(fl_path))
    return fl_path.generic_string();

  if (!fl_path.has_extension()) {
    std::string glob = absl::StrCat(fl_path.generic_string(), "*");
    io::Result<io::StatShortVec> short_vec = io::StatFiles(glob);

    if (short_vec) {
      auto it = std::find_if(short_vec->rbegin(), short_vec->rend(), [](const auto& stat) {
        return absl::EndsWith(stat.name, ".rdb") || absl::EndsWith(stat.name, "summary.dfs");
      });
      if (it != short_vec->rend())
        return it->name;
    } else {
      LOG(WARNING) << "Could not stat " << glob << ", error " << short_vec.error().message();
    }
  }
  return string{};
}

bool IsValidSaveScheduleNibble(string_view time, unsigned int max) {
  /*
   * a nibble is valid iff there exists one time that matches the pattern
   * and that time is <= max. For any wildcard the minimum value is 0.
   * Therefore the minimum time the pattern can match is the time with
   * all *s replaced with 0s. If this time is > max all other times that
   * match the pattern are > max and the pattern is invalid. Otherwise
   * there exists at least one valid nibble specified by this pattern
   *
   * Note the edge case of "*" is equivalent to "**". While using this
   * approach "*" and "**" both map to 0.
   */
  unsigned int min_match = 0;
  for (size_t i = 0; i < time.size(); ++i) {
    // check for valid characters
    if (time[i] != '*' && (time[i] < '0' || time[i] > '9')) {
      return false;
    }
    min_match *= 10;
    min_match += time[i] == '*' ? 0 : time[i] - '0';
  }

  return min_match <= max;
}

// takes ownership over the file.
class LinuxWriteWrapper : public io::Sink {
 public:
  LinuxWriteWrapper(uring::LinuxFile* lf) : lf_(lf) {
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  std::error_code Close() {
    return lf_->Close();
  }

 private:
  std::unique_ptr<uring::LinuxFile> lf_;
  off_t offset_ = 0;
};

class RdbSnapshot {
 public:
  RdbSnapshot(FiberQueueThreadPool* fq_tp) : fq_tp_(fq_tp) {
  }

  error_code Start(SaveMode save_mode, const std::string& path, const StringVec& lua_scripts);
  void StartInShard(EngineShard* shard);

  error_code SaveBody();
  error_code Close();

  const RdbTypeFreqMap freq_map() const {
    return freq_map_;
  }

  bool HasStarted() const {
    return started_ || (saver_ && saver_->Mode() == SaveMode::SUMMARY);
  }

 private:
  bool started_ = false;
  FiberQueueThreadPool* fq_tp_;
  std::unique_ptr<io::Sink> io_sink_;
  std::unique_ptr<RdbSaver> saver_;
  RdbTypeFreqMap freq_map_;

  Cancellation cll_{};
};

io::Result<size_t> LinuxWriteWrapper::WriteSome(const iovec* v, uint32_t len) {
  io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
  if (res) {
    offset_ += *res;
  }

  return res;
}

error_code RdbSnapshot::Start(SaveMode save_mode, const std::string& path,
                              const StringVec& lua_scripts) {
  bool is_direct = false;
  if (fq_tp_) {  // EPOLL
    auto res = util::OpenFiberWriteFile(path, fq_tp_);
    if (!res)
      return res.error();
    io_sink_.reset(*res);
  } else {
    auto res = uring::OpenLinux(path, kRdbWriteFlags, 0666);
    if (!res) {
      return res.error();
    }
    io_sink_.reset(new LinuxWriteWrapper(res->release()));
    is_direct = kRdbWriteFlags & O_DIRECT;
  }

  saver_.reset(new RdbSaver(io_sink_.get(), save_mode, is_direct));

  return saver_->SaveHeader(lua_scripts);
}

error_code RdbSnapshot::SaveBody() {
  return saver_->SaveBody(&cll_, &freq_map_);
}

error_code RdbSnapshot::Close() {
  // TODO: to solve it in a more elegant way.
  if (fq_tp_) {
    return static_cast<io::WriteFile*>(io_sink_.get())->Close();
  }
  return static_cast<LinuxWriteWrapper*>(io_sink_.get())->Close();
}

void RdbSnapshot::StartInShard(EngineShard* shard) {
  saver_->StartSnapshotInShard(false, &cll_, shard);
  started_ = true;
}

string FormatTs(absl::Time now) {
  return absl::FormatTime("%Y-%m-%dT%H:%M:%S", now, absl::LocalTimeZone());
}

void ExtendFilename(absl::Time now, absl::AlphaNum postfix, fs::path* filename) {
  filename->replace_extension();  // clear if exists
  *filename += StrCat("-", FormatTs(now), "-", postfix, ".dfs");
}

void ExtendFilenameWithShard(absl::Time now, int shard, fs::path* filename) {
  if (shard < 0) {
    if (!filename->has_extension()) {
      *filename += StrCat("-", FormatTs(now), ".rdb");
    }
  } else {
    // dragonfly snapshot.
    ExtendFilename(now, absl::Dec(shard, absl::kZeroPad4), filename);
  }
}

}  // namespace

std::optional<SnapshotSpec> ParseSaveSchedule(string_view time) {
  if (time.length() < 3 || time.length() > 5) {
    return std::nullopt;
  }

  size_t separator_idx = time.find(':');
  // the time cannot start with ':' and it must be present in the first 3 characters of any time
  if (separator_idx == 0 || separator_idx >= 3) {
    return std::nullopt;
  }

  SnapshotSpec spec{string(time.substr(0, separator_idx)), string(time.substr(separator_idx + 1))};
  // a minute should be 2 digits as it is zero padded, unless it is a '*' in which case this
  // greedily can make up both digits
  if (spec.minute_spec != "*" && spec.minute_spec.length() != 2) {
    return std::nullopt;
  }

  return IsValidSaveScheduleNibble(spec.hour_spec, 23) &&
                 IsValidSaveScheduleNibble(spec.minute_spec, 59)
             ? std::optional<SnapshotSpec>(spec)
             : std::nullopt;
}

bool DoesTimeNibbleMatchSpecifier(string_view time_spec, unsigned int current_time) {
  // single greedy wildcard matches everything
  if (time_spec == "*") {
    return true;
  }

  for (int i = time_spec.length() - 1; i >= 0; --i) {
    // if the current digit is not a wildcard and it does not match the digit in the current time it
    // does not match
    if (time_spec[i] != '*' && int(current_time % 10) != (time_spec[i] - '0')) {
      return false;
    }
    current_time /= 10;
  }

  return current_time == 0;
}

bool DoesTimeMatchSpecifier(const SnapshotSpec& spec, time_t now) {
  unsigned hour = (now / 3600) % 24;
  unsigned min = (now / 60) % 60;
  return DoesTimeNibbleMatchSpecifier(spec.hour_spec, hour) &&
         DoesTimeNibbleMatchSpecifier(spec.minute_spec, min);
}

ServerFamily::ServerFamily(Service* service) : service_(*service) {
  start_time_ = time(NULL);
  last_save_info_ = make_shared<LastSaveInfo>();
  last_save_info_->save_time = start_time_;
  script_mgr_.reset(new ScriptMgr());
  journal_.reset(new journal::Journal);

  {
    absl::InsecureBitGen eng;
    master_id_ = GetRandomHex(eng, CONFIG_RUN_ID_SIZE);
    DCHECK_EQ(CONFIG_RUN_ID_SIZE, master_id_.size());
  }
}

ServerFamily::~ServerFamily() {
}

void ServerFamily::Init(util::AcceptServer* acceptor, util::ListenerInterface* main_listener) {
  CHECK(acceptor_ == nullptr);
  acceptor_ = acceptor;
  main_listener_ = main_listener;
  dfly_cmd_.reset(new DflyCmd(main_listener, this));

  pb_task_ = shard_set->pool()->GetNextProactor();
  if (pb_task_->GetKind() == ProactorBase::EPOLL) {
    fq_threadpool_.reset(new FiberQueueThreadPool());
  }

  // Unlike EngineShard::Heartbeat that runs independently in each shard thread,
  // this callback runs in a single thread and it aggregates globally stats from all the shards.
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

  uint32_t cache_hz = max(GetFlag(FLAGS_hz) / 10, 1u);
  uint32_t period_ms = max(1u, 1000 / cache_hz);
  stats_caching_task_ =
      pb_task_->AwaitBrief([&] { return pb_task_->AddPeriodic(period_ms, cache_cb); });

  fs::path data_folder = fs::current_path();
  const auto& dir = GetFlag(FLAGS_dir);

  error_code file_ec;
  if (!dir.empty()) {
    data_folder = fs::canonical(dir, file_ec);
  }

  if (!file_ec) {
    LOG(INFO) << "Data directory is " << data_folder;
    string load_path = InferLoadFile(data_folder);
    if (!load_path.empty()) {
      load_result_ = Load(load_path);
    }
  } else {
    LOG(ERROR) << "Data directory error: " << file_ec.message();
  }

  string save_time = GetFlag(FLAGS_save_schedule);
  if (!save_time.empty()) {
    std::optional<SnapshotSpec> spec = ParseSaveSchedule(save_time);
    if (spec) {
      snapshot_fiber_ = service_.proactor_pool().GetNextProactor()->LaunchFiber(
          [save_spec = std::move(spec.value()), this] { SnapshotScheduling(save_spec); });
    } else {
      LOG(WARNING) << "Invalid snapshot time specifier " << save_time;
    }
  }
}

void ServerFamily::Shutdown() {
  VLOG(1) << "ServerFamily::Shutdown";

  if (load_result_.valid())
    load_result_.wait();

  is_snapshot_done_.Notify();
  if (snapshot_fiber_.IsJoinable()) {
    snapshot_fiber_.Join();
  }

  pb_task_->Await([this] {
    pb_task_->CancelPeriodic(stats_caching_task_);
    stats_caching_task_ = 0;

    if (journal_->EnterLameDuck()) {
      auto ec = journal_->Close();
      LOG_IF(ERROR, ec) << "Error closing journal " << ec;
    }

    unique_lock lk(replicaof_mu_);
    if (replica_) {
      replica_->Stop();
    }

    dfly_cmd_->Shutdown();
  });
}

// Load starts as many fibers as there are files to load each one separately.
// It starts one more fiber that waits for all load fibers to finish and returns the first
// error (if any occured) with a future.
fibers::future<std::error_code> ServerFamily::Load(const std::string& load_path) {
  CHECK(absl::EndsWith(load_path, ".rdb") || absl::EndsWith(load_path, "summary.dfs"));

  vector<std::string> paths{{load_path}};

  // Collect all other files in case we're loading dfs.
  if (absl::EndsWith(load_path, "summary.dfs")) {
    std::string glob = absl::StrReplaceAll(load_path, {{"summary", "????"}});
    io::Result<io::StatShortVec> files = io::StatFiles(glob);

    if (files && files->size() == 0) {
      fibers::promise<std::error_code> ec_promise;
      ec_promise.set_value(make_error_code(errc::no_such_file_or_directory));
      return ec_promise.get_future();
    }

    for (auto& fstat : *files) {
      paths.push_back(std::move(fstat.name));
    }
  }

  // Check all paths are valid.
  for (const auto& path : paths) {
    error_code ec;
    fs::canonical(path, ec);
    if (ec) {
      LOG(ERROR) << "Error loading " << load_path << " " << ec.message();
      fibers::promise<std::error_code> ec_promise;
      ec_promise.set_value(ec);
      return ec_promise.get_future();
    }
  }

  LOG(INFO) << "Loading " << load_path;

  GlobalState new_state = service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  if (new_state != GlobalState::LOADING) {
    LOG(WARNING) << GlobalStateName(new_state) << " in progress, ignored";
    return {};
  }

#if 0
  auto& pool = service_.proactor_pool();
  // Deliberately run on all I/O threads to update the state for non-shard threads as well.
  pool.Await([&](ProactorBase*) {
    // TODO: There can be a bug where status is different.
    CHECK(ServerState::tlocal()->gstate() == GlobalState::IDLE);
    ServerState::tlocal()->set_gstate(GlobalState::LOADING);
  });
#endif

  auto& pool = service_.proactor_pool();

  vector<util::fibers_ext::Fiber> load_fibers;
  load_fibers.reserve(paths.size());

  auto first_error = std::make_shared<AggregateError>();

  for (auto& path : paths) {
    // For single file, choose thread that does not handle shards if possible.
    // This will balance out the CPU during the load.
    ProactorBase* proactor;
    if (paths.size() == 1 && shard_count() < pool.size()) {
      proactor = pool.at(shard_count());
    } else {
      proactor = pool.GetNextProactor();
    }

    auto load_fiber = [this, first_error, path = std::move(path)]() {
      *first_error = LoadRdb(path);
    };
    load_fibers.push_back(proactor->LaunchFiber(std::move(load_fiber)));
  }

  boost::fibers::promise<std::error_code> ec_promise;
  boost::fibers::future<std::error_code> ec_future = ec_promise.get_future();

  // Run fiber that empties the channel and sets ec_promise.
  auto load_join_fiber = [this, first_error, load_fibers = std::move(load_fibers),
                          ec_promise = std::move(ec_promise)]() mutable {
    for (auto& fiber : load_fibers) {
      fiber.Join();
    }

    VLOG(1) << "Load finished";
    service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    ec_promise.set_value(**first_error);
  };
  pool.GetNextProactor()->Dispatch(std::move(load_join_fiber));

  return ec_future;
}

void ServerFamily::SnapshotScheduling(const SnapshotSpec& spec) {
  const auto loop_sleep_time = std::chrono::seconds(20);
  while (true) {
    if (is_snapshot_done_.WaitFor(loop_sleep_time)) {
      break;
    }

    time_t now = std::time(NULL);

    if (!DoesTimeMatchSpecifier(spec, now)) {
      continue;
    }

    // if it matches check the last save time, if it is the same minute don't save another snapshot
    time_t last_save;
    {
      lock_guard lk(save_mu_);
      last_save = last_save_info_->save_time;
    }

    if ((last_save / 60) == (now / 60)) {
      continue;
    }

    const CommandId* cid = service().FindCmd("SAVE");
    CHECK_NOTNULL(cid);
    boost::intrusive_ptr<Transaction> trans(new Transaction{cid});
    trans->InitByArgs(0, {});

    GenericError ec = DoSave(false, trans.get());
    if (ec) {
      LOG(WARNING) << "Failed to perform snapshot " << ec.Format();
    }
  }
}

error_code ServerFamily::LoadRdb(const std::string& rdb_file) {
  error_code ec;
  io::ReadonlyFileOrError res;

  if (fq_threadpool_) {
    res = util::OpenFiberReadFile(rdb_file, fq_threadpool_.get());
  } else {
    res = uring::OpenRead(rdb_file);
  }

  if (res) {
    io::FileSource fs(*res);

    RdbLoader loader{&service_};
    ec = loader.Load(&fs);
    if (!ec) {
      LOG(INFO) << "Done loading RDB, keys loaded: " << loader.keys_loaded();
      LOG(INFO) << "Loading finished after "
                << strings::HumanReadableElapsedTime(loader.load_time());
    }
  } else {
    ec = res.error();
  }

  service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);

  return ec;
}

enum MetricType { COUNTER, GAUGE, SUMMARY, HISTOGRAM };

const char* MetricTypeName(MetricType type) {
  switch (type) {
    case MetricType::COUNTER:
      return "counter";
    case MetricType::GAUGE:
      return "gauge";
    case MetricType::SUMMARY:
      return "summary";
    case MetricType::HISTOGRAM:
      return "histogram";
  }
  return "unknown";
}

inline string GetMetricFullName(string_view metric_name) {
  return StrCat("dragonfly_", metric_name);
}

void AppendMetricHeader(string_view metric_name, string_view metric_help, MetricType type,
                        string* dest) {
  const auto full_metric_name = GetMetricFullName(metric_name);
  absl::StrAppend(dest, "# HELP ", full_metric_name, " ", metric_help, "\n");
  absl::StrAppend(dest, "# TYPE ", full_metric_name, " ", MetricTypeName(type), "\n");
}

void AppendLabelTupple(absl::Span<const string_view> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  if (label_names.empty())
    return;

  absl::StrAppend(dest, "{");
  for (size_t i = 0; i < label_names.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(dest, ", ");
    }
    absl::StrAppend(dest, label_names[i], "=\"", label_values[i], "\"");
  }

  absl::StrAppend(dest, "}");
}

void AppendMetricValue(string_view metric_name, const absl::AlphaNum& value,
                       absl::Span<const string_view> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  absl::StrAppend(dest, GetMetricFullName(metric_name));
  AppendLabelTupple(label_names, label_values, dest);
  absl::StrAppend(dest, " ", value, "\n");
}

void AppendMetricWithoutLabels(string_view name, string_view help, const absl::AlphaNum& value,
                               MetricType type, string* dest) {
  AppendMetricHeader(name, help, type, dest);
  AppendMetricValue(name, value, {}, {}, dest);
}

void PrintPrometheusMetrics(const Metrics& m, StringResponse* resp) {
  // Server metrics
  AppendMetricHeader("version", "", MetricType::GAUGE, &resp->body());
  AppendMetricValue("version", 1, {"version"}, {GetVersion()}, &resp->body());
  AppendMetricWithoutLabels("uptime_in_seconds", "", m.uptime, MetricType::GAUGE, &resp->body());

  // Clients metrics
  AppendMetricWithoutLabels("connected_clients", "", m.conn_stats.num_conns, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("client_read_buf_capacity", "", m.conn_stats.read_buf_capacity,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("blocked_clients", "", m.conn_stats.num_blocked_clients,
                            MetricType::GAUGE, &resp->body());

  // Memory metrics
  auto sdata_res = io::ReadStatusInfo();
  AppendMetricWithoutLabels("memory_used_bytes", "", m.heap_used_bytes, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("memory_used_peak_bytes", "", used_mem_peak.load(memory_order_relaxed),
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("comitted_memory", "", GetMallocCurrentCommitted(), MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("memory_max_bytes", "", max_memory_limit, MetricType::GAUGE,
                            &resp->body());
  if (sdata_res.has_value()) {
    AppendMetricWithoutLabels("used_memory_rss_bytes", "", sdata_res->vm_rss, MetricType::GAUGE,
                              &resp->body());
  } else {
    LOG_FIRST_N(ERROR, 10) << "Error fetching /proc/self/status stats. error "
                           << sdata_res.error().message();
  }

  // Stats metrics
  AppendMetricWithoutLabels("commands_processed_total", "", m.conn_stats.command_cnt,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("keyspace_hits_total", "", m.events.hits, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("keyspace_misses_total", "", m.events.misses, MetricType::COUNTER,
                            &resp->body());

  // Net metrics
  AppendMetricWithoutLabels("net_input_bytes_total", "", m.conn_stats.io_read_bytes,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("net_output_bytes_total", "", m.conn_stats.io_write_bytes,
                            MetricType::COUNTER, &resp->body());

  // DB stats
  AppendMetricWithoutLabels("expired_keys_total", "", m.events.expired_keys, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("evicted_keys_total", "", m.events.evicted_keys, MetricType::COUNTER,
                            &resp->body());

  string db_key_metrics;
  string db_key_expire_metrics;

  AppendMetricHeader("db_keys", "Total number of keys by DB", MetricType::GAUGE, &db_key_metrics);
  AppendMetricHeader("db_keys_expiring", "Total number of expiring keys by DB", MetricType::GAUGE,
                     &db_key_expire_metrics);

  for (size_t i = 0; i < m.db.size(); ++i) {
    AppendMetricValue("db_keys", m.db[i].key_count, {"db"}, {StrCat("db", i)}, &db_key_metrics);
    AppendMetricValue("db_keys_expiring", m.db[i].expire_count, {"db"}, {StrCat("db", i)},
                      &db_key_expire_metrics);
  }

  absl::StrAppend(&resp->body(), db_key_metrics);
  absl::StrAppend(&resp->body(), db_key_expire_metrics);
}

void ServerFamily::ConfigureMetrics(util::HttpListenerBase* http_base) {
  // The naming of the metrics should be compatible with redis_exporter, see
  // https://github.com/oliver006/redis_exporter/blob/master/exporter/exporter.go#L111

  auto cb = [this](const util::http::QueryArgs& args, util::HttpContext* send) {
    StringResponse resp = util::http::MakeStringResponse(boost::beast::http::status::ok);
    PrintPrometheusMetrics(this->GetMetrics(), &resp);

    return send->Invoke(std::move(resp));
  };

  http_base->RegisterCb("/metrics", cb);
}

void ServerFamily::PauseReplication(bool pause) {
  unique_lock lk(replicaof_mu_);

  // Switch to primary mode.
  if (!ServerState::tlocal()->is_master) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    repl_ptr->Pause(pause);
  }
}

void ServerFamily::OnClose(ConnectionContext* cntx) {
  dfly_cmd_->OnClose(cntx);
}

void ServerFamily::StatsMC(std::string_view section, facade::ConnectionContext* cntx) {
  if (!section.empty()) {
    return cntx->reply_builder()->SendError("");
  }
  string info;

#define ADD_LINE(name, val) absl::StrAppend(&info, "STAT " #name " ", val, "\r\n")

  time_t now = time(NULL);
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);

  auto dbl_time = [](const timeval& tv) -> double {
    return tv.tv_sec + double(tv.tv_usec) / 1000000.0;
  };

  double utime = dbl_time(ru.ru_utime);
  double systime = dbl_time(ru.ru_stime);

  Metrics m = GetMetrics();

  ADD_LINE(pid, getpid());
  ADD_LINE(uptime, m.uptime);
  ADD_LINE(time, now);
  ADD_LINE(version, kGitTag);
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

// Run callback for all active RdbSnapshots (passed as index).
// .dfs format contains always `shard_set->size() + 1` snapshots (for every shard and summary file).
static void RunStage(bool new_version, std::function<void(unsigned)> cb) {
  if (new_version) {
    shard_set->RunBlockingInParallel([&](EngineShard* es) { cb(es->shard_id()); });
    cb(shard_set->size());
  } else {
    cb(0);
  }
};

using PartialSaveOpts =
    tuple<const fs::path& /*filename*/, const fs::path& /*path*/, absl::Time /*start*/>;

// Start saving a single snapshot of a multi-file dfly snapshot.
// If shard is null, then this is the summary file.
error_code DoPartialSave(PartialSaveOpts opts, const dfly::StringVec& scripts,
                         RdbSnapshot* snapshot, EngineShard* shard) {
  auto [filename, path, now] = opts;
  // Construct resulting filename.
  fs::path full_filename = filename;
  if (shard == nullptr) {
    ExtendFilename(now, "summary", &full_filename);
  } else {
    ExtendFilenameWithShard(now, shard->shard_id(), &full_filename);
  }
  fs::path full_path = path / full_filename;  // use / operator to concatenate paths.

  // Start rdb saving.
  SaveMode mode = shard == nullptr ? SaveMode::SUMMARY : SaveMode::SINGLE_SHARD;
  error_code local_ec = snapshot->Start(mode, full_path.generic_string(), scripts);

  if (!local_ec && mode == SaveMode::SINGLE_SHARD) {
    snapshot->StartInShard(shard);
  }

  return local_ec;
}

GenericError ServerFamily::DoSave(bool new_version, Transaction* trans) {
  fs::path dir_path(GetFlag(FLAGS_dir));
  AggregateGenericError ec;

  // Check directory.
  if (!dir_path.empty()) {
    if (auto local_ec = CreateDirs(dir_path); local_ec) {
      return {local_ec, "create-dir"};
    }
  }

  // Manage global state.
  GlobalState new_state = service_.SwitchState(GlobalState::ACTIVE, GlobalState::SAVING);
  if (new_state != GlobalState::SAVING) {
    return {make_error_code(errc::operation_in_progress),
            StrCat(GlobalStateName(new_state), " - can not save database")};
  }
  absl::Cleanup rev_state = [this] {
    service_.SwitchState(GlobalState::SAVING, GlobalState::ACTIVE);
  };

  const auto& dbfilename = GetFlag(FLAGS_dbfilename);
  fs::path filename = dbfilename.empty() ? "dump" : dbfilename;
  fs::path path = dir_path;

  absl::Time start = absl::Now();
  shared_ptr<LastSaveInfo> save_info;

  vector<unique_ptr<RdbSnapshot>> snapshots;
  absl::flat_hash_map<string_view, size_t> rdb_name_map;
  fibers::mutex mu;  // guards rdb_name_map

  auto save_cb = [&](unsigned index) {
    auto& snapshot = snapshots[index];
    if (snapshot && snapshot->HasStarted()) {
      ec = snapshot->SaveBody();
    }
  };

  auto close_cb = [&](unsigned index) {
    auto& snapshot = snapshots[index];
    if (snapshot) {
      ec = snapshot->Close();

      lock_guard lk(mu);
      for (const auto& k_v : snapshot->freq_map()) {
        rdb_name_map[RdbTypeName(k_v.first)] += k_v.second;
      }
    }
  };

  // Start snapshots.
  if (new_version) {
    auto file_opts = make_tuple(cref(filename), cref(path), start);

    // In the new version (.dfs) we store a file for every shard and one more summary file.
    // Summary file is always last in snapshots array.
    snapshots.resize(shard_set->size() + 1);

    // Save summary file.
    {
      const auto scripts = script_mgr_->GetLuaScripts();
      auto& snapshot = snapshots[shard_set->size()];
      snapshot.reset(new RdbSnapshot(fq_threadpool_.get()));
      if (auto local_ec = DoPartialSave(file_opts, scripts, snapshot.get(), nullptr); local_ec) {
        ec = local_ec;
        snapshot.reset();
      }
    }

    // Save shard files.
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto& snapshot = snapshots[shard->shard_id()];
      snapshot.reset(new RdbSnapshot(fq_threadpool_.get()));
      if (auto local_ec = DoPartialSave(file_opts, {}, snapshot.get(), shard); local_ec) {
        ec = local_ec;
        snapshot.reset();
      }
      return OpStatus::OK;
    };

    trans->ScheduleSingleHop(std::move(cb));
  } else {
    snapshots.resize(1);

    ExtendFilenameWithShard(start, -1, &filename);
    path /= filename;  // use / operator to concatenate paths.

    snapshots[0].reset(new RdbSnapshot(fq_threadpool_.get()));
    const auto lua_scripts = script_mgr_->GetLuaScripts();
    ec = snapshots[0]->Start(SaveMode::RDB, path.generic_string(), lua_scripts);

    if (!ec) {
      auto cb = [&](Transaction* t, EngineShard* shard) {
        snapshots[0]->StartInShard(shard);
        return OpStatus::OK;
      };

      trans->ScheduleSingleHop(std::move(cb));
    } else {
      snapshots[0].reset();
    }
  }

  is_saving_.store(true, memory_order_relaxed);

  // Perform snapshot serialization, block the current fiber until it completes.
  // TODO: Add cancellation in case of error.
  RunStage(new_version, save_cb);

  is_saving_.store(false, memory_order_relaxed);

  RunStage(new_version, close_cb);

  if (new_version) {
    ExtendFilename(start, "summary", &filename);
    path /= filename;
  }

  absl::Duration dur = absl::Now() - start;
  double seconds = double(absl::ToInt64Milliseconds(dur)) / 1000;

  // Populate LastSaveInfo.
  if (!ec) {
    LOG(INFO) << "Saving " << path << " finished after "
              << strings::HumanReadableElapsedTime(seconds);

    save_info = make_shared<LastSaveInfo>();
    for (const auto& k_v : rdb_name_map) {
      save_info->freq_map.emplace_back(k_v);
    }

    save_info->save_time = absl::ToUnixSeconds(start);
    save_info->file_name = path.generic_string();
    save_info->duration_sec = uint32_t(seconds);

    lock_guard lk(save_mu_);
    // swap - to deallocate the old version outstide of the lock.
    last_save_info_.swap(save_info);
  }

  return *ec;
}

error_code ServerFamily::Drakarys(Transaction* transaction, DbIndex db_ind) {
  VLOG(1) << "Drakarys";

  transaction->Schedule();  // TODO: to convert to ScheduleSingleHop ?

  transaction->Execute(
      [db_ind](Transaction* t, EngineShard* shard) {
        shard->db_slice().FlushDb(db_ind);
        return OpStatus::OK;
      },
      true);

  return error_code{};
}

shared_ptr<const LastSaveInfo> ServerFamily::GetLastSaveInfo() const {
  lock_guard lk(save_mu_);
  return last_save_info_;
}

void ServerFamily::DbSize(CmdArgList args, ConnectionContext* cntx) {
  atomic_ulong num_keys{0};

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) {
        auto db_size = shard->db_slice().DbSize(cntx->conn_state.db_index);
        num_keys.fetch_add(db_size, memory_order_relaxed);
      },
      [](ShardId) { return true; });

  return (*cntx)->SendLong(num_keys.load(memory_order_relaxed));
}

void ServerFamily::BreakOnShutdown() {
  dfly_cmd_->BreakOnShutdown();
}

void ServerFamily::FlushDb(CmdArgList args, ConnectionContext* cntx) {
  DCHECK(cntx->transaction);
  Drakarys(cntx->transaction, cntx->transaction->GetDbIndex());
  cntx->reply_builder()->SendOk();
}

void ServerFamily::FlushAll(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  DCHECK(cntx->transaction);
  Drakarys(cntx->transaction, DbSlice::kDbAll);
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
  if (pass == GetFlag(FLAGS_requirepass)) {
    cntx->authenticated = true;
    (*cntx)->SendOk();
  } else {
    (*cntx)->SendError(facade::kAuthRejected);
  }
}

void ServerFamily::Client(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "SETNAME" && args.size() == 3) {
    cntx->owner()->SetName(ArgS(args, 2));
    return (*cntx)->SendOk();
  }

  if (sub_cmd == "GETNAME") {
    const char* name = cntx->owner()->GetName();
    if (*name != 0) {
      return (*cntx)->SendBulkString(name);
    } else {
      return (*cntx)->SendNull();
    }
  }

  if (sub_cmd == "LIST") {
    vector<string> client_info;
    fibers::mutex mu;
    auto cb = [&](util::Connection* conn) {
      facade::Connection* dcon = static_cast<facade::Connection*>(conn);
      string info = dcon->GetClientInfo();
      lock_guard lk(mu);
      client_info.push_back(move(info));
    };

    main_listener_->TraverseConnections(cb);
    string result = absl::StrJoin(move(client_info), "\n");
    result.append("\n");
    return (*cntx)->SendBulkString(result);
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "CLIENT"), kSyntaxErrType);
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
    shard_set->pool()->Await([](auto*) {
      auto* stats = ServerState::tl_connection_stats();
      stats->cmd_count_map.clear();
      stats->err_count_map.clear();
      stats->command_cnt = 0;
      stats->async_writes_cnt = 0;
    });
    return (*cntx)->SendOk();
  } else {
    return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "CONFIG"), kSyntaxErrType);
  }
}

void ServerFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);

  DebugCmd dbg_cmd{this, cntx};

  return dbg_cmd.Run(args);
}

void ServerFamily::Memory(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);

  MemoryCmd mem_cmd{this, cntx};

  return mem_cmd.Run(args);
}

void ServerFamily::Save(CmdArgList args, ConnectionContext* cntx) {
  string err_detail;
  bool new_version = false;
  if (args.size() > 2) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  if (args.size() == 2) {
    ToUpper(&args[1]);
    string_view sub_cmd = ArgS(args, 1);
    if (sub_cmd == "DF") {
      new_version = true;
    } else {
      return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "SAVE"), kSyntaxErrType);
    }
  }

  GenericError ec = DoSave(new_version, cntx->transaction);
  if (ec) {
    (*cntx)->SendError(ec.Format());
  } else {
    (*cntx)->SendOk();
  }
}

static void MergeInto(const DbSlice::Stats& src, Metrics* dest) {
  if (src.db_stats.size() > dest->db.size())
    dest->db.resize(src.db_stats.size());
  for (size_t i = 0; i < src.db_stats.size(); ++i) {
    dest->db[i] += src.db_stats[i];
  }

  dest->events += src.events;
  dest->small_string_bytes += src.small_string_bytes;
}

Metrics ServerFamily::GetMetrics() const {
  Metrics result;

  fibers::mutex mu;

  auto cb = [&](ProactorBase* pb) {
    EngineShard* shard = EngineShard::tlocal();
    ServerState* ss = ServerState::tlocal();

    lock_guard<fibers::mutex> lk(mu);

    result.uptime = time(NULL) - this->start_time_;
    result.conn_stats += ss->connection_stats;
    result.qps += uint64_t(ss->MovingSum6());

    if (shard) {
      MergeInto(shard->db_slice().GetStats(), &result);

      result.heap_used_bytes += shard->UsedMemory();
      if (shard->tiered_storage()) {
        result.tiered_stats += shard->tiered_storage()->GetStats();
      }
      result.shard_stats += shard->stats();
      result.traverse_ttl_per_sec += shard->GetMovingSum6(EngineShard::TTL_TRAVERSE);
      result.delete_ttl_per_sec += shard->GetMovingSum6(EngineShard::TTL_DELETE);
    }
  };

  service_.proactor_pool().AwaitFiberOnAll(std::move(cb));
  result.qps /= 6;  // normalize moving average stats
  result.traverse_ttl_per_sec /= 6;
  result.delete_ttl_per_sec /= 6;

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

  auto should_enter = [&](string_view name, bool hidden = false) {
    bool res = (!hidden && section.empty()) || section == "ALL" || section == name;
    if (res && !info.empty())
      info.append("\r\n");

    return res;
  };

  auto append = [&info](absl::AlphaNum a1, absl::AlphaNum a2) {
    absl::StrAppend(&info, a1, ":", a2, "\r\n");
  };

#define ADD_HEADER(x) absl::StrAppend(&info, x "\r\n")
  Metrics m = GetMetrics();

  if (should_enter("SERVER")) {
    ProactorBase::ProactorKind kind = ProactorBase::me()->GetKind();
    const char* multiplex_api = (kind == ProactorBase::IOURING) ? "iouring" : "epoll";

    ADD_HEADER("# Server");

    append("redis_version", GetVersion());
    append("redis_mode", "standalone");
    append("arch_bits", 64);
    append("multiplexing_api", multiplex_api);
    append("tcp_port", GetFlag(FLAGS_port));

    size_t uptime = m.uptime;
    append("uptime_in_seconds", uptime);
    append("uptime_in_days", uptime / (3600 * 24));
  }

  auto sdata_res = io::ReadStatusInfo();

  DbStats total;
  for (const auto& db_stats : m.db) {
    total += db_stats;
  }

  if (should_enter("CLIENTS")) {
    ADD_HEADER("# Clients");
    append("connected_clients", m.conn_stats.num_conns);
    append("client_read_buf_capacity", m.conn_stats.read_buf_capacity);
    append("blocked_clients", m.conn_stats.num_blocked_clients);
  }

  if (should_enter("MEMORY")) {
    ADD_HEADER("# Memory");

    append("used_memory", m.heap_used_bytes);
    append("used_memory_human", HumanReadableNumBytes(m.heap_used_bytes));
    append("used_memory_peak", used_mem_peak.load(memory_order_relaxed));

    append("comitted_memory", GetMallocCurrentCommitted());

    if (sdata_res.has_value()) {
      append("used_memory_rss", sdata_res->vm_rss);
      append("used_memory_rss_human", HumanReadableNumBytes(sdata_res->vm_rss));
    } else {
      LOG_FIRST_N(ERROR, 10) << "Error fetching /proc/self/status stats. error "
                             << sdata_res.error().message();
    }

    // Blob - all these cases where the key/objects are represented by a single blob allocated on
    // heap. For example, strings or intsets. members of lists, sets, zsets etc
    // are not accounted for to avoid complex computations. In some cases, when number of members
    // is known we approximate their allocations by taking 16 bytes per member.
    append("object_used_memory", total.obj_memory_usage);
    append("table_used_memory", total.table_mem_usage);
    append("num_buckets", total.bucket_count);
    append("num_entries", total.key_count);
    append("inline_keys", total.inline_keys);
    append("strval_bytes", total.strval_memory_usage);
    append("updateval_amount", total.update_value_amount);
    append("listpack_blobs", total.listpack_blob_cnt);
    append("listpack_bytes", total.listpack_bytes);
    append("small_string_bytes", m.small_string_bytes);
    append("maxmemory", max_memory_limit);
    append("maxmemory_human", HumanReadableNumBytes(max_memory_limit));
    append("cache_mode", GetFlag(FLAGS_cache_mode) ? "cache" : "store");
  }

  if (should_enter("STATS")) {
    ADD_HEADER("# Stats");

    append("instantaneous_ops_per_sec", m.qps);
    append("total_commands_processed", m.conn_stats.command_cnt);
    append("total_pipelined_commands", m.conn_stats.pipelined_cmd_cnt);
    append("total_net_input_bytes", m.conn_stats.io_read_bytes);
    append("total_net_output_bytes", m.conn_stats.io_write_bytes);
    append("instantaneous_input_kbps", -1);
    append("instantaneous_output_kbps", -1);
    append("rejected_connections", -1);
    append("expired_keys", m.events.expired_keys);
    append("evicted_keys", m.events.evicted_keys);
    append("hard_evictions", m.events.hard_evictions);
    append("garbage_checked", m.events.garbage_checked);
    append("garbage_collected", m.events.garbage_collected);
    append("bump_ups", m.events.bumpups);
    append("stash_unloaded", m.events.stash_unloaded);
    append("traverse_ttl_sec", m.traverse_ttl_per_sec);
    append("delete_ttl_sec", m.delete_ttl_per_sec);
    append("keyspace_hits", m.events.hits);
    append("keyspace_misses", m.events.misses);
    append("total_reads_processed", m.conn_stats.io_read_cnt);
    append("total_writes_processed", m.conn_stats.io_write_cnt);
    append("async_writes_count", m.conn_stats.async_writes_cnt);
    append("parser_err_count", m.conn_stats.parser_err_cnt);
    append("defrag_attempt_total", m.shard_stats.defrag_attempt_total);
    append("defrag_realloc_total", m.shard_stats.defrag_realloc_total);
    append("defrag_task_invocation_total", m.shard_stats.defrag_task_invocation_total);
  }

  if (should_enter("TIERED", true)) {
    ADD_HEADER("# TIERED");
    append("tiered_entries", total.tiered_entries);
    append("tiered_bytes", total.tiered_size);
    append("tiered_reads", m.tiered_stats.tiered_reads);
    append("tiered_writes", m.tiered_stats.tiered_writes);
    append("tiered_reserved", m.tiered_stats.storage_reserved);
    append("tiered_capacity", m.tiered_stats.storage_capacity);
    append("tiered_aborted_write_total", m.tiered_stats.aborted_write_cnt);
    append("tiered_flush_skip_total", m.tiered_stats.flush_skip_cnt);
  }

  if (should_enter("PERSISTENCE", true)) {
    ADD_HEADER("# PERSISTENCE");
    decltype(last_save_info_) save_info;
    {
      lock_guard lk(save_mu_);
      save_info = last_save_info_;
    }
    // when when last save
    append("last_save", save_info->save_time);
    append("last_save_duration_sec", save_info->duration_sec);
    append("last_save_file", save_info->file_name);

    for (const auto& k_v : save_info->freq_map) {
      append(StrCat("rdb_", k_v.first), k_v.second);
    }
  }

  if (should_enter("REPLICATION")) {
    ADD_HEADER("# Replication");

    ServerState& etl = *ServerState::tlocal();

    if (etl.is_master) {
      append("role", "master");
      append("connected_slaves", m.conn_stats.num_replicas);
      auto replicas = dfly_cmd_->GetReplicasRoleInfo();
      for (auto i = 0; i < replicas.size(); i++) {
        auto& r = replicas[i];
        // e.g. slave0:ip=172.19.0.3,port=6379
        append(StrCat("slave", i), StrCat("ip=", r.address, ",port=", r.listening_port));
      }
      append("master_replid", master_id_);
    } else {
      append("role", "replica");

      // it's safe to access replica_ because replica_ is created before etl.is_master set to
      // false and cleared after etl.is_master is set to true. And since the code here that checks
      // for is_master and copies shared_ptr is atomic, it1 should be correct.
      auto replica_ptr = replica_;
      Replica::Info rinfo = replica_ptr->GetInfo();
      append("master_host", rinfo.host);
      append("master_port", rinfo.port);

      const char* link = rinfo.master_link_established ? "up" : "down";
      append("master_link_status", link);
      append("master_last_io_seconds_ago", rinfo.master_last_io_sec);
      append("master_sync_in_progress", rinfo.sync_in_progress);
    }
  }

  if (should_enter("COMMANDSTATS", true)) {
    ADD_HEADER("# Commandstats");

    auto unknown_cmd = service_.UknownCmdMap();

    for (const auto& k_v : unknown_cmd) {
      append(StrCat("unknown_", k_v.first), k_v.second);
    }

    for (const auto& k_v : m.conn_stats.cmd_count_map) {
      append(StrCat("cmd_", k_v.first), k_v.second);
    }
  }

  if (should_enter("ERRORSTATS", true)) {
    ADD_HEADER("# Errorstats");
    for (const auto& k_v : m.conn_stats.err_count_map) {
      append(k_v.first, k_v.second);
    }
  }

  if (should_enter("KEYSPACE")) {
    ADD_HEADER("# Keyspace");
    for (size_t i = 0; i < m.db.size(); ++i) {
      const auto& stats = m.db[i];
      bool show = (i == 0) || (stats.key_count > 0);
      if (show) {
        string val = StrCat("keys=", stats.key_count, ",expires=", stats.expire_count,
                            ",avg_ttl=-1");  // TODO
        append(StrCat("db", i), val);
      }
    }
  }

  if (should_enter("CPU")) {
    ADD_HEADER("# CPU");
    struct rusage ru, cu, tu;
    getrusage(RUSAGE_SELF, &ru);
    getrusage(RUSAGE_CHILDREN, &cu);
    getrusage(RUSAGE_THREAD, &tu);
    append("used_cpu_sys", StrCat(ru.ru_stime.tv_sec, ".", ru.ru_stime.tv_usec));
    append("used_cpu_user", StrCat(ru.ru_utime.tv_sec, ".", ru.ru_utime.tv_usec));
    append("used_cpu_sys_children", StrCat(cu.ru_stime.tv_sec, ".", cu.ru_stime.tv_usec));
    append("used_cpu_user_children", StrCat(cu.ru_utime.tv_sec, ".", cu.ru_utime.tv_usec));
    append("used_cpu_sys_main_thread", StrCat(tu.ru_stime.tv_sec, ".", tu.ru_stime.tv_usec));
    append("used_cpu_user_main_thread", StrCat(tu.ru_utime.tv_sec, ".", tu.ru_utime.tv_usec));
  }

  (*cntx)->SendBulkString(info);
}

void ServerFamily::Hello(CmdArgList args, ConnectionContext* cntx) {
  // Allow calling this commands with no arguments or protover=2
  // technically that is all that is supported at the moment.
  // For all other cases degrade to 'unknown command' so that clients
  // checking for the existence of the command to detect if RESP3 is
  // supported or whether authentication can be performed using HELLO
  // will gracefully fallback to RESP2 and using the AUTH command explicitly.
  if (args.size() > 1) {
    string_view proto_version = ArgS(args, 1);
    if (proto_version != "2" || args.size() > 2) {
      (*cntx)->SendError(UnknownCmd("HELLO", args.subspan(1)));
      return;
    }
  }

  (*cntx)->StartArray(12);
  (*cntx)->SendBulkString("server");
  (*cntx)->SendBulkString("redis");
  (*cntx)->SendBulkString("version");
  (*cntx)->SendBulkString(GetVersion());
  (*cntx)->SendBulkString("proto");
  (*cntx)->SendLong(2);
  (*cntx)->SendBulkString("id");
  (*cntx)->SendLong(cntx->owner()->GetClientId());
  (*cntx)->SendBulkString("mode");
  (*cntx)->SendBulkString("standalone");
  (*cntx)->SendBulkString("role");
  (*cntx)->SendBulkString((*ServerState::tlocal()).is_master ? "master" : "slave");
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
  } else {
    // TODO: to disconnect all the blocked clients (pubsub, blpop etc)

    pool.AwaitFiberOnAll([&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = false; });
  }

  replica_.swap(new_replica);

  GlobalState new_state = service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  if (new_state != GlobalState::LOADING) {
    LOG(WARNING) << GlobalStateName(new_state) << " in progress, ignored";
    return;
  }

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
  if (!replica_->Start(cntx)) {
    service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    replica_.reset();
  }

  bool is_master = !replica_;
  pool.AwaitFiberOnAll(
      [&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = is_master; });
}

void ServerFamily::ReplConf(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() % 2 == 0)
    goto err;

  for (unsigned i = 1; i < args.size(); i += 2) {
    DCHECK_LT(i + 1, args.size());
    ToUpper(&args[i]);

    std::string_view cmd = ArgS(args, i);
    std::string_view arg = ArgS(args, i + 1);
    if (cmd == "CAPA") {
      if (arg == "dragonfly" && args.size() == 3 && i == 1) {
        uint32_t sid = dfly_cmd_->CreateSyncSession(cntx);
        cntx->owner()->SetName(absl::StrCat("repl_ctrl_", sid));

        string sync_id = absl::StrCat("SYNC", sid);
        cntx->conn_state.replicaiton_info.repl_session_id = sid;

        if (!cntx->replica_conn) {
          ServerState::tl_connection_stats()->num_replicas += 1;
        }
        cntx->replica_conn = true;

        // The response for 'capa dragonfly' is: <masterid> <syncid> <numthreads>
        (*cntx)->StartArray(3);
        (*cntx)->SendSimpleString(master_id_);
        (*cntx)->SendSimpleString(sync_id);
        (*cntx)->SendLong(shard_set->pool()->size());
        return;
      }
    } else if (cmd == "LISTENING-PORT") {
      uint32_t replica_listening_port;
      if (!absl::SimpleAtoi(arg, &replica_listening_port)) {
        (*cntx)->SendError(kInvalidIntErr);
      }
      cntx->conn_state.replicaiton_info.repl_listening_port = replica_listening_port;
    } else {
      VLOG(1) << cmd << " " << arg;
    }
  }

  (*cntx)->SendOk();
  return;

err:
  (*cntx)->SendError(kSyntaxErr);
}

void ServerFamily::Role(CmdArgList args, ConnectionContext* cntx) {
  ServerState& etl = *ServerState::tlocal();
  if (etl.is_master) {
    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString("master");
    auto vec = dfly_cmd_->GetReplicasRoleInfo();
    (*cntx)->StartArray(vec.size());
    for (auto& data : vec) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(data.address);
      (*cntx)->SendBulkString(data.state);
    }

  } else {
    auto replica_ptr = replica_;
    Replica::Info rinfo = replica_ptr->GetInfo();
    (*cntx)->StartArray(4);
    (*cntx)->SendBulkString("replica");
    (*cntx)->SendBulkString(rinfo.host);
    (*cntx)->SendBulkString(absl::StrCat(rinfo.port));
    if (rinfo.sync_in_progress) {
      (*cntx)->SendBulkString("full sync");
    } else if (!rinfo.master_link_established) {
      (*cntx)->SendBulkString("connecting");
    } else {
      (*cntx)->SendBulkString("stable sync");
    }
  }
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
  time_t save_time;
  {
    lock_guard lk(save_mu_);
    save_time = last_save_info_->save_time;
  }
  (*cntx)->SendLong(save_time);
}

void ServerFamily::Latency(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "LATEST") {
    return (*cntx)->SendEmptyArray();
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

void ServerFamily::Dfly(CmdArgList args, ConnectionContext* cntx) {
  dfly_cmd_->Run(args, cntx);
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
            << CI{"DEBUG", CO::ADMIN | CO::LOADING, -2, 0, 0, 0}.HFUNC(Debug)
            << CI{"FLUSHDB", CO::WRITE | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(FlushDb)
            << CI{"FLUSHALL", CO::WRITE | CO::GLOBAL_TRANS, -1, 0, 0, 0}.HFUNC(FlushAll)
            << CI{"INFO", CO::LOADING, -1, 0, 0, 0}.HFUNC(Info)
            << CI{"HELLO", CO::LOADING, -1, 0, 0, 0}.HFUNC(Hello)
            << CI{"LASTSAVE", CO::LOADING | CO::FAST, 1, 0, 0, 0}.HFUNC(LastSave)
            << CI{"LATENCY", CO::NOSCRIPT | CO::LOADING | CO::FAST, -2, 0, 0, 0}.HFUNC(Latency)
            << CI{"MEMORY", kMemOpts, -2, 0, 0, 0}.HFUNC(Memory)
            << CI{"SAVE", CO::ADMIN | CO::GLOBAL_TRANS, -1, 0, 0, 0}.HFUNC(Save)
            << CI{"SHUTDOWN", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0}.HFUNC(_Shutdown)
            << CI{"SLAVEOF", kReplicaOpts, 3, 0, 0, 0}.HFUNC(ReplicaOf)
            << CI{"REPLICAOF", kReplicaOpts, 3, 0, 0, 0}.HFUNC(ReplicaOf)
            << CI{"REPLCONF", CO::ADMIN | CO::LOADING, -1, 0, 0, 0}.HFUNC(ReplConf)
            << CI{"ROLE", CO::LOADING | CO::FAST | CO::NOSCRIPT, 1, 0, 0, 0}.HFUNC(Role)
            // We won't support DF->REDIS replication for now, hence we do not need to support
            // these commands.
            // << CI{"SYNC", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0}.HFUNC(Sync)
            // << CI{"PSYNC", CO::ADMIN | CO::GLOBAL_TRANS, 3, 0, 0, 0}.HFUNC(Psync)
            << CI{"SCRIPT", CO::NOSCRIPT, -2, 0, 0, 0}.HFUNC(Script)
            << CI{"DFLY", CO::ADMIN | CO::GLOBAL_TRANS, -2, 0, 0, 0}.HFUNC(Dfly);
}

}  // namespace dfly
