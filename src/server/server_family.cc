// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>  // for master_id_ generation.
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>
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
#include "croncpp.h"  // cron::cronexpr
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"
#include "io/file_util.h"
#include "io/proc_reader.h"
#include "search/doc_index.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/debugcmd.h"
#include "server/dflycmd.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/journal/journal.h"
#include "server/main_service.h"
#include "server/memory_cmd.h"
#include "server/protocol_client.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "server/version.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/cloud/aws.h"
#include "util/cloud/s3.h"
#include "util/fibers/fiber_file.h"
#include "util/uring/uring_file.h"

using namespace std;

struct ReplicaOfFlag {
  string host;
  string port;

  bool has_value() const {
    return !host.empty() && !port.empty();
  }
};

static bool AbslParseFlag(std::string_view in, ReplicaOfFlag* flag, std::string* err);
static std::string AbslUnparseFlag(const ReplicaOfFlag& flag);

ABSL_FLAG(string, dir, "", "working directory");
ABSL_FLAG(string, dbfilename, "dump-{timestamp}", "the filename to save/load the DB");
ABSL_FLAG(string, requirepass, "",
          "password for AUTH authentication. "
          "If empty can also be set with DFLY_PASSWORD environment variable.");
ABSL_FLAG(uint32_t, maxclients, 64000, "Maximum number of concurrent clients allowed.");

ABSL_FLAG(string, save_schedule, "",
          "glob spec for the UTC time to save a snapshot which matches HH:MM 24h time");
ABSL_FLAG(string, snapshot_cron, "",
          "cron expression for the time to save a snapshot, crontab style");
ABSL_FLAG(bool, df_snapshot_format, true,
          "if true, save in dragonfly-specific snapshotting format");
ABSL_FLAG(int, epoll_file_threads, 0,
          "thread size for file workers when running in epoll mode, default is hardware concurrent "
          "threads");
ABSL_FLAG(ReplicaOfFlag, replicaof, ReplicaOfFlag{},
          "Specifies a host and port which point to a target master "
          "to replicate. "
          "Format should be <IPv4>:<PORT> or host:<PORT> or [<IPv6>]:<PORT>");

ABSL_DECLARE_FLAG(uint32_t, port);
ABSL_DECLARE_FLAG(bool, cache_mode);
ABSL_DECLARE_FLAG(uint32_t, hz);
ABSL_DECLARE_FLAG(bool, tls);
ABSL_DECLARE_FLAG(string, tls_ca_cert_file);
ABSL_DECLARE_FLAG(string, tls_ca_cert_dir);

bool AbslParseFlag(std::string_view in, ReplicaOfFlag* flag, std::string* err) {
#define RETURN_ON_ERROR(cond, m)                                           \
  do {                                                                     \
    if ((cond)) {                                                          \
      *err = m;                                                            \
      LOG(WARNING) << "Error in parsing arguments for --replicaof: " << m; \
      return false;                                                        \
    }                                                                      \
  } while (0)

  if (in.empty()) {  // on empty flag "parse" nothing. If we return false then DF exists.
    *flag = ReplicaOfFlag{};
    return true;
  }

  auto pos = in.find_last_of(':');
  RETURN_ON_ERROR(pos == string::npos, "missing ':'.");

  string_view ip = in.substr(0, pos);
  flag->port = in.substr(pos + 1);

  RETURN_ON_ERROR(ip.empty() || flag->port.empty(), "IP/host or port are empty.");

  // For IPv6: ip1.front == '[' AND ip1.back == ']'
  // For IPv4: ip1.front != '[' AND ip1.back != ']'
  // Together, this ip1.front == '[' iff ip1.back == ']', which can be implemented as XNOR (NOT XOR)
  RETURN_ON_ERROR(((ip.front() == '[') ^ (ip.back() == ']')), "unclosed brackets.");

  if (ip.front() == '[') {
    // shortest possible IPv6 is '::1' (loopback)
    RETURN_ON_ERROR(ip.length() <= 2, "IPv6 host name is too short");

    flag->host = ip.substr(1, ip.length() - 2);
    VLOG(1) << "received IP of type IPv6: " << flag->host;
  } else {
    flag->host = ip;
    VLOG(1) << "received IP of type IPv4 (or a host): " << flag->host;
  }

  VLOG(1) << "--replicaof: Received " << flag->host << " :  " << flag->port;
  return true;
#undef RETURN_ON_ERROR
}

std::string AbslUnparseFlag(const ReplicaOfFlag& flag) {
  return (flag.has_value()) ? absl::StrCat(flag.host, ":", flag.port) : "";
}

namespace dfly {

namespace fs = std::filesystem;

using absl::GetFlag;
using absl::StrCat;
using namespace facade;
using namespace util;
using http::StringResponse;
using strings::HumanReadableNumBytes;

namespace {

const auto kRedisVersion = "6.2.11";
constexpr string_view kS3Prefix = "s3://"sv;

const auto kRdbWriteFlags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_DIRECT;
const size_t kBucketConnectMs = 2000;

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

void SubstituteFilenameTsPlaceholder(fs::path* filename, std::string_view replacement) {
  *filename = absl::StrReplaceAll(filename->string(), {{"{timestamp}", replacement}});
}

bool IsCloudPath(string_view path) {
  return absl::StartsWith(path, kS3Prefix);
}

// Returns bucket_name, obj_path for an s3 path.
optional<pair<string, string>> GetBucketPath(string_view path) {
  string_view clean = absl::StripPrefix(path, kS3Prefix);

  size_t pos = clean.find('/');
  if (pos == string_view::npos)
    return nullopt;

  string bucket_name{clean.substr(0, pos)};
  string obj_path{clean.substr(pos + 1)};
  return make_pair(move(bucket_name), move(obj_path));
}

string InferLoadFile(string_view dir, cloud::AWS* aws) {
  fs::path data_folder;
  string bucket_name, obj_path;

  if (dir.empty()) {
    data_folder = fs::current_path();
  } else {
    if (IsCloudPath(dir)) {
      CHECK(aws);
      auto res = GetBucketPath(dir);
      if (!res) {
        LOG(ERROR) << "Invalid S3 path: " << dir;
        return {};
      }
      data_folder = dir;
      bucket_name = res->first;
      obj_path = res->second;
    } else {
      error_code file_ec;
      data_folder = fs::canonical(dir, file_ec);
      if (file_ec) {
        LOG(ERROR) << "Data directory error: " << file_ec.message() << " for dir " << dir;
        return {};
      }
    }
  }

  LOG(INFO) << "Data directory is " << data_folder;

  const auto& dbname = GetFlag(FLAGS_dbfilename);
  if (dbname.empty())
    return string{};

  if (IsCloudPath(dir)) {
    cloud::S3Bucket bucket(*aws, bucket_name);
    ProactorBase* proactor = shard_set->pool()->GetNextProactor();
    auto ec = proactor->Await([&] { return bucket.Connect(kBucketConnectMs); });
    if (ec) {
      LOG(ERROR) << "Couldn't connect to S3 bucket: " << ec.message();
      return {};
    }

    fs::path fl_path{obj_path};
    fl_path.append(dbname);

    LOG(INFO) << "Loading from s3 path s3://" << bucket_name << "/" << fl_path;
    // TODO: to load from S3 file.
    return {};
  }

  fs::path fl_path = data_folder.append(dbname);
  if (fs::exists(fl_path))
    return fl_path.generic_string();

  SubstituteFilenameTsPlaceholder(&fl_path, "*");
  if (!fl_path.has_extension()) {
    fl_path += "*";
  }
  io::Result<io::StatShortVec> short_vec = io::StatFiles(fl_path.generic_string());

  if (short_vec) {
    // io::StatFiles returns a list of sorted files. Because our timestamp format has the same
    // time order and lexicographic order we iterate from the end to find the latest snapshot.
    auto it = std::find_if(short_vec->rbegin(), short_vec->rend(), [](const auto& stat) {
      return absl::EndsWith(stat.name, ".rdb") || absl::EndsWith(stat.name, "summary.dfs");
    });
    if (it != short_vec->rend())
      return it->name;
  } else {
    LOG(WARNING) << "Could not stat " << fl_path << ", error " << short_vec.error().message();
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
  LinuxWriteWrapper(LinuxFile* lf) : lf_(lf) {
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  error_code Close() {
    return lf_->Close();
  }

 private:
  unique_ptr<LinuxFile> lf_;
  off_t offset_ = 0;
};

class RdbSnapshot {
 public:
  RdbSnapshot(FiberQueueThreadPool* fq_tp, cloud::AWS* aws) : fq_tp_{fq_tp}, aws_{aws} {
  }

  GenericError Start(SaveMode save_mode, const string& path, const RdbSaver::GlobalData& glob_data);
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
  bool is_linux_file_ = false;
  FiberQueueThreadPool* fq_tp_ = nullptr;
  cloud::AWS* aws_ = nullptr;

  unique_ptr<io::Sink> io_sink_;
  unique_ptr<RdbSaver> saver_;
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

GenericError RdbSnapshot::Start(SaveMode save_mode, const std::string& path,
                                const RdbSaver::GlobalData& glob_data) {
  bool is_direct = false;
  VLOG(1) << "Saving RDB " << path;

  if (IsCloudPath(path)) {
    DCHECK(aws_);

    optional<pair<string, string>> bucket_path = GetBucketPath(path);
    if (!bucket_path) {
      return GenericError("Invalid S3 path");
    }
    auto [bucket_name, obj_path] = *bucket_path;

    cloud::S3Bucket bucket(*aws_, bucket_name);
    error_code ec = bucket.Connect(kBucketConnectMs);
    if (ec) {
      return GenericError(ec, "Couldn't connect to S3 bucket");
    }
    auto res = bucket.OpenWriteFile(obj_path);
    if (!res) {
      return GenericError(res.error(), "Couldn't open file for writing");
    }
    io_sink_.reset(*res);
  } else {
    if (fq_tp_) {  // EPOLL
      auto res = util::OpenFiberWriteFile(path, fq_tp_);
      if (!res)
        return GenericError(res.error(), "Couldn't open file for writing");
      io_sink_.reset(*res);
    } else {
      auto res = OpenLinux(path, kRdbWriteFlags, 0666);
      if (!res) {
        return GenericError(
            res.error(),
            "Couldn't open file for writing (is direct I/O supported by the file system?)");
      }
      is_linux_file_ = true;
      io_sink_.reset(new LinuxWriteWrapper(res->release()));
      is_direct = kRdbWriteFlags & O_DIRECT;
    }
  }

  saver_.reset(new RdbSaver(io_sink_.get(), save_mode, is_direct));

  return saver_->SaveHeader(move(glob_data));
}

error_code RdbSnapshot::SaveBody() {
  return saver_->SaveBody(&cll_, &freq_map_);
}

error_code RdbSnapshot::Close() {
  if (is_linux_file_) {
    return static_cast<LinuxWriteWrapper*>(io_sink_.get())->Close();
  }
  return static_cast<io::WriteFile*>(io_sink_.get())->Close();
}

void RdbSnapshot::StartInShard(EngineShard* shard) {
  saver_->StartSnapshotInShard(false, &cll_, shard);
  started_ = true;
}

string FormatTs(absl::Time now) {
  return absl::FormatTime("%Y-%m-%dT%H:%M:%S", now, absl::LocalTimeZone());
}

// modifies 'filename' to be "filename-postfix.extension"
void SetExtension(absl::AlphaNum postfix, string_view extension, fs::path* filename) {
  filename->replace_extension();  // clear if exists
  *filename += StrCat("-", postfix, extension);
}

void ExtendDfsFilenameWithShard(int shard, string_view extension, fs::path* filename) {
  // dragonfly snapshot.
  SetExtension(absl::Dec(shard, absl::kZeroPad4), extension, filename);
}

GenericError ValidateFilename(const fs::path& filename, bool new_version) {
  bool is_cloud_path = IsCloudPath(filename.string());

  if (!filename.parent_path().empty() && !is_cloud_path) {
    return {absl::StrCat("filename may not contain directory separators (Got \"", filename.c_str(),
                         "\"). dbfilename should specify the filename without the directory")};
  }

  if (!filename.has_extension()) {
    return {};
  }

  if (new_version) {
    if (absl::EqualsIgnoreCase(filename.extension().c_str(), ".rdb")) {
      return {absl::StrCat(
          "DF snapshot format is used but '.rdb' extension was given. Use --nodf_snapshot_format "
          "or remove the filename extension.")};
    } else {
      return {absl::StrCat("DF snapshot format requires no filename extension. Got \"",
                           filename.extension().c_str(), "\"")};
    }
  }
  if (!new_version && !absl::EqualsIgnoreCase(filename.extension().c_str(), ".rdb")) {
    return {absl::StrCat("Bad filename extension \"", filename.extension().c_str(),
                         "\" for SAVE with type RDB")};
  }
  return {};
}

void SlowLog(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (sub_cmd == "LEN") {
    return (*cntx)->SendLong(0);
  }

  if (sub_cmd == "GET") {
    return (*cntx)->SendEmptyArray();
  }

  (*cntx)->SendError(UnknownSubCmd(sub_cmd, "SLOWLOG"), kSyntaxErrType);
}

// Check that if TLS is used at least one form of client authentication is
// enabled. That means either using a password or giving a root
// certificate for authenticating client certificates which will
// be required.
void ValidateServerTlsFlags() {
  if (!absl::GetFlag(FLAGS_tls)) {
    return;
  }

  bool has_auth = false;

  if (!dfly::GetPassword().empty()) {
    has_auth = true;
  }

  if (!(absl::GetFlag(FLAGS_tls_ca_cert_file).empty() &&
        absl::GetFlag(FLAGS_tls_ca_cert_dir).empty())) {
    has_auth = true;
  }

  if (!has_auth) {
    LOG(ERROR) << "TLS configured but no authentication method is used!";
    exit(1);
  }
}

bool IsReplicatingNoOne(string_view host, string_view port) {
  return absl::EqualsIgnoreCase(host, "no") && absl::EqualsIgnoreCase(port, "one");
}

struct SaveStagesInputs {
  bool use_dfs_format_;
  string_view basename_;
  Transaction* trans_;
  Service* service_;
  atomic_bool* is_saving_;
  FiberQueueThreadPool* fq_threadpool_;
  shared_ptr<LastSaveInfo>* last_save_info_;
  Mutex* save_mu_;
  unique_ptr<cloud::AWS>* aws_;
};

struct SaveStagesController : public SaveStagesInputs {
  SaveStagesController(SaveStagesInputs&& inputs) : SaveStagesInputs{move(inputs)} {
    start_time_ = absl::Now();
  }

  ~SaveStagesController() {
    service_->SwitchState(GlobalState::SAVING, GlobalState::ACTIVE);
  }

  GenericError Save() {
    if (auto err = BuildFullPath(); err)
      return err;

    if (auto err = SwitchState(); err)
      return err;

    if (auto err = InitResources(); err)
      return err;

    // The stages below report errors to shared_err_
    if (use_dfs_format_)
      SaveDfs();
    else
      SaveRdb();

    is_saving_->store(true, memory_order_relaxed);
    RunStage(&SaveStagesController::SaveCb);
    is_saving_->store(false, memory_order_relaxed);

    RunStage(&SaveStagesController::CloseCb);

    FinalizeFileMovement();

    if (!shared_err_)
      UpdateSaveInfo();

    return *shared_err_;
  }

 private:
  // In the new version (.dfs) we store a file for every shard and one more summary file.
  // Summary file is always last in snapshots array.
  void SaveDfs() {
    // Extend all filenames with -{sid} or -summary and append .dfs.tmp
    const string_view ext = is_cloud_ ? ".dfs" : ".dfs.tmp";
    ShardId sid = 0;
    for (auto& [_, filename] : snapshots_) {
      filename = full_path_;
      if (sid < shard_set->size())
        ExtendDfsFilenameWithShard(sid++, ext, &filename);
      else
        SetExtension("summary", ext, &filename);
    }

    // Save summary file.
    SaveDfsSingle(nullptr);

    // Save shard files.
    auto cb = [this](Transaction* t, EngineShard* shard) {
      SaveDfsSingle(shard);
      return OpStatus::OK;
    };
    trans_->ScheduleSingleHop(std::move(cb));
  }

  // Start saving a dfs file on shard
  void SaveDfsSingle(EngineShard* shard) {
    // for summary file, shard=null and index=shard_set->size(), see SaveDfs() above
    auto& [snapshot, filename] = snapshots_[shard ? shard->shard_id() : shard_set->size()];

    SaveMode mode = shard == nullptr ? SaveMode::SUMMARY : SaveMode::SINGLE_SHARD;
    auto glob_data = shard == nullptr ? GetGlobalData() : RdbSaver::GlobalData{};

    if (auto err = snapshot->Start(mode, filename, glob_data); err) {
      shared_err_ = err;
      snapshot.reset();
      return;
    }

    if (mode == SaveMode::SINGLE_SHARD)
      snapshot->StartInShard(shard);
  }

  // Save a single rdb file
  void SaveRdb() {
    auto& [snapshot, filename] = snapshots_.front();

    filename = full_path_;
    if (!filename.has_extension())
      filename += ".rdb";
    if (!is_cloud_)
      filename += ".tmp";

    if (auto err = snapshot->Start(SaveMode::RDB, filename, GetGlobalData()); err) {
      snapshot.reset();
      return;
    }

    auto cb = [snapshot = snapshot.get()](Transaction* t, EngineShard* shard) {
      snapshot->StartInShard(shard);
      return OpStatus::OK;
    };
    trans_->ScheduleSingleHop(std::move(cb));
  }

  void UpdateSaveInfo() {
    fs::path resulting_path = full_path_;
    if (use_dfs_format_)
      SetExtension("summary", ".dfs", &resulting_path);
    else
      resulting_path.replace_extension();  // remove .tmp

    double seconds = double(absl::ToInt64Milliseconds(absl::Now() - start_time_)) / 1000;
    LOG(INFO) << "Saving " << resulting_path << " finished after "
              << strings::HumanReadableElapsedTime(seconds);

    auto save_info = make_shared<LastSaveInfo>();
    for (const auto& k_v : rdb_name_map_) {
      save_info->freq_map.emplace_back(k_v);
    }
    save_info->save_time = absl::ToUnixSeconds(start_time_);
    save_info->file_name = resulting_path.generic_string();
    save_info->duration_sec = uint32_t(seconds);

    lock_guard lk{*save_mu_};
    last_save_info_->swap(save_info);  // swap - to deallocate the old version outstide of the lock.
  }

  GenericError InitResources() {
    if (is_cloud_ && !aws_) {
      *aws_ = make_unique<cloud::AWS>("s3");
      if (auto ec = aws_->get()->Init(); ec) {
        aws_->reset();
        return {ec, "Couldn't initialize AWS"};
      }
    }

    snapshots_.resize(use_dfs_format_ ? shard_set->size() + 1 : 1);
    for (auto& [snapshot, _] : snapshots_)
      snapshot = make_unique<RdbSnapshot>(fq_threadpool_, aws_->get());
    return {};
  }

  // Remove .tmp extension or delete files in case of error
  void FinalizeFileMovement() {
    if (is_cloud_)
      return;

    // If the shared_err is set, the snapshot saving failed
    bool has_error = bool(shared_err_);

    for (const auto& [_, filename] : snapshots_) {
      if (has_error)
        filesystem::remove(filename);
      else
        filesystem::rename(filename, fs::path{filename}.replace_extension(""));
    }
  }

  // Build full path: get dir, try creating dirs, get filename with placeholder
  GenericError BuildFullPath() {
    fs::path dir_path = GetFlag(FLAGS_dir);
    if (!dir_path.empty()) {
      if (auto ec = CreateDirs(dir_path); ec)
        return {ec, "Failed to create directories"};
    }

    fs::path filename = basename_.empty() ? GetFlag(FLAGS_dbfilename) : basename_;
    if (auto err = ValidateFilename(filename, use_dfs_format_); err)
      return err;

    SubstituteFilenameTsPlaceholder(&filename, FormatTs(start_time_));
    full_path_ = dir_path / filename;
    is_cloud_ = IsCloudPath(full_path_.string());
    return {};
  }

  // Switch to saving state if in active state
  GenericError SwitchState() {
    GlobalState new_state = service_->SwitchState(GlobalState::ACTIVE, GlobalState::SAVING);
    if (new_state != GlobalState::SAVING && new_state != GlobalState::TAKEN_OVER)
      return {make_error_code(errc::operation_in_progress),
              StrCat(GlobalStateName(new_state), " - can not save database")};
    return {};
  }

  void SaveCb(unsigned index) {
    if (auto& snapshot = snapshots_[index].first; snapshot && snapshot->HasStarted())
      shared_err_ = snapshot->SaveBody();
  }

  void CloseCb(unsigned index) {
    if (auto& snapshot = snapshots_[index].first; snapshot) {
      shared_err_ = snapshot->Close();

      lock_guard lk{rdb_name_map_mu_};
      for (const auto& k_v : snapshot->freq_map())
        rdb_name_map_[RdbTypeName(k_v.first)] += k_v.second;
    }

    if (auto* es = EngineShard::tlocal(); use_dfs_format_ && es)
      es->db_slice().ResetUpdateEvents();
  }

  void RunStage(void (SaveStagesController::*cb)(unsigned)) {
    if (use_dfs_format_) {
      shard_set->RunBlockingInParallel([&](EngineShard* es) { (this->*cb)(es->shard_id()); });
      (this->*cb)(shard_set->size());
    } else {
      (this->*cb)(0);
    }
  }

  RdbSaver::GlobalData GetGlobalData() const {
    StringVec script_bodies, search_indices;

    {
      auto scripts = service_->script_mgr()->GetAll();
      script_bodies.reserve(scripts.size());
      for (auto& [sha, data] : scripts)
        script_bodies.push_back(move(data.body));
    }

    {
      shard_set->Await(0, [&] {
        auto* indices = EngineShard::tlocal()->search_indices();
        for (auto index_name : indices->GetIndexNames()) {
          auto index_info = indices->GetIndex(index_name)->GetInfo();
          search_indices.emplace_back(
              absl::StrCat(index_name, " ", index_info.BuildRestoreCommand()));
        }
      });
    }

    return RdbSaver::GlobalData{move(script_bodies), move(search_indices)};
  }

 private:
  absl::Time start_time_;
  fs::path full_path_;
  bool is_cloud_;

  AggregateGenericError shared_err_;
  vector<pair<unique_ptr<RdbSnapshot>, fs::path>> snapshots_;

  absl::flat_hash_map<string_view, size_t> rdb_name_map_;
  Mutex rdb_name_map_mu_;
};

void RebuildAllSearchIndices(Service* service) {
  boost::intrusive_ptr<Transaction> trans{new Transaction{service->FindCmd("FT.CREATE")}};
  trans->InitByArgs(0, {});
  trans->ScheduleSingleHop([](auto* trans, auto* es) {
    es->search_indices()->RebuildAllIndices(trans->GetOpArgs(es));
    return OpStatus::OK;
  });
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

std::optional<cron::cronexpr> InferSnapshotCronExpr() {
  string save_time = GetFlag(FLAGS_save_schedule);
  string snapshot_cron_exp = GetFlag(FLAGS_snapshot_cron);

  if (!snapshot_cron_exp.empty() && !save_time.empty()) {
    LOG(ERROR) << "snapshot_cron and save_schedule flags should not be set simultaneously";
    quick_exit(1);
  }

  string raw_cron_expr;
  if (!save_time.empty()) {
    std::optional<SnapshotSpec> spec = ParseSaveSchedule(save_time);

    if (spec) {
      // Setting snapshot to HH:mm everyday, as specified by `save_schedule` flag
      raw_cron_expr = "0 " + spec.value().minute_spec + " " + spec.value().hour_spec + " * * *";
    } else {
      LOG(WARNING) << "Invalid snapshot time specifier " << save_time;
    }
  } else if (!snapshot_cron_exp.empty()) {
    raw_cron_expr = "0 " + snapshot_cron_exp;
  }

  if (!raw_cron_expr.empty()) {
    try {
      return std::optional<cron::cronexpr>(cron::make_cron(raw_cron_expr));
    } catch (const cron::bad_cronexpr& ex) {
      LOG(WARNING) << "Invalid cron expression: " << ex.what();
    }
  }
  return std::nullopt;
}

ServerFamily::ServerFamily(Service* service) : service_(*service) {
  start_time_ = time(NULL);
  last_save_info_ = make_shared<LastSaveInfo>();
  last_save_info_->save_time = start_time_;
  script_mgr_.reset(new ScriptMgr());
  journal_.reset(new journal::Journal());

  {
    absl::InsecureBitGen eng;
    master_id_ = GetRandomHex(eng, CONFIG_RUN_ID_SIZE);
    DCHECK_EQ(CONFIG_RUN_ID_SIZE, master_id_.size());
  }

  if (auto ec = ValidateFilename(GetFlag(FLAGS_dbfilename), GetFlag(FLAGS_df_snapshot_format));
      ec) {
    LOG(ERROR) << ec.Format();
    exit(1);
  }

  ValidateServerTlsFlags();
  ValidateClientTlsFlags();
}

ServerFamily::~ServerFamily() {
}

void SetMaxClients(std::vector<facade::Listener*>& listeners, uint32_t maxclients) {
  for (auto* listener : listeners) {
    if (!listener->IsAdminInterface()) {
      listener->SetMaxClients(maxclients);
    }
  }
}

void ServerFamily::Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners) {
  CHECK(acceptor_ == nullptr);
  acceptor_ = acceptor;
  listeners_ = std::move(listeners);
  dfly_cmd_ = make_unique<DflyCmd>(this);

  SetMaxClients(listeners_, absl::GetFlag(FLAGS_maxclients));
  config_registry.RegisterMutable("maxclients", [this](const absl::CommandLineFlag& flag) {
    auto res = flag.TryGet<uint32_t>();
    if (res.has_value())
      SetMaxClients(listeners_, res.value());
    return res.has_value();
  });

  pb_task_ = shard_set->pool()->GetNextProactor();
  if (pb_task_->GetKind() == ProactorBase::EPOLL) {
    fq_threadpool_.reset(new FiberQueueThreadPool(absl::GetFlag(FLAGS_epoll_file_threads)));
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

  // check for '--replicaof' before loading anything
  if (ReplicaOfFlag flag = GetFlag(FLAGS_replicaof); flag.has_value()) {
    service_.proactor_pool().GetNextProactor()->Await(
        [this, &flag]() { this->Replicate(flag.host, flag.port); });
    return;  // DONT load any snapshots
  }

  string flag_dir = GetFlag(FLAGS_dir);
  if (IsCloudPath(flag_dir)) {
    aws_ = make_unique<cloud::AWS>("s3");
    if (auto ec = aws_->Init(); ec) {
      LOG(FATAL) << "Failed to initialize AWS " << ec;
    }
  }

  string load_path = InferLoadFile(flag_dir, aws_.get());
  if (!load_path.empty()) {
    load_result_ = Load(load_path);
  }

  snapshot_schedule_fb_ =
      service_.proactor_pool().GetNextProactor()->LaunchFiber([this] { SnapshotScheduling(); });
}

void ServerFamily::Shutdown() {
  VLOG(1) << "ServerFamily::Shutdown";

  if (load_result_.valid())
    load_result_.wait();

  schedule_done_.Notify();
  if (snapshot_schedule_fb_.IsJoinable()) {
    snapshot_schedule_fb_.Join();
  }

  if (save_on_shutdown_ && !absl::GetFlag(FLAGS_dbfilename).empty()) {
    shard_set->pool()->GetNextProactor()->Await([this] {
      GenericError ec = DoSave();
      if (ec) {
        LOG(WARNING) << "Failed to perform snapshot " << ec.Format();
      }
    });
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

struct AggregateLoadResult {
  AggregateError first_error;
  std::atomic<size_t> keys_read;
};

// Load starts as many fibers as there are files to load each one separately.
// It starts one more fiber that waits for all load fibers to finish and returns the first
// error (if any occured) with a future.
Future<std::error_code> ServerFamily::Load(const std::string& load_path) {
  if (!(absl::EndsWith(load_path, ".rdb") || absl::EndsWith(load_path, "summary.dfs"))) {
    LOG(ERROR) << "Bad filename extension \"" << load_path << "\"";
    Promise<std::error_code> ec_promise;
    ec_promise.set_value(make_error_code(errc::invalid_argument));
    return ec_promise.get_future();
  }

  vector<std::string> paths{{load_path}};

  // Collect all other files in case we're loading dfs.
  if (absl::EndsWith(load_path, "summary.dfs")) {
    std::string glob = absl::StrReplaceAll(load_path, {{"summary", "????"}});
    io::Result<io::StatShortVec> files = io::StatFiles(glob);

    if (files && files->size() == 0) {
      Promise<std::error_code> ec_promise;
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
    (void)fs::canonical(path, ec);
    if (ec) {
      LOG(ERROR) << "Error loading " << load_path << " " << ec.message();
      Promise<std::error_code> ec_promise;
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

  auto& pool = service_.proactor_pool();

  vector<Fiber> load_fibers;
  load_fibers.reserve(paths.size());

  auto aggregated_result = std::make_shared<AggregateLoadResult>();

  for (auto& path : paths) {
    // For single file, choose thread that does not handle shards if possible.
    // This will balance out the CPU during the load.
    ProactorBase* proactor;
    if (paths.size() == 1 && shard_count() < pool.size()) {
      proactor = pool.at(shard_count());
    } else {
      proactor = pool.GetNextProactor();
    }

    auto load_fiber = [this, aggregated_result, path = std::move(path)]() {
      auto load_result = LoadRdb(path);
      if (load_result.has_value())
        aggregated_result->keys_read.fetch_add(*load_result);
      else
        aggregated_result->first_error = load_result.error();
    };
    load_fibers.push_back(proactor->LaunchFiber(std::move(load_fiber)));
  }

  Promise<std::error_code> ec_promise;
  Future<std::error_code> ec_future = ec_promise.get_future();

  // Run fiber that empties the channel and sets ec_promise.
  auto load_join_fiber = [this, aggregated_result, load_fibers = std::move(load_fibers),
                          ec_promise = std::move(ec_promise)]() mutable {
    for (auto& fiber : load_fibers) {
      fiber.Join();
    }
    if (aggregated_result->first_error) {
      LOG(ERROR) << "Rdb load failed. " << (*aggregated_result->first_error).message();
      exit(1);
    }
    RebuildAllSearchIndices(&service_);
    LOG(INFO) << "Load finished, num keys read: " << aggregated_result->keys_read;
    service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    ec_promise.set_value(*(aggregated_result->first_error));
  };
  pool.GetNextProactor()->Dispatch(std::move(load_join_fiber));

  return ec_future;
}

void ServerFamily::SnapshotScheduling() {
  const std::optional<cron::cronexpr> cron_expr = InferSnapshotCronExpr();
  if (!cron_expr) {
    return;
  }

  const auto loading_check_interval = std::chrono::seconds(10);
  while (service_.GetGlobalState() == GlobalState::LOADING) {
    schedule_done_.WaitFor(loading_check_interval);
  }

  while (true) {
    const std::chrono::time_point now = std::chrono::system_clock::now();
    const std::chrono::time_point next = cron::cron_next(cron_expr.value(), now);

    if (schedule_done_.WaitFor(next - now)) {
      break;
    };

    GenericError ec = DoSave();
    if (ec) {
      LOG(WARNING) << "Failed to perform snapshot " << ec.Format();
    }
  }
}

io::Result<size_t> ServerFamily::LoadRdb(const std::string& rdb_file) {
  error_code ec;
  io::ReadonlyFileOrError res;

  if (fq_threadpool_) {
    res = util::OpenFiberReadFile(rdb_file, fq_threadpool_.get());
  } else {
    res = OpenRead(rdb_file);
  }

  if (res) {
    io::FileSource fs(*res);

    RdbLoader loader{&service_};
    ec = loader.Load(&fs);
    if (!ec) {
      VLOG(1) << "Done loading RDB from " << rdb_file << ", keys loaded: " << loader.keys_loaded();
      VLOG(1) << "Loading finished after " << strings::HumanReadableElapsedTime(loader.load_time());
      return loader.keys_loaded();
    }
  } else {
    ec = res.error();
  }
  return nonstd::make_unexpected(ec);
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
  AppendMetricHeader("role", "", MetricType::GAUGE, &resp->body());
  AppendMetricValue("role", 1, {"role"}, {m.is_master ? "master" : "replica"}, &resp->body());
  AppendMetricWithoutLabels("master", "1 if master 0 if replica", m.is_master ? 1 : 0,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("uptime_in_seconds", "", m.uptime, MetricType::GAUGE, &resp->body());

  // Clients metrics
  AppendMetricWithoutLabels("connected_clients", "", m.conn_stats.num_conns, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("client_read_buffer_bytes", "", m.conn_stats.read_buf_capacity,
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
  AppendMetricWithoutLabels("connections_received_total", "", m.conn_stats.conn_received_cnt,
                            MetricType::COUNTER, &resp->body());

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

  // Command stats
  {
    string command_metrics;

    AppendMetricHeader("commands", "Metrics for all commands ran", MetricType::COUNTER,
                       &command_metrics);
    for (const auto& [name, stat] : m.cmd_stats_map) {
      const auto calls = stat.first;
      const auto duration_seconds = stat.second * 0.001;
      AppendMetricValue("commands_total", calls, {"cmd"}, {name}, &command_metrics);
      AppendMetricValue("commands_duration_seconds_total", duration_seconds, {"cmd"}, {name},
                        &command_metrics);
    }
    absl::StrAppend(&resp->body(), command_metrics);
  }

  if (!m.replication_metrics.empty()) {
    string replication_lag_metrics;
    AppendMetricHeader("connected_replica_lag_records", "Lag in records of a connected replica.",
                       MetricType::GAUGE, &replication_lag_metrics);
    for (const auto& replica : m.replication_metrics) {
      AppendMetricValue("connected_replica_lag_records", replica.lsn_lag,
                        {"replica_ip", "replica_port", "replica_state"},
                        {replica.address, absl::StrCat(replica.listening_port), replica.state},
                        &replication_lag_metrics);
    }
    absl::StrAppend(&resp->body(), replication_lag_metrics);
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

std::optional<ReplicaOffsetInfo> ServerFamily::GetReplicaOffsetInfo() {
  unique_lock lk(replicaof_mu_);

  // Switch to primary mode.
  if (!ServerState::tlocal()->is_master) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    return ReplicaOffsetInfo{repl_ptr->GetSyncId(), repl_ptr->GetReplicaOffset()};
  }
  return nullopt;
}

bool ServerFamily::HasReplica() const {
  unique_lock lk(replicaof_mu_);
  return replica_ != nullptr;
}

optional<Replica::Info> ServerFamily::GetReplicaInfo() const {
  unique_lock lk(replicaof_mu_);
  if (replica_ == nullptr) {
    return nullopt;
  } else {
    return replica_->GetInfo();
  }
}

string ServerFamily::GetReplicaMasterId() const {
  unique_lock lk(replicaof_mu_);
  return string(replica_->MasterId());
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

GenericError ServerFamily::DoSave() {
  const CommandId* cid = service().FindCmd("SAVE");
  CHECK_NOTNULL(cid);
  boost::intrusive_ptr<Transaction> trans(new Transaction{cid});
  trans->InitByArgs(0, {});
  return DoSave(absl::GetFlag(FLAGS_df_snapshot_format), {}, trans.get());
}

GenericError ServerFamily::DoSave(bool new_version, string_view basename, Transaction* trans) {
  SaveStagesController sc{SaveStagesInputs{new_version, basename, trans, &service_, &is_saving_,
                                           fq_threadpool_.get(), &last_save_info_, &save_mu_,
                                           &aws_}};
  return sc.Save();
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

void ServerFamily::CancelBlockingCommands() {
  auto cb = [](unsigned thread_index, util::Connection* conn) {
    facade::ConnectionContext* fc = static_cast<facade::Connection*>(conn)->cntx();
    if (fc) {
      ConnectionContext* cntx = static_cast<ConnectionContext*>(fc);
      cntx->CancelBlocking();
    }
  };
  for (auto* listener : listeners_) {
    listener->TraverseConnections(cb);
  }
}

bool ServerFamily::AwaitDispatches(absl::Duration timeout,
                                   const std::function<bool(util::Connection*)>& filter) {
  auto start = absl::Now();
  for (auto* listener : listeners_) {
    absl::Duration remaining_time = timeout - (absl::Now() - start);
    if (remaining_time < absl::Nanoseconds(0) ||
        !listener->AwaitDispatches(remaining_time, filter)) {
      return false;
    }
  }
  return true;
}

string GetPassword() {
  string flag = GetFlag(FLAGS_requirepass);
  if (!flag.empty()) {
    return flag;
  }

  const char* env_var = getenv("DFLY_PASSWORD");
  if (env_var) {
    return env_var;
  }

  return "";
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
  if (args.size() > 2) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  if (args.size() == 2) {
    const auto* registry = ServerState::tlocal()->user_registry;
    std::string_view username = facade::ToSV(args[0]);
    std::string_view password = facade::ToSV(args[1]);
    auto is_authorized = registry->AuthUser(username, password);
    if (is_authorized) {
      cntx->authed_username = username;
      auto cred = registry->GetCredentials(username);
      cntx->acl_categories = cred.acl_categories;
      return (*cntx)->SendOk();
    }
    return (*cntx)->SendError(absl::StrCat("Could not authorize user: ", username));
  }

  if (!cntx->req_auth) {
    return (*cntx)->SendError(
        "AUTH <password> called without any password configured for the "
        "default user. Are you sure your configuration is correct?");
  }

  string_view pass = ArgS(args, 0);
  if (pass == GetPassword()) {
    cntx->authenticated = true;
    (*cntx)->SendOk();
  } else {
    (*cntx)->SendError(facade::kAuthRejected);
  }
}

void ServerFamily::Client(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (sub_cmd == "SETNAME" && args.size() == 2) {
    cntx->owner()->SetName(string{ArgS(args, 1)});
    return (*cntx)->SendOk();
  }

  if (sub_cmd == "GETNAME") {
    auto name = cntx->owner()->GetName();
    if (!name.empty()) {
      return (*cntx)->SendBulkString(name);
    } else {
      return (*cntx)->SendNull();
    }
  }

  if (sub_cmd == "LIST") {
    vector<string> client_info;
    absl::base_internal::SpinLock mu;

    // we can not preempt the connection traversal, so we need to use a spinlock.
    // alternatively we could lock when mutating the connection list, but it seems not important.
    auto cb = [&](unsigned thread_index, util::Connection* conn) {
      facade::Connection* dcon = static_cast<facade::Connection*>(conn);
      string info = dcon->GetClientInfo(thread_index);
      absl::base_internal::SpinLockHolder l(&mu);
      client_info.push_back(move(info));
    };

    for (auto* listener : listeners_) {
      listener->TraverseConnections(cb);
    }

    string result = absl::StrJoin(move(client_info), "\n");
    result.append("\n");
    return (*cntx)->SendBulkString(result);
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "CLIENT"), kSyntaxErrType);
}

void ServerFamily::Config(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (sub_cmd == "SET") {
    if (args.size() < 3) {
      return (*cntx)->SendError(WrongNumArgsError("config|set"));
    }

    ToLower(&args[1]);
    string_view param = ArgS(args, 1);

    ConfigRegistry::SetResult result = config_registry.Set(param, ArgS(args, 2));

    const char kErrPrefix[] = "CONFIG SET failed (possibly related to argument '";
    switch (result) {
      case ConfigRegistry::SetResult::OK:
        return (*cntx)->SendOk();
      case ConfigRegistry::SetResult::UNKNOWN:
        return (*cntx)->SendError(
            absl::StrCat("Unknown option or number of arguments for CONFIG SET - '", param, "'"),
            kConfigErrType);

      case ConfigRegistry::SetResult::READONLY:
        return (*cntx)->SendError(
            absl::StrCat(kErrPrefix, param, "') - can't set immutable config"), kConfigErrType);

      case ConfigRegistry::SetResult::INVALID:
        return (*cntx)->SendError(absl::StrCat(kErrPrefix, param, "') - argument can not be set"),
                                  kConfigErrType);
    }
    ABSL_UNREACHABLE();
  }

  if (sub_cmd == "GET" && args.size() == 2) {
    // Send empty response, like Redis does, unless the param is supported

    string_view param = ArgS(args, 1);
    vector<string> names = config_registry.List(param);
    vector<string> res;

    for (const auto& name : names) {
      absl::CommandLineFlag* flag = CHECK_NOTNULL(absl::FindCommandLineFlag(name));
      res.push_back(name);
      res.push_back(flag->CurrentValue());
    }

    return (*cntx)->SendStringArr(res, RedisReplyBuilder::MAP);
  }

  if (sub_cmd == "RESETSTAT") {
    shard_set->pool()->Await([registry = service_.mutable_registry()](unsigned index, auto*) {
      registry->ResetCallStats(index);
      auto& sstate = *ServerState::tlocal();
      auto& stats = sstate.connection_stats;
      stats.err_count_map.clear();
      stats.command_cnt = 0;
      stats.async_writes_cnt = 0;
    });

    return (*cntx)->SendOk();
  } else {
    return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "CONFIG"), kSyntaxErrType);
  }
}

void ServerFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);

  DebugCmd dbg_cmd{this, cntx};

  return dbg_cmd.Run(args);
}

void ServerFamily::Memory(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);

  MemoryCmd mem_cmd{this, cntx};

  return mem_cmd.Run(args);
}

void ServerFamily::Save(CmdArgList args, ConnectionContext* cntx) {
  string err_detail;
  bool new_version = absl::GetFlag(FLAGS_df_snapshot_format);
  if (args.size() > 2) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  if (args.size() >= 1) {
    ToUpper(&args[0]);
    string_view sub_cmd = ArgS(args, 0);
    if (sub_cmd == "DF") {
      new_version = true;
    } else if (sub_cmd == "RDB") {
      new_version = false;
    } else {
      return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "SAVE"), kSyntaxErrType);
    }
  }

  string_view basename;
  if (args.size() == 2) {
    basename = ArgS(args, 1);
  }

  GenericError ec = DoSave(new_version, basename, cntx->transaction);
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

  Mutex mu;

  auto cb = [&](unsigned index, ProactorBase* pb) {
    EngineShard* shard = EngineShard::tlocal();
    ServerState* ss = ServerState::tlocal();

    lock_guard lk(mu);

    result.uptime = time(NULL) - this->start_time_;
    result.conn_stats += ss->connection_stats;
    result.qps += uint64_t(ss->MovingSum6());
    result.ooo_tx_transaction_cnt += ss->stats.ooo_tx_cnt;

    service_.mutable_registry()->MergeCallStats(
        index, [&dest_map = result.cmd_stats_map](string_view name, const CmdCallStats& src) {
          auto& ent = dest_map[string{name}];
          ent.first += src.first;
          ent.second += src.second;
        });

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

  result.is_master = false;
  if (ServerState::tlocal() && ServerState::tlocal()->is_master) {
    result.is_master = true;
    result.replication_metrics = dfly_cmd_->GetReplicasRoleInfo();
  }

  return result;
}

void ServerFamily::Info(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  string_view section;

  if (args.size() == 1) {
    ToUpper(&args[0]);
    section = ArgS(args, 0);
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
    auto kind = ProactorBase::me()->GetKind();
    const char* multiplex_api = (kind == ProactorBase::IOURING) ? "iouring" : "epoll";

    ADD_HEADER("# Server");

    append("redis_version", kRedisVersion);
    append("dragonfly_version", GetVersion());
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
    append("client_read_buffer_bytes", m.conn_stats.read_buf_capacity);
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
    append("pipeline_cache_bytes", m.conn_stats.pipeline_cache_capacity);
    append("maxmemory", max_memory_limit);
    append("maxmemory_human", HumanReadableNumBytes(max_memory_limit));
    if (GetFlag(FLAGS_cache_mode)) {
      append("cache_mode", "cache");
    } else {
      append("cache_mode", "store");
      // Compatible with redis based frameworks.
      append("maxmemory_policy", "noeviction");
    }
  }

  if (should_enter("STATS")) {
    ADD_HEADER("# Stats");

    append("total_connections_received", m.conn_stats.conn_received_cnt);
    append("total_commands_processed", m.conn_stats.command_cnt);
    append("instantaneous_ops_per_sec", m.qps);
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
    append("oom_rejections", m.events.insertion_rejections);
    append("traverse_ttl_sec", m.traverse_ttl_per_sec);
    append("delete_ttl_sec", m.delete_ttl_per_sec);
    append("keyspace_hits", m.events.hits);
    append("keyspace_misses", m.events.misses);
    append("total_reads_processed", m.conn_stats.io_read_cnt);
    append("total_writes_processed", m.conn_stats.io_write_cnt);
    append("async_writes_count", m.conn_stats.async_writes_cnt);
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
    size_t is_loading = service_.GetGlobalState() == GlobalState::LOADING;
    append("loading", is_loading);

    for (const auto& k_v : save_info->freq_map) {
      append(StrCat("rdb_", k_v.first), k_v.second);
    }
    append("rdb_changes_since_last_save", m.events.update);
  }

  if (should_enter("REPLICATION")) {
    ADD_HEADER("# Replication");

    ServerState& etl = *ServerState::tlocal();

    if (etl.is_master) {
      append("role", "master");
      append("connected_slaves", m.conn_stats.num_replicas);
      const auto& replicas = m.replication_metrics;
      for (size_t i = 0; i < replicas.size(); i++) {
        auto& r = replicas[i];
        // e.g. slave0:ip=172.19.0.3,port=6379,state=full_sync
        append(StrCat("slave", i), StrCat("ip=", r.address, ",port=", r.listening_port,
                                          ",state=", r.state, ",lag=", r.lsn_lag));
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
      append("master_sync_in_progress", rinfo.full_sync_in_progress);
    }
  }

  if (should_enter("COMMANDSTATS", true)) {
    ADD_HEADER("# Commandstats");

    auto unknown_cmd = service_.UknownCmdMap();

    auto append_sorted = [&append](string_view prefix, auto display) {
      sort(display.begin(), display.end());

      for (const auto& k_v : display) {
        append(StrCat(prefix, k_v.first), k_v.second);
      }
    };

    vector<pair<string_view, string>> commands;

    for (const auto& [name, stats] : m.cmd_stats_map) {
      const auto calls = stats.first, sum = stats.second;
      commands.push_back(
          {name, absl::StrJoin({absl::StrCat("calls=", calls), absl::StrCat("usec=", sum),
                                absl::StrCat("usec_per_call=", static_cast<double>(sum) / calls)},
                               ",")});
    }

    append_sorted("cmdstat_", move(commands));
    append_sorted("unknown_",
                  vector<pair<string_view, uint64_t>>(unknown_cmd.cbegin(), unknown_cmd.cend()));
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

  if (should_enter("CLUSTER")) {
    ADD_HEADER("# Cluster");
    append("cluster_enabled", ClusterConfig::IsEnabledOrEmulated());
  }

  (*cntx)->SendBulkString(info);
}

void ServerFamily::Hello(CmdArgList args, ConnectionContext* cntx) {
  // If no arguments are provided default to RESP2.
  bool is_resp3 = false;
  bool has_auth = false;
  bool has_setname = false;
  string_view username;
  string_view password;
  string_view clientname;

  if (args.size() > 0) {
    string_view proto_version = ArgS(args, 0);
    is_resp3 = proto_version == "3";
    bool valid_proto_version = proto_version == "2" || is_resp3;
    if (!valid_proto_version) {
      (*cntx)->SendError(UnknownCmd("HELLO", args));
      return;
    }

    for (uint32_t i = 1; i < args.size(); i++) {
      auto sub_cmd = ArgS(args, i);
      auto moreargs = args.size() - 1 - i;
      if (absl::EqualsIgnoreCase(sub_cmd, "AUTH") && moreargs >= 2) {
        has_auth = true;
        username = ArgS(args, i + 1);
        password = ArgS(args, i + 2);
        i += 2;
      } else if (absl::EqualsIgnoreCase(sub_cmd, "SETNAME") && moreargs > 0) {
        has_setname = true;
        clientname = ArgS(args, i + 1);
        i += 1;
      } else {
        (*cntx)->SendError(kSyntaxErr);
        return;
      }
    }
  }

  if (has_auth) {
    if (username == "default" && password == GetPassword()) {
      cntx->authenticated = true;
    } else {
      (*cntx)->SendError(facade::kAuthRejected);
      return;
    }
  }

  if (cntx->req_auth && !cntx->authenticated) {
    (*cntx)->SendError(
        "-NOAUTH HELLO must be called with the client already "
        "authenticated, otherwise the HELLO <proto> AUTH <user> <pass> "
        "option can be used to authenticate the client and "
        "select the RESP protocol version at the same time");
    return;
  }

  if (has_setname) {
    cntx->owner()->SetName(string{clientname});
  }

  int proto_version = 2;
  if (is_resp3) {
    proto_version = 3;
    (*cntx)->SetResp3(true);
  } else {
    // Issuing hello 2 again is valid and should switch back to RESP2
    (*cntx)->SetResp3(false);
  }

  (*cntx)->StartCollection(7, RedisReplyBuilder::MAP);
  (*cntx)->SendBulkString("server");
  (*cntx)->SendBulkString("redis");
  (*cntx)->SendBulkString("version");
  (*cntx)->SendBulkString(kRedisVersion);
  (*cntx)->SendBulkString("dragonfly_version");
  (*cntx)->SendBulkString(GetVersion());
  (*cntx)->SendBulkString("proto");
  (*cntx)->SendLong(proto_version);
  (*cntx)->SendBulkString("id");
  (*cntx)->SendLong(cntx->owner()->GetClientId());
  (*cntx)->SendBulkString("mode");
  (*cntx)->SendBulkString("standalone");
  (*cntx)->SendBulkString("role");
  (*cntx)->SendBulkString((*ServerState::tlocal()).is_master ? "master" : "slave");
}

void ServerFamily::ReplicaOfInternal(string_view host, string_view port_sv, ConnectionContext* cntx,
                                     ActionOnConnectionFail on_err) {
  auto& pool = service_.proactor_pool();
  LOG(INFO) << "Replicating " << host << ":" << port_sv;

  // We lock to protect global state changes that we perform during the replication setup:
  // The replica_ pointer, GlobalState, and the DB itself (we do a flushall txn before syncing).
  // The lock is only released during replica_->Start because we want to allow cancellation during
  // the connection. If another replication command is received during Start() of an old
  // replication, it will acquire the lock, call Stop() on the old replica_ and wait for Stop() to
  // complete. So Replica::Stop() must
  // 1. Be very responsive, as it is called while holding the lock.
  // 2. Leave the DB in a consistent state after it is done.
  // We have a relatively involved state machine inside Replica itself which handels cancellation
  // with those requirements.
  VLOG(1) << "Acquire replica lock";
  unique_lock lk(replicaof_mu_);

  if (IsReplicatingNoOne(host, port_sv)) {
    if (!ServerState::tlocal()->is_master) {
      auto repl_ptr = replica_;
      CHECK(repl_ptr);

      pool.AwaitFiberOnAll(
          [&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = true; });
      replica_->Stop();
      replica_.reset();
    }

    CHECK(service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE) == GlobalState::ACTIVE)
        << "Server is set to replica no one, yet state is not active!";

    return (*cntx)->SendOk();
  }

  uint32_t port;

  if (!absl::SimpleAtoi(port_sv, &port) || port < 1 || port > 65535) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto new_replica = make_shared<Replica>(string(host), port, &service_, master_id());

  if (replica_) {
    replica_->Stop();  // NOTE: consider introducing update API flow.
  } else {
    // TODO: to disconnect all the blocked clients (pubsub, blpop etc)

    pool.AwaitFiberOnAll([&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = false; });
  }
  replica_ = new_replica;

  GlobalState new_state = service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  if (new_state != GlobalState::LOADING) {
    LOG(WARNING) << GlobalStateName(new_state) << " in progress, ignored";
    return;
  }

  // Replica sends response in either case. No need to send response in this function.
  // It's a bit confusing but simpler.
  lk.unlock();
  error_code ec{};

  switch (on_err) {
    case ActionOnConnectionFail::kReturnOnError:
      ec = new_replica->Start(cntx);
      break;
    case ActionOnConnectionFail::kContinueReplication:  // set DF to replicate, and forget about it
      new_replica->EnableReplication(cntx);
      break;
  };

  VLOG(1) << "Acquire replica lock";
  lk.lock();

  // Since we released the replication lock during Start(..), we need to check if this still the
  // last replicaof command we got. If it's not, then we were cancelled and just exit.
  if (replica_ == new_replica) {
    if (ec) {
      service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
      replica_->Stop();
      replica_.reset();
    }
    bool is_master = !replica_;
    pool.AwaitFiberOnAll(
        [&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = is_master; });
  } else {
    new_replica->Stop();
  }
}

void ServerFamily::ReplicaOf(CmdArgList args, ConnectionContext* cntx) {
  string_view host = ArgS(args, 0);
  string_view port = ArgS(args, 1);

  // don't flush if input is NO ONE
  if (!IsReplicatingNoOne(host, port))
    Drakarys(cntx->transaction, DbSlice::kDbAll);

  ReplicaOfInternal(host, port, cntx, ActionOnConnectionFail::kReturnOnError);
}

void ServerFamily::Replicate(string_view host, string_view port) {
  io::NullSink sink;
  ConnectionContext ctxt{&sink, nullptr};

  // we don't flush the database as the context is null
  // (and also because there is nothing to flush)

  ReplicaOfInternal(host, port, &ctxt, ActionOnConnectionFail::kContinueReplication);
}

void ServerFamily::ReplTakeOver(CmdArgList args, ConnectionContext* cntx) {
  VLOG(1) << "Starting take over";
  VLOG(1) << "Acquire replica lock";
  unique_lock lk(replicaof_mu_);

  float_t timeout_sec;
  if (!absl::SimpleAtof(ArgS(args, 0), &timeout_sec)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  if (timeout_sec < 0) {
    return (*cntx)->SendError("timeout is negative");
  }

  if (ServerState::tlocal()->is_master)
    return (*cntx)->SendError("Already a master instance");
  auto repl_ptr = replica_;
  CHECK(repl_ptr);

  auto info = replica_->GetInfo();
  if (!info.full_sync_done) {
    return (*cntx)->SendError("Full sync not done");
  }

  std::error_code ec = replica_->TakeOver(ArgS(args, 0));
  if (ec)
    return (*cntx)->SendError("Couldn't execute takeover");

  LOG(INFO) << "Takeover successful, promoting this instance to master.";
  service_.proactor_pool().AwaitFiberOnAll(
      [&](util::ProactorBase* pb) { ServerState::tlocal()->is_master = true; });
  replica_->Stop();
  replica_.reset();
  return (*cntx)->SendOk();
}

void ServerFamily::ReplConf(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() % 2 == 1)
    goto err;
  for (unsigned i = 0; i < args.size(); i += 2) {
    DCHECK_LT(i + 1, args.size());
    ToUpper(&args[i]);

    std::string_view cmd = ArgS(args, i);
    std::string_view arg = ArgS(args, i + 1);
    if (cmd == "CAPA") {
      if (arg == "dragonfly" && args.size() == 2 && i == 0) {
        auto [sid, replica_info] = dfly_cmd_->CreateSyncSession(cntx);
        cntx->owner()->SetName(absl::StrCat("repl_ctrl_", sid));

        string sync_id = absl::StrCat("SYNC", sid);
        cntx->conn_state.replication_info.repl_session_id = sid;

        if (!cntx->replica_conn) {
          ServerState::tl_connection_stats()->num_replicas += 1;
        }
        cntx->replica_conn = true;

        // The response for 'capa dragonfly' is: <masterid> <syncid> <numthreads> <version>
        (*cntx)->StartArray(4);
        (*cntx)->SendSimpleString(master_id_);
        (*cntx)->SendSimpleString(sync_id);
        (*cntx)->SendLong(replica_info->flows.size());
        (*cntx)->SendLong(unsigned(DflyVersion::CURRENT_VER));
        return;
      }
    } else if (cmd == "LISTENING-PORT") {
      uint32_t replica_listening_port;
      if (!absl::SimpleAtoi(arg, &replica_listening_port)) {
        (*cntx)->SendError(kInvalidIntErr);
        return;
      }
      cntx->conn_state.replication_info.repl_listening_port = replica_listening_port;
    } else if (cmd == "CLIENT-ID" && args.size() == 2) {
      std::string client_id{arg};
      auto& pool = service_.proactor_pool();
      pool.AwaitFiberOnAll(
          [&](util::ProactorBase* pb) { ServerState::tlocal()->remote_client_id_ = arg; });
    } else if (cmd == "CLIENT-VERSION" && args.size() == 2) {
      unsigned version;
      if (!absl::SimpleAtoi(arg, &version)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
      VLOG(1) << "Client version for session_id="
              << cntx->conn_state.replication_info.repl_session_id << " is " << version;
      cntx->conn_state.replication_info.repl_version = DflyVersion(version);
    } else if (cmd == "ACK" && args.size() == 2) {
      // Don't send error/Ok back through the socket, because we don't want to interleave with
      // the journal writes that we write into the same socket.

      if (!cntx->replication_flow) {
        LOG(ERROR) << "No replication flow assigned";
        return;
      }

      uint64_t ack;
      if (!absl::SimpleAtoi(arg, &ack)) {
        LOG(ERROR) << "Bad int in REPLCONF ACK command! arg=" << arg;
        return;
      }
      VLOG(1) << "Received client ACK=" << ack;
      cntx->replication_flow->last_acked_lsn = ack;
      return;
    } else {
      VLOG(1) << cmd << " " << arg << " " << args.size();
      goto err;
    }
  }

  (*cntx)->SendOk();
  return;

err:
  LOG(ERROR) << "Error in receiving command: " << args;
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
      (*cntx)->StartArray(3);
      (*cntx)->SendBulkString(data.address);
      (*cntx)->SendBulkString(absl::StrCat(data.listening_port));
      (*cntx)->SendBulkString(data.state);
    }

  } else {
    auto replica_ptr = replica_;
    CHECK(replica_ptr);
    Replica::Info rinfo = replica_ptr->GetInfo();
    (*cntx)->StartArray(4);
    (*cntx)->SendBulkString("replica");
    (*cntx)->SendBulkString(rinfo.host);
    (*cntx)->SendBulkString(absl::StrCat(rinfo.port));
    if (rinfo.full_sync_done) {
      (*cntx)->SendBulkString("stable_sync");
    } else if (rinfo.full_sync_in_progress) {
      (*cntx)->SendBulkString("full_sync");
    } else if (rinfo.master_link_established) {
      (*cntx)->SendBulkString("preparation");
    } else {
      (*cntx)->SendBulkString("connecting");
    }
  }
}

void ServerFamily::Script(CmdArgList args, ConnectionContext* cntx) {
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
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (sub_cmd == "LATEST") {
    return (*cntx)->SendEmptyArray();
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  (*cntx)->SendError(kSyntaxErr);
}

void ServerFamily::ShutdownCmd(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  if (args.size() == 1) {
    auto sub_cmd = ArgS(args, 0);
    if (absl::EqualsIgnoreCase(sub_cmd, "SAVE")) {
    } else if (absl::EqualsIgnoreCase(sub_cmd, "NOSAVE")) {
      save_on_shutdown_ = false;
    } else {
      (*cntx)->SendError(kSyntaxErr);
      return;
    }
  }

  service_.proactor_pool().AwaitFiberOnAll(
      [](ProactorBase* pb) { ServerState::tlocal()->EnterLameDuck(); });

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

namespace acl {
constexpr uint32_t kAuth = FAST | CONNECTION;
constexpr uint32_t kBGSave = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kClient = SLOW | CONNECTION;
constexpr uint32_t kConfig = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kDbSize = KEYSPACE | READ | FAST;
constexpr uint32_t kDebug = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kFlushDB = KEYSPACE | WRITE | SLOW | DANGEROUS;
constexpr uint32_t kFlushAll = KEYSPACE | WRITE | SLOW | DANGEROUS;
constexpr uint32_t kInfo = SLOW | DANGEROUS;
constexpr uint32_t kHello = FAST | CONNECTION;
constexpr uint32_t kLastSave = ADMIN | FAST | DANGEROUS;
constexpr uint32_t kLatency = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kMemory = READ | SLOW;
constexpr uint32_t kSave = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kShutDown = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kSlaveOf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kReplicaOf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kReplTakeOver = DANGEROUS;
constexpr uint32_t kReplConf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kRole = ADMIN | FAST | DANGEROUS;
constexpr uint32_t kSlowLog = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kScript = SLOW | SCRIPTING;
constexpr uint32_t kDfly = ADMIN;
}  // namespace acl

void ServerFamily::Register(CommandRegistry* registry) {
  constexpr auto kReplicaOpts = CO::LOADING | CO::ADMIN | CO::GLOBAL_TRANS;
  constexpr auto kMemOpts = CO::LOADING | CO::READONLY | CO::FAST | CO::NOSCRIPT;

  *registry
      << CI{"AUTH", CO::NOSCRIPT | CO::FAST | CO::LOADING, -2, 0, 0, 0, acl::kAuth}.HFUNC(Auth)
      << CI{"BGSAVE", CO::ADMIN | CO::GLOBAL_TRANS, 1, 0, 0, 0, acl::kBGSave}.HFUNC(Save)
      << CI{"CLIENT", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, 0, acl::kClient}.HFUNC(Client)
      << CI{"CONFIG", CO::ADMIN, -2, 0, 0, 0, acl::kConfig}.HFUNC(Config)
      << CI{"DBSIZE", CO::READONLY | CO::FAST | CO::LOADING, 1, 0, 0, 0, acl::kDbSize}.HFUNC(DbSize)
      << CI{"DEBUG", CO::ADMIN | CO::LOADING, -2, 0, 0, 0, acl::kDebug}.HFUNC(Debug)
      << CI{"FLUSHDB", CO::WRITE | CO::GLOBAL_TRANS, 1, 0, 0, 0, acl::kFlushDB}.HFUNC(FlushDb)
      << CI{"FLUSHALL", CO::WRITE | CO::GLOBAL_TRANS, -1, 0, 0, 0, acl::kFlushAll}.HFUNC(FlushAll)
      << CI{"INFO", CO::LOADING, -1, 0, 0, 0, acl::kInfo}.HFUNC(Info)
      << CI{"HELLO", CO::LOADING, -1, 0, 0, 0, acl::kHello}.HFUNC(Hello)
      << CI{"LASTSAVE", CO::LOADING | CO::FAST, 1, 0, 0, 0, acl::kLastSave}.HFUNC(LastSave)
      << CI{"LATENCY", CO::NOSCRIPT | CO::LOADING | CO::FAST, -2, 0, 0, 0, acl::kLatency}.HFUNC(
             Latency)
      << CI{"MEMORY", kMemOpts, -2, 0, 0, 0, acl::kMemory}.HFUNC(Memory)
      << CI{"SAVE", CO::ADMIN | CO::GLOBAL_TRANS, -1, 0, 0, 0, acl::kSave}.HFUNC(Save)
      << CI{"SHUTDOWN", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, -1, 0, 0, 0, acl::kShutDown}.HFUNC(
             ShutdownCmd)
      << CI{"SLAVEOF", kReplicaOpts, 3, 0, 0, 0, acl::kSlaveOf}.HFUNC(ReplicaOf)
      << CI{"REPLICAOF", kReplicaOpts, 3, 0, 0, 0, acl::kReplicaOf}.HFUNC(ReplicaOf)
      << CI{"REPLTAKEOVER", CO::ADMIN | CO::GLOBAL_TRANS, 2, 0, 0, 0, acl::kReplTakeOver}.HFUNC(
             ReplTakeOver)
      << CI{"REPLCONF", CO::ADMIN | CO::LOADING, -1, 0, 0, 0, acl::kReplConf}.HFUNC(ReplConf)
      << CI{"ROLE", CO::LOADING | CO::FAST | CO::NOSCRIPT, 1, 0, 0, 0, acl::kRole}.HFUNC(Role)
      << CI{"SLOWLOG", CO::ADMIN | CO::FAST, -2, 0, 0, 0, acl::kSlowLog}.SetHandler(SlowLog)
      << CI{"SCRIPT", CO::NOSCRIPT | CO::NO_KEY_JOURNAL, -2, 0, 0, 0, acl::kScript}.HFUNC(Script)
      << CI{"DFLY", CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN, -2, 0, 0, 0, acl::kDfly}.HFUNC(Dfly);
}

}  // namespace dfly
