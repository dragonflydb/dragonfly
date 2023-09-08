
// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/save_stages_controller.h"

#include <absl/strings/match.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>

#include "base/flags.h"
#include "base/logging.h"
#include "io/file_util.h"
#include "server/main_service.h"
#include "server/script_mgr.h"
#include "server/search/doc_index.h"
#include "server/transaction.h"
#include "strings/human_readable.h"
#include "util/cloud/s3.h"
#include "util/fibers/fiber_file.h"
#include "util/uring/uring_file.h"

using namespace std;

ABSL_DECLARE_FLAG(string, dir);
ABSL_DECLARE_FLAG(string, dbfilename);

namespace dfly {
namespace detail {

using namespace util;
using absl::GetFlag;
using absl::StrCat;
using fb2::OpenLinux;

namespace fs = std::filesystem;

namespace {

const size_t kBucketConnectMs = 2000;

#ifdef __linux__
const int kRdbWriteFlags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_DIRECT;
#endif

constexpr string_view kS3Prefix = "s3://"sv;

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

string FormatTs(absl::Time now) {
  return absl::FormatTime("%Y-%m-%dT%H:%M:%S", now, absl::LocalTimeZone());
}

void SubstituteFilenameTsPlaceholder(fs::path* filename, std::string_view replacement) {
  *filename = absl::StrReplaceAll(filename->string(), {{"{timestamp}", replacement}});
}

// Create a directory and all its parents if they don't exist.
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

// modifies 'filename' to be "filename-postfix.extension"
void SetExtension(absl::AlphaNum postfix, string_view extension, fs::path* filename) {
  filename->replace_extension();  // clear if exists
  *filename += StrCat("-", postfix, extension);
}

void ExtendDfsFilenameWithShard(int shard, string_view extension, fs::path* filename) {
  // dragonfly snapshot.
  SetExtension(absl::Dec(shard, absl::kZeroPad4), extension, filename);
}

// takes ownership over the file.
class LinuxWriteWrapper : public io::Sink {
 public:
  LinuxWriteWrapper(fb2::LinuxFile* lf) : lf_(lf) {
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  error_code Close() {
    return lf_->Close();
  }

 private:
  unique_ptr<fb2::LinuxFile> lf_;
  off_t offset_ = 0;
};

io::Result<size_t> LinuxWriteWrapper::WriteSome(const iovec* v, uint32_t len) {
  io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
  if (res) {
    offset_ += *res;
  }

  return res;
}

}  // namespace

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

FileSnapshotStorage::FileSnapshotStorage(FiberQueueThreadPool* fq_threadpool)
    : fq_threadpool_{fq_threadpool} {
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> FileSnapshotStorage::OpenFile(
    const std::string& path) {
  if (fq_threadpool_) {  // EPOLL
    auto res = util::OpenFiberWriteFile(path, fq_threadpool_);
    if (!res) {
      return nonstd::make_unexpected(GenericError(res.error(), "Couldn't open file for writing"));
    }

    return std::pair(*res, FileType::FILE);
  } else {
#ifdef __linux__
    auto res = OpenLinux(path, kRdbWriteFlags, 0666);
    if (!res) {
      return nonstd::make_unexpected(GenericError(
          res.error(),
          "Couldn't open file for writing (is direct I/O supported by the file system?)"));
    }

    uint8_t file_type = FileType::FILE | FileType::IO_URING;
    if (kRdbWriteFlags & O_DIRECT) {
      file_type |= FileType::DIRECT;
    }
    return std::pair(new LinuxWriteWrapper(res->release()), file_type);
#else
    LOG(FATAL) << "Linux I/O is not supported on this platform";
#endif
  }
}

AwsS3SnapshotStorage::AwsS3SnapshotStorage(util::cloud::AWS* aws) : aws_{aws} {
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> AwsS3SnapshotStorage::OpenFile(
    const std::string& path) {
  DCHECK(aws_);

  optional<pair<string, string>> bucket_path = GetBucketPath(path);
  if (!bucket_path) {
    return nonstd::make_unexpected(GenericError("Invalid S3 path"));
  }
  auto [bucket_name, obj_path] = *bucket_path;

  cloud::S3Bucket bucket(*aws_, bucket_name);
  error_code ec = bucket.Connect(kBucketConnectMs);
  if (ec) {
    return nonstd::make_unexpected(GenericError(ec, "Couldn't connect to S3 bucket"));
  }
  auto res = bucket.OpenWriteFile(obj_path);
  if (!res) {
    return nonstd::make_unexpected(GenericError(res.error(), "Couldn't open file for writing"));
  }

  return std::pair<io::Sink*, uint8_t>(*res, FileType::CLOUD);
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

GenericError RdbSnapshot::Start(SaveMode save_mode, const std::string& path,
                                const RdbSaver::GlobalData& glob_data) {
  VLOG(1) << "Saving RDB " << path;

  auto res = snapshot_storage_->OpenFile(path);
  if (!res) {
    return res.error();
  }

  auto [file, file_type] = *res;
  io_sink_.reset(file);

  is_linux_file_ = file_type & FileType::IO_URING;

  saver_.reset(new RdbSaver(io_sink_.get(), save_mode, file_type | FileType::DIRECT));

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

SaveStagesController::SaveStagesController(SaveStagesInputs&& inputs)
    : SaveStagesInputs{move(inputs)} {
  start_time_ = absl::Now();
}

SaveStagesController::~SaveStagesController() {
  service_->SwitchState(GlobalState::SAVING, GlobalState::ACTIVE);
}

GenericError SaveStagesController::Save() {
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

// In the new version (.dfs) we store a file for every shard and one more summary file.
// Summary file is always last in snapshots array.
void SaveStagesController::SaveDfs() {
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
void SaveStagesController::SaveDfsSingle(EngineShard* shard) {
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
void SaveStagesController::SaveRdb() {
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

void SaveStagesController::UpdateSaveInfo() {
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

GenericError SaveStagesController::InitResources() {
  if (is_cloud_ && !aws_) {
    *aws_ = make_unique<cloud::AWS>("s3");
    if (auto ec = aws_->get()->Init(); ec) {
      aws_->reset();
      return {ec, "Couldn't initialize AWS"};
    }
  }

  snapshots_.resize(use_dfs_format_ ? shard_set->size() + 1 : 1);
  for (auto& [snapshot, _] : snapshots_)
    snapshot = make_unique<RdbSnapshot>(fq_threadpool_, snapshot_storage_.get());
  return {};
}

// Remove .tmp extension or delete files in case of error
void SaveStagesController::FinalizeFileMovement() {
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
GenericError SaveStagesController::BuildFullPath() {
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
GenericError SaveStagesController::SwitchState() {
  GlobalState new_state = service_->SwitchState(GlobalState::ACTIVE, GlobalState::SAVING);
  if (new_state != GlobalState::SAVING && new_state != GlobalState::TAKEN_OVER)
    return {make_error_code(errc::operation_in_progress),
            StrCat(GlobalStateName(new_state), " - can not save database")};
  return {};
}

void SaveStagesController::SaveCb(unsigned index) {
  if (auto& snapshot = snapshots_[index].first; snapshot && snapshot->HasStarted())
    shared_err_ = snapshot->SaveBody();
}

void SaveStagesController::CloseCb(unsigned index) {
  if (auto& snapshot = snapshots_[index].first; snapshot) {
    shared_err_ = snapshot->Close();

    lock_guard lk{rdb_name_map_mu_};
    for (const auto& k_v : snapshot->freq_map())
      rdb_name_map_[RdbTypeName(k_v.first)] += k_v.second;
  }

  if (auto* es = EngineShard::tlocal(); use_dfs_format_ && es)
    es->db_slice().ResetUpdateEvents();
}

void SaveStagesController::RunStage(void (SaveStagesController::*cb)(unsigned)) {
  if (use_dfs_format_) {
    shard_set->RunBlockingInParallel([&](EngineShard* es) { (this->*cb)(es->shard_id()); });
    (this->*cb)(shard_set->size());
  } else {
    (this->*cb)(0);
  }
}

RdbSaver::GlobalData SaveStagesController::GetGlobalData() const {
  StringVec script_bodies, search_indices;

  {
    auto scripts = service_->script_mgr()->GetAll();
    script_bodies.reserve(scripts.size());
    for (auto& [sha, data] : scripts)
      script_bodies.push_back(move(data.body));
  }

#ifndef __APPLE__
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
#endif

  return RdbSaver::GlobalData{move(script_bodies), move(search_indices)};
}

}  // namespace detail
}  // namespace dfly
