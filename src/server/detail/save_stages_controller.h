
// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <filesystem>

#include "server/rdb_save.h"
#include "server/server_family.h"
#include "util/cloud/aws.h"
#include "util/fibers/fiberqueue_threadpool.h"

namespace dfly {

class Transaction;
class Service;

namespace detail {

enum FileType : uint8_t {
  FILE = (1u << 0),
  CLOUD = (1u << 1),
  IO_URING = (1u << 2),
  DIRECT = (1u << 3),
};

class SnapshotStorage {
 public:
  virtual ~SnapshotStorage() = default;

  // Opens the file at the given path, and returns the open file and file
  // type, which is a bitmask of FileType.
  virtual io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenFile(
      const std::string& path) = 0;
};

class FileSnapshotStorage : public SnapshotStorage {
 public:
  FileSnapshotStorage(FiberQueueThreadPool* fq_threadpool);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenFile(
      const std::string& path) override;

 private:
  util::fb2::FiberQueueThreadPool* fq_threadpool_;
};

class AwsS3SnapshotStorage : public SnapshotStorage {
 public:
  AwsS3SnapshotStorage(util::cloud::AWS* aws);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenFile(
      const std::string& path) override;

 private:
  util::cloud::AWS* aws_;
};

struct SaveStagesInputs {
  bool use_dfs_format_;
  std::string_view basename_;
  Transaction* trans_;
  Service* service_;
  std::atomic_bool* is_saving_;
  util::fb2::FiberQueueThreadPool* fq_threadpool_;
  std::shared_ptr<LastSaveInfo>* last_save_info_;
  util::fb2::Mutex* save_mu_;
  std::unique_ptr<util::cloud::AWS>* aws_;
  std::shared_ptr<SnapshotStorage> snapshot_storage_;
};

class RdbSnapshot {
 public:
  RdbSnapshot(FiberQueueThreadPool* fq_tp, SnapshotStorage* snapshot_storage)
      : fq_tp_{fq_tp}, snapshot_storage_{snapshot_storage} {
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
  util::fb2::FiberQueueThreadPool* fq_tp_ = nullptr;
  SnapshotStorage* snapshot_storage_ = nullptr;

  unique_ptr<io::Sink> io_sink_;
  unique_ptr<RdbSaver> saver_;
  RdbTypeFreqMap freq_map_;

  Cancellation cll_{};
};

struct SaveStagesController : public SaveStagesInputs {
  SaveStagesController(SaveStagesInputs&& inputs);

  ~SaveStagesController();

  GenericError Save();

 private:
  // In the new version (.dfs) we store a file for every shard and one more summary file.
  // Summary file is always last in snapshots array.
  void SaveDfs();

  // Start saving a dfs file on shard
  void SaveDfsSingle(EngineShard* shard);

  // Save a single rdb file
  void SaveRdb();

  void UpdateSaveInfo();

  GenericError InitResources();

  // Remove .tmp extension or delete files in case of error
  void FinalizeFileMovement();

  // Build full path: get dir, try creating dirs, get filename with placeholder
  GenericError BuildFullPath();

  // Switch to saving state if in active state
  GenericError SwitchState();

  void SaveCb(unsigned index);

  void CloseCb(unsigned index);

  void RunStage(void (SaveStagesController::*cb)(unsigned));

  RdbSaver::GlobalData GetGlobalData() const;

 private:
  absl::Time start_time_;
  std::filesystem::path full_path_;
  bool is_cloud_;

  AggregateGenericError shared_err_;
  std::vector<std::pair<std::unique_ptr<RdbSnapshot>, std::filesystem::path>> snapshots_;

  absl::flat_hash_map<string_view, size_t> rdb_name_map_;
  Mutex rdb_name_map_mu_;
};

GenericError ValidateFilename(const std::filesystem::path& filename, bool new_version);

std::string InferLoadFile(string_view dir, util::cloud::AWS* aws);

}  // namespace detail
}  // namespace dfly
