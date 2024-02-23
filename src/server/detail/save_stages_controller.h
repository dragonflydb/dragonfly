
// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <filesystem>

#include "server/rdb_save.h"
#include "util/fibers/fiberqueue_threadpool.h"

namespace dfly {

class Transaction;
class Service;

namespace detail {

class SnapshotStorage;

struct SaveInfo {
  time_t save_time = 0;  // epoch time in seconds.
  uint32_t duration_sec = 0;
  std::string file_name;
  std::vector<std::pair<std::string_view, size_t>> freq_map;  // RDB_TYPE_xxx -> count mapping.
  GenericError error;
};

struct SaveStagesInputs {
  bool use_dfs_format_;
  std::string_view basename_;
  Transaction* trans_;
  Service* service_;
  util::fb2::FiberQueueThreadPool* fq_threadpool_;
  std::shared_ptr<SnapshotStorage> snapshot_storage_;
};

class RdbSnapshot {
 public:
  RdbSnapshot(util::fb2::FiberQueueThreadPool* fq_tp, SnapshotStorage* snapshot_storage)
      : snapshot_storage_{snapshot_storage} {
  }

  GenericError Start(SaveMode save_mode, const string& path, const RdbSaver::GlobalData& glob_data);
  void StartInShard(EngineShard* shard);

  error_code SaveBody();
  error_code Close();
  size_t GetSaveBuffersSize();

  const RdbTypeFreqMap& freq_map() const {
    return freq_map_;
  }

  bool HasStarted() const {
    return started_ || (saver_ && saver_->Mode() == SaveMode::SUMMARY);
  }

 private:
  bool started_ = false;
  bool is_linux_file_ = false;
  SnapshotStorage* snapshot_storage_ = nullptr;

  unique_ptr<io::Sink> io_sink_;
  unique_ptr<RdbSaver> saver_;
  RdbTypeFreqMap freq_map_;

  Context cntx_{};
};

struct SaveStagesController : public SaveStagesInputs {
  SaveStagesController(SaveStagesInputs&& input);
  // Objects of this class are used concurrently. Call this function
  // in a mutually exlusive context to avoid data races.
  // Also call this function before any call to `WaitAllSnapshots`
  std::optional<SaveInfo> InitResourcesAndStart();

  ~SaveStagesController();

  // Safe to call and no locks required
  void WaitAllSnapshots();

  // Same semantics as InitResourcesAndStart. Must be used in a mutually exclusive
  // context. Call this function after you `WaitAllSnapshots`to finalize the chore.
  // Performs cleanup of the object internally.
  SaveInfo Finalize();
  size_t GetSaveBuffersSize();
  uint32_t GetCurrentSaveDuration();

 private:
  // In the new version (.dfs) we store a file for every shard and one more summary file.
  // Summary file is always last in snapshots array.
  void SaveDfs();

  // Start saving a dfs file on shard
  void SaveDfsSingle(EngineShard* shard);

  // Save a single rdb file
  void SaveRdb();

  SaveInfo GetSaveInfo();

  void InitResources();

  // Remove .tmp extension or delete files in case of error
  void FinalizeFileMovement();

  // Build full path: get dir, try creating dirs, get filename with placeholder
  GenericError BuildFullPath();

  // Switch to saving state if in active state
  GenericError SwitchState();

  void SaveCb(unsigned index);

  void CloseCb(unsigned index);

  void RunStage(void (SaveStagesController::*cb)(unsigned));

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

}  // namespace detail
}  // namespace dfly
