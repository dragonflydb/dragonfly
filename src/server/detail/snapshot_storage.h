// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/strings/match.h>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "io/file.h"
#include "server/execution_state.h"

namespace util::fb2 {
class FiberQueueThreadPool;
}  // namespace util::fb2

namespace dfly {
namespace detail {

namespace fs = std::filesystem;

constexpr std::string_view kS3Prefix = "s3://";
constexpr std::string_view kGCSPrefix = "gs://";

class SnapshotStorage {
 public:
  virtual ~SnapshotStorage() = default;

  // Opens the file at the given path for writing. Local files are opened with O_DIRECT, so their
  // writes must be aligned; cloud files are published only when closed.
  virtual io::Result<io::WriteFile*, GenericError> OpenWriteFile(const std::string& path) = 0;

  virtual io::ReadonlyFileOrError OpenReadFile(const std::string& path) = 0;

  // Returns the path of the newest RDB file or DFS summary file in `dir` that matches
  // `dbfilename`, or an empty string if `dbfilename` is empty.
  virtual io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                         std::string_view dbfilename) = 0;

  // Returns all the files of the snapshot given the RDB file or DFS summary file path.
  // The DFS summary file is always the last one.
  io::Result<std::vector<std::string>, GenericError> ExpandSnapshot(const std::string& load_path);

  virtual bool IsCloud() const {
    return false;
  }

 protected:
  // Returns an error if `path` can not be loaded.
  virtual std::error_code CheckPath(const std::string& path) = 0;

  // Returns the shard files of the DFS snapshot whose summary file path is `summary_path`.
  virtual io::Result<std::vector<std::string>, GenericError> ListShardFiles(
      const std::string& summary_path) = 0;
};

class FileSnapshotStorage : public SnapshotStorage {
 public:
  // `fq_threadpool` is used for file I/O when the proactors do not support io_uring.
  explicit FileSnapshotStorage(util::fb2::FiberQueueThreadPool* fq_threadpool);

  io::Result<io::WriteFile*, GenericError> OpenWriteFile(const std::string& path) override;

  io::ReadonlyFileOrError OpenReadFile(const std::string& path) override;

  io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                 std::string_view dbfilename) override;

 private:
  std::error_code CheckPath(const std::string& path) final;

  io::Result<std::vector<std::string>, GenericError> ListShardFiles(
      const std::string& summary_path) final;

  util::fb2::FiberQueueThreadPool* fq_threadpool_;
};

// Creates the storage for a cloud path (see IsCloudPath). Returns an error instead of terminating
// the process, as it is also called at runtime by SAVE/LOAD commands, where a transient failure
// (e.g. missing credentials) must not kill an already serving instance.
io::Result<std::shared_ptr<SnapshotStorage>, GenericError> CreateCloudSnapshotStorage(
    std::string_view uri);

struct FilenameSubstitutions {
  std::string_view ts;
  std::string_view year;
  std::string_view month;
  std::string_view day;
};

void SubstituteFilenamePlaceholders(fs::path* filename, const FilenameSubstitutions& fns);

inline bool IsS3Path(std::string_view path) {
  return absl::StartsWith(path, kS3Prefix);
}

inline bool IsGCSPath(std::string_view path) {
  return absl::StartsWith(path, kGCSPrefix);
}

bool IsAzurePath(std::string_view path);

inline bool IsCloudPath(std::string_view path) {
  return IsS3Path(path) || IsGCSPath(path) || IsAzurePath(path);
}

}  // namespace detail
}  // namespace dfly
