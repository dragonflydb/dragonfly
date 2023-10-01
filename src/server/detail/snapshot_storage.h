// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/s3/S3Client.h>

#include <filesystem>
#include <string>
#include <string_view>
#include <utility>

#include "io/io.h"
#include "server/common.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/uring_file.h"

namespace dfly {
namespace detail {

namespace fs = std::filesystem;

constexpr std::string_view kS3Prefix = "s3://";

const size_t kBucketConnectMs = 2000;

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
  virtual io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenWriteFile(
      const std::string& path) = 0;

  virtual io::ReadonlyFileOrError OpenReadFile(const std::string& path) = 0;

  // Returns the path of the RDB file or DFS summary file to load.
  virtual io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                         std::string_view dbfilename) = 0;

  // Returns the snapshot paths given the RDB file or DFS summary file path.
  virtual io::Result<std::vector<std::string>, GenericError> LoadPaths(
      const std::string& load_path) = 0;
};

class FileSnapshotStorage : public SnapshotStorage {
 public:
  FileSnapshotStorage(FiberQueueThreadPool* fq_threadpool);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenWriteFile(
      const std::string& path) override;

  io::ReadonlyFileOrError OpenReadFile(const std::string& path) override;

  io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                 std::string_view dbfilename) override;

  io::Result<std::vector<std::string>, GenericError> LoadPaths(
      const std::string& load_path) override;

 private:
  util::fb2::FiberQueueThreadPool* fq_threadpool_;
};

class AwsS3SnapshotStorage : public SnapshotStorage {
 public:
  AwsS3SnapshotStorage(const std::string& endpoint, bool ec2_metadata);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenWriteFile(
      const std::string& path) override;

  io::ReadonlyFileOrError OpenReadFile(const std::string& path) override;

  io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                 std::string_view dbfilename) override;

  io::Result<std::vector<std::string>, GenericError> LoadPaths(
      const std::string& load_path) override;

 private:
  // List the objects in the given bucket with the given prefix. This must
  // run from a proactor.
  io::Result<std::vector<std::string>, GenericError> ListObjects(std::string_view bucket_name,
                                                                 std::string_view prefix);

  std::shared_ptr<Aws::S3::S3Client> s3_;
};

// Returns bucket_name, obj_path for an s3 path.
std::optional<std::pair<std::string, std::string>> GetBucketPath(std::string_view path);

// takes ownership over the file.
class LinuxWriteWrapper : public io::Sink {
 public:
  LinuxWriteWrapper(util::fb2::LinuxFile* lf) : lf_(lf) {
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  std::error_code Close() {
    return lf_->Close();
  }

 private:
  std::unique_ptr<util::fb2::LinuxFile> lf_;
  off_t offset_ = 0;
};

void SubstituteFilenameTsPlaceholder(fs::path* filename, std::string_view replacement);

}  // namespace detail
}  // namespace dfly
