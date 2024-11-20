// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#ifdef WITH_AWS
#include <aws/s3/S3Client.h>
#endif

#include <filesystem>
#include <string>
#include <string_view>
#include <utility>

#include "io/io.h"
#include "server/common.h"
#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/cloud/gcp/gcs.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/uring_file.h"

namespace dfly {
namespace detail {

namespace fs = std::filesystem;

constexpr std::string_view kS3Prefix = "s3://";
constexpr std::string_view kGCSPrefix = "gs://";

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

  using ExpandResult = std::vector<std::string>;
  // Searches for all the relevant snapshot files given the RDB file or DFS summary file path.
  io::Result<ExpandResult, GenericError> ExpandSnapshot(const std::string& load_path);

  virtual bool IsCloud() const {
    return false;
  }

 protected:
  struct SnapStat {
    SnapStat(std::string file_name, int64_t ts)
        : name(std::move(file_name)), last_modified(std::move(ts)) {
    }
    std::string name;
    int64_t last_modified;
  };

  // Returns empty string if nothing is matched. vector is passed by value on purpose, as it is
  // been sorted inside.
  static std::string FindMatchingFile(std::string_view prefix, std::string_view dbfilename,
                                      std::vector<SnapStat> keys);

  virtual io::Result<std::vector<std::string>, GenericError> ExpandFromPath(
      const std::string& path) = 0;

  virtual std::error_code CheckPath(const std::string& path) = 0;
};

class FileSnapshotStorage : public SnapshotStorage {
 public:
  FileSnapshotStorage(util::fb2::FiberQueueThreadPool* fq_threadpool);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenWriteFile(
      const std::string& path) override;

  io::ReadonlyFileOrError OpenReadFile(const std::string& path) override;

  io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                 std::string_view dbfilename) override;

 private:
  io::Result<std::vector<std::string>, GenericError> ExpandFromPath(const std::string& path) final;

  std::error_code CheckPath(const std::string& path) final;
  util::fb2::FiberQueueThreadPool* fq_threadpool_;
};

class GcsSnapshotStorage : public SnapshotStorage {
 public:
  ~GcsSnapshotStorage();

  std::error_code Init(unsigned connect_ms);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenWriteFile(
      const std::string& path) override;

  io::ReadonlyFileOrError OpenReadFile(const std::string& path) override;

  io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                 std::string_view dbfilename) override;

  bool IsCloud() const final {
    return true;
  }

 private:
  io::Result<std::vector<std::string>, GenericError> ExpandFromPath(const std::string& path) final;

  std::error_code CheckPath(const std::string& path) final;

  util::cloud::GCPCredsProvider creds_provider_;
  SSL_CTX* ctx_ = NULL;
};

#ifdef WITH_AWS
class AwsS3SnapshotStorage : public SnapshotStorage {
 public:
  AwsS3SnapshotStorage(const std::string& endpoint, bool https, bool ec2_metadata,
                       bool sign_payload);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenWriteFile(
      const std::string& path) override;

  io::ReadonlyFileOrError OpenReadFile(const std::string& path) override;

  io::Result<std::string, GenericError> LoadPath(std::string_view dir,
                                                 std::string_view dbfilename) override;

  bool IsCloud() const final {
    return true;
  }

 private:
  io::Result<std::vector<std::string>, GenericError> ExpandFromPath(const std::string& path) final;

  std::error_code CheckPath(const std::string& path) final;

  // List the objects in the given bucket with the given prefix. This must
  // run from a proactor.
  io::Result<std::vector<SnapStat>, GenericError> ListObjects(std::string_view bucket_name,
                                                              std::string_view prefix);

  std::shared_ptr<Aws::S3::S3Client> s3_;
};

#endif

#ifdef __linux__
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
#endif

struct FilenameSubstitutions {
  std::string_view ts;
  std::string_view year;
  std::string_view month;
  std::string_view day;
};

void SubstituteFilenamePlaceholders(fs::path* filename, const FilenameSubstitutions& fns);

}  // namespace detail
}  // namespace dfly
