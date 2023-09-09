// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/detail/snapshot_storage.h"

#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>

#include "base/logging.h"
#include "io/file_util.h"
#include "util/cloud/s3.h"
#include "util/fibers/fiber_file.h"
#include "util/uring/uring_file.h"

namespace dfly {
namespace detail {

std::optional<std::pair<std::string, std::string>> GetBucketPath(std::string_view path) {
  std::string_view clean = absl::StripPrefix(path, kS3Prefix);

  size_t pos = clean.find('/');
  if (pos == std::string_view::npos)
    return std::nullopt;

  std::string bucket_name{clean.substr(0, pos)};
  std::string obj_path{clean.substr(pos + 1)};
  return std::make_pair(std::move(bucket_name), std::move(obj_path));
}

#ifdef __linux__
const int kRdbWriteFlags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_DIRECT;
#endif

FileSnapshotStorage::FileSnapshotStorage(FiberQueueThreadPool* fq_threadpool)
    : fq_threadpool_{fq_threadpool} {
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> FileSnapshotStorage::OpenWriteFile(
    const std::string& path) {
  if (fq_threadpool_) {  // EPOLL
    auto res = util::OpenFiberWriteFile(path, fq_threadpool_);
    if (!res) {
      return nonstd::make_unexpected(GenericError(res.error(), "Couldn't open file for writing"));
    }

    return std::pair(*res, FileType::FILE);
  } else {
#ifdef __linux__
    auto res = util::fb2::OpenLinux(path, kRdbWriteFlags, 0666);
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

io::ReadonlyFileOrError FileSnapshotStorage::OpenReadFile(const std::string& path) {
#ifdef __linux__
  if (fq_threadpool_) {
    return util::OpenFiberReadFile(path, fq_threadpool_);
  } else {
    return util::fb2::OpenRead(path);
  }
#else
  return util::OpenFiberReadFile(path, fq_threadpool_);
#endif
}

std::string FileSnapshotStorage::LoadPath(const std::string_view& dir,
                                          const std::string_view& dbfilename) {
  if (dbfilename.empty())
    return "";

  fs::path data_folder;
  if (dir.empty()) {
    data_folder = fs::current_path();
  } else {
    std::error_code file_ec;
    data_folder = fs::canonical(dir, file_ec);
    if (file_ec) {
      LOG(ERROR) << "Data directory error: " << file_ec.message() << " for dir " << dir;
      return "";
    }
  }

  LOG(INFO) << "Data directory is " << data_folder;

  fs::path fl_path = data_folder.append(dbfilename);
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
  return "";
}

io::Result<std::vector<std::string>> FileSnapshotStorage::LoadPaths(const std::string& load_path) {
  if (!(absl::EndsWith(load_path, ".rdb") || absl::EndsWith(load_path, "summary.dfs"))) {
    LOG(ERROR) << "Bad filename extension \"" << load_path << "\"";
    return nonstd::make_unexpected(std::make_error_code(std::errc::invalid_argument));
  }

  std::vector<std::string> paths{{load_path}};

  // Collect all other files in case we're loading dfs.
  if (absl::EndsWith(load_path, "summary.dfs")) {
    std::string glob = absl::StrReplaceAll(load_path, {{"summary", "????"}});
    io::Result<io::StatShortVec> files = io::StatFiles(glob);

    if (files && files->size() == 0) {
      LOG(ERROR) << "Cound not find DFS snapshot shard files";
      return nonstd::make_unexpected(std::make_error_code(std::errc::no_such_file_or_directory));
    }

    for (auto& fstat : *files) {
      paths.push_back(std::move(fstat.name));
    }
  }

  // Check all paths are valid.
  for (const auto& path : paths) {
    std::error_code ec;
    (void)fs::canonical(path, ec);
    if (ec) {
      LOG(ERROR) << "Error loading " << load_path << " " << ec.message();
      return nonstd::make_unexpected(ec);
    }
  }

  return paths;
}

AwsS3SnapshotStorage::AwsS3SnapshotStorage(util::cloud::AWS* aws) : aws_{aws} {
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> AwsS3SnapshotStorage::OpenWriteFile(
    const std::string& path) {
  DCHECK(aws_);

  std::optional<std::pair<std::string, std::string>> bucket_path = GetBucketPath(path);
  if (!bucket_path) {
    return nonstd::make_unexpected(GenericError("Invalid S3 path"));
  }
  auto [bucket_name, obj_path] = *bucket_path;

  util::cloud::S3Bucket bucket(*aws_, bucket_name);
  std::error_code ec = bucket.Connect(kBucketConnectMs);
  if (ec) {
    return nonstd::make_unexpected(GenericError(ec, "Couldn't connect to S3 bucket"));
  }
  auto res = bucket.OpenWriteFile(obj_path);
  if (!res) {
    return nonstd::make_unexpected(GenericError(res.error(), "Couldn't open file for writing"));
  }

  return std::pair<io::Sink*, uint8_t>(*res, FileType::CLOUD);
}

io::ReadonlyFileOrError AwsS3SnapshotStorage::OpenReadFile(const std::string& path) {
  return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
}

std::string AwsS3SnapshotStorage::LoadPath(const std::string_view& dir,
                                           const std::string_view& dbfilename) {
  LOG(WARNING) << "Loading snapshots from S3 is not supported";
  return "";
}

io::Result<std::vector<std::string>> AwsS3SnapshotStorage::LoadPaths(const std::string& load_path) {
  LOG(WARNING) << "Loading snapshots from S3 is not supported";
  return nonstd::make_unexpected(std::make_error_code(std::errc::invalid_argument));
}

io::Result<size_t> LinuxWriteWrapper::WriteSome(const iovec* v, uint32_t len) {
  io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
  if (res) {
    offset_ += *res;
  }

  return res;
}

void SubstituteFilenameTsPlaceholder(fs::path* filename, std::string_view replacement) {
  *filename = absl::StrReplaceAll(filename->string(), {{"{timestamp}", replacement}});
}

}  // namespace detail
}  // namespace dfly
