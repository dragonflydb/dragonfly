// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/detail/snapshot_storage.h"

#include <absl/strings/strip.h>

#include "base/logging.h"
#include "util/cloud/s3.h"
#include "util/fibers/fiber_file.h"

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

AwsS3SnapshotStorage::AwsS3SnapshotStorage(util::cloud::AWS* aws) : aws_{aws} {
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> AwsS3SnapshotStorage::OpenFile(
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

io::Result<size_t> LinuxWriteWrapper::WriteSome(const iovec* v, uint32_t len) {
  io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
  if (res) {
    offset_ += *res;
  }

  return res;
}

}  // namespace detail
}  // namespace dfly
