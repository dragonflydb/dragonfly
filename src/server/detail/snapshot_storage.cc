// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/detail/snapshot_storage.h"

#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>

#ifdef WITH_AWS
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "util/aws/aws.h"
#include "util/aws/credentials_provider_chain.h"
#include "util/aws/s3_endpoint_provider.h"
#include "util/aws/s3_read_file.h"
#include "util/aws/s3_write_file.h"
#endif

#include <regex>

#include "base/logging.h"
#include "io/file_util.h"
#include "server/engine_shard_set.h"
#include "util/fibers/fiber_file.h"

namespace dfly {
namespace detail {

using namespace util;
using namespace std;

// Returns bucket_name, obj_path for an s3 path.
optional<pair<string, string>> GetBucketPath(string_view path) {
  std::string_view clean = absl::StripPrefix(path, kS3Prefix);

  size_t pos = clean.find('/');
  if (pos == string_view::npos) {
    return make_pair(string(clean), "");
  }

  string bucket_name{clean.substr(0, pos)};
  string obj_path{clean.substr(pos + 1)};
  return std::make_pair(std::move(bucket_name), std::move(obj_path));
}

#ifdef __linux__
const int kRdbWriteFlags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_DIRECT;
#endif

io::Result<vector<string>, GenericError> SnapshotStorage::ExpandSnapshot(const string& load_path) {
  if (!(absl::EndsWith(load_path, ".rdb") || absl::EndsWith(load_path, "summary.dfs"))) {
    return nonstd::make_unexpected(
        GenericError(std::make_error_code(std::errc::invalid_argument), "Bad filename extension"));
  }

  error_code ec = CheckPath(load_path);
  if (ec) {
    return nonstd::make_unexpected(GenericError(ec, "File not found"));
  }

  vector<string> paths{{load_path}};

  // Collect all other files in case we're loading dfs.
  if (absl::EndsWith(load_path, "summary.dfs")) {
    auto res = ExpandFromPath(load_path);
    if (!res) {
      return res;
    }
    paths.insert(paths.end(), res->begin(), res->end());
  }
  return paths;
}

FileSnapshotStorage::FileSnapshotStorage(fb2::FiberQueueThreadPool* fq_threadpool)
    : fq_threadpool_{fq_threadpool} {
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> FileSnapshotStorage::OpenWriteFile(
    const std::string& path) {
  if (fq_threadpool_) {  // EPOLL
    FiberWriteOptions opts;
    opts.direct = true;

    auto res = OpenFiberWriteFile(path, fq_threadpool_, opts);
    if (!res) {
      return nonstd::make_unexpected(GenericError(res.error(), "Couldn't open file for writing"));
    }

    return std::pair(*res, FileType::FILE | FileType::DIRECT);
  } else {
#ifdef __linux__
    auto res = fb2::OpenLinux(path, kRdbWriteFlags, 0666);
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
    return OpenFiberReadFile(path, fq_threadpool_);
  } else {
    return fb2::OpenRead(path);
  }
#else
  return OpenFiberReadFile(path, fq_threadpool_);
#endif
}

io::Result<std::string, GenericError> FileSnapshotStorage::LoadPath(std::string_view dir,
                                                                    std::string_view dbfilename) {
  if (dbfilename.empty())
    return {};

  fs::path data_folder;
  if (dir.empty()) {
    data_folder = fs::current_path();
  } else {
    std::error_code file_ec;
    data_folder = fs::canonical(dir, file_ec);
    if (file_ec) {
      return nonstd::make_unexpected(GenericError{file_ec, "Data directory error"});
    }
  }

  LOG(INFO) << "Load snapshot: Searching for snapshot in directory: " << data_folder;

  fs::path fl_path = data_folder.append(dbfilename);
  // If we've found an exact match we're done.
  if (fs::exists(fl_path))
    return fl_path.generic_string();

  SubstituteFilenamePlaceholders(&fl_path, {"*", "*", "*", "*"});
  if (!fl_path.has_extension()) {
    fl_path += "*";
  }
  io::Result<io::StatShortVec> short_vec = io::StatFiles(fl_path.generic_string());
  if (short_vec) {
    std::sort(short_vec->begin(), short_vec->end(),
              [](const io::StatShort& l, const io::StatShort& r) {
                return std::difftime(l.last_modified, r.last_modified) < 0;
              });
    auto it = std::find_if(short_vec->rbegin(), short_vec->rend(), [](const auto& stat) {
      return absl::EndsWith(stat.name, ".rdb") || absl::EndsWith(stat.name, "summary.dfs");
    });
    if (it != short_vec->rend())
      return it->name;
  } else {
    return nonstd::make_unexpected(
        GenericError(short_vec.error(), "Could not stat snapshot directory"));
  }

  return nonstd::make_unexpected(GenericError(
      std::make_error_code(std::errc::no_such_file_or_directory), "Snapshot not found"));
}

io::Result<vector<string>, GenericError> FileSnapshotStorage::ExpandFromPath(const string& path) {
  string glob = absl::StrReplaceAll(path, {{"summary", "????"}});
  io::Result<io::StatShortVec> files = io::StatFiles(glob);

  if (!files || files->size() == 0) {
    return nonstd::make_unexpected(GenericError(make_error_code(errc::no_such_file_or_directory),
                                                "Cound not find DFS shard files"));
  }

  vector<string> paths;
  for (auto& fstat : *files) {
    paths.push_back(std::move(fstat.name));
  }

  return paths;
}

error_code FileSnapshotStorage::CheckPath(const string& path) {
  error_code ec;
  std::ignore = fs::canonical(path, ec);
  return ec;
}

#ifdef WITH_AWS
AwsS3SnapshotStorage::AwsS3SnapshotStorage(const std::string& endpoint, bool https,
                                           bool ec2_metadata, bool sign_payload) {
  shard_set->pool()->GetNextProactor()->Await([&] {
    if (!ec2_metadata) {
      setenv("AWS_EC2_METADATA_DISABLED", "true", 0);
    }
    // S3ClientConfiguration may request configuration and credentials from
    // EC2 metadata so must be run in a proactor thread.
    Aws::S3::S3ClientConfiguration s3_conf{};
    LOG(INFO) << "Creating AWS S3 client; region=" << s3_conf.region << "; https=" << std::boolalpha
              << https << "; endpoint=" << endpoint;
    if (!sign_payload) {
      s3_conf.payloadSigningPolicy = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::ForceNever;
    }
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
        std::make_shared<aws::CredentialsProviderChain>();
    // Pass a custom endpoint. If empty uses the S3 endpoint.
    std::shared_ptr<Aws::S3::S3EndpointProviderBase> endpoint_provider =
        std::make_shared<aws::S3EndpointProvider>(endpoint, https);
    s3_ = std::make_shared<Aws::S3::S3Client>(credentials_provider, endpoint_provider, s3_conf);
  });
}

io::Result<std::pair<io::Sink*, uint8_t>, GenericError> AwsS3SnapshotStorage::OpenWriteFile(
    const std::string& path) {
  fb2::ProactorBase* proactor = shard_set->pool()->GetNextProactor();
  return proactor->Await([&]() -> io::Result<std::pair<io::Sink*, uint8_t>, GenericError> {
    std::optional<std::pair<std::string, std::string>> bucket_path = GetBucketPath(path);
    if (!bucket_path) {
      return nonstd::make_unexpected(GenericError("Invalid S3 path"));
    }
    auto [bucket, key] = *bucket_path;
    io::Result<aws::S3WriteFile> file = aws::S3WriteFile::Open(bucket, key, s3_);
    if (!file) {
      return nonstd::make_unexpected(GenericError(file.error(), "Failed to open write file"));
    }

    aws::S3WriteFile* f = new aws::S3WriteFile(std::move(*file));
    return std::pair<io::Sink*, uint8_t>(f, FileType::CLOUD);
  });
}

io::ReadonlyFileOrError AwsS3SnapshotStorage::OpenReadFile(const std::string& path) {
  std::optional<std::pair<std::string, std::string>> bucket_path = GetBucketPath(path);
  if (!bucket_path) {
    return nonstd::make_unexpected(GenericError("Invalid S3 path"));
  }
  auto [bucket, key] = *bucket_path;
  return new aws::S3ReadFile(bucket, key, s3_);
}

io::Result<std::string, GenericError> AwsS3SnapshotStorage::LoadPath(std::string_view dir,
                                                                     std::string_view dbfilename) {
  if (dbfilename.empty())
    return "";

  std::optional<std::pair<std::string, std::string>> bucket_path = GetBucketPath(dir);
  if (!bucket_path) {
    return nonstd::make_unexpected(
        GenericError{std::make_error_code(std::errc::invalid_argument), "Invalid S3 path"});
  }
  auto [bucket_name, prefix] = *bucket_path;

  fb2::ProactorBase* proactor = shard_set->pool()->GetNextProactor();
  return proactor->Await(
      [&, bucket_name = bucket_name, prefix = prefix]() -> io::Result<std::string, GenericError> {
        LOG(INFO) << "Load snapshot: Searching for snapshot in S3 path: " << kS3Prefix
                  << bucket_name << "/" << prefix;

        // Create a regex to match the object keys, substituting the timestamp
        // and adding an extension if needed.
        fs::path fl_path{prefix};
        fl_path.append(dbfilename);
        SubstituteFilenamePlaceholders(
            &fl_path, {.ts = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})",
                       .year = "([0-9]{4})",
                       .month = "([0-9]{2})",
                       .day = "([0-9]{2})"});
        if (!fl_path.has_extension()) {
          fl_path += "(-summary.dfs|.rdb)";
        }
        const std::regex re(fl_path.string());

        // Sort the keys in reverse so the first. Since the timestamp format
        // has lexicographic order, the matching snapshot file will be the latest
        // snapshot.
        io::Result<std::vector<SnapStat>, GenericError> keys = ListObjects(bucket_name, prefix);
        if (!keys) {
          return nonstd::make_unexpected(keys.error());
        }

        std::sort(std::rbegin(*keys), std::rend(*keys), [](const SnapStat& l, const SnapStat& r) {
          return l.last_modified < r.last_modified;
        });

        for (const SnapStat& key : *keys) {
          std::smatch m;
          if (std::regex_match(key.name, m, re)) {
            return std::string(kS3Prefix) + bucket_name + "/" + key.name;
          }
        }

        return nonstd::make_unexpected(GenericError(
            std::make_error_code(std::errc::no_such_file_or_directory), "Snapshot not found"));
      });
}

io::Result<vector<string>, GenericError> AwsS3SnapshotStorage::ExpandFromPath(
    const string& load_path) {
  fb2::ProactorBase* proactor = shard_set->pool()->GetNextProactor();
  optional<pair<string, string>> bucket_path = GetBucketPath(load_path);
  if (!bucket_path) {
    return nonstd::make_unexpected(
        GenericError{std::make_error_code(std::errc::invalid_argument), "Invalid S3 path"});
  }
  const auto [bucket_name, obj_path] = *bucket_path;
  const std::regex re(absl::StrReplaceAll(obj_path, {{"summary", "[0-9]{4}"}}));

  // Limit prefix to objects in the same 'directory' as load_path.
  const size_t pos = obj_path.find_last_of('/');
  const std::string prefix = (pos == std::string_view::npos) ? "" : obj_path.substr(0, pos);

  auto paths = proactor->Await([&]() -> io::Result<vector<string>, GenericError> {
    const io::Result<std::vector<SnapStat>, GenericError> keys = ListObjects(bucket_name, prefix);
    if (!keys) {
      return nonstd::make_unexpected(keys.error());
    }
    vector<string> res;
    for (const SnapStat& key : *keys) {
      std::smatch m;
      if (std::regex_match(key.name, m, re)) {
        res.push_back(std::string(kS3Prefix) + bucket_name + "/" + key.name);
      }
    }

    return res;
  });

  if (!paths || paths->empty()) {
    return nonstd::make_unexpected(
        GenericError{std::make_error_code(std::errc::no_such_file_or_directory),
                     "Cound not find DFS snapshot shard files"});
  }

  return *paths;
}

error_code AwsS3SnapshotStorage::CheckPath(const std::string& path) {
  return {};
}

io::Result<std::vector<AwsS3SnapshotStorage::SnapStat>, GenericError>
AwsS3SnapshotStorage::ListObjects(std::string_view bucket_name, std::string_view prefix) {
  // Each list objects request has a 1000 object limit, so page through the
  // objects if needed.
  std::string continuation_token;
  std::vector<SnapStat> keys;
  do {
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(std::string(bucket_name));
    request.SetPrefix(std::string(prefix));
    if (!continuation_token.empty()) {
      request.SetContinuationToken(continuation_token);
    }

    Aws::S3::Model::ListObjectsV2Outcome outcome = s3_->ListObjectsV2(request);
    if (outcome.IsSuccess()) {
      continuation_token = outcome.GetResult().GetNextContinuationToken();
      for (const auto& object : outcome.GetResult().GetContents()) {
        keys.emplace_back(object.GetKey(), object.GetLastModified().Millis());
      }
    } else if (outcome.GetError().GetExceptionName() == "PermanentRedirect") {
      return nonstd::make_unexpected(
          GenericError{"Failed list objects in S3 bucket: Permanent redirect; Ensure your "
                       "configured AWS region matches the S3 bucket region"});
    } else if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_BUCKET) {
      return nonstd::make_unexpected(GenericError{
          "Failed list objects in S3 bucket: Bucket not found: " + std::string(bucket_name)});
    } else if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID) {
      return nonstd::make_unexpected(
          GenericError{"Failed list objects in S3 bucket: Invalid access key ID"});
    } else if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::SIGNATURE_DOES_NOT_MATCH) {
      return nonstd::make_unexpected(
          GenericError{"Failed list objects in S3 bucket: Invalid signature; Check your AWS "
                       "credentials are correct"});
    } else if (outcome.GetError().GetExceptionName() == "InvalidToken") {
      return nonstd::make_unexpected(
          GenericError{"Failed list objects in S3 bucket: Invalid token; Check your AWS "
                       "credentials are correct"});
    } else {
      return nonstd::make_unexpected(GenericError{"Failed list objects in S3 bucket: " +
                                                  outcome.GetError().GetExceptionName()});
    }
  } while (!continuation_token.empty());
  return keys;
}
#endif

#ifdef __linux__
io::Result<size_t> LinuxWriteWrapper::WriteSome(const iovec* v, uint32_t len) {
  io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
  if (res) {
    offset_ += *res;
  }

  return res;
}
#endif

void SubstituteFilenamePlaceholders(fs::path* filename, const FilenameSubstitutions& fns) {
  *filename = absl::StrReplaceAll(
      filename->string(),
      {{"{Y}", fns.year}, {"{m}", fns.month}, {"{d}", fns.day}, {"{timestamp}", fns.ts}});
}

}  // namespace detail
}  // namespace dfly
