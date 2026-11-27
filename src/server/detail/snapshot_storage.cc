// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/detail/snapshot_storage.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <algorithm>
#include <functional>

#include "base/logging.h"
#include "io/file_util.h"
#include "server/engine_shard_set.h"
#include "strings/escaping.h"
#include "strings/human_readable.h"
#include "util/cloud/aws/aws_creds_provider.h"
#include "util/cloud/aws/s3_storage.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/azure/storage.h"
#include "util/cloud/utils.h"
#include "util/fibers/fiber_file.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/uring_file.h"

#ifdef WITH_GCP
#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/cloud/gcp/gcs.h"
#include "util/cloud/gcp/gcs_file.h"
#endif

ABSL_RETIRED_FLAG(bool, s3_use_helio_client, true,
                  "Deprecated: the helio native S3 client is always used.");
ABSL_FLAG(uint32_t, s3_upload_traffic_mark, 0,
          "Linux SO_MARK applied to S3 snapshot upload sockets, so they can be assigned to an "
          "externally configured tc bandwidth class. Requires CAP_NET_ADMIN. When set, socket "
          "pacing from snapshot_egress_limit_bytes is not applied. 0 leaves sockets unmarked.");
ABSL_FLAG(std::string, s3_endpoint, "",
          "endpoint for s3 snapshots, default uses aws regional endpoint");
ABSL_FLAG(bool, s3_use_https, true, "whether to use https for s3 endpoints");
// Disable EC2 metadata by default, or if a users credentials are invalid the
// AWS client will spent 30s trying to connect to inaccessable EC2 endpoints
// to load the credentials.
ABSL_FLAG(bool, s3_ec2_metadata, false,
          "whether to load credentials and configuration from EC2 metadata");
ABSL_DECLARE_FLAG(strings::MemoryBytesFlag, snapshot_egress_limit_bytes);

namespace rng = std::ranges;
namespace dfly {
namespace detail {

using namespace util;
using namespace std;

namespace {

constexpr string_view kSummarySuffix = "summary.dfs"sv;
constexpr string_view kHttpsPrefix = "https://";
constexpr string_view kAzureBlobEndpointSuffix = ".blob.core.windows.net";
constexpr unsigned kBucketConnectMs = 2000;
constexpr unsigned kGcsConnectMs = 3000;

// Matches any digit in a file name mask.
constexpr char kDigit = '\0';

nonstd::unexpected_type<GenericError> Unexpected(errc code, string details) {
  return nonstd::make_unexpected(GenericError(make_error_code(code), std::move(details)));
}

// Attaches `details` to the error of `res`.
template <typename T>
io::Result<T, GenericError> WithDetails(io::Result<T> res, string_view details) {
  if (!res)
    return nonstd::make_unexpected(GenericError(res.error(), string(details)));
  return *res;
}

bool MatchesMask(string_view name, string_view mask) {
  return name.size() == mask.size() &&
         equal(mask.begin(), mask.end(), name.begin(),
               [](char m, char c) { return m == kDigit ? absl::ascii_isdigit(c) : m == c; });
}

// Returns whether `name` is a shard file of the DFS snapshot whose summary file is
// `base` + "summary.dfs", i.e. `base` followed by a 4 digit shard id and ".dfs".
bool IsShardFile(string_view name, string_view base) {
  return absl::ConsumePrefix(&name, base) && absl::ConsumeSuffix(&name, ".dfs") &&
         MatchesMask(name, string(4, kDigit));
}

struct ObjectStat {
  string key;
  int64_t mtime;
};

// Returns the key of the newest object that is a snapshot file named `prefix` + `dbfilename`,
// or an empty string if there is none. The filename placeholders match the digits that
// SaveStagesController writes for them, and a filename without extension matches both RDB and
// DFS snapshots.
string FindNewestSnapshot(string_view prefix, string_view dbfilename,
                          const vector<ObjectStat>& objects) {
  const string d2(2, kDigit), d4(4, kDigit);
  const string ts = absl::StrCat(d4, "-", d2, "-", d2, "T", d2, ":", d2, ":", d2);
  fs::path mask_path = absl::StrCat(prefix, dbfilename);
  SubstituteFilenamePlaceholders(&mask_path, {.ts = ts, .year = d4, .month = d2, .day = d2});
  const string mask = mask_path.string();

  vector<string_view> suffixes = {""};
  if (!fs::path(dbfilename).has_extension())
    suffixes = {"-summary.dfs", ".rdb"};

  auto matches = [&](string_view key) {
    return rng::any_of(suffixes, [&](string_view suffix) {
      string_view name = key;
      return absl::ConsumeSuffix(&name, suffix) && MatchesMask(name, mask);
    });
  };

  const ObjectStat* newest = nullptr;
  for (const ObjectStat& obj : objects) {
    if ((!newest || obj.mtime > newest->mtime) && matches(obj.key))
      newest = &obj;
  }
  return newest ? newest->key : "";
}

#ifdef __linux__
// Writes sequentially to an io_uring file, takes ownership over the file.
class LinuxWriteWrapper : public io::WriteFile {
 public:
  explicit LinuxWriteWrapper(unique_ptr<fb2::LinuxFile> lf) : WriteFile(""), lf_(std::move(lf)) {
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final {
    io::Result<size_t> res = lf_->WriteSome(v, len, offset_, 0);
    if (res)
      offset_ += *res;
    return res;
  }

  error_code Close() final {
    return lf_->Close();
  }

 private:
  unique_ptr<fb2::LinuxFile> lf_;
  off_t offset_ = 0;
};
#endif

bool IsValidAzureAccount(string_view account) {
  return account.size() >= 3 && account.size() <= 24 &&
         std::all_of(account.begin(), account.end(),
                     [](char c) { return absl::ascii_islower(c) || absl::ascii_isdigit(c); });
}

bool IsValidAzureContainer(string_view container) {
  if (container.size() < 3 || container.size() > 63 || container.front() == '-' ||
      container.back() == '-') {
    return false;
  }

  bool previous_hyphen = false;
  for (char c : container) {
    if (absl::ascii_islower(c) || absl::ascii_isdigit(c)) {
      previous_hyphen = false;
    } else if (c == '-' && !previous_hyphen) {
      previous_hyphen = true;
    } else {
      return false;
    }
  }
  return true;
}

struct AzurePath {
  string account;
  string container;
  string key;
};

io::Result<AzurePath, GenericError> ParseAzurePath(string_view path) {
  if (!absl::ConsumePrefix(&path, kHttpsPrefix))
    return Unexpected(errc::invalid_argument, "Invalid Azure path");

  const size_t first_slash = path.find('/');
  string_view host = first_slash == string_view::npos ? path : path.substr(0, first_slash);
  if (!absl::EndsWith(host, kAzureBlobEndpointSuffix) ||
      !IsValidAzureAccount(absl::StripSuffix(host, kAzureBlobEndpointSuffix))) {
    return Unexpected(errc::invalid_argument, "Azure path must include an account hostname");
  }
  string_view account = absl::StripSuffix(host, kAzureBlobEndpointSuffix);

  string_view raw_container_and_key =
      first_slash == string_view::npos ? string_view{} : path.substr(first_slash + 1);
  if (raw_container_and_key.find_first_of("?#") != string_view::npos)
    return Unexpected(errc::invalid_argument, "Azure path must not contain a query");

  const size_t second_slash = raw_container_and_key.find('/');
  string_view raw_container = second_slash == string_view::npos
                                  ? raw_container_and_key
                                  : raw_container_and_key.substr(0, second_slash);
  string_view raw_key = second_slash == string_view::npos
                            ? string_view{}
                            : raw_container_and_key.substr(second_slash + 1);

  string container, key;
  if (!strings::AppendUrlDecoded(raw_container, &container) ||
      !strings::AppendUrlDecoded(raw_key, &key)) {
    return Unexpected(errc::invalid_argument, "Azure path contains invalid percent encoding");
  }
  if (!IsValidAzureContainer(container))
    return Unexpected(errc::invalid_argument, "Azure path must include a container");

  return AzurePath{string(account), std::move(container), std::move(key)};
}

string BuildAzurePath(const AzurePath& path) {
  string result = absl::StrCat(kHttpsPrefix, path.account, kAzureBlobEndpointSuffix, "/");
  strings::AppendUrlEncoded(path.container, &result);
  if (!path.key.empty()) {
    for (string_view component : absl::StrSplit(path.key, '/')) {
      result += '/';
      strings::AppendUrlEncoded(component, &result);
    }
  }
  return result;
}

// The location of an object in an object storage.
struct ObjectPath {
  string bucket;
  string key;
};

// Object storage, where a path addresses an object key inside a bucket.
class CloudSnapshotStorage : public SnapshotStorage {
 public:
  io::Result<string, GenericError> LoadPath(string_view dir, string_view dbfilename) final;

  bool IsCloud() const final {
    return true;
  }

 protected:
  using ListCb = std::function<void(const cloud::StorageListItem&)>;

  virtual io::Result<ObjectPath, GenericError> ParsePath(string_view path) const = 0;

  virtual string BuildPath(string_view bucket, string_view key) const = 0;

  // Lists the objects in `bucket` whose keys start with `prefix`, without descending into
  // sub-directories. Must run in a proactor thread.
  virtual error_code List(string_view bucket, string_view prefix, const ListCb& cb) = 0;

 private:
  error_code CheckPath(const string& path) final;

  io::Result<vector<string>, GenericError> ListShardFiles(const string& summary_path) final;

  io::Result<vector<ObjectStat>, GenericError> ListObjects(string_view bucket, string_view prefix);
};

io::Result<string, GenericError> CloudSnapshotStorage::LoadPath(string_view dir,
                                                                string_view dbfilename) {
  if (dbfilename.empty())
    return "";

  auto path = ParsePath(dir);
  if (!path)
    return nonstd::make_unexpected(path.error());

  // Only the objects in the `dir` directory itself are snapshot candidates.
  string& prefix = path->key;
  if (!prefix.empty() && prefix.back() != '/')
    prefix += '/';

  LOG(INFO) << "Load snapshot: Searching for snapshot in " << dir;
  auto objects = ListObjects(path->bucket, prefix);
  if (!objects)
    return nonstd::make_unexpected(objects.error());

  string key = FindNewestSnapshot(prefix, dbfilename, *objects);
  if (key.empty())
    return Unexpected(errc::no_such_file_or_directory, "Snapshot not found");
  return BuildPath(path->bucket, key);
}

error_code CloudSnapshotStorage::CheckPath(const string& path) {
  auto res = ParsePath(path);
  return res ? error_code{} : error_code(res.error());
}

io::Result<vector<string>, GenericError> CloudSnapshotStorage::ListShardFiles(
    const string& summary_path) {
  auto path = ParsePath(summary_path);
  if (!path)
    return nonstd::make_unexpected(path.error());

  string_view base = absl::StripSuffix(path->key, kSummarySuffix);
  auto objects = ListObjects(path->bucket, base);
  if (!objects)
    return nonstd::make_unexpected(objects.error());

  vector<string> paths;
  for (const ObjectStat& obj : *objects) {
    if (IsShardFile(obj.key, base))
      paths.push_back(BuildPath(path->bucket, obj.key));
  }
  return paths;
}

io::Result<vector<ObjectStat>, GenericError> CloudSnapshotStorage::ListObjects(string_view bucket,
                                                                               string_view prefix) {
  VLOG(1) << "Listing objects in bucket " << bucket << " with prefix " << prefix;

  // Use a proactor because this function might be called from the main thread.
  vector<ObjectStat> objects;
  error_code ec = shard_set->pool()->GetNextProactor()->Await([&] {
    return List(bucket, prefix, [&objects](const cloud::StorageListItem& item) {
      objects.push_back({string(item.key), item.mtime_ns});
    });
  });
  if (ec)
    return nonstd::make_unexpected(GenericError(ec, "Failed to list objects"));
  return objects;
}

// Parses a `scheme`bucket/key path.
io::Result<ObjectPath, GenericError> ParseBucketPath(string_view path, string_view scheme) {
  if (!absl::ConsumePrefix(&path, scheme))
    return Unexpected(errc::invalid_argument, absl::StrCat("Path must start with ", scheme));
  pair<string, string> bucket_key = absl::StrSplit(path, absl::MaxSplits('/', 1));
  return ObjectPath{std::move(bucket_key.first), std::move(bucket_key.second)};
}

#ifdef WITH_GCP
class GcsSnapshotStorage final : public CloudSnapshotStorage {
 public:
  ~GcsSnapshotStorage() {
    if (ctx_)
      http::TlsClient::FreeContext(ctx_);
  }

  error_code Init(unsigned connect_ms) {
    RETURN_ERROR(creds_provider_.Init(connect_ms));
    ctx_ = http::TlsClient::CreateSslContext();
    return {};
  }

  io::Result<io::WriteFile*, GenericError> OpenWriteFile(const string& path) final {
    auto obj = ParsePath(path);
    if (!obj)
      return nonstd::make_unexpected(obj.error());

    cloud::GcsWriteFileOptions opts;
    opts.creds_provider = &creds_provider_;
    opts.pool = CreatePool();
    opts.pool_owned = true;
    return WithDetails(cloud::OpenWriteGcsFile(obj->bucket, obj->key, opts), "Could not open file");
  }

  io::ReadonlyFileOrError OpenReadFile(const string& path) final {
    auto obj = ParsePath(path);
    if (!obj)
      return nonstd::make_unexpected(obj.error());

    cloud::GcsReadFileOptions opts;
    opts.creds_provider = &creds_provider_;
    opts.pool = CreatePool();
    opts.pool_owned = true;
    return cloud::OpenReadGcsFile(obj->bucket, obj->key, opts);
  }

 private:
  io::Result<ObjectPath, GenericError> ParsePath(string_view path) const final {
    return ParseBucketPath(path, kGCSPrefix);
  }

  string BuildPath(string_view bucket, string_view key) const final {
    return absl::StrCat(kGCSPrefix, bucket, "/", key);
  }

  error_code List(string_view bucket, string_view prefix, const ListCb& cb) final {
    cloud::GCS gcs(&creds_provider_, ctx_, fb2::ProactorBase::me());
    string cursor;
    do {
      RETURN_ERROR(gcs.List(bucket, prefix, false, 500, cb, &cursor));
    } while (!cursor.empty());
    return {};
  }

  http::ClientPool* CreatePool() {
    CHECK(ctx_);
    return cloud::GCS::CreateApiConnectionPool(ctx_, fb2::ProactorBase::me()).release();
  }

  cloud::GCPCredsProvider creds_provider_;
  SSL_CTX* ctx_ = nullptr;
};
#endif

class AzureSnapshotStorage final : public CloudSnapshotStorage {
 public:
  ~AzureSnapshotStorage() {
    if (ctx_)
      http::TlsClient::FreeContext(ctx_);
  }

  error_code Init(string_view uri, unsigned connect_ms) {
    auto azure_path = ParseAzurePath(uri);
    if (!azure_path)
      return azure_path.error();

    creds_provider_ = make_unique<cloud::azure::Credentials>(std::move(azure_path->account));
    RETURN_ERROR(creds_provider_->Init(connect_ms));
    if (creds_provider_->IsHttps())
      ctx_ = http::TlsClient::CreateSslContext();
    return {};
  }

  io::Result<io::WriteFile*, GenericError> OpenWriteFile(const string& path) final {
    auto obj = ParsePath(path);
    if (!obj)
      return nonstd::make_unexpected(obj.error());

    cloud::azure::WriteFileOptions opts;
    opts.creds_provider = creds_provider_.get();
    opts.ssl_cntx = ctx_;
    return WithDetails(cloud::azure::OpenWriteFile(obj->bucket, obj->key, opts),
                       "Could not open file");
  }

  io::ReadonlyFileOrError OpenReadFile(const string& path) final {
    auto obj = ParsePath(path);
    if (!obj)
      return nonstd::make_unexpected(obj.error());

    cloud::azure::ReadFileOptions opts;
    opts.creds_provider = creds_provider_.get();
    opts.ssl_cntx = ctx_;
    return cloud::azure::OpenReadFile(obj->bucket, obj->key, opts);
  }

 private:
  io::Result<ObjectPath, GenericError> ParsePath(string_view path) const final {
    auto azure_path = ParseAzurePath(path);
    if (!azure_path)
      return nonstd::make_unexpected(azure_path.error());
    return ObjectPath{std::move(azure_path->container), std::move(azure_path->key)};
  }

  string BuildPath(string_view bucket, string_view key) const final {
    return BuildAzurePath({.account = creds_provider_->account_name(),
                           .container = string(bucket),
                           .key = string(key)});
  }

  error_code List(string_view bucket, string_view prefix, const ListCb& cb) final {
    cloud::azure::Storage azure(creds_provider_.get());
    string cursor;
    do {
      RETURN_ERROR(azure.List(bucket, prefix, false, 500, cb, &cursor));
    } while (!cursor.empty());
    return {};
  }

  unique_ptr<cloud::azure::Credentials> creds_provider_;
  SSL_CTX* ctx_ = nullptr;
};

class AwsS3SnapshotStorage final : public CloudSnapshotStorage {
 public:
  AwsS3SnapshotStorage(const string& endpoint, bool https, bool ec2_metadata)
      : creds_provider_({}, endpoint), https_(https) {
    if (!ec2_metadata)
      setenv("AWS_EC2_METADATA_DISABLED", "true", 0);
    LOG(INFO) << "Creating AWS S3 client; https=" << std::boolalpha << https
              << "; endpoint=" << endpoint;
  }

  ~AwsS3SnapshotStorage() {
    if (ctx_)
      http::TlsClient::FreeContext(ctx_);
  }

  error_code Init(unsigned connect_ms) {
    RETURN_ERROR(creds_provider_.Init(connect_ms));
    if (https_) {
      ctx_ = http::TlsClient::CreateSslContext();
      if (!ctx_)
        return make_error_code(errc::operation_not_permitted);
    }
    return {};
  }

  io::Result<io::WriteFile*, GenericError> OpenWriteFile(const string& path) final {
    auto obj = ParsePath(path);
    if (!obj)
      return nonstd::make_unexpected(obj.error());

    cloud::aws::WriteFileOptions opts;
    opts.creds_provider = &creds_provider_;
    opts.ssl_cntx = ctx_;
    opts.traffic_mark = absl::GetFlag(FLAGS_s3_upload_traffic_mark);
    uint64_t egress_limit = absl::GetFlag(FLAGS_snapshot_egress_limit_bytes).value;

    // A traffic mark delegates bandwidth shaping to the external tc class, so skip pacing.
    // Otherwise each file uploads one part at a time over its own connection pool, and pacing
    // every connection at the per-shard budget keeps 8MB part PUTs from leaving at line rate.
    if (opts.traffic_mark) {
      LOG_IF(INFO, egress_limit) << "S3 upload traffic mark " << opts.traffic_mark
                                 << " is set, skipping socket pacing for " << path;
    } else {
      opts.max_pacing_rate = egress_limit;
    }

    return WithDetails(cloud::aws::OpenWriteFile(obj->bucket, obj->key, opts),
                       "Could not open file");
  }

  io::ReadonlyFileOrError OpenReadFile(const string& path) final {
    VLOG(1) << "Opening S3 read file: " << path;
    auto obj = ParsePath(path);
    if (!obj)
      return nonstd::make_unexpected(obj.error());

    cloud::aws::ReadFileOptions opts;
    opts.creds_provider = &creds_provider_;
    opts.ssl_cntx = ctx_;
    return cloud::aws::OpenReadFile(obj->bucket, obj->key, opts);
  }

 private:
  io::Result<ObjectPath, GenericError> ParsePath(string_view path) const final {
    return ParseBucketPath(path, kS3Prefix);
  }

  string BuildPath(string_view bucket, string_view key) const final {
    return absl::StrCat(kS3Prefix, bucket, "/", key);
  }

  error_code List(string_view bucket, string_view prefix, const ListCb& cb) final {
    cloud::aws::S3Storage s3(&creds_provider_, ctx_, fb2::ProactorBase::me());
    string cursor;

    // Each list request returns at most 1000 objects, so page through them.
    do {
      RETURN_ERROR(s3.List(bucket, prefix, false, 1000, cb, &cursor));
    } while (!cursor.empty());
    return {};
  }

  cloud::aws::AwsCredsProvider creds_provider_;
  SSL_CTX* ctx_ = nullptr;
  bool https_ = true;
};

// Initializes `storage` in a proactor thread.
template <typename Storage, typename... Args>
io::Result<shared_ptr<SnapshotStorage>, GenericError> InitStorage(shared_ptr<Storage> storage,
                                                                  string_view what, Args... args) {
  error_code ec =
      shard_set->pool()->GetNextProactor()->Await([&] { return storage->Init(args...); });
  if (ec)
    return nonstd::make_unexpected(GenericError{ec, absl::StrCat("Failed to initialize ", what)});
  return storage;
}

}  // namespace

// Helps to recognize Azure path - used in server_family.cc to initialize AzureSnapshotStorage.
bool IsAzurePath(string_view path) {
  return bool(ParseAzurePath(path));
}

io::Result<vector<string>, GenericError> SnapshotStorage::ExpandSnapshot(const string& load_path) {
  const bool is_dfs = absl::EndsWith(load_path, kSummarySuffix);
  if (!is_dfs && !absl::EndsWith(load_path, ".rdb"))
    return Unexpected(errc::invalid_argument, "Bad filename extension");

  if (error_code ec = CheckPath(load_path); ec)
    return nonstd::make_unexpected(GenericError(ec, "File not found"));

  if (!is_dfs)
    return vector<string>{load_path};

  auto files = ListShardFiles(load_path);
  if (!files)
    return files;
  if (files->empty())
    return Unexpected(errc::no_such_file_or_directory, "Could not find DFS shard files");
  files->push_back(load_path);
  return files;
}

FileSnapshotStorage::FileSnapshotStorage(fb2::FiberQueueThreadPool* fq_threadpool)
    : fq_threadpool_{fq_threadpool} {
}

io::Result<io::WriteFile*, GenericError> FileSnapshotStorage::OpenWriteFile(const string& path) {
#ifdef __linux__
  if (!fq_threadpool_) {
    const int flags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC | O_DIRECT;
    auto res = fb2::OpenLinux(path, flags, 0666);
    if (!res) {
      return nonstd::make_unexpected(GenericError(
          res.error(),
          "Couldn't open file for writing (is direct I/O supported by the file system?)"));
    }
    return new LinuxWriteWrapper(std::move(*res));
  }
#endif

  FiberWriteOptions opts;
  opts.direct = true;
  return WithDetails(OpenFiberWriteFile(path, fq_threadpool_, opts),
                     "Couldn't open file for writing");
}

io::ReadonlyFileOrError FileSnapshotStorage::OpenReadFile(const string& path) {
#ifdef __linux__
  if (!fq_threadpool_)
    return fb2::OpenRead(path);
#endif
  return OpenFiberReadFile(path, fq_threadpool_);
}

io::Result<string, GenericError> FileSnapshotStorage::LoadPath(string_view dir,
                                                               string_view dbfilename) {
  if (dbfilename.empty())
    return "";

  fs::path data_folder;
  if (dir.empty()) {
    data_folder = fs::current_path();
  } else {
    error_code file_ec;
    data_folder = fs::canonical(dir, file_ec);
    if (file_ec)
      return nonstd::make_unexpected(GenericError{file_ec, "Data directory error"});
  }

  LOG(INFO) << "Load snapshot: Searching for snapshot in directory: " << data_folder;

  fs::path fl_path = data_folder.append(dbfilename);

  // If we've found an exact match we're done.
  if (fs::exists(fl_path))
    return fl_path.generic_string();

  SubstituteFilenamePlaceholders(&fl_path, {"*", "*", "*", "*"});
  if (!fl_path.has_extension())
    fl_path += "*";

  io::Result<io::StatShortVec> files = io::StatFiles(fl_path.generic_string());
  if (!files)
    return nonstd::make_unexpected(
        GenericError(files.error(), "Could not stat snapshot directory"));

  const io::StatShort* newest = nullptr;
  for (const io::StatShort& stat : *files) {
    if ((!newest || stat.last_modified > newest->last_modified) &&
        (absl::EndsWith(stat.name, ".rdb") || absl::EndsWith(stat.name, kSummarySuffix))) {
      newest = &stat;
    }
  }
  if (!newest)
    return Unexpected(errc::no_such_file_or_directory, "Snapshot not found");
  return newest->name;
}

io::Result<vector<string>, GenericError> FileSnapshotStorage::ListShardFiles(
    const string& summary_path) {
  string_view base = absl::StripSuffix(summary_path, kSummarySuffix);
  io::Result<io::StatShortVec> files = io::StatFiles(absl::StrCat(base, "????.dfs"));
  if (!files)
    return nonstd::make_unexpected(GenericError(files.error(), "Could not list DFS shard files"));

  vector<string> paths;
  for (io::StatShort& stat : *files) {
    if (IsShardFile(stat.name, base))
      paths.push_back(std::move(stat.name));
  }
  return paths;
}

error_code FileSnapshotStorage::CheckPath(const string& path) {
  error_code ec;
  fs::path canonical = fs::canonical(path, ec);
  if (ec)
    return ec;

  // A directory (or other non-regular file) shadowing a .rdb path passes canonical() but fails
  // to load; reject it before the caller flushes the dataset.
  error_code type_ec;
  if (!fs::is_regular_file(canonical, ec) && !ec)
    ec = fs::is_directory(canonical, type_ec) ? make_error_code(errc::is_a_directory)
                                              : make_error_code(errc::invalid_argument);
  return ec;
}

io::Result<shared_ptr<SnapshotStorage>, GenericError> CreateCloudSnapshotStorage(string_view uri) {
  if (IsS3Path(uri)) {
    return InitStorage(make_shared<AwsS3SnapshotStorage>(absl::GetFlag(FLAGS_s3_endpoint),
                                                         absl::GetFlag(FLAGS_s3_use_https),
                                                         absl::GetFlag(FLAGS_s3_ec2_metadata)),
                       "AWS S3 snapshot storage", kBucketConnectMs);
  }
  if (IsGCSPath(uri)) {
#ifdef WITH_GCP
    return InitStorage(make_shared<GcsSnapshotStorage>(), "GCS snapshot storage", kGcsConnectMs);
#else
    return Unexpected(errc::not_supported, "Compiled without GCP support");
#endif
  }
  if (IsAzurePath(uri)) {
    return InitStorage(make_shared<AzureSnapshotStorage>(), "Azure snapshot storage", uri,
                       kBucketConnectMs);
  }
  return Unexpected(errc::invalid_argument, absl::StrCat("Unknown cloud storage ", uri));
}

void SubstituteFilenamePlaceholders(fs::path* filename, const FilenameSubstitutions& fns) {
  *filename = absl::StrReplaceAll(
      filename->string(),
      {{"{Y}", fns.year}, {"{m}", fns.month}, {"{d}", fns.day}, {"{timestamp}", fns.ts}});
}

}  // namespace detail
}  // namespace dfly
