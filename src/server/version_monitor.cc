// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/version_monitor.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <openssl/err.h>
#include <sys/utsname.h>

#include <boost/beast/http/string_body.hpp>
#include <regex>

#include "base/logging.h"
#include "io/file_util.h"
#include "server/common.h"
#include "server/version.h"

namespace dfly {

using namespace std;
using namespace util;
using http::TlsClient;

namespace {

// Initialized during static initialization, i.e. at process start.
const auto kProcessStart = chrono::steady_clock::now();

std::optional<std::string> GetVersionString(const std::string& version_str) {
  // The server sends a message such as {"latest": "0.12.0"}
  const auto reg_match_expr = R"(\{\"latest"\:[ \t]*\"([0-9]+\.[0-9]+\.[0-9]+)\"\})";
  VLOG(1) << "checking version '" << version_str << "'";
  auto const regex = std::regex(reg_match_expr);
  std::smatch match;
  if (std::regex_match(version_str, match, regex) && match.size() > 1) {
    // the second entry is the match to the group that holds the version string
    return match[1].str();
  } else {
    LOG_FIRST_N(WARNING, 1) << "Remote version - invalid version number: '" << version_str << "'";
    return std::nullopt;
  }
}

std::optional<std::string> GetRemoteVersion(ProactorBase* proactor, SSL_CTX* ssl_context,
                                            const std::string host, std::string_view service,
                                            const std::string& resource,
                                            const std::string& ver_header,
                                            const VersionMonitor::HeaderList& info_headers) {
  namespace bh = boost::beast::http;
  using ResponseType = bh::response<bh::string_body>;

  bh::request<bh::string_body> req{bh::verb::get, resource, 11 /*http 1.1*/};
  req.set(bh::field::host, host);
  req.set(bh::field::user_agent, ver_header);
  for (const auto& [name, value] : info_headers) {
    req.set(boost::beast::string_view{name.data(), name.size()}, value);
  }
  ResponseType res;
  TlsClient http_client{proactor};
  http_client.set_connect_timeout_ms(2000);

  auto ec = http_client.Connect(host, service, ssl_context);

  if (ec) {
    LOG_FIRST_N(WARNING, 1) << "Remote version - connection error [" << host << ":" << service
                            << "] : " << ec.message();
    return nullopt;
  }

  ec = http_client.Send(req, &res);
  if (!ec) {
    VLOG(1) << "successfully got response from HTTP GET for host " << host << ":" << service << "/"
            << resource << " response code is " << res.result();

    if (res.result() == bh::status::ok) {
      return GetVersionString(res.body());
    }
  } else {
    static bool is_logged{false};
    if (!is_logged) {
      is_logged = true;

#if (OPENSSL_VERSION_NUMBER >= 0x30000000L)
      const char* func_err = "ssl_internal_error";
#else
      const char* func_err = ERR_func_error_string(ec.value());
#endif

      // Unfortunately AsioStreamAdapter looses the original error category
      // because std::error_code can not be converted into boost::system::error_code.
      // It's fixed in later versions of Boost, but for now we assume it's from TLS.
      LOG(WARNING) << "Remote version - HTTP GET error [" << host << ":" << service << resource
                   << "], error: " << ec.value();
      LOG(WARNING) << "ssl error: " << func_err << "/" << ERR_reason_error_string(ec.value());
    }
  }

  return nullopt;
}

// Maps the DMI system vendor to a coarse cloud provider name. Only the category is reported.
string_view GetCloudProvider() {
  auto vendor = io::ReadFileToString("/sys/class/dmi/id/sys_vendor");
  if (!vendor)
    return "unknown";

  string_view v = absl::StripAsciiWhitespace(*vendor);
  if (absl::StartsWith(v, "Amazon"))
    return "aws";
  if (absl::StartsWith(v, "Google"))
    return "gcp";
  if (absl::StartsWith(v, "Microsoft"))
    return "azure";

  // Older Xen-based EC2 instances report "Xen" as the vendor.
  auto bios = io::ReadFileToString("/sys/class/dmi/id/bios_version");
  if (bios && absl::StrContains(absl::AsciiStrToLower(*bios), "amazon"))
    return "aws";

  return "other";
}

// Returns the exclusive upper limit of the bucket that contains bytes.
string_view MemoryBucket(uint64_t bytes) {
  constexpr uint64_t kGB = 1ULL << 30;
  if (bytes < kGB)
    return "1G";
  if (bytes < 10 * kGB)
    return "10G";
  if (bytes < 100 * kGB)
    return "100G";
  if (bytes < 1024 * kGB)
    return "1T";
  return "inf";
}

}  // namespace

bool VersionMonitor::IsVersionOutdated(const std::string_view remote,
                                       const std::string_view current) const {
  const absl::InlinedVector<absl::string_view, 3> remote_xyz = absl::StrSplit(remote, ".");
  const absl::InlinedVector<absl::string_view, 3> current_xyz = absl::StrSplit(current, ".");
  if (remote_xyz.size() != current_xyz.size()) {
    LOG(WARNING) << "Can't compare Dragonfly version " << current << " to latest version "
                 << remote;
    return false;
  }
  const auto print_to_log = [](const std::string_view version, const absl::string_view part) {
    LOG(WARNING) << "Can't parse " << version << " part of version " << part << " as a number";
  };
  for (size_t i = 0; i < remote_xyz.size(); ++i) {
    size_t remote_x = 0;
    if (!absl::SimpleAtoi(remote_xyz[i], &remote_x)) {
      print_to_log(remote, remote_xyz[i]);
      return false;
    }
    size_t current_x = 0;
    if (!absl::SimpleAtoi(current_xyz[i], &current_x)) {
      print_to_log(current, current_xyz[i]);
      return false;
    }
    if (remote_x > current_x) {
      return true;
    }

    if (remote_x < current_x) {
      return false;
    }
  }

  return false;
}

void VersionMonitor::Run(ProactorPool* proactor_pool) {
  // Avoid running dev environments.
  if (getenv("DFLY_DEV_ENV")) {
    LOG(WARNING) << "Running in dev environment (DFLY_DEV_ENV is set) - version monitoring is "
                    "disabled";
    return;
  }

  SslPtr ssl_ctx(TlsClient::CreateSslContext());
  if (!ssl_ctx) {
    VLOG(1) << "Remote version - failed to create SSL context - cannot run version monitoring";
    return;
  }

  utsname uts;
  string_view arch = uname(&uts) == 0 ? uts.machine : "unknown";
  bool is_uring = proactor_pool->at(0)->GetKind() == ProactorBase::IOURING;

  // Low-cardinality platform info goes into the User-Agent comment. The field order is part of
  // the protocol: append new fields at the end and never reorder or remove existing ones.
  platform_ = absl::StrCat(arch, "; ", is_uring ? "io_uring" : "epoll", "; ", GetCloudProvider());

  static_headers_ = {
      {"Dfly-Threads", absl::StrCat(proactor_pool->size())},
      {"Dfly-Kernel", absl::StrCat(kernel_version / 100, ".", kernel_version % 100)},
  };

  version_fiber_ = proactor_pool->GetNextProactor()->LaunchFiber(
      [ssl_ctx = std::move(ssl_ctx), this]() mutable { RunTask(std::move(ssl_ctx)); });
}

void VersionMonitor::Shutdown() {
  monitor_ver_done_.Notify();
  if (version_fiber_.IsJoinable()) {
    version_fiber_.Join();
  }
}

VersionMonitor::HeaderList VersionMonitor::BuildInfoHeaders() const {
  HeaderList headers = static_headers_;
  auto uptime = chrono::duration_cast<chrono::seconds>(chrono::steady_clock::now() - kProcessStart);
  headers.emplace_back("Dfly-Uptime", absl::StrCat(uptime.count()));
  headers.emplace_back("Dfly-Max-Mem",
                       string(MemoryBucket(max_memory_limit.load(memory_order_relaxed))));
  return headers;
}

void VersionMonitor::RunTask(SslPtr ssl_ctx) {
  const auto loop_sleep_time = std::chrono::hours(24);  // every 24 hours
  // Delay the first check so that short-lived or crash-looping processes do not ping the server.
  const auto initial_delay = std::chrono::minutes(10);

  const std::string host_name = "version.dragonflydb.io";
  const std::string_view port = "443";
  const std::string resource = "/v1";
  string_view current_version(kGitTag);

  current_version.remove_prefix(1);
  const std::string version_header =
      absl::StrCat("DragonflyDB/", current_version, " (", platform_, ")");

  if (monitor_ver_done_.WaitFor(initial_delay)) {
    VLOG(1) << "finish running version monitor task";
    return;
  }

  ProactorBase* my_pb = ProactorBase::me();
  while (true) {
    const HeaderList info_headers = BuildInfoHeaders();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << "version check User-Agent: " << version_header;
      for (const auto& [name, value] : info_headers)
        VLOG(1) << "version check header " << name << ": " << value;
    }
    const std::optional<std::string> remote_version = GetRemoteVersion(
        my_pb, ssl_ctx.get(), host_name, port, resource, version_header, info_headers);
    if (remote_version) {
      const std::string_view rv = remote_version.value();
      if (IsVersionOutdated(rv, current_version)) {
        LOG_FIRST_N(INFO, 1) << "Your current version '" << current_version
                             << "' is not the latest version. A newer version '" << rv
                             << "' is now available. Please consider an update.";
      }
    }
    if (monitor_ver_done_.WaitFor(loop_sleep_time)) {
      VLOG(1) << "finish running version monitor task";
      return;
    }
  }
}

}  // namespace dfly
