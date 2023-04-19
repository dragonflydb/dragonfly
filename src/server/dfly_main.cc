
// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifdef NDEBUG
#include <mimalloc-new-delete.h>
#endif

#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <liburing.h>
#include <mimalloc.h>
#include <openssl/err.h>
#include <signal.h>

#include <regex>

#include "base/init.h"
#include "base/proc_util.h"  // for GetKernelVersion
#include "facade/dragonfly_listener.h"
#include "io/file.h"
#include "io/file_util.h"
#include "io/proc_reader.h"
#include "server/common.h"
#include "server/generic_family.h"
#include "server/main_service.h"
#include "server/version.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"
#include "util/http/http_client.h"
#include "util/varz.h"

#define STRING_PP_NX(A) #A
#define STRING_MAKE_PP(A) STRING_PP_NX(A)

// This would create a string value from a "defined" location of the source code
// Note that SOURCE_PATH_FROM_BUILD_ENV is taken from the build system
#define BUILD_LOCATION_PATH STRING_MAKE_PP(SOURCE_PATH_FROM_BUILD_ENV)

using namespace std;

struct MaxMemoryFlag {
  MaxMemoryFlag() = default;
  MaxMemoryFlag(const MaxMemoryFlag&) = default;
  MaxMemoryFlag& operator=(const MaxMemoryFlag&) = default;
  MaxMemoryFlag(uint64_t v) : value(v) {
  }  // NOLINT

  uint64_t value;
};

bool AbslParseFlag(absl::string_view in, MaxMemoryFlag* flag, std::string* err) {
  int64_t val;
  if (dfly::ParseHumanReadableBytes(in, &val) && val >= 0) {
    flag->value = val;
    return true;
  }

  *err = "Use human-readable format, eg.: 1G, 1GB, 10GB";
  return false;
}

std::string AbslUnparseFlag(const MaxMemoryFlag& flag) {
  return strings::HumanReadableNumBytes(flag.value);
}

ABSL_DECLARE_FLAG(uint32_t, port);
ABSL_DECLARE_FLAG(uint32_t, memcache_port);
ABSL_DECLARE_FLAG(uint16_t, admin_port);
ABSL_DECLARE_FLAG(std::string, admin_bind);

ABSL_FLAG(bool, use_large_pages, false, "If true - uses large memory pages for allocations");
ABSL_FLAG(string, bind, "",
          "Bind address. If empty - binds on all interfaces. "
          "It's not advised due to security implications.");
ABSL_FLAG(string, pidfile, "", "If not empty - server writes its pid into the file");
ABSL_FLAG(string, unixsocket, "",
          "If not empty - specifies path for the Unis socket that will "
          "be used for listening for incoming connections.");
ABSL_FLAG(bool, force_epoll, false,
          "If true - uses linux epoll engine underneath."
          "Can fit for kernels older than 5.10.");
ABSL_FLAG(MaxMemoryFlag, maxmemory, MaxMemoryFlag(0),
          "Limit on maximum-memory that is used by the database. "
          "0 - means the program will automatically determine its maximum memory usage. "
          "default: 0");

ABSL_FLAG(bool, version_check, true,
          "If true, Will monitor for new releases on Dragonfly servers once a day.");

using namespace util;
using namespace facade;
using namespace io;
using absl::GetFlag;
using absl::StrCat;
using strings::HumanReadableNumBytes;

namespace dfly {

namespace {

using util::http::TlsClient;

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
                                            const std::string& ver_header) {
  namespace bh = boost::beast::http;
  using ResponseType = bh::response<bh::string_body>;

  bh::request<bh::string_body> req{bh::verb::get, resource, 11 /*http 1.1*/};
  req.set(bh::field::host, host);
  req.set(bh::field::user_agent, ver_header);
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

struct VersionMonitor {
  Fiber version_fiber_;
  Done monitor_ver_done_;

  void Run(ProactorPool* proactor_pool);

  void Shutdown() {
    monitor_ver_done_.Notify();
    if (version_fiber_.IsJoinable()) {
      version_fiber_.Join();
    }
  }

 private:
  void RunTask(SSL_CTX* ssl_ctx);
};

void VersionMonitor::Run(ProactorPool* proactor_pool) {
  // Avoid running dev environments.
  bool is_dev_env = false;
  const char* env_var = getenv("DFLY_DEV_ENV");
  if (env_var) {
    LOG(WARNING) << "Running in dev environment (DFLY_DEV_ENV is set) - version monitoring is "
                    "disabled";
    is_dev_env = true;
  }
  if (!GetFlag(FLAGS_version_check) || is_dev_env ||
      // not a production release tag.
      kGitTag[0] != 'v' || strchr(kGitTag, '-') != NULL) {
    return;
  }

  SSL_CTX* ssl_ctx = TlsClient::CreateSslContext();
  if (!ssl_ctx) {
    VLOG(1) << "Remote version - failed to create SSL context - cannot run version monitoring";
    return;
  }

  version_fiber_ =
      proactor_pool->GetNextProactor()->LaunchFiber([ssl_ctx, this] { RunTask(ssl_ctx); });
}

void VersionMonitor::RunTask(SSL_CTX* ssl_ctx) {
  const auto loop_sleep_time = std::chrono::hours(24);  // every 24 hours

  const std::string host_name = "version.dragonflydb.io";
  const std::string_view port = "443";
  const std::string resource = "/v1";
  string_view current_version(kGitTag);

  current_version.remove_prefix(1);
  const std::string version_header = absl::StrCat("DragonflyDB/", current_version);

  ProactorBase* my_pb = ProactorBase::me();
  while (true) {
    const std::optional<std::string> remote_version =
        GetRemoteVersion(my_pb, ssl_ctx, host_name, port, resource, version_header);
    if (remote_version) {
      const std::string rv = remote_version.value();
      if (rv != current_version) {
        LOG_FIRST_N(INFO, 1) << "Your current version '" << current_version
                             << "' is not the latest version. A newer version '" << rv
                             << "' is now available. Please consider an update.";
      }
    }
    if (monitor_ver_done_.WaitFor(loop_sleep_time)) {
      TlsClient::FreeContext(ssl_ctx);
      VLOG(1) << "finish running version monitor task";
      return;
    }
  }
}

enum class TermColor { kDefault, kRed, kGreen, kYellow };
// Returns the ANSI color code for the given color. TermColor::kDefault is
// an invalid input.
static const char* GetAnsiColorCode(TermColor color) {
  switch (color) {
    case TermColor::kRed:
      return "1";
    case TermColor::kGreen:
      return "2";
    case TermColor::kYellow:
      return "3";
    default:
      return nullptr;
  }
}

string ColorStart(TermColor color) {
  return StrCat("\033[0;3", GetAnsiColorCode(color), "m");
}

// Resets the terminal to default.
const char kColorEnd[] = "\033[m";

string ColoredStr(TermColor color, string_view str) {
  return StrCat(ColorStart(color), str, kColorEnd);
}

bool HelpshortFlags(std::string_view f) {
  return absl::StartsWith(f, "\033[0;32");
}

bool HelpFlags(std::string_view f) {
  return absl::StartsWith(f, "\033[0;3");
}

string NormalizePaths(std::string_view path) {
  const std::string FULL_PATH = BUILD_LOCATION_PATH;
  const std::string FULL_PATH_SRC = FULL_PATH + "/src";
  const std::string FULL_PATH_HELIO = FULL_PATH + "/helio";

  if (absl::ConsumePrefix(&path, "../src/") || absl::ConsumePrefix(&path, FULL_PATH_SRC))
    return ColoredStr(TermColor::kGreen, path);

  if (absl::ConsumePrefix(&path, "../") || absl::ConsumePrefix(&path, FULL_PATH_HELIO))
    return ColoredStr(TermColor::kYellow, path);

  if (absl::ConsumePrefix(&path, "_deps/"))
    return string(path);

  return string(path);
}

bool RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  auto maxmemory = GetFlag(FLAGS_maxmemory).value;
  if (maxmemory > 0 && maxmemory < pool->size() * 256_MB) {
    LOG(ERROR) << "There are " << pool->size() << " threads, so "
               << HumanReadableNumBytes(pool->size() * 256_MB) << " are required. Exiting...";
    return false;
  }

  Service service(pool);

  Listener* main_listener = new Listener{Protocol::REDIS, &service};

  Service::InitOpts opts;
  opts.disable_time_update = false;
  service.Init(acceptor, main_listener, opts);
  const auto& bind = GetFlag(FLAGS_bind);
  const char* bind_addr = bind.empty() ? nullptr : bind.c_str();
  auto port = GetFlag(FLAGS_port);
  auto mc_port = GetFlag(FLAGS_memcache_port);
  string unix_sock = GetFlag(FLAGS_unixsocket);
  bool unlink_uds = false;

  if (!unix_sock.empty()) {
    unlink(unix_sock.c_str());

    Listener* uds_listener = new Listener{Protocol::REDIS, &service};
    error_code ec = acceptor->AddUDSListener(unix_sock.c_str(), uds_listener);
    if (ec) {
      LOG(WARNING) << "Could not open unix socket " << unix_sock << ", error " << ec;
      delete uds_listener;
    } else {
      LOG(INFO) << "Listening on unix socket " << unix_sock;
      unlink_uds = true;
    }
  }

  std::uint16_t admin_port = GetFlag(FLAGS_admin_port);
  if (admin_port != 0) {
    const std::string& admin_bind = GetFlag(FLAGS_admin_bind);
    // Note passing the result of c_str() for empty string in optimized mode don't work, we must
    // explicitly set this to null in this case
    const char* interface_addr = admin_bind.empty() ? nullptr : admin_bind.c_str();
    const std::string printable_addr =
        absl::StrCat("admin socket ", interface_addr ? interface_addr : "any", ":", admin_port);
    Listener* admin_listener = new Listener{Protocol::REDIS, &service};
    error_code ec = acceptor->AddListener(interface_addr, admin_port, admin_listener);

    if (ec) {
      LOG(ERROR) << "Failed to open " << printable_addr << ", error: " << ec.message();
      delete admin_listener;
    } else {
      LOG(INFO) << "Listening on " << printable_addr;
    }
  }

  error_code ec = acceptor->AddListener(bind_addr, port, main_listener);

  if (ec) {
    LOG(ERROR) << "Could not open port " << port << ", error: " << ec.message();
    exit(1);
  }

  if (mc_port > 0) {
    acceptor->AddListener(mc_port, new Listener{Protocol::MEMCACHE, &service});
  }

  VersionMonitor version_monitor;

  acceptor->Run();
  version_monitor.Run(pool);
  acceptor->Wait();
  version_monitor.Shutdown();
  service.Shutdown();

  if (unlink_uds) {
    unlink(unix_sock.c_str());
  }

  return true;
}

bool CreatePidFile(const string& path) {
  Result<WriteFile*> res = OpenWrite(path);
  if (!res) {
    LOG(ERROR) << "Failed to open pidfile with error: " << res.error().message() << ". Exiting...";
    return false;
  }

  unique_ptr<WriteFile> wf(res.value());
  auto ec = wf->Write(to_string(getpid()));
  if (ec) {
    LOG(ERROR) << "Failed to write pid into pidfile with error: " << ec.message() << ". Exiting...";
    return false;
  }

  ec = wf->Close();
  if (ec) {
    LOG(WARNING) << "Failed to close pidfile file descriptor with error: " << ec.message() << ".";
  }

  return true;
}

bool ShouldUseEpollAPI(const base::sys::KernelVersion& kver) {
  if (GetFlag(FLAGS_force_epoll))
    return true;

  if (kver.kernel < 5 || (kver.kernel == 5 && kver.major < 10)) {
    LOG(WARNING) << "Kernel is older than 5.10, switching to epoll engine.";
    return true;
  }

  struct io_uring ring;
  io_uring_params params;
  memset(&params, 0, sizeof(params));

  int iouring_res = io_uring_queue_init_params(1024, &ring, &params);

  if (iouring_res == 0) {
    io_uring_queue_exit(&ring);
    return false;
  }

  iouring_res = -iouring_res;

  if (iouring_res == ENOSYS) {
    LOG(WARNING) << "iouring API is not supported. switching to epoll.";
  } else if (iouring_res == ENOMEM) {
    LOG(WARNING) << "io_uring does not have enough memory. That can happen when your "
                    "max locked memory is too limited. If you run via docker, "
                    "try adding '--ulimit memlock=-1' to \"docker run\" command."
                    "Meanwhile, switching to epoll";
  } else {
    LOG(WARNING) << "Weird error " << iouring_res << " switching to epoll";
  }

  return true;
}

string get_cgroup_path(void) {
  // Begin by reading /proc/self/cgroup

  auto cg = io::ReadFileToString("/proc/self/cgroup");
  CHECK(cg.has_value());

  // Here, we assume that CGroup v1 is being used. This
  // is quite likely, as CGroup v1 was introduced back in 2015.

  // strip 0::<path> into just <path>, and newline.
  auto cgv = std::move(cg.value());
  return cgv.substr(3, cgv.length() - 3 - 1);
  /**
   * -3: we are skipping the first three characters
   * -1: we discard the last character (a newline)
   */
}

const auto cgroup = "/sys/fs/cgroup/" + get_cgroup_path();

bool InsideContainer() {
  /**
   * (attemps) to check whether we are running
   * inside a container or not.
   *
   * We employ several tests, all of which
   * are flawed, however together do cover a
   * good portion of cases.
   *
   * 1. Checking '/.dockerenv': very simple, however
   * only works for Docker, and this file may be moved
   * in the future.
   * 2. Checking '/sys/fs/cgroup/memory.max': This
   * directory -cannot- be edited (not even using sudoedit),
   * so users are not able to add files to there. The file
   * 'memory.max' should contain a memory limit for processes.
   *
   * We also use this file to find out how much memory we can use.
   * However, on LXC this is placed in a different directory:
   * 3. Checking '/sys/fs/cgroup/memory/memory.limit_in_bytes':
   * Same idea.
   */

  using io::Exists;

  return Exists("/.dockerenv") || Exists(cgroup + "/memory.max");
}

void ReadContainerMemoryLimits(io::MemInfoData& mdata) {
  auto max = io::ReadFileToString(cgroup + "/memory.max");

  if (max.has_value()) {
    auto max_val = max.value();
    if (max_val.find("max") != max_val.npos)
      return; /*use the host's settings. */
    else
      CHECK(absl::SimpleAtoi(max.value(), &mdata.mem_total));
  }

  mdata.mem_avail = mdata.mem_total;
  auto high = io::ReadFileToString(cgroup + "/memory.high");

  if (high.has_value()) {
    auto high_val = high.value();
    if (high_val.find("max") != high_val.npos)
      return;
    else
      CHECK(absl::SimpleAtoi(high.value(), &mdata.mem_avail));
  }
}

size_t ReadContainerThreadLimits() {
  using namespace io;

  /**
   * In order to check for CPU limits, we check two files:
   * - /sys/fs/cgroup/cpu.max: this file contains two tokens:
   *  <a> <b>
   * where <a> is either max, or some number, and <b> is the size
   * of a time share. If <a> is not max, then <a>/<b> is the number
   * of useable CPUs (where a non-integer is treated as a CPU
   * available for a fraction of a time share)
   * - /sys/fs/cgroup/cpuset.cpus: this file is either empty,
   * or otherwise holds a list of the physical CPUs allocated to the container.
   * Since cpuset limits what physical cores the container can use,
   * then we can skip reading this file as we get its value later, as
   * such we return 0.
   */

  if (auto cpu = ReadFileToString(cgroup + "/cpu.max"); cpu.has_value()) {
    vector<string_view> res = absl::StrSplit(cpu.value(), ' ');

    CHECK_EQ(res.size(), 2u);

    if (res[0] == "max")
      return 0;
    else {
      double count, timeshare;
      CHECK(absl::SimpleAtod(res[0], &count));
      CHECK(absl::SimpleAtod(res[1], &timeshare));

      return static_cast<size_t>(ceil(count / timeshare));
    }
  }

  return 0;
}

}  // namespace
}  // namespace dfly

extern "C" void _mi_options_init();

using namespace dfly;

void sigill_hdlr(int signo) {
  LOG(ERROR) << "An attempt to execute an instruction failed."
             << "The root cause might be an old hardware. Exiting...";
  exit(1);
}

int main(int argc, char* argv[]) {
  absl::SetProgramUsageMessage(
      R"(a modern in-memory store.

Usage: dragonfly [FLAGS]
)");

  absl::FlagsUsageConfig config;
  config.contains_help_flags = dfly::HelpFlags;
  config.contains_helpshort_flags = dfly::HelpshortFlags;
  config.normalize_filename = dfly::NormalizePaths;
  config.version_string = [] {
    string version = StrCat(dfly::kGitTag, "-", dfly::kGitSha);
    return StrCat("dragonfly ", ColoredStr(TermColor::kGreen, version),
                  "\nbuild time: ", ColoredStr(TermColor::kYellow, dfly::kBuildTime), "\n");
  };

  absl::SetFlagsUsageConfig(config);

  MainInitGuard guard(&argc, &argv);

  LOG(INFO) << "Starting dragonfly " << GetVersion() << "-" << kGitSha;

  struct sigaction act;
  act.sa_handler = sigill_hdlr;
  sigemptyset(&act.sa_mask);
  sigaction(SIGILL, &act, nullptr);

  CHECK_GT(GetFlag(FLAGS_port), 0u);
  mi_stats_reset();

  if (GetFlag(FLAGS_dbnum) > dfly::kMaxDbId) {
    LOG(ERROR) << "dbnum is too big. Exiting...";
    return 1;
  }

  string pidfile_path = GetFlag(FLAGS_pidfile);
  if (!pidfile_path.empty()) {
    if (!CreatePidFile(pidfile_path)) {
      return 1;
    }
  }

  auto memory = ReadMemInfo().value();

  auto inside_container = InsideContainer();
  if (inside_container)
    ReadContainerMemoryLimits(memory);

  if (memory.swap_total != 0)
    LOG(WARNING) << "SWAP is enabled. Consider disabling it when running Dragonfly.";

  if (GetFlag(FLAGS_maxmemory).value == 0) {
    LOG(INFO) << "maxmemory has not been specified. Deciding myself....";

    size_t available = memory.mem_avail;
    size_t maxmemory = size_t(0.8 * available);
    LOG(INFO) << "Found " << HumanReadableNumBytes(available)
              << " available memory. Setting maxmemory to " << HumanReadableNumBytes(maxmemory);
    absl::SetFlag(&FLAGS_maxmemory, MaxMemoryFlag(maxmemory));
  } else {
    auto limit = GetFlag(FLAGS_maxmemory).value;
    auto hr_limit = HumanReadableNumBytes(limit);
    if (limit > memory.mem_avail)
      LOG(WARNING) << "Got memory limit " << hr_limit << ", however only "
                   << HumanReadableNumBytes(memory.mem_avail) << " was found.";
    LOG(INFO) << "Max memory limit is: " << hr_limit;
  }

  dfly::max_memory_limit = GetFlag(FLAGS_maxmemory).value;

  if (GetFlag(FLAGS_use_large_pages)) {
    mi_option_enable(mi_option_large_os_pages);
  }
  mi_option_enable(mi_option_show_errors);
  mi_option_set(mi_option_max_warnings, 0);
  mi_option_set(mi_option_decommit_delay, 0);

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  CHECK_LT(kver.major, 99u);
  dfly::kernel_version = kver.kernel * 100 + kver.major;

  unique_ptr<util::ProactorPool> pool;

  bool use_epoll = ShouldUseEpollAPI(kver);
  size_t max_available_threads{0};

  if (inside_container)
    max_available_threads = ReadContainerThreadLimits();

  if (use_epoll) {
    pool.reset(fb2::Pool::Epoll(max_available_threads));
  } else {
    pool.reset(fb2::Pool::IOUring(1024, max_available_threads));  // 1024 - iouring queue size.
  }

  pool->Run();

  AcceptServer acceptor(pool.get());

  int res = dfly::RunEngine(pool.get(), &acceptor) ? 0 : -1;

  pool->Stop();

  if (!pidfile_path.empty()) {
    unlink(pidfile_path.c_str());
  }

  return res;
}
