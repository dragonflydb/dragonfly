
// Copyright 2023, DragonflyDB authors.  All rights reserved.
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

ABSL_FLAG(string, bind, "",
          "Bind address. If empty - binds on all interfaces. "
          "It's not advised due to security implications.");
ABSL_FLAG(string, pidfile, "", "If not empty - server writes its pid into the file");
ABSL_FLAG(string, unixsocket, "",
          "If not empty - specifies path for the Unis socket that will "
          "be used for listening for incoming connections.");
ABSL_FLAG(string, unixsocketperm, "", "Set permissions for unixsocket, in octal value.");
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

  auto tcp_disabled = GetFlag(FLAGS_port) == 0u;
  Listener* main_listener = nullptr;

  if (!tcp_disabled)
    main_listener = new Listener{Protocol::REDIS, &service};

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
    string perm_str = GetFlag(FLAGS_unixsocketperm);
    mode_t unix_socket_perm;
    if (perm_str.empty()) {
      // get umask of running process, indicates the permission bits that are turned off
      mode_t umask_val = umask(0);
      umask(umask_val);
      unix_socket_perm = 0777 & ~umask_val;
    } else {
      if (!absl::numbers_internal::safe_strtoi_base(perm_str, &unix_socket_perm, 8) ||
          unix_socket_perm > 0777) {
        LOG(ERROR) << "Invalid unixsocketperm: " << perm_str;
        exit(1);
      }
    }
    unlink(unix_sock.c_str());

    Listener* uds_listener = new Listener{Protocol::REDIS, &service};
    error_code ec = acceptor->AddUDSListener(unix_sock.c_str(), unix_socket_perm, uds_listener);
    if (ec) {
      if (tcp_disabled) {
        LOG(ERROR) << "Could not open unix socket " << unix_sock
                   << ", and TCP listening is disabled (error: " << ec << "). Exiting.";
        exit(1);
      } else {
        LOG(WARNING) << "Could not open unix socket " << unix_sock << ", error " << ec;
      }
      delete uds_listener;
    } else {
      LOG(INFO) << "Listening on unix socket " << unix_sock;
      unlink_uds = true;
    }
  } else if (tcp_disabled) {
    LOG(ERROR)
        << "Did not receive a unix socket to listen to, yet TCP listening is disabled. Exiting.";
    exit(1);
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

  if (!tcp_disabled) {
    error_code ec = acceptor->AddListener(bind_addr, port, main_listener);

    if (ec) {
      LOG(ERROR) << "Could not open port " << port << ", error: " << ec.message();
      exit(1);
    }
  }

  if (mc_port > 0 && !tcp_disabled) {
    acceptor->AddListener(mc_port, new Listener{Protocol::MEMCACHE, &service});
  }

  VersionMonitor version_monitor;
  version_monitor.Run(pool);

  // Start the acceptor loop and wait for the server to shutdown.
  acceptor->Run();
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

error_code GetCGroupPath(string* memory_path, string* cpu_path) {
  CHECK(memory_path != nullptr) << "memory_path is null! (this shouldn't happen!)";
  CHECK(cpu_path != nullptr) << "cpu_path is null! (this shouldn't happen!)";

  // Begin by reading /proc/self/cgroup

  auto cg = io::ReadFileToString("/proc/self/cgroup");
  CHECK(cg.has_value()) << "Failed to read /proc/self/cgroup";

  string cgv = std::move(cg).value();

  // Next, depending on cgroup version we either read:
  // N:<cgroup name>:<path> -- in case of v1, in many lines
  // 0::<cgroup name> -- in case of v2, in a single line

  auto stripped = absl::StripAsciiWhitespace(cgv);

  vector<string_view> groups = absl::StrSplit(stripped, '\n');

  if (groups.size() == 1) {
    // for v2 we only read 0::<name>
    size_t pos = cgv.rfind(':');
    if (pos == string::npos)
      return make_error_code(errc::not_supported);

    string_view res = absl::StripTrailingAsciiWhitespace(cgv.substr(pos + 1));

    *memory_path = absl::StrCat("/sys/fs/cgroup/", res);
    *cpu_path = *memory_path;  // in v2 the path to the cgroup is singular
  } else {
    for (const auto& sv : groups) {
      // in v1 the format is
      // N:s1:2 where N is an integer, s1, s2 strings with s1 maybe empty.
      vector<string_view> entry = absl::StrSplit(sv, ':');
      CHECK_EQ(entry.size(), 3u);

      // in v1 there are several 'canonical' cgroups
      // we are interested in the 'memory' and the 'cpu,cpuacct' ones
      // which specify memory and cpu limits, respectively.
      if (entry[1] == "memory")
        *memory_path = absl::StrCat("/sys/fs/cgroup/memory/", entry[2]);

      if (entry[1] == "cpu,cpuacct")
        *cpu_path = absl::StrCat("/sys/fs/cgroup/cpu,cpuacct/", entry[2]);
    }
  }

  return error_code{};
}

void UpdateResourceLimitsIfInsideContainer(io::MemInfoData* mdata, size_t* max_threads) {
  using io::Exists;

  string mem_path, cpu_path;
  auto err = GetCGroupPath(&mem_path, &cpu_path);

  if (mem_path.empty() || cpu_path.empty()) {
    VLOG(1) << "Failed to get cgroup path, error: " << err;
    return;
  }

  auto original_memory = mdata->mem_total;

  /* Update memory limits */
  auto mlimit = io::ReadFileToString(mem_path + "/memory.limit_in_bytes");
  DVLOG(1) << "memory/memory.limit_in_bytes: " << mlimit.value_or("N/A");

  if (mlimit && !absl::StartsWith(*mlimit, "max")) {
    CHECK(absl::SimpleAtoi(*mlimit, &mdata->mem_total));
  }

  auto mmax = io::ReadFileToString(mem_path + "/memory.max");
  DVLOG(1) << "memory.max: " << mmax.value_or("N/A");

  if (mmax && !absl::StartsWith(*mmax, "max")) {
    CHECK(absl::SimpleAtoi(*mmax, &mdata->mem_total));
  }

  mdata->mem_avail = mdata->mem_total;
  auto mhigh = io::ReadFileToString(mem_path + "/memory.high");

  if (mhigh && !absl::StartsWith(*mhigh, "max")) {
    CHECK(absl::SimpleAtoi(*mhigh, &mdata->mem_avail));
  }

  if (numeric_limits<size_t>::max() == mdata->mem_total)
    mdata->mem_avail = original_memory;

  /* Update thread limits */

  double count = 0, timeshare = 1;

  if (auto cpu = ReadFileToString(cpu_path + "/cpu.max"); cpu.has_value()) {
    vector<string_view> res = absl::StrSplit(cpu.value(), ' ');

    CHECK_EQ(res.size(), 2u);

    if (res[0] == "max")
      *max_threads = 0u;
    else {
      CHECK(absl::SimpleAtod(res[0], &count));
      CHECK(absl::SimpleAtod(res[1], &timeshare));

      *max_threads = static_cast<size_t>(ceil(count / timeshare));
    }
  } else if (auto quota = ReadFileToString(cpu_path + "/cpu.cfs_quota_us"); quota.has_value()) {
    auto period = ReadFileToString(cpu_path + "/cpu.cfs_period_us");

    CHECK(period.has_value()) << "Failed to read cgroup cpu.cfs_period_us, but read cpu.cfs_quota "
                                 "us (this shouldn't happen!)";

    CHECK(absl::SimpleAtod(quota.value(), &count));

    if (count == -1)  // on -1 there is no limit.
      count = 0;

    CHECK(absl::SimpleAtod(period.value(), &timeshare));
  } else {
    *max_threads = 0u;  // cpuset, this is handled later.
    return;
  }

  *max_threads = static_cast<size_t>(ceil(count / timeshare));
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

  if (GetFlag(FLAGS_port) == 0u) {
    string usock = GetFlag(FLAGS_unixsocket);
    if (usock.length() == 0u) {
      LOG(ERROR) << "received --port 0, yet no unix socket to listen to. Exiting.";
      exit(1);
    }
    LOG(INFO) << "received --port 0, disabling TCP listening.";
    LOG(INFO) << "listening on unix socket " << usock << ".";
  }

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
  size_t max_available_threads = 0u;

  UpdateResourceLimitsIfInsideContainer(&memory, &max_available_threads);

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
    size_t limit = GetFlag(FLAGS_maxmemory).value;
    string hr_limit = HumanReadableNumBytes(limit);
    if (limit > memory.mem_avail)
      LOG(WARNING) << "Got memory limit " << hr_limit << ", however only "
                   << HumanReadableNumBytes(memory.mem_avail) << " was found.";
    LOG(INFO) << "Max memory limit is: " << hr_limit;
  }

  dfly::max_memory_limit = GetFlag(FLAGS_maxmemory).value;

  mi_option_enable(mi_option_show_errors);
  mi_option_set(mi_option_max_warnings, 0);
  mi_option_set(mi_option_decommit_delay, 0);

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  CHECK_LT(kver.major, 99u);
  dfly::kernel_version = kver.kernel * 100 + kver.major;

  unique_ptr<util::ProactorPool> pool;

  bool use_epoll = ShouldUseEpollAPI(kver);

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
