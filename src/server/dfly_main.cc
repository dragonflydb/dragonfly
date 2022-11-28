
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
#include <absl/strings/strip.h>
#include <liburing.h>
#include <mimalloc.h>
#include <signal.h>

#include "base/init.h"
#include "base/proc_util.h"  // for GetKernelVersion
#include "facade/dragonfly_listener.h"
#include "io/file.h"
#include "io/file_util.h"
#include "io/proc_reader.h"
#include "server/common.h"
#include "server/main_service.h"
#include "server/version.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/epoll/epoll_pool.h"
#include "util/uring/uring_pool.h"
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
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(uint32_t, memcache_port);

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

using namespace util;
using namespace facade;
using namespace io;
using absl::GetFlag;
using absl::StrCat;
using strings::HumanReadableNumBytes;

namespace dfly {

namespace {

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

  error_code ec = acceptor->AddListener(bind_addr, port, main_listener);

  LOG_IF(FATAL, ec) << "Could not open port " << port << ", error: " << ec.message();

  if (mc_port > 0) {
    acceptor->AddListener(mc_port, new Listener{Protocol::MEMCACHE, &service});
  }

  acceptor->Run();
  acceptor->Wait();

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

  if (GetFlag(FLAGS_maxmemory).value == 0) {
    LOG(INFO) << "maxmemory has not been specified. Deciding myself....";

    Result<MemInfoData> res = ReadMemInfo();
    size_t available = res->mem_avail;
    size_t maxmemory = size_t(0.8 * available);
    LOG(INFO) << "Found " << HumanReadableNumBytes(available)
              << " available memory. Setting maxmemory to " << HumanReadableNumBytes(maxmemory);
    absl::SetFlag(&FLAGS_maxmemory, MaxMemoryFlag(maxmemory));
  } else {
    LOG(INFO) << "Max memory limit is: " << HumanReadableNumBytes(GetFlag(FLAGS_maxmemory).value);
  }

  dfly::max_memory_limit = GetFlag(FLAGS_maxmemory).value;

  if (GetFlag(FLAGS_use_large_pages)) {
    mi_option_enable(mi_option_large_os_pages);
  }
  mi_option_enable(mi_option_show_errors);
  mi_option_set(mi_option_max_warnings, 0);

  // TODO: remove in case of merge
  mi_option_set(mi_option_decommit_extend_delay, 0);
  mi_option_set(mi_option_decommit_delay, 0);

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  CHECK_LT(kver.major, 99u);
  dfly::kernel_version = kver.kernel * 100 + kver.major;

  unique_ptr<util::ProactorPool> pool;

  bool use_epoll = ShouldUseEpollAPI(kver);
  if (use_epoll) {
    pool.reset(new epoll::EpollPool);
  } else {
    pool.reset(new uring::UringPool(1024));  // 1024 - iouring queue size.
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
