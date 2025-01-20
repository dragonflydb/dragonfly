
// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include "absl/cleanup/cleanup.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/numbers.h"

#ifdef DFLY_ENABLE_MEMORY_TRACKING
#define INJECT_ALLOCATION_TRACKER
#include "core/allocation_tracker.h"
#else
#include <mimalloc-new-delete.h>
#endif

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#endif

#include <mimalloc.h>
#include <signal.h>

#include <iostream>
#include <memory>

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
#include "server/version_monitor.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"
#include "util/varz.h"

#ifdef __APPLE__
#include <crt_externs.h>
#define environ (*_NSGetEnviron())
#else
extern char** environ;
#endif

using namespace std;

ABSL_DECLARE_FLAG(int32_t, port);
ABSL_DECLARE_FLAG(uint32_t, memcached_port);
ABSL_DECLARE_FLAG(uint16_t, admin_port);
ABSL_DECLARE_FLAG(std::string, admin_bind);

ABSL_FLAG(string, bind, "",
          "Bind address. If empty - binds on all interfaces. "
          "It's not advised due to security implications.");
ABSL_FLAG(string, pidfile, "", "If not empty - server writes its pid into the file");
ABSL_FLAG(string, unixsocket, "",
          "If not empty - specifies path for the Unix socket that will "
          "be used for listening for incoming connections.");
ABSL_FLAG(string, unixsocketperm, "", "Set permissions for unixsocket, in octal value.");
ABSL_FLAG(bool, force_epoll, false,
          "If true - uses linux epoll engine underneath. "
          "Can fit for kernels older than 5.10.");
ABSL_FLAG(
    string, allocation_tracker, "",
    "Logs stack trace of memory allocation within these ranges. Format is min:max,min:max,....");

ABSL_FLAG(bool, version_check, true,
          "If true, Will monitor for new releases on Dragonfly servers once a day.");

ABSL_FLAG(uint16_t, tcp_backlog, 256, "TCP listen(2) backlog parameter.");
ABSL_FLAG(uint16_t, uring_recv_buffer_cnt, 0,
          "How many socket recv buffers of size 256 to allocate per thread."
          "Relevant only for modern kernels with io_uring enabled");

using namespace util;
using namespace facade;
using namespace io;
using absl::GetFlag;
using absl::StrCat;
using strings::HumanReadableNumBytes;

namespace dfly {

namespace {

// Default stack size for fibers. We decrease it by 16 bytes because some allocators
// need additional 8-16 bytes for their internal structures, thus over reserving additional
// memory pages if using round sizes.
#ifdef NDEBUG
constexpr size_t kFiberDefaultStackSize = 32_KB - 16;
#else
// Increase stack size for debug builds.
constexpr size_t kFiberDefaultStackSize = 40_KB - 16;
#endif

using util::http::TlsClient;

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

#define STRING_PP_NX(A) #A
#define STRING_MAKE_PP(A) STRING_PP_NX(A)

// This would create a string value from a "defined" location of the source code
// Note that SOURCE_PATH_FROM_BUILD_ENV is taken from the build system
#define BUILD_LOCATION_PATH STRING_MAKE_PP(SOURCE_PATH_FROM_BUILD_ENV)

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

template <typename... Args> unique_ptr<Listener> MakeListener(Args&&... args) {
  auto res = make_unique<Listener>(forward<Args>(args)...);
  res->SetConnFiberStackSize(kFiberDefaultStackSize);
  return res;
}

void RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  uint64_t maxmemory = GetMaxMemoryFlag();
  if (maxmemory > 0 && maxmemory < pool->size() * 256_MB) {
    LOG(ERROR) << "There are " << pool->size() << " threads, so "
               << HumanReadableNumBytes(pool->size() * 256_MB) << " are required. Exiting...";
    exit(1);
  }

  Service service(pool);

  auto tcp_disabled = GetFlag(FLAGS_port) == 0u;
  Listener* main_listener = nullptr;

  std::vector<facade::Listener*> listeners;

  // If we ever add a new listener, plz don't change this,
  // we depend on tcp listener to be at the front since we later
  // need to pass it to the AclFamily::Init
  if (!tcp_disabled) {
    auto listener = MakeListener(Protocol::REDIS, &service, Listener::Role::MAIN);
    main_listener = listener.get();
    listeners.push_back(listener.release());
  }

  const auto& bind = GetFlag(FLAGS_bind);
  const char* bind_addr = bind.empty() ? nullptr : bind.c_str();

  int32_t port = GetFlag(FLAGS_port);
  // The reason for this code is a bit silly. We want to provide a way to
  // bind any 'random' available port. The way to do that is to call
  // bind with the argument port 0. However we can't expose this functionality
  // as is to our users: Since giving --port=0 to redis DISABLES the network
  // interface that would break users' existing configurations in potentionally
  // unsafe ways. For that reason the user's --port=-1 means to us 'bind port 0'.
  if (port == -1) {
    port = 0;
  } else if (port < 0 || port > 65535) {
    LOG(ERROR) << "Bad port number " << port;
    exit(1);
  }

  auto mc_port = GetFlag(FLAGS_memcached_port);
  string unix_sock = GetFlag(FLAGS_unixsocket);
  bool unlink_uds = false;
  absl::Cleanup maybe_unlink_uds([&unlink_uds, &unix_sock]() {
    if (unlink_uds) {
      unlink(unix_sock.c_str());
    }
  });

  if (!unix_sock.empty()) {
    string perm_str = GetFlag(FLAGS_unixsocketperm);
    uint32_t unix_socket_perm;
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

    auto uds_listener = MakeListener(Protocol::REDIS, &service);
    error_code ec =
        acceptor->AddUDSListener(unix_sock.c_str(), unix_socket_perm, uds_listener.get());
    if (ec) {
      if (tcp_disabled) {
        LOG(ERROR) << "Could not open unix socket " << unix_sock
                   << ", and TCP listening is disabled (error: " << ec << "). Exiting.";
        exit(1);
      } else {
        LOG(WARNING) << "Could not open unix socket " << unix_sock << ", error " << ec;
      }
    } else {
      LOG(INFO) << "Listening on unix socket " << unix_sock;
      listeners.push_back(uds_listener.release());
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
    auto admin_listener = MakeListener(Protocol::REDIS, &service, Listener::Role::PRIVILEGED);

    error_code ec = acceptor->AddListener(interface_addr, admin_port, admin_listener.get());

    if (ec) {
      LOG(ERROR) << "Failed to open " << printable_addr << ", error: " << ec.message();
    } else {
      LOG(INFO) << "Listening on " << printable_addr;
      listeners.push_back(admin_listener.release());
    }
  }

  if (main_listener) {
    error_code ec = acceptor->AddListener(bind_addr, port, main_listener);

    if (ec) {
      LOG(ERROR) << "Could not open port " << port << ", error: " << ec.message();
      exit(1);
    }

    if (port == 0) {
      absl::SetFlag(&FLAGS_port, main_listener->socket()->LocalEndpoint().port());
    }
  }

  if (mc_port > 0 && !tcp_disabled) {
    auto listener = MakeListener(Protocol::MEMCACHE, &service);
    acceptor->AddListener(mc_port, listener.get());
    listeners.push_back(listener.release());
  }

  service.Init(acceptor, listeners);

  VersionMonitor version_monitor;

  // check if it's a production release tag.
  if (GetFlag(FLAGS_version_check) && kGitTag[0] == 'v' && strchr(kGitTag, '-') == nullptr) {
    version_monitor.Run(pool);
  }

  // Start the acceptor loop and wait for the server to shutdown.
  acceptor->Run();
  google::FlushLogFiles(google::INFO);  // Flush the header.

  acceptor->Wait();

  version_monitor.Shutdown();
  service.Shutdown();
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

#ifdef __linux__
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

void GetCGroupPath(string* memory_path, string* cpu_path) {
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
    if (pos == string::npos) {
      LOG(ERROR) << "Failed to parse cgroupv2 format, got: " << cgv;
      exit(1);
    }

    auto cgroup = string_view(cgv.c_str() + pos + 1);
    string_view cgroup_stripped = absl::StripTrailingAsciiWhitespace(cgroup);

    *memory_path = absl::StrCat("/sys/fs/cgroup/", cgroup_stripped);
    *cpu_path = *memory_path;  // in v2 the path to the cgroup is singular
  } else {
    for (const auto& sv : groups) {
      // in v1 the format is
      // N:s1:2 where N is an integer, s1, s2 strings with s1 maybe empty.
      vector<string_view> entry = absl::StrSplit(sv, ':');
      if (entry.size() != 3u) {
        LOG(ERROR) << "Unsupported group " << sv;
        continue;
      }

      // in v1 there are several 'canonical' cgroups
      // we are interested in the 'memory' and the 'cpu,cpuacct' ones
      // which specify memory and cpu limits, respectively.
      if (entry[1] == "memory")
        *memory_path = absl::StrCat("/sys/fs/cgroup/memory/", entry[2]);

      if (entry[1] == "cpu,cpuacct")
        *cpu_path = absl::StrCat("/sys/fs/cgroup/cpu,cpuacct/", entry[2]);
    }
  }
}

// returns true on success.
bool UpdateResourceLimitsIfInsideContainer(io::MemInfoData* mdata, size_t* max_threads) {
  using absl::StrCat;

  // did we succeed in reading *something*? if not, exit.
  // note that all processes in Linux are in some cgroup, so at the very
  // least we should read something.
  bool read_something = false;

  auto read_mem = [&read_something](string_view path, size_t* output) {
    auto file = io::ReadFileToString(path);
    DVLOG(1) << "container limits: read " << path << ": " << file.value_or("N/A");

    size_t temp = numeric_limits<size_t>::max();

    if (file.has_value()) {
      if (!absl::StartsWith(*file, "max"))
        CHECK(absl::SimpleAtoi(*file, &temp))
            << "Failed in parsing cgroup limits, path: " << path << " (read: " << *file << ")";
      read_something = true;
    }

    *output = min(*output, temp);
  };

  string mem_path, cpu_path;
  GetCGroupPath(&mem_path, &cpu_path);

  if (mem_path.empty() || cpu_path.empty()) {
    return true;  // not a container
  }

  VLOG(1) << "mem_path = " << mem_path;
  VLOG(1) << "cpu_path = " << cpu_path;

  /* Update memory limits */

  // Start by reading global memory limits
  auto parse_limits = [&](std::string_view base_mem) {
    read_mem(StrCat(base_mem, "/memory.limit_in_bytes"), &mdata->mem_total);
    read_mem(StrCat(base_mem, "/memory.max"), &mdata->mem_total);
  };

  // For v1
  constexpr auto base_mem_v1 = "/sys/fs/cgroup/memory"sv;
  parse_limits(base_mem_v1);
  // For v2 if the previous failed
  constexpr auto base_mem_v2 = "/sys/fs/cgroup"sv;
  parse_limits(base_mem_v2);
  // For v2 under /user.slice
  constexpr auto base_mem_v2_slice = "/sys/fs/cgroup/user.slice"sv;
  parse_limits(base_mem_v2_slice);

  // Read cgroup-specific limits
  read_mem(StrCat(mem_path, "/memory.limit_in_bytes"), &mdata->mem_total);
  read_mem(StrCat(mem_path, "/memory.max"), &mdata->mem_total);
  read_mem(StrCat(mem_path, "/memory.high"), &mdata->mem_avail);
  mdata->mem_avail = min(mdata->mem_avail, mdata->mem_total);

  /* Update thread limits */

  auto read_cpu = [&read_something](string_view path, size_t* output) {
    double count{0}, timeshare{1};

    /**
     * Summarized: the function does one of the following:
     *
     * 1. read path/cpu.max -- for v2. The format of this file is:
     *  $COUNT $PERIOD
     * which indicates that we can use upto $COUNT shares in a $PERIOD of time.
     * If $COUNT is max, then we can use as much CPU as the system has. Otherwise,
     * this translates to $COUNT/$PERIOD threads.
     *
     * 2. read path/cpu.cfs_quota_us & path/cpu.cfs_period_us -- same idea, but for v1.
     */

    if (auto cpu = ReadFileToString(StrCat(path, "/cpu.max")); cpu.has_value()) {
      vector<string_view> res = absl::StrSplit(*cpu, ' ');

      // Some linux distributions do not have anything there.
      if (res.size() == 2u) {
        if (res[0] == "max")
          *output = 0u;
        else {
          CHECK(absl::SimpleAtod(res[0], &count))
              << "Failed in parsing cgroupv2 cpu count, path = " << path << " (read: " << *cpu
              << ")";
          CHECK(absl::SimpleAtod(res[1], &timeshare))
              << "Failed in parsing cgroupv2 cpu timeshare, path = " << path << " (read: " << *cpu
              << ")";

          *output = static_cast<size_t>(ceil(count / timeshare));
        }

        read_something = true;
      }
    } else if (auto quota = ReadFileToString(StrCat(path, "/cpu.cfs_quota_us"));
               quota.has_value()) {
      auto period = ReadFileToString(StrCat(path, "/cpu.cfs_period_us"));

      CHECK(period.has_value()) << "Failed to read cgroup cpu.cfs_period_us, but read "
                                   "cpu.cfs_quota_us (this shouldn't happen!)";

      CHECK(absl::SimpleAtod(quota.value(), &count))
          << "Failed in parsing cgroupv1 cpu timeshare, quota = " << path << " (read: " << *quota
          << ")";

      if (count == -1)  // on -1 there is no limit.
        count = 0;

      CHECK(absl::SimpleAtod(period.value(), &timeshare))
          << "Failed in parsing cgroupv1 cpu timeshare, path = " << path << " (read: " << *period
          << ")";

      *output = static_cast<size_t>(count / timeshare);
      read_something = true;
    }
  };

  constexpr auto base_cpu = "/sys/fs/cgroup/cpu"sv;
  read_cpu(base_cpu, max_threads);  // global cpu limits
  constexpr auto base_cpu_v2 = "/sys/fs/cgroup"sv;
  read_cpu(base_cpu_v2, max_threads);  // global cpu limits
  constexpr auto base_cpu_v2_slice = "/sys/fs/cgroup/user.slice"sv;
  read_cpu(base_cpu_v2_slice, max_threads);  // global cpu limits
  read_cpu(cpu_path, max_threads);           // cgroup-specific limits

  if (!read_something) {
    LOG(ERROR) << "Failed in deducing any cgroup limits with paths " << mem_path << " and "
               << cpu_path;
    return false;
  }
  return true;
}

#endif

void SetupAllocationTracker(ProactorPool* pool) {
#ifdef DFLY_ENABLE_MEMORY_TRACKING
  string flag = absl::GetFlag(FLAGS_allocation_tracker);
  vector<pair<size_t, size_t>> track_ranges;
  for (string_view entry : absl::StrSplit(flag, ",", absl::SkipEmpty())) {
    auto separator = entry.find(":");
    if (separator == entry.npos) {
      LOG(ERROR) << "Can't find ':' in element";
      exit(-1);
    }

    pair<size_t, size_t> p;
    if (!absl::SimpleAtoi(entry.substr(0, separator), &p.first)) {
      LOG(ERROR) << "Can't parse first number in pair";
      exit(-1);
    }
    if (!absl::SimpleAtoi(entry.substr(separator + 1), &p.second)) {
      LOG(ERROR) << "Can't parse second number in pair";
      exit(-1);
    }

    track_ranges.push_back(p);
  }

  pool->AwaitBrief([&](unsigned, ProactorBase*) {
    for (auto range : track_ranges) {
      if (!AllocationTracker::Get().Add(
              {.lower_bound = range.first, .upper_bound = range.second, .sample_odds = 1.0})) {
        LOG(ERROR) << "Unable to track allocation range";
        exit(-1);
      }
    }
  });
#endif
}

void RegisterBufRings(ProactorPool* pool) {
#ifdef __linux__
  auto bufcnt = absl::GetFlag(FLAGS_uring_recv_buffer_cnt);
  if (bufcnt == 0) {
    return;
  }

  if (dfly::kernel_version < 602 || pool->at(0)->GetKind() != ProactorBase::IOURING) {
    LOG(WARNING) << "uring_recv_buffer_cnt is only supported on kernels >= 6.2 and with "
                    "io_uring proactor";
    return;
  }

  // We need a power of 2 length.
  bufcnt = absl::bit_ceil(bufcnt);
  pool->AwaitBrief([&](unsigned, ProactorBase* pb) {
    auto up = static_cast<fb2::UringProactor*>(pb);
    int res = up->RegisterBufferRing(facade::kRecvSockGid, bufcnt, facade::kRecvBufSize);
    if (res != 0) {
      LOG(ERROR) << "Failed to register buf ring for proactor "
                 << util::detail::SafeErrorMessage(res);
      exit(1);
    }
  });
  LOG(INFO) << "Registered a bufring with " << bufcnt << " buffers of size " << facade::kRecvBufSize
            << " per thread ";
#endif
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

void PrintBasicUsageInfo() {
  std::cout << "* Logs will be written to the first available of the following paths:\n";
  for (const auto& dir : google::GetLoggingDirectories()) {
    const string_view maybe_slash = absl::EndsWith(dir, "/") ? "" : "/";
    std::cout << dir << maybe_slash << "dragonfly.*\n";
  }
  std::cout << "* For the available flags type dragonfly [--help | --helpfull]\n";
  std::cout << "* Documentation can be found at: https://www.dragonflydb.io/docs";
  std::cout << endl;
}

void ParseFlagsFromEnv() {
  if (getenv("DFLY_PASSWORD")) {
    LOG(FATAL) << "DFLY_PASSWORD environment variable was deprecated in favor of DFLY_requirepass";
  }

  // Allowed environment variable names that can have
  // DFLY_ prefix, but don't necessarily have an ABSL flag created
  absl::flat_hash_set<std::string_view> ignored_environment_flag_names = {"DEV_ENV", "PASSWORD"};
  const auto& flags = absl::GetAllFlags();
  for (char** env = environ; *env != nullptr; env++) {
    constexpr string_view kPrefix = "DFLY_";
    string_view environ_var = *env;
    if (absl::StartsWith(environ_var, kPrefix)) {
      // Per 'man environ', environment variables are included with their values
      // in the format "name=value". Need to strip them apart, in order to work with flags object
      pair<string_view, string_view> environ_pair =
          absl::StrSplit(absl::StripPrefix(environ_var, kPrefix), absl::MaxSplits('=', 1));
      const auto& [flag_name, flag_value] = environ_pair;
      if (ignored_environment_flag_names.contains(flag_name)) {
        continue;
      }
      auto entry = flags.find(flag_name);
      if (entry != flags.end()) {
        if (absl::flags_internal::WasPresentOnCommandLine(flag_name)) {
          continue;
        }
        string error;
        auto& flag = entry->second;
        bool success = flag->ParseFrom(flag_value, &error);
        if (!success) {
          LOG(FATAL) << "could not parse flag " << flag->Name()
                     << " from environment variable. Error: " << error;
        }
      } else {
        LOG(FATAL) << "unknown environment variable DFLY_" << flag_name;
      }
    }
  }
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
  google::InitGoogleLogging(argv[0]);
  google::SetLogFilenameExtension(".log");

  MainInitGuard guard(&argc, &argv);

  ParseFlagsFromEnv();

  PrintBasicUsageInfo();
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

  io::MemInfoData mem_info = ReadMemInfo().value_or(io::MemInfoData{});
  size_t max_available_threads = 0u;

#ifdef __linux__
  UpdateResourceLimitsIfInsideContainer(&mem_info, &max_available_threads);
#endif

  if (mem_info.swap_total != 0)
    LOG(WARNING) << "SWAP is enabled. Consider disabling it when running Dragonfly.";

  dfly::max_memory_limit = dfly::GetMaxMemoryFlag();

  if (dfly::max_memory_limit == 0) {
    LOG(INFO) << "maxmemory has not been specified. Deciding myself....";

    size_t available = mem_info.mem_avail;
    size_t maxmemory = size_t(0.8 * available);
    if (maxmemory == 0) {
      LOG(ERROR) << "Could not deduce how much memory available. "
                 << "Use --maxmemory=... to specify explicitly";
      return 1;
    }
    LOG(INFO) << "Found " << HumanReadableNumBytes(available)
              << " available memory. Setting maxmemory to " << HumanReadableNumBytes(maxmemory);

    SetMaxMemoryFlag(maxmemory);
    dfly::max_memory_limit = maxmemory;
  } else {
    string hr_limit = HumanReadableNumBytes(dfly::max_memory_limit);
    if (dfly::max_memory_limit > mem_info.mem_avail)
      LOG(WARNING) << "Got memory limit " << hr_limit << ", however only "
                   << HumanReadableNumBytes(mem_info.mem_avail) << " was found.";
    LOG(INFO) << "Max memory limit is: " << hr_limit;
  }

  // Initialize mi_malloc options
  // export MIMALLOC_VERBOSE=1 to see the options before the override.
  mi_option_enable(mi_option_show_errors);
  mi_option_set(mi_option_max_warnings, 0);
  mi_option_enable(mi_option_purge_decommits);

  fb2::SetDefaultStackResource(&fb2::std_malloc_resource, kFiberDefaultStackSize);

  {
    unique_ptr<util::ProactorPool> pool;

#ifdef __linux__
    base::sys::KernelVersion kver;
    base::sys::GetKernelVersion(&kver);

    CHECK_LT(kver.major, 99u);
    dfly::kernel_version = kver.kernel * 100 + kver.major;

    bool use_epoll = ShouldUseEpollAPI(kver);

    if (use_epoll) {
      pool.reset(fb2::Pool::Epoll(max_available_threads));
    } else {
      pool.reset(fb2::Pool::IOUring(1024, max_available_threads));  // 1024 - iouring queue size.
    }
#else
    pool.reset(fb2::Pool::Epoll(max_available_threads));
#endif

    pool->Run();

    SetupAllocationTracker(pool.get());
    RegisterBufRings(pool.get());

    AcceptServer acceptor(pool.get(), &fb2::std_malloc_resource, true);
    acceptor.set_back_log(absl::GetFlag(FLAGS_tcp_backlog));

    dfly::RunEngine(pool.get(), &acceptor);

    pool->Stop();

    if (!pidfile_path.empty()) {
      unlink(pidfile_path.c_str());
    }
  }

  return 0;
}
