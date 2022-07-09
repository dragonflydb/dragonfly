// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifdef NDEBUG
#include <mimalloc-new-delete.h>
#endif

#include <liburing.h>
#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <mimalloc.h>

#include "base/init.h"
#include "base/proc_util.h"  // for GetKernelVersion
#include "facade/dragonfly_listener.h"
#include "io/proc_reader.h"
#include "server/main_service.h"
#include "server/version.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/uring/uring_pool.h"
#include "util/varz.h"

using namespace std;

ABSL_DECLARE_FLAG(uint32_t, port);
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(uint32_t, memcache_port);
ABSL_DECLARE_FLAG(uint64_t, maxmemory);

ABSL_FLAG(bool, use_large_pages, false, "If true - uses large memory pages for allocations");
ABSL_FLAG(string, bind, "",
          "Bind address. If empty - binds on all interfaces. "
          "It's not advised due to security implications.");

using namespace util;
using namespace facade;
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
  if (absl::ConsumePrefix(&path, "../src/"))
    return ColoredStr(TermColor::kGreen, path);

  if (absl::ConsumePrefix(&path, "../"))
    return ColoredStr(TermColor::kYellow, path);

  if (absl::ConsumePrefix(&path, "_deps/"))
    return string(path);

  return string(path);
}

bool RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  auto maxmemory = GetFlag(FLAGS_maxmemory);

  if (maxmemory > 0 && maxmemory < pool->size() * 256_MB) {
    LOG(ERROR) << "Max memory is less than 256MB per thread. Exiting...";
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

  error_code ec = acceptor->AddListener(bind_addr, port, main_listener);

  LOG_IF(FATAL, ec) << "Cound not open port " << port << ", error: " << ec.message();

  if (mc_port > 0) {
    acceptor->AddListener(mc_port, new Listener{Protocol::MEMCACHE, &service});
  }

  acceptor->Run();
  acceptor->Wait();

  service.Shutdown();
  return true;
}

}  // namespace
}  // namespace dfly

extern "C" void _mi_options_init();

using namespace dfly;

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
    return StrCat("dragonfly ", ColoredStr(TermColor::kGreen, dfly::kGitTag),
                  "\nbuild time: ", dfly::kBuildTime, "\n");
  };

  absl::SetFlagsUsageConfig(config);

  MainInitGuard guard(&argc, &argv);

  CHECK_GT(GetFlag(FLAGS_port), 0u);
  mi_stats_reset();

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  if (kver.kernel < 5 || (kver.kernel == 5 && kver.major < 10)) {
    LOG(ERROR) << "Kernel 5.10 or later is supported. Exiting...";
    return 1;
  }

  int iouring_res = io_uring_queue_init_params(0, nullptr, nullptr);
  if (-iouring_res == ENOSYS) {
    LOG(ERROR)
        << "iouring system call interface is not supported. Exiting...";
    return 1;
  }

  if (GetFlag(FLAGS_dbnum) > dfly::kMaxDbId) {
    LOG(ERROR) << "dbnum is too big. Exiting...";
    return 1;
  }

  CHECK_LT(kver.major, 99u);
  dfly::kernel_version = kver.kernel * 100 + kver.major;

  if (GetFlag(FLAGS_maxmemory) == 0) {
    LOG(INFO) << "maxmemory has not been specified. Deciding myself....";

    io::Result<io::MemInfoData> res = io::ReadMemInfo();
    size_t available = res->mem_avail;
    size_t maxmemory = size_t(0.8 * available);
    LOG(INFO) << "Found " << HumanReadableNumBytes(available)
              << " available memory. Setting maxmemory to " << HumanReadableNumBytes(maxmemory);
    absl::SetFlag(&FLAGS_maxmemory, maxmemory);
  } else {
    LOG(INFO) << "Max memory limit is: " << HumanReadableNumBytes(GetFlag(FLAGS_maxmemory));
  }

  dfly::max_memory_limit = GetFlag(FLAGS_maxmemory);

  if (GetFlag(FLAGS_use_large_pages)) {
    mi_option_enable(mi_option_large_os_pages);
  }
  mi_option_enable(mi_option_show_errors);
  mi_option_set(mi_option_max_warnings, 0);

  uring::UringPool pp{1024};
  pp.Run();

  AcceptServer acceptor(&pp);

  int res = dfly::RunEngine(&pp, &acceptor) ? 0 : -1;

  pp.Stop();

  return res;
}
