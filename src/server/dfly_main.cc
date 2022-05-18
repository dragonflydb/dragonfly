// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifdef NDEBUG
#include <mimalloc-new-delete.h>
#endif

#include <mimalloc.h>

#include "base/init.h"
#include "base/proc_util.h"  // for GetKernelVersion
#include "facade/dragonfly_listener.h"
#include "io/proc_reader.h"
#include "server/main_service.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/uring/uring_pool.h"
#include "util/varz.h"


DECLARE_uint32(port);
DECLARE_uint32(memcache_port);
DECLARE_uint64(maxmemory);
DEFINE_bool(use_large_pages, false, "If true - uses large memory pages for allocations");
DEFINE_string(bind, "",
              "Bind address. If empty - binds on all interfaces. "
              "It's not advised due to security implications.");

using namespace util;
using namespace std;
using namespace facade;
using strings::HumanReadableNumBytes;

namespace dfly {

bool RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  if (FLAGS_maxmemory > 0 && FLAGS_maxmemory < pool->size() * 256_MB) {
    LOG(ERROR) << "Max memory is less than 256MB per thread. Exiting...";
    return false;
  }

  Service service(pool);

  Listener* main_listener = new Listener{Protocol::REDIS, &service};

  Service::InitOpts opts;
  opts.disable_time_update = false;
  service.Init(acceptor, main_listener, opts);

  const char* bind_addr = FLAGS_bind.empty() ? nullptr : FLAGS_bind.c_str();

  error_code ec = acceptor->AddListener(bind_addr, FLAGS_port, main_listener);

  LOG_IF(FATAL, ec) << "Cound not open port " << FLAGS_port << ", error: " << ec.message();

  if (FLAGS_memcache_port > 0) {
    acceptor->AddListener(FLAGS_memcache_port, new Listener{Protocol::MEMCACHE, &service});
  }

  acceptor->Run();
  acceptor->Wait();

  service.Shutdown();
  return true;
}

}  // namespace dfly

extern "C" void _mi_options_init();

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("dragonfly [FLAGS]");
  gflags::SetVersionString("v0.2");

  MainInitGuard guard(&argc, &argv);

  CHECK_GT(FLAGS_port, 0u);
  mi_stats_reset();

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  if (kver.major < 5 || (kver.major == 5 && kver.minor < 11)) {
    LOG(ERROR) << "Kernel 5.11 or later is supported. Exiting...";
    return 1;
  }
  CHECK_LT(kver.minor, 99u);
  dfly::kernel_version = kver.major * 100 + kver.minor;

  if (FLAGS_maxmemory == 0) {
    LOG(INFO) << "maxmemory has not been specified. Deciding myself....";

    io::Result<io::MemInfoData> res = io::ReadMemInfo();
    size_t available = res->mem_avail;
    size_t maxmemory = size_t(0.8 * available);
    LOG(INFO) << "Found " << HumanReadableNumBytes(available)
              << " available memory. Setting maxmemory to " << HumanReadableNumBytes(maxmemory);
    FLAGS_maxmemory = maxmemory;
  } else {
    LOG(INFO) << "Max memory limit is: " << HumanReadableNumBytes(FLAGS_maxmemory);
  }

  dfly::max_memory_limit = FLAGS_maxmemory;

  if (FLAGS_use_large_pages) {
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
