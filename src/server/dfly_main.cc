// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <mimalloc.h>

#include "base/init.h"
#include "base/proc_util.h"

#include "facade/dragonfly_listener.h"
#include "server/main_service.h"
#include "util/accept_server.h"
#include "util/uring/uring_pool.h"
#include "util/varz.h"

DECLARE_uint32(port);
DECLARE_uint32(memcache_port);
DECLARE_uint64(maxmemory);
DEFINE_bool(use_large_pages, false, "If true - uses large memory pages for allocations");

using namespace util;
using namespace std;
using namespace facade;

namespace dfly {

bool RunEngine(ProactorPool* pool, AcceptServer* acceptor) {

  if (FLAGS_maxmemory > 0 && FLAGS_maxmemory < pool->size() * 256_MB ) {
    LOG(ERROR) << "Max memory is less than 256MB per thread. Exiting...";
    return false;
  }

  Service service(pool);

  service.Init(acceptor);
  acceptor->AddListener(FLAGS_port, new Listener{Protocol::REDIS, &service});

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

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  if (kver.major < 5 || (kver.major == 5 && kver.minor < 11)) {
    LOG(ERROR) << "Kernel 5.11 or later is supported. Exiting...";
    return 1;
  }

  if (FLAGS_use_large_pages) {
    mi_option_enable(mi_option_large_os_pages);
  }
  mi_option_enable(mi_option_show_errors);
  mi_option_set(mi_option_max_warnings, 0);
  _mi_options_init();

  uring::UringPool pp{1024};
  pp.Run();

  AcceptServer acceptor(&pp);

  int res = dfly::RunEngine(&pp, &acceptor) ? 0 : -1;

  pp.Stop();

  return res;
}
