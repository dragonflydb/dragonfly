// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/init.h"
#include "server/main_service.h"
#include "server/dragonfly_listener.h"
#include "util/accept_server.h"
#include "util/uring/uring_pool.h"
#include "util/varz.h"

DEFINE_int32(http_port, 8080, "Http port.");
DECLARE_uint32(port);
DECLARE_uint32(memcache_port);

using namespace util;

namespace dfly {

void RunEngine(ProactorPool* pool, AcceptServer* acceptor, HttpListener<>* http) {
  Service service(pool);
  service.Init(acceptor);

  if (http) {
    service.RegisterHttp(http);
  }

  acceptor->AddListener(FLAGS_port, new Listener{Protocol::REDIS, &service});
  if (FLAGS_memcache_port > 0) {
    acceptor->AddListener(FLAGS_memcache_port, new Listener{Protocol::MEMCACHE, &service});
  }

  acceptor->Run();
  acceptor->Wait();

  service.Shutdown();
}

}  // namespace dfly


int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(FLAGS_port, 0u);

  uring::UringPool pp{1024};
  pp.Run();

  AcceptServer acceptor(&pp);
  HttpListener<>* http_listener = nullptr;

  if (FLAGS_http_port >= 0) {
    http_listener = new HttpListener<>;

    // Ownership over http_listener is moved to the acceptor.
    uint16_t port = acceptor.AddListener(FLAGS_http_port, http_listener);

    LOG(INFO) << "Started http service on port " << port;
  }

  dfly::RunEngine(&pp, &acceptor, http_listener);

  pp.Stop();

  return 0;
}
