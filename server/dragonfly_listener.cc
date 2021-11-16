// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/dragonfly_listener.h"

#include "base/logging.h"
#include "server/dragonfly_connection.h"
#include "util/proactor_pool.h"

using namespace util;

DEFINE_uint32(conn_threads, 0, "Number of threads used for handing server connections");


namespace dfly {

Listener::Listener(Service* e) : engine_(e) {
}

Listener::~Listener() {
}

util::Connection* Listener::NewConnection(ProactorBase* proactor) {
  return new Connection{engine_};
}

void Listener::PreShutdown() {
}

void Listener::PostShutdown() {
}

// We can limit number of threads handling dragonfly connections.
ProactorBase* Listener::PickConnectionProactor(LinuxSocketBase* sock) {
  uint32_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
  uint32_t total = FLAGS_conn_threads;
  util::ProactorPool* pp = pool();

  if (total == 0 || total > pp->size()) {
    total = pp->size();
  }

  return pp->at(id % total);
}

}  // namespace dfly
