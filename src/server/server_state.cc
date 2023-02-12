// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_state.h"

#include <mimalloc.h>

extern "C" {
#include "redis/zmalloc.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "facade/conn_context.h"

ABSL_FLAG(uint32_t, interpreter_per_thread, 10, "Lua interpreters per thread");

namespace dfly {

thread_local ServerState ServerState::state_;

void MonitorsRepo::Add(facade::Connection* connection) {
  VLOG(1) << "register connection "
          << " at address 0x" << std::hex << (const void*)connection << " for thread "
          << util::ProactorBase::GetIndex();

  monitors_.push_back(connection);
}

void MonitorsRepo::Remove(const facade::Connection* conn) {
  auto it = std::find_if(monitors_.begin(), monitors_.end(),
                         [&conn](const auto& val) { return val == conn; });
  if (it != monitors_.end()) {
    VLOG(1) << "removing connection 0x" << std::hex << conn << " releasing token";
    monitors_.erase(it);
  } else {
    VLOG(1) << "no connection 0x" << std::hex << conn << " found in the registered list here";
  }
}

void MonitorsRepo::NotifyChangeCount(bool added) {
  if (added) {
    ++global_count_;
  } else {
    DCHECK(global_count_ > 0);
    --global_count_;
  }
}

ServerState::ServerState() : interpreter_mgr_{absl::GetFlag(FLAGS_interpreter_per_thread)} {
  CHECK(mi_heap_get_backing() == mi_heap_get_default());

  mi_heap_t* tlh = mi_heap_new();
  init_zmalloc_threadlocal(tlh);
  data_heap_ = tlh;
}

ServerState::~ServerState() {
}

void ServerState::Init() {
  gstate_ = GlobalState::ACTIVE;
}

void ServerState::Shutdown() {
  gstate_ = GlobalState::SHUTTING_DOWN;
}

Interpreter* ServerState::BorrowInterpreter() {
  return interpreter_mgr_.Get();
}

void ServerState::ReturnInterpreter(Interpreter* ir) {
  interpreter_mgr_.Return(ir);
}

}  // end of namespace dfly
