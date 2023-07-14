// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_state.h"

#include <mimalloc.h>

#include <algorithm>
#include <cstdint>
#include <ostream>

extern "C" {
#include "absl/flags/flag.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "glog/logging.h"
#include "redis/zmalloc.h"
#include "util/fibers/proactor_base.h"
}

#include "server/journal/journal.h"

ABSL_FLAG(uint32_t, interpreter_per_thread, 10, "Lua interpreters per thread");

namespace dfly {

__thread ServerState* ServerState::state_ = nullptr;

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

void ServerState::Init(uint32_t thread_index) {
  state_ = new ServerState();
  state_->gstate_ = GlobalState::ACTIVE;
  state_->thread_index_ = thread_index;
}

void ServerState::Destroy() {
  delete state_;
  state_ = nullptr;
}

bool ServerState::AllowInlineScheduling() const {
  // We can't allow inline scheduling during a full sync, because then journaling transactions
  // will be scheduled before RdbLoader::LoadItemsBuffer is finished. We can't use the regular
  // locking mechanism because RdbLoader is not using transactions.
  if (gstate_ == GlobalState::LOADING)
    return false;

  // Journal callbacks can preempt; This means we have to disallow inline scheduling
  // because then we might interleave the callbacks loop from an inlined-scheduled command
  // and a normally-scheduled command.
  // The problematic loop is in JournalSlice::AddLogRecord, going over all the callbacks.

  if (journal_ && journal_->HasRegisteredCallbacks())
    return false;

  return true;
}

Interpreter* ServerState::BorrowInterpreter() {
  return interpreter_mgr_.Get();
}

void ServerState::ReturnInterpreter(Interpreter* ir) {
  interpreter_mgr_.Return(ir);
}

}  // end of namespace dfly
