// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_state.h"

#include <mimalloc.h>

#include "server/acl/user_registry.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "facade/conn_context.h"
#include "server/journal/journal.h"

ABSL_FLAG(uint32_t, interpreter_per_thread, 10, "Lua interpreters per thread");

namespace dfly {

__thread ServerState* ServerState::state_ = nullptr;

ServerState::Stats& ServerState::Stats::operator+=(const ServerState::Stats& other) {
  this->ooo_tx_cnt += other.ooo_tx_cnt;
  this->eval_io_coordination_cnt += other.eval_io_coordination_cnt;
  this->eval_shardlocal_coordination_cnt += other.eval_shardlocal_coordination_cnt;
  this->eval_squashed_flushes += other.eval_squashed_flushes;
  this->tx_schedule_cancel_cnt += other.tx_schedule_cancel_cnt;

  static_assert(sizeof(Stats) == 5 * 8);
  return *this;
}

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

void ServerState::Init(uint32_t thread_index, acl::UserRegistry* registry) {
  state_ = new ServerState();
  state_->gstate_ = GlobalState::ACTIVE;
  state_->thread_index_ = thread_index;
  state_->user_registry = registry;
}

void ServerState::Destroy() {
  delete state_;
  state_ = nullptr;
}

uint64_t ServerState::GetUsedMemory(uint64_t now_ns) {
  static constexpr uint64_t kCacheEveryNs = 1000;
  if (now_ns > used_mem_last_update_ + kCacheEveryNs) {
    used_mem_last_update_ = now_ns;
    used_mem_cached_ = used_mem_current.load(std::memory_order_relaxed);
  }
  return used_mem_cached_;
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

void ServerState::SetPauseState(ClientPause state, bool start) {
  client_pauses_[int(state)] += (start ? 1 : -1);
  if (!client_pauses_[int(state)]) {
    client_pause_ec_.notifyAll();
  }
}

bool ServerState::IsPaused() const {
  return client_pauses_[0] || client_pauses_[1];
}

void ServerState::AwaitPauseState(bool is_write) {
  client_pause_ec_.await([is_write, this]() {
    if (client_pauses_[int(ClientPause::ALL)]) {
      return false;
    }
    if (is_write && client_pauses_[int(ClientPause::WRITE)]) {
      return false;
    }
    return true;
  });
}

void ServerState::AwaitOnPauseDispatch() {
  pause_dispatch_ec_.await([this]() {
    if (pause_dispatch_) {
      return false;
    }
    return true;
  });
}

void ServerState::SetPauseDispatch(bool pause) {
  pause_dispatch_ = pause;
  if (!pause_dispatch_) {
    pause_dispatch_ec_.notifyAll();
  }
}

Interpreter* ServerState::BorrowInterpreter() {
  return interpreter_mgr_.Get();
}

void ServerState::ReturnInterpreter(Interpreter* ir) {
  interpreter_mgr_.Return(ir);
}

}  // end of namespace dfly
