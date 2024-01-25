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

ServerState::Stats::Stats() {
  tx_type_cnt.fill(0);
}

ServerState::Stats::~Stats() {
  delete[] tx_width_freq_arr;
}

auto ServerState::Stats::operator=(Stats&& other) -> Stats& {
  for (int i = 0; i < NUM_TX_TYPES; ++i) {
    this->tx_type_cnt[i] = other.tx_type_cnt[i];
  }
  this->eval_io_coordination_cnt = other.eval_io_coordination_cnt;
  this->eval_shardlocal_coordination_cnt = other.eval_shardlocal_coordination_cnt;
  this->eval_squashed_flushes = other.eval_squashed_flushes;
  this->tx_schedule_cancel_cnt = other.tx_schedule_cancel_cnt;

  delete[] this->tx_width_freq_arr;
  this->tx_width_freq_arr = other.tx_width_freq_arr;
  other.tx_width_freq_arr = nullptr;

  return *this;
}

ServerState::Stats& ServerState::Stats::Add(unsigned num_shards, const ServerState::Stats& other) {
  static_assert(sizeof(Stats) == 13 * 8, "Stats size mismatch");

  for (int i = 0; i < NUM_TX_TYPES; ++i) {
    this->tx_type_cnt[i] += other.tx_type_cnt[i];
  }

  this->eval_io_coordination_cnt += other.eval_io_coordination_cnt;
  this->eval_shardlocal_coordination_cnt += other.eval_shardlocal_coordination_cnt;
  this->eval_squashed_flushes += other.eval_squashed_flushes;
  this->tx_schedule_cancel_cnt += other.tx_schedule_cancel_cnt;

  this->multi_squash_executions += other.multi_squash_executions;
  this->multi_squash_exec_hop_usec += other.multi_squash_exec_hop_usec;
  this->multi_squash_exec_reply_usec += other.multi_squash_exec_reply_usec;

  this->blocked_on_interpreter += other.blocked_on_interpreter;

  if (this->tx_width_freq_arr == nullptr) {
    this->tx_width_freq_arr = new uint64_t[num_shards];
    std::copy_n(other.tx_width_freq_arr, num_shards, this->tx_width_freq_arr);
  } else {
    for (unsigned i = 0; i < num_shards; ++i) {
      this->tx_width_freq_arr[i] += other.tx_width_freq_arr[i];
    }
  }

  return *this;
}

void MonitorsRepo::Add(facade::Connection* connection) {
  VLOG(1) << "register connection "
          << " at address 0x" << std::hex << (const void*)connection << " for thread "
          << util::ProactorBase::me()->GetPoolIndex();

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

void ServerState::Init(uint32_t thread_index, uint32_t num_shards, acl::UserRegistry* registry) {
  state_ = new ServerState();
  state_->gstate_ = GlobalState::ACTIVE;
  state_->thread_index_ = thread_index;
  state_->user_registry = registry;
  state_->stats.tx_width_freq_arr = new uint64_t[num_shards];
  std::fill_n(state_->stats.tx_width_freq_arr, num_shards, 0);
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

void ServerState::AwaitPauseState(bool is_write) {
  client_pause_ec_.await([is_write, this]() {
    return client_pauses_[int(ClientPause::ALL)] == 0 &&
           (!is_write || client_pauses_[int(ClientPause::WRITE)] == 0);
  });
}

bool ServerState::IsPaused() const {
  return (client_pauses_[0] + client_pauses_[1]) > 0;
}

void ServerState::AwaitIfMigrationFinalization(uint16_t id) {
  if (migrated_slots_.find(id) != migrated_slots_.end())
    migration_finalization_ec_.await(
        [this, id] { return migrated_slots_.find(id) == migrated_slots_.end(); });
}

void ServerState::SetMigratedSlots(SlotSet slots) {
  migrated_slots_ = std::move(slots);
  migration_finalization_ec_.notifyAll();
}

bool ServerState::IsMigrationFinalization() const {
  return !migrated_slots_.empty();
}

Interpreter* ServerState::BorrowInterpreter() {
  stats.blocked_on_interpreter++;
  auto* ptr = interpreter_mgr_.Get();
  stats.blocked_on_interpreter--;
  return ptr;
}

void ServerState::ReturnInterpreter(Interpreter* ir) {
  interpreter_mgr_.Return(ir);
}

void ServerState::ResetInterpreter() {
  interpreter_mgr_.Reset();
}

ServerState* ServerState::SafeTLocal() {
  // https://stackoverflow.com/a/75622732
  asm volatile("");
  return state_;
}

bool ServerState::ShouldLogSlowCmd(unsigned latency_usec) const {
  return slow_log_shard_.IsEnabled() && latency_usec >= log_slower_than_usec;
}
}  // end of namespace dfly
