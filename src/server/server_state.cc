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
#include "facade/dragonfly_connection.h"
#include "server/journal/journal.h"
#include "util/listener_interface.h"

ABSL_FLAG(uint32_t, interpreter_per_thread, 10, "Lua interpreters per thread");
ABSL_FLAG(uint32_t, timeout, 0,
          "Close the connection after it is idle for N seconds (0 to disable)");

namespace dfly {

using namespace std;
using namespace std::chrono_literals;

__thread ServerState* ServerState::state_ = nullptr;

ServerState::Stats::Stats(unsigned num_shards) : tx_width_freq_arr(num_shards) {
}

ServerState::Stats& ServerState::Stats::Add(const ServerState::Stats& other) {
  static_assert(sizeof(Stats) == 20 * 8, "Stats size mismatch");

#define ADD(x) this->x += (other.x)

  ADD(eval_io_coordination_cnt);

  ADD(eval_shardlocal_coordination_cnt);
  ADD(eval_squashed_flushes);

  ADD(tx_global_cnt);
  ADD(tx_normal_cnt);
  ADD(tx_inline_runs);
  ADD(tx_schedule_cancel_cnt);

  ADD(multi_squash_executions);
  ADD(multi_squash_exec_hop_usec);
  ADD(multi_squash_exec_reply_usec);
  ADD(squashed_commands);

  ADD(blocked_on_interpreter);
  ADD(rdb_save_usec);
  ADD(rdb_save_count);

  ADD(big_value_preemptions);
  ADD(compressed_blobs);

  ADD(oom_error_cmd_cnt);
  ADD(conn_timeout_events);
  if (this->tx_width_freq_arr.size() > 0) {
    DCHECK_EQ(this->tx_width_freq_arr.size(), other.tx_width_freq_arr.size());
    this->tx_width_freq_arr += other.tx_width_freq_arr;
  } else {
    this->tx_width_freq_arr = other.tx_width_freq_arr;
  }
  return *this;
#undef ADD
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
  watcher_fiber_.JoinIfNeeded();
}

void ServerState::Init(uint32_t thread_index, uint32_t num_shards,
                       util::ListenerInterface* main_listener, acl::UserRegistry* registry) {
  state_ = new ServerState();
  state_->gstate_ = GlobalState::ACTIVE;
  state_->thread_index_ = thread_index;
  state_->user_registry = registry;
  state_->stats = Stats(num_shards);
  if (main_listener) {
    state_->watcher_fiber_ = util::fb2::Fiber(
        util::fb2::Launch::post, "ConnectionsWatcher",
        [state = state_, main_listener] { state->ConnectionsWatcherFb(main_listener); });
  }
}

void ServerState::Destroy() {
  delete state_;
  state_ = nullptr;
}

void ServerState::EnterLameDuck() {
  gstate_ = GlobalState::SHUTTING_DOWN;
  watcher_cv_.notify_all();
}

ServerState::MemoryUsageStats ServerState::GetMemoryUsage(uint64_t now_ns) {
  static constexpr uint64_t kCacheEveryNs = 1000;
  if (now_ns > used_mem_last_update_ + kCacheEveryNs) {
    used_mem_last_update_ = now_ns;
    memory_stats_cached_.used_mem = used_mem_current.load(std::memory_order_relaxed);
    memory_stats_cached_.rss_mem = rss_mem_current.load(std::memory_order_relaxed);
  }
  return memory_stats_cached_;
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

void ServerState::DecommitMemory(uint8_t flags) {
  if (flags & kDataHeap) {
    mi_heap_collect(data_heap(), true);
  }
  if (flags & kBackingHeap) {
    mi_heap_collect(mi_heap_get_backing(), true);
  }

  if (flags & kGlibcmalloc) {
    // trims the memory (reduces RSS usage) from the malloc allocator. Does not present in
    // MUSL lib.
#ifdef __GLIBC__
// There is an issue with malloc_trim and sanitizers because the asan replace malloc but is not
// aware of malloc_trim which causes malloc_trim to segfault because it's not initialized properly
#ifndef SANITIZERS
    malloc_trim(0);
#endif
#endif
  }
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

void ServerState::AlterInterpreters(std::function<void(Interpreter*)> modf) {
  interpreter_mgr_.Alter(std::move(modf));
}

ServerState* ServerState::SafeTLocal() {
  // https://stackoverflow.com/a/75622732
  asm volatile("");
  return state_;
}

bool ServerState::ShouldLogSlowCmd(unsigned latency_usec) const {
  return slow_log_shard_.IsEnabled() && latency_usec >= log_slower_than_usec;
}

void ServerState::ConnectionsWatcherFb(util::ListenerInterface* main) {
  optional<facade::Connection::WeakRef> last_reference;

  while (true) {
    util::fb2::NoOpLock noop;
    if (watcher_cv_.wait_for(noop, 1s, [this] { return gstate_ == GlobalState::SHUTTING_DOWN; })) {
      break;
    }

    uint32_t timeout = absl::GetFlag(FLAGS_timeout);
    if (timeout == 0) {
      continue;
    }

    facade::Connection* from = nullptr;
    if (last_reference && !last_reference->IsExpired()) {
      from = last_reference->Get();
    }

    // We use weak refs, because ShutdownSelf below can potentially block the fiber,
    // and during this time some of the connections might be destroyed. Weak refs allow checking
    // validity of each connection.
    vector<facade::Connection::WeakRef> conn_refs;

    auto cb = [&](unsigned thread_index, util::Connection* conn) {
      facade::Connection* dfly_conn = static_cast<facade::Connection*>(conn);
      using Phase = facade::Connection::Phase;
      auto phase = dfly_conn->phase();
      bool is_replica = true;
      if (dfly_conn->cntx()) {
        is_replica = dfly_conn->cntx()->replica_conn;
      }

      if ((phase == Phase::READ_SOCKET || dfly_conn->IsSending()) &&
          !is_replica && dfly_conn->idle_time() > timeout) {
        conn_refs.push_back(dfly_conn->Borrow());
      }
    };

    util::Connection* next = main->TraverseConnectionsOnThread(cb, 100, from);
    if (next) {
      last_reference = static_cast<facade::Connection*>(next)->Borrow();
    } else {
      last_reference.reset();
    }

    for (auto& ref : conn_refs) {
      facade::Connection* conn = ref.Get();
      if (conn) {
        VLOG(1) << "Closing connection due to timeout: " << conn->GetClientInfo();
        conn->ShutdownSelf();
        stats.conn_timeout_events++;
      }
    }
  }
}

}  // end of namespace dfly
