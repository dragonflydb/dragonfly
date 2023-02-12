// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <vector>

#include "base/histogram.h"
#include "core/interpreter.h"
#include "server/common.h"
#include "util/sliding_counter.h"

typedef struct mi_heap_s mi_heap_t;

namespace facade {
class Connection;
}

namespace dfly {

namespace journal {
class Journal;
}  // namespace journal

// This would be used as a thread local storage of sending
// monitor messages.
// Each thread will have its own list of all the connections that are
// used for monitoring. When a connection is set to monitor it would register
// itself to this list on all i/o threads. When a new command is dispatched,
// and this list is not empty, it would send in the same thread context as then
// thread that registered here the command.
// Note about performance: we are assuming that we would not have many connections
// that are registered here. This is not pub sub where it must be high performance
// and may support many to many with tens or more of connections. It is assumed that
// since monitoring is for debugging only, we would have less than 1 in most cases.
// Also note that we holding this list on the thread level since this is the context
// at which this would run. It also minimized the number of copied for this list.
class MonitorsRepo {
 public:
  using MonitorVec = std::vector<facade::Connection*>;

  // This function adds a new connection to be monitored. This function only add
  // new connection that belong to this thread! Must not be called outside of this
  // thread context
  void Add(facade::Connection* conn);

  // This function remove a connection what was monitored. This function only removes
  // a connection that belong to this thread! Must not be called outside of this
  // thread context
  void Remove(const facade::Connection* conn);

  // We have for each thread the total number of monitors in the application.
  // So this call is thread safe since we hold a copy of this for each thread.
  // If this return true, then we don't need to run the monitor operation at all.
  bool Empty() const {
    return global_count_ == 0u;
  }

  // This function is run on all threads to either increment or decrement the "shared" counter
  // of the monitors - it must be called as part of removing a monitor (for example
  // when a connection is closed).
  void NotifyChangeCount(bool added);

  std::size_t Size() const {
    return monitors_.size();
  }

  const MonitorVec& monitors() const {
    return monitors_;
  }

 private:
  MonitorVec monitors_;            // save connections belonging to this thread only!
  unsigned int global_count_ = 0;  // by global its means that we count the monitor for all threads
};

// Present in every server thread. This class differs from EngineShard. The latter manages
// state around engine shards while the former represents coordinator/connection state.
// There may be threads that handle engine shards but not IO, there may be threads that handle IO
// but not engine shards and there can be threads that handle both.
// Instances of ServerState are present only for threads that handle
// IO and manage incoming connections.
class ServerState {  // public struct - to allow initialization.
  ServerState(const ServerState&) = delete;
  void operator=(const ServerState&) = delete;

 public:
  struct Stats {
    uint64_t ooo_tx_cnt = 0;
  };

  static ServerState* tlocal() {
    return &state_;
  }

  static facade::ConnectionStats* tl_connection_stats() {
    return &state_.connection_stats;
  }

  ServerState();
  ~ServerState();

  void Init();
  void Shutdown();

  bool is_master = true;

  facade::ConnectionStats connection_stats;

  void TxCountInc() {
    ++live_transactions_;
  }

  void TxCountDec() {
    --live_transactions_;  // can go negative since we can start on one thread and end on another.
  }

  int64_t live_transactions() const {
    return live_transactions_;
  }

  GlobalState gstate() const {
    return gstate_;
  }

  void set_gstate(GlobalState s) {
    gstate_ = s;
  }

  // Borrow interpreter from internal manager. Return int with ReturnInterpreter.
  Interpreter* BorrowInterpreter();

  // Return interpreter to internal manager to be re-used.
  void ReturnInterpreter(Interpreter*);

  // Returns sum of all requests in the last 6 seconds
  // (not including the current one).
  uint32_t MovingSum6() const {
    return qps_.SumTail();
  }

  void RecordCmd() {
    ++connection_stats.command_cnt;
    qps_.Inc();
  }

  // data heap used by zmalloc and shards.
  mi_heap_t* data_heap() {
    return data_heap_;
  }

  journal::Journal* journal() {
    return journal_;
  }

  void set_journal(journal::Journal* j) {
    journal_ = j;
  }

  constexpr MonitorsRepo& Monitors() {
    return monitors_;
  }

  const absl::flat_hash_map<std::string, base::Histogram>& call_latency_histos() const {
    return call_latency_histos_;
  }

  void RecordCallLatency(std::string_view sha, uint64_t latency_usec) {
    call_latency_histos_[sha].Add(latency_usec);
  }

  Stats stats;

 private:
  int64_t live_transactions_ = 0;
  mi_heap_t* data_heap_;
  journal::Journal* journal_ = nullptr;

  InterpreterManager interpreter_mgr_;
  GlobalState gstate_ = GlobalState::ACTIVE;

  using Counter = util::SlidingCounter<7>;
  Counter qps_;

  MonitorsRepo monitors_;

  absl::flat_hash_map<std::string, base::Histogram> call_latency_histos_;

  static thread_local ServerState state_;
};

}  // namespace dfly
