// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <vector>

#include "core/interpreter.h"
#include "server/common.h"
#include "util/sliding_counter.h"

typedef struct mi_heap_s mi_heap_t;

namespace dfly {

class ConnectionContext;
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
  struct MonitorInfo {
    ConnectionContext* connection = nullptr;
    util::fibers_ext::BlockingCounter token;
    std::uint32_t thread_id = 0;

    explicit MonitorInfo(ConnectionContext* conn);

    void Send(std::string_view msg, std::uint32_t tid, util::fibers_ext::BlockingCounter borrows);
  };

  void Add(const MonitorInfo& info);

  void Send(std::string_view msg, util::fibers_ext::BlockingCounter borrows, std::uint32_t tid);

  void Release(std::uint32_t tid);

  void Remove(const ConnectionContext* conn);

  bool Empty() const {
    return monitors_.empty();
  }

  std::size_t Size() const {
    return monitors_.size();
  }

 private:
  using MonitorVec = std::vector<MonitorInfo>;
  MonitorVec monitors_;
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

  Interpreter& GetInterpreter();

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

 private:
  int64_t live_transactions_ = 0;
  mi_heap_t* data_heap_;
  journal::Journal* journal_ = nullptr;

  std::optional<Interpreter> interpreter_;
  GlobalState gstate_ = GlobalState::ACTIVE;

  using Counter = util::SlidingCounter<7>;
  Counter qps_;

  MonitorsRepo monitors_;

  static thread_local ServerState state_;
};

}  // namespace dfly
