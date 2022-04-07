// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <vector>

#include "core/interpreter.h"
#include "server/common.h"
#include "server/global_state.h"
#include "util/sliding_counter.h"

namespace dfly {

// Present in every server thread. This class differs from EngineShard. The latter manages
// state around engine shards while the former represents coordinator/connection state.
// There may be threads that handle engine shards but not IO, there may be threads that handle IO
// but not engine shards and there can be threads that handle both. This class is present only
// for threads that handle IO and manage incoming connections.
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

  GlobalState::S gstate() const {
    return gstate_;
  }
  void set_gstate(GlobalState::S s) {
    gstate_ = s;
  }

  Interpreter& GetInterpreter();

  // Returns sum of all requests in the last 6 seconds
  // (not including the current one).
  uint32_t MovingSum6() const { return qps_.SumTail(); }

  void RecordCmd() {
    ++connection_stats.command_cnt;
    qps_.Inc();
  }

 private:
  int64_t live_transactions_ = 0;
  std::optional<Interpreter> interpreter_;
  GlobalState::S gstate_ = GlobalState::IDLE;

  using Counter = util::SlidingCounter<7>;
  Counter qps_;

  static thread_local ServerState state_;
};

}  // namespace dfly
