// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>

#include "server/journal/executor.h"
#include "server/journal/tx_executor.h"

namespace dfly {

// Executes the journal records of one flow. A record of a single shard runs right away; a global
// command is synchronized across all flows and executed once.
// Each flow (a replica flow, or a shard during AOF replay) has its own applier and calls Apply
// from a single fiber. The flows of one sync session share multi_shard_exe_ across fibers, which
// may run on different proactors: a global command is the synchronization point where all flows
// rendezvous, so that exactly one of them executes it and none runs later records before it
// finishes.
class JournalApplier {
 public:
  // The only customization point: called right before a record executes, once per execution (so
  // once for a global command). The replica uses it for traffic logging.
  using ExecuteHook = std::function<void(const TransactionData&)>;

  JournalApplier(Service* service, std::shared_ptr<MultiShardExecution> multi_shard_exe,
                 uint32_t num_flows, ExecuteHook execute_hook = {});

  // Return true if the transaction executed successfully. On error,
  // or on context cancellation return false.
  // A global command waits in multi_shard_exe_ for every flow, and exactly one of them executes it.
  bool Apply(TransactionData&& tx_data, ExecutionState* cntx);

 private:
  bool Execute(TransactionData&& tx_data);

  JournalExecutor executor_;
  // Shared by the appliers of all flows of a sync session; synchronizes their global commands.
  std::shared_ptr<MultiShardExecution> multi_shard_exe_;
  uint32_t num_flows_;
  ExecuteHook execute_hook_;
};

}  // namespace dfly
