// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>

#include "server/journal/executor.h"
#include "server/journal/tx_executor.h"

namespace dfly {

class JournalApplier {
 public:
  using ExecuteHook = std::function<void(const TransactionData&)>;

  JournalApplier(Service* service, std::shared_ptr<MultiShardExecution> multi_shard_exe,
                 ExecuteHook execute_hook = {});

  // Return true if the transaction executed successfully. On error,
  // or on context cancellation return false.
  bool Apply(TransactionData&& tx_data, ExecutionState* cntx);

 private:
  JournalExecutor executor_;
  std::shared_ptr<MultiShardExecution> multi_shard_exe_;
  ExecuteHook execute_hook_;
};

}  // namespace dfly
