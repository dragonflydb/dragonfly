// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>

#include "server/common.h"
#include "server/tx_base.h"

namespace dfly {
struct DbTable;
class ShardDocIndex;
}  // namespace dfly

namespace dfly::search {

// Asynchronous index builder
struct IndexBuilder {
  explicit IndexBuilder(ShardDocIndex* index) : index_{index} {
  }

  // Start building and call `on_complete` on finish from worker fiber
  void Start(const OpArgs& op_args, std::function<void()> on_complete);

  // Cancel building and wait for worker to finish. Safe to delete after
  // TODO: Maybe implement nonblocking version?
  void Cancel();

  // Get fiber reference. Temporary to polyfill sync constuction places
  util::fb2::Fiber Worker();

 private:
  // Main fiber function
  void MainLoopFb(DbTable* table, DbContext db_cntx);

  dfly::ExecutionState state_;
  ShardDocIndex* index_;
  util::fb2::Fiber fiber_;
};

}  // namespace dfly::search
