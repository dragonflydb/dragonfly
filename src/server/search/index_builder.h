// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>

#include "server/execution_state.h"
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

  // Start building and call `on_complete` on finish from worker fiber.
  // If `is_restored` is true, VectorLoop will use UpdateVectorData instead of Add
  // for HNSW indices (restored from RDB). This flag is passed from PerformPostLoad.
  void Start(const OpArgs& op_args, bool is_restored, std::function<void()> on_complete);

  // Cancel building and wait for worker to finish. Safe to delete after
  // TODO: Maybe implement nonblocking version?
  void Cancel();

  // Get fiber reference. Temporary to polyfill sync construction places
  util::fb2::Fiber Worker();

 private:
  // Loop with cursor over table and add entries to regular index
  void CursorLoop(DbTable* table, DbContext db_cntx);

  // Loop with cursor over table and add entries to global HNSW vector indices
  void VectorLoop(DbTable* table, DbContext db_cntx);

  dfly::ExecutionState state_;
  ShardDocIndex* index_;
  bool is_restored_ = false;
  util::fb2::Fiber fiber_;
};

}  // namespace dfly::search
