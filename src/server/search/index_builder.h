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

  // Start building provided index in worker fiber.
  // Calls `on_complete` from worker fiber at the end.
  // This PR does not have cancellation because it's used only in sync mode
  void Start(const OpArgs& op_args, std::function<void()> on_complete);

 private:
  // Main fiber function
  void MainLoopFb(DbTable* table, DbContext db_cntx);

  ShardDocIndex* index_;
  util::fb2::Fiber fiber_;
};

}  // namespace dfly::search
