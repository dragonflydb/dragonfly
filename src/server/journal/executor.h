// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include "facade/service_interface.h"
#include "server/cluster/cluster_defs.h"
#include "server/journal/types.h"

namespace facade {
class CapturingReplyBuilder;
}  // namespace facade

namespace dfly {

class Service;

// JournalExecutor allows executing journal entries.
class JournalExecutor {
 public:
  explicit JournalExecutor(Service* service);
  ~JournalExecutor();

  JournalExecutor(JournalExecutor&&) = delete;

  // Returns the result of Service::DispatchCommand
  facade::DispatchResult Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd);

  void FlushAll();  // Execute FLUSHALL.
  void FlushSlots(const cluster::SlotRange& slot_range);

  ConnectionContext* connection_context() {
    return &conn_context_;
  }

 private:
  facade::DispatchResult Execute(journal::ParsedEntry::CmdData& cmd);

  // Select database. Ensure it exists if accessed for first time.
  void SelectDb(DbIndex dbid);

  Service* service_;
  std::unique_ptr<facade::CapturingReplyBuilder> reply_builder_;
  ConnectionContext conn_context_;

  std::vector<bool> ensured_dbs_;
};

}  // namespace dfly
