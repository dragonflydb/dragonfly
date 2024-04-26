// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include "facade/reply_capture.h"
#include "server/cluster/cluster_defs.h"
#include "server/journal/types.h"

namespace dfly {

class Service;

// JournalExecutor allows executing journal entries.
class JournalExecutor {
 public:
  explicit JournalExecutor(Service* service);
  ~JournalExecutor();

  JournalExecutor(JournalExecutor&&) = delete;

  void Execute(DbIndex dbid, absl::Span<journal::ParsedEntry::CmdData> cmds);
  void Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd);

  void FlushAll();  // Execute FLUSHALL.
  void FlushSlots(const cluster::SlotRange& slot_range);

  ConnectionContext* connection_context() {
    return &conn_context_;
  }

 private:
  void Execute(journal::ParsedEntry::CmdData& cmd);

  // Select database. Ensure it exists if accessed for first time.
  void SelectDb(DbIndex dbid);

  Service* service_;
  facade::CapturingReplyBuilder reply_builder_;
  ConnectionContext conn_context_;

  std::vector<bool> ensured_dbs_;
};

}  // namespace dfly
