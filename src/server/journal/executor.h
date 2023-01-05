// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/journal/types.h"

namespace dfly {

class Service;

// JournalExecutor allows executing journal entries.
class JournalExecutor {
 public:
  JournalExecutor(Service* service);
  void Execute(DbIndex dbid, std::vector<journal::ParsedEntry::CmdData>& cmds);
  void Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd);

 private:
  void Execute(journal::ParsedEntry::CmdData& cmd);
  Service* service_;
  ConnectionContext conn_context_;
  io::NullSink null_sink_;
};

}  // namespace dfly
