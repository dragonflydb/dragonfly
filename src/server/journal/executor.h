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
  void Execute(std::vector<journal::ParsedEntry>& entries);
  void Execute(journal::ParsedEntry& entry);

 private:
  Service* service_;
  ConnectionContext conn_context_;
  io::NullSink null_sink_;
};

}  // namespace dfly
