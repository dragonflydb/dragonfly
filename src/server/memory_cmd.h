// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <map>
#include <string>

#include "server/conn_context.h"

namespace dfly {

class ServerFamily;

class MemoryCmd {
 public:
  MemoryCmd(ServerFamily* owner, ConnectionContext* cntx);

  void Run(CmdArgList args);

  std::map<std::string, size_t> GetMemoryStats() const;

 private:
  void Stats();
  void Usage(std::string_view key);

  ConnectionContext* cntx_;
  ServerFamily* owner_;
};

}  // namespace dfly
