// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string>

#include "server/conn_context.h"

namespace dfly {

class EngineShardSet;
class ServerFamily;

class DebugCmd {
 public:
  DebugCmd(ServerFamily* owner, ConnectionContext* cntx);

  void Run(CmdArgList args);

  struct PopulateBatch {
    DbIndex dbid;
    uint64_t index[32];
    uint64_t sz = 0;

    PopulateBatch(DbIndex id) : dbid(id) {
    }
  };

 private:
  struct KeyRange {
    uint64_t from, to;
    std::string_view prefix;
  };

  using PopulateFunction = std::function<void(std::string_view prefix, const PopulateBatch& batch)>;

  void Populate(CmdArgList args);
  void MemoryFuzz(CmdArgList args);
  void Reload(CmdArgList args);
  void Replica(CmdArgList args);
  void Load(std::string_view filename);
  void Inspect(std::string_view key);
  void Watched();

  // Run populate function on given range in batches
  void RunPopulate(KeyRange range, DebugCmd::PopulateFunction func);
  void PopulateRangeFiber(KeyRange range, PopulateFunction func);

  ServerFamily& sf_;
  ConnectionContext* cntx_;
};

}  // namespace dfly
