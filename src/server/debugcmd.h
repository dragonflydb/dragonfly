// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/cluster_config.h"
#include "server/conn_context.h"

namespace dfly {

class EngineShardSet;
class ServerFamily;

class DebugCmd {
 private:
  struct PopulateOptions {
    uint64_t total_count = 0;
    std::string_view prefix{"key"};
    uint32_t val_size = 0;
    bool populate_random_values = false;

    std::optional<ClusterConfig::SlotRange> slot_range;
  };

 public:
  DebugCmd(ServerFamily* owner, ConnectionContext* cntx);

  void Run(CmdArgList args);

 private:
  void Populate(CmdArgList args);
  std::optional<PopulateOptions> ParsePopulateArgs(CmdArgList args);
  void PopulateRangeFiber(uint64_t from, uint64_t count, const PopulateOptions& opts);
  void Reload(CmdArgList args);
  void Replica(CmdArgList args);
  void Load(std::string_view filename);
  void Inspect(std::string_view key);
  void Watched();
  void TxAnalysis();
  void ObjHist();
  void Stacktrace();

  ServerFamily& sf_;
  ConnectionContext* cntx_;
};

}  // namespace dfly
