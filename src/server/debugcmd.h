// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/cluster_defs.h"
#include "server/conn_context.h"

namespace dfly {

class EngineShardSet;
class ServerFamily;

class DebugCmd {
 private:
  struct PopulateOptions {
    uint64_t total_count = 0;
    std::string_view prefix{"key"};
    uint32_t val_size = 16;
    bool populate_random_values = false;
    std::string type{"STRING"};
    uint32_t elements = 1;

    std::optional<cluster::SlotRange> slot_range;
  };

 public:
  DebugCmd(ServerFamily* owner, ConnectionContext* cntx);

  void Run(CmdArgList args);

  static void Shutdown();

 private:
  void Populate(CmdArgList args);
  std::optional<PopulateOptions> ParsePopulateArgs(CmdArgList args);
  void PopulateRangeFiber(uint64_t from, uint64_t count, const PopulateOptions& opts);

  void Reload(CmdArgList args);
  void Replica(CmdArgList args);

  void Exec();
  void Inspect(std::string_view key, CmdArgList args);
  void Watched();
  void TxAnalysis();
  void ObjHist();
  void Stacktrace();
  void Shards();
  void LogTraffic(CmdArgList);
  void RecvSize(std::string_view param);

  ServerFamily& sf_;
  ConnectionContext* cntx_;
  facade::SinkReplyBuilder* builder_;
};

}  // namespace dfly
