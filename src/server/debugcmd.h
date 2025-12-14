// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/cluster_defs.h"
#include "server/conn_context.h"

namespace dfly {

namespace cluster {
class ClusterFamily;
}

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
    std::optional<std::pair<uint32_t, uint32_t>> expire_ttl_range;
  };

 public:
  DebugCmd(ServerFamily* owner, cluster::ClusterFamily* cf, ConnectionContext* cntx);

  void Run(CmdArgList args, CommandContext* cmd_cntx);

  static void Shutdown();

 private:
  void Populate(CmdArgList args, CommandContext* cmd_cntx);
  static std::optional<PopulateOptions> ParsePopulateArgs(CmdArgList args,
                                                          CommandContext* cmd_cntx);
  void PopulateRangeFiber(uint64_t from, uint64_t count, const PopulateOptions& opts);

  void Reload(CmdArgList args, CommandContext* cmd_cntx);
  void Replica(CmdArgList args, CommandContext* cmd_cntx);
  void Migration(CmdArgList args, CommandContext* cmd_cntx);

  void Exec(CommandContext* cmd_cntx);
  void Inspect(std::string_view key, CmdArgList args, CommandContext* cmd_cntx);
  void Watched(CommandContext* cmd_cntx);
  void TxAnalysis(CommandContext* cmd_cntx);
  void ObjHist(CommandContext* cmd_cntx);
  void Stacktrace(CommandContext* cmd_cntx);
  void Shards(CommandContext* cmd_cntx);
  void LogTraffic(CmdArgList, CommandContext* cmd_cntx);
  void RecvSize(std::string_view param, CommandContext* cmd_cntx);
  void Topk(CmdArgList args, CommandContext* cmd_cntx);
  void Keys(CmdArgList args, CommandContext* cmd_cntx);
  void Values(CmdArgList args, CommandContext* cmd_cntx);
  void Compression(CmdArgList args, CommandContext* cmd_cntx);
  void IOStats(CmdArgList args, CommandContext* cmd_cntx);
  void Segments(CmdArgList args, CommandContext* cmd_cntx);
  struct PopulateBatch {
    DbIndex dbid;
    uint64_t index[32];
    uint64_t sz = 0;

    explicit PopulateBatch(DbIndex id) : dbid(id) {
    }
  };

  void DoPopulateBatch(const PopulateOptions& options, const PopulateBatch& batch);

  ServerFamily& sf_;
  cluster::ClusterFamily& cf_;
  ConnectionContext* cntx_;
};

}  // namespace dfly
