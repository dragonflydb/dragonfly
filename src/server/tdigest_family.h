// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"

namespace dfly {

class CommandRegistry;
struct CommandContext;

class TDigestFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Create(CmdArgList args, const CommandContext& cmd_cntx);
  static void Add(CmdArgList args, const CommandContext& cmd_cntx);
  static void Rank(CmdArgList args, const CommandContext& cmd_cntx);
  static void RevRank(CmdArgList args, const CommandContext& cmd_cntx);
  static void ByRank(CmdArgList args, const CommandContext& cmd_cntx);
  static void ByRevRank(CmdArgList args, const CommandContext& cmd_cntx);
  static void Reset(CmdArgList args, const CommandContext& cmd_cntx);
  static void Info(CmdArgList args, const CommandContext& cmd_cntx);
  static void Max(CmdArgList args, const CommandContext& cmd_cntx);
  static void Min(CmdArgList args, const CommandContext& cmd_cntx);
  static void Cdf(CmdArgList args, const CommandContext& cmd_cntx);
  static void Quantile(CmdArgList args, const CommandContext& cmd_cntx);
  static void TrimmedMean(CmdArgList args, const CommandContext& cmd_cntx);
  // TODO
  // static void Merge(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
