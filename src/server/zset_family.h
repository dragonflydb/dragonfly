// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <variant>

#include "facade/op_status.h"
#include "server/common_types.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

class ZSetFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void ZCard(CmdArgList args,  ConnectionContext* cntx);
  static void ZAdd(CmdArgList args,  ConnectionContext* cntx);
  static void ZIncrBy(CmdArgList args,  ConnectionContext* cntx);
  static void ZRange(CmdArgList args,  ConnectionContext* cntx);
  static void ZRem(CmdArgList args,  ConnectionContext* cntx);
  static void ZScore(CmdArgList args,  ConnectionContext* cntx);
  static void ZRangeByScore(CmdArgList args,  ConnectionContext* cntx);

  struct ZParams {
    unsigned flags = 0;  // mask of ZADD_IN_ macros.
    bool ch = false;     // Corresponds to CH option.
  };

  using ScoredMemberView = std::pair<double, std::string_view>;
  using ScoredMemberSpan = absl::Span<ScoredMemberView>;
  template <typename T> using OpResult = facade::OpResult<T>;

  static OpResult<unsigned> OpAdd(const ZParams& zparams, const OpArgs& op_args,
                                  std::string_view key, const ScoredMemberSpan& members);

};

}  // namespace dfly
