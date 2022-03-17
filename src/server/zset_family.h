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

  using IndexInterval = std::pair<int32_t, int32_t>;

  struct Bound {
    double val;
    bool is_open = false;
  };

  using ScoreInterval = std::pair<Bound, Bound>;

  struct ZRangeSpec {
    std::variant<IndexInterval, ScoreInterval> interval;
    // TODO: handle open/close, inf etc.
  };

  using ScoredMember = std::pair<std::string, double>;
  using ScoredArray = std::vector<ScoredMember>;

 private:
  static void ZCard(CmdArgList args, ConnectionContext* cntx);
  static void ZAdd(CmdArgList args, ConnectionContext* cntx);
  static void ZIncrBy(CmdArgList args, ConnectionContext* cntx);
  static void ZRange(CmdArgList args, ConnectionContext* cntx);
  static void ZRem(CmdArgList args, ConnectionContext* cntx);
  static void ZScore(CmdArgList args, ConnectionContext* cntx);
  static void ZRangeByScore(CmdArgList args, ConnectionContext* cntx);

  struct ZParams {
    unsigned flags = 0;  // mask of ZADD_IN_ macros.
    bool ch = false;     // Corresponds to CH option.
  };

  using ScoredMemberView = std::pair<double, std::string_view>;
  using ScoredMemberSpan = absl::Span<ScoredMemberView>;
  template <typename T> using OpResult = facade::OpResult<T>;

  static OpResult<unsigned> OpAdd(const ZParams& zparams, const OpArgs& op_args,
                                  std::string_view key, ScoredMemberSpan members);
  static OpResult<unsigned> OpRem(const OpArgs& op_args, std::string_view key, ArgSlice members);
  static OpResult<double> OpScore(const OpArgs& op_args, std::string_view key,
                                  std::string_view member);
  static OpResult<ScoredArray> OpRange(const ZRangeSpec& range_spec, const OpArgs& op_args,
                                       std::string_view key);

};

}  // namespace dfly
