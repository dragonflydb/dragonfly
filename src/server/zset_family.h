// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <variant>

#include "facade/op_status.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class CommandRegistry;
struct CommandContext;

class ZSetFamily {
 public:
  static void Register(CommandRegistry* registry);

  using IndexInterval = std::pair<int32_t, int32_t>;
  using MScoreResponse = std::vector<std::optional<double>>;

  struct Bound {
    double val;
    bool is_open = false;
  };

  using ScoreInterval = std::pair<Bound, Bound>;

  struct LexBound {
    std::string_view val;
    enum Type { PLUS_INF, MINUS_INF, OPEN, CLOSED } type = CLOSED;
  };

  using LexInterval = std::pair<LexBound, LexBound>;

  using TopNScored = uint32_t;

  struct RangeParams {
    uint32_t offset = 0;
    uint32_t limit = UINT32_MAX;
    bool with_scores = false;
    bool reverse = false;
    enum IntervalType { LEX, RANK, SCORE } interval_type = RANK;
    std::optional<std::string_view> store_key = std::nullopt;
  };

  struct ZRangeSpec {
    std::variant<IndexInterval, ScoreInterval, LexInterval, TopNScored> interval;
    RangeParams params;
    ZRangeSpec() = default;
    ZRangeSpec(const ScoreInterval& si, const RangeParams& rp) : interval(si), params(rp){};
  };

 private:
  template <typename T> using OpResult = facade::OpResult<T>;
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void BZPopMin(CmdArgList args, const CommandContext& cmd_cntx);
  static void BZPopMax(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZAdd(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZCard(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZCount(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZDiff(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZIncrBy(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZInterStore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZInter(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZInterCard(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZLexCount(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZMPop(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZPopMax(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZPopMin(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRange(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRank(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRem(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRandMember(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZScore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZMScore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRangeByLex(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRevRangeByLex(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRangeByScore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRangeStore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRemRangeByRank(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRemRangeByScore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRemRangeByLex(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRevRange(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRevRangeByScore(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZRevRank(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZScan(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZUnion(CmdArgList args, const CommandContext& cmd_cntx);
  static void ZUnionStore(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoAdd(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoHash(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoPos(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoDist(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoSearch(CmdArgList args, const CommandContext& cmd_cntx);
  static void GeoRadiusByMember(CmdArgList args, const CommandContext& cmd_cntx);
};

}  // namespace dfly
