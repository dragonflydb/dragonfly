// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <variant>

#include "facade/op_status.h"
#include "server/acl/acl_commands_def.h"
#include "server/common.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

class ZSetFamily {
 public:
  static void Register(CommandRegistry* registry, acl::CommandTableBuilder builder);

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
  };

  struct ZRangeSpec {
    std::variant<IndexInterval, ScoreInterval, LexInterval, TopNScored> interval;
    RangeParams params;
  };

 private:
  template <typename T> using OpResult = facade::OpResult<T>;

  static void BZPopMin(CmdArgList args, ConnectionContext* cntx);
  static void BZPopMax(CmdArgList args, ConnectionContext* cntx);

  static void ZAdd(CmdArgList args, ConnectionContext* cntx);
  static void ZCard(CmdArgList args, ConnectionContext* cntx);
  static void ZCount(CmdArgList args, ConnectionContext* cntx);
  static void ZDiff(CmdArgList args, ConnectionContext* cntx);
  static void ZIncrBy(CmdArgList args, ConnectionContext* cntx);
  static void ZInterStore(CmdArgList args, ConnectionContext* cntx);
  static void ZInterCard(CmdArgList args, ConnectionContext* cntx);
  static void ZLexCount(CmdArgList args, ConnectionContext* cntx);
  static void ZPopMax(CmdArgList args, ConnectionContext* cntx);
  static void ZPopMin(CmdArgList args, ConnectionContext* cntx);
  static void ZRange(CmdArgList args, ConnectionContext* cntx);
  static void ZRank(CmdArgList args, ConnectionContext* cntx);
  static void ZRem(CmdArgList args, ConnectionContext* cntx);
  static void ZScore(CmdArgList args, ConnectionContext* cntx);
  static void ZMScore(CmdArgList args, ConnectionContext* cntx);
  static void ZRangeByLex(CmdArgList args, ConnectionContext* cntx);
  static void ZRevRangeByLex(CmdArgList args, ConnectionContext* cntx);
  static void ZRangeByLexInternal(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static void ZRangeByScore(CmdArgList args, ConnectionContext* cntx);
  static void ZRemRangeByRank(CmdArgList args, ConnectionContext* cntx);
  static void ZRemRangeByScore(CmdArgList args, ConnectionContext* cntx);
  static void ZRemRangeByLex(CmdArgList args, ConnectionContext* cntx);
  static void ZRevRange(CmdArgList args, ConnectionContext* cntx);
  static void ZRevRangeByScore(CmdArgList args, ConnectionContext* cntx);
  static void ZRevRank(CmdArgList args, ConnectionContext* cntx);
  static void ZScan(CmdArgList args, ConnectionContext* cntx);
  static void ZUnion(CmdArgList args, ConnectionContext* cntx);
  static void ZUnionStore(CmdArgList args, ConnectionContext* cntx);

  static void ZRangeByScoreInternal(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static void ZRemRangeGeneric(std::string_view key, const ZRangeSpec& range_spec,
                               ConnectionContext* cntx);
  static void ZRangeGeneric(CmdArgList args, RangeParams range_params, ConnectionContext* cntx);
  static void ZRankGeneric(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static bool ParseRangeByScoreParams(CmdArgList args, RangeParams* params);
  static void ZPopMinMax(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static OpResult<MScoreResponse> ZGetMembers(CmdArgList args, ConnectionContext* cntx);

  static void GeoAdd(CmdArgList args, ConnectionContext* cntx);
  static void GeoHash(CmdArgList args, ConnectionContext* cntx);
  static void GeoPos(CmdArgList args, ConnectionContext* cntx);
  static void GeoDist(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
