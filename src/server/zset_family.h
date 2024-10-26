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

class ConnectionContext;
class CommandRegistry;
class Transaction;

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

  static void BZPopMin(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                       ConnectionContext* cntx);
  static void BZPopMax(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                       ConnectionContext* cntx);

  static void ZAdd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZCard(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZCount(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZDiff(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZIncrBy(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZInterStore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZInter(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZInterCard(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZLexCount(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZPopMax(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZPopMin(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRank(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRem(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRandMember(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZScore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZMScore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRangeByLex(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRevRangeByLex(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRangeByScore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRangeStore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRemRangeByRank(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRemRangeByScore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRemRangeByLex(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRevRange(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRevRangeByScore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZRevRank(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZScan(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZUnion(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void ZUnionStore(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);

  static void GeoAdd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void GeoHash(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void GeoPos(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void GeoDist(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void GeoSearch(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void GeoRadiusByMember(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
