// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <variant>

#include "facade/op_status.h"
#include "server/common.h"

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

  using ScoredMember = std::pair<std::string, double>;
  using ScoredArray = std::vector<ScoredMember>;

 private:
  template <typename T> using OpResult = facade::OpResult<T>;

  static void ZAdd(CmdArgList args, ConnectionContext* cntx);
  static void ZCard(CmdArgList args, ConnectionContext* cntx);
  static void ZCount(CmdArgList args, ConnectionContext* cntx);
  static void ZIncrBy(CmdArgList args, ConnectionContext* cntx);
  static void ZInterStore(CmdArgList args, ConnectionContext* cntx);
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
  static void ZUnionStore(CmdArgList args, ConnectionContext* cntx);

  static void ZRangeByScoreInternal(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static void OutputScoredArrayResult(const OpResult<ScoredArray>& arr, const RangeParams& params,
                                      ConnectionContext* cntx);
  static void ZRemRangeGeneric(std::string_view key, const ZRangeSpec& range_spec,
                               ConnectionContext* cntx);
  static void ZRangeGeneric(CmdArgList args, RangeParams range_params, ConnectionContext* cntx);
  static void ZRankGeneric(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static bool ParseRangeByScoreParams(CmdArgList args, RangeParams* params);
  static void ZPopMinMax(CmdArgList args, bool reverse, ConnectionContext* cntx);
  static OpResult<StringVec> OpScan(const OpArgs& op_args, std::string_view key, uint64_t* cursor);

  static OpResult<unsigned> OpRem(const OpArgs& op_args, std::string_view key, ArgSlice members);
  static OpResult<double> OpScore(const OpArgs& op_args, std::string_view key,
                                  std::string_view member);
  using MScoreResponse = std::vector<std::optional<double>>;
  static OpResult<MScoreResponse> OpMScore(const OpArgs& op_args, std::string_view key,
                                           ArgSlice members);
  static OpResult<ScoredArray> OpPopCount(const ZRangeSpec& range_spec, const OpArgs& op_args,
                                          std::string_view key);
  static OpResult<ScoredArray> OpRange(const ZRangeSpec& range_spec, const OpArgs& op_args,
                                       std::string_view key);
  static OpResult<unsigned> OpRemRange(const OpArgs& op_args, std::string_view key,
                                       const ZRangeSpec& spec);

  static OpResult<unsigned> OpRank(const OpArgs& op_args, std::string_view key,
                                   std::string_view member, bool reverse);

  static OpResult<unsigned> OpCount(const OpArgs& op_args, std::string_view key,
                                    const ScoreInterval& interval);

  static OpResult<unsigned> OpLexCount(const OpArgs& op_args, std::string_view key,
                                       const LexInterval& interval);
};

}  // namespace dfly
