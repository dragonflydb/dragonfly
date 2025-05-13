// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tdigest_family.h"

#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/transaction.h"

extern "C" {
#include "redis/tdigest.h"
}

namespace dfly {

void TDigestFamily::Create(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  size_t compression = 50;
  if (parser.HasNext() && !parser.Check("COMPRESSION", &compression)) {
    return cmd_cntx.rb->SendError(facade::kSyntaxErr);
  }

  auto cb = [key, compression](Transaction* tx, EngineShard* es) -> OpResult<bool> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto res = db_slice.AddOrFind(db_cntx, key);
    if (!res) {
      return res.status();
    }

    if (!res->is_new) {
      return OpStatus::KEY_EXISTS;
    }

    td_histogram_t* td = td_new(compression);
    // DENYOOM should cover this
    if (!td) {
      db_slice.Del(db_cntx, res->it);
      return OpStatus::OUT_OF_MEMORY;
    }
    res->it->second.InitRobj(OBJ_TDIGEST, 0, td);
    return OpStatus::OK;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

void TDigestFamily::Add(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<double> values;
  while (parser.HasNext()) {
    double val = parser.Next<double>();
    values.push_back(val);
  }

  if (parser.HasError()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  auto cb = [key, &values](Transaction* tx, EngineShard* es) -> OpResult<bool> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    ;
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    for (auto value : values) {
      if (td_add(td, value, 1) != 0) {
        return OpStatus::OUT_OF_RANGE;
      }
    }
    return OpStatus::OK;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

double TdGetByRank(td_histogram_t* td, double total_obs, double rnk) {
  const double input_p = rnk / total_obs;
  return td_quantile(td, input_p);
}

void ByRankImpl(CmdArgList args, const CommandContext& cmd_cntx, bool reverse) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<size_t> ranks;
  while (parser.HasNext()) {
    double val = parser.Next<double>();
    ranks.push_back(val);
  }

  if (parser.HasError()) {
    cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  using ByRankResult = std::vector<double>;
  auto cb = [key, reverse, &ranks](Transaction* tx, EngineShard* es) -> OpResult<ByRankResult> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    ;
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    const double size = (double)td_size(td);
    const double min = td_min(td);
    const double max = td_max(td);

    ByRankResult result;
    for (auto rnk : ranks) {
      if (size == 0) {
        result.push_back(NAN);
      } else if (rnk == 0) {
        result.push_back(reverse ? max : min);
      } else if (rnk >= size) {
        result.push_back(reverse ? -INFINITY : INFINITY);
      } else {
        result.push_back(TdGetByRank(td, size, reverse ? (size - rnk - 1) : rnk));
      }
    }
    return result;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

void TDigestFamily::ByRank(CmdArgList args, const CommandContext& cmd_cntx) {
  return ByRankImpl(args, cmd_cntx, false);
}

void TDigestFamily::ByRevRank(CmdArgList args, const CommandContext& cmd_cntx) {
  return ByRankImpl(args, cmd_cntx, true);
}

struct InfoResult {
  double compression;
  int cap;
  int merged_nodes;
  int unmerged_nodes;
  int64_t merged_weight;
  int64_t unmerged_weight;
  int64_t observations;
  int64_t total_compressions;
  size_t mem_usage;
};

void TDigestFamily::Info(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();

  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<InfoResult> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindReadOnly(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->GetInnerIt()->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    InfoResult res;
    res.compression = td->compression;
    res.cap = td->cap;
    res.merged_nodes = td->merged_nodes;
    res.unmerged_nodes = td->unmerged_nodes;
    res.merged_weight = td->merged_weight;
    res.unmerged_weight = td->unmerged_weight;
    res.observations = res.unmerged_weight + res.merged_weight;
    res.total_compressions = td->total_compressions;
    res.mem_usage = wrapper->MallocUsed(false);
    return res;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(9 * 2);
  rb->SendSimpleString("Compression");
  rb->SendLong(res->compression);
  rb->SendSimpleString("Capacity");
  rb->SendLong(res->cap);
  rb->SendSimpleString("Merged nodes");
  rb->SendLong(res->merged_nodes);
  rb->SendSimpleString("Unmerged nodes");
  rb->SendLong(res->unmerged_nodes);
  rb->SendSimpleString("Merged weight");
  rb->SendLong(res->merged_weight);
  rb->SendSimpleString("Unmerged weight");
  rb->SendLong(res->unmerged_weight);
  rb->SendSimpleString("Observations");
  rb->SendLong(res->observations);
  rb->SendSimpleString("Total compressions");
  rb->SendLong(res->total_compressions);
  rb->SendSimpleString("Memory usage");
  rb->SendLong(res->mem_usage);
}

struct MinMax {
  double min = 0;
  double max = 0;
};

OpResult<MinMax> MinMaxImpl(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();

  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<MinMax> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindReadOnly(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->GetInnerIt()->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    const double min = (td_size(td) > 0) ? td_min(td) : NAN;
    const double max = (td_size(td) > 0) ? td_max(td) : NAN;
    return MinMax{min, max};
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  return res;
}

void TDigestFamily::Max(CmdArgList args, const CommandContext& cmd_cntx) {
  auto res = MinMaxImpl(args, cmd_cntx);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->SendDouble(res->min);
}

void TDigestFamily::Min(CmdArgList args, const CommandContext& cmd_cntx) {
  auto res = MinMaxImpl(args, cmd_cntx);
  if (!res) {
    return cmd_cntx.rb->SendError(res.status());
  }
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->SendDouble(res->min);
}

void TDigestFamily::Reset(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  auto key = parser.Next<std::string_view>();

  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<bool> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    td_reset(td);
    return OpStatus::OK;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

double HalfRoundDown(double f) {
  double int_part;
  double frac_part = modf(f, &int_part);

  if (fabs(frac_part) <= 0.5)
    return int_part;

  return int_part >= 0.0 ? int_part + 1.0 : int_part - 1.0;
}

void RankImpl(CmdArgList args, const CommandContext& cmd_cntx, bool reverse) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<size_t> ranks;
  while (parser.HasNext()) {
    double val = parser.Next<double>();
    ranks.push_back(val);
  }

  if (parser.HasError()) {
    cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  using RankResult = std::vector<double>;
  auto cb = [key, reverse, &ranks](Transaction* tx, EngineShard* es) -> OpResult<RankResult> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    const double size = (double)td_size(td);
    const double min = td_min(td);
    const double max = td_max(td);

    RankResult result;

    for (auto rnk : ranks) {
      if (size == 0) {
        result.push_back(-2);
      } else if (rnk < min) {
        result.push_back(reverse ? size : -1);
      } else if (rnk > max) {
        result.push_back(reverse ? -1 : size);
      } else {
        const double cdf_val = td_cdf(td, rnk);
        const double cdf_val_prior_round = cdf_val * size;
        const double cdf_to_absolute =
            reverse ? round(cdf_val_prior_round) : HalfRoundDown(cdf_val_prior_round);
        const double res = reverse ? round(size - cdf_to_absolute) : cdf_to_absolute;
        result.push_back(res);
      }
    }
    return result;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

void TDigestFamily::Rank(CmdArgList args, const CommandContext& cmd_cntx) {
  return RankImpl(args, cmd_cntx, false);
}

void TDigestFamily::RevRank(CmdArgList args, const CommandContext& cmd_cntx) {
  return RankImpl(args, cmd_cntx, true);
}

void TDigestFamily::Cdf(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<size_t> values;
  while (parser.HasNext()) {
    double val = parser.Next<double>();
    values.push_back(val);
  }

  if (parser.HasError()) {
    cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  using Result = std::vector<double>;
  auto cb = [key, &values](Transaction* tx, EngineShard* es) -> OpResult<Result> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    Result result;
    for (auto val : values) {
      result.push_back(td_cdf(td, val));
    }
    return result;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

void TDigestFamily::Quantile(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  std::vector<double> quantiles;
  while (parser.HasNext()) {
    double val = parser.Next<double>();
    if (val < 0 || val > 1.0) {
      cmd_cntx.rb->SendError("quantile should be in [0,1]");
    }
    quantiles.push_back(val);
  }

  if (parser.HasError()) {
    cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  using Result = std::vector<double>;
  auto cb = [key, &quantiles](Transaction* tx, EngineShard* es) -> OpResult<Result> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    Result result;
    result.resize(quantiles.size());
    auto total = quantiles.size();
    for (size_t i = 0; i < total; ++i) {
      int start = i;
      while (i < total - 1 && quantiles[i] <= quantiles[i + 1]) {
        ++i;
      }
      td_quantiles(td, quantiles.data() + start, result.data() + start, i - start + 1);
    }
    return result;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

void TDigestFamily::TrimmedMean(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto key = parser.Next<std::string_view>();
  auto low_cut = parser.Next<double>();
  auto high_cut = parser.Next<double>();
  auto out_of_range = [](auto e) { return e < 0 || e > 1.0; };

  if (out_of_range(low_cut) || out_of_range(high_cut)) {
    cmd_cntx.rb->SendError("cut value should be in [0,1]");
  }

  if (parser.Error()) {
    cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  auto cb = [key, high_cut, low_cut](Transaction* tx, EngineShard* es) -> OpResult<double> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto db_cntx = tx->GetDbContext();
    auto it = db_slice.FindMutable(db_cntx, key, OBJ_TDIGEST);
    if (!it) {
      return it.status();
    }
    auto* wrapper = it->it->second.GetRobjWrapper();
    auto* td = (td_histogram_t*)wrapper->inner_obj();
    const double value = td_trimmed_mean(td, low_cut, high_cut);
    return value;
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(cb);
  // SendError covers ok
  return cmd_cntx.rb->SendError(res.status());
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&TDigestFamily::x)

void TDigestFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry << CI{"TDIGEST.CREATE", CO::WRITE | CO::DENYOOM, -1, 1, 1, acl::TDIGEST}.HFUNC(Create)
            << CI{"TDIGEST.ADD", CO::WRITE | CO::DENYOOM, -2, 1, 1, acl::TDIGEST}.HFUNC(Add)
            << CI{"TDIGEST.RESET", CO::WRITE, 2, 1, 1, acl::TDIGEST}.HFUNC(Reset)
            << CI{"TDIGEST.CDF", CO::READONLY, -2, 1, 1, acl::TDIGEST}.HFUNC(Cdf)
            << CI{"TDIGEST.RANK", CO::READONLY, -2, 1, 1, acl::TDIGEST}.HFUNC(Rank)
            << CI{"TDIGEST.REVRANK", CO::READONLY, -2, 1, 1, acl::TDIGEST}.HFUNC(RevRank)
            << CI{"TDIGEST.BYRANK", CO::READONLY, -2, 1, 1, acl::TDIGEST}.HFUNC(ByRank)
            << CI{"TDIGEST.BYREVRANK", CO::READONLY, -2, 1, 1, acl::TDIGEST}.HFUNC(ByRevRank)
            << CI{"TDIGEST.INFO", CO::READONLY, 2, 1, 1, acl::TDIGEST}.HFUNC(Info)
            << CI{"TDIGEST.MAX", CO::READONLY, 2, 1, 1, acl::TDIGEST}.HFUNC(Max)
            << CI{"TDIGEST.MIN", CO::READONLY, 2, 1, 1, acl::TDIGEST}.HFUNC(Min)
            << CI{"TDIGEST.TRIMMED_MEAN", CO::READONLY, 3, 1, 1, acl::TDIGEST}.HFUNC(TrimmedMean)
            << CI{"TDIGEST.QUANTILE", CO::READONLY, -2, 1, 1, acl::TDIGEST}.HFUNC(Quantile);
};

}  // namespace dfly
