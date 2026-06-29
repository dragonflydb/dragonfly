// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>

#include <cmath>
#include <limits>

#include "core/cms.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_families.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

constexpr char kCmsNotFound[] = "CMS: key does not exist";
constexpr char kCmsWrongNumKeys[] = "CMS: wrong number of keys";
constexpr char kCmsWrongNumKeysWeights[] = "CMS: wrong number of keys/weights";
constexpr char kCmsCannotParseNumber[] = "CMS: Cannot parse number";

constexpr uint32_t kMaxCmsWidth = 1'000'000;
constexpr uint32_t kMaxCmsDepth = 100;

bool ValidateCmsDimensions(uint32_t width, uint32_t depth, RedisReplyBuilder* rb) {
  if (width == 0 || depth == 0) {
    rb->SendError("CMS: width and depth must be greater than 0");
    return false;
  }

  if ((width > kMaxCmsWidth) || (depth > kMaxCmsDepth)) {
    rb->SendError(absl::StrCat("CMS: width must not exceed ", kMaxCmsWidth,
                               " and depth must not exceed ", kMaxCmsDepth));
    return false;
  }

  return true;
}

bool ComputeCmsDimensions(double error, double probability, RedisReplyBuilder* rb, uint32_t* width,
                          uint32_t* depth) {
  double computed_width = std::ceil(M_E / error);
  double computed_depth = std::ceil(std::log(1.0 / probability));

  if (!std::isfinite(computed_width) || !std::isfinite(computed_depth) || computed_width <= 0 ||
      computed_depth <= 0 || computed_width > std::numeric_limits<uint32_t>::max() ||
      computed_depth > std::numeric_limits<uint32_t>::max()) {
    rb->SendError("CMS: invalid error/probability");
    return false;
  }

  *width = static_cast<uint32_t>(computed_width);
  *depth = static_cast<uint32_t>(computed_depth);
  return ValidateCmsDimensions(*width, *depth, rb);
}

OpStatus OpInitByDim(const OpArgs& op_args, string_view key, uint32_t width, uint32_t depth) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CMS);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  PrimeValue& pv = op_res->it->second;
  pv.SetCMS(width, depth);

  return OpStatus::OK;
}

OpResult<vector<int64_t>> OpIncrBy(const OpArgs& op_args, string_view key,
                                   const vector<pair<string_view, int64_t>>& items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_CMS);
  if (!op_res)
    return op_res.status();

  CMS* cms = op_res->it->second.GetCMS();
  vector<int64_t> result;
  result.reserve(items.size());

  for (const auto& [item, incr] : items) {
    result.push_back(cms->IncrBy(item, incr));
  }

  return result;
}

OpResult<vector<int64_t>> OpQuery(const OpArgs& op_args, string_view key, ParsedArgs items) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CMS);
  if (!op_res)
    return op_res.status();

  const CMS* cms = op_res.value()->second.GetCMS();
  vector<int64_t> result;
  result.reserve(items.size());

  for (auto arg : items) {
    result.push_back(cms->Query(ToSV(arg)));
  }

  return result;
}

struct CmsInfo {
  uint32_t width = 0;
  uint32_t depth = 0;
  int64_t count = 0;
};

OpResult<CmsInfo> OpInfo(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  OpResult op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CMS);
  if (!op_res)
    return op_res.status();

  const CMS* cms = op_res.value()->second.GetCMS();
  return CmsInfo{cms->width(), cms->depth(), cms->total_count()};
}

void CmdInitByDim(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  uint32_t width, depth;

  tie(width, depth) = parser.Next<uint32_t, uint32_t>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (!ValidateCmsDimensions(width, depth, rb))
    return;

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInitByDim(t->GetOpArgs(shard), key, width, depth);
  };

  OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("item exists");
  }
  if (res == OpStatus::OK) {
    return rb->SendOk();
  }
  return rb->SendError(res);
}

void CmdInitByProb(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  double error, probability;

  tie(error, probability) = parser.Next<double, double>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (!(error > 0 && error < 1)) {
    return rb->SendError("CMS: error must be between 0 and 1 exclusive");
  }
  if (!(probability > 0 && probability < 1)) {
    return rb->SendError("CMS: probability must be between 0 and 1 exclusive");
  }

  uint32_t width = 0, depth = 0;
  if (!ComputeCmsDimensions(error, probability, rb, &width, &depth))
    return;

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInitByDim(t->GetOpArgs(shard), key, width, depth);
  };

  OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("item exists");
  }
  if (res == OpStatus::OK) {
    return rb->SendOk();
  }
  return rb->SendError(res);
}

void CmdIncrBy(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();

  // Parse item/increment pairs. tail_args() includes the key, so subtract it.
  size_t num_pair_args = cmd_cntx->tail_args().size() - 1;
  if (num_pair_args < 2 || num_pair_args % 2 != 0) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  vector<pair<string_view, int64_t>> items;
  items.reserve(num_pair_args / 2);

  while (parser.HasNext()) {
    string_view item = parser.Next();
    int64_t incr;
    if (!absl::SimpleAtoi(parser.Next(), &incr)) {
      return cmd_cntx->SendError(kCmsCannotParseNumber);
    }
    if (incr <= 0) {
      return cmd_cntx->SendError("CMS: increment must be a positive integer");
    }
    items.emplace_back(item, incr);
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, items);
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  OpResult<vector<int64_t>> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      return rb->SendError(kCmsNotFound);
    }
    return rb->SendError(res.status());
  }

  SinkReplyBuilder::ReplyScope scope(rb);
  rb->StartArray(res->size());
  for (int64_t count : *res) {
    rb->SendLong(count);
  }
}

void CmdQuery(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();

  ParsedArgs items = parser.UnparsedArgs();
  if (items.empty()) {
    return cmd_cntx->SendError(kSyntaxErr);
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpQuery(t->GetOpArgs(shard), key, items);
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  OpResult<vector<int64_t>> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      return rb->SendError(kCmsNotFound);
    }
    return rb->SendError(res.status());
  }

  SinkReplyBuilder::ReplyScope scope(rb);
  rb->StartArray(res->size());
  for (int64_t count : *res) {
    rb->SendLong(count);
  }
}

void CmdInfo(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInfo(t->GetOpArgs(shard), key);
  };

  OpResult<CmsInfo> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      return rb->SendError(kCmsNotFound);
    }
    return rb->SendError(res.status());
  }

  {
    SinkReplyBuilder::ReplyScope scope(rb);
    rb->StartArray(6);
    rb->SendBulkString("width");
    rb->SendLong(res->width);
    rb->SendBulkString("depth");
    rb->SendLong(res->depth);
    rb->SendBulkString("count");
    rb->SendLong(res->count);
  }
}

// Structure to hold CMS data collected from a shard when merging
struct CmsShardData {
  size_t src_index = 0;
  string_view key;
  uint32_t width = 0;
  uint32_t depth = 0;
  int64_t count = 0;
  vector<int64_t> counters;

  CmsShardData(size_t src_idx, string_view k, uint32_t w, uint32_t d, int64_t c,
               const int64_t* data, size_t size)
      : src_index(src_idx), key(k), width(w), depth(d), count(c), counters(data, data + size) {
  }
};

struct CmsMergeArgs {
  string_view dest_key;
  vector<string_view> src_keys;
  vector<int64_t> weights;
};

bool ParseMergeArgs(CmdArgParser parser, RedisReplyBuilder* rb, CmsMergeArgs* out) {
  out->dest_key = parser.Next();

  auto keys = parser.NextRange(1, kCmsWrongNumKeys);
  if (auto err = parser.TakeError(); err) {
    rb->SendError(err.MakeReply());
    return false;
  }
  out->src_keys.assign(keys.begin(), keys.end());
  uint32_t num_keys = out->src_keys.size();

  if (parser.HasNext()) {
    if (!parser.Check("WEIGHTS")) {
      rb->SendError(kCmsWrongNumKeysWeights);
      return false;
    }

    auto weights = parser.RemainingRange();
    if (weights.size() != num_keys) {
      rb->SendError(kCmsWrongNumKeysWeights);
      return false;
    }
    out->weights.reserve(num_keys);
    for (string_view w : weights) {
      int64_t weight;
      if (!absl::SimpleAtoi(w, &weight)) {
        rb->SendError(kCmsCannotParseNumber);
        return false;
      }
      out->weights.push_back(weight);
    }
  } else {
    out->weights.resize(num_keys, 1);
  }

  return true;
}

// Merge multiple CMS structures into a destination key.
void CmdMerge(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  CmsMergeArgs merge_args;
  if (!ParseMergeArgs(parser, rb, &merge_args)) {
    return;
  }

  // multi-shard implementation
  // 1. fetch from all shards
  // 2. merge to dest
  Transaction* tx = cmd_cntx->tx();

  vector<OpResult<vector<CmsShardData>>> shard_results(shard_set->size(), OpStatus::SKIPPED);

  auto read_cb = [&](Transaction* t, EngineShard* shard) -> OpStatus {
    auto& db_slice = t->GetOpArgs(shard).GetDbSlice();
    const DbContext& db_cntx = t->GetDbContext();
    vector<CmsShardData> cms_list;

    // Check each source key to see if it belongs to this shard
    for (size_t src_idx = 0; src_idx < merge_args.src_keys.size(); ++src_idx) {
      string_view key = merge_args.src_keys[src_idx];
      ShardId key_shard = Shard(key, shard_set->size());
      if (key_shard != shard->shard_id()) {
        continue;
      }

      OpResult src_res = db_slice.FindReadOnly(db_cntx, key, OBJ_CMS);
      if (!src_res) {
        shard_results[shard->shard_id()] = src_res.status();
        return OpStatus::OK;
      }

      const CMS* cms = src_res.value()->second.GetCMS();
      size_t counter_count = cms->NumCounters();
      cms_list.emplace_back(src_idx, key, cms->width(), cms->depth(), cms->total_count(),
                            cms->Data(), counter_count);
    }

    if (!cms_list.empty()) {
      shard_results[shard->shard_id()] = std::move(cms_list);
    }
    return OpStatus::OK;
  };

  tx->Execute(read_cb, false /* do not conclude */);

  // Validate dimensions and make sure we found data for every source.
  uint32_t ref_width = 0, ref_depth = 0;
  size_t seen_sources = 0;

  // Check for errors and validate dimensions.
  for (auto& result : shard_results) {
    if (result.status() == OpStatus::SKIPPED)
      continue;

    if (!result) {
      tx->Conclude();
      if (result.status() == OpStatus::KEY_NOTFOUND) {
        return rb->SendError(kCmsNotFound);
      }
      return rb->SendError(result.status());
    }

    for (auto& cms_data : result.value()) {
      if (seen_sources == 0) {
        ref_width = cms_data.width;
        ref_depth = cms_data.depth;
      } else if (cms_data.width != ref_width || cms_data.depth != ref_depth) {
        tx->Conclude();
        return rb->SendError("CMS: dimension mismatch");
      }
      ++seen_sources;
    }
  }

  if (seen_sources != merge_args.src_keys.size()) {
    tx->Conclude();
    return rb->SendError(kCmsNotFound);
  }

  // Now write merged data to destination shard
  ShardId dest_shard_id = Shard(merge_args.dest_key, shard_set->size());
  OpStatus write_result = OpStatus::OK;

  auto write_cb = [&](Transaction* t, EngineShard* shard) -> OpStatus {
    if (shard->shard_id() != dest_shard_id) {
      return OpStatus::OK;
    }

    auto& db_slice = t->GetOpArgs(shard).GetDbSlice();
    OpResult dest_res = db_slice.FindMutable(t->GetDbContext(), merge_args.dest_key, OBJ_CMS);
    if (!dest_res) {
      write_result = dest_res.status();
      return OpStatus::OK;
    }

    CMS* dest_cms = dest_res->it->second.GetCMS();

    // Validate destination dimensions
    if (ref_width != dest_cms->width() || ref_depth != dest_cms->depth()) {
      write_result = OpStatus::INVALID_VALUE;
      return OpStatus::OK;
    }

    // Reset destination before merging so the result is the weighted sum of sources only.
    dest_cms->Reset();

    for (const auto& result : shard_results) {
      if (result.status() == OpStatus::SKIPPED)
        continue;

      for (const auto& cms_data : result.value()) {
        CMS temp_cms(cms_data.width, cms_data.depth, CompactObj::memory_resource());
        temp_cms.Load(cms_data.count, cms_data.counters.data());

        if (!dest_cms->MergeFrom(temp_cms, merge_args.weights[cms_data.src_index])) {
          write_result = OpStatus::INVALID_VALUE;
          return OpStatus::OK;
        }
      }
    }

    return OpStatus::OK;
  };

  tx->Execute(write_cb, true /* conclude */);

  if (write_result == OpStatus::KEY_NOTFOUND) {
    return rb->SendError(kCmsNotFound);
  }
  if (write_result == OpStatus::INVALID_VALUE) {
    return rb->SendError("CMS: dimension mismatch");
  }
  return rb->SendOk();
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterCmsFamily(CommandRegistry* registry) {
  registry->StartFamily(acl::CMS);

  *registry << CI{"CMS.INITBYDIM", CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(InitByDim)
            << CI{"CMS.INITBYPROB", CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(InitByProb)
            << CI{"CMS.INCRBY", CO::DENYOOM | CO::FAST, -4, 1, 1}.HFUNC(IncrBy)
            << CI{"CMS.QUERY", CO::READONLY | CO::FAST, -3, 1, 1}.HFUNC(Query)
            << CI{"CMS.INFO", CO::READONLY | CO::FAST, 2, 1, 1}.HFUNC(Info)
            << CI{"CMS.MERGE", CO::DENYOOM | CO::VARIADIC_KEYS, -4, 3, 3}.HFUNC(Merge);
}

}  // namespace dfly
