// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <cmath>
#include <mutex>

#include "core/cms.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_families.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

// CMS.INITBYDIM key width depth
OpStatus OpInitByDim(const OpArgs& op_args, string_view key, uint32_t width, uint32_t depth) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CMS);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  PrimeValue& pv = op_res->it->second;
  CMS* cms = CompactObj::AllocateMR<CMS>(width, depth, CompactObj::memory_resource());
  pv.SetCMS(cms);

  return OpStatus::OK;
}

// CMS.INITBYPROB key error probability
OpStatus OpInitByProb(const OpArgs& op_args, string_view key, double error, double probability) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CMS);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  // Calculate width and depth from error and probability
  // Width = ceil(e / error) where e is Euler's number
  // Depth = ceil(ln(1 / probability))
  uint32_t width = static_cast<uint32_t>(std::ceil(std::exp(1.0) / error));
  uint32_t depth = static_cast<uint32_t>(std::ceil(std::log(1.0 / probability)));

  // Ensure minimum dimensions
  width = std::max(width, 1u);
  depth = std::max(depth, 1u);

  PrimeValue& pv = op_res->it->second;
  CMS* cms = CompactObj::AllocateMR<CMS>(width, depth, CompactObj::memory_resource());
  pv.SetCMS(cms);

  return OpStatus::OK;
}

// CMS.INCRBY key item increment [item increment ...]
OpResult<vector<int64_t>> OpIncrBy(const OpArgs& op_args, string_view key, CmdArgList args) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_CMS);

  if (!op_res)
    return op_res.status();

  CMS* cms = op_res->it->second.GetCMS();
  vector<int64_t> results;
  results.reserve(args.size() / 2);

  for (size_t i = 0; i < args.size(); i += 2) {
    string_view item = ToSV(args[i]);
    string_view incr_str = ToSV(args[i + 1]);

    int64_t increment;
    if (!absl::SimpleAtoi(incr_str, &increment)) {
      return OpStatus::INVALID_INT;
    }

    int64_t count = cms->IncrBy(item, increment);
    results.push_back(count);
  }

  return results;
}

// CMS.QUERY key item [item ...]
OpResult<vector<int64_t>> OpQuery(const OpArgs& op_args, string_view key, CmdArgList items) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CMS);

  if (!op_res)
    return op_res.status();

  const CMS* cms = op_res.value()->second.GetCMS();
  vector<int64_t> results;
  results.reserve(items.size());

  for (auto& item : items) {
    results.push_back(cms->Query(ToSV(item)));
  }

  return results;
}

// CMS.INFO key
struct CmsInfo {
  uint32_t width;
  uint32_t depth;
  int64_t count;
};

OpResult<CmsInfo> OpInfo(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CMS);

  if (!op_res)
    return op_res.status();

  const CMS* cms = op_res.value()->second.GetCMS();
  return CmsInfo{cms->Width(), cms->Depth(), cms->Count()};
}

// Command handlers

void CmdInitByDim(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();
  uint32_t width = parser.Next<uint32_t>();
  uint32_t depth = parser.Next<uint32_t>();

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (auto err = parser.TakeError(); err) {
    return rb->SendError(err.MakeReply());
  }

  if (width == 0) {
    return rb->SendError("CMS: width must be a positive integer");
  }
  if (depth == 0) {
    return rb->SendError("CMS: depth must be a positive integer");
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInitByDim(t->GetOpArgs(shard), key, width, depth);
  };

  OpStatus res = cmd_cntx->tx->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("CMS: key already exists");
  }
  if (res == OpStatus::OK) {
    return rb->SendOk();
  }
  return rb->SendError(res);
}

void CmdInitByProb(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view key = parser.Next();
  double error_rate = parser.Next<double>();
  double probability = parser.Next<double>();

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (auto err = parser.TakeError(); err) {
    return rb->SendError(err.MakeReply());
  }

  if (error_rate <= 0 || error_rate >= 1) {
    return rb->SendError("CMS: error must be a value between 0 and 1 exclusive");
  }
  if (probability <= 0 || probability >= 1) {
    return rb->SendError("CMS: probability must be a value between 0 and 1 exclusive");
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInitByProb(t->GetOpArgs(shard), key, error_rate, probability);
  };

  OpStatus res = cmd_cntx->tx->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("CMS: key already exists");
  }
  if (res == OpStatus::OK) {
    return rb->SendOk();
  }
  return rb->SendError(res);
}

void CmdIncrBy(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  // Must have pairs of item/increment
  if (args.size() % 2 != 0) {
    return rb->SendError(kSyntaxErr);
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, args);
  };

  OpResult<vector<int64_t>> res = cmd_cntx->tx->ScheduleSingleHopT(std::move(cb));

  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      return rb->SendError("CMS: key does not exist");
    }
    if (res.status() == OpStatus::INVALID_INT) {
      return rb->SendError("CMS: Cannot parse number");
    }
    return rb->SendError(res.status());
  }

  rb->StartArray(res->size());
  for (int64_t count : *res) {
    rb->SendLong(count);
  }
}

void CmdQuery(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpQuery(t->GetOpArgs(shard), key, args);
  };

  OpResult<vector<int64_t>> res = cmd_cntx->tx->ScheduleSingleHopT(std::move(cb));

  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      return rb->SendError("CMS: key does not exist");
    }
    return rb->SendError(res.status());
  }

  rb->StartArray(res->size());
  for (int64_t count : *res) {
    rb->SendLong(count);
  }
}

void CmdInfo(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInfo(t->GetOpArgs(shard), key);
  };

  OpResult<CmsInfo> res = cmd_cntx->tx->ScheduleSingleHopT(std::move(cb));

  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      return rb->SendError("CMS: key does not exist");
    }
    return rb->SendError(res.status());
  }

  rb->StartArray(6);
  rb->SendBulkString("width");
  rb->SendLong(res->width);
  rb->SendBulkString("depth");
  rb->SendLong(res->depth);
  rb->SendBulkString("count");
  rb->SendLong(res->count);
}

// CMS.MERGE destination numkeys source [source ...] [WEIGHTS weight [weight ...]]
void CmdMerge(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser(args);
  string_view dest_key = parser.Next();
  uint32_t num_keys = parser.Next<uint32_t>();

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (auto err = parser.TakeError(); err) {
    return rb->SendError(err.MakeReply());
  }

  if (num_keys == 0) {
    return rb->SendError("CMS: wrong number of arguments");
  }

  // Parse source keys
  vector<string_view> source_keys;
  source_keys.reserve(num_keys);
  for (uint32_t i = 0; i < num_keys && parser.HasNext(); ++i) {
    source_keys.push_back(parser.Next());
  }

  if (source_keys.size() != num_keys) {
    return rb->SendError("CMS: wrong number of keys");
  }

  // Parse optional WEIGHTS
  vector<int64_t> weights(num_keys, 1);
  if (parser.HasNext()) {
    string_view weights_keyword = parser.Next();
    if (!absl::EqualsIgnoreCase(weights_keyword, "WEIGHTS")) {
      return rb->SendError(kSyntaxErr);
    }

    for (uint32_t i = 0; i < num_keys && parser.HasNext(); ++i) {
      int64_t weight = parser.Next<int64_t>();
      if (auto err = parser.TakeError(); err) {
        return rb->SendError(err.MakeReply());
      }
      weights[i] = weight;
    }
  }

  // Structure to hold CMS data including full counter arrays
  struct CmsData {
    uint32_t width = 0;
    uint32_t depth = 0;
    int64_t count = 0;
    vector<int64_t> counters;  // Full counter array for cross-shard merge
    bool found = false;        // Track if source key was found
  };

  vector<CmsData> cms_data(num_keys);
  atomic<OpStatus> error_status{OpStatus::OK};
  mutex data_mutex;  // Protect concurrent writes to cms_data

  // Destination info (checked during read phase)
  atomic<bool> dest_found{false};
  atomic<uint32_t> dest_width{0};
  atomic<uint32_t> dest_depth{0};

  // First phase: Read all source CMS data and verify destination exists
  auto read_cb = [&](Transaction* t, EngineShard* shard) {
    auto& db_slice = t->GetOpArgs(shard).GetDbSlice();
    ShardArgs shard_args = t->GetShardArgs(shard->shard_id());

    for (string_view skey : shard_args) {
      // Check if this is the destination key
      if (skey == dest_key) {
        auto op_res = db_slice.FindReadOnly(t->GetDbContext(), dest_key, OBJ_CMS);
        if (op_res) {
          const CMS* cms = op_res.value()->second.GetCMS();
          dest_found.store(true, memory_order_relaxed);
          dest_width.store(cms->Width(), memory_order_relaxed);
          dest_depth.store(cms->Depth(), memory_order_relaxed);
        } else if (op_res.status() != OpStatus::KEY_NOTFOUND) {
          error_status.store(op_res.status(), memory_order_relaxed);
        }
        continue;
      }

      // Find which source key this is
      for (uint32_t i = 0; i < num_keys; ++i) {
        if (source_keys[i] != skey) {
          continue;
        }

        auto op_res = db_slice.FindReadOnly(t->GetDbContext(), skey, OBJ_CMS);

        if (!op_res) {
          if (op_res.status() != OpStatus::KEY_NOTFOUND) {
            error_status.store(op_res.status(), memory_order_relaxed);
          }
          continue;
        }

        const CMS* cms = op_res.value()->second.GetCMS();

        // Copy CMS data including full counter array
        lock_guard<mutex> lock(data_mutex);
        cms_data[i].width = cms->Width();
        cms_data[i].depth = cms->Depth();
        cms_data[i].count = cms->Count();
        cms_data[i].found = true;
        // Copy counters for cross-shard merge
        const auto& src_counters = cms->Counters();
        cms_data[i].counters.assign(src_counters.begin(), src_counters.end());
      }
    }
    return OpStatus::OK;
  };

  cmd_cntx->tx->Execute(std::move(read_cb), false);

  // Check for errors
  OpStatus stored_error = error_status.load(memory_order_relaxed);
  if (stored_error != OpStatus::OK) {
    cmd_cntx->tx->Conclude();
    return rb->SendError(stored_error);
  }

  // Check destination exists
  if (!dest_found.load(memory_order_relaxed)) {
    cmd_cntx->tx->Conclude();
    return rb->SendError("CMS: key does not exist");
  }

  // Check all source keys were found (Redis requires all sources to exist)
  for (uint32_t i = 0; i < num_keys; ++i) {
    if (!cms_data[i].found) {
      cmd_cntx->tx->Conclude();
      return rb->SendError("CMS: key does not exist");
    }
  }

  // Get dimensions from first source (all must match)
  uint32_t expected_width = cms_data[0].width;
  uint32_t expected_depth = cms_data[0].depth;

  // Verify all sources have same dimensions
  for (uint32_t i = 1; i < num_keys; ++i) {
    if (cms_data[i].width != expected_width || cms_data[i].depth != expected_depth) {
      cmd_cntx->tx->Conclude();
      return rb->SendError("CMS: width/depth of source keys must match");
    }
  }

  // Verify destination has same dimensions
  if (dest_width.load(memory_order_relaxed) != expected_width ||
      dest_depth.load(memory_order_relaxed) != expected_depth) {
    cmd_cntx->tx->Conclude();
    return rb->SendError("CMS: width/depth of source keys must match");
  }

  // Second phase: Update destination with merged data
  auto write_cb = [&](Transaction* t, EngineShard* shard) {
    auto& db_slice = t->GetOpArgs(shard).GetDbSlice();

    auto op_res = db_slice.FindMutable(t->GetDbContext(), dest_key, OBJ_CMS);
    if (!op_res) {
      // Should not happen since we verified in read phase
      return OpStatus::OK;
    }

    CMS* dest_cms = op_res->it->second.GetCMS();

    // Reset destination before merging (Redis replaces, doesn't add)
    dest_cms->Reset();

    // Merge all source CMS data from stored counter arrays
    for (uint32_t i = 0; i < num_keys; ++i) {
      dest_cms->MergeCounters(cms_data[i].counters, cms_data[i].count, weights[i]);
    }

    return OpStatus::OK;
  };

  cmd_cntx->tx->ScheduleSingleHop(std::move(write_cb));
  rb->SendOk();
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterCmsFamily(CommandRegistry* registry) {
  registry->StartFamily();

  *registry
      << CI{"CMS.INITBYDIM", CO::JOURNALED | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::CMS}.HFUNC(
             InitByDim)
      << CI{"CMS.INITBYPROB", CO::JOURNALED | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::CMS}.HFUNC(
             InitByProb)
      << CI{"CMS.INCRBY", CO::JOURNALED | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::CMS}.HFUNC(IncrBy)
      << CI{"CMS.QUERY", CO::READONLY | CO::FAST, -3, 1, 1, acl::CMS}.HFUNC(Query)
      << CI{"CMS.INFO", CO::READONLY | CO::FAST, 2, 1, 1, acl::CMS}.HFUNC(Info)
      << CI{"CMS.MERGE", CO::JOURNALED | CO::DENYOOM, -4, 1, -1, acl::CMS}.HFUNC(Merge);
}

}  // namespace dfly
