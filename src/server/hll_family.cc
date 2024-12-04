// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hll_family.h"

#include "server/acl/acl_commands_def.h"

extern "C" {
#include "redis/hyperloglog.h"
}

#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

template <typename T>
void HandleOpValueResult(const OpResult<T>& result, SinkReplyBuilder* builder) {
  static_assert(std::is_integral<T>::value,
                "we are only handling types that are integral types in the return types from "
                "here");
  if (result) {
    builder->SendLong(result.value());
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        builder->SendError(kWrongTypeErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        builder->SendError(kOutOfMemory);
        break;
      case OpStatus::INVALID_VALUE:
        builder->SendError(HllFamily::kInvalidHllErr);
        break;
      default:
        builder->SendLong(0);  // in case we don't have the value we should just send 0
        break;
    }
  }
}

HllBufferPtr StringToHllPtr(string_view hll) {
  return {.hll = (unsigned char*)hll.data(), .size = hll.size()};
}

void ConvertToDenseIfNeeded(string* hll) {
  if (isValidHLL(StringToHllPtr(*hll)) == HLL_VALID_SPARSE) {
    string new_hll;
    new_hll.resize(getDenseHllSize());
    int result = convertSparseToDenseHll(StringToHllPtr(*hll), StringToHllPtr(new_hll));
    DCHECK_EQ(result, 0);
    *hll = std::move(new_hll);
  }
}

OpResult<int> AddToHll(const OpArgs& op_args, string_view key, CmdArgList values) {
  auto& db_slice = op_args.GetDbSlice();

  string hll;

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& res = *op_res;
  if (res.is_new) {
    hll.resize(getSparseHllInitSize());
    initSparseHll(StringToHllPtr(hll));
  } else if (res.it->second.ObjType() != OBJ_STRING) {
    return OpStatus::WRONG_TYPE;
  } else {
    res.it->second.GetString(&hll);
  }
  if (isValidHLL(StringToHllPtr(hll)) == HLL_INVALID) {
    return OpStatus::INVALID_VALUE;
  }

  int updated = 0;
  bool is_sparse = isValidHLL(StringToHllPtr(hll)) == HLL_VALID_SPARSE;
  sds hll_sds;
  if (is_sparse) {
    hll_sds = sdsnewlen(hll.data(), hll.size());
  }

  for (const auto& value : values) {
    int added;
    if (is_sparse) {
      // Inserting to sparse hll might extend it.
      // We can't use std::string with sds
      // `promoted` will be assigned 1 if sparse hll was promoted to dense
      int promoted = 0;
      added = pfadd_sparse(&hll_sds, (unsigned char*)value.data(), value.size(), &promoted);
      if (promoted == 1) {
        is_sparse = false;
        hll = string{hll_sds, sdslen(hll_sds)};
        sdsfree(hll_sds);
        DCHECK_EQ(isValidHLL(StringToHllPtr(hll)), HLL_VALID_DENSE);
      }
    } else {
      added = pfadd_dense(StringToHllPtr(hll), (unsigned char*)value.data(), value.size());
    }
    if (added < 0) {
      return OpStatus::INVALID_VALUE;
    }
    updated += added;
  }

  if (is_sparse) {
    hll = string{hll_sds, sdslen(hll_sds)};
    sdsfree(hll_sds);
  }
  res.it->second.SetString(hll);
  return std::min(updated, 1);
}

void PFAdd(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return AddToHll(t->GetOpArgs(shard), key, args);
  };

  OpResult<int> res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cmd_cntx.rb);
}

OpResult<int64_t> CountHllsSingle(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();

  auto it = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STRING);
  if (it.ok()) {
    string hll;
    string_view hll_view = it.value()->second.GetSlice(&hll);

    switch (isValidHLL(StringToHllPtr(hll_view))) {
      case HLL_VALID_DENSE:
        break;
      case HLL_VALID_SPARSE:
        // Even in the case of a read - we still want to convert the hll to dense format, as it
        // could originate in Redis (like in replication or rdb load).
        hll = hll_view;
        ConvertToDenseIfNeeded(&hll);
        hll_view = hll;
        break;
      case HLL_INVALID:
      default:
        return OpStatus::INVALID_VALUE;
    }

    return pfcountSingle(StringToHllPtr(hll_view));
  } else if (it.status() == OpStatus::WRONG_TYPE) {
    return it.status();
  } else {
    // Non existing keys count as 0.
    return 0;
  }
}

OpResult<vector<string>> ReadValues(const OpArgs& op_args, const ShardArgs& keys) {
  try {
    vector<string> values;
    for (string_view key : keys) {
      auto it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_STRING);
      if (it.ok()) {
        string hll;
        it.value()->second.GetString(&hll);
        ConvertToDenseIfNeeded(&hll);
        if (isValidHLL(StringToHllPtr(hll)) != HLL_VALID_DENSE) {
          return OpStatus::INVALID_VALUE;
        } else {
          values.push_back(std::move(hll));
        }
      } else if (it.status() == OpStatus::WRONG_TYPE) {
        return OpStatus::WRONG_TYPE;
      }
    }
    return values;
  } catch (const std::bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }
}

vector<HllBufferPtr> ConvertShardVector(const vector<vector<string>>& hlls) {
  vector<HllBufferPtr> ptrs;
  ptrs.reserve(hlls.size());
  for (auto& shard_hlls : hlls) {
    for (auto& hll : shard_hlls) {
      ptrs.push_back(StringToHllPtr(hll));
    }
  }
  return ptrs;
}

OpResult<int64_t> PFCountMulti(CmdArgList args, const CommandContext& cmd_cntx) {
  vector<vector<string>> hlls;
  hlls.resize(shard_set->size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    ShardArgs shard_args = t->GetShardArgs(shard->shard_id());
    auto result = ReadValues(t->GetOpArgs(shard), shard_args);
    if (result.ok()) {
      hlls[sid] = std::move(result.value());
    }
    return result.status();
  };

  cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  vector<HllBufferPtr> ptrs = ConvertShardVector(hlls);
  int64_t pf_count = pfcountMulti(ptrs.data(), ptrs.size());
  if (pf_count < 0) {
    return OpStatus::INVALID_VALUE;
  } else {
    return pf_count;
  }
}

void PFCount(CmdArgList args, const CommandContext& cmd_cntx) {
  if (args.size() == 1) {
    string_view key = ArgS(args, 0);
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return CountHllsSingle(t->GetOpArgs(shard), key);
    };

    OpResult<int64_t> res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
    HandleOpValueResult(res, cmd_cntx.rb);
  } else {
    HandleOpValueResult(PFCountMulti(args, cmd_cntx), cmd_cntx.rb);
  }
}

OpResult<int> PFMergeInternal(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder) {
  vector<vector<string>> hlls;
  hlls.resize(shard_set->size());

  atomic_bool success = true;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    ShardArgs shard_args = t->GetShardArgs(shard->shard_id());
    auto result = ReadValues(t->GetOpArgs(shard), shard_args);
    if (result.ok()) {
      hlls[sid] = std::move(result.value());
    } else {
      success = false;
    }
    return result.status();
  };

  tx->Execute(std::move(cb), false);

  if (!success) {
    tx->Conclude();
    return OpStatus::INVALID_VALUE;
  }

  vector<HllBufferPtr> ptrs = ConvertShardVector(hlls);

  string hll;
  hll.resize(getDenseHllSize());
  createDenseHll(StringToHllPtr(hll));
  int result = pfmerge(ptrs.data(), ptrs.size(), StringToHllPtr(hll));

  auto set_cb = [&](Transaction* t, EngineShard* shard) {
    string_view key = ArgS(args, 0);
    const OpArgs& op_args = t->GetOpArgs(shard);
    auto& db_slice = op_args.GetDbSlice();
    auto op_res = db_slice.AddOrFind(t->GetDbContext(), key);
    RETURN_ON_BAD_STATUS(op_res);
    auto& res = *op_res;
    res.it->second.SetString(hll);

    if (op_args.shard->journal()) {
      RecordJournal(op_args, "SET", ArgSlice{key, hll});
    }

    return OpStatus::OK;
  };
  tx->Execute(std::move(set_cb), true);

  return result;
}

void PFMerge(CmdArgList args, const CommandContext& cmd_cntx) {
  OpResult<int> result = PFMergeInternal(args, cmd_cntx.tx, cmd_cntx.rb);
  if (result.ok()) {
    if (result.value() == 0) {
      cmd_cntx.rb->SendOk();
    } else {
      cmd_cntx.rb->SendError(HllFamily::kInvalidHllErr);
    }
  } else {
    HandleOpValueResult(result, cmd_cntx.rb);
  }
}

}  // namespace

namespace acl {
constexpr uint32_t kPFAdd = WRITE | HYPERLOGLOG | FAST;
constexpr uint32_t kPFCount = READ | HYPERLOGLOG | SLOW;
constexpr uint32_t kPFMerge = WRITE | HYPERLOGLOG | SLOW;
}  // namespace acl

void HllFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;
  registry->StartFamily();
  *registry << CI{"PFADD", CO::WRITE, -3, 1, 1, acl::kPFAdd}.SetHandler(PFAdd)
            << CI{"PFCOUNT", CO::READONLY, -2, 1, -1, acl::kPFCount}.SetHandler(PFCount)
            << CI{"PFMERGE", CO::WRITE | CO::NO_AUTOJOURNAL, -2, 1, -1, acl::kPFMerge}.SetHandler(
                   PFMerge);
}

const char HllFamily::kInvalidHllErr[] = "Key is not a valid HyperLogLog string value.";

}  // namespace dfly
