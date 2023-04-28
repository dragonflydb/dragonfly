// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hll_family.h"

extern "C" {
#include "redis/hyperloglog.h"
}

#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

template <typename T> void HandleOpValueResult(const OpResult<T>& result, ConnectionContext* cntx) {
  static_assert(std::is_integral<T>::value,
                "we are only handling types that are integral types in the return types from "
                "here");
  if (result) {
    (*cntx)->SendLong(result.value());
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        (*cntx)->SendError(kOutOfMemory);
        break;
      case OpStatus::INVALID_VALUE:
        (*cntx)->SendError(HllFamily::kInvalidHllErr);
        break;
      default:
        (*cntx)->SendLong(0);  // in case we don't have the value we should just send 0
        break;
    }
  }
}

HllBufferPtr StringToHllPtr(string* hll) {
  return {.hll = (unsigned char*)hll->data(), .size = hll->size()};
}

void ConvertToDenseIfNeeded(string* hll) {
  if (isValidHLL(StringToHllPtr(hll)) == HLL_VALID_SPARSE) {
    string new_hll;
    new_hll.resize(getDenseHllSize());
    int result = convertSparseToDenseHll(StringToHllPtr(hll), StringToHllPtr(&new_hll));
    DCHECK_EQ(result, 0);
    *hll = std::move(new_hll);
  }
}

OpResult<int> AddToHll(const OpArgs& op_args, string_view key, CmdArgList values) {
  auto& db_slice = op_args.shard->db_slice();

  string hll;

  try {
    auto [it, inserted] = db_slice.AddOrFind(op_args.db_cntx, key);
    if (inserted) {
      hll.resize(getDenseHllSize());
      createDenseHll(StringToHllPtr(&hll));
    } else if (it->second.ObjType() != OBJ_STRING) {
      return OpStatus::WRONG_TYPE;
    } else {
      it->second.GetString(&hll);
      ConvertToDenseIfNeeded(&hll);
    }

    int updated = 0;
    for (const auto& value : values) {
      int added = pfadd(StringToHllPtr(&hll), (unsigned char*)value.data(), value.size());
      if (added < 0) {
        return OpStatus::INVALID_VALUE;
      }
      updated += added;
    }

    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
    it->second.SetString(hll);
    db_slice.PostUpdate(op_args.db_cntx.db_index, it, key, !inserted);

    return std::min(updated, 1);
  } catch (const std::bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }
}

void PFAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return AddToHll(t->GetOpArgs(shard), key, args);
  };

  Transaction* trans = cntx->transaction;
  OpResult<int> res = trans->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cntx);
}

OpResult<int64_t> CountHllsSingle(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();

  OpResult<PrimeIterator> it = db_slice.Find(op_args.db_cntx, key, OBJ_STRING);
  if (it.ok()) {
    string hll;
    // TODO: We should provide a GetView() method in compact object to avoid some allocations:
    // string_view GetView(string& scratch);
    it.value()->second.GetString(&hll);

    // Even in the case of a read - we still want to convert the hll to dense format, as it could
    // originate in Redis (like in replication or rdb load).
    ConvertToDenseIfNeeded(&hll);

    if (isValidHLL(StringToHllPtr(&hll)) != HLL_VALID_DENSE) {
      return OpStatus::INVALID_VALUE;
    }

    return pfcountSingle(StringToHllPtr(&hll));
  } else if (it.status() == OpStatus::WRONG_TYPE) {
    return it.status();
  } else {
    // Non existing keys count as 0.
    return 0;
  }
}

vector<OpResult<string>> ReadValues(const OpArgs& op_args, ArgSlice keys) {
  vector<OpResult<string>> values;
  for (size_t i = 0; i < keys.size(); ++i) {
    OpResult<PrimeIterator> it =
        op_args.shard->db_slice().Find(op_args.db_cntx, keys[i], OBJ_STRING);
    if (it.ok()) {
      string hll;
      it.value()->second.GetString(&hll);
      ConvertToDenseIfNeeded(&hll);
      if (isValidHLL(StringToHllPtr(&hll)) != HLL_VALID_DENSE) {
        values.push_back(OpStatus::INVALID_VALUE);
      } else {
        values.push_back(std::move(hll));
      }
    } else if (it.status() == OpStatus::WRONG_TYPE) {
      values.push_back(OpStatus::WRONG_TYPE);
    }
  }
  return values;
}

OpResult<int64_t> PFCountMulti(CmdArgList args, ConnectionContext* cntx) {
  vector<vector<OpResult<string>>> hlls;
  hlls.resize(shard_set->size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    ArgSlice shard_args = t->GetShardArgs(shard->shard_id());
    hlls[sid] = ReadValues(t->GetOpArgs(shard), shard_args);
    return OpStatus::OK;
  };

  Transaction* trans = cntx->transaction;
  trans->ScheduleSingleHop(std::move(cb));

  vector<HllBufferPtr> ptrs;
  ptrs.reserve(hlls.size());
  for (auto& shard_hlls : hlls) {
    for (auto& hll : shard_hlls) {
      if (!hll.ok()) {
        return hll.status();
      }
      ptrs.push_back(StringToHllPtr(&hll.value()));
    }
  }
  int64_t pf_count = pfcountMulti(ptrs.data(), ptrs.size());
  if (pf_count < 0) {
    return OpStatus::INVALID_VALUE;
  } else {
    return pf_count;
  }
}

void PFCount(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() == 1) {
    string_view key = ArgS(args, 0);
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return CountHllsSingle(t->GetOpArgs(shard), key);
    };

    Transaction* trans = cntx->transaction;
    OpResult<int64_t> res = trans->ScheduleSingleHopT(std::move(cb));
    HandleOpValueResult(res, cntx);
  } else {
    HandleOpValueResult(PFCountMulti(args, cntx), cntx);
  }
}

}  // namespace

void HllFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"PFADD", CO::WRITE, -3, 1, 1, 1}.SetHandler(PFAdd)
            << CI{"PFCOUNT", CO::WRITE, -2, 1, -1, 1}.SetHandler(PFCount);
}

const char HllFamily::kInvalidHllErr[] = "Key is not a valid HyperLogLog string value.";

}  // namespace dfly
