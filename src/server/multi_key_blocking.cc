// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/multi_key_blocking.h"

#include <chrono>

namespace dfly {

OpResult<ShardFFResult> FindFirst(Transaction* trans, int req_obj_type) {
  using FFResult = std::pair<PrimeKey, unsigned>;  // key, argument index.
  VLOG(2) << "FindFirst::Find " << trans->DebugId();

  // Holds Find results: (iterator to a found key, and its index in the passed arguments).
  // See DbSlice::FindFirst for more details.
  // spans all the shards for now.
  std::vector<OpResult<FFResult>> find_res(shard_set->size());
  std::fill(find_res.begin(), find_res.end(), OpStatus::KEY_NOTFOUND);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    OpResult<std::pair<PrimeIterator, unsigned>> ff_res =
        shard->db_slice().FindFirst(t->GetDbContext(), args, req_obj_type);

    if (ff_res) {
      FFResult ff_result(ff_res->first->first.AsRef(), ff_res->second);
      find_res[shard->shard_id()] = std::move(ff_result);
    } else {
      find_res[shard->shard_id()] = ff_res.status();
    }

    return OpStatus::OK;
  };

  trans->Execute(std::move(cb), false);

  uint32_t min_arg_indx = UINT32_MAX;
  ShardFFResult shard_result;

  // We iterate over all results to find the key with the minimal arg_index
  // after reversing the arg indexing permutation.
  for (size_t sid = 0; sid < find_res.size(); ++sid) {
    const auto& fr = find_res[sid];
    auto status = fr.status();
    if (status == OpStatus::KEY_NOTFOUND)
      continue;
    if (status == OpStatus::WRONG_TYPE) {
      return status;
    }
    CHECK(fr);

    const auto& it_pos = fr.value();

    size_t arg_indx = trans->ReverseArgIndex(sid, it_pos.second);
    if (arg_indx < min_arg_indx) {
      min_arg_indx = arg_indx;
      shard_result.sid = sid;

      // we do not dereference the key, do not extract the string value, so it it
      // ok to just move it. We can not dereference it due to limitations of SmallString
      // that rely on thread-local data-structure for pointer translation.
      shard_result.key = it_pos.first.AsRef();
    }
  }

  if (shard_result.sid == kInvalidSid) {
    return OpStatus::KEY_NOTFOUND;
  }

  return OpResult<ShardFFResult>{std::move(shard_result)};
}

}  // namespace dfly
