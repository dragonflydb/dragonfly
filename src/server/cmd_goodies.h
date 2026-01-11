#pragma once

#include "server/async_cmd.h"
#include "server/engine_shard.h"
#include "server/transaction.h"

namespace dfly::cmd {

template <typename F>
requires std::invocable<F, const ShardArgs&, const OpArgs&>
inline auto single_hop(F&& f) {
  return single_hop([f = std::forward<F>(f)](const Transaction* t, EngineShard* shard) {
    return f(t->GetShardArgs(shard->shard_id()), t->GetOpArgs(shard));
  });
}

}  // namespace dfly::cmd
