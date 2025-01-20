// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_utility.h"

#include "server/cluster/cluster_defs.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"

using namespace std;

namespace dfly::cluster {

uint64_t GetKeyCount(const SlotRanges& slots) {
  std::atomic_uint64_t keys = 0;

  shard_set->pool()->AwaitFiberOnAll([&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    uint64_t shard_keys = 0;
    for (const SlotRange& range : slots) {
      for (SlotId slot = range.start; slot <= range.end; slot++) {
        shard_keys += namespaces->GetDefaultNamespace()
                          .GetDbSlice(shard->shard_id())
                          .GetSlotStats(slot)
                          .key_count;
      }
    }
    keys.fetch_add(shard_keys);
  });

  return keys.load();
}

}  // namespace dfly::cluster
