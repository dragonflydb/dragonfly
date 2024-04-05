// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_slot_migration.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>

#include "base/logging.h"
#include "server/cluster/cluster_shard_migration.h"
#include "server/error.h"
#include "server/journal/tx_executor.h"
#include "server/main_service.h"

ABSL_FLAG(int, source_connect_timeout_ms, 20000,
          "Timeout for establishing connection to a source node");

ABSL_DECLARE_FLAG(int32_t, port);

namespace dfly {

using namespace std;
using namespace util;
using namespace facade;
using absl::GetFlag;

namespace {
// Distribute flow indices over all available threads (shard_set pool size).
vector<vector<unsigned>> Partition(unsigned num_flows) {
  vector<vector<unsigned>> partition(shard_set->pool()->size());
  for (unsigned i = 0; i < num_flows; ++i) {
    partition[i % partition.size()].push_back(i);
  }
  return partition;
}

atomic_uint32_t next_local_sync_id{1};

}  // namespace

ClusterSlotMigration::ClusterSlotMigration(string source_id, Service* se, SlotRanges slots,
                                           uint32_t shards_num)
    : source_id_(std::move(source_id)),
      service_(*se),
      slots_(std::move(slots)),
      state_(MigrationState::C_CONNECTING),
      partitions_(Partition(shards_num)),
      bc_(shards_num) {
  local_sync_id_ = next_local_sync_id.fetch_add(1);

  shard_flows_.resize(shards_num);
  for (unsigned i = 0; i < shards_num; ++i) {
    shard_flows_[i].reset(new ClusterShardMigration(local_sync_id_, i, &service_));
  }
}

ClusterSlotMigration::~ClusterSlotMigration() {
  sync_fb_.JoinIfNeeded();
}

void ClusterSlotMigration::Join() {
  bc_->Wait();
  state_ = MigrationState::C_FINISHED;
}

void ClusterSlotMigration::StartFlow(uint32_t shard, io::Source* source) {
  VLOG(1) << "Start flow for shard: " << shard;

  shard_flows_[shard]->Start(&cntx_, source);
  bc_->Dec();
}

}  // namespace dfly
