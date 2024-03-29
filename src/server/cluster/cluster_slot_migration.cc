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

ClusterSlotMigration::ClusterSlotMigration(string host_ip, uint16_t port, Service* se,
                                           SlotRanges slots, uint32_t shards_num)
    : ProtocolClient(std::move(host_ip), port),
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

ClusterSlotMigration::Info ClusterSlotMigration::GetInfo() const {
  const auto& ctx = server();
  return {ctx.host, ctx.port};
}

bool ClusterSlotMigration::IsFinalized() const {
  return std::all_of(shard_flows_.begin(), shard_flows_.end(),
                     [](const auto& el) { return el->IsFinalized(); });
}

void ClusterSlotMigration::Join() {
  bc_->Wait();
  state_ = MigrationState::C_FINISHED;
}

void ClusterSlotMigration::StartFlow(uint32_t shard, io::Source* source) {
  VLOG(1) << "Start flow for shard: " << shard;

  shard_flows_[shard]->Start(&cntx_, source);
  bc_->Dec();

  // lock_guard lk{flows_op_mu_};

  // if (!sync_fb_.IsJoinable() && std::all_of(shard_flows_.begin(), shard_flows_.end(),
  //                                           [](const auto& m) { return m->IsFinalized(); })) {
  //   sync_fb_ = fb2::Fiber("main_migration", &ClusterSlotMigration::MainMigrationFb, this);
  // }
}

// void ClusterSlotMigration::MainMigrationFb() {
//   VLOG(1) << "Main migration fiber started ";

//   // if (IsFinalized()) {
//   //   // TODO move ack code to outgoing_slot_migration
//   //   ResolveHostDns();
//   //   ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_);

//   //   ResetParser(false);

//   //   auto cmd = absl::StrCat("DFLYMIGRATE ACK ", sync_id_);
//   //   VLOG(1) << "send " << cmd;

//   //   auto err = SendCommandAndReadResponse(cmd);
//   //   LOG_IF(WARNING, err) << err;

//   //   VLOG(1) << "SendCommandAndReadResponse " << cmd;

//   //   if (!err) {
//   //     LOG_IF(WARNING, !CheckRespIsSimpleReply("OK")) <<
//   ToSV(LastResponseArgs().front().GetBuf());
//   //   }

//     state_ = MigrationState::C_FINISHED;

//   }
// }

}  // namespace dfly
