// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/outgoing_slot_migration.h"

#include "server/db_slice.h"
#include "server/journal/streamer.h"

namespace dfly {

class OutgoingMigration::Flow {
 public:
  Flow(DbSlice* slice, SlotSet slots, uint32_t sync_id, journal::Journal* journal, Context* cntx)
      : streamer(slice, std::move(slots), sync_id, journal, cntx) {
  }

  void Start(io::Sink* dest) {
    streamer.Start(dest);
    state = MigrationState::C_FULL_SYNC;
  }

  MigrationState GetState() const {
    return state == MigrationState::C_FULL_SYNC && streamer.IsStableSync()
               ? MigrationState::C_STABLE_SYNC
               : state;
  }

 private:
  RestoreStreamer streamer;
  MigrationState state = MigrationState::C_CONNECTING;
};

OutgoingMigration::OutgoingMigration(std::uint32_t flows_num, std::string ip, uint16_t port,
                                     std::vector<ClusterConfig::SlotRange> slots,
                                     Context::ErrHandler err_handler)
    : host_ip(ip), port(port), slots(slots), cntx(err_handler), flows(flows_num) {
}

OutgoingMigration::~OutgoingMigration() = default;

void OutgoingMigration::StartFlow(DbSlice* slice, uint32_t sync_id, journal::Journal* journal,
                                  io::Sink* dest) {
  // TODO refactor: maybe we can use vector<slotRange> instead of SlotSet
  SlotSet sset;
  for (const auto& slot_range : slots) {
    for (auto i = slot_range.start; i <= slot_range.end; ++i)
      sset.insert(i);
  }

  const auto shard_id = slice->shard_id();

  std::scoped_lock lck(flows_mu_);
  flows[shard_id] = std::make_unique<Flow>(slice, std::move(sset), sync_id, journal, &cntx);
  flows[shard_id]->Start(dest);
}

MigrationState OutgoingMigration::GetState() {
  std::scoped_lock lck(flows_mu_);
  MigrationState min_state = MigrationState::C_STABLE_SYNC;
  for (const auto& flow : flows) {
    if (flow)
      min_state = std::min(min_state, flow->GetState());
  }
  return min_state;
}

}  // namespace dfly
