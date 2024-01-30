// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/outgoing_slot_migration.h"

#include "server/db_slice.h"
#include "server/journal/streamer.h"

namespace dfly {

class OutgoingMigration::SliceSlotMigration {
 public:
  SliceSlotMigration(DbSlice* slice, SlotSet slots, uint32_t sync_id, journal::Journal* journal,
                     Context* cntx, io::Sink* dest)
      : streamer_(slice, std::move(slots), sync_id, journal, cntx) {
    streamer_.Start(dest);
    state_ = MigrationState::C_FULL_SYNC;
  }

  void Cancel() {
    streamer_.Cancel();
  }

  void Finalize() {
    streamer_.SendFinalize();
  }

  MigrationState GetState() const {
    return state_ == MigrationState::C_FULL_SYNC && streamer_.IsSnapshotFinished()
               ? MigrationState::C_STABLE_SYNC
               : state_;
  }

 private:
  RestoreStreamer streamer_;
  MigrationState state_ = MigrationState::C_CONNECTING;
};

OutgoingMigration::OutgoingMigration(std::uint32_t flows_num, std::string ip, uint16_t port,
                                     std::vector<ClusterConfig::SlotRange> slots,
                                     Context::ErrHandler err_handler)
    : host_ip_(ip), port_(port), slots_(slots), cntx_(err_handler), slot_migrations_(flows_num) {
}

OutgoingMigration::~OutgoingMigration() = default;

void OutgoingMigration::StartFlow(DbSlice* slice, uint32_t sync_id, journal::Journal* journal,
                                  io::Sink* dest) {
  SlotSet sset;
  for (const auto& slot_range : slots_) {
    for (auto i = slot_range.start; i <= slot_range.end; ++i)
      sset.insert(i);
  }

  const auto shard_id = slice->shard_id();

  std::lock_guard lck(flows_mu_);
  slot_migrations_[shard_id] =
      std::make_unique<SliceSlotMigration>(slice, std::move(sset), sync_id, journal, &cntx_, dest);
}

void OutgoingMigration::Finalize(uint32_t shard_id) {
  slot_migrations_[shard_id]->Finalize();
}

void OutgoingMigration::Cancel(uint32_t shard_id) {
  slot_migrations_[shard_id]->Cancel();
}

MigrationState OutgoingMigration::GetState() const {
  std::lock_guard lck(flows_mu_);
  return GetStateImpl();
}

MigrationState OutgoingMigration::GetStateImpl() const {
  MigrationState min_state = MigrationState::C_STABLE_SYNC;
  for (const auto& slot_migration : slot_migrations_) {
    if (slot_migration)
      min_state = std::min(min_state, slot_migration->GetState());
  }
  return min_state;
}

}  // namespace dfly
