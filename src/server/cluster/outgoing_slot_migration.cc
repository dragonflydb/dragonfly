// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/outgoing_slot_migration.h"

#include <absl/flags/flag.h>

#include <atomic>

#include "absl/cleanup/cleanup.h"
#include "base/logging.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/streamer.h"
#include "server/server_family.h"

ABSL_DECLARE_FLAG(int, source_connect_timeout_ms);

ABSL_DECLARE_FLAG(uint16_t, admin_port);

using namespace std;
using namespace util;

namespace dfly {

class OutgoingMigration::SliceSlotMigration {
 public:
  SliceSlotMigration(DbSlice* slice, SlotSet slots, uint32_t sync_id, journal::Journal* journal,
                     Context* cntx, io::Sink* dest)
      : streamer_(slice, std::move(slots), sync_id, journal, cntx) {
    state_.store(MigrationState::C_SYNC, memory_order_relaxed);
    sync_fb_ = fb2::Fiber("slot-snapshot", [this, dest] { streamer_.Start(dest); });
  }

  void Cancel() {
    streamer_.Cancel();
  }

  void WaitForSnapshotFinished() {
    sync_fb_.JoinIfNeeded();
  }

  void Finalize() {
    streamer_.SendFinalize();
    state_.store(MigrationState::C_FINISHED, memory_order_relaxed);
  }

  MigrationState GetState() const {
    return state_.load(memory_order_relaxed);
  }

 private:
  RestoreStreamer streamer_;
  // Atomic only for simple read operation, writes - from the same thread, reads - from any thread
  atomic<MigrationState> state_ = MigrationState::C_CONNECTING;
  fb2::Fiber sync_fb_;
};

OutgoingMigration::OutgoingMigration(std::string ip, uint16_t port, SlotRanges slots,
                                     uint32_t sync_id, Context::ErrHandler err_handler,
                                     ServerFamily* sf)
    : ProtocolClient(ip, port),
      host_ip_(ip),
      port_(port),
      sync_id_(sync_id),
      slots_(slots),
      cntx_(err_handler),
      slot_migrations_(shard_set->size()),
      server_family_(sf) {
}

OutgoingMigration::~OutgoingMigration() {
  main_sync_fb_.JoinIfNeeded();
}

void OutgoingMigration::StartFlow(journal::Journal* journal, io::Sink* dest) {
  EngineShard* shard = EngineShard::tlocal();
  DbSlice* slice = &shard->db_slice();

  const auto shard_id = slice->shard_id();

  MigrationState state = MigrationState::C_NO_STATE;
  {
    std::lock_guard lck(flows_mu_);
    slot_migrations_[shard_id] =
        std::make_unique<SliceSlotMigration>(slice, slots_, sync_id_, journal, &cntx_, dest);
    state = GetStateImpl();
  }

  if (state == MigrationState::C_SYNC) {
    main_sync_fb_ = fb2::Fiber("outgoing_migration", &OutgoingMigration::SyncFb, this);
  }
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
  MigrationState min_state = MigrationState::C_MAX_INVALID;
  for (const auto& slot_migration : slot_migrations_) {
    if (slot_migration) {
      min_state = std::min(min_state, slot_migration->GetState());
    } else {
      min_state = MigrationState::C_NO_STATE;
    }
  }
  return min_state;
}

void OutgoingMigration::SyncFb() {
  for (auto& migration : slot_migrations_) {
    migration->WaitForSnapshotFinished();
  }
  VLOG(1) << "Migrations snapshot is finihed";

  // TODO implement blocking on migrated slots only

  bool is_block_active = true;
  auto is_pause_in_progress = [&is_block_active] { return is_block_active; };
  auto pause_fb_opt =
      Pause(server_family_->GetListeners(), nullptr, ClientPause::WRITE, is_pause_in_progress);

  if (!pause_fb_opt) {
    LOG(WARNING) << "Cluster migration finalization time out";
  }

  absl::Cleanup cleanup([&is_block_active, &pause_fb_opt] {
    is_block_active = false;
    pause_fb_opt->JoinIfNeeded();
  });

  auto cb = [this](util::ProactorBase* pb) {
    if (const auto* shard = EngineShard::tlocal(); shard) {
      // TODO add error processing to move back into STABLE_SYNC state
      Finalize(shard->shard_id());
    }
  };

  shard_set->pool()->AwaitFiberOnAll(std::move(cb));

  // TODO add ACK here and config update
}

std::error_code OutgoingMigration::Start(ConnectionContext* cntx) {
  VLOG(1) << "Starting outgoing migration";

  auto check_connection_error = [&cntx](error_code ec, const char* msg) -> error_code {
    if (ec) {
      cntx->SendError(absl::StrCat(msg, ec.message()));
    }
    return ec;
  };

  VLOG(1) << "Resolving host DNS";
  error_code ec = ResolveHostDns();
  RETURN_ON_ERR(check_connection_error(ec, "could not resolve host dns"));

  VLOG(1) << "Connecting to source";
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_);
  RETURN_ON_ERR(check_connection_error(ec, "couldn't connect to source"));

  VLOG(1) << "Migration initiating";
  ResetParser(false);
  auto port = absl::GetFlag(FLAGS_admin_port);
  auto cmd = absl::StrCat("DFLYMIGRATE INIT ", sync_id_, " ", port, " ", slot_migrations_.size());
  for (const auto& s : slots_) {
    absl::StrAppend(&cmd, " ", s.start, " ", s.end);
  }
  RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
  LOG_IF(ERROR, !CheckRespIsSimpleReply("OK")) << facade::ToSV(LastResponseArgs().front().GetBuf());

  // sync_fb_ = fb2::Fiber("main_migration", &ClusterSlotMigration::MainMigrationFb, this);

  return {};
}

}  // namespace dfly
