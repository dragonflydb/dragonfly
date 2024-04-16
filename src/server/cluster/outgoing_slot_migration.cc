// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/outgoing_slot_migration.h"

#include <absl/flags/flag.h>

#include <atomic>

#include "absl/cleanup/cleanup.h"
#include "base/logging.h"
#include "cluster_family.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/streamer.h"
#include "server/server_family.h"

ABSL_FLAG(int, slot_migration_connection_timeout_ms, 2000, "Timeout for network operations");

using namespace std;
using namespace facade;
using namespace util;

namespace dfly {

class OutgoingMigration::SliceSlotMigration : private ProtocolClient {
 public:
  SliceSlotMigration(DbSlice* slice, ServerContext server_context, SlotSet slots,
                     journal::Journal* journal, Context* cntx)
      : ProtocolClient(server_context), streamer_(slice, std::move(slots), journal, cntx) {
  }
  ~SliceSlotMigration() {
    sync_fb_.JoinIfNeeded();
  }

  std::error_code Start(const std::string& node_id, uint32_t shard_id) {
    RETURN_ON_ERR(
        ConnectAndAuth(absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms, &cntx_));
    ResetParser(/*server_mode=*/false);

    std::string cmd = absl::StrCat("DFLYMIGRATE FLOW ", node_id, " ", shard_id);
    VLOG(1) << "cmd: " << cmd;

    RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
    LOG_IF(WARNING, !CheckRespIsSimpleReply("OK")) << ToSV(LastResponseArgs().front().GetBuf());

    sync_fb_ = fb2::Fiber("slot-snapshot", [this] { streamer_.Start(Sock()); });
    return {};
  }

  void Cancel() {
    streamer_.Cancel();
  }

  void WaitForSnapshotFinished() {
    sync_fb_.JoinIfNeeded();
  }

  void Finalize() {
    streamer_.SendFinalize();
  }

 private:
  RestoreStreamer streamer_;
  fb2::Fiber sync_fb_;
};

OutgoingMigration::OutgoingMigration(MigrationInfo info, ClusterFamily* cf,
                                     Context::ErrHandler err_handler, ServerFamily* sf)
    : ProtocolClient(info.ip, info.port),
      migration_info_(std::move(info)),
      cntx_(err_handler),
      slot_migrations_(shard_set->size()),
      server_family_(sf),
      cf_(cf) {
}

OutgoingMigration::~OutgoingMigration() {
  main_sync_fb_.JoinIfNeeded();
}

MigrationState OutgoingMigration::GetState() const {
  return state_.load();
}

void OutgoingMigration::SyncFb() {
  state_.store(MigrationState::C_SYNC);
  auto start_cb = [this](util::ProactorBase* pb) {
    if (auto* shard = EngineShard::tlocal(); shard) {
      server_family_->journal()->StartInThread();
      slot_migrations_[shard->shard_id()] = std::make_unique<SliceSlotMigration>(
          &shard->db_slice(), server(), migration_info_.slot_ranges, server_family_->journal(),
          &cntx_);
      slot_migrations_[shard->shard_id()]->Start(cf_->MyID(), shard->shard_id());
    }
  };

  state_.store(MigrationState::C_SYNC);

  shard_set->pool()->AwaitFiberOnAll(std::move(start_cb));

  for (auto& migration : slot_migrations_) {
    migration->WaitForSnapshotFinished();
  }
  VLOG(1) << "Migrations snapshot is finihed";

  // TODO implement blocking on migrated slots only

  long attempt = 0;
  while (!FinishMigration(++attempt)) {
    // process commands that were on pause and try again
    ThisFiber::SleepFor(500ms);
  }
}

bool OutgoingMigration::FinishMigration(long attempt) {
  bool is_block_active = true;
  auto is_pause_in_progress = [&is_block_active] { return is_block_active; };
  auto pause_fb_opt = Pause(server_family_->GetNonPriviligedListeners(), nullptr,
                            ClientPause::WRITE, is_pause_in_progress);

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
      VLOG(1) << "FINALIZE outgoing migration" << shard->shard_id();
      slot_migrations_[shard->shard_id()]->Finalize();
    }
  };

  shard_set->pool()->AwaitFiberOnAll(std::move(cb));

  auto cmd = absl::StrCat("DFLYMIGRATE ACK ", cf_->MyID(), " ", attempt);
  VLOG(1) << "send " << cmd;

  auto err = SendCommand(cmd);
  LOG_IF(WARNING, err) << err;

  if (!err) {
    long attempt_res = kInvalidAttempt;
    do {  // we can have response from previos time so we need to read until get response for the
          // last attempt
      auto resp = ReadRespReply(absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms));

      if (!resp) {
        LOG(WARNING) << resp.error();
        // TODO implement connection issue error processing
        return false;
      }

      if (!CheckRespFirstTypes({RespExpr::INT64})) {
        LOG(WARNING) << "Incorrect response type: "
                     << facade::ToSV(LastResponseArgs().front().GetBuf());
        return false;
      }
      attempt_res = get<int64_t>(LastResponseArgs().front().u);
      if (attempt_res == kInvalidAttempt) {
        return false;
      }
    } while (attempt_res != attempt);

    shard_set->pool()->AwaitFiberOnAll([this](util::ProactorBase* pb) {
      if (const auto* shard = EngineShard::tlocal(); shard)
        slot_migrations_[shard->shard_id()]->Cancel();
    });

    state_.store(MigrationState::C_FINISHED);
    cf_->UpdateConfig(migration_info_.slot_ranges, false);
    VLOG(1) << "Config is updated for " << cf_->MyID();
    return true;
  } else {
    // TODO implement connection issue error processing
  }
  return false;
}

std::error_code OutgoingMigration::Start(ConnectionContext* cntx) {
  VLOG(1) << "Starting outgoing migration";

  state_.store(MigrationState::C_CONNECTING);

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
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms, &cntx_);
  RETURN_ON_ERR(check_connection_error(ec, "couldn't connect to source"));

  VLOG(1) << "Migration initiating";
  ResetParser(false);
  auto cmd = absl::StrCat("DFLYMIGRATE INIT ", cf_->MyID(), " ", slot_migrations_.size());
  for (const auto& s : migration_info_.slot_ranges) {
    absl::StrAppend(&cmd, " ", s.start, " ", s.end);
  }
  RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
  LOG_IF(ERROR, !CheckRespIsSimpleReply("OK")) << facade::ToSV(LastResponseArgs().front().GetBuf());

  main_sync_fb_ = fb2::Fiber("outgoing_migration", &OutgoingMigration::SyncFb, this);

  return {};
}

}  // namespace dfly
