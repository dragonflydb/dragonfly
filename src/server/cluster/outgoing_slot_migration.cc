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

namespace dfly::cluster {

class OutgoingMigration::SliceSlotMigration : private ProtocolClient {
 public:
  SliceSlotMigration(DbSlice* slice, ServerContext server_context, SlotSet slots,
                     journal::Journal* journal)
      : ProtocolClient(server_context), streamer_(slice, std::move(slots), journal, &cntx_) {
  }

  void Sync(const std::string& node_id, uint32_t shard_id) {
    VLOG(1) << "Connecting to source node_id " << node_id << " shard_id " << shard_id;
    auto timeout = absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms;
    if (auto ec = ConnectAndAuth(timeout, &cntx_); ec) {
      cntx_.ReportError(GenericError(ec, "Couldn't connect to source."));
      return;
    }

    ResetParser(/*server_mode=*/false);

    std::string cmd = absl::StrCat("DFLYMIGRATE FLOW ", node_id, " ", shard_id);
    VLOG(1) << "cmd: " << cmd;

    if (auto ec = SendCommandAndReadResponse(cmd); ec) {
      cntx_.ReportError(GenericError(ec, cmd));
      return;
    }

    if (!CheckRespIsSimpleReply("OK")) {
      LOG(WARNING) << "Incorrect response for FLOW cmd: "
                   << ToSV(LastResponseArgs().front().GetBuf());
      cntx_.ReportError("Incorrect response for FLOW cmd");
      return;
    }

    if (cancelled_) {
      return;
    }

    streamer_.Start(Sock());
  }

  void Cancel() {
    streamer_.Cancel();
    cancelled_ = true;
  }

  void Finalize() {
    streamer_.SendFinalize();
  }

  const dfly::GenericError GetError() const {
    return cntx_.GetError();
  }

 private:
  RestoreStreamer streamer_;
  bool cancelled_ = false;
};

OutgoingMigration::OutgoingMigration(MigrationInfo info, ClusterFamily* cf, ServerFamily* sf)
    : ProtocolClient(info.ip, info.port),
      migration_info_(std::move(info)),
      slot_migrations_(shard_set->size()),
      server_family_(sf),
      cf_(cf) {
}

OutgoingMigration::~OutgoingMigration() {
  main_sync_fb_.JoinIfNeeded();
}

void OutgoingMigration::Finish(bool is_error) {
  std::lock_guard lk(finish_mu_);
  if (state_.load() != MigrationState::C_FINISHED) {
    const auto new_state = is_error ? MigrationState::C_ERROR : MigrationState::C_FINISHED;
    state_.store(new_state);
    shard_set->pool()->AwaitFiberOnAll([this](util::ProactorBase* pb) {
      if (const auto* shard = EngineShard::tlocal(); shard) {
        auto& migration = slot_migrations_[shard->shard_id()];
        if (migration != nullptr) {
          migration->Cancel();
        }
      }
    });
  }
}

MigrationState OutgoingMigration::GetState() const {
  return state_.load();
}

void OutgoingMigration::SyncFb() {
  // we retry starting migration until "cancel" is happened
  while (true) {
    auto state = state_.load();
    if (state == MigrationState::C_FINISHED || state == MigrationState::C_ERROR) {
      break;
    }

    state_.store(MigrationState::C_CONNECTING);
    last_error_ = cntx_.GetError();
    cntx_.Reset(nullptr);

    if (last_error_) {
      LOG(ERROR) << last_error_.Format();
      // if error is happened on the previous attempt we wait for some time and try again
      ThisFiber::SleepFor(1000ms);
    }

    VLOG(1) << "Connecting to source";
    auto timeout = absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms;
    if (auto ec = ConnectAndAuth(timeout, &cntx_); ec) {
      cntx_.ReportError(GenericError(ec, "Couldn't connect to source."));
      continue;
    }

    VLOG(1) << "Migration initiating";
    ResetParser(false);
    auto cmd = absl::StrCat("DFLYMIGRATE INIT ", cf_->MyID(), " ", slot_migrations_.size());
    for (const auto& s : migration_info_.slot_ranges) {
      absl::StrAppend(&cmd, " ", s.start, " ", s.end);
    }

    if (auto ec = SendCommandAndReadResponse(cmd); ec) {
      cntx_.ReportError(GenericError(ec, "Could send INIT command."));
      continue;
    }

    if (!CheckRespIsSimpleReply("OK")) {
      cntx_.ReportError(GenericError(std::string(ToSV(LastResponseArgs().front().GetBuf()))));
      continue;
    }

    state_.store(MigrationState::C_SYNC);

    shard_set->pool()->AwaitFiberOnAll([this](util::ProactorBase* pb) {
      if (auto* shard = EngineShard::tlocal(); shard) {
        server_family_->journal()->StartInThread();
        slot_migrations_[shard->shard_id()] = std::make_unique<SliceSlotMigration>(
            &shard->db_slice(), server(), migration_info_.slot_ranges, server_family_->journal());
        auto& migration = *slot_migrations_[shard->shard_id()];
        migration.Sync(cf_->MyID(), shard->shard_id());
        if (migration.GetError()) {
          Finish(true);
        }
      }
    });

    if (CheckFlowsForErrors()) {
      continue;
    }

    VLOG(1) << "Migrations snapshot is finished";

    long attempt = 0;
    while (state_.load() != MigrationState::C_FINISHED && !FinalyzeMigration(++attempt)) {
      // process commands that were on pause and try again
      ThisFiber::SleepFor(500ms);
    }
    if (CheckFlowsForErrors()) {
      continue;
    }
    break;
  }
}

bool OutgoingMigration::FinalyzeMigration(long attempt) {
  // if it's not the 1st attempt and flows are work correctly we try to reconnect and ACK one more
  // time
  if (attempt > 1) {
    if (CheckFlowsForErrors()) {
      Finish(true);
      return true;
    }
    VLOG(1) << "Reconnecting to source";
    auto timeout = absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms;
    if (auto ec = ConnectAndAuth(timeout, &cntx_); ec) {
      cntx_.ReportError(GenericError(ec, "Couldn't connect to source."));
      return false;
    }
  }
  // TODO implement blocking on migrated slots only
  bool is_block_active = true;
  auto is_pause_in_progress = [&is_block_active] { return is_block_active; };
  auto pause_fb_opt = Pause(server_family_->GetNonPriviligedListeners(), nullptr,
                            ClientPause::WRITE, is_pause_in_progress);

  if (!pause_fb_opt) {
    LOG(WARNING) << "Cluster migration finalization time out";
  }

  absl::Cleanup cleanup([&is_block_active, &pause_fb_opt]() {
    is_block_active = false;
    pause_fb_opt->JoinIfNeeded();
  });

  auto cb = [this](util::ProactorBase* pb) {
    if (const auto* shard = EngineShard::tlocal(); shard) {
      VLOG(1) << "FINALIZE outgoing migration" << shard->shard_id();
      slot_migrations_[shard->shard_id()]->Finalize();
    }
  };

  shard_set->pool()->AwaitFiberOnAll(std::move(cb));

  auto cmd = absl::StrCat("DFLYMIGRATE ACK ", cf_->MyID(), " ", attempt);
  VLOG(1) << "send " << cmd;

  auto err = SendCommand(cmd);
  LOG_IF(WARNING, err) << err;

  if (err) {
    LOG(WARNING) << "Error during sending DFLYMIGRATE ACK: " << err.message();
    return false;
  }

  if (auto resp = ReadRespReply(absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms)); !resp) {
    LOG(WARNING) << resp.error();
    return false;
  }

  if (!CheckRespFirstTypes({RespExpr::INT64})) {
    LOG(WARNING) << "Incorrect response type: "
                 << facade::ToSV(LastResponseArgs().front().GetBuf());
    return false;
  }

  const auto attempt_res = get<int64_t>(LastResponseArgs().front().u);
  if (attempt_res == kInvalidAttempt) {
    return false;
  }

  auto is_error = CheckFlowsForErrors();
  Finish(is_error);
  if (!is_error) {
    cf_->UpdateConfig(migration_info_.slot_ranges, false);
    VLOG(1) << "Config is updated for " << cf_->MyID();
  }
  return true;
}

void OutgoingMigration::Start() {
  VLOG(1) << "Resolving host DNS for outgoing migration";
  if (error_code ec = ResolveHostDns(); ec) {
    cntx_.ReportError(GenericError(ec, "Could not resolve host dns."));
    return;
  }

  main_sync_fb_ = fb2::Fiber("outgoing_migration", &OutgoingMigration::SyncFb, this);
}

bool OutgoingMigration::CheckFlowsForErrors() {
  for (const auto& flow : slot_migrations_) {
    if (flow->GetError()) {
      cntx_.ReportError(flow->GetError());
      return true;
    }
  }
  return false;
}
}  // namespace dfly::cluster
