// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/outgoing_slot_migration.h"

#include <absl/flags/flag.h>

#include <atomic>

#include "absl/cleanup/cleanup.h"
#include "base/logging.h"
#include "cluster_family.h"
#include "cluster_utility.h"
#include "facade/socket_utils.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/streamer.h"
#include "server/main_service.h"
#include "server/server_family.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(int, slot_migration_connection_timeout_ms, 2000,
          "Connection creating timeout for migration operations");
ABSL_FLAG(int, migration_finalization_timeout_ms, 30000,
          "Timeout for migration finalization operation");

using namespace std;
using namespace facade;
using namespace util;

namespace dfly::cluster {

class OutgoingMigration::SliceSlotMigration : private ProtocolClient {
 public:
  SliceSlotMigration(DbSlice* slice, ServerContext server_context, SlotSet slots,
                     journal::Journal* journal, OutgoingMigration* om)
      : ProtocolClient(server_context), streamer_(slice, std::move(slots), journal, &exec_st_) {
    exec_st_.SwitchErrorHandler([om](auto ge) { om->Finish(std::move(ge)); });
  }

  ~SliceSlotMigration() {
    Cancel();
    exec_st_.JoinErrorHandler();
  }

  // Send DFLYMIGRATE FLOW
  void PrepareFlow(const std::string& node_id) {
    uint32_t shard_id = EngineShard::tlocal()->shard_id();

    VLOG(1) << "Connecting to source node_id " << node_id << " shard_id " << shard_id;
    auto timeout = absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms;
    if (auto ec = ConnectAndAuth(timeout, &exec_st_); ec) {
      LOG(WARNING) << "Couldn't connect to source node_id " << node_id << " shard_id " << shard_id
                   << ": " << ec.message()
                   << ", socket state: " + GetSocketInfo(Sock()->native_handle());
      exec_st_.ReportError(GenericError(ec, "Couldn't connect to source."));
      return;
    }

    ResetParser(RedisParser::Mode::CLIENT);

    std::string cmd = absl::StrCat("DFLYMIGRATE FLOW ", node_id, " ", shard_id);
    VLOG(1) << "cmd: " << cmd;

    if (auto ec = SendCommandAndReadResponse(cmd); ec) {
      exec_st_.ReportError(GenericError(ec, cmd));
      return;
    }

    if (!CheckRespIsSimpleReply("OK")) {
      exec_st_.ReportError(absl::StrCat("Incorrect response for FLOW cmd: ",
                                        ToSV(LastResponseArgs().front().GetBuf())));
      return;
    }
  }

  // Register db_slice and journal change listeners
  void PrepareSync() {
    streamer_.Start(Sock());
  }

  // Run restore streamer
  void RunSync() {
    streamer_.Run();
  }

  void Cancel() {
    // Close socket for clean disconnect.
    CloseSocket();
    streamer_.Cancel();
  }

  void Finalize(long attempt) {
    streamer_.SendFinalize(attempt);
  }

  const dfly::GenericError GetError() const {
    return exec_st_.GetError();
  }

 private:
  RestoreStreamer streamer_;
};

OutgoingMigration::OutgoingMigration(MigrationInfo info, ClusterFamily* cf, ServerFamily* sf)
    : ProtocolClient(info.node_info.ip, info.node_info.port),
      migration_info_(std::move(info)),
      slot_migrations_(shard_set->size()),
      server_family_(sf),
      cf_(cf),
      tx_(new Transaction{sf->service().FindCmd("DFLYCLUSTER")}) {
  tx_->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {});
}

OutgoingMigration::~OutgoingMigration() {
  main_sync_fb_.JoinIfNeeded();

  exec_st_.JoinErrorHandler();
  // Destroy each flow in its dedicated thread, because we could be the last
  // owner of the db tables
  OnAllShards([](auto& migration) { migration.reset(); });
}

bool OutgoingMigration::ChangeState(MigrationState new_state) {
  util::fb2::LockGuard lk(state_mu_);
  if (state_ == MigrationState::C_FINISHED) {
    return false;
  }

  state_ = new_state;
  return true;
}

void OutgoingMigration::OnAllShards(
    std::function<void(std::unique_ptr<SliceSlotMigration>&)> func) {
  shard_set->pool()->AwaitFiberOnAll([this, &func](util::ProactorBase* pb) {
    if (const auto* shard = EngineShard::tlocal(); shard) {
      func(slot_migrations_[shard->shard_id()]);
    }
  });
}

void OutgoingMigration::Finish(GenericError error) {
  auto next_state = MigrationState::C_FINISHED;
  if (error) {
    // If OOM error move to FATAL, non-recoverable  state
    if (error == errc::not_enough_memory) {
      next_state = MigrationState::C_FATAL;
    } else {
      next_state = MigrationState::C_ERROR;
      exec_st_.ReportError(std::move(error));
    }
    LOG(WARNING) << "Finish outgoing migration for " << cf_->MyID() << ": "
                 << migration_info_.node_info.id << " with error: " << error.Format();

  } else {
    LOG(INFO) << "Finish outgoing migration for " << cf_->MyID() << ": "
              << migration_info_.node_info.id;
  }

  bool should_cancel_flows = false;
  {
    util::fb2::LockGuard lk(state_mu_);
    switch (state_) {
      case MigrationState::C_FATAL:
      case MigrationState::C_FINISHED:
        return;  // Already finished, nothing else to do

      case MigrationState::C_CONNECTING:
        should_cancel_flows = false;
        break;

      case MigrationState::C_SYNC:
      case MigrationState::C_ERROR:
        should_cancel_flows = true;
        break;
    }
    state_ = next_state;
  }

  if (next_state == MigrationState::C_FATAL) {
    // Fatal state stop any further processing of migration so we need to update error here
    SetLastError(error);
  }

  if (should_cancel_flows) {
    OnAllShards([](auto& migration) {
      CHECK(migration != nullptr);
      migration->Cancel();
    });
    exec_st_.JoinErrorHandler();
  }
}

MigrationState OutgoingMigration::GetState() const {
  util::fb2::LockGuard lk(state_mu_);
  return state_;
}

void OutgoingMigration::SyncFb() {
  VLOG(1) << "Starting outgoing migration fiber for migration " << migration_info_.ToString();

  const absl::Time start_time = absl::Now();

  // we retry starting migration until "cancel" is happened
  while (GetState() != MigrationState::C_FINISHED) {
    if (!ChangeState(MigrationState::C_CONNECTING)) {
      break;
    }

    if (exec_st_.IsError()) {
      ResetError();
      ThisFiber::SleepFor(500ms);  // wait some time before next retry
    }

    VLOG(1) << "Connecting to target node";
    auto timeout = absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms;
    if (auto ec = ConnectAndAuth(timeout, &exec_st_); ec) {
      LOG(WARNING) << "Can't connect to target node " << server().Description()
                   << " for migration: " << ec.message()
                   << ", socket state: " + GetSocketInfo(Sock()->native_handle());
      exec_st_.ReportError(GenericError(ec, "Couldn't connect to source."));
      continue;
    }

    VLOG(1) << "Migration initiating";
    ResetParser(RedisParser::Mode::CLIENT);
    auto cmd = absl::StrCat("DFLYMIGRATE INIT ", cf_->MyID(), " ", slot_migrations_.size());
    for (const auto& s : migration_info_.slot_ranges) {
      absl::StrAppend(&cmd, " ", s.start, " ", s.end);
    }

    if (auto ec = SendCommandAndReadResponse(cmd); ec) {
      LOG(WARNING) << "Could not send INIT command to " << server().Description()
                   << " for migration: " << ec.message()
                   << ", socket state: " + GetSocketInfo(Sock()->native_handle());
      exec_st_.ReportError(GenericError(ec, "Could not send INIT command."));
      continue;
    }

    if (!CheckRespIsSimpleReply("OK")) {
      // Break outgoing migration if INIT from incoming node responded with OOM. Usually this will
      // happen on second iteration after first failed with OOM. Sending second INIT is required to
      // cleanup slots on incoming slot migration node.
      if (CheckRespSimpleError(kIncomingMigrationOOM)) {
        Finish(GenericError{std::make_error_code(errc::not_enough_memory),
                            std::string(kIncomingMigrationOOM)});
        break;
      }
      if (CheckRespIsSimpleReply(kUnknownMigration)) {
        const absl::Duration passed = absl::Now() - start_time;
        // we provide 30 seconds to distribute the config to all nodes to avoid extra errors
        // reporting
        if (passed >= absl::Milliseconds(30000)) {
          exec_st_.ReportError(GenericError(LastResponseArgs().front().GetString()));
        } else {
          ThisFiber::SleepFor(500ms);  // to prevent too many attempts
        }
      } else {
        exec_st_.ReportError(GenericError(LastResponseArgs().front().GetString()));
      }
      continue;
    }

    OnAllShards([this](auto& migration) {
      DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
      server_family_->journal()->StartInThread();
      migration = std::make_unique<SliceSlotMigration>(
          &db_slice, server(), migration_info_.slot_ranges, server_family_->journal(), this);
    });

    if (!ChangeState(MigrationState::C_SYNC)) {
      break;
    }

    OnAllShards([this](auto& migration) { migration->PrepareFlow(cf_->MyID()); });
    if (!exec_st_.IsRunning()) {
      continue;
    }

    // Global transactional cut for migration to register db_slice and journal
    // listeners
    {
      Transaction::Guard tg{tx_.get()};
      OnAllShards([](auto& migration) { migration->PrepareSync(); });
    }

    if (!exec_st_.IsRunning()) {
      continue;
    }

    OnAllShards([](auto& migration) { migration->RunSync(); });

    if (!exec_st_.IsRunning()) {
      continue;
    }

    long attempt = 0;
    while (GetState() != MigrationState::C_FINISHED && !FinalizeMigration(++attempt)) {
      // Break loop and don't sleep in case of C_FATAL
      if (GetState() == MigrationState::C_FATAL) {
        break;
      }
      // Process commands that were on pause and try again
      VLOG(1) << "Waiting for migration to finalize...";
      ThisFiber::SleepFor(500ms);
    }
    if (!exec_st_.IsRunning()) {
      continue;
    }
    break;
  }

  VLOG(1) << "Exiting outgoing migration fiber for migration " << migration_info_.ToString();
}

bool OutgoingMigration::FinalizeMigration(long attempt) {
  // if it's not the 1st attempt and flows are work correctly we try to
  // reconnect and ACK one more time
  LOG(INFO) << "Finalize migration for " << cf_->MyID() << " : " << migration_info_.node_info.id
            << " attempt " << attempt;
  if (attempt > 1) {
    if (!exec_st_.IsRunning()) {
      return true;
    }
    auto timeout = absl::GetFlag(FLAGS_slot_migration_connection_timeout_ms) * 1ms;
    if (auto ec = ConnectAndAuth(timeout, &exec_st_); ec) {
      LOG(WARNING) << "Couldn't connect to " << cf_->MyID() << " : " << migration_info_.node_info.id
                   << " attempt " << attempt << ": " << ec.message()
                   << ", socket state: " + GetSocketInfo(Sock()->native_handle());
      return false;
    }
  }

  // Migration finalization has to be done via client pause because commands need to
  // be blocked on coordinator level to avoid intializing transactions with stale cluster slot info
  // TODO implement blocking on migrated slots only
  bool is_block_active = true;
  auto is_pause_in_progress = [&is_block_active] { return is_block_active; };
  auto pause_fb_opt =
      dfly::Pause(server_family_->GetNonPriviligedListeners(), &namespaces->GetDefaultNamespace(),
                  nullptr, ClientPause::ALL, is_pause_in_progress);

  DCHECK(pause_fb_opt);
  if (!pause_fb_opt) {
    auto err = absl::StrCat("Migration finalization time out ", cf_->MyID(), " : ",
                            migration_info_.node_info.id, " attempt ", attempt);

    LOG(WARNING) << err;
    SetLastError(std::move(err));
  }

  absl::Cleanup cleanup([&is_block_active, &pause_fb_opt]() {
    if (pause_fb_opt) {
      is_block_active = false;
      pause_fb_opt->JoinIfNeeded();
    }
  });

  LOG(INFO) << "FINALIZE flows for " << cf_->MyID() << " : " << migration_info_.node_info.id;
  OnAllShards([attempt](auto& migration) { migration->Finalize(attempt); });

  auto cmd = absl::StrCat("DFLYMIGRATE ACK ", cf_->MyID(), " ", attempt);
  VLOG(1) << "send " << cmd;

  if (auto err = SendCommand(cmd); err) {
    LOG(WARNING) << "Error during sending DFLYMIGRATE ACK to " << server().Description() << ": "
                 << err.message() << ", socket state: " + GetSocketInfo(Sock()->native_handle());
    return false;
  }

  const absl::Time start = absl::Now();
  const absl::Duration timeout =
      absl::Milliseconds(absl::GetFlag(FLAGS_migration_finalization_timeout_ms));
  while (true) {
    const absl::Time now = absl::Now();
    const absl::Duration passed = now - start;
    if (passed >= timeout) {
      LOG(WARNING) << "Timeout fot ACK " << cf_->MyID() << " : " << migration_info_.node_info.id
                   << " attempt " << attempt;
      return false;
    }

    if (auto resp = ReadRespReply(absl::ToInt64Milliseconds(passed - timeout)); !resp) {
      LOG(WARNING) << "Error reading response to ACK command from " << server().Description()
                   << ": " << resp.error()
                   << ", socket state: " + GetSocketInfo(Sock()->native_handle());
      return false;
    }

    // Check OOM from incoming slot migration on ACK request
    if (CheckRespSimpleError(kIncomingMigrationOOM)) {
      Finish(GenericError{std::make_error_code(errc::not_enough_memory),
                          std::string(kIncomingMigrationOOM)});
      return false;
    }

    if (!CheckRespFirstTypes({RespExpr::INT64})) {
      LOG(WARNING) << "Incorrect response type for " << cf_->MyID() << " : "
                   << migration_info_.node_info.id << " attempt " << attempt
                   << " msg: " << facade::ToSV(LastResponseArgs().front().GetBuf());
      return false;
    }

    if (const auto res = get<int64_t>(LastResponseArgs().front().u); res == attempt) {
      break;
    } else {
      LOG(WARNING) << "Incorrect attempt payload " << cf_->MyID() << " : "
                   << migration_info_.node_info.id << ", sent " << attempt << " received " << res;
    }
  }

  if (!exec_st_.GetError()) {
    Finish();
    keys_number_ = cluster::GetKeyCount(migration_info_.slot_ranges);
    cf_->ApplyMigrationSlotRangeToConfig(migration_info_.node_info.id, migration_info_.slot_ranges,
                                         false);
  }
  return true;
}

void OutgoingMigration::Start() {
  VLOG(1) << "Resolving host DNS for outgoing migration";
  if (error_code ec = ResolveHostDns(); ec) {
    LOG(WARNING) << "Could not resolve host DNS for outgoing migration to "
                 << server().Description() << ": " << ec.message();
    exec_st_.ReportError(GenericError(ec, "Could not resolve host dns."));
    return;
  }

  main_sync_fb_ = fb2::Fiber("outgoing_migration", &OutgoingMigration::SyncFb, this);
}

size_t OutgoingMigration::GetKeyCount() const {
  util::fb2::LockGuard lk(state_mu_);
  if (state_ == MigrationState::C_FINISHED) {
    return keys_number_;
  }
  return cluster::GetKeyCount(migration_info_.slot_ranges);
}
}  // namespace dfly::cluster
