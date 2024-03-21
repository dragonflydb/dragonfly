// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_slot_migration.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>

#include "base/logging.h"
#include "server/cluster/cluster_family.h"
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

ClusterSlotMigration::ClusterSlotMigration(ClusterFamily* cl_fm, string host_ip, uint16_t port,
                                           Service* se, SlotRanges slots)
    : ProtocolClient(std::move(host_ip), port),
      cluster_family_(cl_fm),
      service_(*se),
      slots_(std::move(slots)) {
  local_sync_id_ = next_local_sync_id.fetch_add(1);
}

ClusterSlotMigration::~ClusterSlotMigration() {
  sync_fb_.JoinIfNeeded();
}

error_code ClusterSlotMigration::Init(uint32_t sync_id, uint32_t shards_num) {
  VLOG(1) << "Init slot migration";

  state_ = MigrationState::C_CONNECTING;

  sync_id_ = sync_id;
  source_shards_num_ = shards_num;

  VLOG(1) << "Resolving host DNS";
  error_code ec = ResolveHostDns();
  if (ec)
    return ec;

  VLOG(1) << "Connecting to source";
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_);
  if (ec)
    return ec;

  ResetParser(false);

  sync_fb_ = fb2::Fiber("main_migration", &ClusterSlotMigration::MainMigrationFb, this);

  return ec;
}

ClusterSlotMigration::Info ClusterSlotMigration::GetInfo() const {
  const auto& ctx = server();
  return {ctx.host, ctx.port};
}

bool ClusterSlotMigration::IsFinalized() const {
  return std::all_of(shard_flows_.begin(), shard_flows_.end(),
                     [](const auto& el) { return el->IsFinalized(); });
}

void ClusterSlotMigration::Stop() {
  for (auto& flow : shard_flows_) {
    flow->Cancel();
  }
}

void ClusterSlotMigration::MainMigrationFb() {
  VLOG(1) << "Main migration fiber started " << sync_id_;

  state_ = MigrationState::C_SYNC;

  // TODO add reconnection code
  if (auto ec = InitiateSlotsMigration(); ec) {
    LOG(WARNING) << "Error syncing with " << server().Description() << " " << ec << " "
                 << ec.message();
  }

  if (IsFinalized()) {
    state_ = MigrationState::C_FINISHED;

    auto cmd = absl::StrCat("DFLYMIGRATE ACK ", sync_id_);
    VLOG(1) << "send " << cmd;

    auto err = SendCommandAndReadResponse(cmd);
    auto success = !err && CheckRespIsSimpleReply("OK");

    LOG_IF(WARNING, !success) << ToSV(LastResponseArgs().front().GetBuf());

    cluster_family_->FinalizeIncomingMigration(local_sync_id_);
  }
}

std::error_code ClusterSlotMigration::InitiateSlotsMigration() {
  shard_flows_.resize(source_shards_num_);
  for (unsigned i = 0; i < source_shards_num_; ++i) {
    shard_flows_[i].reset(
        new ClusterShardMigration(server(), local_sync_id_, i, sync_id_, &service_));
  }

  absl::Cleanup cleanup = [this]() {
    // We do the following operations regardless of outcome.
    for (auto& flow : shard_flows_) {
      flow->JoinFlow();
    }
  };

  // Switch to new error handler that closes flow sockets.
  auto err_handler = [this](const auto& ge) mutable {
    // Make sure the flows are not in a state transition
    lock_guard lk{flows_op_mu_};

    // Unblock all sockets.
    DefaultErrorHandler(ge);
    for (auto& flow : shard_flows_)
      flow->Cancel();
  };
  RETURN_ON_ERR(cntx_.SwitchErrorHandler(std::move(err_handler)));

  std::atomic_uint32_t synced_shards = 0;
  auto partition = Partition(source_shards_num_);
  auto shard_cb = [&](unsigned index, auto*) {
    for (auto id : partition[index]) {
      auto ec = shard_flows_[id]->StartSyncFlow(&cntx_);
      if (!ec) {
        ++synced_shards;
      } else {
        cntx_.ReportError(ec);
      }
    }
  };
  // Lock to prevent the error handler from running instantly
  // while the flows are in a mixed state.
  lock_guard lk{flows_op_mu_};
  shard_set->pool()->AwaitFiberOnAll(std::move(shard_cb));

  VLOG(1) << synced_shards << " from " << source_shards_num_ << " shards were set flow";
  if (synced_shards != source_shards_num_) {
    cntx_.ReportError(std::make_error_code(errc::state_not_recoverable),
                      "incorrect shards num, only for tests");
  }

  return cntx_.GetError();
}

}  // namespace dfly
