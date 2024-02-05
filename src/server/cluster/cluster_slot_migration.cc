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

}  // namespace

ClusterSlotMigration::ClusterSlotMigration(string host_ip, uint16_t port, Service* se,
                                           std::vector<ClusterConfig::SlotRange> slots)
    : ProtocolClient(move(host_ip), port), service_(*se), slots_(std::move(slots)) {
}

ClusterSlotMigration::~ClusterSlotMigration() {
  sync_fb_.JoinIfNeeded();
}

error_code ClusterSlotMigration::Start(ConnectionContext* cntx) {
  VLOG(1) << "Starting slot migration";

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

  state_ = MigrationState::C_CONNECTING;

  VLOG(1) << "Greeting";
  ec = Greet();
  RETURN_ON_ERR(check_connection_error(ec, "couldn't greet source "));

  sync_fb_ = fb2::Fiber("main_migration", &ClusterSlotMigration::MainMigrationFb, this);

  return {};
}

error_code ClusterSlotMigration::Greet() {
  ResetParser(false);
  VLOG(1) << "greeting message handling";
  RETURN_ON_ERR(SendCommandAndReadResponse("PING"));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("PONG"));

  auto port = absl::GetFlag(FLAGS_port);
  auto cmd = absl::StrCat("DFLYMIGRATE CONF ", port);
  for (const auto& s : slots_) {
    absl::StrAppend(&cmd, " ", s.start, " ", s.end);
  }
  VLOG(1) << "Migration command: " << cmd;
  RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
  // Response is: sync_id, num_shards
  if (!CheckRespFirstTypes({RespExpr::INT64, RespExpr::INT64}))
    return make_error_code(errc::bad_message);

  sync_id_ = get<int64_t>(LastResponseArgs()[0].u);
  source_shards_num_ = get<int64_t>(LastResponseArgs()[1].u);

  return error_code{};
}

ClusterSlotMigration::Info ClusterSlotMigration::GetInfo() const {
  const auto& ctx = server();
  return {ctx.host, ctx.port};
}

void ClusterSlotMigration::SetStableSyncForFlow(uint32_t flow) {
  DCHECK(shard_flows_.size() > flow);
  shard_flows_[flow]->SetStableSync();

  if (std::all_of(shard_flows_.begin(), shard_flows_.end(),
                  [](const auto& el) { return el->IsStableSync(); })) {
    state_ = MigrationState::C_STABLE_SYNC;
  }
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
  VLOG(1) << "Main migration fiber started";

  state_ = MigrationState::C_FULL_SYNC;

  // TODO add reconnection code
  if (auto ec = InitiateSlotsMigration(); ec) {
    LOG(WARNING) << "Error syncing with " << server().Description() << " " << ec << " "
                 << ec.message();
  }

  if (IsFinalized()) {
    state_ = MigrationState::C_FINISHED;
  }
}

std::error_code ClusterSlotMigration::InitiateSlotsMigration() {
  shard_flows_.resize(source_shards_num_);
  for (unsigned i = 0; i < source_shards_num_; ++i) {
    shard_flows_[i].reset(new ClusterShardMigration(server(), i, sync_id_, &service_));
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
