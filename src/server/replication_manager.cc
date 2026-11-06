// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/replication_manager.h"

#include <absl/strings/str_cat.h>

#include "base/flags.h"
#include "base/logging.h"
#include "server/cluster/cluster_family.h"
#include "server/cluster_support.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/replica.h"
#include "server/server_state.h"

// TODO deprecate when flipped in production
ABSL_FLAG(bool, replicaof_no_one_start_journal, true,
          "when set, preserves journal offsets after REPLICAOF NO ONE");

namespace dfly {

using namespace std;

namespace {

bool IsMaster() {
  // We call this function on startup where tlocal() == nullptr. We handle
  // this case below.
  if (!ServerState::tlocal()) {
    return true;
  }
  return ServerState::tlocal()->is_master;
}

void SetMasterFlagOnAllThreads(bool is_master) {
  auto cb = [is_master](unsigned, auto*) { ServerState::tlocal()->is_master = is_master; };
  shard_set->pool()->AwaitBrief(cb);
}

}  // namespace

ReplicationManager::ReplicationManager(Service* service, string master_replid,
                                       function<void()> unblock_all_clients)
    : service_(service),
      master_replid_(std::move(master_replid)),
      unblock_all_clients_(std::move(unblock_all_clients)) {
}

ReplicationManager::~ReplicationManager() {
}

GenericError ReplicationManager::StartReplication(const string& host, uint16_t port,
                                                  optional<cluster::SlotRange> slot_range,
                                                  ActionOnConnectionFail on_error) {
  auto new_replica = make_shared<Replica>(host, port, service_, master_replid_, slot_range);
  GenericError ec;
  switch (on_error) {
    case ActionOnConnectionFail::kReturnOnError:
      ec = new_replica->Start();
      break;
    case ActionOnConnectionFail::kContinueReplication:
      new_replica->EnableReplication();
      break;
  };

  if (ec || new_replica->IsContextCancelled()) {
    return ec ? ec : GenericError("replication cancelled");
  }

  // Critical section.
  // 1. Stop the old replica_ if it exists
  // 2. Update all the pointers to the new replica and update master flag
  // 3. Start the main replication fiber
  util::fb2::LockGuard lk(mu_);
  optional<LastMasterSyncData> last_master_data;
  if (replica_)
    last_master_data = replica_->Stop();

  StopAllClusterReplicas();

  if (ServerState::tlocal()->gstate() == GlobalState::TAKEN_OVER)
    service_->SwitchState(GlobalState::TAKEN_OVER, GlobalState::LOADING);

  // TODO Update thread locals. That way INFO never blocks
  replica_ = new_replica;
  SetMasterFlagOnAllThreads(false);

  // Blocked writers would otherwise consume elements applied from the replication stream.
  unblock_all_clients_();

  if (on_error == ActionOnConnectionFail::kReturnOnError) {
    replica_->StartMainReplicationFiber(last_master_data);
  }

  return {};
}

void ReplicationManager::PromoteToMaster() {
  util::fb2::LockGuard lk(mu_);

  if (!IsMaster()) {
    CHECK(replica_);

    auto repl_ptr = replica_;
    if (absl::GetFlag(FLAGS_replicaof_no_one_start_journal)) {
      // Start journal and keep offsets.
      repl_ptr->StartJournalAtOwnLSN();
    }

    // flip flag before clearing replica_
    SetMasterFlagOnAllThreads(true);

    last_master_data_ = replica_->Stop();
    replica_.reset();
    StopAllClusterReplicas();
  }

  // May not switch to ACTIVE if the process is, for example, shutting down at the same time.
  service_->SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
}

GenericError ReplicationManager::TakeOver(int timeout_sec, bool save_flag) {
  // The LockGuard must precede the master check to ensure atomicity for the subsequent repl_ptr
  // check
  util::fb2::LockGuard lk(mu_);

  // We return OK, to support idempotency semantics.
  if (IsMaster())
    return {};

  auto repl_ptr = replica_;
  CHECK(repl_ptr);

  // Start journal to allow partial sync from same source master
  repl_ptr->StartJournalAtOwnLSN();

  auto info = replica_->GetSummary();
  if (!info.full_sync_done) {
    return GenericError("Full sync not done");
  }

  std::error_code res = replica_->TakeOver(timeout_sec, save_flag);
  if (res) {
    LOG(WARNING) << "Takeover failed with error: " << res << " - " << res.message();
    return GenericError(absl::StrCat("Couldn't execute takeover: ", res.message()));
  }

  LOG(INFO) << "Takeover successful, promoting this instance to master.";

  if (IsClusterEnabled()) {
    service_->cluster_family().ReconcileReplicaSlots();
  }

  last_master_data_ = replica_->Stop();
  replica_.reset();

  SetMasterFlagOnAllThreads(true);
  return {};
}

GenericError ReplicationManager::AddClusterReplica(const string& host, uint16_t port,
                                                   optional<cluster::SlotRange> slot_range) {
  util::fb2::LockGuard lk(mu_);
  if (IsMaster()) {
    return GenericError("Calling ADDREPLICAOFF allowed only after server is already a replica");
  }
  CHECK(replica_);

  auto add_replica = make_unique<Replica>(host, port, service_, master_replid_, slot_range);
  GenericError ec = add_replica->Start();
  if (ec) {
    return ec;
  }
  add_replica->StartMainReplicationFiber(nullopt);
  cluster_replicas_.push_back(std::move(add_replica));
  return {};
}

void ReplicationManager::Shutdown() {
  util::fb2::LockGuard lk(mu_);
  if (replica_) {
    replica_->Stop();
  }
  StopAllClusterReplicas();
}

bool ReplicationManager::IsReplica() const {
  util::fb2::LockGuard lk(mu_);
  return !IsMaster();
}

void ReplicationManager::StopAllClusterReplicas() {
  // Stop all cluster replication.
  for (auto& replica : cluster_replicas_) {
    replica->Stop();
    replica.reset();
  }
  cluster_replicas_.clear();
}

void ReplicationManager::Pause(bool pause) {
  util::fb2::LockGuard lk(mu_);

  // Switch to primary mode.
  if (!IsMaster()) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    repl_ptr->Pause(pause);
  }
}

optional<ReplicaOffsetInfo> ReplicationManager::GetOffsetInfo() const {
  util::fb2::LockGuard lk(mu_);

  // Switch to primary mode.
  if (!IsMaster()) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    return ReplicaOffsetInfo{repl_ptr->GetSyncId(), repl_ptr->GetReplicaOffset()};
  }
  return nullopt;
}

optional<int> ReplicationManager::GetMasterSocketUnreadBytes() const {
  util::fb2::LockGuard lk(mu_);

  if (!IsMaster()) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    return repl_ptr->GetMasterSocketUnreadBytes();
  }
  return nullopt;
}

optional<Metrics::ReplicaInfo> ReplicationManager::GetSummary() const {
  util::fb2::LockGuard lk(mu_);
  if (replica_ == nullptr) {
    return nullopt;
  }

  Metrics::ReplicaInfo info;
  info.summary = replica_->GetSummary();
  for (const auto& cl_repl : cluster_replicas_) {
    info.cl_repl_summary.push_back(cl_repl->GetSummary());
  }

  return info;
}

vector<ReplicationManager::MasterLinkClientInfo> ReplicationManager::GetMasterLinkClientInfo()
    const {
  vector<MasterLinkClientInfo> out;
  util::fb2::LockGuard lk(mu_);
  if (replica_ == nullptr)
    return out;
  out.push_back({replica_->GetClientId(), replica_->GetClientInfo()});
  for (const auto& cl_repl : cluster_replicas_) {
    out.push_back({cl_repl->GetClientId(), cl_repl->GetClientInfo()});
  }
  return out;
}

bool ReplicationManager::IsMasterLinkClientId(uint32_t id) const {
  util::fb2::LockGuard lk(mu_);
  if (replica_ && replica_->GetClientId() == id)
    return true;
  for (const auto& cl_repl : cluster_replicas_) {
    if (cl_repl->GetClientId() == id)
      return true;
  }
  return false;
}

string ReplicationManager::GetLineageId() const {
  util::fb2::LockGuard lk(mu_);
  return replica_ ? replica_->GetLineageId() : master_replid_;
}

}  // namespace dfly
