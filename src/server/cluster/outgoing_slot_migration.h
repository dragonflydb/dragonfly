// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/cluster/cluster_defs.h"
#include "server/protocol_client.h"
#include "server/transaction.h"

namespace dfly {
class DbSlice;
class ServerFamily;

namespace journal {
class Journal;
}
}  // namespace dfly
namespace dfly::cluster {
class ClusterFamily;

// Whole outgoing slots migration manager
class OutgoingMigration : private ProtocolClient {
 public:
  OutgoingMigration(MigrationInfo info, ClusterFamily* cf, ServerFamily* sf);
  ~OutgoingMigration();

  // start migration process, sends INIT command to the target node
  void Start();

  // if is_error = false mark migration as FINISHED and cancel migration if it's not finished yet
  // can be called from any thread, but only after Start()
  // if is_error = true and migration is in progress it will be restarted otherwise nothing happens
  void Finish(GenericError error = {}) ABSL_LOCKS_EXCLUDED(state_mu_);

  MigrationState GetState() const ABSL_LOCKS_EXCLUDED(state_mu_);

  const std::string& GetHostIp() const {
    return server().host;
  };

  uint16_t GetPort() const {
    return server().port;
  };

  const SlotRanges& GetSlots() const {
    return migration_info_.slot_ranges;
  }

  const MigrationInfo GetMigrationInfo() const {
    return migration_info_;
  }

  void ResetError() {
    if (cntx_.IsError()) {
      SetLastError(cntx_.GetError());
      cntx_.Reset(nullptr);
    }
  }

  void SetLastError(dfly::GenericError err) ABSL_LOCKS_EXCLUDED(error_mu_) {
    if (!err)
      return;
    errors_count_.fetch_add(1, std::memory_order_relaxed);
    util::fb2::LockGuard lk(error_mu_);
    last_error_ = std::move(err);
  }

  std::string GetErrorStr() const ABSL_LOCKS_EXCLUDED(error_mu_) {
    util::fb2::LockGuard lk(error_mu_);
    return last_error_.Format();
  }

  size_t GetErrorsCount() const {
    return errors_count_.load(std::memory_order_relaxed);
  }

  size_t GetKeyCount() const ABSL_LOCKS_EXCLUDED(state_mu_);

  static constexpr std::string_view kUnknownMigration = "UNKNOWN_MIGRATION";

 private:
  // should be run for all shards
  void StartFlow(journal::Journal* journal, io::Sink* dest);

  MigrationState GetStateImpl() const;
  // SliceSlotMigration manages state and data transfering for the corresponding shard
  class SliceSlotMigration;

  void SyncFb();
  // return true if migration is finalized even with C_ERROR state
  bool FinalizeMigration(long attempt);

  bool ChangeState(MigrationState new_state) ABSL_LOCKS_EXCLUDED(state_mu_);

  void OnAllShards(std::function<void(std::unique_ptr<SliceSlotMigration>&)>);

 private:
  MigrationInfo migration_info_;
  std::vector<std::unique_ptr<SliceSlotMigration>> slot_migrations_;
  ServerFamily* server_family_;
  ClusterFamily* cf_;
  mutable util::fb2::Mutex error_mu_;
  dfly::GenericError last_error_ ABSL_GUARDED_BY(error_mu_);
  std::atomic<size_t> errors_count_ = 0;

  util::fb2::Fiber main_sync_fb_;

  mutable util::fb2::Mutex state_mu_;
  MigrationState state_ ABSL_GUARDED_BY(state_mu_) = MigrationState::C_CONNECTING;

  boost::intrusive_ptr<Transaction> tx_;

  // when migration is finished we need to store number of migrated keys
  // because new request can add or remove keys and we get incorrect statistic
  size_t keys_number_ = 0;
};

}  // namespace dfly::cluster
