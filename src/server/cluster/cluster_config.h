// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "src/server/cluster/slot_set.h"

namespace dfly::cluster {

class ClusterConfig {
 public:
  // Returns an instance with `config` if it is valid.
  // Returns heap-allocated object as it is too big for a stack frame.
  static std::shared_ptr<ClusterConfig> CreateFromConfig(std::string_view my_id,
                                                         const ClusterShardInfos& config);

  // Parses `json_config` into `ClusterShardInfos` and calls the above overload.
  static std::shared_ptr<ClusterConfig> CreateFromConfig(std::string_view my_id,
                                                         std::string_view json_config);

  std::shared_ptr<ClusterConfig> CloneWithChanges(const SlotRanges& enable_slots,
                                                  const SlotRanges& disable_slots) const;

  std::shared_ptr<ClusterConfig> CloneWithoutMigrations() const;

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id) const;
  bool IsMySlot(std::string_view key) const;

  // Returns the master configured for `id`.
  ClusterNodeInfo GetMasterNodeForSlot(SlotId id) const;

  ClusterShardInfos GetConfig() const;

  const SlotSet& GetOwnedSlots() const;

  std::vector<MigrationInfo> GetNewOutgoingMigrations(
      const std::shared_ptr<ClusterConfig>& prev) const;
  std::vector<MigrationInfo> GetNewIncomingMigrations(
      const std::shared_ptr<ClusterConfig>& prev) const;
  std::vector<MigrationInfo> GetFinishedOutgoingMigrations(
      const std::shared_ptr<ClusterConfig>& prev) const;
  std::vector<MigrationInfo> GetFinishedIncomingMigrations(
      const std::shared_ptr<ClusterConfig>& prev) const;

  std::vector<MigrationInfo> GetIncomingMigrations() const {
    return my_incoming_migrations_;
  }

 private:
  struct SlotEntry {
    const ClusterShardInfo* shard = nullptr;
    bool owned_by_me = false;
  };

  ClusterConfig() = default;

  std::string my_id_;
  ClusterShardInfos config_;

  SlotSet my_slots_;
  std::vector<MigrationInfo> my_outgoing_migrations_;
  std::vector<MigrationInfo> my_incoming_migrations_;
};

}  // namespace dfly::cluster
