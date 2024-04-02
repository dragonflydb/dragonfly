// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <memory>
#include <string_view>
#include <vector>

#include "src/server/cluster/slot_set.h"
#include "src/server/common.h"

namespace dfly {

// MigrationState constants are ordered in state changing order
enum class MigrationState : uint8_t {
  C_NO_STATE,
  C_CONNECTING,
  C_SYNC,
  C_FINISHED,
  C_MAX_INVALID = std::numeric_limits<uint8_t>::max()
};

struct ClusterNodeConfig {
  std::string id;
  std::string ip;
  uint16_t port = 0;
};

struct MigrationInfo {
  std::vector<SlotRange> slot_ranges;
  std::string target_id;
  std::string ip;
  uint16_t port = 0;

  bool operator==(const MigrationInfo& r) const {
    return ip == r.ip && port == r.port && slot_ranges == r.slot_ranges && target_id == r.target_id;
  }
};

struct ClusterShardConfig {
  SlotRanges slot_ranges;
  ClusterNodeConfig master;
  std::vector<ClusterNodeConfig> replicas;
  std::vector<MigrationInfo> migrations;
};

using ClusterShardConfigs = std::vector<ClusterShardConfig>;

class ClusterConfig {
 public:
  static constexpr SlotId kMaxSlotNum = 0x3FFF;
  static constexpr SlotId kInvalidSlotId = kMaxSlotNum + 1;

  static SlotId KeySlot(std::string_view key);

  static void Initialize();
  static bool IsEnabled();
  static bool IsEmulated();

  static bool IsEnabledOrEmulated() {
    return IsEnabled() || IsEmulated();
  }

  static bool IsShardedByTag() {
    return IsEnabledOrEmulated() || KeyLockArgs::IsLockHashTagEnabled();
  }

  // If the key contains the {...} pattern, return only the part between { and }
  static std::string_view KeyTag(std::string_view key);

  // Returns an instance with `config` if it is valid.
  // Returns heap-allocated object as it is too big for a stack frame.
  static std::shared_ptr<ClusterConfig> CreateFromConfig(std::string_view my_id,
                                                         const ClusterShardConfigs& config);

  // Parses `json_config` into `ClusterShardConfigs` and calls the above overload.
  static std::shared_ptr<ClusterConfig> CreateFromConfig(std::string_view my_id,
                                                         std::string_view json_config);

  std::shared_ptr<ClusterConfig> CloneWithChanges(const std::vector<SlotRange>& slots,
                                                  bool enable) const;

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id) const;
  bool IsMySlot(std::string_view key) const;

  // Returns the master configured for `id`.
  ClusterNodeConfig GetMasterNodeForSlot(SlotId id) const;

  ClusterShardConfigs GetConfig() const;

  const SlotSet& GetOwnedSlots() const;

  std::vector<MigrationInfo> GetNewOutgoingMigrations(std::shared_ptr<ClusterConfig> prev) const;
  std::vector<MigrationInfo> GetNewIncomingMigrations(std::shared_ptr<ClusterConfig> prev) const;
  std::vector<MigrationInfo> GetFinishedOutgoingMigrations(
      std::shared_ptr<ClusterConfig> prev) const;
  std::vector<MigrationInfo> GetFinishedIncomingMigrations(
      std::shared_ptr<ClusterConfig> prev) const;

 private:
  struct SlotEntry {
    const ClusterShardConfig* shard = nullptr;
    bool owned_by_me = false;
  };

  ClusterConfig() = default;

  ClusterShardConfigs config_;

  SlotSet my_slots_;
  std::vector<MigrationInfo> my_outgoing_migrations_;
  std::vector<MigrationInfo> my_incoming_migrations_;
};

}  // namespace dfly
