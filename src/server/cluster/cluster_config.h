// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <memory>
#include <string_view>
#include <vector>

#include "core/json/json_object.h"
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

class ClusterConfig {
 public:
  static constexpr SlotId kMaxSlotNum = 0x3FFF;
  static constexpr SlotId kInvalidSlotId = kMaxSlotNum + 1;

  struct Node {
    std::string id;
    std::string ip;
    uint16_t port = 0;
  };

  struct MigrationInfo {
    std::vector<SlotRange> slot_ranges;
    std::string target_id;
    std::string ip;
    uint16_t port = 0;

    bool operator==(const ClusterConfig::MigrationInfo& r) const {
      return ip == r.ip && port == r.port && slot_ranges == r.slot_ranges &&
             target_id == r.target_id;
    }
  };

  struct ClusterShard {
    SlotRanges slot_ranges;
    Node master;
    std::vector<Node> replicas;
    std::vector<MigrationInfo> migrations;
  };

  using ClusterShards = std::vector<ClusterShard>;

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
                                                         const ClusterShards& config);

  // Parses `json_config` into `ClusterShards` and calls the above overload.
  static std::shared_ptr<ClusterConfig> CreateFromConfig(std::string_view my_id,
                                                         const JsonType& json_config);

  std::shared_ptr<ClusterConfig> CloneWithChanges(const std::vector<SlotRange>& slots,
                                                  bool enable) const;

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id) const;
  bool IsMySlot(std::string_view key) const;

  // Returns the master configured for `id`.
  Node GetMasterNodeForSlot(SlotId id) const;

  ClusterShards GetConfig() const;

  const SlotSet& GetOwnedSlots() const;

  const std::vector<MigrationInfo>& GetOutgoingMigrations() const {
    return my_outgoing_migrations_;
  }

  const std::vector<MigrationInfo>& GetIncomingMigrations() const {
    return my_incoming_migrations_;
  }

 private:
  struct SlotEntry {
    const ClusterShard* shard = nullptr;
    bool owned_by_me = false;
  };

  ClusterConfig() = default;

  ClusterShards config_;

  SlotSet my_slots_;
  std::vector<MigrationInfo> my_outgoing_migrations_;
  std::vector<MigrationInfo> my_incoming_migrations_;
};

}  // namespace dfly
