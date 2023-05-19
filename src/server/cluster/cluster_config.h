// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/thread_annotations.h>

#include <array>
#include <optional>
#include <string_view>
#include <vector>

#include "core/json_object.h"
#include "src/core/fibers.h"

namespace dfly {

using SlotId = uint16_t;

class ClusterConfig {
 public:
  static constexpr SlotId kMaxSlotNum = 0x3FFF;

  struct Node {
    std::string id;
    std::string ip;
    uint16_t port = 0;
  };

  struct SlotRange {
    SlotId start = 0;
    SlotId end = 0;
  };

  struct ClusterShard {
    std::vector<SlotRange> slot_ranges;
    Node master;
    std::vector<Node> replicas;
  };

  using ClusterShards = std::vector<ClusterShard>;

  explicit ClusterConfig(std::string_view my_id);

  static SlotId KeySlot(std::string_view key);

  static bool IsClusterEnabled() {
    return cluster_enabled;
  }

  // If the key contains the {...} pattern, return only the part between { and }
  static std::string_view KeyTag(std::string_view key);

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id) const;

  // Returns the master configured for `id`. Returns a default-initialized `Node` if `SetConfig()`
  // was never completed successfully.
  Node GetMasterNodeForSlot(SlotId id) const;

  ClusterShards GetConfig() const;

  // Returns true if `new_config` is valid and internal state was changed. Returns false and changes
  // nothing otherwise.
  bool SetConfig(const ClusterShards& new_config);

  // Parses `json` into `ClusterShards` and calls the above overload.
  bool SetConfig(const JsonType& json);

 private:
  struct SlotEntry {
    const ClusterShard* shard = nullptr;
    bool owned_by_me = false;
  };

  bool IsConfigValid(const ClusterShards& new_config);

  static bool cluster_enabled;

  const std::string my_id_;

  mutable util::SharedMutex mu_;

  ClusterShards config_ ABSL_GUARDED_BY(mu_);

  // This array covers the whole range of possible slots for fast access. It points into `config_`.
  std::array<SlotEntry, kMaxSlotNum + 1> slots_ ABSL_GUARDED_BY(mu_) = {};
};

}  // namespace dfly
