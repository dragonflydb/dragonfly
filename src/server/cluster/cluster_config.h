// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_set.h>
#include <stdint.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/hash/hash.h"
#include "core/json_object.h"
#include "src/core/fibers.h"

namespace dfly {

using SlotId = uint16_t;
using SlotSet = absl::flat_hash_set<SlotId>;

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

  static SlotId KeySlot(std::string_view key);

  static void EnableCluster() {
    cluster_enabled = true;
  }

  static bool IsClusterEnabled() {
    return cluster_enabled;
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

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id) const;

  // Returns the master configured for `id`.
  Node GetMasterNodeForSlot(SlotId id) const;

  ClusterShards GetConfig() const;

  SlotSet GetOwnedSlots() const;

 private:
  struct SlotEntry {
    const ClusterShard* shard = nullptr;
    bool owned_by_me = false;
  };

  static bool cluster_enabled;

  ClusterConfig() = default;

  ClusterShards config_;

  // True bits in `my_slots_` indicate that this slot is owned by this node.
  std::bitset<kMaxSlotNum + 1> my_slots_;
};

}  // namespace dfly
