// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/thread_annotations.h>

#include <array>
#include <optional>
#include <string_view>
#include <vector>

#include "src/core/fibers.h"

namespace dfly {

using SlotId = uint16_t;

class ClusterConfig {
 public:
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

  explicit ClusterConfig(std::string_view my_id);

  static SlotId KeySlot(std::string_view key);

  static bool IsClusterEnabled() {
    return cluster_enabled;
  }

  // If the key contains the {...} pattern, return only the part between { and }
  static std::string_view KeyTag(std::string_view key);

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id) const;

  // Returns nodes that own `id`. Result will always have the first element set to be the master,
  // and the following 0 or more elements are the replicas.
  std::vector<Node> GetNodesForSlot(SlotId id) const;

  // Returns the master configured for `id`. Returns a default-initialized `Node` if `SetConfig()`
  // was never completed successfully.
  Node GetMasterNodeForSlot(SlotId id) const;

  // Returns true if `new_config` is valid and internal state was changed. Returns false and changes
  // nothing otherwise.
  bool SetConfig(const std::vector<ClusterShard>& new_config);

 private:
  struct SlotOwner {
    Node master;
    std::vector<Node> replicas;
    bool owned_by_me = false;
  };

  bool IsConfigValid(const std::vector<ClusterShard>& new_config);

  static bool cluster_enabled;
  static constexpr SlotId kMaxSlotNum = 0x3FFF;

  const std::string my_id_;

  mutable util::SharedMutex slots_mu_;

  // This array covers the whole range of possible slots.
  std::array<SlotOwner, kMaxSlotNum + 1> slots_ ABSL_GUARDED_BY(slots_mu_) = {};
};

}  // namespace dfly
