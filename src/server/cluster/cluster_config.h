// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/thread_annotations.h>

#include <optional>
#include <string_view>
#include <vector>

#include "src/core/fibers.h"

namespace dfly {

using SlotId = uint16_t;

class ClusterConfig {
 public:
  enum class Role {
    kMaster,
    kReplica,
  };

  struct Node {
    std::string id;
    std::string ip;
    uint16_t port = 0;
    Role role;
  };

  struct SlotRange {
    SlotId start = 0;
    SlotId end = 0;
  };

  struct ClusterShard {
    std::vector<SlotRange> slot_ranges;
    std::vector<Node> nodes;
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

  // Returns nodes that own `id`. Empty if owned by me.
  std::optional<Node> GetNodeForSlot(SlotId id) const;

  void SetConfig(const std::vector<ClusterShard>& new_config);

 private:
  static bool cluster_enabled;
  static constexpr SlotId kMaxSlotNum = 0x3FFF;

  const std::string my_id_;

  mutable util::SharedMutex slots_mu_;

  // This array covers the whole range of possible slots. We keep nullopt for the current instance
  // ("me"), as we do not need any additional information for it (such as ip and port).
  std::array<std::optional<Node>, kMaxSlotNum + 1> config_ ABSL_GUARDED_BY(slots_mu_) = {};
};

}  // namespace dfly
