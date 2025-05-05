// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace dfly {

namespace detail {

enum class ClusterMode {
  kUninitialized,
  kNoCluster,
  kEmulatedCluster,
  kRealCluster,
};

extern ClusterMode cluster_mode;
extern bool cluster_shard_by_slot;

};  // namespace detail

using SlotId = std::uint16_t;
constexpr SlotId kMaxSlotNum = 0x3FFF;

// A simple utility class that "aggregates" SlotId-s and can tell whether all inputs were the same.
// Only works when cluster is enabled.
class UniqueSlotChecker {
 public:
  void Add(std::string_view key);
  void Add(SlotId slot_id);

  std::optional<SlotId> GetUniqueSlotId() const;

  bool IsCrossSlot() const {
    return slot_id_ == kCrossSlot;
  }

  void Reset() {
    slot_id_ = kNoSlotId;
  }

 private:
  // kNoSlotId - if slot wasn't set at all
  static constexpr SlotId kNoSlotId = kMaxSlotNum + 1;
  // kCrossSlot - if several different slots were set
  static constexpr SlotId kCrossSlot = kNoSlotId + 1;

  SlotId slot_id_ = kNoSlotId;
};

SlotId KeySlot(std::string_view key);

// if !IsClusterEnabled() returns default_value
SlotId KeySlotOr(std::string_view key, SlotId default_value);

void InitializeCluster();

inline bool IsClusterEnabled() {
  return detail::cluster_mode == detail::ClusterMode::kRealCluster;
}

inline bool IsClusterEmulated() {
  return detail::cluster_mode == detail::ClusterMode::kEmulatedCluster;
}

inline bool IsClusterEnabledOrEmulated() {
  return IsClusterEnabled() || IsClusterEmulated();
}

inline bool IsClusterShardedBySlot() {
  return detail::cluster_shard_by_slot;
}

bool IsClusterShardedByTag();

}  // namespace dfly
