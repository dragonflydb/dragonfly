// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace dfly {

using SlotId = std::uint16_t;
constexpr SlotId kMaxSlotNum = 0x3FFF;

// A simple utility class that "aggregates" SlotId-s and can tell whether all inputs were the same.
// Only works when cluster is enabled.
class UniqueSlotChecker {
 public:
  void Add(std::string_view key);
  void Add(SlotId slot_id);

  std::optional<SlotId> GetUniqueSlotId() const;

  void Reset() {
    slot_id_ = kNoSlotId;
  }

 private:
  // kNoSlotId - if slot wasn't set at all
  static constexpr SlotId kNoSlotId = kMaxSlotNum + 1;
  // kInvalidSlotId - if several different slots were set
  static constexpr SlotId kInvalidSlotId = kNoSlotId + 1;

  SlotId slot_id_ = kNoSlotId;
};

SlotId KeySlot(std::string_view key);

void InitializeCluster();
bool IsClusterEnabled();
bool IsClusterEmulated();
bool IsClusterEnabledOrEmulated();
bool IsClusterShardedByTag();

}  // namespace dfly
