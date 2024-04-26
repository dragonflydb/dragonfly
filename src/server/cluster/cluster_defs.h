// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace dfly::cluster {

using SlotId = uint16_t;

constexpr SlotId kMaxSlotNum = 0x3FFF;
constexpr SlotId kInvalidSlotId = kMaxSlotNum + 1;

struct SlotRange {
  static constexpr SlotId kMaxSlotId = 0x3FFF;
  SlotId start = 0;
  SlotId end = 0;

  bool operator==(const SlotRange& r) const {
    return start == r.start && end == r.end;
  }
  bool IsValid() {
    return start <= end && start <= kMaxSlotId && end <= kMaxSlotId;
  }

  std::string ToString() const {
    return absl::StrCat("[", start, ", ", end, "]");
  }

  static std::string ToString(const std::vector<SlotRange>& ranges) {
    return absl::StrJoin(ranges, ", ", [](std::string* out, SlotRange range) {
      absl::StrAppend(out, range.ToString());
    });
  }
};

using SlotRanges = std::vector<SlotRange>;

struct ClusterNodeInfo {
  std::string id;
  std::string ip;
  uint16_t port = 0;
};

struct MigrationInfo {
  std::vector<SlotRange> slot_ranges;
  std::string node_id;
  std::string ip;
  uint16_t port = 0;

  bool operator==(const MigrationInfo& r) const {
    return ip == r.ip && port == r.port && slot_ranges == r.slot_ranges && node_id == r.node_id;
  }
};

struct ClusterShardInfo {
  SlotRanges slot_ranges;
  ClusterNodeInfo master;
  std::vector<ClusterNodeInfo> replicas;
  std::vector<MigrationInfo> migrations;
};

using ClusterShardInfos = std::vector<ClusterShardInfo>;

// MigrationState constants are ordered in state changing order
enum class MigrationState : uint8_t {
  C_NO_STATE,
  C_CONNECTING,
  C_SYNC,
  C_FINISHED,
  C_MAX_INVALID = std::numeric_limits<uint8_t>::max()
};

SlotId KeySlot(std::string_view key);

void InitializeCluster();
bool IsClusterEnabled();
bool IsClusterEmulated();
bool IsClusterEnabledOrEmulated();
bool IsClusterShardedByTag();

}  // namespace dfly::cluster
