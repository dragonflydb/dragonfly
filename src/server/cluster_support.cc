// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

extern "C" {
#include "redis/crc16.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "cluster_support.h"
#include "common.h"

using namespace std;

ABSL_FLAG(string, cluster_mode, "",
          "Cluster mode supported. Possible values are "
          "'emulated', 'yes' or ''");

ABSL_FLAG(bool, experimental_cluster_shard_by_slot, false,
          "If true, cluster mode is enabled and sharding is done by slot. "
          "Otherwise, sharding is done by hash tag.");

namespace dfly {

void UniqueSlotChecker::Add(std::string_view key) {
  if (!IsClusterEnabled()) {
    return;
  }

  Add(KeySlot(key));
}

void UniqueSlotChecker::Add(SlotId slot_id) {
  if (!IsClusterEnabled()) {
    return;
  }

  if (slot_id_ == kNoSlotId) {
    slot_id_ = slot_id;
  } else if (slot_id_ != slot_id) {
    slot_id_ = kCrossSlot;
  }
}

optional<SlotId> UniqueSlotChecker::GetUniqueSlotId() const {
  return slot_id_ > kMaxSlotNum ? optional<SlotId>() : slot_id_;
}

namespace detail {
ClusterMode cluster_mode = ClusterMode::kUninitialized;
bool cluster_shard_by_slot = false;

}  // namespace detail

using namespace detail;

void InitializeCluster() {
  string cluster_mode_str = absl::GetFlag(FLAGS_cluster_mode);

  if (cluster_mode_str == "emulated") {
    cluster_mode = ClusterMode::kEmulatedCluster;
  } else if (cluster_mode_str == "yes") {
    cluster_mode = ClusterMode::kRealCluster;
  } else if (cluster_mode_str.empty()) {
    cluster_mode = ClusterMode::kNoCluster;
  } else {
    LOG(ERROR) << "Invalid value for flag --cluster_mode. Exiting...";
    exit(1);
  }

  if (cluster_mode != ClusterMode::kNoCluster) {
    cluster_shard_by_slot = absl::GetFlag(FLAGS_experimental_cluster_shard_by_slot);
    LOG(INFO) << "FLAG IS " << cluster_shard_by_slot;
  }
}

SlotId KeySlot(std::string_view key) {
  string_view tag = LockTagOptions::instance().Tag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

bool IsClusterShardedByTag() {
  return IsClusterEnabledOrEmulated() || LockTagOptions::instance().enabled;
}

}  // namespace dfly
