// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "cluster_defs.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include "facade/error.h"
#include "slot_set.h"

// TODO remove when tl_cluster_config will be moved out from it
#include "server/cluster/cluster_family.h"

using namespace std;

namespace dfly::cluster {
std::string SlotRange::ToString() const {
  return absl::StrCat("[", start, ", ", end, "]");
}

SlotRanges::SlotRanges(std::vector<SlotRange> ranges) : ranges_(std::move(ranges)) {
  std::sort(ranges_.begin(), ranges_.end());
}

void SlotRanges::Merge(const SlotRanges& sr) {
  // TODO rewrite it
  SlotSet slots(*this);
  slots.Set(sr, true);
  ranges_ = std::move(slots.ToSlotRanges().ranges_);
}

std::string SlotRanges::ToString() const {
  return absl::StrJoin(ranges_, ", ", [](std::string* out, SlotRange range) {
    absl::StrAppend(out, range.ToString());
  });
}

std::string MigrationInfo::ToString() const {
  return absl::StrCat(node_info.id, ",", node_info.ip, ":", node_info.port, " (",
                      slot_ranges.ToString(), ")");
}

bool ClusterShardInfo::operator==(const ClusterShardInfo& r) const {
  if (slot_ranges == r.slot_ranges && master == r.master) {
    auto lreplicas = replicas;
    auto lmigrations = migrations;
    auto rreplicas = r.replicas;
    auto rmigrations = r.migrations;
    std::sort(lreplicas.begin(), lreplicas.end());
    std::sort(lmigrations.begin(), lmigrations.end());
    std::sort(rreplicas.begin(), rreplicas.end());
    std::sort(rmigrations.begin(), rmigrations.end());
    return lreplicas == rreplicas && lmigrations == rmigrations;
  }
  return false;
}

ClusterShardInfos::ClusterShardInfos(std::vector<ClusterShardInfo> infos)
    : infos_(std::move(infos)) {
  std::sort(infos_.begin(), infos_.end());
}

facade::ErrorReply SlotOwnershipError(SlotId slot_id) {
  const cluster::ClusterConfig* cluster_config = ClusterFamily::cluster_config();
  if (!cluster_config)
    return facade::ErrorReply{facade::kClusterNotConfigured};

  if (!cluster_config->IsMySlot(slot_id)) {
    // See more details here: https://redis.io/docs/reference/cluster-spec/#moved-redirection
    cluster::ClusterNodeInfo master = cluster_config->GetMasterNodeForSlot(slot_id);
    return facade::ErrorReply{absl::StrCat("-MOVED ", slot_id, " ", master.ip, ":", master.port),
                              "MOVED"};
  }
  return facade::ErrorReply{facade::OpStatus::OK};
}
}  // namespace dfly::cluster
