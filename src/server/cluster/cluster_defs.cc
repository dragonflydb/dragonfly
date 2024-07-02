
extern "C" {
#include "redis/crc16.h"
}

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include "base/flags.h"
#include "base/logging.h"
#include "cluster_defs.h"
#include "slot_set.h"
#include "src/server/common.h"

using namespace std;

ABSL_FLAG(string, cluster_mode, "", "Cluster mode supported. Default: \"\"");

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

namespace {
enum class ClusterMode {
  kUninitialized,
  kNoCluster,
  kEmulatedCluster,
  kRealCluster,
};

ClusterMode cluster_mode = ClusterMode::kUninitialized;
}  // namespace

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
}

bool IsClusterEnabled() {
  return cluster_mode == ClusterMode::kRealCluster;
}

bool IsClusterEmulated() {
  return cluster_mode == ClusterMode::kEmulatedCluster;
}

SlotId KeySlot(std::string_view key) {
  string_view tag = LockTagOptions::instance().Tag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

bool IsClusterEnabledOrEmulated() {
  return IsClusterEnabled() || IsClusterEmulated();
}

bool IsClusterShardedByTag() {
  return IsClusterEnabledOrEmulated() || LockTagOptions::instance().enabled;
}

}  // namespace dfly::cluster
