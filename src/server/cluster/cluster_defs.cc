
extern "C" {
#include "redis/crc16.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "cluster_defs.h"
#include "src/server/common.h"

using namespace std;

ABSL_FLAG(string, cluster_mode, "", "Cluster mode supported. Default: \"\"");

namespace dfly::cluster {
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

SlotId ClusterKeySlot(std::string_view key) {
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
