#include <mutex>
extern "C" {
#include "redis/crc16.h"
}

#include <shared_mutex>
#include <string_view>

#include "base/logging.h"
#include "cluster_config.h"

using namespace std;

namespace dfly {

bool ClusterConfig::cluster_enabled = false;

string_view ClusterConfig::KeyTag(string_view key) {
  size_t start = key.find('{');
  if (start == key.npos) {
    return key;
  }
  size_t end = key.find('}', start + 1);
  if (end == key.npos || end == start + 1) {
    return key;
  }
  return key.substr(start + 1, end - start - 1);
}

SlotId ClusterConfig::KeySlot(string_view key) {
  string_view tag = KeyTag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

ClusterConfig::ClusterConfig(string_view my_id) : my_id_(my_id) {
  cluster_enabled = true;
}

namespace {
optional<ClusterConfig::Node> GetMasterNodeForSlot(const vector<ClusterConfig::Node>& nodes,
                                                   const string& my_id) {
  for (const auto& node : nodes) {
    if (node.id == my_id) {
      // We do not store information for our own node, as it's unneeded, and it also helps us to
      // determine our ownership quicker.
      return nullopt;
    }

    if (node.role == ClusterConfig::Role::kMaster) {
      return node;
    }
  }

  CHECK(false) << "No master present in nodes";
  return nullopt;
}
}  // namespace

void ClusterConfig::SetConfig(const vector<ClusterShard>& new_config) {
  lock_guard gu(slots_mu_);

  for (const auto& shard : new_config) {
    optional<Node> node = GetMasterNodeForSlot(shard.nodes, my_id_);

    for (const auto& slot_range : shard.slot_ranges) {
      for (SlotId i = slot_range.start; i <= slot_range.end; ++i) {
        config_[i] = node;
      }
    }
  }
}

bool ClusterConfig::IsMySlot(SlotId id) const {
  return GetNodeForSlot(id) == nullopt;
}

optional<ClusterConfig::Node> ClusterConfig::GetNodeForSlot(SlotId id) const {
  CHECK(id < config_.size());

  shared_lock sl(slots_mu_);
  return config_[id];
}

}  // namespace dfly
