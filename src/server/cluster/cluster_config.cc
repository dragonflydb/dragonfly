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
bool IsMyNode(const vector<ClusterConfig::Node>& nodes, const string& my_id) {
  bool is_me = false;
  for (const auto& node : nodes) {
    if (node.id == my_id) {
      is_me = true;
      break;
    }
  }
  return is_me;
}
}  // namespace

void ClusterConfig::SetConfig(const vector<ClusterShard>& new_config) {
  lock_guard gu(slots_mu_);

  for (const auto& shard : new_config) {
    shared_ptr<vector<Node>> nodes;

    // We do not store information for our own node, as it's unneeded, and it also helps us to
    // determine our ownership quicker.
    if (!IsMyNode(shard.nodes, my_id_)) {
      nodes = make_shared<vector<Node>>(shard.nodes);
    }

    for (const auto& slot_range : shard.slot_ranges) {
      for (SlotId i = slot_range.start; i <= slot_range.end; ++i) {
        config_[i] = nodes;
      }
    }
  }
}

bool ClusterConfig::IsMySlot(SlotId id) const {
  return GetNodesForSlot(id) == nullptr;
}

shared_ptr<vector<ClusterConfig::Node>> ClusterConfig::GetNodesForSlot(SlotId id) const {
  CHECK(id < config_.size());

  shared_lock sl(slots_mu_);
  return config_[id];
}

}  // namespace dfly
