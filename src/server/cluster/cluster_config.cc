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

bool ClusterConfig::IsConfigValid(const vector<ClusterShard>& new_config) {
  // Make sure that all slots are set exactly once.
  array<bool, tuple_size<decltype(slots_)>::value> slots_found = {};
  for (const auto& shard : new_config) {
    for (const auto& slot_range : shard.slot_ranges) {
      if (slot_range.start > slot_range.end) {
        return false;
      }

      for (SlotId slot = slot_range.start; slot <= slot_range.end; ++slot) {
        if (slot >= slots_found.size()) {
          return false;
        }

        if (slots_found[slot]) {
          // `slot` was already seen
          return false;
        }

        slots_found[slot] = true;
      }
    }
  }

  if (!all_of(slots_found.begin(), slots_found.end(), [](bool b) { return b; }) > 0UL) {
    // Missing slot
    return false;
  }

  return true;
}

bool ClusterConfig::SetConfig(const vector<ClusterShard>& new_config) {
  if (!IsConfigValid(new_config)) {
    return false;
  }

  lock_guard gu(slots_mu_);

  for (const auto& shard : new_config) {
    for (const auto& slot_range : shard.slot_ranges) {
      bool owned_by_me =
          shard.master.id == my_id_ || any_of(shard.replicas.begin(), shard.replicas.end(),
                                              [&](const Node& node) { return node.id == my_id_; });
      for (SlotId i = slot_range.start; i <= slot_range.end; ++i) {
        slots_[i] = {
            .master = shard.master, .replicas = shard.replicas, .owned_by_me = owned_by_me};
      }
    }
  }
  return true;
}

bool ClusterConfig::IsMySlot(SlotId id) const {
  if (id >= slots_.size()) {
    DCHECK(false) << "Requesting a non-existing slot id " << id;
    return false;
  }

  return slots_[id].owned_by_me;
}

vector<ClusterConfig::Node> ClusterConfig::GetNodesForSlot(SlotId id) const {
  if (id >= slots_.size()) {
    DCHECK(false) << "Requesting a non-existing slot id " << id;
    return {};
  }

  const SlotOwner& slot = slots_[id];
  vector<ClusterConfig::Node> results;
  results.reserve(slot.replicas.size() + 1);
  results.push_back(slot.master);
  results.insert(results.end(), slot.replicas.begin(), slot.replicas.end());
  return results;
}

ClusterConfig::Node ClusterConfig::GetMasterNodeForSlot(SlotId id) const {
  CHECK_LT(id, slots_.size()) << "Requesting a non-existing slot id " << id;

  return slots_[id].master;
}

}  // namespace dfly
