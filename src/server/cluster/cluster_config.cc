extern "C" {
#include "redis/crc16.h"
}

#include <shared_mutex>
#include <string_view>

#include "cluster_config.h"

namespace dfly {

bool ClusterConfig::cluster_enabled = false;

static constexpr SlotId kMaxSlotNum = 0x3FFF;

std::string_view ClusterConfig::KeyTag(std::string_view key) {
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

SlotId ClusterConfig::KeySlot(std::string_view key) {
  std::string_view tag = KeyTag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

ClusterConfig::ClusterConfig() {
  cluster_enabled = true;
  AddSlots();
}

void ClusterConfig::AddSlots() {
  // TODO update logic acording to config
  // currently add all slots to owned slots
  std::lock_guard lk{slots_mu_};
  for (SlotId slot_id = 0; slot_id <= kMaxSlotNum; ++slot_id) {
    owned_slots_.emplace(slot_id);
  }
  return;
}

bool ClusterConfig::IsMySlot(SlotId id) {
  std::shared_lock sl(slots_mu_);
  return owned_slots_.contains(id);
}

}  // namespace dfly
