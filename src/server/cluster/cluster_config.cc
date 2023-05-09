extern "C" {
#include "redis/crc16.h"
}

#include <shared_mutex>
#include <string_view>

#include "cluster_config.h"

namespace dfly {

bool cluster_enabled = false;

static constexpr SlotId kMaxSlotNum = 0x3FFF;

std::string_view KeyTag(std::string_view key) {
  size_t s, e; /* start-end indexes of { and } */

  for (s = 0; s < key.length(); s++)
    if (key[s] == '{')
      break;

  // No '{'
  if (s == key.length())
    return key;

  // '{' found, Check if we have the corresponding '}'.
  for (e = s + 1; e < key.length(); e++)
    if (key[e] == '}')
      break;

  // No '}'
  if (e == key.length() || e == s + 1)
    return key;

  // return only the part between { and }
  return key.substr(s + 1, e - s - 1);
}

SlotId ClusterConfig::KeySlot(std::string_view key) {
  std::string_view tag = KeyTag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

ClusterConfig::ClusterConfig() {
  AddSlots();
}

bool ClusterConfig::AddSlots() {
  // TODO update logic acording to config
  // currently add all slots to owned slots
  std::lock_guard lk{slots_mu_};
  for (SlotId slot_id = 0; slot_id <= kMaxSlotNum; ++slot_id) {
    owned_slots_.emplace(slot_id);
  }
  return true;
}

bool ClusterConfig::IsMySlot(SlotId id) {
  std::shared_lock sl(slots_mu_);
  size_t count = owned_slots_.count(id);
  return count > 0;
}

}  // namespace dfly
