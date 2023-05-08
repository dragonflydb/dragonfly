extern "C" {
#include "redis/crc16.h"
}

#include <string_view>

#include "cluster_data.h"

namespace dfly {

bool cluster_enabled = false;

// If the key contains the {...} pattern, return only the part between { and }
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

uint16_t ClusterData::keyHashSlot(std::string_view key) {
  std::string_view tag = KeyTag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

ClusterData::ClusterData() {
  AddSlots();
}

bool ClusterData::AddSlots() {
  // TODO update logic acording to config
  // currently add all slots to owned slots
  std::lock_guard lk{slots_mu_};
  for (uint16_t slot_id = 0; slot_id <= kMaxSlotNum; ++slot_id) {
    owned_slots_.emplace(slot_id);
  }
  return true;
}

bool ClusterData::IsKeyInMySlot(std::string_view key) {
  uint16_t slot_id = keyHashSlot(key);
  slots_mu_.lock_shared();
  size_t count = owned_slots_.count(slot_id);
  slots_mu_.unlock_shared();
  return count > 0;
}

}  // namespace dfly
