#include "server/cluster/unique_slot_checker.h"

using namespace std;

namespace dfly {

void UniqueSlotChecker::Add(std::string_view key) {
  if (!ClusterConfig::IsEnabled()) {
    return;
  }

  Add(ClusterConfig::KeySlot(key));
}

void UniqueSlotChecker::Add(SlotId slot_id) {
  if (!ClusterConfig::IsEnabled()) {
    return;
  }

  if (!slot_id_.has_value()) {
    slot_id_ = slot_id;
    return;
  }

  if (*slot_id_ != slot_id) {
    slot_id_ = ClusterConfig::kInvalidSlotId;
  }
}

optional<SlotId> UniqueSlotChecker::GetUniqueSlotId() const {
  if (slot_id_.has_value() && *slot_id_ == ClusterConfig::kInvalidSlotId) {
    return nullopt;
  }

  return slot_id_;
}

}  // namespace dfly
