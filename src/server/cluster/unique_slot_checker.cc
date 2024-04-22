#include "server/cluster/unique_slot_checker.h"

#include "server/cluster/cluster_defs.h"

using namespace std;

namespace dfly::cluster {

void UniqueSlotChecker::Add(std::string_view key) {
  if (!IsClusterEnabled()) {
    return;
  }

  Add(KeySlot(key));
}

void UniqueSlotChecker::Add(SlotId slot_id) {
  if (!IsClusterEnabled()) {
    return;
  }

  if (!slot_id_.has_value()) {
    slot_id_ = slot_id;
    return;
  }

  if (*slot_id_ != slot_id) {
    slot_id_ = kInvalidSlotId;
  }
}

optional<SlotId> UniqueSlotChecker::GetUniqueSlotId() const {
  if (slot_id_.has_value() && *slot_id_ == kInvalidSlotId) {
    return nullopt;
  }

  return slot_id_;
}

}  // namespace dfly::cluster
