// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bitset>
#include <memory>
#include <vector>

#include "cluster_defs.h"

namespace dfly::cluster {

class SlotSet {
 public:
  static constexpr SlotId kSlotsNumber = SlotRange::kMaxSlotId + 1;
  using TBitSet = std::bitset<kSlotsNumber>;

  SlotSet(bool full_house = false) {
    if (full_house)
      slots_->flip();
  }

  SlotSet(const SlotRanges& slot_ranges) {
    Set(slot_ranges, true);
  }

  SlotSet(const TBitSet& s) {
    *slots_ = s;
  }

  SlotSet(const SlotSet& s) {
    *slots_ = *s.slots_;
  }

  bool Contains(SlotId slot) const {
    return slots_->test(slot);
  }

  void Set(const SlotRanges& slot_ranges, bool value) {
    for (const auto& slot_range : slot_ranges) {
      for (auto i = slot_range.start; i <= slot_range.end; ++i) {
        slots_->set(i, value);
      }
    }
  }

  void Set(SlotId slot, bool value) {
    slots_->set(slot, value);
  }

  bool Empty() const {
    return slots_->none();
  }

  size_t Count() const {
    return slots_->count();
  }

  bool All() const {
    return slots_->all();
  }

  // Get SlotSet that are absent in the slots
  SlotSet GetRemovedSlots(const SlotSet& slots) const {
    return *slots_ & ~*slots.slots_;
  }

  SlotRanges ToSlotRanges() const {
    SlotRanges res;

    for (SlotId i = 0; i < kSlotsNumber; ++i) {
      if (!slots_->test(i)) {
        continue;
      } else {
        auto& range = res.emplace_back(SlotRange{i, i});
        for (++i; i < kSlotsNumber && slots_->test(i); ++i) {
          range.end = i;
        }
      }
    }

    return res;
  }

 private:
  std::unique_ptr<TBitSet> slots_{std::make_unique<TBitSet>()};
};

}  // namespace dfly::cluster
