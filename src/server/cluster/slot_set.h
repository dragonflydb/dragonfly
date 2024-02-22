// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bitset>
#include <memory>
#include <vector>

namespace dfly {

using SlotId = uint16_t;

struct SlotRange {
  SlotId start = 0;
  SlotId end = 0;
};

using SlotRanges = std::vector<SlotRange>;

class SlotSet {
 public:
  static constexpr SlotId kMaxSlot = 0x3FFF;
  static constexpr SlotId kSlotsNumber = kMaxSlot + 1;

  SlotSet(bool full_house = false) : slots_(std::make_unique<BitsetType>()) {
    if (full_house)
      slots_->flip();
  }

  SlotSet(const SlotRanges& slot_ranges) : SlotSet() {
    Set(slot_ranges, true);
  }

  SlotSet(const SlotSet& s) : SlotSet() {
    *slots_ = *s.slots_;
  }

  bool Contains(SlotId slot) const {
    return slots_->test(slot);
  }

  void Set(const SlotRanges& slot_ranges, bool value) {
    for (const auto& slot_range : slot_ranges) {
      for (auto i = slot_range.start; i <= slot_range.end; ++i) {
        slots_->set(i);
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

  SlotSet GetRemovedSlots(SlotSet slots) {
    slots.slots_->flip();
    *slots.slots_ &= *slots_;
    return slots;
  }

  SlotRanges ToSlotRanges() const {
    SlotRanges res;
    bool current_val = false;

    for (SlotId i = 0; i <= kMaxSlot; ++i) {
      if (slots_->test(i) == current_val) {
        continue;
      } else {
        if (!current_val) {
          res.emplace_back(SlotRange{i, i});
        } else {
          res.back().end = i - 1;
        }
      }
    }

    if (current_val) {
      res.back().end = kMaxSlot;
    }
    return res;
  }

 private:
  using BitsetType = std::bitset<kSlotsNumber>;
  std::unique_ptr<BitsetType> slots_;
};

}  // namespace dfly
