// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bitset>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"

namespace dfly {

using SlotId = uint16_t;

struct SlotRange {
  static constexpr SlotId kMaxSlotId = 0x3FFF;
  SlotId start = 0;
  SlotId end = 0;

  bool operator==(const SlotRange& r) const {
    return start == r.start && end == r.end;
  }
  bool IsValid() {
    return start <= end && start <= kMaxSlotId && end <= kMaxSlotId;
  }

  std::string ToString() const {
    return absl::StrCat("[", start, ", ", end, "]");
  }

  static std::string ToString(const std::vector<SlotRange>& ranges) {
    return absl::StrJoin(ranges, ", ", [](std::string* out, SlotRange range) {
      absl::StrAppend(out, range.ToString());
    });
  }
};

using SlotRanges = std::vector<SlotRange>;

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

}  // namespace dfly
