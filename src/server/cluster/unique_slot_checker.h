// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string_view>

#include "server/cluster/slot_set.h"

namespace dfly {

// A simple utility class that "aggregates" SlotId-s and can tell whether all inputs were the same.
// Only works when cluster is enabled.
class UniqueSlotChecker {
 public:
  void Add(std::string_view key);
  void Add(SlotId slot_id);

  std::optional<SlotId> GetUniqueSlotId() const;

 private:
  std::optional<SlotId> slot_id_;
};

}  // namespace dfly
