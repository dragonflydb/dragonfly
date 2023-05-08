// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>
#include <unordered_set>
#include <vector>

#include "src/core/fibers.h"

namespace dfly {

class ClusterData {
 public:
  ClusterData();
  static uint16_t keyHashSlot(std::string_view key);

  bool IsKeyInMySlot(std::string_view key);

 private:
  bool AddSlots();

  util::SharedMutex slots_mu_;
  std::unordered_set<uint16_t> owned_slots_;
  static constexpr uint16_t kMaxSlotNum = 0x3FFF;
};

std::string_view KeyTag(std::string_view key);

extern bool cluster_enabled;

}  // namespace dfly
