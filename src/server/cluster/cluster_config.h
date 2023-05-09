// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <absl/container/flat_hash_set.h>

#include <string_view>
#include <tuple>

#include "src/core/fibers.h"

namespace dfly {

typedef uint16_t SlotId;

class ClusterConfig {
 public:
  ClusterConfig();
  static SlotId KeySlot(std::string_view key);
  static bool IsClusterEnabled() {
    return cluster_enabled;
  }
  // If the key contains the {...} pattern, return only the part between { and }
  static std::string_view KeyTag(std::string_view key);

  // If key is in my slots ownership return true
  bool IsMySlot(SlotId id);

 private:
  bool AddSlots();

  util::SharedMutex slots_mu_;
  absl::flat_hash_set<SlotId> owned_slots_;
  static bool cluster_enabled;
};

}  // namespace dfly
