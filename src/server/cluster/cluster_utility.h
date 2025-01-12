// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/cluster_defs.h"

namespace dfly::cluster {

uint64_t GetKeyCount(const SlotRanges& slots);

}  // namespace dfly::cluster
