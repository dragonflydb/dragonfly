// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

// Transaction, replication and partitioning identifiers
using LSN = uint64_t;
using TxId = uint64_t;
using TxClock = uint64_t;
using SlotId = std::uint16_t;

}  // namespace dfly
