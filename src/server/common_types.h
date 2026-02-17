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

// Database and shard identifiers (moved from tx_base.h to reduce compilation dependencies)
using DbIndex = uint16_t;
using ShardId = uint16_t;
using LockFp = uint64_t;  // a key fingerprint used by the LockTable.

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);
constexpr DbIndex kMaxDbId = 1024;  // Reasonable starting point.

}  // namespace dfly
