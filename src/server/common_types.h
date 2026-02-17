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

// Server state and time enums (moved from common.h to reduce compilation dependencies)
enum class GlobalState : uint8_t {
  ACTIVE,
  LOADING,
  SHUTTING_DOWN,
  TAKEN_OVER,
};

enum class TimeUnit : uint8_t { SEC, MSEC };

enum ExpireFlags {
  EXPIRE_ALWAYS = 0,
  EXPIRE_NX = 1 << 0,  // Set expiry only when key has no expiry
  EXPIRE_XX = 1 << 2,  // Set expiry only when the key has expiry
  EXPIRE_GT = 1 << 3,  // GT: Set expiry only when the new expiry is greater than current one
  EXPIRE_LT = 1 << 4,  // LT: Set expiry only when the new expiry is less than current one
};

// Forward declarations for commonly used classes (to reduce header dependencies)
class EngineShard;
class Transaction;
class DbSlice;
class ConnectionContext;
class CommandContext;
class Namespace;
class CommandRegistry;
class Interpreter;

namespace journal {
class Journal;
}  // namespace journal

}  // namespace dfly
