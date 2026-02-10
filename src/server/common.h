// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// NOTE: This header is being refactored for better cohesion.
// Include the specific headers you need instead of this catch-all header:
//   - transaction_types.h: TxId, LSN, TxClock, SlotId
//   - global_state.h: GlobalState, kernel_version, memory globals
//   - stats.h: TieredStats, SearchStats
//   - scan_options.h: ScanOpts
//   - error_types.h: GenericError, ExecutionState, AggregateError
//   - sync_primitives.h: ThreadLocalMutex, LocalLatch, SharedLock, LockTagOptions
//   - interpreter_utils.h: BorrowedInterpreter
//   - type_aliases.h: CompactObjType, TimeUnit, ExpireFlags, etc.

#pragma once

// Include all the new focused headers for backward compatibility
#include "server/error_types.h"
#include "server/global_state.h"
#include "server/interpreter_utils.h"
#include "server/scan_options.h"
#include "server/stats.h"
#include "server/sync_primitives.h"
#include "server/transaction_types.h"
#include "server/type_aliases.h"

namespace dfly {

// Forward declarations
class CommandId;
class Transaction;
class EngineShard;
struct ConnectionState;
class Interpreter;

}  // namespace dfly
