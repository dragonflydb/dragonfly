// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// NOTE: This file is deprecated and exists only for backward compatibility.
// The implementation has been split into focused files:
//   - error_types.cc
//   - global_state.cc
//   - interpreter_utils.cc
//   - scan_options.cc
//   - stats.cc
//   - sync_primitives.cc
//   - type_aliases.cc

#include "server/common.h"

// All implementation moved to specific files. This file now serves only as
// a compatibility layer that includes the common.h header which in turn includes
// all the new focused headers.

namespace dfly {

// Forward declaration for test function that was exposed in common.h/common.cc
void TEST_InvalidateLockTagOptions();

}  // namespace dfly
