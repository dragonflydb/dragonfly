// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace cmn {

// Forward declaration of BackedArguments to avoid including the full header
// which pulls in absl/container/inlined_vector.h (heavy template dependency).
//
// BackedArguments is a class that stores command arguments in a compact format,
// using an inlined vector for storage. It's used throughout the codebase for
// efficient argument passing in the command processing pipeline.
//
// Include "common/backed_args.h" in .cc files when you need the full definition.
// Use this forward declaration in headers when you only need to reference
// BackedArguments via pointer or reference.
class BackedArguments;

}  // namespace cmn
