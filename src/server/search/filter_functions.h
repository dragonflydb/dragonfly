// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <functional>
#include <string_view>

#include "core/search/base.h"

namespace dfly::aggregate {

using Value = ::dfly::search::SortableValue;

// Span of evaluated argument values passed to a built-in function.
using FuncArgs = absl::Span<const Value>;

// A built-in function implementation.
using FuncImpl = std::function<Value(FuncArgs)>;

// Look up a built-in function by name.
// name must already be lowercase -- the lexer guarantees this for all identifiers.
// Returns nullptr if the function is not registered.
const FuncImpl* FindFilterFunction(std::string_view name);

}  // namespace dfly::aggregate
