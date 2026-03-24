// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/search/aggregator.h"
#include "server/search/filter_expr.h"

namespace dfly::aggregate {

// Returns true if a Value is "truthy":
//   monostate        -> false
//   double 0.0 / NaN -> false,  any other double -> true
//   empty string     -> false,  non-empty string  -> true
bool IsTruthy(const Value& v);

// Evaluate the filter expression AST against a single document.
// Returns a Value; use IsTruthy() to convert the result to a boolean for
// filtering purposes.
Value EvalFilterExpr(const FilterExprNode& node, const DocValues& doc);

}  // namespace dfly::aggregate
