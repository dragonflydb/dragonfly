// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>

#include "server/search/aggregator.h"
#include "server/search/filter_expr.h"
#include "server/search/filter_functions.h"

namespace dfly::aggregate {

// Returns true if a Value is "truthy":
//   monostate        -> false
//   double 0.0 / NaN -> false,  any other double -> true
//   empty string     -> false,  non-empty string  -> true
bool IsTruthy(const Value& v);

// Flattens the tree once into postfix code with jumps; the tree must outlive the evaluator.
class FilterExprEvaluator {
 public:
  explicit FilterExprEvaluator(const FilterExprNode& root);

  Value Eval(const DocValues& doc);

 private:
  enum class Op : uint8_t {
    kField,
    kNum,
    kStr,
    kNull,
    kCmp,
    kArith,
    kAnd,
    kOr,
    kToBool,
    kNot,
    kNegate,
    kCall
  };

  struct Instr {
    Op op;
    uint32_t arg;  // jump target of kAnd and kOr, argument count of kCall
    const FilterExprNode* node;
    const FuncImpl* fn;
  };

  std::vector<Instr> code_;
  std::vector<Value> stack_;
};

// Evaluate the filter expression AST against a single document.
// Returns a Value; use IsTruthy() to convert the result to a boolean for
// filtering purposes.
Value EvalFilterExpr(const FilterExprNode& node, const DocValues& doc);

}  // namespace dfly::aggregate
