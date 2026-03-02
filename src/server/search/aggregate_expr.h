// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <variant>

#include "core/search/base.h"
#include "server/search/aggregator.h"

namespace dfly::aggregate {

// Simple expression tree for FT.AGGREGATE FILTER clauses.
// Supports: @field references, numeric/string literals,
// comparisons (== != < <= > >=), and logical operators (&& ||).

struct Expr;
using ExprPtr = std::unique_ptr<Expr>;

struct FieldRef {
  std::string name;
};

struct DoubleLiteral {
  double value;
};

struct StringLiteral {
  std::string value;
};

enum class CmpOp { EQ, NE, LT, LE, GT, GE };

struct Comparison {
  ExprPtr left;
  CmpOp op;
  ExprPtr right;
};

enum class LogicOp { AND, OR };

struct Logical {
  ExprPtr left;
  LogicOp op;
  ExprPtr right;
};

struct Expr {
  using Variant = std::variant<FieldRef, DoubleLiteral, StringLiteral, Comparison, Logical>;
  Variant v;

  explicit Expr(Variant vv) : v(std::move(vv)) {
  }
};

// Parse a FILTER expression string. Returns nullptr on error.
// Examples:
//   "@count > 5"
//   "(@route_name == 'tech' && @distance < 0.5) || (@route_name == 'sports')"
ExprPtr ParseExpression(std::string_view input);

// Evaluate a parsed expression against a row of aggregated values.
// Field references are resolved from the DocValues map.
// Returns false if a field is missing or types don't match.
bool Evaluate(const Expr& expr, const DocValues& doc);

// Make a FILTER aggregation step from an expression.
AggregationStep MakeFilterStep(ExprPtr expr);

}  // namespace dfly::aggregate
