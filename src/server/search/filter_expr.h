// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace dfly::aggregate {

// Forward declaration -- FilterExpr is a pointer to a node
struct FilterExprNode;

// Owning pointer to a filter expression node.
// Enables recursive structures (nodes contain children of the same type).
using FilterExpr = std::unique_ptr<FilterExprNode>;

// Leaf nodes

// Reference to a pipeline field: @fieldname (stored without '@')
struct FieldRef {
  std::string name;
};

// Numeric literal: 3.14, 1e5, inf
struct NumLiteral {
  double value;
};

// String literal: 'hello' or "hello"
struct StrLiteral {
  std::string value;
};

// NULL literal
struct NullLiteral {};

// Operator enums

enum class CmpOp { EQ, NEQ, LT, LTE, GT, GTE };
enum class ArithOp { ADD, SUB, MUL, DIV, MOD, POW };
enum class LogicOp { AND, OR };

// Composite nodes (children stored as FilterExpr / unique_ptr)

struct CmpExpr {
  CmpOp op;
  FilterExpr lhs, rhs;
};

struct ArithExpr {
  ArithOp op;
  FilterExpr lhs, rhs;
};

// Logical AND / OR
struct LogicExpr {
  LogicOp op;
  FilterExpr lhs, rhs;
};

// Logical NOT: !operand
struct NotExpr {
  FilterExpr operand;
};

// Unary minus: -operand
struct NegateExpr {
  FilterExpr operand;
};

// Function call: lower(@name), exists(@field), abs(@val), year(@ts), ...
// name is always lowercase.
struct FuncCallExpr {
  std::string name;
  std::vector<FilterExpr> args;
};

// ---------------------------------------------------------------------------
// The node variant and wrapper
// ---------------------------------------------------------------------------

using FilterExprVariant = std::variant<FieldRef, NumLiteral, StrLiteral, NullLiteral, CmpExpr,
                                       ArithExpr, LogicExpr, NotExpr, NegateExpr, FuncCallExpr>;

// FilterExprNode wraps the variant so that FilterExpr (unique_ptr<FilterExprNode>)
// can be used as a recursive child type.
struct FilterExprNode : public FilterExprVariant {
  using FilterExprVariant::FilterExprVariant;
};

// ---------------------------------------------------------------------------
// Parse result
// ---------------------------------------------------------------------------

// On success: holds a FilterExpr (non-null unique_ptr).
// On failure: holds a std::string with the error message.
using FilterParseResult = std::variant<FilterExpr, std::string>;

}  // namespace dfly::aggregate
