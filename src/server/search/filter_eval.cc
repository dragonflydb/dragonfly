// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/filter_eval.h"

#include <cmath>
#include <variant>
#include <vector>

#include "server/search/filter_functions.h"

namespace dfly::aggregate {

bool IsTruthy(const Value& v) {
  if (std::holds_alternative<std::monostate>(v))
    return false;
  if (std::holds_alternative<double>(v)) {
    double d = std::get<double>(v);
    return d != 0.0 && !std::isnan(d);  // Inf is intentionally truthy
  }
  return !std::get<std::string>(v).empty();
}

namespace {

// Forward declaration for mutual recursion.
Value EvalNode(const FilterExprNode& node, const DocValues& doc);

// Comparison evaluation
Value EvalCmp(CmpOp op, const Value& lhs, const Value& rhs) {
  // Numeric comparison when both sides are doubles.
  if (std::holds_alternative<double>(lhs) && std::holds_alternative<double>(rhs)) {
    double l = std::get<double>(lhs);
    double r = std::get<double>(rhs);
    switch (op) {
      case CmpOp::EQ:
        return l == r ? 1.0 : 0.0;
      case CmpOp::NEQ:
        return l != r ? 1.0 : 0.0;
      case CmpOp::LT:
        return l < r ? 1.0 : 0.0;
      case CmpOp::LTE:
        return l <= r ? 1.0 : 0.0;
      case CmpOp::GT:
        return l > r ? 1.0 : 0.0;
      case CmpOp::GTE:
        return l >= r ? 1.0 : 0.0;
    }
  }

  // Lexicographic comparison when both sides are strings.
  if (std::holds_alternative<std::string>(lhs) && std::holds_alternative<std::string>(rhs)) {
    const auto& l = std::get<std::string>(lhs);
    const auto& r = std::get<std::string>(rhs);
    switch (op) {
      case CmpOp::EQ:
        return l == r ? 1.0 : 0.0;
      case CmpOp::NEQ:
        return l != r ? 1.0 : 0.0;
      case CmpOp::LT:
        return l < r ? 1.0 : 0.0;
      case CmpOp::LTE:
        return l <= r ? 1.0 : 0.0;
      case CmpOp::GT:
        return l > r ? 1.0 : 0.0;
      case CmpOp::GTE:
        return l >= r ? 1.0 : 0.0;
    }
  }

  // NULL == NULL -> true; NULL <= NULL, NULL >= NULL -> true.
  if (std::holds_alternative<std::monostate>(lhs) && std::holds_alternative<std::monostate>(rhs))
    return (op == CmpOp::EQ || op == CmpOp::LTE || op == CmpOp::GTE) ? 1.0 : 0.0;

  // Mixed types -> only != is true, everything else false.
  return op == CmpOp::NEQ ? 1.0 : 0.0;
}

// Arithmetic evaluation
Value EvalArith(ArithOp op, const Value& lhs, const Value& rhs) {
  // Both operands must be numeric; otherwise propagate null.
  if (!std::holds_alternative<double>(lhs) || !std::holds_alternative<double>(rhs))
    return {};
  double l = std::get<double>(lhs);
  double r = std::get<double>(rhs);
  switch (op) {
    case ArithOp::ADD:
      return l + r;
    case ArithOp::SUB:
      return l - r;
    case ArithOp::MUL:
      return l * r;
    case ArithOp::DIV:
      return r != 0.0 ? Value{l / r} : Value{};
    case ArithOp::MOD:
      return r != 0.0 ? Value{std::fmod(l, r)} : Value{};
    case ArithOp::POW:
      return std::pow(l, r);
  }
  return {};
}

// Visitor
struct EvalVisitor {
  const DocValues& doc;

  Value operator()(const FieldRef& n) const {
    auto it = doc.find(n.name);
    return it != doc.end() ? it->second : Value{};
  }

  Value operator()(const NumLiteral& n) const {
    return n.value;
  }

  Value operator()(const StrLiteral& n) const {
    return n.value;
  }

  Value operator()(const NullLiteral& /*n*/) const {
    return {};
  }

  Value operator()(const CmpExpr& n) const {
    return EvalCmp(n.op, EvalNode(*n.lhs, doc), EvalNode(*n.rhs, doc));
  }

  Value operator()(const ArithExpr& n) const {
    return EvalArith(n.op, EvalNode(*n.lhs, doc), EvalNode(*n.rhs, doc));
  }

  Value operator()(const LogicExpr& n) const {
    if (n.op == LogicOp::AND) {
      // Short-circuit: don't evaluate RHS if LHS is false.
      if (!IsTruthy(EvalNode(*n.lhs, doc)))
        return 0.0;
      return IsTruthy(EvalNode(*n.rhs, doc)) ? 1.0 : 0.0;
    }
    // OR: short-circuit if LHS is true.
    if (IsTruthy(EvalNode(*n.lhs, doc)))
      return 1.0;
    return IsTruthy(EvalNode(*n.rhs, doc)) ? 1.0 : 0.0;
  }

  Value operator()(const NotExpr& n) const {
    return IsTruthy(EvalNode(*n.operand, doc)) ? 0.0 : 1.0;
  }

  Value operator()(const NegateExpr& n) const {
    Value v = EvalNode(*n.operand, doc);
    if (!std::holds_alternative<double>(v))
      return {};
    return -std::get<double>(v);
  }

  Value operator()(const FuncCallExpr& n) const {
    const FuncImpl* fn = FindFilterFunction(n.name);
    if (!fn)
      return {};  // unknown function -> null

    std::vector<Value> arg_vals;
    arg_vals.reserve(n.args.size());
    for (const auto& arg : n.args)
      arg_vals.push_back(EvalNode(*arg, doc));

    return (*fn)(arg_vals);
  }
};

Value EvalNode(const FilterExprNode& node, const DocValues& doc) {
  return std::visit(EvalVisitor{doc}, static_cast<const FilterExprVariant&>(node));
}

}  // namespace

Value EvalFilterExpr(const FilterExprNode& node, const DocValues& doc) {
  return EvalNode(node, doc);
}

}  // namespace dfly::aggregate
