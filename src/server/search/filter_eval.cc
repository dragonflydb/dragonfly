// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/filter_eval.h"

#include <cmath>
#include <variant>
#include <vector>

#include "base/logging.h"
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
      return l / r;
    case ArithOp::MOD:
      return std::fmod(l, r);
    case ArithOp::POW:
      return std::pow(l, r);
  }
  return {};
}

}  // namespace

FilterExprEvaluator::FilterExprEvaluator(const FilterExprNode& root) {
  using Type = FilterExprNode::Type;
  struct Pending {
    const FilterExprNode* node;
    uint32_t done;  // children already compiled
    uint32_t jump;  // kAnd or kOr instruction to patch
  };

  std::vector<Pending> pending{{&root, 0, 0}};
  while (!pending.empty()) {
    Pending& p = pending.back();
    const FilterExprNode* child = nullptr;
    auto emit = [&](Op op, uint32_t arg = 0, const FuncImpl* fn = nullptr) {
      code_.push_back({op, arg, p.node, fn});
    };
    auto binary = [&](const auto& n, Op op) {
      if (p.done < 2)
        child = p.done == 0 ? n.lhs.get() : n.rhs.get();
      else
        emit(op);
    };
    auto unary = [&](const auto& n, Op op) {
      if (p.done == 0)
        child = n.operand.get();
      else
        emit(op);
    };

    switch (p.node->type()) {
      case Type::FIELD_REF:
        emit(Op::kField);
        break;
      case Type::NUM_LITERAL:
        emit(Op::kNum);
        break;
      case Type::STR_LITERAL:
        emit(Op::kStr);
        break;
      case Type::NULL_LITERAL:
        emit(Op::kNull);
        break;
      case Type::CMP:
        binary(static_cast<const CmpExpr&>(*p.node), Op::kCmp);
        break;
      case Type::ARITH:
        binary(static_cast<const ArithExpr&>(*p.node), Op::kArith);
        break;
      case Type::NOT:
        unary(static_cast<const NotExpr&>(*p.node), Op::kNot);
        break;
      case Type::NEGATE:
        unary(static_cast<const NegateExpr&>(*p.node), Op::kNegate);
        break;
      case Type::LOGIC: {
        const auto& n = static_cast<const LogicExpr&>(*p.node);
        if (p.done == 0) {
          child = n.lhs.get();
        } else if (p.done == 1) {
          p.jump = code_.size();
          emit(n.op == LogicOp::AND ? Op::kAnd : Op::kOr);
          child = n.rhs.get();
        } else {
          emit(Op::kToBool);
          code_[p.jump].arg = code_.size();
        }
        break;
      }
      case Type::FUNC_CALL: {
        const auto& n = static_cast<const FuncCallExpr&>(*p.node);
        const FuncImpl* fn = FindFilterFunction(n.name);
        if (!fn)
          emit(Op::kNull);  // unknown function -> null, its arguments are not evaluated
        else if (p.done < n.args.size())
          child = n.args[p.done].get();
        else
          emit(Op::kCall, n.args.size(), fn);
        break;
      }
    }

    if (child) {
      ++p.done;
      pending.push_back({child, 0, 0});
    } else {
      pending.pop_back();
    }
  }
}

Value FilterExprEvaluator::Eval(const DocValues& doc) {
  stack_.clear();

  // A local span stays in registers, while code_ would be reloaded after every stack_ write.
  const absl::Span<const Instr> code{code_};
  for (size_t pc = 0; pc < code.size();) {
    const Instr& in = code[pc++];
    switch (in.op) {
      case Op::kField: {
        auto it = doc.find(static_cast<const FieldRef*>(in.node)->name);
        if (it != doc.end())
          stack_.push_back(it->second);
        else
          stack_.emplace_back();
        break;
      }
      case Op::kNum:
        stack_.emplace_back(static_cast<const NumLiteral*>(in.node)->value);
        break;
      case Op::kStr:
        stack_.emplace_back(static_cast<const StrLiteral*>(in.node)->value);
        break;
      case Op::kNull:
        stack_.emplace_back();
        break;

      // Paired operators share a case: separate cases measured up to 20% slower on aarch64.
      case Op::kCmp:
      case Op::kArith: {
        const Value& lhs = stack_[stack_.size() - 2];
        const Value& rhs = stack_.back();
        Value result = in.op == Op::kCmp
                           ? EvalCmp(static_cast<const CmpExpr*>(in.node)->op, lhs, rhs)
                           : EvalArith(static_cast<const ArithExpr*>(in.node)->op, lhs, rhs);
        stack_.pop_back();
        stack_.back() = std::move(result);
        break;
      }
      case Op::kAnd:
      case Op::kOr:
        // && stops at a falsy left operand and || at a truthy one; otherwise the right one decides.
        if (IsTruthy(stack_.back()) == (in.op == Op::kOr)) {
          stack_.back() = in.op == Op::kOr ? 1.0 : 0.0;
          pc = in.arg;
        } else {
          stack_.pop_back();
        }
        break;
      case Op::kToBool:
        stack_.back() = IsTruthy(stack_.back()) ? 1.0 : 0.0;
        break;
      case Op::kNot:
        stack_.back() = IsTruthy(stack_.back()) ? 0.0 : 1.0;
        break;
      case Op::kNegate:
        if (const double* d = std::get_if<double>(&stack_.back()))
          stack_.back() = -*d;
        else
          stack_.back() = Value{};
        break;
      case Op::kCall: {
        const size_t first = stack_.size() - in.arg;
        Value result = (*in.fn)(absl::MakeConstSpan(stack_).subspan(first));
        stack_.resize(first);
        stack_.push_back(std::move(result));
        break;
      }
    }
  }
  DCHECK_EQ(stack_.size(), 1u);
  return std::move(stack_.back());
}

Value EvalFilterExpr(const FilterExprNode& node, const DocValues& doc) {
  return FilterExprEvaluator{node}.Eval(doc);
}

}  // namespace dfly::aggregate
