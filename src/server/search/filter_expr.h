// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace dfly::aggregate {

// Forward declaration -- FilterExpr is a pointer to a node
struct FilterExprNode;

// Keeps destruction of deeply nested expressions iterative and allocation-free.
struct FilterExprDeleter {
  FilterExprDeleter() = default;

  // Allow nodes created with std::make_unique to transfer ownership into FilterExpr.
  template <typename T> FilterExprDeleter(std::default_delete<T>) {
  }

  void operator()(FilterExprNode* node) const noexcept;
};

// Owning pointer to a filter expression node.
// Enables recursive structures (nodes contain children of the same type).
using FilterExpr = std::unique_ptr<FilterExprNode, FilterExprDeleter>;

// Nodes own their children through FilterExpr.
struct FilterExprNode {
  enum class Type {
    FIELD_REF,
    NUM_LITERAL,
    STR_LITERAL,
    NULL_LITERAL,
    CMP,
    ARITH,
    LOGIC,
    NOT,
    NEGATE,
    FUNC_CALL,
  };

  virtual ~FilterExprNode() = default;
  FilterExprNode(const FilterExprNode&) = delete;
  FilterExprNode& operator=(const FilterExprNode&) = delete;

  Type type() const {
    return type_;
  }

 protected:
  explicit FilterExprNode(Type type) : type_{type} {
  }

  static void EnqueueChild(FilterExpr& child, FilterExprNode*& pending) noexcept {
    if (auto* node = child.release()) {
      node->teardown_next_ = pending;
      pending = node;
    }
  }

 private:
  friend struct FilterExprDeleter;
  virtual void Destroy(FilterExprNode*& pending) noexcept = 0;

  const Type type_;
  FilterExprNode* teardown_next_ = nullptr;
};

template <typename Derived, FilterExprNode::Type kNodeType>
struct TypedFilterExprNode : FilterExprNode {
 protected:
  TypedFilterExprNode() : FilterExprNode(kNodeType) {
  }

 private:
  void Destroy(FilterExprNode*& pending) noexcept final {
    auto& self = static_cast<Derived&>(*this);
    if constexpr (requires { self.rhs; }) {
      EnqueueChild(self.lhs, pending);
      EnqueueChild(self.rhs, pending);
    } else if constexpr (requires { self.operand; }) {
      EnqueueChild(self.operand, pending);
    } else if constexpr (requires { self.args; }) {
      for (auto& arg : self.args)
        EnqueueChild(arg, pending);
    }
    delete &self;
  }
};

// Leaf nodes

// Reference to a pipeline field: @fieldname (stored without '@')
struct FieldRef final : TypedFilterExprNode<FieldRef, FilterExprNode::Type::FIELD_REF> {
  explicit FieldRef(std::string name) : name{std::move(name)} {
  }

  std::string name;
};

// Numeric literal: 3.14, 1e5, inf
struct NumLiteral final : TypedFilterExprNode<NumLiteral, FilterExprNode::Type::NUM_LITERAL> {
  explicit NumLiteral(double value) : value{value} {
  }

  double value;
};

// String literal: 'hello' or "hello"
struct StrLiteral final : TypedFilterExprNode<StrLiteral, FilterExprNode::Type::STR_LITERAL> {
  explicit StrLiteral(std::string value) : value{std::move(value)} {
  }

  std::string value;
};

// NULL literal
struct NullLiteral final : TypedFilterExprNode<NullLiteral, FilterExprNode::Type::NULL_LITERAL> {};

// Operator enums

enum class CmpOp { EQ, NEQ, LT, LTE, GT, GTE };
enum class ArithOp { ADD, SUB, MUL, DIV, MOD, POW };
enum class LogicOp { AND, OR };

// Composite nodes (children stored as FilterExpr / unique_ptr)

struct CmpExpr final : TypedFilterExprNode<CmpExpr, FilterExprNode::Type::CMP> {
  CmpExpr(CmpOp op, FilterExpr lhs, FilterExpr rhs)
      : op{op}, lhs{std::move(lhs)}, rhs{std::move(rhs)} {
  }

  CmpOp op;
  FilterExpr lhs, rhs;
};

struct ArithExpr final : TypedFilterExprNode<ArithExpr, FilterExprNode::Type::ARITH> {
  ArithExpr(ArithOp op, FilterExpr lhs, FilterExpr rhs)
      : op{op}, lhs{std::move(lhs)}, rhs{std::move(rhs)} {
  }

  ArithOp op;
  FilterExpr lhs, rhs;
};

// Logical AND / OR
struct LogicExpr final : TypedFilterExprNode<LogicExpr, FilterExprNode::Type::LOGIC> {
  LogicExpr(LogicOp op, FilterExpr lhs, FilterExpr rhs)
      : op{op}, lhs{std::move(lhs)}, rhs{std::move(rhs)} {
  }

  LogicOp op;
  FilterExpr lhs, rhs;
};

// Logical NOT: !operand
struct NotExpr final : TypedFilterExprNode<NotExpr, FilterExprNode::Type::NOT> {
  explicit NotExpr(FilterExpr operand) : operand{std::move(operand)} {
  }

  FilterExpr operand;
};

// Unary minus: -operand
struct NegateExpr final : TypedFilterExprNode<NegateExpr, FilterExprNode::Type::NEGATE> {
  explicit NegateExpr(FilterExpr operand) : operand{std::move(operand)} {
  }

  FilterExpr operand;
};

// Function call: lower(@name), exists(@field), abs(@val), year(@ts), ...
// name is always lowercase.
struct FuncCallExpr final : TypedFilterExprNode<FuncCallExpr, FilterExprNode::Type::FUNC_CALL> {
  FuncCallExpr(std::string name, std::vector<FilterExpr> args)
      : name{std::move(name)}, args{std::move(args)} {
  }

  std::string name;
  std::vector<FilterExpr> args;
};

// ---------------------------------------------------------------------------
// Parse result
// ---------------------------------------------------------------------------

// On success: holds a FilterExpr (non-null unique_ptr).
// On failure: holds a std::string with the error message.
using FilterParseResult = std::variant<FilterExpr, std::string>;

}  // namespace dfly::aggregate
