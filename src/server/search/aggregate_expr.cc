// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/aggregate_expr.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cctype>

#include "base/logging.h"

namespace dfly::aggregate {

namespace {

// ── Tokenizer ──────────────────────────────────────────────────────────

enum class TokenType {
  FIELD,         // @name
  NUMBER,        // 0.5, 100, -3.14
  STRING,        // 'text'
  LPAREN,        // (
  RPAREN,        // )
  EQ,            // ==
  NE,            // !=
  LT,            // <
  LE,            // <=
  GT,            // >
  GE,            // >=
  AND,           // &&
  OR,            // ||
  END_OF_INPUT,  //
};

struct Token {
  TokenType type;
  std::string_view text;  // points into original input
};

class Tokenizer {
 public:
  explicit Tokenizer(std::string_view input) : input_(input), pos_(0) {
  }

  Token Next() {
    SkipWhitespace();
    if (pos_ >= input_.size())
      return {TokenType::END_OF_INPUT, {}};

    char c = input_[pos_];

    // Field reference: @name
    if (c == '@') {
      size_t start = pos_;
      pos_++;  // skip @
      while (pos_ < input_.size() && (std::isalnum(input_[pos_]) || input_[pos_] == '_'))
        pos_++;
      return {TokenType::FIELD, input_.substr(start + 1, pos_ - start - 1)};
    }

    // String literal: 'text' or "text"
    if (c == '\'' || c == '"') {
      char quote = c;
      pos_++;
      size_t start = pos_;
      while (pos_ < input_.size() && input_[pos_] != quote)
        pos_++;
      auto text = input_.substr(start, pos_ - start);
      if (pos_ < input_.size())
        pos_++;  // skip closing quote
      return {TokenType::STRING, text};
    }

    // Number: digits, optional dot, optional leading minus
    if (std::isdigit(c) ||
        (c == '-' && pos_ + 1 < input_.size() && std::isdigit(input_[pos_ + 1])) ||
        (c == '.' && pos_ + 1 < input_.size() && std::isdigit(input_[pos_ + 1]))) {
      size_t start = pos_;
      if (c == '-')
        pos_++;
      while (pos_ < input_.size() && std::isdigit(input_[pos_]))
        pos_++;
      if (pos_ < input_.size() && input_[pos_] == '.') {
        pos_++;
        while (pos_ < input_.size() && std::isdigit(input_[pos_]))
          pos_++;
      }
      return {TokenType::NUMBER, input_.substr(start, pos_ - start)};
    }

    // Two-character operators
    if (pos_ + 1 < input_.size()) {
      std::string_view two = input_.substr(pos_, 2);
      if (two == "==") {
        pos_ += 2;
        return {TokenType::EQ, two};
      }
      if (two == "!=") {
        pos_ += 2;
        return {TokenType::NE, two};
      }
      if (two == "<=") {
        pos_ += 2;
        return {TokenType::LE, two};
      }
      if (two == ">=") {
        pos_ += 2;
        return {TokenType::GE, two};
      }
      if (two == "&&") {
        pos_ += 2;
        return {TokenType::AND, two};
      }
      if (two == "||") {
        pos_ += 2;
        return {TokenType::OR, two};
      }
    }

    // Single-character operators
    if (c == '<') {
      pos_++;
      return {TokenType::LT, input_.substr(pos_ - 1, 1)};
    }
    if (c == '>') {
      pos_++;
      return {TokenType::GT, input_.substr(pos_ - 1, 1)};
    }
    if (c == '(') {
      pos_++;
      return {TokenType::LPAREN, input_.substr(pos_ - 1, 1)};
    }
    if (c == ')') {
      pos_++;
      return {TokenType::RPAREN, input_.substr(pos_ - 1, 1)};
    }

    // Unknown character — skip it
    pos_++;
    return Next();
  }

  Token Peek() {
    size_t saved = pos_;
    Token t = Next();
    pos_ = saved;
    return t;
  }

 private:
  void SkipWhitespace() {
    while (pos_ < input_.size() && std::isspace(input_[pos_]))
      pos_++;
  }

  std::string_view input_;
  size_t pos_;
};

// ── Recursive descent parser ───────────────────────────────────────────
//
// Grammar:
//   expr     → and_expr ( '||' and_expr )*
//   and_expr → cmp_expr ( '&&' cmp_expr )*
//   cmp_expr → atom (cmp_op atom)?
//   cmp_op   → '==' | '!=' | '<' | '<=' | '>' | '>='
//   atom     → '(' expr ')' | FIELD | NUMBER | STRING

class Parser {
 public:
  explicit Parser(Tokenizer* tok) : tok_(tok) {
  }

  ExprPtr Parse() {
    auto result = ParseOr();
    return result;
  }

 private:
  ExprPtr ParseOr() {
    auto left = ParseAnd();
    if (!left)
      return nullptr;
    while (tok_->Peek().type == TokenType::OR) {
      tok_->Next();  // consume ||
      auto right = ParseAnd();
      if (!right)
        return nullptr;
      left = std::make_unique<Expr>(Logical{std::move(left), LogicOp::OR, std::move(right)});
    }
    return left;
  }

  ExprPtr ParseAnd() {
    auto left = ParseComparison();
    if (!left)
      return nullptr;
    while (tok_->Peek().type == TokenType::AND) {
      tok_->Next();  // consume &&
      auto right = ParseComparison();
      if (!right)
        return nullptr;
      left = std::make_unique<Expr>(Logical{std::move(left), LogicOp::AND, std::move(right)});
    }
    return left;
  }

  ExprPtr ParseComparison() {
    auto left = ParseAtom();
    if (!left)
      return nullptr;

    Token peek = tok_->Peek();
    CmpOp op;
    switch (peek.type) {
      case TokenType::EQ:
        op = CmpOp::EQ;
        break;
      case TokenType::NE:
        op = CmpOp::NE;
        break;
      case TokenType::LT:
        op = CmpOp::LT;
        break;
      case TokenType::LE:
        op = CmpOp::LE;
        break;
      case TokenType::GT:
        op = CmpOp::GT;
        break;
      case TokenType::GE:
        op = CmpOp::GE;
        break;
      default:
        return left;  // no comparison operator, just return the atom
    }
    tok_->Next();  // consume the operator

    auto right = ParseAtom();
    if (!right)
      return nullptr;
    return std::make_unique<Expr>(Comparison{std::move(left), op, std::move(right)});
  }

  ExprPtr ParseAtom() {
    Token t = tok_->Peek();
    switch (t.type) {
      case TokenType::LPAREN: {
        tok_->Next();  // consume (
        auto inner = ParseOr();
        if (!inner)
          return nullptr;
        // expect )
        if (tok_->Peek().type == TokenType::RPAREN)
          tok_->Next();
        return inner;
      }
      case TokenType::FIELD: {
        tok_->Next();
        return std::make_unique<Expr>(FieldRef{std::string(t.text)});
      }
      case TokenType::NUMBER: {
        tok_->Next();
        double val = 0;
        (void)absl::SimpleAtod(t.text, &val);
        return std::make_unique<Expr>(DoubleLiteral{val});
      }
      case TokenType::STRING: {
        tok_->Next();
        return std::make_unique<Expr>(StringLiteral{std::string(t.text)});
      }
      default:
        return nullptr;
    }
  }

  Tokenizer* tok_;
};

// ── Evaluator ──────────────────────────────────────────────────────────

using Value = ::dfly::search::SortableValue;

// Resolve an expression node to a Value (for comparison operands).
Value Resolve(const Expr& expr, const DocValues& doc) {
  return std::visit(
      [&](const auto& node) -> Value {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, FieldRef>) {
          auto it = doc.find(node.name);
          return it != doc.end() ? it->second : Value{};
        } else if constexpr (std::is_same_v<T, DoubleLiteral>) {
          return node.value;
        } else if constexpr (std::is_same_v<T, StringLiteral>) {
          return node.value;
        } else {
          // Comparison/Logical nodes shouldn't appear as comparison operands
          return Value{};
        }
      },
      expr.v);
}

// Try to get both sides as doubles for numeric comparison.
// If a Value holds a string that looks like a number, convert it.
std::optional<std::pair<double, double>> AsDoubles(const Value& lv, const Value& rv) {
  auto to_double = [](const Value& v) -> std::optional<double> {
    if (std::holds_alternative<double>(v))
      return std::get<double>(v);
    if (std::holds_alternative<std::string>(v)) {
      double d;
      if (absl::SimpleAtod(std::get<std::string>(v), &d))
        return d;
    }
    return std::nullopt;
  };
  auto ld = to_double(lv);
  auto rd = to_double(rv);
  if (ld && rd)
    return std::make_pair(*ld, *rd);
  return std::nullopt;
}

bool CompareValues(const Value& lv, CmpOp op, const Value& rv) {
  // Try numeric comparison first
  if (auto doubles = AsDoubles(lv, rv); doubles) {
    auto [l, r] = *doubles;
    switch (op) {
      case CmpOp::EQ:
        return l == r;
      case CmpOp::NE:
        return l != r;
      case CmpOp::LT:
        return l < r;
      case CmpOp::LE:
        return l <= r;
      case CmpOp::GT:
        return l > r;
      case CmpOp::GE:
        return l >= r;
    }
  }

  // Fall back to string comparison
  auto to_str = [](const Value& v) -> std::string {
    if (std::holds_alternative<std::string>(v))
      return std::get<std::string>(v);
    if (std::holds_alternative<double>(v))
      return absl::StrCat(std::get<double>(v));
    return "";
  };

  std::string ls = to_str(lv);
  std::string rs = to_str(rv);

  switch (op) {
    case CmpOp::EQ:
      return ls == rs;
    case CmpOp::NE:
      return ls != rs;
    case CmpOp::LT:
      return ls < rs;
    case CmpOp::LE:
      return ls <= rs;
    case CmpOp::GT:
      return ls > rs;
    case CmpOp::GE:
      return ls >= rs;
  }
  return false;
}

}  // namespace

// ── Public API ─────────────────────────────────────────────────────────

ExprPtr ParseExpression(std::string_view input) {
  Tokenizer tok(input);
  Parser parser(&tok);
  return parser.Parse();
}

bool Evaluate(const Expr& expr, const DocValues& doc) {
  return std::visit(
      [&](const auto& node) -> bool {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, Comparison>) {
          Value lv = Resolve(*node.left, doc);
          Value rv = Resolve(*node.right, doc);
          return CompareValues(lv, node.op, rv);
        } else if constexpr (std::is_same_v<T, Logical>) {
          bool lv = Evaluate(*node.left, doc);
          if (node.op == LogicOp::AND)
            return lv && Evaluate(*node.right, doc);
          else
            return lv || Evaluate(*node.right, doc);
        } else {
          // A bare field/literal evaluates to true if non-empty
          Value v = Resolve(expr, doc);
          return !std::holds_alternative<std::monostate>(v);
        }
      },
      expr.v);
}

AggregationStep MakeFilterStep(ExprPtr expr) {
  auto shared_expr = std::shared_ptr<Expr>(std::move(expr));
  return [shared_expr](Aggregator* agg) {
    auto& values = agg->result.values;
    values.erase(std::remove_if(values.begin(), values.end(),
                                [&](const DocValues& doc) { return !Evaluate(*shared_expr, doc); }),
                 values.end());
  };
}

}  // namespace dfly::aggregate
