// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <ostream>
#include <vector>

namespace dfly {

namespace search {

// Describes a single node of the filter AST tree.
class AstNode {
 public:
  virtual ~AstNode() = default;

  // Check if this input is matched by the node.
  virtual bool Check(std::string_view input) const = 0;

  // Debug print node.
  virtual std::string Debug() const = 0;
};

using NodePtr = std::shared_ptr<AstNode>;
using AstExpr = NodePtr;

template <typename T, typename... Ts> AstExpr MakeExpr(Ts... ts) {
  auto ptr = std::make_shared<T>(std::forward<Ts>(ts)...);
  return AstExpr{std::reinterpret_pointer_cast<AstNode>(move(ptr))};
}

// AST term node, matches only if input contains term.
class AstTermNode : AstNode {
 public:
  AstTermNode(std::string term) : term_{move(term)} {
  }
  virtual bool Check(std::string_view input) const;
  virtual std::string Debug() const;

 private:
  std::string term_;
};

// Ast negation node, matches only if its sub node didn't match.
class AstNegateNode : AstNode {
 public:
  AstNegateNode(NodePtr node) : node_{node} {
  }
  virtual bool Check(std::string_view needle) const;
  virtual std::string Debug() const;

 private:
  NodePtr node_;
};

// Ast logical operation node, matches only if sub nodes match
// in respect to logical operation (and/or).
class AstLogicalNode : AstNode {
 public:
  AstLogicalNode(NodePtr l, NodePtr r, bool disjunction) : l_{l}, r_{r}, disjunction_{disjunction} {
  }
  virtual bool Check(std::string_view needle) const;
  virtual std::string Debug() const;

 private:
  NodePtr l_, r_;
  bool disjunction_;
};

}  // namespace search
}  // namespace dfly

namespace std {

inline std::ostream& operator<<(std::ostream& os, const dfly::search::AstExpr& ast) {
  os << "ast{" << ast->Debug() << "}";
  return os;
}

}  // namespace std
