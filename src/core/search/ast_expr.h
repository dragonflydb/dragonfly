// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <ostream>
#include <regex>
#include <vector>

#include "core/search/base.h"

namespace dfly {

namespace search {

// Describes a single node of the filter AST tree.
class AstNode {
 public:
  virtual ~AstNode() = default;

  // Check if this input is matched by the node.
  virtual bool Check(SearchInput*) const = 0;

  // Debug print node.
  virtual std::string Debug() const = 0;
};

using NodePtr = std::shared_ptr<AstNode>;
using AstExpr = NodePtr;

template <typename T, typename... Ts> AstExpr MakeExpr(Ts&&... ts) {
  return std::make_shared<T>(std::forward<Ts>(ts)...);
}

// AST term node, matches only if input contains term.
class AstTermNode : public AstNode {
 public:
  AstTermNode(std::string term);
  virtual bool Check(SearchInput*) const;
  virtual std::string Debug() const;

 private:
  std::string term_;
  std::regex pattern_;
};

// Ast negation node, matches only if its subtree didn't match.
class AstNegateNode : public AstNode {
 public:
  AstNegateNode(NodePtr node) : node_{node} {
  }
  bool Check(SearchInput*) const override;
  std::string Debug() const override;

 private:
  NodePtr node_;
};

// Ast logical operation node, matches only if subtrees match
// in respect to logical operation (and/or).
class AstLogicalNode : public AstNode {
 public:
  enum Op {
    kAnd,
    kOr,
  };

  AstLogicalNode(NodePtr l, NodePtr r, Op op) : l_{l}, r_{r}, op_{op} {
  }
  bool Check(SearchInput*) const override;
  std::string Debug() const override;

 private:
  NodePtr l_, r_;
  Op op_;
};

// Ast field node, selects a field from the input for its subtree.
class AstFieldNode : public AstNode {
 public:
  AstFieldNode(std::string field, NodePtr node) : field_{field.substr(1)}, node_{node} {
  }

  bool Check(SearchInput*) const override;
  std::string Debug() const override;

 private:
  std::string field_;
  NodePtr node_;
};

// Ast range node, checks if input is inside int range
class AstRangeNode : public AstNode {
 public:
  AstRangeNode(int64_t l, int64_t r) : l_{l}, r_{r} {
  }

  bool Check(SearchInput*) const override;
  std::string Debug() const override;

 private:
  int64_t l_, r_;
};

}  // namespace search
}  // namespace dfly

namespace std {

inline std::ostream& operator<<(std::ostream& os, const dfly::search::AstExpr& ast) {
  os << "ast{" << ast->Debug() << "}";
  return os;
}

}  // namespace std
