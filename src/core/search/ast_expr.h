// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <ostream>
#include <regex>
#include <variant>
#include <vector>

namespace dfly {

namespace search {

struct AstNode;

struct AstTermNode {
  AstTermNode(std::string term);

  std::string term;
  std::regex pattern;
};

struct AstRangeNode {
  AstRangeNode(int64_t lo, int64_t hi);

  int64_t lo, hi;
};

struct AstNegateNode {
  AstNegateNode(AstNode&& node);

  std::unique_ptr<AstNode> node;
};

struct AstLogicalNode {
  enum LogicOp { AND, OR };

  AstLogicalNode(AstNode&& l, AstNode&& r, LogicOp op);

  LogicOp op;
  std::vector<AstNode> nodes;
};

struct AstFieldNode {
  AstFieldNode(std::string field, AstNode&& node);

  std::string field;
  std::unique_ptr<AstNode> node;
};

using NodeVariants = std::variant<std::monostate, AstTermNode, AstRangeNode, AstNegateNode,
                                  AstLogicalNode, AstFieldNode>;
struct AstNode : NodeVariants {
  using variant::variant;
};

using AstExpr = AstNode;

}  // namespace search
}  // namespace dfly

namespace std {

inline std::ostream& operator<<(std::ostream& os, const dfly::search::AstExpr& ast) {
  // os << "ast{" << ast->Debug() << "}";
  return os;
}

}  // namespace std
