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

// Matches terms in text fields
struct AstTermNode {
  AstTermNode(std::string term);

  std::string term;
  std::regex pattern;
};

// Matches numeric range
struct AstRangeNode {
  AstRangeNode(int64_t lo, int64_t hi);

  int64_t lo, hi;
};

// Negates subtree
struct AstNegateNode {
  AstNegateNode(AstNode&& node);

  std::unique_ptr<AstNode> node;
};

// Applies logical operation to results of all sub-nodes
struct AstLogicalNode {
  enum LogicOp { AND, OR };

  // If either node is already a logical node with the same op, it'll be re-used.
  AstLogicalNode(AstNode&& l, AstNode&& r, LogicOp op);

  LogicOp op;
  std::vector<AstNode> nodes;
};

// Selects specific field for subtree
struct AstFieldNode {
  AstFieldNode(std::string field, AstNode&& node);

  std::string field;
  std::unique_ptr<AstNode> node;
};

// Stores a list of tags for a tag query
struct AstTagsNode {
  AstTagsNode(std::string tag);
  AstTagsNode(AstNode&& l, std::string tag);

  std::vector<std::string> tags;
};

using NodeVariants = std::variant<std::monostate, AstTermNode, AstRangeNode, AstNegateNode,
                                  AstLogicalNode, AstFieldNode, AstTagsNode>;
struct AstNode : public NodeVariants {
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
