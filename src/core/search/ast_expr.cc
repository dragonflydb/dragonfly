// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <absl/strings/numbers.h>

#include <algorithm>
#include <regex>

using namespace std;

namespace dfly::search {

AstTermNode::AstTermNode(string term)
    : term{term}, pattern{"\\b" + term + "\\b", std::regex::icase} {
}

AstRangeNode::AstRangeNode(int64_t lo, int64_t hi) : lo{lo}, hi{hi} {
}

AstNegateNode::AstNegateNode(AstNode&& node) : node{make_unique<AstNode>(move(node))} {
}

AstLogicalNode::AstLogicalNode(AstNode&& l, AstNode&& r, LogicOp op) : op{op}, nodes{} {
  // If the left node is already a logical node with the same op,
  // we can re-use it, as logical ops are associative.
  if (auto* ln = get_if<AstLogicalNode>(&l); ln && ln->op == op) {
    *this = move(*ln);
    nodes.emplace_back(move(r));
    return;
  }

  nodes.emplace_back(move(l));
  nodes.emplace_back(move(r));
}

AstFieldNode::AstFieldNode(string field, AstNode&& node)
    : field{field.substr(1)}, node{make_unique<AstNode>(move(node))} {
}

}  // namespace dfly::search
