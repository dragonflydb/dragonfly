// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <absl/strings/numbers.h>

#include <algorithm>
#include <regex>

#include "base/logging.h"

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
  // If either node is already a logical node with the same op,
  // we can re-use it, as logical ops are associative.
  for (auto* node : {&l, &r}) {
    if (auto* ln = get_if<AstLogicalNode>(node); ln && ln->op == op) {
      *this = move(*ln);
      nodes.emplace_back(move(*(node == &l ? &r : &l)));
      return;
    }
  }

  nodes.emplace_back(move(l));
  nodes.emplace_back(move(r));
}

AstFieldNode::AstFieldNode(string field, AstNode&& node)
    : field{field.substr(1)}, node{make_unique<AstNode>(move(node))} {
}

AstTagsNode::AstTagsNode(std::string tag) {
  tags = {move(tag)};
}

AstTagsNode::AstTagsNode(AstExpr&& l, std::string tag) {
  DCHECK(holds_alternative<AstTagsNode>(l));
  auto& tags_node = get<AstTagsNode>(l);

  tags = move(tags_node.tags);
  tags.push_back(move(tag));
}

}  // namespace dfly::search
