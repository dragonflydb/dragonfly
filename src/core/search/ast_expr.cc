// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <absl/strings/numbers.h>

#include <algorithm>
#include <cmath>
#include <regex>

#include "base/logging.h"

using namespace std;

namespace dfly::search {

AstTermNode::AstTermNode(string term) : term{term} {
}

AstRangeNode::AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl)
    : lo{lo_excl ? nextafter(lo, hi) : lo}, hi{hi_excl ? nextafter(hi, lo) : hi} {
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

AstKnnNode::AstKnnNode(uint32_t limit, std::string_view field, OwnedFtVector vec,
                       std::string_view score_alias)
    : filter{nullptr},
      limit{limit},
      field{field.substr(1)},
      vec{std::move(vec)},
      score_alias{score_alias} {
}

AstKnnNode::AstKnnNode(AstNode&& filter, AstKnnNode&& self) {
  *this = std::move(self);
  this->filter = make_unique<AstNode>(std::move(filter));
}

}  // namespace dfly::search
