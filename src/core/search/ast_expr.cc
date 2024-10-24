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

AstTermNode::AstTermNode(string term) : term{std::move(term)} {
}

AstPrefixNode::AstPrefixNode(string prefix) : prefix{std::move(prefix)} {
  this->prefix.pop_back();
}

AstRangeNode::AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl)
    : lo{lo_excl ? nextafter(lo, hi) : lo}, hi{hi_excl ? nextafter(hi, lo) : hi} {
}

AstNegateNode::AstNegateNode(AstNode&& node) : node{make_unique<AstNode>(std::move(node))} {
}

AstLogicalNode::AstLogicalNode(AstNode&& l, AstNode&& r, LogicOp op) : op{op}, nodes{} {
  // If either node is already a logical node with the same op,
  // we can re-use it, as logical ops are associative.
  for (auto* node : {&l, &r}) {
    if (auto* ln = get_if<AstLogicalNode>(node); ln && ln->op == op) {
      *this = std::move(*ln);
      nodes.emplace_back(std::move(*(node == &l ? &r : &l)));
      return;
    }
  }

  nodes.emplace_back(std::move(l));
  nodes.emplace_back(std::move(r));
}

AstFieldNode::AstFieldNode(string field, AstNode&& node)
    : field{field.substr(1)}, node{make_unique<AstNode>(std::move(node))} {
}

AstTagsNode::AstTagsNode(TagValue tag) {
  tags = {std::move(tag)};
}

AstTagsNode::AstTagsNode(AstExpr&& l, TagValue tag) {
  DCHECK(holds_alternative<AstTagsNode>(l));
  auto& tags_node = get<AstTagsNode>(l);

  tags = std::move(tags_node.tags);
  tags.push_back(std::move(tag));
}

AstKnnNode::AstKnnNode(uint32_t limit, std::string_view field, OwnedFtVector vec,
                       std::string_view score_alias, std::optional<size_t> ef_runtime)
    : filter{nullptr},
      limit{limit},
      field{field.substr(1)},
      vec{std::move(vec)},
      score_alias{score_alias},
      ef_runtime{ef_runtime} {
}

AstKnnNode::AstKnnNode(AstNode&& filter, AstKnnNode&& self) {
  *this = std::move(self);
  this->filter = make_unique<AstNode>(std::move(filter));
}

}  // namespace dfly::search

namespace std {
ostream& operator<<(ostream& os, optional<size_t> o) {
  return os;
}

ostream& operator<<(ostream& os, dfly::search::AstTagsNode::TagValueProxy o) {
  return os;
}
}  // namespace std
