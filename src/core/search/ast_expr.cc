// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cmath>
#include <regex>

#include "base/logging.h"

using namespace std;

namespace dfly::search {

AstRangeNode::AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl)
    : lo{lo_excl ? nextafter(lo, hi) : lo}, hi{hi_excl ? nextafter(hi, lo) : hi} {
}

AstGeoNode::AstGeoNode(double lon, double lat, double radius, std::string unit)
    : lon(lon), lat(lat), radius(radius), unit(std::move(unit)) {
}

AstOptionalNode::AstOptionalNode(AstNode&& node) : node{make_unique<AstNode>(std::move(node))} {
}

AstNegateNode::AstNegateNode(AstNode&& node) : node{make_unique<AstNode>(std::move(node))} {
}

AstAttributeNode::AstAttributeNode(AstNode&& node, double weight)
    : node{make_unique<AstNode>(std::move(node))}, weight{weight} {
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

AstKnnNode::AstKnnNode(uint32_t limit, std::string_view field, std::string blob,
                       std::string_view score_alias, std::optional<uint32_t> ef_runtime)
    : filter{nullptr},
      limit{limit},
      field{field.substr(1)},
      blob{std::move(blob)},
      score_alias{score_alias.empty() ? absl::StrCat("__", field.substr(1), "_score")
                                      : std::string{score_alias}},
      ef_runtime{ef_runtime} {
}

AstKnnNode::AstKnnNode(AstNode&& filter, AstKnnNode&& self) {
  *this = std::move(self);
  this->filter = make_unique<AstNode>(std::move(filter));
}

AstVectorRangeNode::AstVectorRangeNode(std::string field, double radius, std::string blob,
                                       std::string score_alias, std::optional<double> epsilon)
    : field{field.substr(1)},
      radius{radius},
      blob{std::move(blob)},
      score_alias{std::move(score_alias)},
      epsilon{epsilon} {
}

bool AstKnnNode::HasPreFilter() const {
  // If we have pre filter knn query should not hold filter variable. It will be
  // moved to SearchAlgorithm::query_ variable.
  return filter == nullptr;
}

AstNode::~AstNode() noexcept {
  if (holds_alternative<monostate>(*this))
    return;

  // Reuse a link in each node instead of allocating a work stack. Keep ownership intact while
  // building reverse preorder, so every descendant appears before its owner in the cleanup list.
  AstNode* pending = this;
  AstNode* reverse_order = nullptr;
  teardown_next_ = nullptr;
  while (pending) {
    AstNode* node = pending;
    pending = node->teardown_next_;
    ForEachChild(*node, {}, [&pending](AstNode& child, string_view) {
      child.teardown_next_ = pending;
      pending = &child;
    });
    node->teardown_next_ = reverse_order;
    reverse_order = node;
  }

  while (reverse_order) {
    AstNode* node = reverse_order;
    reverse_order = node->teardown_next_;
    // Children are already empty: releasing their unique_ptr/vector storage only invokes the
    // early return above, keeping the call stack bounded regardless of the tree's depth.
    node->emplace<monostate>();
  }
}

}  // namespace dfly::search

namespace std {
ostream& operator<<(ostream& os, optional<uint32_t> o) {
  return os;
}

}  // namespace std
