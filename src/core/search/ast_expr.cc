// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <absl/strings/str_cat.h>

#include <cmath>

using namespace std;

namespace dfly::search {

AstRangeNode::AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl)
    : lo{lo_excl ? nextafter(lo, hi) : lo}, hi{hi_excl ? nextafter(hi, lo) : hi} {
}

AstGeoNode::AstGeoNode(double lon, double lat, double radius, std::string unit)
    : lon(lon), lat(lat), radius(radius), unit(std::move(unit)) {
}

AstLogicalNode::AstLogicalNode(AstExpr l, AstExpr r, LogicOp op) : op{op} {
  nodes.reserve(2);
  nodes.push_back(std::move(l));
  nodes.push_back(std::move(r));
}

AstExpr AstLogicalNode::Combine(AstExpr l, AstExpr r, LogicOp op) {
  // If either node is already a logical node with the same op,
  // we can re-use it, as logical ops are associative.
  for (auto* node : {&l, &r}) {
    if (auto* logical = (*node)->As<AstLogicalNode>(); logical && logical->op == op) {
      logical->nodes.push_back(std::move(node == &l ? r : l));
      return std::move(*node);
    }
  }

  return make_unique<AstLogicalNode>(std::move(l), std::move(r), op);
}

AstFieldNode::AstFieldNode(string field, AstExpr node)
    : field{field.substr(1)}, node{std::move(node)} {
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

void AstNodeDeleter::operator()(AstNode* node) const noexcept {
  if (!node)
    return;

  // Detach children before deleting their owner. The intrusive work list needs no allocation,
  // even when a failed parse is being unwound after an allocation failure.
  AstNode* pending = node;
  node->teardown_next_ = nullptr;
  while (pending) {
    node = pending;
    pending = node->teardown_next_;
    node->ReleaseChildren(pending);
    delete node;
  }
}

}  // namespace dfly::search

namespace std {
ostream& operator<<(ostream& os, optional<uint32_t> o) {
  return os;
}

}  // namespace std
