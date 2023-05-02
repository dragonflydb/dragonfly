// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <algorithm>

using namespace std;

namespace dfly::search {

bool AstTermNode::Check(string_view input) const {
  return input.find(term_) != string::npos;
}

string AstTermNode::Debug() const {
  return "term{" + term_ + "}";
}

bool AstNegateNode::Check(string_view input) const {
  return !node_->Check(input);
}

string AstNegateNode::Debug() const {
  return "not{" + node_->Debug() + "}";
}

bool AstLogicalNode::Check(string_view input) const {
  return disjunction_ ? (l_->Check(input) || r_->Check(input))
                      : (l_->Check(input) && r_->Check(input));
}

string AstLogicalNode::Debug() const {
  string op = disjunction_ ? "or" : "and";
  return op + "{" + l_->Debug() + "," + r_->Debug() + "}";
}

}  // namespace dfly::search
