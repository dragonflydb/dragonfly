// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <algorithm>
#include <regex>

using namespace std;

namespace dfly::search {

AstTermNode::AstTermNode(std::string term)
    : term_{move(term)}, pattern_{"\\b" + term_ + "\\b", std::regex::icase} {
}

bool AstTermNode::Check(string_view input) const {
  return regex_search(input.begin(), input.begin() + input.size(), pattern_);
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
  return op_ == kOr ? (l_->Check(input) || r_->Check(input))
                    : (l_->Check(input) && r_->Check(input));
}

string AstLogicalNode::Debug() const {
  string op = op_ == kOr ? "or" : "and";
  return op + "{" + l_->Debug() + "," + r_->Debug() + "}";
}

}  // namespace dfly::search
