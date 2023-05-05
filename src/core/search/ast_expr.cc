// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/ast_expr.h"

#include <absl/strings/numbers.h>

#include <algorithm>
#include <regex>

using namespace std;

namespace dfly::search {

AstTermNode::AstTermNode(std::string term)
    : term_{move(term)}, pattern_{"\\b" + term_ + "\\b", std::regex::icase} {
}

bool AstTermNode::Check(SearchInput* input) const {
  return input->Check([this](string_view str) {
    return regex_search(str.begin(), str.begin() + str.size(), pattern_);
  });
}

string AstTermNode::Debug() const {
  return "term{" + term_ + "}";
}

bool AstNegateNode::Check(SearchInput* input) const {
  return !node_->Check(input);
}

string AstNegateNode::Debug() const {
  return "not{" + node_->Debug() + "}";
}

bool AstLogicalNode::Check(SearchInput* input) const {
  return op_ == kOr ? (l_->Check(input) || r_->Check(input))
                    : (l_->Check(input) && r_->Check(input));
}

string AstLogicalNode::Debug() const {
  string op = op_ == kOr ? "or" : "and";
  return op + "{" + l_->Debug() + "," + r_->Debug() + "}";
}

bool AstFieldNode::Check(SearchInput* input) const {
  input->SelectField(field_);
  bool res = node_->Check(input);
  input->ClearField();
  return res;
}

string AstFieldNode::Debug() const {
  return "field:" + field_ + "{" + node_->Debug() + "}";
}

bool AstRangeNode::Check(SearchInput* input) const {
  return input->Check([this](string_view str) {
    int64_t v;
    if (!absl::SimpleAtoi(str, &v))
      return false;
    return l_ <= v && v <= r_;
  });
}

string AstRangeNode::Debug() const {
  return "range{" + to_string(l_) + " " + to_string(r_) + "}";
}

}  // namespace dfly::search
