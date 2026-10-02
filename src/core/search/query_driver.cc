// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/query_driver.h"

namespace dfly {
namespace search {

QueryDriver::QueryDriver() : scanner_(std::make_unique<Scanner>()) {
  StartGlue({});
}

QueryDriver::~QueryDriver() {
}

void QueryDriver::ResetScanner() {
  scanner_ = std::make_unique<Scanner>();
  scanner_->SetParams(params_);
  StartGlue({});
  // Clear any AST left over from a previous parse: on a reused driver a failed parse never calls
  // Set(), so Take() would otherwise return the stale result of the prior query.
  expr_ = AstExpr{};
}

void QueryDriver::SetInput(std::string str) {
  cur_str_ = std::move(str);
  scanner_->in(cur_str_);
  StartGlue(cur_str_);
}

void QueryDriver::StartGlue(std::string_view input) {
  glue_.emplace(
      [this] {
        Parser::symbol_type sym = scanner_->Lex();
        const auto& m = scanner_->matcher();
        return RawToken{std::move(sym), m.first(), m.last(), m.size() > 0 && *m.begin() == '$'};
      },
      input);
}

void QueryDriver::Error(const Parser::location_type& loc, std::string_view msg) {
  VLOG(1) << "Parse error " << loc << ": " << msg;
}

void QueryDriver::SetOptionalFilters(const OptionalFilters* filters) {
  if (filters) {
    for (auto& [field, filter] : *filters) {
      expr_ = AstLogicalNode(std::move(expr_), filter->Node(field), AstLogicalNode::AND);
    }
  }
}

}  // namespace search

}  // namespace dfly
