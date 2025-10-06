// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/query_driver.h"

namespace dfly {
namespace search {

QueryDriver::QueryDriver() : scanner_(std::make_unique<Scanner>()) {
}

QueryDriver::~QueryDriver() {
}

void QueryDriver::ResetScanner() {
  scanner_ = std::make_unique<Scanner>();
  scanner_->SetParams(params_);
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
