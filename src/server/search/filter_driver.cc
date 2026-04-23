// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/filter_driver.h"

#include "base/logging.h"
#include "server/search/filter_parser.hh"
#include "server/search/filter_scanner.h"

namespace dfly::aggregate {

FilterDriver::FilterDriver() : scanner_(std::make_unique<FilterScanner>()) {
}

FilterDriver::~FilterDriver() = default;

void FilterDriver::SetInput(std::string str) {
  cur_str_ = std::move(str);
  scanner_->in(cur_str_);
}

void FilterDriver::Error(const FilterParser::location_type& loc, const std::string& msg) {
  VLOG(1) << "Filter parse error " << loc << ": " << msg;
  if (error_.empty())
    error_ = msg;
}

FilterParseResult ParseFilterExpr(std::string_view input) {
  FilterDriver driver;
  driver.SetInput(std::string{input});
  FilterParser parser(&driver);
  parser.parse();
  return driver.TakeResult();
}

}  // namespace dfly::aggregate
