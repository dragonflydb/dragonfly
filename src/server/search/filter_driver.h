// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "server/search/filter_expr.h"
#include "server/search/filter_parser.hh"
#include "server/search/filter_scanner.h"

namespace dfly {
namespace aggregate {

class FilterDriver {
 public:
  FilterDriver();
  ~FilterDriver();

  void SetInput(std::string str);

  FilterScanner* scanner() {
    return scanner_.get();
  }

  void Set(FilterExpr expr) {
    expr_ = std::move(expr);
  }

  void Error(const FilterParser::location_type& loc, const std::string& msg);

  // Constructs and returns the parse result as a variant.
  // Must be called at most once after parsing.
  FilterParseResult TakeResult() {
    if (expr_)
      return std::move(expr_);
    return error_.empty() ? std::string{"parse failed"} : std::move(error_);
  }

 private:
  FilterExpr expr_;
  std::string error_;
  std::string cur_str_;
  std::unique_ptr<FilterScanner> scanner_;
};

// Parse a FILTER expression string into an AST.
// Returns a FilterParseResult; check operator bool() before using expr.
FilterParseResult ParseFilterExpr(std::string_view input);

}  // namespace aggregate
}  // namespace dfly
