// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "core/search/ast_expr.h"
#include "core/search/base.h"
#include "core/search/parser.hh"
#include "core/search/scanner.h"

namespace dfly {

namespace search {

class QueryDriver {
 public:
  QueryDriver();
  ~QueryDriver();

  void SetInput(std::string str) {
    cur_str_ = std::move(str);
    scanner()->in(cur_str_);
  }

  void SetParams(const QueryParams* params) {
    params_ = params;
    scanner_->SetParams(params);
  }

  Parser::symbol_type Lex() {
    return scanner()->Lex();
  }

  void ResetScanner();

  void Set(AstExpr expr) {
    expr_ = std::move(expr);
  }

  AstExpr Take() {
    return std::move(expr_);
  }

  const QueryParams& GetParams() const {
    return *params_;
  }

  Scanner* scanner() {
    return scanner_.get();
  }

  void Error(const Parser::location_type& loc, std::string_view msg);

 public:
  Parser::location_type location;

 private:
  const QueryParams* params_;
  AstExpr expr_;

  std::string cur_str_;
  std::unique_ptr<Scanner> scanner_;
};

}  // namespace search
}  // namespace dfly
