// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <utility>

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

  Scanner* scanner() {
    return scanner_.get();
  }

  void SetInput(std::string str) {
    cur_str_ = std::move(str);
    scanner()->in(cur_str_);
  }

  void SetParams(QueryParams params) {
    params_ = std::move(params);
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

  const QueryParams& GetParams() {
    return params_;
  }

 public:
  Parser::location_type location;

 private:
  QueryParams params_;
  AstExpr expr_;

  std::string cur_str_;
  std::unique_ptr<Scanner> scanner_;
};

}  // namespace search
}  // namespace dfly
