// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include "core/search/parser.hh"

namespace dfly {
namespace search {

class QueryDriver;

class Scanner : public yyFlexLexer {
 public:
  Scanner() {
  }

  Parser::symbol_type ParserLex(QueryDriver& drv);

 private:
  std::string Matched() const {
    return std::string(YYText(), YYLeng());
  }
};

}  // namespace search
}  // namespace dfly
