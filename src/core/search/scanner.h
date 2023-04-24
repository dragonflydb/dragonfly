// Copyright 2023, Roman Gershman.  All rights reserved.
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

  std::string matched() {
    return yytext;
  }
};

}  // namespace search
}  // namespace dfly
