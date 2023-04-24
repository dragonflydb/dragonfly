// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "base/gtest.h"
#include "core/search/query_driver.h"

namespace dfly {
namespace search {

using namespace std;

class SearchParserTest : public ::testing::Test {
 protected:
  SearchParserTest() {
  }

  void SetInput(const std::string& str) {
    istr_.str(str);
    query_driver_.scanner()->switch_streams(&istr_);
  }

  Parser::symbol_type Lex() {
    return query_driver_.scanner()->ParserLex(query_driver_);
  }

  QueryDriver query_driver_;

  std::istringstream istr_;
};

TEST_F(SearchParserTest, Scanner) {
  SetInput("ab cd");
  Parser::symbol_type tok = Lex();

  // 3.5.1 does not have name() method.
  // EXPECT_STREQ("term", tok.name());
  EXPECT_EQ(tok.type_get(), Parser::token::TOK_TERM);
  EXPECT_EQ("ab", tok.value.as<string>());
}

}  // namespace search

}  // namespace dfly
