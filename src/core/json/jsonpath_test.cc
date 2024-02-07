// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "core/json/driver.h"
#include "core/json/lexer_impl.h"

namespace dfly::json {

using namespace std;

class JsonPathTest : public ::testing::Test {
 protected:
  JsonPathTest() {
    driver_.lexer()->set_debug(1);
  }

  void SetInput(const std::string& str) {
    driver_.SetInput(str);
  }

  Parser::symbol_type Lex() {
    return driver_.lexer()->Lex();
  }

  Driver driver_;
};

#define NEXT_TOK(tok_enum)                                    \
  {                                                           \
    auto tok = Lex();                                         \
    ASSERT_EQ(tok.type_get(), Parser::token::TOK_##tok_enum); \
  }

#define NEXT_EQ(tok_enum, type, val)                          \
  {                                                           \
    auto tok = Lex();                                         \
    ASSERT_EQ(tok.type_get(), Parser::token::TOK_##tok_enum); \
    EXPECT_EQ(val, tok.value.as<type>());                     \
  }

TEST_F(JsonPathTest, Scanner) {
  SetInput("$.мага-зин2.book[0].title");
  NEXT_TOK(ROOT);
  NEXT_TOK(DOT);
  NEXT_EQ(UNQ_STR, string, "мага-зин2");
}

}  // namespace dfly::json
