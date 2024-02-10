// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/json/driver.h"
#include "core/json/lexer_impl.h"

namespace dfly::json {

using namespace std;

using testing::ElementsAre;

MATCHER_P(SegType, value, "") {
  return ExplainMatchResult(testing::Property(&PathSegment::type, value), arg, result_listener);
}

void PrintTo(SegmentType st, std::ostream* os) {
  *os << " segment(";
  switch (st) {
    {
      case SegmentType::IDENTIFIER:
        *os << "IDENTIFIER";
        break;
      case SegmentType::INDEX:
        *os << "INDEX";
        break;
      case SegmentType::WILDCARD:
        *os << "WILDCARD";
        break;
    }
  }
  *os << ")";
}

class TestDriver : public Driver {
 public:
  void Error(const location& l, const std::string& msg) final {
    LOG(INFO) << "Error at " << l << ": " << msg;
  }
};

class JsonPathTest : public ::testing::Test {
 protected:
  JsonPathTest() {
    driver_.lexer()->set_debug(1);
  }

  void SetInput(const std::string& str) {
    driver_.SetInput(str);
  }

  Parser::symbol_type Lex() {
    try {
      return driver_.lexer()->Lex();
    } catch (const Parser::syntax_error& e) {
      LOG(INFO) << "Caught exception: " << e.what();

      // with later bison versions we can return make_YYerror
      return Parser::make_YYEOF(e.location);
    }
  }

  int Parse(const std::string& str) {
    driver_.ResetScanner();
    driver_.SetInput(str);

    return Parser(&driver_)();
  }

  TestDriver driver_;
};

#define NEXT_TOK(tok_enum)                                    \
  {                                                           \
    auto tok = Lex();                                         \
    ASSERT_EQ(Parser::token::TOK_##tok_enum, tok.type_get()); \
  }

#define NEXT_EQ(tok_enum, type, val)                          \
  {                                                           \
    auto tok = Lex();                                         \
    ASSERT_EQ(Parser::token::TOK_##tok_enum, tok.type_get()); \
    EXPECT_EQ(val, tok.value.as<type>());                     \
  }

TEST_F(JsonPathTest, Scanner) {
  SetInput("$.мага-зин2.book[0].*");
  NEXT_TOK(ROOT);
  NEXT_TOK(DOT);
  NEXT_EQ(UNQ_STR, string, "мага-зин2");
  NEXT_TOK(DOT);
  NEXT_EQ(UNQ_STR, string, "book");
  NEXT_TOK(LBRACKET);
  NEXT_EQ(UINT, unsigned, 0);
  NEXT_TOK(RBRACKET);
  NEXT_TOK(DOT);
  NEXT_TOK(WILDCARD);

  SetInput("|");
  NEXT_TOK(YYEOF);
}

TEST_F(JsonPathTest, Parser) {
  EXPECT_NE(0, Parse("foo"));
  EXPECT_NE(0, Parse("$foo"));
  EXPECT_NE(0, Parse("$|foo"));

  EXPECT_EQ(0, Parse("$.foo.bar"));
  Path path = driver_.Release();

  // TODO: to improve the UX with gmock/c++ magic.
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[0].identifier());
  EXPECT_EQ("bar", path[1].identifier());
}

}  // namespace dfly::json
