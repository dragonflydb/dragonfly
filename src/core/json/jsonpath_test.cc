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
      case SegmentType::DESCENT:
        *os << "DESCENT";
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

  SetInput("$..*");
  NEXT_TOK(ROOT);
  NEXT_TOK(DESCENT);
  NEXT_TOK(WILDCARD);
}

TEST_F(JsonPathTest, Parser) {
  EXPECT_NE(0, Parse("foo"));
  EXPECT_NE(0, Parse("$foo"));
  EXPECT_NE(0, Parse("$|foo"));

  EXPECT_EQ(0, Parse("$.foo.bar"));
  Path path = driver_.TakePath();

  // TODO: to improve the UX with gmock/c++ magic.
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[0].identifier());
  EXPECT_EQ("bar", path[1].identifier());

  EXPECT_EQ(0, Parse("$.*.bar[1]"));
  path = driver_.TakePath();
  ASSERT_EQ(3, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::WILDCARD));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[2], SegType(SegmentType::INDEX));
  EXPECT_EQ("bar", path[1].identifier());
  EXPECT_EQ(1, path[2].index());
}

TEST_F(JsonPathTest, Descent) {
  EXPECT_EQ(0, Parse("$..foo"));
  Path path = driver_.TakePath();
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::DESCENT));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[1].identifier());

  EXPECT_EQ(0, Parse("$..*"));
  ASSERT_EQ(2, path.size());
  path = driver_.TakePath();
  EXPECT_THAT(path[0], SegType(SegmentType::DESCENT));
  EXPECT_THAT(path[1], SegType(SegmentType::WILDCARD));

  EXPECT_NE(0, Parse("$.."));
  EXPECT_NE(0, Parse("$...foo"));
}

TEST_F(JsonPathTest, Path) {
  Path path;
  JsonType json = JsonFromString(R"({"v11":{ "f" : 1, "a2": [0]}, "v12": {"f": 2, "a2": [1]},
      "v13": 3
      })")
                      .value();
  int called = 0;

  // Empty path
  EvaluatePath(path, json, [&](const JsonType& val) { ++called; });
  ASSERT_EQ(0, called);

  path.emplace_back(SegmentType::IDENTIFIER, "v13");
  EvaluatePath(path, json, [&](const JsonType& val) {
    ++called;
    ASSERT_EQ(3, val.as<int>());
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::IDENTIFIER, "v11");
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](const JsonType& val) {
    ++called;
    ASSERT_EQ(1, val.as<int>());
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::WILDCARD);
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](const JsonType& val) {
    ++called;
    ASSERT_TRUE(val.is<int>());
  });
  ASSERT_EQ(2, called);
}

}  // namespace dfly::json
