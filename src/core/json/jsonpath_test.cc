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
  *os << " segment(" << SegmentName(st) << ")";
}

class TestDriver : public Driver {
 public:
  void Error(const location& l, const std::string& msg) final {
    LOG(INFO) << "Error at " << l << ": " << msg;
  }
};

inline JsonType ValidJson(string_view str) {
  auto res = ::dfly::JsonFromString(str, pmr::get_default_resource());
  CHECK(res) << "Failed to parse json: " << str;
  return *res;
}

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

  EXPECT_EQ(0, Parse("$.plays[*].game"));
}

TEST_F(JsonPathTest, Root) {
  JsonType json = ValidJson(R"({"foo" : 1, "bar": "str" })");
  ASSERT_EQ(0, Parse("$"));
  Path path = driver_.TakePath();
  int called = 0;
  EvaluatePath(path, json, [&](optional<string_view>, const JsonType& val) {
    ++called;
    ASSERT_TRUE(val.is_object());
    ASSERT_EQ(json, val);
  });
  ASSERT_EQ(1, called);
}

TEST_F(JsonPathTest, Functions) {
  ASSERT_EQ(0, Parse("max($.plays[*].score)"));
  Path path = driver_.TakePath();
  ASSERT_EQ(4, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::FUNCTION));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[2], SegType(SegmentType::WILDCARD));
  EXPECT_THAT(path[3], SegType(SegmentType::IDENTIFIER));
  JsonType json = ValidJson(R"({"plays": [{"score": 1}, {"score": 2}]})");
  int called = 0;
  EvaluatePath(path, json, [&](auto, const JsonType& val) {
    ASSERT_TRUE(val.is<int>());
    ASSERT_EQ(2, val.as<int>());
    ++called;
  });
  ASSERT_EQ(1, called);
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
  JsonType json = ValidJson(R"({"v11":{ "f" : 1, "a2": [0]}, "v12": {"f": 2, "a2": [1]},
      "v13": 3
      })");
  int called = 0;

  // Empty path
  EvaluatePath(path, json, [&](optional<string_view>, const JsonType& val) { ++called; });
  ASSERT_EQ(1, called);
  called = 0;

  path.emplace_back(SegmentType::IDENTIFIER, "v13");
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ++called;
    ASSERT_EQ(3, val.as<int>());
    EXPECT_EQ("v13", key);
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::IDENTIFIER, "v11");
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ++called;
    ASSERT_EQ(1, val.as<int>());
    EXPECT_EQ("f", key);
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::WILDCARD);
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ++called;
    ASSERT_TRUE(val.is<int>());
    EXPECT_EQ("f", key);
  });
  ASSERT_EQ(2, called);
}

TEST_F(JsonPathTest, EvalDescent) {
  JsonType json = ValidJson(R"(
    {"v11":{ "f" : 1, "a2": [0]}, "v12": {"f": 2, "v21": {"f": 3, "a2": [1]}},
      "v13": { "a2" : { "b" : {"f" : 4}}}
      })");

  Path path;

  int called_arr = 0, called_obj = 0;

  path.emplace_back(SegmentType::DESCENT);
  path.emplace_back(SegmentType::IDENTIFIER, "a2");
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    EXPECT_EQ("a2", key);
    if (val.is_array()) {
      ++called_arr;
    } else if (val.is_object()) {
      ++called_obj;
    } else {
      FAIL() << "Unexpected type";
    }
  });
  ASSERT_EQ(2, called_arr);
  ASSERT_EQ(1, called_obj);

  path.pop_back();
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  int called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ASSERT_TRUE(val.is<int>());
    ASSERT_EQ("f", key);
    ++called;
  });
  ASSERT_EQ(4, called);

  json = ValidJson(R"(
    {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
  )");
  path.pop_back();
  path.emplace_back(SegmentType::IDENTIFIER, "a");

  using jsoncons::json_type;
  vector<json_type> arr;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    arr.push_back(val.type());
    ASSERT_EQ("a", key);
  });
  ASSERT_THAT(arr, ElementsAre(json_type::array_value, json_type::object_value));
}

TEST_F(JsonPathTest, Wildcard) {
  ASSERT_EQ(0, Parse("$[*]"));
  Path path = driver_.TakePath();
  ASSERT_EQ(1, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::WILDCARD));

  JsonType json = ValidJson(R"([1, 2, 3])");
  vector<int> arr;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ASSERT_FALSE(key);
    arr.push_back(val.as<int>());
  });
  ASSERT_THAT(arr, ElementsAre(1, 2, 3));
}

TEST_F(JsonPathTest, Mutate) {
  JsonType json = ValidJson(R"([1, 2, 3, 5, 6])");
  ASSERT_EQ(0, Parse("$[*]"));
  Path path = driver_.TakePath();
  MutateCallback cb = [&](optional<string_view>, JsonType* val) {
    int intval = val->as<int>();
    *val = intval + 1;
    return false;
  };

  MutatePath(path, cb, &json);
  vector<int> arr;
  for (auto& el : json.array_range()) {
    arr.push_back(el.as<int>());
  }
  ASSERT_THAT(arr, ElementsAre(2, 3, 4, 6, 7));

  json = ValidJson(R"(
    {"a":[7], "inner": {"a": {"bool": true, "c": 42}}}
  )");
  ASSERT_EQ(0, Parse("$..a.*"));
  path = driver_.TakePath();
  MutatePath(
      path,
      [&](optional<string_view> key, JsonType* val) {
        if (val->is_int64() && !key) {  // array element
          *val = 42;
          return false;
        }
        if (val->is_bool()) {
          *val = false;
          return false;
        }
        return true;
      },
      &json);

  ASSERT_EQ(R"({"a":[42],"inner":{"a":{"bool":false}}})", json.to_string());
}

}  // namespace dfly::json
