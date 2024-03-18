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

template <typename JSON> JSON ValidJson(string_view str);

template <> JsonType ValidJson<JsonType>(string_view str) {
  auto res = ::dfly::JsonFromString(str, pmr::get_default_resource());
  CHECK(res) << "Failed to parse json: " << str;
  return *res;
}

bool is_int(const JsonType& val) {
  return val.is<int>();
}

int to_int(const JsonType& val) {
  return val.as<int>();
}

class ScannerTest : public ::testing::Test {
 protected:
  ScannerTest() {
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

  TestDriver driver_;
};

template <typename JSON> class JsonPathTest : public ScannerTest {
 protected:
  int Parse(const std::string& str) {
    driver_.ResetScanner();
    driver_.SetInput(str);

    return Parser(&driver_)();
  }
};
using MyTypes = ::testing::Types<JsonType>;
TYPED_TEST_SUITE(JsonPathTest, MyTypes);

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

TEST_F(ScannerTest, Basic) {
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

TYPED_TEST(JsonPathTest, Parser) {
  EXPECT_NE(0, this->Parse("foo"));
  EXPECT_NE(0, this->Parse("$foo"));
  EXPECT_NE(0, this->Parse("$|foo"));

  EXPECT_EQ(0, this->Parse("$.foo.bar"));
  Path path = this->driver_.TakePath();

  // TODO: to improve the UX with gmock/c++ magic.
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[0].identifier());
  EXPECT_EQ("bar", path[1].identifier());

  EXPECT_EQ(0, this->Parse("$.*.bar[1]"));
  path = this->driver_.TakePath();
  ASSERT_EQ(3, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::WILDCARD));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[2], SegType(SegmentType::INDEX));
  EXPECT_EQ("bar", path[1].identifier());
  EXPECT_EQ(1, path[2].index());

  EXPECT_EQ(0, this->Parse("$.plays[*].game"));
}

TYPED_TEST(JsonPathTest, Root) {
  TypeParam json = ValidJson<TypeParam>(R"({"foo" : 1, "bar": "str" })");
  ASSERT_EQ(0, this->Parse("$"));
  Path path = this->driver_.TakePath();
  int called = 0;
  EvaluatePath(path, json, [&](optional<string_view>, const TypeParam& val) {
    ++called;
    ASSERT_TRUE(val.is_object());
    ASSERT_EQ(json, val);
  });
  ASSERT_EQ(1, called);
}

TYPED_TEST(JsonPathTest, Functions) {
  ASSERT_EQ(0, this->Parse("max($.plays[*].score)"));
  Path path = this->driver_.TakePath();
  ASSERT_EQ(4, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::FUNCTION));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_THAT(path[2], SegType(SegmentType::WILDCARD));
  EXPECT_THAT(path[3], SegType(SegmentType::IDENTIFIER));
  TypeParam json = ValidJson<TypeParam>(R"({"plays": [{"score": 1}, {"score": 2}]})");
  int called = 0;
  EvaluatePath(path, json, [&](auto, const TypeParam& val) {
    ASSERT_TRUE(is_int(val));
    ASSERT_EQ(2, to_int(val));
    ++called;
  });
  ASSERT_EQ(1, called);
}

TYPED_TEST(JsonPathTest, Descent) {
  EXPECT_EQ(0, this->Parse("$..foo"));
  Path path = this->driver_.TakePath();
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::DESCENT));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[1].identifier());

  EXPECT_EQ(0, this->Parse("$..*"));
  ASSERT_EQ(2, path.size());
  path = this->driver_.TakePath();
  EXPECT_THAT(path[0], SegType(SegmentType::DESCENT));
  EXPECT_THAT(path[1], SegType(SegmentType::WILDCARD));

  EXPECT_NE(0, this->Parse("$.."));
  EXPECT_NE(0, this->Parse("$...foo"));
}

TYPED_TEST(JsonPathTest, Path) {
  Path path;
  TypeParam json = ValidJson<TypeParam>(R"({"v11":{ "f" : 1, "a2": [0]}, "v12": {"f": 2, "a2": [1]},
      "v13": 3
      })");
  int called = 0;

  // Empty path
  EvaluatePath(path, json, [&](optional<string_view>, const TypeParam& val) { ++called; });
  ASSERT_EQ(1, called);
  called = 0;

  path.emplace_back(SegmentType::IDENTIFIER, "v13");
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    ++called;
    ASSERT_EQ(3, to_int(val));
    EXPECT_EQ("v13", key);
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::IDENTIFIER, "v11");
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    ++called;
    ASSERT_EQ(1, to_int(val));
    EXPECT_EQ("f", key);
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::WILDCARD);
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    ++called;
    ASSERT_TRUE(is_int(val));
    EXPECT_EQ("f", key);
  });
  ASSERT_EQ(2, called);
}

TYPED_TEST(JsonPathTest, EvalDescent) {
  TypeParam json = ValidJson<TypeParam>(R"(
    {"v11":{ "f" : 1, "a2": [0]}, "v12": {"f": 2, "v21": {"f": 3, "a2": [1]}},
      "v13": { "a2" : { "b" : {"f" : 4}}}
      })");

  Path path;

  int called_arr = 0, called_obj = 0;

  path.emplace_back(SegmentType::DESCENT);
  path.emplace_back(SegmentType::IDENTIFIER, "a2");
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
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
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    ASSERT_TRUE(is_int(val));
    ASSERT_EQ("f", key);
    ++called;
  });
  ASSERT_EQ(4, called);

  json = ValidJson<TypeParam>(R"(
    {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
  )");
  path.pop_back();
  path.emplace_back(SegmentType::IDENTIFIER, "a");

  using jsoncons::json_type;
  vector<json_type> arr;
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    arr.push_back(val.type());
    ASSERT_EQ("a", key);
  });
  ASSERT_THAT(arr, ElementsAre(json_type::array_value, json_type::object_value));
}

TYPED_TEST(JsonPathTest, Wildcard) {
  ASSERT_EQ(0, this->Parse("$[*]"));
  Path path = this->driver_.TakePath();
  ASSERT_EQ(1, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::WILDCARD));

  TypeParam json = ValidJson<TypeParam>(R"([1, 2, 3])");
  vector<int> arr;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ASSERT_FALSE(key);
    arr.push_back(val.as<int>());
  });
  ASSERT_THAT(arr, ElementsAre(1, 2, 3));
}

TYPED_TEST(JsonPathTest, Mutate) {
  JsonType json = ValidJson<TypeParam>(R"([1, 2, 3, 5, 6])");
  ASSERT_EQ(0, this->Parse("$[*]"));
  Path path = this->driver_.TakePath();
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

  json = ValidJson<TypeParam>(R"(
    {"a":[7], "inner": {"a": {"bool": true, "c": 42}}}
  )");
  ASSERT_EQ(0, this->Parse("$..a.*"));
  path = this->driver_.TakePath();
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
