// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/json/driver.h"
#include "core/json/lexer_impl.h"
#include "core/mi_memory_resource.h"

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

JsonType ValidJson(string_view str) {
  auto res = ParseJsonUsingShardHeap(str);
  CHECK(res) << "Failed to parse json: " << str;
  return *res;
}

bool is_int(const JsonType& val) {
  return val.is<int>();
}

int to_int(const JsonType& val) {
  return val.as<int>();
}

bool is_object(const JsonType& val) {
  return val.is_object();
}

bool is_array(const JsonType& val) {
  return val.is_array();
}

class ScannerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Test::SetUp();
    InitTLStatelessAllocMR(&m_);
  }

  ScannerTest() : m_(mi_heap_get_backing()) {
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

  MiMemoryResource m_;
  TestDriver driver_;
};

class JsonPathTest : public ScannerTest {
 protected:
  int Parse(const std::string& str) {
    driver_.ResetScanner();
    driver_.SetInput(str);

    return Parser(&driver_)();
  }
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

TEST_F(ScannerTest, Basic) {
  SetInput("$.мага-зин2.book[0].*");
  NEXT_TOK(ROOT);
  NEXT_TOK(DOT);
  NEXT_EQ(UNQ_STR, string, "мага-зин2");
  NEXT_TOK(DOT);
  NEXT_EQ(UNQ_STR, string, "book");
  NEXT_TOK(LBRACKET);
  NEXT_EQ(INT, string, "0");
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
  EXPECT_EQ(IndexExpr(1, 1), path[2].index());

  EXPECT_EQ(0, Parse("$.plays[*].game"));
  EXPECT_EQ(0, Parse("$.bar[ -1]"));
  path = driver_.TakePath();
  EXPECT_THAT(path[1], SegType(SegmentType::INDEX));
  EXPECT_EQ(IndexExpr(-1, -1), path[1].index());
}

TEST_F(JsonPathTest, Root) {
  JsonType json = ValidJson(R"({"foo" : 1, "bar": "str" })");
  ASSERT_EQ(0, Parse("$"));
  Path path = driver_.TakePath();
  int called = 0;
  EvaluatePath(path, json, [&](optional<string_view>, const JsonType& val) {
    ++called;
    ASSERT_TRUE(is_object(val));
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
  EXPECT_THAT(path[2], SegType(SegmentType::INDEX));
  EXPECT_THAT(path[3], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ(IndexExpr::All(), path[2].index());

  JsonType json = ValidJson(R"({"plays": [{"score": 1}, {"score": 2}]})");
  int called = 0;
  EvaluatePath(path, json, [&](auto, const JsonType& val) {
    ++called;
    ASSERT_TRUE(is_int(val));
    ASSERT_EQ(2, to_int(val));
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

TEST_F(JsonPathTest, QuotedStrings) {
  EXPECT_EQ(0, Parse("$[\"foo\"]"));
  Path path = driver_.TakePath();

  ASSERT_EQ(1, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[0].identifier());

  EXPECT_EQ(0, Parse("$['foo']"));  // single quoted string
  path = driver_.TakePath();

  ASSERT_EQ(1, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[0].identifier());

  EXPECT_EQ(0, Parse("$.[\"foo\"]"));
  path = driver_.TakePath();

  ASSERT_EQ(1, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[0].identifier());

  EXPECT_EQ(0, Parse("$..[\"foo\"]"));
  path = driver_.TakePath();

  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::DESCENT));
  EXPECT_THAT(path[1], SegType(SegmentType::IDENTIFIER));
  EXPECT_EQ("foo", path[1].identifier());

  EXPECT_NE(0, Parse("\"a\""));
  EXPECT_NE(0, Parse("$\"a\""));
  EXPECT_NE(0, Parse("$.\"a\""));
  EXPECT_NE(0, Parse("$..\"a\""));

  // Single quoted string
  EXPECT_NE(0, Parse("'a'"));
  EXPECT_NE(0, Parse("$'a'"));
  EXPECT_NE(0, Parse("$.'a'"));
  EXPECT_NE(0, Parse("$..'a'"));
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
    ASSERT_EQ(3, to_int(val));
    EXPECT_EQ("v13", key);
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::IDENTIFIER, "v11");
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ++called;
    ASSERT_EQ(1, to_int(val));
    EXPECT_EQ("f", key);
  });
  ASSERT_EQ(1, called);

  path.clear();
  path.emplace_back(SegmentType::WILDCARD);
  path.emplace_back(SegmentType::IDENTIFIER, "f");
  called = 0;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ++called;
    ASSERT_TRUE(is_int(val));
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
    if (is_array(val)) {
      ++called_arr;
    } else if (is_object(val)) {
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
    ASSERT_TRUE(is_int(val));
    ASSERT_EQ("f", key);
    ++called;
  });
  ASSERT_EQ(4, called);

  json = ValidJson(R"(
    {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
  )");
  path.pop_back();
  path.emplace_back(SegmentType::IDENTIFIER, "a");

  vector<char> arr;
  auto gettype = [](const JsonType& p) {
    if (is_array(p))
      return 'a';
    return is_object(p) ? 'o' : 'u';
  };

  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    arr.push_back(gettype(val));
    ASSERT_EQ("a", key);
  });
  ASSERT_THAT(arr, ElementsAre('a', 'o'));
}

TEST_F(JsonPathTest, EvalDescent2) {
  JsonType json = ValidJson(R"(
    {"a":[{"val": 1}, {"val": 2}, {"val": 3}]}
  )");

  ASSERT_EQ(0, Parse("$..val"));
  Path path = driver_.TakePath();
  vector<int> arr;
  EvaluatePath(path, json,
               [&](optional<string_view> key, const JsonType& val) { arr.push_back(to_int(val)); });
  ASSERT_THAT(arr, ElementsAre(1, 2, 3));

  int called = 0;
  ASSERT_EQ(0, Parse("$..*"));
  path = driver_.TakePath();
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) { ++called; });
  EXPECT_EQ(7, called);

  called = 0;
  json = ValidJson(R"(
    {
       "store": {
        "nums": [
         5
       ]
      }
    }
    )");
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) { ++called; });
  EXPECT_EQ(3, called);
}

TEST_F(JsonPathTest, Wildcard) {
  ASSERT_EQ(0, Parse("$.arr[*]"));
  Path path = driver_.TakePath();
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[1], SegType(SegmentType::INDEX));

  JsonType json = ValidJson(R"({"arr": [1, 2, 3], "i":1})");
  vector<int> arr;
  EvaluatePath(path, json, [&](optional<string_view> key, const JsonType& val) {
    ASSERT_FALSE(key);
    arr.push_back(to_int(val));
  });
  ASSERT_THAT(arr, ElementsAre(1, 2, 3));

  ASSERT_EQ(0, Parse("$.i[*]"));
  path = driver_.TakePath();
  arr.clear();
  EvaluatePath(path, json,
               [&](optional<string_view> key, const JsonType& val) { arr.push_back(to_int(val)); });
  ASSERT_THAT(arr, ElementsAre());
}

TEST_F(JsonPathTest, Mutate) {
  ASSERT_EQ(0, Parse("$[*]"));
  Path path = driver_.TakePath();

  JsonType json = ValidJson(R"([1, 2, 3, 5, 6])");
  auto cb = [](optional<string_view>, JsonType* val) {
    int intval = val->as<int>();
    *val = intval + 1;
  };

  vector<int> arr;

  MutatePath(path, cb, &json);

  for (JsonType& el : json.array_range()) {
    arr.push_back(to_int(el));
  }
  ASSERT_THAT(arr, ElementsAre(2, 3, 4, 6, 7));

  json = ValidJson(R"(
    {"a":[7], "inner": {"a": {"bool": true, "c": 42}}}
  )");
  ASSERT_EQ(0, Parse("$..a.*"));
  path = driver_.TakePath();

  auto cb2 = [](optional<string_view> key, JsonType* val) {
    if (val->is_int64() && !key) {  // array element
      *val = 42;
    }
    if (val->is_bool()) {
      *val = false;
    }
  };

  auto expected = ValidJson(R"({"a":[42],"inner":{"a":{"bool":false,"c":42}}})");
  MutatePath(path, cb2, &json);

  ASSERT_EQ(expected, json);
}

TEST_F(JsonPathTest, MutateRecursiveDescentKey) {
  ASSERT_EQ(0, Parse("$..value"));
  Path path = driver_.TakePath();

  JsonType json = ValidJson(R"({"data":{"value":10,"subdata":{"value":20}}})");
  JsonType replacement = ValidJson(R"({"value": 30})");

  auto cb = [&](optional<string_view> key, JsonType* val) {
    if (key && key.value() == "value" && (val->is_int64() || val->is_double())) {
      *val = replacement;
    }
  };

  unsigned reported_matches = MutatePath(path, cb, &json);

  JsonType expected =
      ValidJson(R"({"data":{"subdata":{"value":{"value":30}},"value":{"value":30}}})");

  EXPECT_EQ(expected, json);
  EXPECT_EQ(0, reported_matches);
}

TEST_F(JsonPathTest, SubRange) {
  JsonType json = ValidJson(R"({"arr": [1, 2, 3, 4, 5]})");
  ASSERT_EQ(0, Parse("$.arr[1:2]"));
  Path path = driver_.TakePath();
  ASSERT_EQ(2, path.size());
  EXPECT_THAT(path[1], SegType(SegmentType::INDEX));

  vector<int> arr;
  auto cb = [&arr](optional<string_view> key, const JsonType& val) {
    ASSERT_FALSE(key);
    arr.push_back(to_int(val));
  };

  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre(2));
  arr.clear();

  ASSERT_EQ(0, Parse("$.arr[0:2]"));
  path = driver_.TakePath();
  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre(1, 2));
  arr.clear();

  ASSERT_EQ(0, Parse("$.arr[2:-1]"));
  path = driver_.TakePath();
  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre(3, 4));
  arr.clear();

  ASSERT_EQ(0, Parse("$.arr[-2:-1]"));
  path = driver_.TakePath();
  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre(4));
  arr.clear();

  ASSERT_EQ(0, Parse("$.arr[-2:-2]"));
  path = driver_.TakePath();
  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre());
  arr.clear();

  ASSERT_EQ(0, Parse("$.arr[:2]"));
  path = driver_.TakePath();
  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre(1, 2));
  arr.clear();

  ASSERT_EQ(0, Parse("$.arr[2:]"));
  path = driver_.TakePath();
  EvaluatePath(path, json, cb);
  ASSERT_THAT(arr, ElementsAre(3, 4, 5));
  arr.clear();
}

TEST_F(JsonPathTest, DeleteNestedWithSameKey) {
  // Test for deleting nested elements with the same key using "$..a"
  // Corresponds to command: JSON.DEL doc1 "$..a"
  ASSERT_EQ(0, Parse("$..a"));
  Path path = driver_.TakePath();

  JsonType json = ValidJson(R"({"a": 1, "nested": {"a": 2, "b": 3}})");

  unsigned reported_matches = DeletePath(path, &json);
  EXPECT_EQ(2, reported_matches);

  auto expected = ValidJson(R"({"nested": {"b": 3}})");
  EXPECT_EQ(expected, json);
}

TEST_F(JsonPathTest, DeleteRecursiveWithKeysAndArrayValues) {
  ASSERT_EQ(0, Parse("$..a"));
  Path path = driver_.TakePath();

  JsonType json =
      ValidJson(R"({"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [true, "a", "b"]}})");

  unsigned reported_matches = DeletePath(path, &json);
  EXPECT_EQ(1, reported_matches);

  auto expected = ValidJson(R"({"b": ["a", "b"], "nested": {"b": [true, "a", "b"]}})");
  EXPECT_EQ(expected, json);
}

}  // namespace dfly::json
