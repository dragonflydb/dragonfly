// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/json/driver.h"
#include "core/json/lexer_impl.h"

namespace flexbuffers {
bool operator==(const Reference left, const Reference right) {
  return left.ToString() == right.ToString();
}
}  // namespace flexbuffers

namespace dfly::json {

using namespace std;

using testing::ElementsAre;
using FlatJson = flexbuffers::Reference;

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

template <> FlatJson ValidJson<FlatJson>(string_view str) {
  static flexbuffers::Builder fbb;
  flatbuffers::Parser parser;

  fbb.Clear();
  CHECK(parser.ParseFlexBuffer(str.data(), nullptr, &fbb));
  fbb.Finish();
  const auto& buffer = fbb.GetBuffer();
  return flexbuffers::GetRoot(buffer);
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

int is_int(FlatJson ref) {
  return ref.IsInt();
}

int to_int(FlatJson ref) {
  return ref.AsInt32();
}

bool is_object(FlatJson ref) {
  return ref.IsMap();
}

bool is_array(FlatJson ref) {
  return ref.IsVector();
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
using MyTypes = ::testing::Types<JsonType, FlatJson>;
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
  if constexpr (std::is_same_v<TypeParam, FlatJson>) {
    return;  // TODO
  }
  TypeParam json = ValidJson<TypeParam>(R"({"foo" : 1, "bar": "str" })");
  ASSERT_EQ(0, this->Parse("$"));
  Path path = this->driver_.TakePath();
  int called = 0;
  EvaluatePath(path, json, [&](optional<string_view>, const TypeParam& val) {
    ++called;
    ASSERT_TRUE(is_object(val));
    ASSERT_EQ(json, val);
  });
  ASSERT_EQ(1, called);
}

TYPED_TEST(JsonPathTest, Functions) {
  if constexpr (std::is_same_v<TypeParam, FlatJson>) {
    return;  // TODO
  }

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
  if constexpr (std::is_same_v<TypeParam, FlatJson>) {
    return;  // TODO
  }

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
  if constexpr (std::is_same_v<TypeParam, FlatJson>) {
    return;  // TODO
  }

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
  if constexpr (std::is_same_v<TypeParam, FlatJson>) {
    return;  // TODO
  }

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

  vector<char> arr;
  auto gettype = [](const TypeParam& p) {
    if (is_array(p))
      return 'a';
    return is_object(p) ? 'o' : 'u';
  };

  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    arr.push_back(gettype(val));
    ASSERT_EQ("a", key);
  });
  ASSERT_THAT(arr, ElementsAre('a', 'o'));
}

TYPED_TEST(JsonPathTest, Wildcard) {
  if constexpr (std::is_same_v<TypeParam, FlatJson>) {
    return;  // TODO
  }

  ASSERT_EQ(0, this->Parse("$[*]"));
  Path path = this->driver_.TakePath();
  ASSERT_EQ(1, path.size());
  EXPECT_THAT(path[0], SegType(SegmentType::WILDCARD));

  TypeParam json = ValidJson<TypeParam>(R"([1, 2, 3])");
  vector<int> arr;
  EvaluatePath(path, json, [&](optional<string_view> key, const TypeParam& val) {
    ASSERT_FALSE(key);
    arr.push_back(to_int(val));
  });
  ASSERT_THAT(arr, ElementsAre(1, 2, 3));
}

TYPED_TEST(JsonPathTest, Mutate) {
  ASSERT_EQ(0, this->Parse("$[*]"));
  Path path = this->driver_.TakePath();

  // Currently this code compiles only for JsonType.
  if constexpr (std::is_same_v<TypeParam, JsonType>) {
    TypeParam json = ValidJson<TypeParam>(R"([1, 2, 3, 5, 6])");
    MutateCallback cb = [&](optional<string_view>, JsonType* val) {
      int intval = val->as<int>();
      *val = intval + 1;
      return false;
    };

    MutatePath(path, cb, &json);
    vector<int> arr;
    for (JsonType& el : json.array_range()) {
      arr.push_back(to_int(el));
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
}

}  // namespace dfly::json
