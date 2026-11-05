// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <absl/strings/str_replace.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/error.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

ABSL_DECLARE_FLAG(bool, jsonpathv2);

namespace dfly {

class JsonFamilyTest : public BaseFamilyTest {
 protected:
  struct CompatibilityStep {
    CompatibilityStep(string_view command, Matcher<RespExpr> reply)
        : modern_command(command),
          legacy_command(command),
          modern_reply(reply),
          legacy_reply(reply) {
    }

    CompatibilityStep(string_view modern, string_view legacy, Matcher<RespExpr> modern_expected,
                      Matcher<RespExpr> legacy_expected)
        : modern_command(modern),
          legacy_command(legacy),
          modern_reply(modern_expected),
          legacy_reply(legacy_expected) {
    }

    string_view modern_command;
    string_view legacy_command;
    Matcher<RespExpr> modern_reply;
    Matcher<RespExpr> legacy_reply;
  };

  void CheckCompatibility(initializer_list<CompatibilityStep> steps) {
    for (bool legacy : {false, true}) {
      SCOPED_TRACE(legacy ? "legacy" : "modern");
      ASSERT_THAT(Run("FLUSHDB"), "OK");
      for (const auto& step : steps) {
        auto command = legacy ? step.legacy_command : step.modern_command;

        // An empty command marks a check specific to the other path mode.
        if (command.empty())
          continue;
        SCOPED_TRACE(command);
        ASSERT_THAT(Run(command), legacy ? step.legacy_reply : step.modern_reply);
      }
    }
  }
};

MATCHER_P(ElementsAreArraysMatcher, matchers, "") {
  const auto& vec = arg.GetVec();
  const size_t expected_size = std::tuple_size<decltype(matchers)>::value;

  if (vec.size() != expected_size) {
    *result_listener << "size mismatch: expected " << expected_size << " but got " << vec.size();
    return false;
  }

  bool result = true;
  size_t index = 0;

  auto check_matcher = [&](const auto& matcher) {
    if (!ExplainMatchResult(matcher, vec[index].GetVec(), result_listener)) {
      *result_listener << " at index " << index;
      result = false;
    }
    index++;
  };

  std::apply([&check_matcher](const auto&... matchers) { (check_matcher(matchers), ...); },
             matchers);

  return result;
}

template <typename... Matchers> auto ElementsAreArrays(Matchers&&... matchers) {
  return ElementsAreArraysMatcher(std::make_tuple(std::forward<Matchers>(matchers)...));
}

TEST_F(JsonFamilyTest, SetGetBasic) {
  string json = R"(
    {
       "store": {
        "book": [
         {
           "category": "Fantasy",
           "author": "J. K. Rowling",
           "title": "Harry Potter and the Philosopher's Stone",
           "isbn": 9780747532743,
           "price": 5.99
         }
       ]
      }
    }
)";

  string xml = R"(
    <?xml version="1.0" encoding="UTF-8" ?>
    <store>
      <book>
        <category>Fantasy</category>
        <author>J. K. Rowling</author>
        <title>Harry Potter and the Philosopher&#x27;s Stone</title>
        <isbn>9780747532743</isbn>
        <price>5.99</price>
      </book>
    </store>
)";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json", "$..*"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"JSON.GET", "json", "$..book[0].price"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"JSON.GET", "json", "//*"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));

  resp = Run({"JSON.GET", "json", "//book[0]"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));

  resp = Run({"JSON.GET", "json", "store.book[0].category"});
  EXPECT_EQ(resp, "\"Fantasy\"");

  resp = Run({"JSON.GET", "json", ".store.book[0].category"});
  EXPECT_EQ(resp, "\"Fantasy\"");

  resp = Run({"SET", "xml", xml});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "xml", "$..*"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));
}

TEST_F(JsonFamilyTest, GetLegacy) {
  string json = R"({"name":"Leonard Cohen","lastSeen":1478476800,"loggedOut": true})";

  auto resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json"});  // V1 Response
  ASSERT_THAT(resp, "{\"lastSeen\":1478476800,\"loggedOut\":true,\"name\":\"Leonard Cohen\"}");

  resp = Run({"JSON.GET", "json", "."});  // V1 Response
  ASSERT_THAT(resp, "{\"lastSeen\":1478476800,\"loggedOut\":true,\"name\":\"Leonard Cohen\"}");

  resp = Run({"JSON.GET", "json", "$"});  // V2 Response
  ASSERT_THAT(resp, "[{\"lastSeen\":1478476800,\"loggedOut\":true,\"name\":\"Leonard Cohen\"}]");

  resp = Run({"JSON.GET", "json", ".name"});  // V1 Response
  ASSERT_THAT(resp, "\"Leonard Cohen\"");

  resp = Run({"JSON.GET", "json", "$.name"});  // V2 Response
  ASSERT_THAT(resp, "[\"Leonard Cohen\"]");

  resp = Run({"JSON.GET", "json", ".name", "$.lastSeen"});  // V2 Response
  ASSERT_THAT(resp, "{\"$.lastSeen\":[1478476800],\".name\":[\"Leonard Cohen\"]}");

  resp = Run({"JSON.GET", "json", ".name", ".lastSeen"});  // V1 Response
  ASSERT_THAT(resp, "{\".lastSeen\":1478476800,\".name\":\"Leonard Cohen\"}");

  resp = Run({"JSON.GET", "json", "$.name", "$.lastSeen"});  // V2 Response
  ASSERT_THAT(resp, "{\"$.lastSeen\":[1478476800],\"$.name\":[\"Leonard Cohen\"]}");

  json = R"(
    {"a":"first","b":{"field":"second"},"c":{"field":"third"}}
  )";

  resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json", "bar"});  // V1 Response
  ASSERT_THAT(resp, ErrArg("ERR invalid JSON path"));

  resp = Run({"JSON.GET", "json", ".", "bar"});  // V1 Response
  ASSERT_THAT(resp, ErrArg("ERR invalid JSON path"));

  resp = Run({"JSON.GET", "json", ".a", "bar", "foo", "third", "."});  // V1 Response
  ASSERT_THAT(resp, ErrArg("ERR invalid JSON path"));

  resp = Run({"JSON.GET", "json", "$.bar"});  // V2 Response
  ASSERT_THAT(resp, "[]");

  resp = Run({"JSON.GET", "json", "bar", "$.a"});  // V2 Response
  ASSERT_THAT(resp, R"({"$.a":["first"],"bar":[]})");

  resp = Run({"JSON.GET", "json", "$.bar"});  // V2 Response
  ASSERT_THAT(resp, "[]");
}

static const string PhonebookJson = R"(
    {
      "firstName":"John",
      "lastName":"Smith",
      "age":27,
      "weight":135.25,
      "isAlive":true,
      "address":{
          "street":"21 2nd Street",
          "city":"New York",
          "state":"NY",
          "zipcode":"10021-3100"
      },
      "phoneNumbers":[
          {
            "type":"home",
            "number":"212 555-1234"
          },
          {
            "type":"office",
            "number":"646 555-4567"
          }
      ],
      "children":[

      ],
      "spouse":null
    }
  )";

TEST_F(JsonFamilyTest, SetGetFromPhonebook) {
  auto resp = Run({"JSON.SET", "json", ".", PhonebookJson});
  ASSERT_THAT(resp, "OK");

  auto compact_json = jsoncons::json::parse(PhonebookJson).as_string();

  resp = Run({"JSON.GET", "json", "."});
  EXPECT_EQ(resp, compact_json);

  resp = Run({"JSON.GET", "json", "$"});
  EXPECT_EQ(resp, "[" + compact_json + "]");

  resp = Run({"JSON.GET", "json", "$.address.*"});
  EXPECT_EQ(resp, R"(["New York","NY","21 2nd Street","10021-3100"])");

  resp = Run({"JSON.GET", "json", "$.firstName", "$.age", "$.lastName"});
  EXPECT_EQ(resp, R"({"$.age":[27],"$.firstName":["John"],"$.lastName":["Smith"]})");

  resp = Run({"JSON.GET", "json", "$.spouse.*"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.GET", "json", "$.children.*"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.GET", "json", "$..phoneNumbers[1].*"});
  EXPECT_EQ(resp, R"(["646 555-4567","office"])");

  resp = Run({"JSON.GET", "json", "$.address.*", "INDENT", "indent", "NEWLINE", "newline"});
  EXPECT_EQ(
      resp,
      R"([newlineindent"New York",newlineindent"NY",newlineindent"21 2nd Street",newlineindent"10021-3100"newline])");

  resp = Run({"JSON.GET", "json", "$.address", "SPACE", "space"});
  EXPECT_EQ(
      resp,
      R"([{"city":space"New York","state":space"NY","street":space"21 2nd Street","zipcode":space"10021-3100"}])");

  resp = Run({"JSON.GET", "json", "$.firstName", "$.age", "$.lastName", "INDENT", "indent",
              "NEWLINE", "newline", "SPACE", "space"});
  EXPECT_EQ(
      resp,
      R"({newlineindent"$.age":space[newlineindentindent27newlineindent],newlineindent"$.firstName":space[newlineindentindent"John"newlineindent],newlineindent"$.lastName":space[newlineindentindent"Smith"newlineindent]newline})");

  resp =
      Run({"JSON.GET", "json", "$..phoneNumbers.*", "INDENT", "t", "NEWLINE", "s", "SPACE", "s"});
  EXPECT_EQ(
      resp,
      R"([st{stt"number":s"212 555-1234",stt"type":s"home"st},st{stt"number":s"646 555-4567",stt"type":s"office"st}s])");
}

TEST_F(JsonFamilyTest, GetBrackets) {
  string json = R"(
    {"a":"first", "b":{"a":"second"}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json", "$[\"a\"]"});
  ASSERT_THAT(resp, "[\"first\"]");

  resp = Run({"JSON.GET", "json", "$..[\"a\"]"});
  ASSERT_THAT(resp, R"(["first","second"])");

  resp = Run({"JSON.GET", "json", "$.b[\"a\"]"});
  ASSERT_THAT(resp, "[\"second\"]");

  resp = Run({"JSON.GET", "json", "[\"a\"]"});
  ASSERT_THAT(resp, "\"first\"");

  resp = Run({"JSON.GET", "json", "..[\"a\"]"});
  ASSERT_THAT(resp, "\"second\"");

  json = R"(
    ["first", ["second"]]
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json", "$[0]"});
  ASSERT_THAT(resp, "[\"first\"]");

  resp = Run({"JSON.GET", "json", "$..[0]"});
  ASSERT_THAT(resp, R"(["first","second"])");

  resp = Run({"JSON.GET", "json", "[0]"});
  ASSERT_THAT(resp, "\"first\"");

  resp = Run({"JSON.GET", "json", "..[0]"});
  ASSERT_THAT(resp, "\"second\"");

  resp = Run({"JSON.GET", "json", "$[\"first\"]"});
  ASSERT_THAT(resp, "[]");

  json = R"(
    {"a":{"b":{"c":"first"}}, "b":{"b":{"c":"second"}}, "c":{"b":{"c":"third"}}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json", R"($["a"]['b']["c"])"});
  ASSERT_THAT(resp, "[\"first\"]");

  resp = Run({"JSON.GET", "json", R"($["a"].b['c'])"});
  ASSERT_THAT(resp, "[\"first\"]");

  resp = Run({"JSON.GET", "json", R"($..['b']["c"])"});
  ASSERT_THAT(resp, R"(["first","second","third"])");

  resp = Run({"JSON.GET", "json", R"($.c['b']["c"])"});
  ASSERT_THAT(resp, "[\"third\"]");
}

TEST_F(JsonFamilyTest, GetWithNoEscape) {
  string json = R"({"key": "value with special characters: \n \t \" \""})";
  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  // Test without NOESCAPE option
  resp = Run({"JSON.GET", "json", "."});
  EXPECT_EQ(resp, "{\"key\":\"value with special characters: \\n \\t \\\" \\\"\"}");

  // Test with NOESCAPE option
  resp = Run({"JSON.GET", "json", ".", "NOESCAPE"});
  EXPECT_EQ(resp, "{\"key\":\"value with special characters: \\n \\t \\\" \\\"\"}");  // No changes
}

TEST_F(JsonFamilyTest, Type) {
  string json = R"(
    [1, 2.3, "foo", true, null, {}, []]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TYPE", "json", "$[*]"});
  ASSERT_THAT(resp, RespArray(ElementsAre("integer", "number", "string", "boolean", "null",
                                          "object", "array")));

  resp = Run({"JSON.TYPE", "json", "$[10]"});
  EXPECT_THAT(resp, ArrLen(0));

  resp = Run({"JSON.TYPE", "not_exist_key", "$[10]"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(JsonFamilyTest, TypeLegacy) {
  string json = R"(
    {
      "firstName":"John",
      "lastName":"Smith",
      "age":27,
      "weight":135.25,
      "isAlive":true,
      "address":{"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"},
      "phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"}],
      "children":[],
      "spouse":null
    }
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TYPE", "json"});
  EXPECT_EQ(resp, "object");

  resp = Run({"JSON.TYPE", "json", ".children"});
  EXPECT_EQ(resp, "array");

  resp = Run({"JSON.TYPE", "json", ".firstName"});
  EXPECT_EQ(resp, "string");

  resp = Run({"JSON.TYPE", "json", ".age"});
  EXPECT_EQ(resp, "integer");

  resp = Run({"JSON.TYPE", "json", ".weight"});
  EXPECT_EQ(resp, "number");

  resp = Run({"JSON.TYPE", "json", ".isAlive"});
  EXPECT_EQ(resp, "boolean");

  resp = Run({"JSON.TYPE", "json", ".spouse"});
  EXPECT_EQ(resp, "null");

  resp = Run({"JSON.TYPE", "not_exist_key", ".some_field"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(JsonFamilyTest, StrLenCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . {"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})",
       Eq("OK")},
      {"", "JSON.STRLEN json", _, ErrArg("wrong JSON type of path value")},
      {"JSON.STRLEN json $.a.a", "JSON.STRLEN json .a.a", RespElementsAre(IntArg(1)), IntArg(1)},
      {"JSON.STRLEN json $.a", "JSON.STRLEN json .a", RespElementsAre(ArgType(RespExpr::NIL)),
       ErrArg("wrong JSON type of path value")},
      {"JSON.STRLEN json $.a.*", "JSON.STRLEN json .a.*", RespElementsAre(IntArg(1)), IntArg(1)},
      {"JSON.STRLEN json $.c.b", "JSON.STRLEN json .c.b", RespElementsAre(IntArg(2)), IntArg(2)},
      {"JSON.STRLEN non_existent_key $.c.b", "JSON.STRLEN non_existent_key .c.b",
       ErrArg("no such key"), ArgType(RespExpr::NIL)},
      {"JSON.STRLEN non_existent_key $", "", ErrArg("no such key"), _},
      {"JSON.STRLEN json $.c.*", "JSON.STRLEN json .c.*",
       RespArray(ElementsAre(IntArg(1), IntArg(2))), IntArg(1)},
      {"JSON.STRLEN json $.d.*", "JSON.STRLEN json .d.*",
       RespArray(ElementsAre(ArgType(RespExpr::NIL), IntArg(1), ArgType(RespExpr::NIL))),
       IntArg(1)},
  });
}

TEST_F(JsonFamilyTest, ObjLenCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . {"a":{},"b":{"a":"a"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":{"a":3,"b":4}},"e":1})",
       Eq("OK")},
      {"", "JSON.STRLEN json", _, ErrArg("wrong JSON type of path value")},
      {"JSON.OBJLEN json $.a", "JSON.OBJLEN json .a", RespElementsAre(IntArg(0)), IntArg(0)},
      {"JSON.OBJLEN json $.a.*", "JSON.OBJLEN json .a.*", RespArray(IsEmpty()),
       ArgType(RespExpr::NIL)},
      {"JSON.OBJLEN json $.b", "JSON.OBJLEN json .b", RespElementsAre(IntArg(1)), IntArg(1)},
      {"JSON.OBJLEN json $.b.*", "JSON.OBJLEN json .b.*", RespElementsAre(ArgType(RespExpr::NIL)),
       ErrArg("wrong JSON type of path value")},
      {"JSON.OBJLEN json $.c", "JSON.OBJLEN json .c", RespElementsAre(IntArg(2)), IntArg(2)},
      {"JSON.OBJLEN json $.d", "JSON.OBJLEN json .d", RespElementsAre(IntArg(3)), IntArg(3)},
      {"JSON.OBJLEN non_existent_key $.a", "JSON.OBJLEN non_existent_key .a", ErrArg("no such key"),
       ArgType(RespExpr::NIL)},
      {"", "JSON.OBJLEN json .none", _, ArgType(RespExpr::NIL)},
      {"JSON.OBJLEN json $.c.*", "JSON.OBJLEN json .c.*",
       RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL))),
       ErrArg("wrong JSON type of path value")},
      {"JSON.OBJLEN json $.d.*", "JSON.OBJLEN json .d.*",
       RespArray(ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), IntArg(2))),
       IntArg(2)},
      {"JSON.OBJLEN json $.*", "JSON.OBJLEN json .*",
       RespArray(ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(3), ArgType(RespExpr::NIL))),
       IntArg(0)},
  });
}

TEST_F(JsonFamilyTest, ArrLenCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . [[],["a"],["a","b"],["a","b","c"]])", Eq("OK")},
      {"", "JSON.ARRLEN json", _, IntArg(4)},
      {"JSON.ARRLEN json $[*]", "JSON.ARRLEN json [*]",
       RespArray(ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(3))), IntArg(0)},
      {"", "JSON.ARRLEN json [3]", _, IntArg(3)},
      {R"(JSON.SET json . [[],"a",["a","b"],["a","b","c"],4])", Eq("OK")},
      {"JSON.ARRLEN json $[*]", "JSON.ARRLEN json [*]",
       RespArray(ElementsAre(IntArg(0), ArgType(RespExpr::NIL), IntArg(2), IntArg(3),
                             ArgType(RespExpr::NIL))),
       IntArg(0)},
      {"", "JSON.ARRLEN json [1]", _, ErrArg("wrong JSON type of path value")},
      {"", "JSON.ARRLEN json [2]", _, IntArg(2)},
      {"JSON.OBJLEN non_existent_key $[*]", "JSON.OBJLEN non_existent_key [*]",
       ErrArg("no such key"), ArgType(RespExpr::NIL)},
  });
}

TEST_F(JsonFamilyTest, Toggle) {
  string json = R"(
    {"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TOGGLE", "json", "$.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(1), ArgType(RespExpr::NIL), ArgType(RespExpr::NIL),
                          ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([false,true,1,null,"foo",[],{}])");

  resp = Run({"JSON.TOGGLE", "json", "$.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(IntArg(1), IntArg(0), ArgType(RespExpr::NIL), ArgType(RespExpr::NIL),
                          ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([true,false,1,null,"foo",[],{}])");
}

TEST_F(JsonFamilyTest, ToggleLegacy) {
  string json = R"(
    {"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TOGGLE", "json"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  resp = Run({"JSON.TOGGLE", "json", ".*"});
  EXPECT_EQ(resp, "true");

  resp = Run({"JSON.TOGGLE", "json", ".*"});
  EXPECT_EQ(resp, "false");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(R"([true,false,1,null,"foo",[],{}])", resp);

  resp = Run({"JSON.SET", "json", ".", "true"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TOGGLE", "json", "."});
  EXPECT_EQ(resp, "false");

  resp = Run({"JSON.TOGGLE", "json", "."});
  EXPECT_EQ(resp, "true");

  json = R"(
    {"isAvailable": false}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TOGGLE", "json", ".isAvailable"});
  EXPECT_EQ(resp, "true");

  resp = Run({"JSON.TOGGLE", "json", ".isAvailable"});
  EXPECT_EQ(resp, "false");
}

TEST_F(JsonFamilyTest, NumIncrByCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . {"e":1.5,"a":1})", "", Eq("OK"), _},
      {"JSON.NUMINCRBY json $.a -2", "", Eq("[-1]"), _},

      // At 2^63 the double ULP is 2048, so this increment is exact.
      {R"(JSON.SET json . {"a":9223372036854775808})", "", Eq("OK"), _},
      {"JSON.NUMINCRBY json $.a 2048", "", Eq("[9223372036854777856]"), _},

      // A result below INT64_MIN must report overflow.
      {R"(JSON.SET json . {"a":-9223372036854775808})", "", Eq("OK"), _},
      {"JSON.NUMINCRBY json $.a -9223372036854775808", "", ErrArg("ERR result is not a number"), _},
      {R"(JSON.SET json . {"e":1.5,"a":1})", Eq("OK")},
      {"JSON.NUMINCRBY json $.a 1.1", "JSON.NUMINCRBY json .a 1.1", Eq("[2.1]"), Eq("2.1")},
      {"JSON.NUMINCRBY json $.e 1", "JSON.NUMINCRBY json .e 1", Eq("[2.5]"), Eq("2.5")},
      {"JSON.NUMINCRBY json $.e inf", "JSON.NUMINCRBY json .e inf",
       ErrArg("ERR result is not a number"), ErrArg("ERR result is not a number")},
      {R"(JSON.SET json . {"e":1.5,"a":1})", Eq("OK")},
      {"JSON.NUMINCRBY json $.e 1.7e308", "JSON.NUMINCRBY json .e 1.7e308", Eq("[1.7e+308]"),
       Eq("1.7e+308")},
      {"JSON.NUMINCRBY json $.e 1.7e308", "JSON.NUMINCRBY json .e 1.7e308",
       ErrArg("ERR result is not a number"), ErrArg("ERR result is not a number")},
      {"JSON.GET json $.*", Eq("[1,1.7e+308]")},
      {R"(JSON.SET json . {"a":[],"b":[1],"c":[1,2],"d":[1,2,3]})", Eq("OK")},
      {"JSON.NUMINCRBY json $.d[*] 10", "JSON.NUMINCRBY json .d[*] 10", Eq("[11,12,13]"), Eq("13")},
      {"JSON.GET json $.d[*]", Eq("[11,12,13]")},
      {R"(JSON.SET json . {"a":[],"b":[1],"c":[1,2],"d":[1,2,3]})", Eq("OK")},
      {"JSON.NUMINCRBY json $.a[*] 1", "JSON.NUMINCRBY json .a[*] 1", Eq("[]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMINCRBY json $.b[*] 1", "JSON.NUMINCRBY json .b[*] 1", Eq("[2]"), Eq("2")},
      {"JSON.NUMINCRBY json $.c[*] 1", "JSON.NUMINCRBY json .c[*] 1", Eq("[2,3]"), Eq("3")},
      {"JSON.NUMINCRBY json $.d[*] 1", "JSON.NUMINCRBY json .d[*] 1", Eq("[2,3,4]"), Eq("4")},
      {"JSON.NUMINCRBY json $.d[2] 1", "JSON.NUMINCRBY json .d[2] 1", Eq("[5]"), Eq("5")},
      {"JSON.GET json", Eq(R"({"a":[],"b":[2],"c":[2,3],"d":[2,3,5]})")},
      {R"(JSON.SET json . {"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{"a":1,"b":2,"c":3}})",
       Eq("OK")},
      {"JSON.NUMINCRBY json $.a.* 1", "JSON.NUMINCRBY json .a.* 1", Eq("[]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMINCRBY json $.b.* 1", "JSON.NUMINCRBY json .b.* 1", Eq("[2]"), Eq("2")},
      {"JSON.NUMINCRBY json $.c.* 1", "JSON.NUMINCRBY json .c.* 1", Eq("[2,3]"), Eq("3")},
      {"JSON.NUMINCRBY json $.d.* 1", "JSON.NUMINCRBY json .d.* 1", Eq("[2,3,4]"), Eq("4")},
      {"JSON.GET json", Eq(R"({"a":{},"b":{"a":2},"c":{"a":2,"b":3},"d":{"a":2,"b":3,"c":4}})")},
      {R"(JSON.SET json . {"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"b"},"d":{"a":1,"b":"b","c":3}})",
       Eq("OK")},
      {"JSON.NUMINCRBY json $.a.* 1", "JSON.NUMINCRBY json .a.* 1", Eq("[null]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMINCRBY json $.b.* 1", "JSON.NUMINCRBY json .b.* 1", Eq("[null,2]"), Eq("2")},
      {"JSON.NUMINCRBY json $.c.* 1", "JSON.NUMINCRBY json .c.* 1", Eq("[null,null]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMINCRBY json $.d.* 1", "JSON.NUMINCRBY json .d.* 1", Eq("[2,null,4]"), Eq("4")},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":4}})")},
  });
}

TEST_F(JsonFamilyTest, NumMultByCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . {"a":[],"b":[1],"c":[1,2],"d":[1,2,3]})", Eq("OK")},
      {"JSON.NUMMULTBY json $.d[*] 2", "JSON.NUMMULTBY json .d[*] 2", Eq("[2,4,6]"), Eq("6")},
      {"JSON.GET json $.d[*]", Eq("[2,4,6]")},
      {R"(JSON.SET json . {"a":[],"b":[1],"c":[1,2],"d":[1,2,3]})", Eq("OK")},
      {"JSON.NUMMULTBY json $.a[*] 2", "JSON.NUMMULTBY json .a[*] 2", Eq("[]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMMULTBY json $.b[*] 2", "JSON.NUMMULTBY json .b[*] 2", Eq("[2]"), Eq("2")},
      {"JSON.NUMMULTBY json $.c[*] 2", "JSON.NUMMULTBY json .c[*] 2", Eq("[2,4]"), Eq("4")},
      {"JSON.NUMMULTBY json $.d[*] 2", "JSON.NUMMULTBY json .d[*] 2", Eq("[2,4,6]"), Eq("6")},
      {"JSON.GET json", Eq(R"({"a":[],"b":[2],"c":[2,4],"d":[2,4,6]})")},
      {R"(JSON.SET json . {"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{"a":1,"b":2,"c":3}})",
       Eq("OK")},
      {"JSON.NUMMULTBY json $.a.* 2", "JSON.NUMMULTBY json .a.* 2", Eq("[]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMMULTBY json $.b.* 2", "JSON.NUMMULTBY json .b.* 2", Eq("[2]"), Eq("2")},
      {"JSON.NUMMULTBY json $.c.* 2", "JSON.NUMMULTBY json .c.* 2", Eq("[2,4]"), Eq("4")},
      {"JSON.NUMMULTBY json $.d.* 2", "JSON.NUMMULTBY json .d.* 2", Eq("[2,4,6]"), Eq("6")},
      {"JSON.GET json", Eq(R"({"a":{},"b":{"a":2},"c":{"a":2,"b":4},"d":{"a":2,"b":4,"c":6}})")},
      {R"(JSON.SET json . {"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"b"},"d":{"a":1,"b":"b","c":3}})",
       Eq("OK")},
      {"JSON.NUMMULTBY json $.a.* 2", "JSON.NUMMULTBY json .a.* 2", Eq("[null]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMMULTBY json $.b.* 2", "JSON.NUMMULTBY json .b.* 2", Eq("[null,2]"), Eq("2")},
      {"JSON.NUMMULTBY json $.c.* 2", "JSON.NUMMULTBY json .c.* 2", Eq("[null,null]"),
       ErrArg("wrong JSON type of path value")},
      {"JSON.NUMMULTBY json $.d.* 2", "JSON.NUMMULTBY json .d.* 2", Eq("[2,null,6]"), Eq("6")},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":6}})")},
  });
}

TEST_F(JsonFamilyTest, NumericOperationsWithConversionsCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . {"a":2.0})", Eq("OK")},
      {"JSON.NUMINCRBY json $.a 1", "JSON.NUMINCRBY json .a 1", Eq("[3.0]"), Eq("3.0")},
      {"JSON.NUMINCRBY json $.a 1.0", "JSON.NUMINCRBY json .a 1.0", Eq("[4.0]"), Eq("4.0")},
      {"JSON.NUMMULTBY json $.a 2", "JSON.NUMMULTBY json .a 2", Eq("[8.0]"), Eq("8.0")},
      {"JSON.NUMMULTBY json $.a 2.0", "JSON.NUMMULTBY json .a 2.0", Eq("[16.0]"), Eq("16.0")},
      {"JSON.GET json", Eq(R"({"a":16.0})")},
      {R"(JSON.SET json . {"a":2})", Eq("OK")},
      {"JSON.NUMINCRBY json $.a 1", "JSON.NUMINCRBY json .a 1", Eq("[3]"), Eq("3")},
      {"JSON.GET json", Eq(R"({"a":3})")},
      {"JSON.NUMINCRBY json $.a 1.0", "JSON.NUMINCRBY json .a 1.0", Eq("[4.0]"), Eq("4.0")},
      {"JSON.GET json", Eq(R"({"a":4.0})")},
      {R"(JSON.SET json . {"a":2})", Eq("OK")},
      {"JSON.NUMMULTBY json $.a 2", "JSON.NUMMULTBY json .a 2", Eq("[4]"), Eq("4")},
      {"JSON.GET json", Eq(R"({"a":4})")},
      {"JSON.NUMMULTBY json $.a 2.0", "JSON.NUMMULTBY json .a 2.0", Eq("[8.0]"), Eq("8.0")},
      {"JSON.GET json", Eq(R"({"a":8.0})")},
  });
}

TEST_F(JsonFamilyTest, NumericOperationsResp2Resp3) {
  // Test RESP2 behavior
  Run({"HELLO", "2"});

  auto resp = Run({"JSON.SET", "a", "$", "1"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "a", "$", "1"});
  EXPECT_EQ(resp, "[2]");  // Currently returns string "[2]"

  resp = Run({"JSON.TYPE", "a", "$"});
  EXPECT_THAT(resp, RespElementsAre("integer"));

  resp = Run({"JSON.TYPE", "a", "."});
  EXPECT_EQ(resp, "integer");

  resp = Run({"JSON.NUMMULTBY", "a", "$", "2"});
  EXPECT_EQ(resp, "[4]");  // Currently returns string "[4]"

  // Test RESP3 behavior
  Run({"HELLO", "3"});
  Run({"FLUSHALL"});

  resp = Run({"JSON.SET", "a", "$", "1"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "a", "$", "1"});
  // In RESP3, this should return a proper array with integer: 1) (integer) 2
  EXPECT_THAT(resp, RespElementsAre(IntArg(2)));

  resp = Run({"JSON.TYPE", "a", "$"});
  EXPECT_THAT(resp, RespElementsAre(RespArray(ElementsAre("integer"))));

  resp = Run({"JSON.TYPE", "a", "."});
  EXPECT_THAT(resp, RespElementsAre("integer"));

  resp = Run({"JSON.NUMMULTBY", "a", "$", "2"});
  // In RESP3, this should return a proper array with integer: 1) (integer) 4
  EXPECT_THAT(resp, RespElementsAre(IntArg(4)));
}

TEST_F(JsonFamilyTest, Del) {
  string json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}, "e": [1,2,3,4,5]}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", "$.d.*"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[1,2,3,4,5]})");

  resp = Run({"JSON.DEL", "json", "$.e[*]"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[]})");

  resp = Run({"JSON.DEL", "json", "$..*"});

  // TODO: legacy jsoncons implementation returns, 8 but in practive it should return 5.
  // redis-stack returns 5 as well.
  // Once we drop jsoncons path, we can enforce here equality.
  EXPECT_GE(resp.GetInt(), 5);

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");

  resp = Run({"JSON.DEL", "json"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});  // This is legal since the key was removed
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  json = R"(
    {"a":[{"b": [1,2,3]}], "b": [{"c": 2}], "c']":[1,2,3]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", "$.a[0].b[0]"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});  // not a legal type
  EXPECT_THAT(resp, ErrArg("Operation against a key holding the wrong kind of value"));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[{"b":[2,3]}],"b":[{"c":2}],"c']":[1,2,3]})");

  resp = Run({"JSON.DEL", "json", "$.b[0].c"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[{"b":[2,3]}],"b":[{}],"c']":[1,2,3]})");

  resp = Run({"JSON.DEL", "json", "$.*"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");

  resp = Run({"JSON.SET", "json", "$", R"({"a": 1})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", "$"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  if (absl::GetFlag(FLAGS_jsonpathv2)) {
    // Test recursive delete with $..a path
    resp = Run({"JSON.SET", "doc2", "$",
                R"({"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [true, "a", "b"]}})"});
    ASSERT_THAT(resp, "OK");

    resp = Run({"JSON.GET", "doc2"});
    EXPECT_EQ(resp, R"({"a":{"a":2,"b":3},"b":["a","b"],"nested":{"b":[true,"a","b"]}})");

    // JSON.DEL with $..a should find and delete the key "a" at root level
    // but not string values "a" inside arrays
    resp = Run({"JSON.DEL", "doc2", "$..a"});
    EXPECT_THAT(resp, IntArg(1));

    resp = Run({"JSON.GET", "doc2"});
    EXPECT_EQ(resp, R"({"b":["a","b"],"nested":{"b":[true,"a","b"]}})");
  }
}

TEST_F(JsonFamilyTest, DelLegacy) {
  string json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}, "e": [1,2,3,4,5]}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", ".d.*"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[1,2,3,4,5]})");

  resp = Run({"JSON.DEL", "json", ".e[*]"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[]})");

  resp = Run({"JSON.DEL", "json", "..*"});
  EXPECT_GE(resp.GetInt(), 5);

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");

  resp = Run({"JSON.DEL", "json"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"GET", "json"});  // This is legal since the key was removed
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  json = R"(
    {"a":[{"b": [1,2,3]}], "b": [{"c": 2}], "c']":[1,2,3]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", ".a[0].b[0]"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});  // not a legal type
  EXPECT_THAT(resp, ErrArg("Operation against a key holding the wrong kind of value"));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[{"b":[2,3]}],"b":[{"c":2}],"c']":[1,2,3]})");

  resp = Run({"JSON.DEL", "json", ".b[0].c"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[{"b":[2,3]}],"b":[{}],"c']":[1,2,3]})");

  resp = Run({"JSON.DEL", "json", ".*"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");

  resp = Run({"JSON.SET", "json", ".", R"({"a": 1})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", "."});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(JsonFamilyTest, ObjKeys) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$"});
  EXPECT_THAT(resp, RespElementsAre(RespArray(ElementsAre("a", "b", "c", "d", "e"))));

  resp = Run({"JSON.OBJKEYS", "json", "$.a"});
  EXPECT_THAT(resp, RespElementsAre(RespArray(IsEmpty())));

  resp = Run({"JSON.OBJKEYS", "json", "$.b"});
  EXPECT_THAT(resp, RespElementsAre(RespArray(ElementsAre("a"))));

  resp = Run({"JSON.OBJKEYS", "json", "$.*"});
  EXPECT_THAT(resp, ElementsAreArrays(IsEmpty(), ElementsAre("a"), ElementsAre("a", "b"),
                                      ElementsAre("a", "b", "c"), IsEmpty()));

  resp = Run({"JSON.OBJKEYS", "json", "$.notfound"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());

  json = R"(
     {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
   )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$..a"});
  EXPECT_THAT(resp, ElementsAreArrays(IsEmpty(), ElementsAre("b", "c")));

  json = R"(
     {"a":{}, "b":{"c":{"d": {"e": 1337}}}}
   )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$..*"});
  EXPECT_THAT(resp, ElementsAreArrays(IsEmpty(), ElementsAre("c"), ElementsAre("d"),
                                      ElementsAre("e"), IsEmpty()));
}

TEST_F(JsonFamilyTest, ObjKeysLegacy) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "b", "c", "d", "e"));

  resp = Run({"JSON.OBJKEYS", "json", "."});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "b", "c", "d", "e"));

  resp = Run({"JSON.OBJKEYS", "json", ".a"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());

  resp = Run({"JSON.OBJKEYS", "json", ".b"});
  EXPECT_THAT(resp, RespElementsAre("a"));

  resp = Run({"JSON.OBJKEYS", "json", ".*"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());

  resp = Run({"JSON.OBJKEYS", "json", ".notfound"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  json = R"(
     {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
   )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "..a"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());

  json = R"(
     {"a":{}, "b":{"c":{"d": {"e": 1337}}}}
   )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "..*"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());
}

TEST_F(JsonFamilyTest, StrAppendDefaultPath) {
  auto resp = Run({"JSON.SET", "json", ".", R"("foo")"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", R"("bar")"});
  EXPECT_THAT(resp, IntArg(6));

  resp = Run({"JSON.STRAPPEND", "json", ".", R"("baz")", "extra"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(resp, R"("foobar")");
}

TEST_F(JsonFamilyTest, StrAppendCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . {"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})",
       Eq("OK")},
      {R"(JSON.STRAPPEND json $.a.a "ab")", R"(JSON.STRAPPEND json .a.a "ab")",
       RespElementsAre(IntArg(3)), IntArg(3)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aab"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})")},
      {R"(JSON.STRAPPEND json $.a.* "a")", R"(JSON.STRAPPEND json .a.* "a")",
       RespElementsAre(IntArg(4)), IntArg(4)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})")},
      {R"(JSON.STRAPPEND json $.c.b "a")", R"(JSON.STRAPPEND json .c.b "a")",
       RespElementsAre(IntArg(3)), IntArg(3)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bba"},"d":{"a":1,"b":"b","c":3}})")},
      {R"(JSON.STRAPPEND json $.b.* "a")", R"(JSON.STRAPPEND json .b.* "a")",
       RespArray(ElementsAre(IntArg(2), ArgType(RespExpr::NIL))), IntArg(2)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"a","b":"bba"},"d":{"a":1,"b":"b","c":3}})")},
      {R"(JSON.STRAPPEND json $.c.* "a")", R"(JSON.STRAPPEND json .c.* "a")",
       RespArray(ElementsAre(IntArg(2), IntArg(4))), IntArg(4)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"b","c":3}})")},
      {R"(JSON.STRAPPEND json $.d.* "a")", R"(JSON.STRAPPEND json .d.* "a")",
       RespArray(ElementsAre(ArgType(RespExpr::NIL), IntArg(2), ArgType(RespExpr::NIL))),
       IntArg(2)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"ba","c":3}})")},
      {R"(JSON.SET json . {"a":{"a":"a","b":"aa","c":"aaa"},"b":{"a":"aaa","b":"aa","c":"a"}})",
       Eq("OK")},
      {R"(JSON.STRAPPEND json $.a.* "a")", R"(JSON.STRAPPEND json .a.* "a")",
       RespArray(ElementsAre(IntArg(2), IntArg(3), IntArg(4))), IntArg(4)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aa","b":"aaa","c":"aaaa"},"b":{"a":"aaa","b":"aa","c":"a"}})")},
      {R"(JSON.STRAPPEND json $.b.* "a")", R"(JSON.STRAPPEND json .b.* "a")",
       RespArray(ElementsAre(IntArg(4), IntArg(3), IntArg(2))), IntArg(2)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aa","b":"aaa","c":"aaaa"},"b":{"a":"aaaa","b":"aaa","c":"aa"}})")},
      {R"(JSON.SET json . {"a":{"a":"a","b":"aa","c":["aaaaa","aaaaa"]},"b":{"a":"aaa","b":["aaaaa","aaaaa"],"c":"a"}})",
       Eq("OK")},
      {R"(JSON.STRAPPEND json $.a.* "a")", R"(JSON.STRAPPEND json .a.* "a")",
       RespArray(ElementsAre(IntArg(2), IntArg(3), ArgType(RespExpr::NIL))), IntArg(3)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aa","b":"aaa","c":["aaaaa","aaaaa"]},"b":{"a":"aaa","b":["aaaaa","aaaaa"],"c":"a"}})")},
      {R"(JSON.STRAPPEND json $.b.* "a")", R"(JSON.STRAPPEND json .b.* "a")",
       RespArray(ElementsAre(IntArg(4), ArgType(RespExpr::NIL), IntArg(2))), IntArg(2)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aa","b":"aaa","c":["aaaaa","aaaaa"]},"b":{"a":"aaaa","b":["aaaaa","aaaaa"],"c":"aa"}})")},
      {R"(JSON.SET json . {"a":{"a":"a","b":"aa","c":{"c":"aaaaa"}},"b":{"a":"aaa","b":{"b":"aaaaa"},"c":"a"}})",
       Eq("OK")},
      {R"(JSON.STRAPPEND json $.a.* "a")", R"(JSON.STRAPPEND json .a.* "a")",
       RespArray(ElementsAre(IntArg(2), IntArg(3), ArgType(RespExpr::NIL))), IntArg(3)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aa","b":"aaa","c":{"c":"aaaaa"}},"b":{"a":"aaa","b":{"b":"aaaaa"},"c":"a"}})")},
      {R"(JSON.STRAPPEND json $.b.* "a")", R"(JSON.STRAPPEND json .b.* "a")",
       RespArray(ElementsAre(IntArg(4), ArgType(RespExpr::NIL), IntArg(2))), IntArg(2)},
      {"JSON.GET json",
       Eq(R"({"a":{"a":"aa","b":"aaa","c":{"c":"aaaaa"}},"b":{"a":"aaaa","b":{"b":"aaaaa"},"c":"aa"}})")},
      {R"(JSON.SET json . {"a":"foo","inner":{"a":"bye"},"inner1":{"a":7}})", Eq("OK")},
      {R"(JSON.STRAPPEND json $..a "bar")", R"(JSON.STRAPPEND json ..a "bar")",
       RespArray(ElementsAre(IntArg(6), IntArg(6), ArgType(RespExpr::NIL))), IntArg(6)},
      {"JSON.GET json", Eq(R"({"a":"foobar","inner":{"a":"byebar"},"inner1":{"a":7}})")},
  });
}

TEST_F(JsonFamilyTest, ClearCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . [[],[0],[0,1],[0,1,2],1,true,null,"d"])", Eq("OK")},
      {"JSON.CLEAR json $[*]", "JSON.CLEAR json [*]", IntArg(5), IntArg(5)},
      {"JSON.GET json", Eq(R"([[],[],[],[],0,true,null,"d"])")},
      {"JSON.CLEAR json $", "", IntArg(1), _},
      {"", "JSON.CLEAR json .", _, IntArg(1)},
      {"JSON.GET json", Eq("[]")},
      {R"(JSON.SET json . {"children":["Yossi","Rafi","Benni","Avraham","Yehoshua","Moshe"]})",
       Eq("OK")},
      {"JSON.CLEAR json $.children", "JSON.CLEAR json .children", IntArg(1), IntArg(1)},
      {"JSON.GET json", Eq(R"({"children":[]})")},
      {"", "JSON.CLEAR json .", _, IntArg(1)},
      {"", "JSON.GET json", _, Eq("{}")},
      {"", R"(JSON.SET json . {"children":["Yossi","Rafi","Benni","Avraham","Yehoshua","Moshe"]})",
       _, Eq("OK")},
      {"JSON.CLEAR json $", "JSON.CLEAR json", IntArg(1), IntArg(1)},
      {"JSON.GET json", Eq("{}")},
  });
}

TEST_F(JsonFamilyTest, ArrPopCompatibility) {
  CheckCompatibility({
      {"JSON.SET json . [[6,1,6],[7,2,7],[8,3,8]]", Eq("OK")},
      {"JSON.ARRPOP json $[*] -2", "JSON.ARRPOP json [*] -2", RespArray(ElementsAre("1", "2", "3")),
       Eq("3")},
      {"JSON.GET json", Eq("[[6,6],[7,7],[8,8]]")},
      {R"(JSON.SET json . [[],["a"],["a","b"]])", Eq("OK")},
      {"JSON.ARRPOP json $[*]", "",
       RespArray(ElementsAre(ArgType(RespExpr::NIL), R"("a")", R"("b")")), _},
      {"", "JSON.ARRPOP json .", _, Eq(R"(["a","b"])")},
      {"JSON.GET json", "JSON.GET json", Eq(R"([[],[],["a"]])"), Eq(R"([[],["a"]])")},
      {"", R"(JSON.SET json . [[],["a"],["a","b"]])", _, Eq("OK")},
      {"", "JSON.ARRPOP json . 0", _, Eq("[]")},
      {"", "JSON.GET json", _, Eq(R"([["a"],["a","b"]])")},
      {"", "JSON.ARRPOP json", _, Eq(R"(["a","b"])")},
      {"", R"(JSON.SET json . {"a":"b"})", _, Eq("OK")},
      {"", "JSON.ARRPOP json .", _, ArgType(RespExpr::NIL)},
      {"", "JSON.SET json . []", _, Eq("OK")},
      {"", "JSON.ARRPOP json .", _, ArgType(RespExpr::NIL)},
  });
}

TEST_F(JsonFamilyTest, ArrPopOutOfRange) {
  string json = R"(
    [0,1,2,3,4,5]
  )";

  auto resp = Run({"JSON.SET", "arr", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "arr", "$", "-55"});
  EXPECT_THAT(resp, RespElementsAre("0"));

  resp = Run({"JSON.SET", "arr", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "arr", "$", "55"});
  EXPECT_THAT(resp, RespElementsAre("5"));

  // Test legacy mode
  resp = Run({"JSON.SET", "arr", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "arr", ".", "-55"});
  EXPECT_EQ(resp, "0");

  resp = Run({"JSON.SET", "arr", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "arr", ".", "55"});
  EXPECT_EQ(resp, "5");
}

TEST_F(JsonFamilyTest, ArrTrimCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . [[],["a"],["a","b"],["a","b","c"]])", Eq("OK")},
      {"JSON.ARRTRIM json $[*] 0 1", "JSON.ARRTRIM json [*] 0 1",
       RespArray(ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(2))), IntArg(2)},
      {"JSON.GET json", Eq(R"([[],["a"],["a","b"],["a","b"]])")},
      {R"(JSON.SET json . {"a":[],"nested":{"a":[1,4]}})", Eq("OK")},
      {"JSON.ARRTRIM json $..a 0 1", "JSON.ARRTRIM json ..a 0 1",
       RespArray(ElementsAre(IntArg(0), IntArg(2))), IntArg(2)},
      {"JSON.GET json", Eq(R"({"a":[],"nested":{"a":[1,4]}})")},
      {R"(JSON.SET json . {"a":[1,2,3,2],"nested":{"a":false}})", Eq("OK")},
      {"JSON.ARRTRIM json $..a 1 2", "JSON.ARRTRIM json ..a 1 2",
       RespArray(ElementsAre(IntArg(2), ArgType(RespExpr::NIL))), IntArg(2)},
      {"JSON.GET json", Eq(R"({"a":[2,3],"nested":{"a":false}})")},
      {"JSON.SET json $ [1,2,3,4,5,6,7]", Eq("OK")},
      {"JSON.ARRTRIM json $ 2 3", "JSON.ARRTRIM json . 2 3", RespElementsAre(IntArg(2)), IntArg(2)},
      {"JSON.GET json", Eq("[3,4]")},
      {"", R"(JSON.SET json . {"a":"b"})", _, Eq("OK")},
      {"", "JSON.ARRTRIM json . 0 0", _, ErrArg("wrong JSON type of path value")},
  });
}

TEST_F(JsonFamilyTest, ArrTrimOutOfRange) {
  string arr = R"(
    [0,1,2,3,4]
  )";

  auto resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-1", "3"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(0)));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "54", "55"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(0)));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "56", "55"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(0)));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-55", "-55"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(1)));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[0]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-2", "-1"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(2)));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[3,4]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-1", "-2"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(0)));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");
}

TEST_F(JsonFamilyTest, ArrInsertCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . [[],["a"],["a","b"]])", Eq("OK")},
      {R"(JSON.ARRINSERT json $[*] 0 "a")", "",
       RespArray(ElementsAre(IntArg(1), IntArg(2), IntArg(3))), _},
      {"JSON.GET json", "", Eq(R"([["a"],["a","a"],["a","a","b"]])"), _},
      {R"(JSON.ARRINSERT json $[*] -1 "b")", "",
       RespArray(ElementsAre(IntArg(2), IntArg(3), IntArg(4))), _},
      {"JSON.GET json", "", Eq(R"([["b","a"],["a","b","a"],["a","a","b","b"]])"), _},
      {R"(JSON.ARRINSERT json $[*] 1 "c")", "",
       RespArray(ElementsAre(IntArg(3), IntArg(4), IntArg(5))), _},
      {"", R"(JSON.ARRINSERT json [*] 0 "c")", _, IntArg(3)},
      {"", R"(JSON.ARRINSERT json . 0 "c")", _, IntArg(4)},
      {"JSON.GET json", "JSON.GET json",
       Eq(R"([["b","c","a"],["a","c","b","a"],["a","c","a","b","b"]])"),
       Eq(R"(["c",["c"],["c","a"],["c","a","b"]])")},
      {R"(JSON.SET json . {"a":{"b":"c"},"b":[["a"],["a","b"]]})", Eq("OK")},
      {R"(JSON.ARRINSERT json $.a 0 "c")", R"(JSON.ARRINSERT json .a 0 "c")",
       RespElementsAre(ArgType(RespExpr::NIL)), ErrArg("wrong JSON type of path value")},
      {"JSON.ARRINSERT json $ 0", "", ErrArg("wrong number of arguments"), _},
  });
}

TEST_F(JsonFamilyTest, ArrInsertOutOfRange) {
  string json = R"(
    [0,1,2,3,4,5]
  )";

  auto resp = Run({"JSON.SET", "arr", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINSERT", "arr", "$", "-55", "6"});
  EXPECT_THAT(resp, ErrArg("index out of range"));

  resp = Run({"JSON.ARRINSERT", "arr", "$", "55", "6"});
  EXPECT_THAT(resp, ErrArg("index out of range"));

  resp = Run({"JSON.ARRINSERT", "arr", ".", "-55", "6"});  // Legacy mode
  EXPECT_THAT(resp, ErrArg("index out of range"));

  resp = Run({"JSON.ARRINSERT", "arr", ".", "55", "6"});  // Legacy mode
  EXPECT_THAT(resp, ErrArg("index out of range"));

  resp = Run({"JSON.SET", "arr", ".", "[]"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINSERT", "arr", "$", "-1", "2"});
  EXPECT_THAT(resp, ErrArg("index out of range"));

  resp = Run({"JSON.ARRINSERT", "arr", "$", "1", "2"});
  EXPECT_THAT(resp, ErrArg("index out of range"));

  resp = Run({"JSON.ARRINSERT", "arr", "$", "0", "2"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(1)));

  resp = Run({"JSON.GET", "arr"});
  EXPECT_EQ(resp, "[2]");
}

TEST_F(JsonFamilyTest, ArrAppendCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . [[],["a"],["a","b"]])", Eq("OK")},
      {R"(JSON.ARRAPPEND json $[*] "a")", "",
       RespArray(ElementsAre(IntArg(1), IntArg(2), IntArg(3))), _},
      {R"(JSON.ARRAPPEND json $[*] "b")", "",
       RespArray(ElementsAre(IntArg(2), IntArg(3), IntArg(4))), _},
      {R"(JSON.SET json . {"a":[1],"nested":{"a":[1,2],"nested2":{"a":42}}})", "", Eq("OK"), _},
      {"JSON.ARRAPPEND json $..a 3", "",
       RespArray(ElementsAre(IntArg(2), IntArg(3), ArgType(RespExpr::NIL))), _},
      {"", R"(JSON.ARRAPPEND json [-1] "c")", _, IntArg(3)},
      {"", R"(JSON.ARRAPPEND json .* "c")", _, IntArg(4)},
      {"JSON.GET json", "JSON.GET json",
       Eq(R"({"a":[1,3],"nested":{"a":[1,2,3],"nested2":{"a":42}}})"),
       Eq(R"([["c"],["a","c"],["a","b","c","c"]])")},
      {"", R"(JSON.SET json . {"a":"b"})", _, Eq("OK")},
      {"", R"(JSON.ARRAPPEND json . "c")", _, ErrArg("wrong JSON type of path value")},
  });
}

TEST_F(JsonFamilyTest, ArrIndexCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET json . [[],["a"],["a","b"],["a","b","c"]])", "", Eq("OK"), _},
      {R"(JSON.ARRINDEX json $[*] "b")", "",
       RespArray(ElementsAre(IntArg(-1), IntArg(-1), IntArg(1), IntArg(1))), _},
      {R"(JSON.SET json . {"a":["a","b","c","d"],"nested":{"a":["c","d"]}})", "", Eq("OK"), _},
      {R"(JSON.ARRINDEX json $..a "b")", "", RespArray(ElementsAre(IntArg(1), IntArg(-1))), _},
      {R"(JSON.SET json . {"a":["a","b","c","d"],"nested":{"a":false}})", "", Eq("OK"), _},
      {R"(JSON.ARRINDEX json $..a "b")", "",
       RespArray(ElementsAre(IntArg(1), ArgType(RespExpr::NIL))), _},
      {R"(JSON.SET json . {"key":["Alice","Bob","Carol","David","Eve","Frank"]})", "", Eq("OK"), _},
      {R"(JSON.ARRINDEX json $.key "Bob")", "", RespElementsAre(IntArg(1)), _},
      {R"(JSON.ARRINDEX json $.key "Bob" 1 2)", "", RespElementsAre(IntArg(1)), _},
      {"", R"(JSON.SET json . {"children":["John","Jack","Tom","Bob","Mike"]})", _, Eq("OK")},
      {"", R"(JSON.ARRINDEX json .children "Tom")", _, IntArg(2)},
      {"", R"(JSON.ARRINDEX json .children "DoesNotExist")", _, IntArg(-1)},
      {"", "JSON.ARRINDEX json .children.[0].notexist 3", _, ArgType(RespExpr::ERROR)},
      {"", R"(JSON.SET json . {"a":"b"})", _, Eq("OK")},
      {"", R"(JSON.ARRINDEX json . "Tom")", _, ErrArg("wrong JSON type of path value")},
  });
}

TEST_F(JsonFamilyTest, ArrIndexWithNumericValuesCompatibility) {
  CheckCompatibility({
      {"JSON.SET json $ [2,3.0,3]", "JSON.SET json . [2,3.0,3]", Eq("OK"), Eq("OK")},
      {"JSON.ARRINDEX json $ 3", "JSON.ARRINDEX json . 3", RespElementsAre(IntArg(2)), IntArg(2)},
      {"JSON.ARRINDEX json $ 3.0", "JSON.ARRINDEX json . 3.0", RespElementsAre(IntArg(1)),
       IntArg(1)},
      {"JSON.SET json $ [[1,2,3],[1.0,2.0,3.0],2.0,[1,2,3]]", "", Eq("OK"), _},
      {"JSON.ARRINDEX json $ [1,2,3]", "", RespElementsAre(IntArg(0)), _},
      {"JSON.ARRINDEX json $ [1.0,2.0,3.0]", "", RespElementsAre(IntArg(1)), _},
      {R"(JSON.SET json $ [{"a":2},{"a":2.0},2.0])", "", Eq("OK"), _},
      {R"(JSON.ARRINDEX json $ {"a":2})", "", RespElementsAre(IntArg(0)), _},
      {R"(JSON.ARRINDEX json $ {"a":2.0})", "", RespElementsAre(IntArg(1)), _},
      {R"(JSON.SET json $ [{"arr":[1,2,3],"number":2},{"arr":[1.0,2.0,3.0],"number":2.0},2])",
       R"(JSON.SET json . [{"arr":[1,2,3],"number":2},{"arr":[1.0,2.0,3.0],"number":2.0},2])",
       Eq("OK"), Eq("OK")},
      {R"(JSON.ARRINDEX json $ {"arr":[1,2,3],"number":2})",
       R"(JSON.ARRINDEX json . {"arr":[1,2,3],"number":2})", RespElementsAre(IntArg(0)), IntArg(0)},
      {R"(JSON.ARRINDEX json $ {"arr":[1.0,2.0,3.0],"number":2.0})",
       R"(JSON.ARRINDEX json . {"arr":[1.0,2.0,3.0],"number":2.0})", RespElementsAre(IntArg(1)),
       IntArg(1)},
      {R"(JSON.ARRINDEX json $ {"arr":[1,2,3],"number":2.0})",
       R"(JSON.ARRINDEX json . {"arr":[1,2,3],"number":2.0})", RespElementsAre(IntArg(-1)),
       IntArg(-1)},
      {R"(JSON.ARRINDEX json $ {"arr":[1.0,2.0,3.0],"number":2})",
       R"(JSON.ARRINDEX json . {"arr":[1.0,2.0,3.0],"number":2})", RespElementsAre(IntArg(-1)),
       IntArg(-1)},
  });
}

TEST_F(JsonFamilyTest, ArrIndexOutOfRange) {
  auto resp = Run({"JSON.SET", "arr", ".", R"([1,1,1,1,1])"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-55", "-55"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(-1)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-55", "-56"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(-1)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-55", "-54"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(-1)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-2"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(3)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-2", "-1"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(3)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-2", "-3"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(-1)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "55", "56"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(4)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "55", "54"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(4)));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "5", "4"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(-1)));
}

TEST_F(JsonFamilyTest, MGet) {
  string json[] = {
      R"(
    {"address":{"street":"14 Imber Street","city":"Petah-Tikva","country":"Israel","zipcode":"49511"}}
  )",
      R"(
    {"address":{"street":"Oranienburger Str. 27","city":"Berlin","country":"Germany","zipcode":"10117"}}
  )",
      R"(
    {"a":1, "b": 2, "nested": {"a": 3}, "c": null}
  )",
      R"(
    {"a":4, "b": 5, "nested": {"a": 6}, "c": null}
  )"};

  auto resp = Run({"JSON.SET", "json1", ".", json[0]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json2", ".", json[1]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.MGET", "json1", "??INNNNVALID??"});
  EXPECT_THAT(resp, ErrArg("ERR syntax error"));

  resp = Run({"JSON.MGET", "json1", "json2", "json3", "$.address.country"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(R"(["Israel"])", R"(["Germany"])", ArgType(RespExpr::NIL)));

  resp = Run({"JSON.SET", "json3", ".", json[2]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json4", ".", json[3]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.MGET", "json3", "json4", "$..a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(R"([1,3])", R"([4,6])"));
}

TEST_F(JsonFamilyTest, MGetLegacy) {
  string json[] = {
      R"(
    {"address":{"street":"14 Imber Street","city":"Petah-Tikva","country":"Israel","zipcode":"49511"}}
  )",
      R"(
    {"address":{"street":"Oranienburger Str. 27","city":"Berlin","country":"Germany","zipcode":"10117"}}
  )",
      R"(
    {"a":1, "b": 2, "nested": {"a": 3}, "c": null}
  )",
      R"(
    {"a":4, "b": 5, "nested": {"a": 6}, "c": null}
  )"};

  auto resp = Run({"JSON.SET", "json1", ".", json[0]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json2", ".", json[1]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.MGET", "json1", "json2", "json3", ".address.country"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(R"("Israel")", R"("Germany")", ArgType(RespExpr::NIL)));

  resp = Run({"JSON.MGET", "json1", "json2", ".[0]"});
  if (auto jsonpathv2 = absl::GetFlag(FLAGS_jsonpathv2); jsonpathv2) {
    ASSERT_EQ(RespExpr::ARRAY, resp.type);
    EXPECT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));
  } else {
    EXPECT_THAT(resp, ErrArg("ERR syntax error"));
  }

  resp = Run({"JSON.SET", "json3", ".", json[2]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json4", ".", json[3]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.MGET", "json3", "json4", "..a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(R"(3)", R"(6)"));
}

TEST_F(JsonFamilyTest, DebugHelp) {
  auto resp = Run({"JSON.DEBUG", "HELP"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_EQ(resp.GetVec().size(), 3);

  EXPECT_THAT(resp.GetVec()[0].GetString(), HasSubstr("MEMORY"));
  EXPECT_THAT(resp.GetVec()[1].GetString(), HasSubstr("FIELDS"));
  EXPECT_THAT(resp.GetVec()[2].GetString(), HasSubstr("HELP"));
}

TEST_F(JsonFamilyTest, DebugMissingKey) {
  // Missing key: the default root path parses fine, so the unchecked parser error must
  // still be surfaced rather than left to abort the parser destructor.
  auto resp = Run({"JSON.DEBUG", "FIELDS"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  resp = Run({"JSON.DEBUG", "MEMORY"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(JsonFamilyTest, DebugFields) {
  string json = R"(
    [1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]]
  )";

  auto resp = Run({"JSON.SET", "json1", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "fields", "json1", "$[*]"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(1), IntArg(1), IntArg(1), IntArg(1),
                                         IntArg(0), IntArg(0), IntArg(2), IntArg(3)));

  resp = Run({"JSON.DEBUG", "fields", "json1", "$"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(14)));

  json = R"(
    [[1,2,3, [4,5,6,[6,7,8]]], {"a": {"b": {"c": 1337}}}]
  )";

  resp = Run({"JSON.SET", "json1", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "fields", "json1", "$[*]"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(11), IntArg(3)));

  resp = Run({"JSON.DEBUG", "fields", "json1", "$"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(16)));

  json = R"({"a":1, "b":2, "c":{"k1":1,"k2":2}})";

  resp = Run({"JSON.SET", "obj_doc", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "FIELDS", "obj_doc", "$.a"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(1)));

  resp = Run({"JSON.DEBUG", "fields", "obj_doc", "$.a"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(1)));
}

TEST_F(JsonFamilyTest, DebugFieldsLegacy) {
  string json = R"(
    [1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]]
  )";

  auto resp = Run({"JSON.SET", "json1", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "fields", "json1", "[*]"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.DEBUG", "fields", "json1", "."});
  EXPECT_THAT(resp, IntArg(14));

  resp = Run({"JSON.DEBUG", "fields", "json1"});
  EXPECT_THAT(resp, IntArg(14));

  json = R"(
    [[1,2,3, [4,5,6,[6,7,8]]], {"a": {"b": {"c": 1337}}}]
  )";

  resp = Run({"JSON.SET", "json1", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "fields", "json1", "[*]"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.DEBUG", "fields", "json1", "."});
  EXPECT_THAT(resp, IntArg(16));

  json = R"({"a":1, "b":2, "c":{"k1":1,"k2":2}})";

  resp = Run({"JSON.SET", "obj_doc", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "FIELDS", "obj_doc", ".a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.DEBUG", "fields", "obj_doc", ".a"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(JsonFamilyTest, DebugMemory) {
  auto resp = Run({"JSON.SET", "json1", "$",
                   R"([1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]])"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.DEBUG", "memory", "json1", "$[*]"});
  EXPECT_EQ(resp.type, RespExpr::ARRAY);
  EXPECT_EQ(resp.GetVec().size(), 9);
  EXPECT_EQ(resp.GetVec()[0].GetInt(), 0);
  EXPECT_EQ(resp.GetVec()[1].GetInt(), 0);
  EXPECT_EQ(resp.GetVec()[2].GetInt(), 0);
  EXPECT_EQ(resp.GetVec()[3].GetInt(), 0);
  EXPECT_EQ(resp.GetVec()[4].GetInt(), 0);
  EXPECT_GE(resp.GetVec()[5].GetInt(), 0);
  EXPECT_GE(resp.GetVec()[6].GetInt(), 0);
  EXPECT_GT(resp.GetVec()[7].GetInt(), 0);
  EXPECT_GT(resp.GetVec()[8].GetInt(), 0);

  resp = Run({"JSON.DEBUG", "memory", "json1", "$"});
  EXPECT_GT(resp.GetVec()[0].GetInt().value_or(0), 0);

  resp = Run({"JSON.SET", "bigstr", "$",
              R"({"text":"This is a longer string that should definitely exceed SSO buffer"})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.DEBUG", "memory", "bigstr", "$.text"});
  EXPECT_GT(resp.GetVec()[0].GetInt().value_or(0), 0);

  resp = Run({"JSON.SET", "obj_doc", "$", R"({"num":42, "obj":{"k1":1,"k2":2}})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.DEBUG", "MEMORY", "obj_doc", "$.num"});
  EXPECT_EQ(resp.GetVec()[0].GetInt().value_or(-1), 0);
  resp = Run({"JSON.DEBUG", "memory", "obj_doc", "$.obj"});
  EXPECT_GT(resp.GetVec()[0].GetInt().value_or(0), 0);
}

TEST_F(JsonFamilyTest, DebugMemoryLegacy) {
  auto resp = Run({"JSON.SET", "json1", "$",
                   R"([1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]])"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.DEBUG", "memory", "json1", "."});
  EXPECT_EQ(resp.type, RespExpr::INT64);
  EXPECT_GT(resp.GetInt(), 0);

  resp = Run({"JSON.DEBUG", "memory", "json1"});
  EXPECT_EQ(resp.type, RespExpr::INT64);
  EXPECT_GT(resp.GetInt(), 0);

  resp = Run({"JSON.SET", "primitives", "$", R"({"num":42, "bool":true, "null":null})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.DEBUG", "memory", "primitives", ".num"});
  EXPECT_EQ(resp.GetInt(), 0);
  resp = Run({"JSON.DEBUG", "memory", "primitives", ".bool"});
  EXPECT_EQ(resp.GetInt(), 0);
  resp = Run({"JSON.DEBUG", "memory", "primitives", ".null"});
  EXPECT_EQ(resp.GetInt(), 0);

  resp = Run({"JSON.SET", "obj_doc", "$",
              R"({"longstring":"This is a very long string that definitely exceeds SSO buffer"})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.DEBUG", "MEMORY", "obj_doc", ".longstring"});
  EXPECT_GT(resp.GetInt(), 0);

  resp = Run({"JSON.SET", "arr", "$", R"([1,2,3,4,5,6,7,8,9,10])"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.DEBUG", "memory", "arr", "."});
  EXPECT_GT(resp.GetInt(), 0);

  resp = Run({"JSON.SET", "obj", "$", R"({"a":1, "b":2, "c":3})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.DEBUG", "memory", "obj", "."});
  EXPECT_GT(resp.GetInt(), 0);
}

TEST_F(JsonFamilyTest, Resp) {
  auto resp = Run({"JSON.SET", "json", ".", PhonebookJson});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.RESP", "json", "$"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);

  resp = Run({"JSON.RESP", "json", "$.address.*"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("New York", "NY", "21 2nd Street", "10021-3100"));

  resp = Run({"JSON.RESP", "json", "$.isAlive"});
  EXPECT_THAT(resp, RespElementsAre("true"));

  resp = Run({"JSON.RESP", "json", "$.age"});
  EXPECT_THAT(resp, RespElementsAre(IntArg(27)));

  resp = Run({"JSON.RESP", "json", "$.weight"});
  EXPECT_THAT(resp, RespElementsAre("135.25"));
}

TEST_F(JsonFamilyTest, RespLegacy) {
  auto resp = Run({"JSON.SET", "json", ".", PhonebookJson});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.RESP", "json"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);

  resp = Run({"JSON.RESP", "json", ".address.*"});
  EXPECT_THAT(resp, "10021-3100");

  resp = Run({"JSON.RESP", "json", ".isAlive"});
  EXPECT_THAT(resp, "true");

  resp = Run({"JSON.RESP", "json", ".age"});
  EXPECT_THAT(resp, IntArg(27));

  resp = Run({"JSON.RESP", "json", ".weight"});
  EXPECT_THAT(resp, "135.25");
}

TEST_F(JsonFamilyTest, Set) {
  string json = R"(
    {"a":{"a":1, "b":2, "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json1", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json1", "$.a.*", "0"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json1"});
  EXPECT_EQ(resp, R"({"a":{"a":0,"b":0,"c":0}})");

  json = R"(
    {"a": [1,2,3,4,5]}
  )";

  resp = Run({"JSON.SET", "json2", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json2", "$.a[*]", "0"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json2"});
  EXPECT_EQ(resp, R"({"a":[0,0,0,0,0]})");

  json = R"(
    {"a": 2}
  )";

  resp = Run({"JSON.SET", "json3", "$", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", "$.b", "8"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", "$.c", "[1,2,3]"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", "$.z", "3", "XX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.SET", "json3", "$.b", "4", "NX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.GET", "json3"});
  EXPECT_EQ(resp, R"({"a":2,"b":8,"c":[1,2,3]})");
}

TEST_F(JsonFamilyTest, SetLegacy) {
  string json = R"(
    {"a":{"a":1, "b":2, "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json1", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json1", ".a.*", "0"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json1"});
  EXPECT_EQ(resp, R"({"a":{"a":0,"b":0,"c":0}})");

  json = R"(
    {"a": [1,2,3,4,5]}
  )";

  resp = Run({"JSON.SET", "json2", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json2", ".a[*]", "0"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json2"});
  EXPECT_EQ(resp, R"({"a":[0,0,0,0,0]})");

  json = R"(
    {"a": 2}
  )";

  resp = Run({"JSON.SET", "json3", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", ".b", "8"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", ".c", "[1,2,3]"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", ".z", "3", "XX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.SET", "json3", ".z", "3"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", ".z", "4", "XX"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", ".b", "4", "NX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.SET", "json3", ".b", "5"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json3", ".", "[]", "NX"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.GET", "json3"});
  EXPECT_EQ(resp, R"({"a":2,"b":5,"c":[1,2,3],"z":4})");

  json = R"(
    {"foo": "bar"}
  )";

  resp = Run({"JSON.SET", "json4", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json4", "foo", "\"baz\"", "XX"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json4", "foo2", "\"qaz\"", "NX"});
  EXPECT_THAT(resp, "OK");
}

TEST_F(JsonFamilyTest, MSet) {
  string json1 = R"({"a":{"a":1,"b":2,"c":3}})";
  string json2 = R"({"a":{"a":4,"b":5,"c":6}})";

  auto resp = Run({"JSON.MSET", "j1", "$"});
  EXPECT_THAT(resp, ErrArg("wrong number"));
  resp = Run({"JSON.MSET", "j1", "$", json1, "j3", "$"});
  EXPECT_THAT(resp, ErrArg("wrong number"));

  resp = Run({"JSON.MSET", "j1", "$", json1, "j2", "$", json2, "j3", "$", json1, "j4", "$", json2});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.MGET", "j1", "j2", "j3", "j4", "$"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("[" + json1 + "]", "[" + json2 + "]", "[" + json1 + "]",
                                         "[" + json2 + "]"));
}

TEST_F(JsonFamilyTest, MSetLegacy) {
  string json1 = R"({"a":{"a":1,"b":2,"c":3}})";
  string json2 = R"({"a":{"a":4,"b":5,"c":6}})";

  auto resp = Run({"JSON.MSET", "j1", "."});
  EXPECT_THAT(resp, ErrArg("wrong number"));
  resp = Run({"JSON.MSET", "j1", ".", json1, "j3", "."});
  EXPECT_THAT(resp, ErrArg("wrong number"));

  resp = Run({"JSON.MSET", "j1", ".", json1, "j2", ".", json2, "j3", ".", json1, "j4", ".", json2});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.MGET", "j1", "j2", "j3", "j4", "$"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("[" + json1 + "]", "[" + json2 + "]", "[" + json1 + "]",
                                         "[" + json2 + "]"));
}

TEST_F(JsonFamilyTest, MergeCompatibility) {
  CheckCompatibility({
      {R"(JSON.SET j1 $ {"a":"b","c":{"d":"e","f":"g"}})", Eq("OK")},
      {R"(JSON.MERGE new $ {"a":"z","c":{"f":null}})",
       R"(JSON.MERGE new . {"a":"z","c":{"f":null}})", Eq("OK"), Eq("OK")},
      {"JSON.GET new", Eq(R"({"a":"z","c":{"f":null}})")},
      {R"(JSON.MERGE j1 $ {"a":"z","c":{"f":null}})", R"(JSON.MERGE j1 . {"a":"z","c":{"f":null}})",
       Eq("OK"), Eq("OK")},
      {"JSON.GET j1", Eq(R"({"a":"z","c":{"d":"e"}})")},
      {R"(JSON.SET foo $ "{"f1":1,"common":2}")", Eq("OK")},
      {R"(JSON.MERGE foo $ {"f2":2,"common":4})", R"(JSON.MERGE foo . {"f2":2,"common":4})",
       Eq("OK"), Eq("OK")},
      {"JSON.GET foo", Eq(R"({"common":4,"f2":2})")},
      {R"(JSON.SET j2 $ {"ans":{"x":{"y":{"doubled":false,"answers":["foo","bar"]}}}})", Eq("OK")},
      {R"(JSON.MERGE j2 $.ans.x {"z":{"doubled":false,"answers":["xxx","yyy"]},"y":{"doubled":true}})",
       R"(JSON.MERGE j2 .ans.x {"z":{"doubled":false,"answers":["xxx","yyy"]},"y":{"doubled":true}})",
       Eq("OK"), Eq("OK")},
      {"JSON.GET j2",
       Eq(R"({"ans":{"x":{"y":{"answers":["foo","bar"],"doubled":true},"z":{"answers":["xxx","yyy"],"doubled":false}}}})")},
      {R"(JSON.MERGE j3 $ {"z":{"doubled":false,"answers":["xxx","yyy"]},"y":{"doubled":true}})",
       R"(JSON.MERGE j3 . {"z":{"doubled":false,"answers":["xxx","yyy"]},"y":{"doubled":true}})",
       Eq("OK"), Eq("OK")},
      {"JSON.GET j3",
       Eq(R"({"y":{"doubled":true},"z":{"answers":["xxx","yyy"],"doubled":false}})")},
  });
}

TEST_F(JsonFamilyTest, GetString) {
  string json = R"(
  { "a": "b",
    "c": {
      "d": "e",
      "f": "g"
    }
  }
  )";

  auto resp = Run({"SET", "json", json});
  EXPECT_THAT(resp, "OK");
  resp = Run({"JSON.GET", "json", "$.c"});
  EXPECT_EQ(resp, R"([{"d":"e","f":"g"}])");
  Run({"SET", "not_json", "not_json"});
  resp = Run({"JSON.GET", "not_json", "$.c"});
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));
}

TEST_F(JsonFamilyTest, MaxNestingJsonDepth) {
  auto generate_nested_json = [](int depth) -> std::string {
    std::string json = "{";
    for (int i = 0; i < depth - 1; ++i) {
      json += R"("key": {)";
    }
    json += R"("key": "value")";  // Innermost value
    for (int i = 0; i < depth - 1; ++i) {
      json += "}";
    }
    json += "}";
    return json;
  };

  // Generate JSON with maximum allowed depth (256)
  /* std::string valid_json = generate_nested_json(255);

  // Test with valid JSON at depth 256
  auto resp = Run({"JSON.SET", "valid_json",  ".", valid_json});
  EXPECT_THAT(resp, "OK"); */

  // Generate JSON exceeding maximum depth (257)
  std::string invalid_json = generate_nested_json(257);

  // Test with invalid JSON at depth 257
  auto resp = Run({"JSON.SET", "invalid_json", ".", invalid_json});
  EXPECT_THAT(resp, ErrArg("failed to parse JSON"));
}

TEST_F(JsonFamilyTest, SetNestedFields) {
  auto resp = Run({"JSON.SET", "json", "$", "{}"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json", "$['field1']", "1"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"field1":1})");

  resp = Run({"JSON.SET", "json", "$['-field2']", "2"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"-field2":2,"field1":1})");
}

TEST_F(JsonFamilyTest, ArrPopWithFormatParameter) {
  auto resp = Run({"JSON.ARRPOP", "test_resp3", "FORMAT", "EXPAND", "$.a"});
  ASSERT_THAT(resp, ErrArg("value is not an integer or out of range"));
}

TEST_F(JsonFamilyTest, DepthLimitExceeded) {
  string deep_json =
      R"({"jdiqr":{"nro":{"uzuf":{"bq":{"yc":{"zodmw":{"zbbq":{"sf":{"oule":{"j":{"mjsss":{"tap":{"bh":{"f":{"zlwgu":{"s":{"kt":{"fnmo":{"hub":{"xj":{"jo":{"ofara":{"kx":{"uw":{"z":{"mwvk":{"jo":{"qqz":{"b":{"tbp":{"esx":{"g":{"p":{"tpzk":{"i":{"azq":{"ttcd":{"wl":{"zo":{"l":{"nsq":{"tulso":{"uk":{"imfzw":{"vlub":{"k":{"ypml":{"voack":{"sosd":{"f":{"x":{"usv":{"hnw":{"ax":{"e":{"ozi":{"doi":{"k":{"bz":{"vxhp":{"e":{"vnpv":{"rhs":{"j":{"esp":{"f":{"ykyvy":{"xvmhg":{"eks":{"oijy":{"sjk":{"a":{"sejgy":{"msd":{"acyo":{"yxss":{"slbf":{"ssuns":{"c":{"kv":{"i":{"y":{"ubqz":{"uam":{"igaq":{"jl":{"vy":{"zlu":{"gscx":{"mb":{"idca":{"k":{"twx":{"ngjs":{"k":{"xcx":{"sxc":{"ye":{"fty":{"pho":{"lrn":{"wmv":{"h":{"sfuk":{"ilwzy":{"nlofv":{"mpcms":{"bg":{"jykgm":{"x":{"nbe":{"ixbyh":{"tmus":{"nqulr":{"cqxdw":{"wwpi":{"kj":{"udb":{"oct":{"tqkv":{"r":{"zev":{"rsu":{"gs":{"pyzm":{"au":{"__leaf":42}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}})";

  auto resp = Run({"JSON.SET", "test", "$", deep_json});
  ASSERT_THAT(resp, ErrArg("ERR failed to parse JSON"));
}

TEST_F(JsonFamilyTest, JsonCommandsWorkingWithOtherTypesBug) {
  std::string_view wrong_type_err{kWrongTypeErr};
  wrong_type_err.remove_prefix(1);  // Remove the leading - character

  auto resp = Run({"HSET", "k1", "field", "value"});
  EXPECT_THAT(resp, IntArg(1));

  // First bug: JSON.SET should return an error
  resp = Run({"JSON.SET", "k1", "$", R"({"a":"b"})"});
  ASSERT_THAT(resp, ErrArg(wrong_type_err));

  // Second bug: JSON.DEL should not delete the hash
  resp = Run({"HSET", "k2", "field", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.DEL", "k2"});
  ASSERT_THAT(resp, ErrArg(wrong_type_err));

  resp = Run({"HGET", "k2", "field"});
  EXPECT_THAT(resp, "value");
}

TEST_F(JsonFamilyTest, ResetStringKeyWithSetGet) {
  auto resp = Run({"JSON.SET", "key", "$", R"({"a":"b"})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "key"});
  EXPECT_THAT(resp, R"({"a":"b"})");

  // Resetting the key with a string value
  resp = Run({"SET", "key", R"({"a":"b"})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"GET", "key"});
  EXPECT_THAT(resp, R"({"a":"b"})");

  // JSON.GET should still work after resetting the key with a string value
  resp = Run({"JSON.GET", "key"});
  EXPECT_THAT(resp, R"({"a":"b"})");

  // Resetting the key again with JSON.SET
  // This should not cause any issues
  resp = Run({"JSON.SET", "key", "$", R"({"a":"b"})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "key"});
  EXPECT_THAT(resp, R"({"a":"b"})");
}

TEST_F(JsonFamilyTest, DelNonExistingKey) {
  auto resp = Run({"EXISTS", "nonexisting_key"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.DEL", "nonexisting_key", "."});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.DEL", "nonexisting_key", "$"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.DEL", "nonexisting_key"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(JsonFamilyTest, JsonKeysWithDots) {
  auto resp = Run(
      {"JSON.SET", "OFFERS:DBX-AGG1611-IGN", "$",
       R"({"Gallery": {"Images": {"bdz1xjm.jpeg": "some_value", "bdz1xjm": "another_value"}}})"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "OFFERS:DBX-AGG1611-IGN", "$['Gallery']['Images']['bdz1xjm']"});
  EXPECT_THAT(resp, "[\"another_value\"]");

  resp = Run({"JSON.GET", "OFFERS:DBX-AGG1611-IGN", "$['Gallery']['Images']['bdz1xjm.jpeg']"});
  EXPECT_THAT(resp, "[\"some_value\"]");
}

TEST_F(JsonFamilyTest, JsonSetDeleteExpiryOfExistingKey) {
  auto resp = Run("SET key foo EX 1000");
  ASSERT_THAT(resp, "OK");
  resp = Run("JSON.SET key $ {}");
  ASSERT_THAT(resp, "OK");
  resp = Run("TTL key");
  ASSERT_THAT(resp, IntArg(-1));
  resp = Run("EXPIRE key 100");
  ASSERT_THAT(resp, IntArg(1));
  resp = Run("TTL key");
  EXPECT_THAT(resp.GetInt(), 100);
}

TEST_F(JsonFamilyTest, JsonIntPathTest) {
  auto resp = Run(
      R"(JSON.SET test:images $ {"images":[{"id":1,"sizes":{"1":"small.jpg","10":"medium.jpg","14":"large.jpg","8":"thumb.jpg"}}]})");
  ASSERT_THAT(resp, "OK");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes.10)");
  EXPECT_THAT(resp, "[\"medium.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes["10"])");
  EXPECT_THAT(resp, "[\"medium.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes['10'])");
  EXPECT_THAT(resp, "[\"medium.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0]["sizes"]["10"])");
  EXPECT_THAT(resp, "[\"medium.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes.8)");
  EXPECT_THAT(resp, "[\"thumb.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes.14)");
  EXPECT_THAT(resp, "[\"large.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes["8"])");
  EXPECT_THAT(resp, "[\"thumb.jpg\"]");
  resp = Run(R"(JSON.GET test:images $.images[0].sizes["14"])");
  EXPECT_THAT(resp, "[\"large.jpg\"]");
}

TEST_F(JsonFamilyTest, ARRLEN_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  string json = R"({"a":[1], "b":{"a":[1,2,3]}, "c":{"x":"not_a"}})";
  auto resp = Run({"JSON.SET", "doc", ".", json});
  ASSERT_THAT(resp, "OK");

  // In RESP3 mode, this should return [1, 3] (direct integers)
  // NOT [[1], [3]] (integers wrapped in arrays)
  resp = Run({"JSON.ARRLEN", "doc", "$..a"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_EQ(resp.GetVec().size(), 2);

  // The bug: each element is wrapped in array when it shouldn't be
  // Check that elements are NOT arrays themselves
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));  // Should be integer, not array
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));  // Should be integer, not array

  // Verify the actual values
  EXPECT_THAT(resp.GetVec()[0], IntArg(1));
  EXPECT_THAT(resp.GetVec()[1], IntArg(3));
}

TEST_F(JsonFamilyTest, ARRAPPEND_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"a":[1], "b":{"a":[1,2,3]}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRAPPEND", "doc", "$..a", "2"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(2));
  EXPECT_THAT(resp.GetVec()[1], IntArg(4));
}

TEST_F(JsonFamilyTest, ARRINDEX_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"a":["x","y"], "b":{"a":["y","z"]}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "doc", "$..a", R"("y")"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(1));
  EXPECT_THAT(resp.GetVec()[1], IntArg(0));
}

TEST_F(JsonFamilyTest, ARRPOP_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"a":[7], "b":{"a":[8]}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "doc", "$..a"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
}

TEST_F(JsonFamilyTest, ARRTRIM_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"a":[1,2], "b":{"a":[3,4,5]}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "doc", "$..a", "0", "0"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(1));
  EXPECT_THAT(resp.GetVec()[1], IntArg(1));
}

TEST_F(JsonFamilyTest, STRLEN_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"s":"hi", "b":{"s":"abc"}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRLEN", "doc", "$..s"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(2));
  EXPECT_THAT(resp.GetVec()[1], IntArg(3));
}

TEST_F(JsonFamilyTest, OBJLEN_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"o":{"k":1}, "b":{"o":{"k":1,"m":2}}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJLEN", "doc", "$..o"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(1));
  EXPECT_THAT(resp.GetVec()[1], IntArg(2));
}

TEST_F(JsonFamilyTest, OBJKEYS_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"o":{"k":1}, "b":{"o":{"k":1,"m":2}}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "doc", "$..o"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  // Each element should be array of keys, not array wrapped again
  auto& el0 = resp.GetVec()[0];
  auto& el1 = resp.GetVec()[1];
  ASSERT_THAT(el0, ArgType(RespExpr::ARRAY));
  ASSERT_THAT(el1, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(el0.GetVec(), ElementsAre("k"));
  // Order of keys in objects is not guaranteed, so check size only for the second
  EXPECT_EQ(el1.GetVec().size(), 2);
}

TEST_F(JsonFamilyTest, STRAPPEND_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"s":"a", "b":{"s":"zz"}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "doc", "$..s", R"("b")"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(2));
  EXPECT_THAT(resp.GetVec()[1], IntArg(3));
}

TEST_F(JsonFamilyTest, TOGGLE_RESP3NestedArrayBug) {
  Run({"HELLO", "3"});

  auto resp = Run({"JSON.SET", "doc", ".", R"({"b":true, "x":{"b":false}})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TOGGLE", "doc", "$..b"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  ASSERT_EQ(resp.GetVec().size(), 2);
  EXPECT_THAT(resp.GetVec()[0], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[1], Not(ArgType(RespExpr::ARRAY)));
  EXPECT_THAT(resp.GetVec()[0], IntArg(0));
  EXPECT_THAT(resp.GetVec()[1], IntArg(1));
}

TEST_F(JsonFamilyTest, SetOverLargeStringKey) {
  // Create a key with a large string value (must be heap-allocated, >16 bytes).
  string large_value(16000, 'x');
  Run({"SET", "key", large_value});

  // Overwrite the string key with a small JSON using root path.
  // Without the fix, freeing the old string inside SetJson caused a negative
  // memory diff in JsonAutoUpdater::SetJsonSize while bytes_used was 0.
  auto resp = Run({"JSON.SET", "key", "$", "1"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "key"});
  EXPECT_EQ(resp, "1");
}

TEST_F(JsonFamilyTest, SetFullJsonInvalidOnNewKey) {
  // Try to set invalid JSON on a non-existent key
  auto resp = Run("JSON.SET newkey $ {invalid}");
  EXPECT_THAT(resp, ErrArg("failed to parse JSON"));

  // Verify the key was NOT created (proper cleanup)
  resp = Run("EXISTS newkey");
  EXPECT_THAT(resp, IntArg(0));
}

}  // namespace dfly
