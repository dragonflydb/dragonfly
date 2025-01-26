// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

#include <absl/strings/str_replace.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class JsonFamilyTest : public BaseFamilyTest {
 protected:
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

TEST_F(JsonFamilyTest, StrLen) {
  string json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  /* Test simple response from only one value */

  resp = Run({"JSON.STRLEN", "json", "$.a.a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", "$.a"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.STRLEN", "json", "$.a.*"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", "$.c.b"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.STRLEN", "non_existent_key", "$.c.b"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  /*
  Test response from several possible values
  In JSON V2, the response is an array of all possible values
  */

  resp = Run({"JSON.STRLEN", "json", "$.c.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(2)));

  resp = Run({"JSON.STRLEN", "json", "$.d.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), IntArg(1), ArgType(RespExpr::NIL)));
}

TEST_F(JsonFamilyTest, StrLenLegacy) {
  string json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  /* Test simple response from only one value */

  resp = Run({"JSON.STRLEN", "json"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.STRLEN", "json", ".a.a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", ".a"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.STRLEN", "json", ".a.*"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", ".c.b"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.STRLEN", "non_existent_key", ".c.b"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  /*
  Test response from several possible values
  In JSON legacy mode, the response contains only one value - the first string's length.
  */

  resp = Run({"JSON.STRLEN", "json", ".c.*"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", ".d.*"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(JsonFamilyTest, ObjLen) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  /* Test simple response from only one value */

  resp = Run({"JSON.OBJLEN", "json", "$.a"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.OBJLEN", "json", "$.a.*"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());

  resp = Run({"JSON.OBJLEN", "json", "$.b"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.OBJLEN", "json", "$.b.*"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.OBJLEN", "json", "$.c"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.OBJLEN", "json", "$.d"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.OBJLEN", "non_existent_key", "$.a"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  /*
  Test response from several possible values
  In JSON V2, the response is an array of all possible values
  */

  resp = Run({"JSON.OBJLEN", "json", "$.c.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.OBJLEN", "json", "$.d.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), IntArg(2)));

  resp = Run({"JSON.OBJLEN", "json", "$.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(3), ArgType(RespExpr::NIL)));
}

TEST_F(JsonFamilyTest, ObjLenLegacy) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  /* Test simple response from only one value */
  resp = Run({"JSON.STRLEN", "json"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.OBJLEN", "json", ".a"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.OBJLEN", "json", ".a.*"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.OBJLEN", "json", ".b"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.OBJLEN", "json", ".b.*"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.OBJLEN", "json", ".c"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.OBJLEN", "json", ".d"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.OBJLEN", "non_existent_key", ".a"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.OBJLEN", "json", ".none"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  /*
  Test response from several possible values
  In JSON legacy mode, the response contains only one value - the first object's length.
  */

  resp = Run({"JSON.OBJLEN", "json", ".c.*"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.OBJLEN", "json", ".d.*"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.OBJLEN", "json", ".*"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(JsonFamilyTest, ArrLen) {
  string json = R"(
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRLEN", "json", "$[*]"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(3)));

  json = R"(
    [[], "a", ["a", "b"], ["a", "b", "c"], 4]
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRLEN", "json", "$[*]"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(0), ArgType(RespExpr::NIL), IntArg(2), IntArg(3),
                                         ArgType(RespExpr::NIL)));

  resp = Run({"JSON.OBJLEN", "non_existent_key", "$[*]"});
  EXPECT_THAT(resp, ErrArg("no such key"));
}

TEST_F(JsonFamilyTest, ArrLenLegacy) {
  string json = R"(
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRLEN", "json"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.ARRLEN", "json", "[*]"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.ARRLEN", "json", "[3]"});
  EXPECT_THAT(resp, IntArg(3));

  json = R"(
    [[], "a", ["a", "b"], ["a", "b", "c"], 4]
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRLEN", "json", "[*]"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.ARRLEN", "json", "[1]"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.ARRLEN", "json", "[2]"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.OBJLEN", "non_existent_key", "[*]"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
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

TEST_F(JsonFamilyTest, NumIncrBy) {
  string json = R"(
    {"e":1.5,"a":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a", "1.1"});
  EXPECT_EQ(resp, "[2.1]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.e", "1"});
  EXPECT_EQ(resp, "[2.5]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.e", "inf"});
  EXPECT_THAT(resp, ErrArg("ERR result is not a number"));

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.e", "1.7e308"});
  EXPECT_EQ(resp, "[1.7e+308]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.e", "1.7e308"});
  EXPECT_THAT(resp, ErrArg("ERR result is not a number"));

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([1,1.7e+308])");

  json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d[*]", "10"});
  EXPECT_EQ(resp, "[11,12,13]");

  resp = Run({"JSON.GET", "json", "$.d[*]"});
  EXPECT_EQ(resp, "[11,12,13]");

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a[*]", "1"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.b[*]", "1"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.c[*]", "1"});
  EXPECT_EQ(resp, "[2,3]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d[*]", "1"});
  EXPECT_EQ(resp, "[2,3,4]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d[2]", "1"});
  EXPECT_EQ(resp, "[5]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[],"b":[2],"c":[2,3],"d":[2,3,5]})");

  json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a.*", "1"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.b.*", "1"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.c.*", "1"});
  EXPECT_EQ(resp, "[2,3]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d.*", "1"});
  EXPECT_EQ(resp, "[2,3,4]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":2},"c":{"a":2,"b":3},"d":{"a":2,"b":3,"c":4}})");

  json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a.*", "1"});
  EXPECT_EQ(resp, "[null]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.b.*", "1"});
  EXPECT_EQ(resp, "[null,2]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.c.*", "1"});
  EXPECT_EQ(resp, "[null,null]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d.*", "1"});
  EXPECT_EQ(resp, "[2,null,4]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":4}})");
}

TEST_F(JsonFamilyTest, NumIncrByLegacy) {
  string json = R"(
    {"e":1.5,"a":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".a", "1.1"});
  EXPECT_EQ(resp, "2.1");

  resp = Run({"JSON.NUMINCRBY", "json", ".e", "1"});
  EXPECT_EQ(resp, "2.5");

  resp = Run({"JSON.NUMINCRBY", "json", ".e", "inf"});
  EXPECT_THAT(resp, ErrArg("ERR result is not a number"));

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".e", "1.7e308"});
  EXPECT_EQ(resp, "1.7e+308");

  resp = Run({"JSON.NUMINCRBY", "json", ".e", "1.7e308"});
  EXPECT_THAT(resp, ErrArg("ERR result is not a number"));

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([1,1.7e+308])");

  json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".d[*]", "10"});
  EXPECT_EQ(resp, "13");

  resp = Run({"JSON.GET", "json", "$.d[*]"});
  EXPECT_EQ(resp, "[11,12,13]");

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".a[*]", "1"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMINCRBY", "json", ".b[*]", "1"});
  EXPECT_EQ(resp, "2");

  resp = Run({"JSON.NUMINCRBY", "json", ".c[*]", "1"});
  EXPECT_EQ(resp, "3");

  resp = Run({"JSON.NUMINCRBY", "json", ".d[*]", "1"});
  EXPECT_EQ(resp, "4");

  resp = Run({"JSON.NUMINCRBY", "json", ".d[2]", "1"});
  EXPECT_EQ(resp, "5");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[],"b":[2],"c":[2,3],"d":[2,3,5]})");

  json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".a.*", "1"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMINCRBY", "json", ".b.*", "1"});
  EXPECT_EQ(resp, "2");

  resp = Run({"JSON.NUMINCRBY", "json", ".c.*", "1"});
  EXPECT_EQ(resp, "3");

  resp = Run({"JSON.NUMINCRBY", "json", ".d.*", "1"});
  EXPECT_EQ(resp, "4");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":2},"c":{"a":2,"b":3},"d":{"a":2,"b":3,"c":4}})");

  json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".a.*", "1"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMINCRBY", "json", ".b.*", "1"});
  EXPECT_EQ(resp, "2");

  resp = Run({"JSON.NUMINCRBY", "json", ".c.*", "1"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMINCRBY", "json", ".d.*", "1"});
  EXPECT_EQ(resp, "4");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":4}})");
}

TEST_F(JsonFamilyTest, NumMultBy) {
  string json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d[*]", "2"});
  EXPECT_EQ(resp, "[2,4,6]");

  resp = Run({"JSON.GET", "json", "$.d[*]"});
  EXPECT_EQ(resp, R"([2,4,6])");

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a[*]", "2"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.b[*]", "2"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.c[*]", "2"});
  EXPECT_EQ(resp, "[2,4]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d[*]", "2"});
  EXPECT_EQ(resp, "[2,4,6]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[],"b":[2],"c":[2,4],"d":[2,4,6]})");

  json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a.*", "2"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.b.*", "2"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.c.*", "2"});
  EXPECT_EQ(resp, "[2,4]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d.*", "2"});
  EXPECT_EQ(resp, "[2,4,6]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":2},"c":{"a":2,"b":4},"d":{"a":2,"b":4,"c":6}})");

  json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a.*", "2"});
  EXPECT_EQ(resp, "[null]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.b.*", "2"});
  EXPECT_EQ(resp, "[null,2]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.c.*", "2"});
  EXPECT_EQ(resp, "[null,null]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d.*", "2"});
  EXPECT_EQ(resp, "[2,null,6]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":6}})");
}

TEST_F(JsonFamilyTest, NumMultByLegacy) {
  string json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", ".d[*]", "2"});
  EXPECT_EQ(resp, "6");

  resp = Run({"JSON.GET", "json", "$.d[*]"});
  EXPECT_EQ(resp, R"([2,4,6])");

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", ".a[*]", "2"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMMULTBY", "json", ".b[*]", "2"});
  EXPECT_EQ(resp, "2");

  resp = Run({"JSON.NUMMULTBY", "json", ".c[*]", "2"});
  EXPECT_EQ(resp, "4");

  resp = Run({"JSON.NUMMULTBY", "json", ".d[*]", "2"});
  EXPECT_EQ(resp, "6");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[],"b":[2],"c":[2,4],"d":[2,4,6]})");

  json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", ".a.*", "2"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMMULTBY", "json", ".b.*", "2"});
  EXPECT_EQ(resp, "2");

  resp = Run({"JSON.NUMMULTBY", "json", ".c.*", "2"});
  EXPECT_EQ(resp, "4");

  resp = Run({"JSON.NUMMULTBY", "json", ".d.*", "2"});
  EXPECT_EQ(resp, "6");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":2},"c":{"a":2,"b":4},"d":{"a":2,"b":4,"c":6}})");

  json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", ".a.*", "2"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMMULTBY", "json", ".b.*", "2"});
  EXPECT_EQ(resp, "2");

  resp = Run({"JSON.NUMMULTBY", "json", ".c.*", "2"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));

  resp = Run({"JSON.NUMMULTBY", "json", ".d.*", "2"});
  EXPECT_EQ(resp, "6");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":6}})");
}

TEST_F(JsonFamilyTest, NumericOperationsWithConversions) {
  auto resp = Run({"JSON.SET", "json", ".", R"({"a":2.0})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a", "1"});
  EXPECT_EQ(resp, "[3.0]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a", "1.0"});
  EXPECT_EQ(resp, "[4.0]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a", "2"});
  EXPECT_EQ(resp, "[8.0]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a", "2.0"});
  EXPECT_EQ(resp, "[16.0]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":16.0})");

  resp = Run({"JSON.SET", "json", ".", R"({"a":2})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a", "1"});
  EXPECT_EQ(resp, "[3]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":3})");  // Is still integer

  resp = Run({"JSON.NUMINCRBY", "json", "$.a", "1.0"});
  EXPECT_EQ(resp, "[4.0]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":4.0})");  // Is converted to double

  resp = Run({"JSON.SET", "json", ".", R"({"a":2})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a", "2"});
  EXPECT_EQ(resp, "[4]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":4})");  // Is still integer

  resp = Run({"JSON.NUMMULTBY", "json", "$.a", "2.0"});
  EXPECT_EQ(resp, "[8.0]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":8.0})");  // Is converted to double
}

TEST_F(JsonFamilyTest, NumericOperationsWithConversionsLegacy) {
  auto resp = Run({"JSON.SET", "json", ".", R"({"a":2.0})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".a", "1"});
  EXPECT_EQ(resp, "3.0");

  resp = Run({"JSON.NUMINCRBY", "json", ".a", "1.0"});
  EXPECT_EQ(resp, "4.0");

  resp = Run({"JSON.NUMMULTBY", "json", ".a", "2"});
  EXPECT_EQ(resp, "8.0");

  resp = Run({"JSON.NUMMULTBY", "json", ".a", "2.0"});
  EXPECT_EQ(resp, "16.0");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":16.0})");

  resp = Run({"JSON.SET", "json", ".", R"({"a":2})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", ".a", "1"});
  EXPECT_EQ(resp, "3");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":3})");  // Is still integer

  resp = Run({"JSON.NUMINCRBY", "json", ".a", "1.0"});
  EXPECT_EQ(resp, "4.0");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":4.0})");  // Is converted to double

  resp = Run({"JSON.SET", "json", ".", R"({"a":2})"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", ".a", "2"});
  EXPECT_EQ(resp, "4");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":4})");  // Is still integer

  resp = Run({"JSON.NUMMULTBY", "json", ".a", "2.0"});
  EXPECT_EQ(resp, "8.0");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":8.0})");  // Is converted to double
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
  EXPECT_THAT(resp.GetVec(), ElementsAre("a", "b", "c", "d", "e"));

  resp = Run({"JSON.OBJKEYS", "json", "$.a"});
  EXPECT_THAT(resp.GetVec(), IsEmpty());

  resp = Run({"JSON.OBJKEYS", "json", "$.b"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a"));

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
  EXPECT_THAT(resp, "a");

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

TEST_F(JsonFamilyTest, StrAppend) {
  string json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  /* Test simple response from only one value */

  resp = Run({"JSON.STRAPPEND", "json", "$.a.a", "\"ab\""});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aab"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  const char kVal[] = "\"a\"";

  resp = Run({"JSON.STRAPPEND", "json", "$.a.*", kVal});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.c.b", kVal});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bba"},"d":{"a":1,"b":"b","c":3}})");

  /*
  Test response from several possible values
  In JSON V2, the response is an array of all possible values
  */

  resp = Run({"JSON.STRAPPEND", "json", "$.b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"a","b":"bba"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.c.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(4)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.d.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"ba","c":3}})");

  json = R"(
    {"a":{"a":"a", "b":"aa", "c":"aaa"}, "b":{"a":"aaa", "b":"aa", "c":"a"}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3), IntArg(4)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{"a":"aa","b":"aaa","c":"aaaa"},"b":{"a":"aaa","b":"aa","c":"a"}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(4), IntArg(3), IntArg(2)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{"a":"aa","b":"aaa","c":"aaaa"},"b":{"a":"aaaa","b":"aaa","c":"aa"}})");

  json = R"(
    {"a":{"a":"a", "b":"aa", "c":["aaaaa", "aaaaa"]}, "b":{"a":"aaa", "b":["aaaaa", "aaaaa"], "c":"a"}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":["aaaaa","aaaaa"]},"b":{"a":"aaa","b":["aaaaa","aaaaa"],"c":"a"}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(4), ArgType(RespExpr::NIL), IntArg(2)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":["aaaaa","aaaaa"]},"b":{"a":"aaaa","b":["aaaaa","aaaaa"],"c":"aa"}})");

  json = R"(
    {"a":{"a":"a", "b":"aa", "c":{"c": "aaaaa"}}, "b":{"a":"aaa", "b":{"b": "aaaaa"}, "c":"a"}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":{"c":"aaaaa"}},"b":{"a":"aaa","b":{"b":"aaaaa"},"c":"a"}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(4), ArgType(RespExpr::NIL), IntArg(2)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":{"c":"aaaaa"}},"b":{"a":"aaaa","b":{"b":"aaaaa"},"c":"aa"}})");

  json = R"(
    {"a":"foo", "inner": {"a": "bye"}, "inner1": {"a": 7}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$..a", "\"bar\""});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(6), IntArg(6), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":"foobar","inner":{"a":"byebar"},"inner1":{"a":7}})");
}

TEST_F(JsonFamilyTest, StrAppendLegacyMode) {
  string json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  /* Test simple response from only one value */

  resp = Run({"JSON.STRAPPEND", "json", ".a.a", "\"ab\""});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(
      resp,
      R"({"a":{"a":"aab"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  const char kVal[] = "\"a\"";

  resp = Run({"JSON.STRAPPEND", "json", ".a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", ".c.b", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bba"},"d":{"a":1,"b":"b","c":3}})");

  /*
  Test response from several possible values
  In JSON legacy mode, the response contains only one value - the new length of the last updated
  string.
  */

  resp = Run({"JSON.STRAPPEND", "json", ".b.*", kVal});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"a","b":"bba"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", ".c.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", ".d.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_THAT(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"ba","c":3}})");

  json = R"(
    {"a":{"a":"a", "b":"aa", "c":"aaa"}, "b":{"a":"aaa", "b":"aa", "c":"a"}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", ".a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{"a":"aa","b":"aaa","c":"aaaa"},"b":{"a":"aaa","b":"aa","c":"a"}})");

  resp = Run({"JSON.STRAPPEND", "json", ".b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":{"a":"aa","b":"aaa","c":"aaaa"},"b":{"a":"aaaa","b":"aaa","c":"aa"}})");

  json = R"(
    {"a":{"a":"a", "b":"aa", "c":["aaaaa", "aaaaa"]}, "b":{"a":"aaa", "b":["aaaaa", "aaaaa"], "c":"a"}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", ".a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":["aaaaa","aaaaa"]},"b":{"a":"aaa","b":["aaaaa","aaaaa"],"c":"a"}})");

  resp = Run({"JSON.STRAPPEND", "json", ".b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":["aaaaa","aaaaa"]},"b":{"a":"aaaa","b":["aaaaa","aaaaa"],"c":"aa"}})");

  json = R"(
    {"a":{"a":"a", "b":"aa", "c":{"c": "aaaaa"}}, "b":{"a":"aaa", "b":{"b": "aaaaa"}, "c":"a"}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", ".a.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":{"c":"aaaaa"}},"b":{"a":"aaa","b":{"b":"aaaaa"},"c":"a"}})");

  resp = Run({"JSON.STRAPPEND", "json", ".b.*", kVal});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aa","b":"aaa","c":{"c":"aaaaa"}},"b":{"a":"aaaa","b":{"b":"aaaaa"},"c":"aa"}})");

  json = R"(
    {"a":"foo", "inner": {"a": "bye"}, "inner1": {"a": 7}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "..a", "\"bar\""});
  ASSERT_THAT(resp, ArgType(RespExpr::INT64));
  EXPECT_THAT(resp, IntArg(6));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":"foobar","inner":{"a":"byebar"},"inner1":{"a":7}})");
}

TEST_F(JsonFamilyTest, Clear) {
  string json = R"(
    [[], [0], [0,1], [0,1,2], 1, true, null, "d"]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json", "$[*]"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[],[],[],[],0,true,null,"d"])");

  resp = Run({"JSON.CLEAR", "json", "$"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([])");

  json = R"(
    {"children": ["Yossi", "Rafi", "Benni", "Avraham", "Yehoshua", "Moshe"]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json", "$.children"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"children":[]})");

  resp = Run({"JSON.CLEAR", "json", "$"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");
}

TEST_F(JsonFamilyTest, ClearLegacy) {
  string json = R"(
    [[], [0], [0,1], [0,1,2], 1, true, null, "d"]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json", "[*]"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[],[],[],[],0,true,null,"d"])");

  resp = Run({"JSON.CLEAR", "json", "."});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([])");

  json = R"(
    {"children": ["Yossi", "Rafi", "Benni", "Avraham", "Yehoshua", "Moshe"]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json", ".children"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"children":[]})");

  resp = Run({"JSON.CLEAR", "json", "."});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({})");
}

TEST_F(JsonFamilyTest, ArrPop) {
  string json = R"(
    [[6,1,6], [7,2,7], [8,3,8]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "$[*]", "-2"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("1", "2", "3"));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[6,6],[7,7],[8,8]])");

  json = R"(
    [[], ["a"], ["a", "b"]]
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "$[*]"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::NIL), R"("a")", R"("b")"));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[],[],["a"]])");
}

TEST_F(JsonFamilyTest, ArrPopLegacy) {
  string json = R"(
    [[6,1,6], [7,2,7], [8,3,8]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "[*]", "-2"});
  EXPECT_EQ(resp, R"(3)");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[6,6],[7,7],[8,8]])");

  json = R"(
    [[], ["a"], ["a", "b"]]
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "."});
  EXPECT_EQ(resp, R"(["a","b"])");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[],["a"]])");

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", ".", "0"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([["a"],["a","b"]])");

  resp = Run({"JSON.ARRPOP", "json"});
  EXPECT_EQ(resp, R"(["a","b"])");

  json = R"(
    {"a":"b"}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "."});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.SET", "json", ".", "[]"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "."});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(JsonFamilyTest, ArrPopOutOfRange) {
  string json = R"(
    [0,1,2,3,4,5]
  )";

  auto resp = Run({"JSON.SET", "arr", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "arr", "$", "-55"});
  EXPECT_EQ(resp, "0");

  resp = Run({"JSON.SET", "arr", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "arr", "$", "55"});
  EXPECT_EQ(resp, "5");

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

TEST_F(JsonFamilyTest, ArrTrim) {
  string json = R"(
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "$[*]", "0", "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(2)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[],["a"],["a","b"],["a","b"]])");

  json = R"(
    {"a":[], "nested": {"a": [1,4]}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "$..a", "0", "1"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(0), IntArg(2)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[],"nested":{"a":[1,4]}})");

  json = R"(
    {"a":[1,2,3,2], "nested": {"a": false}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "$..a", "1", "2"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[2,3],"nested":{"a":false}})");

  json = R"(
    [1,2,3,4,5,6,7]
  )";

  resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "$", "2", "3"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([3,4])");
}

TEST_F(JsonFamilyTest, ArrTrimLegacy) {
  string json = R"(
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "[*]", "0", "1"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([[],["a"],["a","b"],["a","b"]])");

  json = R"(
    {"a":[], "nested": {"a": [1,4]}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "..a", "0", "1"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[],"nested":{"a":[1,4]}})");

  json = R"(
    {"a":[1,2,3,2], "nested": {"a": false}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", "..a", "1", "2"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[2,3],"nested":{"a":false}})");

  json = R"(
    [1,2,3,4,5,6,7]
  )";

  resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", ".", "2", "3"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([3,4])");

  json = R"(
    {"a":"b"}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "json", ".", "0", "0"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));
}

TEST_F(JsonFamilyTest, ArrTrimOutOfRange) {
  string arr = R"(
    [0,1,2,3,4]
  )";

  auto resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-1", "3"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "54", "55"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "56", "55"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-55", "-55"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[0]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-2", "-1"});
  EXPECT_THAT(resp, IntArg(2));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[3,4]");

  resp = Run({"JSON.SET", "arr", "$", arr});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRTRIM", "arr", "$", "-1", "-2"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(Run({"JSON.GET", "arr"}), "[]");
}

TEST_F(JsonFamilyTest, ArrInsert) {
  string json = R"(
    [[], ["a"], ["a", "b"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINSERT", "json", "$[*]", "0", R"("a")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(2), IntArg(3)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([["a"],["a","a"],["a","a","b"]])");

  resp = Run({"JSON.ARRINSERT", "json", "$[*]", "-1", R"("b")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3), IntArg(4)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([["b","a"],["a","b","a"],["a","a","b","b"]])");

  resp = Run({"JSON.ARRINSERT", "json", "$[*]", "1", R"("c")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(3), IntArg(4), IntArg(5)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([["b","c","a"],["a","c","b","a"],["a","c","a","b","b"]])");

  json = R"(
    {"a":{"b":"c"}, "b":[["a"], ["a", "b"]]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINSERT", "json", "$.a", "0", R"("c")"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(JsonFamilyTest, ArrInsertLegacy) {
  string json = R"(
    [[], ["a"], ["a", "b"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINSERT", "json", "[*]", "0", R"("c")"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.ARRINSERT", "json", ".", "0", R"("c")"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"(["c",["c"],["c","a"],["c","a","b"]])");

  json = R"(
    {"a":{"b":"c"}, "b":[["a"], ["a", "b"]]}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINSERT", "json", ".a", "0", R"("c")"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));
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
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.GET", "arr"});
  EXPECT_EQ(resp, "[2]");
}

TEST_F(JsonFamilyTest, ArrAppend) {
  string json = R"(
    [[], ["a"], ["a", "b"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRAPPEND", "json", "$[*]", R"("a")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(2), IntArg(3)));

  resp = Run({"JSON.ARRAPPEND", "json", "$[*]", R"("b")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3), IntArg(4)));

  json = R"(
    {"a": [1], "nested": {"a": [1,2], "nested2": {"a": 42}}}
  )";
  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRAPPEND", "json", "$..a", "3"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"({"a":[1,3],"nested":{"a":[1,2,3],"nested2":{"a":42}}})");
}

TEST_F(JsonFamilyTest, ArrAppendLegacy) {
  string json = R"(
    [[], ["a"], ["a", "b"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRAPPEND", "json", "[-1]", R"("c")"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.ARRAPPEND", "json", ".*", R"("c")"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(resp, R"([["c"],["a","c"],["a","b","c","c"]])");

  json = R"(
    {"a":"b"}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRAPPEND", "json", ".", R"("c")"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));
}

TEST_F(JsonFamilyTest, ArrIndex) {
  string json = R"(
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$[*]", R"("b")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(-1), IntArg(-1), IntArg(1), IntArg(1)));

  json = R"(
    {"a":["a","b","c","d"], "nested": {"a": ["c","d"]}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$..a", R"("b")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(-1)));

  json = R"(
    {"a":["a","b","c","d"], "nested": {"a": false}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$..a", R"("b")"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), ArgType(RespExpr::NIL)));

  resp = Run(
      {"JSON.SET", "json", ".", R"({"key" : ["Alice", "Bob", "Carol", "David", "Eve", "Frank"]})"});
  ASSERT_EQ(resp, "OK");
  resp = Run({"JSON.ARRINDEX", "json", "$.key", R"("Bob")"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.ARRINDEX", "json", "$.key", R"("Bob")", "1", "2"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(JsonFamilyTest, ArrIndexLegacy) {
  string json = R"(
    {"children": ["John", "Jack", "Tom", "Bob", "Mike"]}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", ".children", R"("Tom")"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.ARRINDEX", "json", ".children", R"("DoesNotExist")"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "json", ".children.[0].notexist", "3"});
  EXPECT_THAT(resp.type, RespExpr::ERROR);

  json = R"(
    {"a":"b"}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", ".", R"("Tom")"});
  EXPECT_THAT(resp, ErrArg("wrong JSON type of path value"));
}

TEST_F(JsonFamilyTest, ArrIndexWithNumericValues) {
  string json = R"(
    [2, 3.0, 3]
  )";

  auto resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$", "3"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.ARRINDEX", "json", "$", "3.0"});
  EXPECT_THAT(resp, IntArg(1));

  json = R"(
    [[1, 2, 3], [1.0, 2.0, 3.0], 2.0, [1,2,3]]
  )";

  resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$", "[1,2,3]"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.ARRINDEX", "json", "$", "[1.0,2.0,3.0]"});
  EXPECT_THAT(resp, IntArg(1));

  json = R"(
    [{"a":2},{"a":2.0},2.0]
  )";

  resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$", R"({"a":2})"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.ARRINDEX", "json", "$", R"({"a":2.0})"});
  EXPECT_THAT(resp, IntArg(1));

  json = R"(
    [{"arr":[1,2,3],"number":2},{"arr":[1.0,2.0,3.0],"number":2.0},2]
  )";

  resp = Run({"JSON.SET", "json", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", "$", R"({"arr":[1,2,3],"number":2})"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.ARRINDEX", "json", "$", R"({"arr":[1.0,2.0,3.0],"number":2.0})"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.ARRINDEX", "json", "$", R"({"arr":[1,2,3],"number":2.0})"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "json", "$", R"({"arr":[1.0,2.0,3.0],"number":2})"});
  EXPECT_THAT(resp, IntArg(-1));
}

TEST_F(JsonFamilyTest, ArrIndexWithNumericValuesLegacy) {
  string json = R"(
    [2, 3.0, 3]
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", ".", "3"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.ARRINDEX", "json", ".", "3.0"});
  EXPECT_THAT(resp, IntArg(1));

  json = R"(
    [{"arr":[1,2,3],"number":2},{"arr":[1.0,2.0,3.0],"number":2.0},2]
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "json", ".", R"({"arr":[1,2,3],"number":2})"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.ARRINDEX", "json", ".", R"({"arr":[1.0,2.0,3.0],"number":2.0})"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.ARRINDEX", "json", ".", R"({"arr":[1,2,3],"number":2.0})"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "json", ".", R"({"arr":[1.0,2.0,3.0],"number":2})"});
  EXPECT_THAT(resp, IntArg(-1));
}

TEST_F(JsonFamilyTest, ArrIndexOutOfRange) {
  auto resp = Run({"JSON.SET", "arr", ".", R"([1,1,1,1,1])"});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-55", "-55"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-55", "-56"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-55", "-54"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-2"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-2", "-1"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "-2", "-3"});
  EXPECT_THAT(resp, IntArg(-1));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "55", "56"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "55", "54"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.ARRINDEX", "arr", "$", "1", "5", "4"});
  EXPECT_THAT(resp, IntArg(-1));
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

#ifndef SANITIZERS
  resp = Run({"JSON.MGET", "json1", "??INNNNVALID??"});
  EXPECT_THAT(resp, ErrArg("ERR syntax error"));
#endif

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

  resp = Run({"JSON.SET", "json3", ".", json[2]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json4", ".", json[3]});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.MGET", "json3", "json4", "..a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(R"(3)", R"(6)"));
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
  EXPECT_THAT(resp, IntArg(14));

  json = R"(
    [[1,2,3, [4,5,6,[6,7,8]]], {"a": {"b": {"c": 1337}}}]
  )";

  resp = Run({"JSON.SET", "json1", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "fields", "json1", "$[*]"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(11), IntArg(3)));

  resp = Run({"JSON.DEBUG", "fields", "json1", "$"});
  EXPECT_THAT(resp, IntArg(16));

  json = R"({"a":1, "b":2, "c":{"k1":1,"k2":2}})";

  resp = Run({"JSON.SET", "obj_doc", "$", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEBUG", "FIELDS", "obj_doc", "$.a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.DEBUG", "fields", "obj_doc", "$.a"});
  EXPECT_THAT(resp, IntArg(1));
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

TEST_F(JsonFamilyTest, Resp) {
  auto resp = Run({"JSON.SET", "json", ".", PhonebookJson});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.RESP", "json", "$"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);

  resp = Run({"JSON.RESP", "json", "$.address.*"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("New York", "NY", "21 2nd Street", "10021-3100"));

  resp = Run({"JSON.RESP", "json", "$.isAlive"});
  EXPECT_THAT(resp, "true");

  resp = Run({"JSON.RESP", "json", "$.age"});
  EXPECT_THAT(resp, IntArg(27));

  resp = Run({"JSON.RESP", "json", "$.weight"});
  EXPECT_THAT(resp, "135.25");
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

TEST_F(JsonFamilyTest, Merge) {
  string json = R"(
  { "a": "b",
    "c": {
      "d": "e",
      "f": "g"
    }
  }
  )";

  auto resp = Run({"JSON.SET", "j1", "$", json});
  EXPECT_EQ(resp, "OK");

  string patch = R"(
    {
      "a":"z",
      "c": {
      "f": null
      }
    }
  )";

  resp = Run({"JSON.MERGE", "new", "$", patch});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.GET", "new"});
  EXPECT_EQ(resp, R"({"a":"z","c":{"f":null}})");

  resp = Run({"JSON.MERGE", "j1", "$", patch});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j1"});
  EXPECT_EQ(resp, R"({"a":"z","c":{"d":"e"}})");

  resp = Run({"JSON.SET", "foo", "$", R"("{"f1":1, "common":2}")"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.MERGE", "foo", "$", R"({"f2":2, "common":4})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "foo"});
  EXPECT_EQ(resp, R"({"common":4,"f2":2})");

  json = R"({
  "ans": {
    "x": {
      "y" : {
        "doubled": false,
        "answers": [
          "foo",
          "bar"
        ]
      }
    }
  }
  })";
  resp = Run({"JSON.SET", "j2", "$", json});
  ASSERT_EQ(resp, "OK");

  patch = R"(
    {"z": {
      "doubled": false,
      "answers": ["xxx",  "yyy"]
     },
     "y": { "doubled": true}
     })";

  resp = Run({"JSON.MERGE", "j2", "$.ans.x", patch});

  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j2"});
  EXPECT_EQ(resp, R"({"ans":{"x":{"y":{"answers":["foo","bar"],"doubled":true},)"
                  R"("z":{"answers":["xxx","yyy"],"doubled":false}}}})");

  // Test not existing entry
  resp = Run({"JSON.MERGE", "j3", "$", patch});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j3"});
  EXPECT_EQ(resp, R"({"y":{"doubled":true},"z":{"answers":["xxx","yyy"],"doubled":false}})");
}

TEST_F(JsonFamilyTest, MergeLegacy) {
  string json = R"(
  { "a": "b",
    "c": {
      "d": "e",
      "f": "g"
    }
  }
  )";

  auto resp = Run({"JSON.SET", "j1", "$", json});
  EXPECT_EQ(resp, "OK");

  string patch = R"(
    {
      "a":"z",
      "c": {
      "f": null
      }
    }
  )";

  resp = Run({"JSON.MERGE", "new", ".", patch});
  EXPECT_EQ(resp, "OK");

  resp = Run({"JSON.GET", "new"});
  EXPECT_EQ(resp, R"({"a":"z","c":{"f":null}})");

  resp = Run({"JSON.MERGE", "j1", ".", patch});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j1"});
  EXPECT_EQ(resp, R"({"a":"z","c":{"d":"e"}})");

  resp = Run({"JSON.SET", "foo", "$", R"("{"f1":1, "common":2}")"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.MERGE", "foo", ".", R"({"f2":2, "common":4})"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "foo"});
  EXPECT_EQ(resp, R"({"common":4,"f2":2})");

  json = R"({
  "ans": {
    "x": {
      "y" : {
        "doubled": false,
        "answers": [
          "foo",
          "bar"
        ]
      }
    }
  }
  })";
  resp = Run({"JSON.SET", "j2", "$", json});
  ASSERT_EQ(resp, "OK");

  patch = R"(
    {"z": {
      "doubled": false,
      "answers": ["xxx",  "yyy"]
     },
     "y": { "doubled": true}
     })";

  resp = Run({"JSON.MERGE", "j2", ".ans.x", patch});

  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j2"});
  EXPECT_EQ(resp, R"({"ans":{"x":{"y":{"answers":["foo","bar"],"doubled":true},)"
                  R"("z":{"answers":["xxx","yyy"],"doubled":false}}}})");

  // Test not existing entry
  resp = Run({"JSON.MERGE", "j3", ".", patch});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j3"});
  EXPECT_EQ(resp, R"({"y":{"doubled":true},"z":{"answers":["xxx","yyy"],"doubled":false}})");
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

}  // namespace dfly
