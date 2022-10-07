// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

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

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json", "$..*"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"JSON.GET", "json", "$..book[0].price"});
  EXPECT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"JSON.GET", "json", "//*"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));

  resp = Run({"JSON.GET", "json", "//book[0]"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));

  resp = Run({"set", "xml", xml});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "xml", "$..*"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));
}

TEST_F(JsonFamilyTest, SetGetFromPhonebook) {
  string json = R"(
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

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

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
}

TEST_F(JsonFamilyTest, Type) {
  string json = R"(
    [1, 2.3, "foo", true, null, {}, []]
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TYPE", "json", "$[*]"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("integer", "number", "string", "boolean", "null", "object", "array"));

  resp = Run({"JSON.TYPE", "json", "$[10]"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.TYPE", "not_exist_key", "$[10]"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(JsonFamilyTest, StrLen) {
  string json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRLEN", "json", "$.a.a"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", "$.a.*"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.STRLEN", "json", "$.c.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(2)));

  resp = Run({"JSON.STRLEN", "json", "$.c.b"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.STRLEN", "json", "$.d.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), IntArg(1), ArgType(RespExpr::NIL)));
}

TEST_F(JsonFamilyTest, ObjLen) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJLEN", "json", "$.a"});
  EXPECT_THAT(resp, IntArg(0));

  resp = Run({"JSON.OBJLEN", "json", "$.a.*"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  resp = Run({"JSON.OBJLEN", "json", "$.b"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"JSON.OBJLEN", "json", "$.b.*"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  resp = Run({"JSON.OBJLEN", "json", "$.c"});
  EXPECT_THAT(resp, IntArg(2));

  resp = Run({"JSON.OBJLEN", "json", "$.c.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.OBJLEN", "json", "$.d"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.OBJLEN", "json", "$.d.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), ArgType(RespExpr::NIL), IntArg(2)));

  resp = Run({"JSON.OBJLEN", "json", "$.*"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(3), ArgType(RespExpr::NIL)));
}

TEST_F(JsonFamilyTest, ArrLen) {
  string json = R"(
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRLEN", "json", "$[*]"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(0), IntArg(1), IntArg(2), IntArg(3)));

  json = R"(
    [[], "a", ["a", "b"], ["a", "b", "c"], 4]
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRLEN", "json", "$[*]"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(0), ArgType(RespExpr::NIL), IntArg(2), IntArg(3),
                                         ArgType(RespExpr::NIL)));
}

TEST_F(JsonFamilyTest, Toggle) {
  string json = R"(
    {"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}
  )";

  auto resp = Run({"set", "json", json});
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

TEST_F(JsonFamilyTest, NumIncrBy) {
  string json = R"(
    {"e":1.5,"a":1}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a", "1.1"});
  EXPECT_EQ(resp, "[2.1]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.e", "1"});
  EXPECT_EQ(resp, "[2.5]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.e", "inf"});
  EXPECT_THAT(resp, ErrArg("ERR result is not a number"));

  json = R"(
    {"e":1.5,"a":1}
  )";

  resp = Run({"set", "json", json});
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

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d[*]", "10"});
  EXPECT_EQ(resp, "[11,12,13]");

  resp = Run({"JSON.GET", "json", "$.d[*]"});
  EXPECT_EQ(resp, "[11,12,13]");

  json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a[*]", "1"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.b[*]", "1"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.c[*]", "1"});
  EXPECT_EQ(resp, "[2,3]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d[*]", "1"});
  EXPECT_EQ(resp, "[2,3,4]");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([[],[2],[2,3],[2,3,4]])");

  json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a.*", "1"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.b.*", "1"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.c.*", "1"});
  EXPECT_EQ(resp, "[2,3]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d.*", "1"});
  EXPECT_EQ(resp, "[2,3,4]");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{},{"a":2},{"a":2,"b":3},{"a":2,"b":3,"c":4}])");

  json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMINCRBY", "json", "$.a.*", "1"});
  EXPECT_EQ(resp, "[null]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.b.*", "1"});
  EXPECT_EQ(resp, "[null,2]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.c.*", "1"});
  EXPECT_EQ(resp, "[null,null]");

  resp = Run({"JSON.NUMINCRBY", "json", "$.d.*", "1"});
  EXPECT_EQ(resp, "[2,null,4]");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{"a":"a"},{"a":"a","b":2},{"a":"a","b":"b"},{"a":2,"b":"b","c":4}])");
}

TEST_F(JsonFamilyTest, NumMultBy) {
  string json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d[*]", "2"});
  EXPECT_EQ(resp, "[2,4,6]");

  resp = Run({"JSON.GET", "json", "$.d[*]"});
  EXPECT_EQ(resp, R"([2,4,6])");

  json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a[*]", "2"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.b[*]", "2"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.c[*]", "2"});
  EXPECT_EQ(resp, "[2,4]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d[*]", "2"});
  EXPECT_EQ(resp, "[2,4,6]");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([[],[2],[2,4],[2,4,6]])");

  json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a.*", "2"});
  EXPECT_EQ(resp, "[]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.b.*", "2"});
  EXPECT_EQ(resp, "[2]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.c.*", "2"});
  EXPECT_EQ(resp, "[2,4]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d.*", "2"});
  EXPECT_EQ(resp, "[2,4,6]");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{},{"a":2},{"a":2,"b":4},{"a":2,"b":4,"c":6}])");

  json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.NUMMULTBY", "json", "$.a.*", "2"});
  EXPECT_EQ(resp, "[null]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.b.*", "2"});
  EXPECT_EQ(resp, "[null,2]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.c.*", "2"});
  EXPECT_EQ(resp, "[null,null]");

  resp = Run({"JSON.NUMMULTBY", "json", "$.d.*", "2"});
  EXPECT_EQ(resp, "[2,null,6]");

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{"a":"a"},{"a":"a","b":2},{"a":"a","b":"b"},{"a":2,"b":"b","c":6}])");
}

TEST_F(JsonFamilyTest, Del) {
  string json = R"(
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}, "e": [1,2,3,4,5]}}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", "$.d.*"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[1,2,3,4,5]})");

  resp = Run({"JSON.DEL", "json", "$.e[*]"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[]})");

  resp = Run({"JSON.DEL", "json", "$..*"});
  EXPECT_THAT(resp, IntArg(8));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({})");

  resp = Run({"JSON.DEL", "json"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  json = R"(
    {"a":[{"b": [1,2,3]}], "b": [{"c": 2}], "c']":[1,2,3]}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "json", "$.a[0].b[0]"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({"a":[{"b":[2,3]}],"b":[{"c":2}],"c']":[1,2,3]})");

  resp = Run({"JSON.DEL", "json", "$.b[0].c"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({"a":[{"b":[2,3]}],"b":[{}],"c']":[1,2,3]})");

  resp = Run({"JSON.DEL", "json", "$.*"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({})");
}

TEST_F(JsonFamilyTest, ObjKeys) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$.a"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  resp = Run({"JSON.OBJKEYS", "json", "$.b"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("a"));

  resp = Run({"JSON.OBJKEYS", "json", "$.*"});
  ASSERT_THAT(resp, ArrLen(5));
  const auto& arr = resp.GetVec();
  EXPECT_THAT(arr[0], ArgType(RespExpr::NIL_ARRAY));
  EXPECT_THAT(arr[1].GetVec(), ElementsAre("a"));
  EXPECT_THAT(arr[2].GetVec(), ElementsAre("a", "b"));
  EXPECT_THAT(arr[3].GetVec(), ElementsAre("a", "b", "c"));
  EXPECT_THAT(arr[4], ArgType(RespExpr::NIL_ARRAY));

  resp = Run({"JSON.OBJKEYS", "json", "$.notfound"});
  EXPECT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp, ArrLen(0));

  json = R"(
    {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$..a"});
  ASSERT_THAT(resp, ArrLen(2));
  const auto& arr1 = resp.GetVec();
  EXPECT_THAT(arr1[0], ArgType(RespExpr::NIL_ARRAY));
  EXPECT_THAT(arr1[1].GetVec(), ElementsAre("b", "c"));

  json = R"(
    {"a":{}, "b":{"c":{"d": {"e": 1337}}}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$..*"});
  ASSERT_THAT(resp, ArrLen(5));
  const auto& arr2 = resp.GetVec();
  EXPECT_THAT(arr2[0], ArgType(RespExpr::NIL_ARRAY));
  EXPECT_THAT(arr2[1].GetVec(), ElementsAre("c"));
  EXPECT_THAT(arr2[2].GetVec(), ElementsAre("d"));
  EXPECT_THAT(arr2[3].GetVec(), ElementsAre("e"));
  EXPECT_THAT(arr2[4], ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(JsonFamilyTest, StrAppend) {
  string json = R"(
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.a", "a", "b"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aab"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.*", "a"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.b.*", "a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.c.*", "a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3)));

  resp = Run({"GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bba"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.c.b", "a"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.d.*", "a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"ba","c":3}})");

  json = R"(
    {"a":"foo", "inner": {"a": "bye"}, "inner1": {"a": 7}}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$..a", "bar"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(6), IntArg(6), ArgType(RespExpr::NIL)));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({"a":"foobar","inner":{"a":"byebar"},"inner1":{"a":7}})");
}

TEST_F(JsonFamilyTest, Clear) {
  string json = R"(
    [[], [0], [0,1], [0,1,2], 1, true, null, "d"]
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json", "$[*]"});
  EXPECT_THAT(resp, IntArg(5));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"([[],[],[],[],0,true,null,"d"])");

  resp = Run({"JSON.CLEAR", "json", "$"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"([])");

  json = R"(
    {"children": ["Yossi", "Rafi", "Benni", "Avraham", "Yehoshua", "Moshe"]}
  )";

  resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.CLEAR", "json", "$.children"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({"children":[]})");

  resp = Run({"JSON.CLEAR", "json", "$"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"({})");
}

TEST_F(JsonFamilyTest, ArrPop) {
  string json = R"(
    [[6,1,6], [7,2,7], [8,3,8]]
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.ARRPOP", "json", "$[*]", "-2"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("1", "2", "3"));

  resp = Run({"GET", "json"});
  EXPECT_EQ(resp, R"([[6,6],[7,7],[8,8]])");
}

}  // namespace dfly
