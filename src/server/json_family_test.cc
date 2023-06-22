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

  resp = Run({"SET", "xml", xml});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "xml", "$..*"});
  EXPECT_THAT(resp, ArgType(RespExpr::ERROR));
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

  auto resp = Run({"JSON.SET", "json", ".", json});
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

  auto resp = Run({"JSON.SET", "json", ".", json});
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

  auto resp = Run({"JSON.SET", "json", ".", json});
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

  json = R"(
    {"e":1.5,"a":1}
  )";

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

  json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

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

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([[],[2],[2,3],[2,3,4]])");

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

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{},{"a":2},{"a":2,"b":3},{"a":2,"b":3,"c":4}])");

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

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{"a":"a"},{"a":"a","b":2},{"a":"a","b":"b"},{"a":2,"b":"b","c":4}])");
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

  json = R"(
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  )";

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

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([[],[2],[2,4],[2,4,6]])");

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

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{},{"a":2},{"a":2,"b":4},{"a":2,"b":4,"c":6}])");

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

  resp = Run({"JSON.GET", "json", "$.*"});
  EXPECT_EQ(resp, R"([{"a":"a"},{"a":"a","b":2},{"a":"a","b":"b"},{"a":2,"b":"b","c":6}])");
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
  EXPECT_THAT(resp, IntArg(8));

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
}

TEST_F(JsonFamilyTest, ObjKeys) {
  string json = R"(
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  )";

  auto resp = Run({"JSON.SET", "json", ".", json});
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

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.OBJKEYS", "json", "$..a"});
  ASSERT_THAT(resp, ArrLen(2));
  const auto& arr1 = resp.GetVec();
  EXPECT_THAT(arr1[0], ArgType(RespExpr::NIL_ARRAY));
  EXPECT_THAT(arr1[1].GetVec(), ElementsAre("b", "c"));

  json = R"(
    {"a":{}, "b":{"c":{"d": {"e": 1337}}}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
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

  auto resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.a", "a", "b"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aab"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.a.*", "a"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.b.*", "a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.c.*", "a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(2), IntArg(3)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bba"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.c.b", "a"});
  EXPECT_THAT(resp, IntArg(4));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"b","c":3}})");

  resp = Run({"JSON.STRAPPEND", "json", "$.d.*", "a"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(),
              ElementsAre(ArgType(RespExpr::NIL), IntArg(2), ArgType(RespExpr::NIL)));

  resp = Run({"JSON.GET", "json"});
  EXPECT_EQ(
      resp,
      R"({"a":{"a":"aaba"},"b":{"a":"aa","b":1},"c":{"a":"aa","b":"bbaa"},"d":{"a":1,"b":"ba","c":3}})");

  json = R"(
    {"a":"foo", "inner": {"a": "bye"}, "inner1": {"a": 7}}
  )";

  resp = Run({"JSON.SET", "json", ".", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.STRAPPEND", "json", "$..a", "bar"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(6), IntArg(6), ArgType(RespExpr::NIL)));

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
}

TEST_F(JsonFamilyTest, Resp) {
  auto resp = Run({"JSON.SET", "json", ".", PhonebookJson});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.RESP", "json"});
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

TEST_F(JsonFamilyTest, Set) {
  string json = R"(
    {"a":{"a":1, "b":2, "c":3}}
  )";

  auto resp = Run({"JSON.SET", "json1", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json1", "$.a.*", "0"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json1", "$"});
  EXPECT_EQ(resp, R"([{"a":{"a":0,"b":0,"c":0}}])");

  json = R"(
    {"a": [1,2,3,4,5]}
  )";

  resp = Run({"JSON.SET", "json2", ".", json});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "json2", "$.a[*]", "0"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.GET", "json2", "$"});
  EXPECT_EQ(resp, R"([{"a":[0,0,0,0,0]}])");

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

  resp = Run({"JSON.GET", "json3", "$"});
  EXPECT_EQ(resp, R"([{"a":2,"b":8,"c":[1,2,3]}])");
}

TEST_F(JsonFamilyTest, LegacyV1) {
  string json = R"({"key":[1,2,3,4]})";

  auto resp = Run({"JSON.SET", "json1", ".", json});
  EXPECT_THAT(resp, "OK");

  // JSON.GET key "." is the same as JSON.GET key "$"
  resp = Run({"JSON.GET", "json1", "."});
  EXPECT_THAT(resp, absl::StrCat("[", absl::StripAsciiWhitespace(json), "]"));
}

}  // namespace dfly
