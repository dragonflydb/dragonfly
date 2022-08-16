// Copyright 2022, Roman Gershman.  All rights reserved.
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

}  // namespace dfly
