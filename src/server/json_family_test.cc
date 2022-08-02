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
  EXPECT_EQ(resp, string_view(R"(["New York","NY","21 2nd Street","10021-3100"])", 46));

  resp = Run({"JSON.GET", "json", "$.firstName", "$.age", "$.lastName"});
  EXPECT_EQ(resp, string_view(R"({"$.age":[27],"$.firstName":["John"],"$.lastName":["Smith"]})", 60));

  resp = Run({"JSON.GET", "json", "$.spouse.*"});
  EXPECT_EQ(resp, string_view("[]", 2));

  resp = Run({"JSON.GET", "json", "$.children.*"});
  EXPECT_EQ(resp, string_view("[]", 2));

  resp = Run({"JSON.GET", "json", "$..phoneNumbers[1].*"});
  EXPECT_EQ(resp, string_view(R"(["646 555-4567","office"])", 25));
}

TEST_F(JsonFamilyTest, Type) {
  string json = R"(
    [1, 2.3, "foo", true, null, {}, []]
  )";

  auto resp = Run({"set", "json", json});
  ASSERT_THAT(resp, "OK");

  resp = Run({"JSON.TYPE", "json", "$[*]"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type);
  EXPECT_THAT(resp.GetVec(), ElementsAre("integer", "number", "string", "boolean", "null", "object", "array"));

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

  resp = Run({"JSON.STRLEN", "json", "$.c.b"});
  EXPECT_THAT(resp, IntArg(2));
}

}  // namespace dfly
