// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {
using namespace std;
using namespace jsoncons;
using namespace jsoncons::literals;

class JsonTest : public ::testing::Test {
 protected:
  JsonTest() {
  }
};

TEST_F(JsonTest, Basic) {
  string data = R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum",
           "assertion": "advanced",
           "rated": "Marilyn C",
           "rating": 0.90,
           "confidence": 0.99
         }
       ]
    }
)";

  json j = json::parse(data);
  EXPECT_TRUE(j.contains("reputons"));
  jsonpath::json_replace(j, "$.reputons[*].rating", 1.1);
  EXPECT_EQ(1.1, j["reputons"][0]["rating"].as_double());
}

TEST_F(JsonTest, Query) {
  json j = R"(
{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}}
)"_json;

  json out = jsonpath::json_query(j, "$..*");
  EXPECT_EQ(R"([{},{"a":1},{"a":1,"b":2},1,1,2])"_json, out);

  json j2 = R"(
    {"firstName":"John","lastName":"Smith","age":27,"weight":135.25,"isAlive":true,"address":{"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"},"phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"}],"children":[],"spouse":null}
  )"_json;

  // json_query always returns arrays.
  // See here: https://github.com/danielaparker/jsoncons/issues/82
  // Therefore we are going to only support the "extended" semantics
  // of json API (as they are called in AWS documentation).
  out = jsonpath::json_query(j2, "$.address");
  EXPECT_EQ(R"([{"street":"21 2nd Street","city":"New York",
      "state":"NY","zipcode":"10021-3100"}])"_json,
            out);
}

TEST_F(JsonTest, Errors) {
  auto cb = [](json_errc err, const ser_context& contexts) { return false; };

  json_decoder<json> decoder;
  basic_json_parser<char> parser(basic_json_decode_options<char>{}, cb);

  string_view input{"\000bla"};
  parser.update(input.data(), input.size());
  parser.parse_some(decoder);
  parser.finish_parse(decoder);
  parser.check_done();
  EXPECT_FALSE(decoder.is_valid());
}

}  // namespace dfly