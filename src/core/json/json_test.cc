// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <gmock/gmock.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <memory_resource>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {
using namespace jsoncons;
using namespace jsoncons::literals;
using namespace testing;

class JsonTest : public ::testing::Test {
 protected:
  JsonTest() {
  }
};

TEST_F(JsonTest, Basic) {
  std::string data = R"(
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

  pmr::json j = pmr::json::parse(data);
  EXPECT_TRUE(j.contains("reputons"));
  jsonpath::json_replace(j, "$.reputons[*].rating", 1.1);
  EXPECT_EQ(1.1, j["reputons"][0]["rating"].as_double());
}

TEST_F(JsonTest, SetEmpty) {
  pmr::json dest{json_object_arg};  // crashes on UB without the tag.
  dest["bar"] = "foo";
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

  std::string_view input{"\000bla"};
  parser.update(input.data(), input.size());
  parser.parse_some(decoder);
  parser.finish_parse(decoder);
  parser.check_done();
  EXPECT_FALSE(decoder.is_valid());
}

TEST_F(JsonTest, Path) {
  std::error_code ec;
  json j1 = R"({"field" : 1, "field-dash": 2})"_json;

  auto expr = jsonpath::make_expression<json>("$.field", ec);
  EXPECT_FALSE(ec);

  expr.evaluate(j1, [](const std::string& path, const json& val) {
    ASSERT_EQ("$['field']", path);
    ASSERT_EQ(1, val.as<int>());
  });

  expr = jsonpath::make_expression<json>("$.field-dash", ec);
  ASSERT_FALSE(ec);  // parses '-'

  expr.evaluate(j1, [](const std::string& path, const json& val) {
    ASSERT_EQ("$['field-dash']", path);
    ASSERT_EQ(2, val.as<int>());
  });

  int called = 0;
  jsonpath::json_query(j1, "max($.*)", [&](const std::string& path, const json& val) {
    EXPECT_EQ("$", path);
    ASSERT_EQ(2, val.as<int>());
    ++called;
  });
  EXPECT_EQ(1, called);

  auto res = jsonpath::json_query(j1, "max($.*)");
  ASSERT_TRUE(res.is_array() && res.size() == 1);
  EXPECT_EQ(2, res[0].as<int>());

  called = 0;
  json j2 = R"({"field" : [1, 2, 3, 4, 5]})"_json;
  jsonpath::json_query(j2, "$.field[1:2]", [&](const std::string& path, const json& val) {
    EXPECT_EQ("$['field'][1]", path);
    ASSERT_EQ(2, val.as<int>());
    ++called;
  });
  EXPECT_EQ(1, called);

  std::vector<int> vals;
  jsonpath::json_query(j2, "$.field[1:]", [&](const std::string& path, const json& val) {
    vals.push_back(val.as<int>());
  });
  EXPECT_THAT(vals, ElementsAre(2, 3, 4, 5));

  jsonpath::json_query(j2, "$.field[-1]", [&](const std::string& path, const json& val) {
    EXPECT_EQ(5, val.as<int>());
  });

  jsonpath::json_query(j2, "$.field[-6:1]", [&](const std::string& path, const json& val) {
    EXPECT_EQ(1, val.as<int>());
  });
}

TEST_F(JsonTest, Delete) {
  json j1 = R"({"c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}, "e": [1,2]})"_json;

  auto deleter = [](const json::string_view_type& path, json& val) {
    LOG(INFO) << "path: " << path;
    // val.evaluate();
    // if (val.is_object())
    //   val.erase(val.object_range().begin(), val.object_range().end());
  };
  jsonpath::json_replace(j1, "$.d.*", deleter);

  auto expr = jsonpath::make_expression<json>("$.d.*");

  auto callback = [](const std::string& path, const json& val) {
    LOG(INFO) << path << ": " << val << "\n";
  };
  expr.evaluate(j1, callback, jsonpath::result_options::path);
  auto it = j1.find("d");
  ASSERT_TRUE(it != j1.object_range().end());

  it->value().erase("a");
  EXPECT_EQ(R"({"c":{"a":1, "b":2}, "d":{"b":2, "c":3}, "e": [1,2]})"_json, j1);
}

TEST_F(JsonTest, JsonWithPolymorhicAllocator) {
  char buffer[1024] = {};
  std::pmr::monotonic_buffer_resource pool{std::data(buffer), std::size(buffer)};
  std::pmr::polymorphic_allocator<char> alloc(&pool);

  std::string input = R"(
{ "store": {
    "book": [
      { "category": "Roman",
        "author": "Felix Lobrecht",
        "title": "Sonne und Beton",
        "price": 12.99
      },
      { "category": "Roman",
        "author": "Thomas F. Schneider",
        "title": "Im Westen nichts Neues",
        "price": 10.00
      }
    ]
  }
}
)";

  auto j1 = pmr::json::parse(combine_allocators(alloc), input, json_options{});
  EXPECT_EQ("Roman", j1["store"]["book"][0]["category"].as_string());
  EXPECT_EQ("Felix Lobrecht", j1["store"]["book"][0]["author"].as_string());
  EXPECT_EQ(12.99, j1["store"]["book"][0]["price"].as_double());

  EXPECT_EQ("Roman", j1["store"]["book"][1]["category"].as_string());
  EXPECT_EQ("Im Westen nichts Neues", j1["store"]["book"][1]["title"].as_string());
  EXPECT_EQ(10.00, j1["store"]["book"][1]["price"].as_double());
}
}  // namespace dfly
