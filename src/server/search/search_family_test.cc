// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/flags/flag.h>

#include <algorithm>

#include "absl/strings/str_format.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/error.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;

ABSL_DECLARE_FLAG(bool, search_reject_legacy_field);

namespace dfly {

class SearchFamilyTest : public BaseFamilyTest {
 protected:
};

const auto kNoResults = IntArg(0);  // tests auto destruct single element arrays

/* Asserts that response is array of two arrays. Used to test FT.PROFILE response */
::testing::AssertionResult AssertArrayOfTwoArrays(const RespExpr& resp) {
  if (resp.GetVec().size() != 2) {
    return ::testing::AssertionFailure()
           << "Expected response array length to be 2, but was " << resp.GetVec().size();
  }

  const auto& vec = resp.GetVec();
  if (vec[0].type != RespExpr::ARRAY) {
    return ::testing::AssertionFailure()
           << "Expected resp[0] to be an array, but was " << vec[0].type;
  }
  if (vec[1].type != RespExpr::ARRAY) {
    return ::testing::AssertionFailure()
           << "Expected resp[1] to be an array, but was " << vec[1].type;
  }
  return ::testing::AssertionSuccess();
}

#define ASSERT_ARRAY_OF_TWO_ARRAYS(resp) ASSERT_PRED1(AssertArrayOfTwoArrays, resp)

MATCHER_P2(DocIds, total, arg_ids, "") {
  if (arg_ids.empty()) {
    if (auto res = arg.GetInt(); !res || *res != 0) {
      *result_listener << "Expected single zero";
      return false;
    }
    return true;
  }

  if (arg.type != RespExpr::ARRAY) {
    *result_listener << "Wrong response type: " << int(arg.type);
    return false;
  }

  auto results = arg.GetVec();
  if (results.size() != arg_ids.size() * 2 + 1) {
    *result_listener << "Wrong resp vec size: " << results.size();
    return false;
  }

  if (auto num_results = results[0].GetInt(); !num_results || size_t(*num_results) != total) {
    *result_listener << "Bad total count in reply: " << num_results.value_or(-1);
    return false;
  }

  vector<string> received_ids;
  for (size_t i = 1; i < results.size(); i += 2)
    received_ids.push_back(results[i].GetString());

  vector<string> expected_ids = arg_ids;
  sort(received_ids.begin(), received_ids.end());
  sort(expected_ids.begin(), expected_ids.end());

  return expected_ids == received_ids;
}

template <typename... Args> auto AreDocIds(Args... args) {
  return DocIds(sizeof...(args), vector<string>{args...});
}

template <typename... Args> auto IsArray(Args... args) {
  return RespArray(ElementsAre(std::forward<Args>(args)...));
}

template <typename... Args> auto IsUnordArray(Args... args) {
  return RespArray(UnorderedElementsAre(std::forward<Args>(args)...));
}
template <typename Expected, size_t... Is>
void BuildKvMatchers(std::vector<Matcher<std::pair<std::string, RespExpr>>>& kv_matchers,
                     const Expected& expected, std::index_sequence<Is...>) {
  (kv_matchers.emplace_back(Pair(std::get<Is * 2>(expected), std::get<Is * 2 + 1>(expected))), ...);
}

MATCHER_P(IsMapMatcher, expected, "") {
  if (arg.type != RespExpr::ARRAY) {
    *result_listener << "Wrong response type: " << arg.type;
    return false;
  }

  constexpr size_t expected_size = std::tuple_size<decltype(expected)>::value;
  constexpr size_t exprected_pairs_number = expected_size / 2;

  auto result = arg.GetVec();
  if (result.size() != expected_size) {
    *result_listener << "Wrong resp array size: " << result.size();
    return false;
  }

  std::vector<std::pair<std::string, RespExpr>> received_pairs;
  for (size_t i = 0; i < result.size(); i += 2) {
    received_pairs.emplace_back(result[i].GetString(), result[i + 1]);
  }

  std::vector<Matcher<std::pair<std::string, RespExpr>>> kv_matchers;
  BuildKvMatchers(kv_matchers, expected, std::make_index_sequence<exprected_pairs_number>{});

  return ExplainMatchResult(UnorderedElementsAreArray(kv_matchers), received_pairs,
                            result_listener);
}

template <typename... Args> auto IsMap(Args... args) {
  return IsMapMatcher(std::make_tuple(args...));
}

MATCHER_P(IsMapWithSizeMatcher, expected, "") {
  if (arg.type != RespExpr::ARRAY) {
    *result_listener << "Wrong response type: " << arg.type;
    return false;
  }
  constexpr size_t expected_size = std::tuple_size<decltype(expected)>::value;
  constexpr size_t exprected_pairs_number = expected_size / 2;

  auto result = arg.GetVec();
  if (result.size() != expected_size + 1 || result.size() % 2 != 1) {
    *result_listener << "Wrong resp array size: " << result.size();
    return false;
  }

  if (result[0].GetInt() != exprected_pairs_number) {
    *result_listener << "Wrong pairs count: " << result[0].GetInt().value_or(-1);
    return false;
  }

  std::vector<std::pair<std::string, RespExpr>> received_pairs;
  for (size_t i = 1; i < result.size(); i += 2) {
    received_pairs.emplace_back(result[i].GetString(), result[i + 1]);
  }

  std::vector<Matcher<std::pair<std::string, RespExpr>>> kv_matchers;
  BuildKvMatchers(kv_matchers, expected, std::make_index_sequence<exprected_pairs_number>{});

  return ExplainMatchResult(UnorderedElementsAreArray(kv_matchers), received_pairs,
                            result_listener);
}

template <typename... Args> auto IsMapWithSize(Args... args) {
  return IsMapWithSizeMatcher(std::make_tuple(args...));
}

MATCHER_P(IsUnordArrayWithSizeMatcher, expected, "") {
  if (arg.type != RespExpr::ARRAY) {
    *result_listener << "Wrong response type: " << arg.type;
    return false;
  }

  auto result = arg.GetVec();
  size_t expected_size = std::tuple_size<decltype(expected)>::value;
  if (result.size() != expected_size + 1) {
    *result_listener << "Wrong resp array size: " << result.size();
    return false;
  }

  if (result[0].GetInt() != expected_size) {
    *result_listener << "Wrong elements count: " << result[0].GetInt().value_or(-1);
    return false;
  }

  std::vector<RespExpr> received_elements(result.begin() + 1, result.end());

  // Create a vector of matchers from the tuple
  std::vector<Matcher<RespExpr>> matchers;
  std::apply([&matchers](auto&&... args) { ((matchers.push_back(args)), ...); }, expected);

  return ExplainMatchResult(UnorderedElementsAreArray(matchers), received_elements,
                            result_listener);
}

template <typename... Matchers> auto IsUnordArrayWithSize(Matchers... matchers) {
  return IsUnordArrayWithSizeMatcher(std::make_tuple(matchers...));
}

TEST_F(SearchFamilyTest, CreateDropListIndex) {
  EXPECT_EQ(Run({"ft.create", "idx-1", "ON", "HASH", "PREFIX", "1", "prefix-1"}), "OK");
  EXPECT_EQ(Run({"ft.create", "idx-2", "ON", "JSON", "PREFIX", "1", "prefix-2"}), "OK");
  EXPECT_EQ(Run({"ft.create", "idx-3", "ON", "JSON", "PREFIX", "1", "prefix-3"}), "OK");

  EXPECT_THAT(Run({"ft._list"}).GetVec(), testing::UnorderedElementsAre("idx-1", "idx-2", "idx-3"));

  EXPECT_EQ(Run({"ft.dropindex", "idx-2"}), "OK");
  EXPECT_THAT(Run({"ft._list"}).GetVec(), testing::UnorderedElementsAre("idx-1", "idx-3"));

  EXPECT_THAT(Run({"ft.create", "idx-1"}), ErrArg("Index already exists"));

  EXPECT_THAT(Run({"ft.dropindex", "idx-100"}), ErrArg("Unknown Index name"));

  EXPECT_EQ(Run({"ft.dropindex", "idx-1"}), "OK");
  EXPECT_EQ(Run({"ft._list"}), "idx-3");
}

TEST_F(SearchFamilyTest, CreateDropDifferentDatabases) {
  // Create index on db 0
  auto resp =
      Run({"ft.create", "idx-1", "ON", "HASH", "PREFIX", "1", "doc-", "SCHEMA", "name", "TEXT"});
  EXPECT_EQ(resp, "OK");

  EXPECT_EQ(Run({"select", "1"}), "OK");  // change database

  // Creating an index on non zero database must fail
  resp = Run({"ft.create", "idx-2", "ON", "JSON", "PREFIX", "1", "prefix-2"});
  EXPECT_THAT(resp, ErrArg("ERR Cannot create index on db != 0"));

  // Add some data to the index
  Run({"hset", "doc-0", "name", "Name of 0"});

  // ft.search must work on the another database
  resp = Run({"ft.search", "idx-1", "*"});
  EXPECT_THAT(resp, IsMapWithSize("doc-0", IsMap("name", "Name of 0")));

  // ft.dropindex must work on the another database
  EXPECT_EQ(Run({"ft.dropindex", "idx-1"}), "OK");

  EXPECT_THAT(Run({"ft.info", "idx-1"}), ErrArg("ERR Unknown Index name"));
  EXPECT_EQ(Run({"select", "0"}), "OK");
  EXPECT_THAT(Run({"ft.info", "idx-1"}), ErrArg("ERR Unknown Index name"));
}

TEST_F(SearchFamilyTest, AlterIndex) {
  Run({"hset", "d:1", "color", "blue", "cost", "150"});
  Run({"hset", "d:2", "color", "green", "cost", "200"});

  Run({"ft.create", "idx-1", "ON", "HASH"});

  EXPECT_EQ(Run({"ft.alter", "idx-1", "schema", "add", "color", "tag"}), "OK");
  EXPECT_THAT(Run({"ft.search", "idx-1", "@color:{blue}"}), AreDocIds("d:1"));
  EXPECT_THAT(Run({"ft.search", "idx-1", "@color:{green}"}), AreDocIds("d:2"));

  EXPECT_EQ(Run({"ft.alter", "idx-1", "schema", "add", "cost", "numeric"}), "OK");
  EXPECT_THAT(Run({"ft.search", "idx-1", "@cost:[0 100]"}), kNoResults);
  EXPECT_THAT(Run({"ft.search", "idx-1", "@cost:[100 300]"}), AreDocIds("d:1", "d:2"));

  EXPECT_THAT(Run({"ft.alter", "idx-2", "schema", "add", "price", "numeric"}),
              ErrArg("Index not found"));
}

TEST_F(SearchFamilyTest, InfoIndex) {
  EXPECT_EQ(
      Run({"ft.create", "idx-1", "ON", "HASH", "PREFIX", "1", "doc-", "SCHEMA", "name", "TEXT"}),
      "OK");

  for (size_t i = 0; i < 15; i++) {
    Run({"hset", absl::StrCat("doc-", i), "name", absl::StrCat("Name of", i)});
  }

  auto info = Run({"ft.info", "idx-1"});
  EXPECT_THAT(info,
              IsArray(_, _, _, IsArray("key_type", "HASH", "prefix", "doc-"), "attributes",
                      IsArray(IsArray("identifier", "name", "attribute", "name", "type", "TEXT")),
                      "num_docs", IntArg(15)));
}

TEST_F(SearchFamilyTest, Stats) {
  EXPECT_EQ(
      Run({"ft.create", "idx-1", "ON", "HASH", "PREFIX", "1", "doc1-", "SCHEMA", "name", "TEXT"}),
      "OK");

  EXPECT_EQ(
      Run({"ft.create", "idx-2", "ON", "HASH", "PREFIX", "1", "doc2-", "SCHEMA", "name", "TEXT"}),
      "OK");

  for (size_t i = 0; i < 50; i++) {
    Run({"hset", absl::StrCat("doc1-", i), "name", absl::StrCat("Name of", i)});
    Run({"hset", absl::StrCat("doc2-", i), "name", absl::StrCat("Name of", i)});
  }

  auto metrics = GetMetrics();
  EXPECT_EQ(metrics.search_stats.num_indices, 2);
  EXPECT_EQ(metrics.search_stats.num_entries, 50 * 2);

  size_t expected_usage = 2 * (50 + 3 /* number of distinct words*/) * (24 + 48 /* kv size */) +
                          50 * 2 * 1 /* posting list entries */;
  EXPECT_GE(metrics.search_stats.used_memory, expected_usage);
  EXPECT_LE(metrics.search_stats.used_memory, 3 * expected_usage);
}

// todo: ASAN fails heres on arm
#ifndef SANITIZERS
TEST_F(SearchFamilyTest, Simple) {
  Run({"hset", "d:1", "foo", "baz", "k", "v"});
  Run({"hset", "d:2", "foo", "bar", "k", "v"});
  Run({"hset", "d:3", "foo", "bad", "k", "v"});

  EXPECT_EQ(Run({"ft.create", "i1", "PREFIX", "1", "d:", "SCHEMA", "foo", "TEXT", "k", "TEXT"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "@foo:bar"}), AreDocIds("d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@foo:bar | @foo:baz"}), AreDocIds("d:1", "d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@foo:(bar|baz|bad)"}), AreDocIds("d:1", "d:2", "d:3"));

  EXPECT_THAT(Run({"ft.search", "i1", "@foo:none"}), kNoResults);

  EXPECT_THAT(Run({"ft.search", "iNone", "@foo:bar"}), ErrArg("iNone: no such index"));
  EXPECT_THAT(Run({"ft.search", "i1", "@@NOTAQUERY@@"}), ErrArg("Query syntax error"));

  // w: prefix is not part of index
  Run({"hset", "w:2", "foo", "this", "k", "v"});
  EXPECT_THAT(Run({"ft.search", "i1", "@foo:this"}), kNoResults);
}
#endif

TEST_F(SearchFamilyTest, Errors) {
  Run({"ft.create", "i1", "PREFIX", "1", "d:", "SCHEMA", "foo", "TAG", "bar", "TEXT"});

  // Wrong field
  EXPECT_THAT(Run({"ft.search", "i1", "@whoami:lol"}), ErrArg("Invalid field: whoami"));

  // Wrong field type
  EXPECT_THAT(Run({"ft.search", "i1", "@foo:lol"}), ErrArg("Wrong access type for field: foo"));

  // ft.create index on json schema $.sometag AS sometag TAG SEPARATOR
  EXPECT_THAT(Run({"ft.create", "i2", "ON", "JSON", "SCHEMA", "$.sometag", "AS", "sometag", "TAG",
                   "SEPARATOR"}),
              ErrArg("Tag separator must be a single character. Got ``"));
}

TEST_F(SearchFamilyTest, NoPrefix) {
  Run({"hset", "d:1", "a", "one", "k", "v"});
  Run({"hset", "d:2", "a", "two", "k", "v"});
  Run({"hset", "d:3", "a", "three", "k", "v"});

  EXPECT_EQ(Run({"ft.create", "i1", "schema", "a", "text", "k", "text"}), "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "one | three"}), AreDocIds("d:1", "d:3"));
}

TEST_F(SearchFamilyTest, Json) {
  Run({"json.set", "k1", ".", R"({"a": "small test", "b": "some details"})"});
  Run({"json.set", "k2", ".", R"({"a": "another test", "b": "more details"})"});
  Run({"json.set", "k3", ".", R"({"a": "last test", "b": "secret details"})"});

  EXPECT_EQ(Run({"ft.create", "i1", "on", "json", "schema", "$.a", "as", "a", "text", "$.b", "as",
                 "b", "text"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "some|more"}), AreDocIds("k1", "k2"));
  EXPECT_THAT(Run({"ft.search", "i1", "some|more|secret"}), AreDocIds("k1", "k2", "k3"));

  EXPECT_THAT(Run({"ft.search", "i1", "@a:last @b:details"}), AreDocIds("k3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@a:(another|small)"}), AreDocIds("k1", "k2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@a:(another|small|secret)"}), AreDocIds("k1", "k2"));

  EXPECT_THAT(Run({"ft.search", "i1", "none"}), kNoResults);
  EXPECT_THAT(Run({"ft.search", "i1", "@a:small @b:secret"}), kNoResults);
}

TEST_F(SearchFamilyTest, JsonAttributesPaths) {
  Run({"json.set", "k1", ".", R"(   {"nested": {"value": "no"}} )"});
  Run({"json.set", "k2", ".", R"(   {"nested": {"value": "yes"}} )"});
  Run({"json.set", "k3", ".", R"(   {"nested": {"value": "maybe"}} )"});

  EXPECT_EQ(
      Run({"ft.create", "i1", "on", "json", "schema", "$.nested.value", "as", "value", "text"}),
      "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "yes"}), AreDocIds("k2"));
}

TEST_F(SearchFamilyTest, JsonIdentifierWithBrackets) {
  Run({"json.set", "k1", ".", R"({"name":"London","population":8.8,"continent":"Europe"})"});
  Run({"json.set", "k2", ".", R"({"name":"Athens","population":3.1,"continent":"Europe"})"});
  Run({"json.set", "k3", ".", R"({"name":"Tel-Aviv","population":1.3,"continent":"Asia"})"});
  Run({"json.set", "k4", ".", R"({"name":"Hyderabad","population":9.8,"continent":"Asia"})"});

  EXPECT_EQ(Run({"ft.create", "i1", "on", "json", "schema", "$[\"name\"]", "as", "name", "tag",
                 "$[\"population\"]", "as", "population", "numeric", "sortable", "$[\"continent\"]",
                 "as", "continent", "tag"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "(@continent:{Europe})"}), AreDocIds("k1", "k2"));
}

// todo: fails on arm build
#ifndef SANITIZERS
TEST_F(SearchFamilyTest, JsonArrayValues) {
  string_view D1 = R"(
{
  "name": "Alex",
  "plays" : [
    {"game": "Pacman", "score": 10},
    {"game": "Tetris", "score": 15}
  ],
  "areas": ["EU-west", "EU-central"]
}
)";
  string_view D2 = R"(
{
  "name": "Bob",
  "plays" : [
    {"game": "Pacman", "score": 15},
    {"game": "Mario", "score": 7}
  ],
  "areas": ["US-central"]
}
)";
  string_view D3 = R"(
{
  "name": "Caren",
  "plays" : [
    {"game": "Mario", "score": 9},
    {"game": "Doom", "score": 20}
  ],
  "areas": ["EU-central", "EU-east"]
}
)";

  Run({"json.set", "k1", ".", D1});
  Run({"json.set", "k2", ".", D2});
  Run({"json.set", "k3", ".", D3});

  Run({"ft.create", "i1",
       "on",        "json",
       "schema",    "$.name",
       "as",        "name",
       "text",      "$.plays[*].game",
       "as",        "games",
       "tag",       "$.plays[*].score",
       "as",        "scores",
       "numeric",   "$.areas[*]",
       "as",        "areas",
       "tag"});

  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("k1", "k2", "k3"));

  // Find players by games
  EXPECT_THAT(Run({"ft.search", "i1", "@games:{Tetris | Mario | Doom}"}),
              AreDocIds("k1", "k2", "k3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@games:{Pacman}"}), AreDocIds("k1", "k2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@games:{Mario}"}), AreDocIds("k2", "k3"));

  // Find players by scores
  EXPECT_THAT(Run({"ft.search", "i1", "@scores:[15 15]"}), AreDocIds("k1", "k2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@scores:[0 (10]"}), AreDocIds("k2", "k3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@scores:[(15 20]"}), AreDocIds("k3"));

  // Find platers by areas
  EXPECT_THAT(Run({"ft.search", "i1", "@areas:{'EU-central'}"}), AreDocIds("k1", "k3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@areas:{'US-central'}"}), AreDocIds("k2"));

  // Test complicated RETURN expression
  auto res = Run(
      {"ft.search", "i1", "@name:bob", "return", "1", "max($.plays[*].score)", "as", "max-score"});
  EXPECT_THAT(res, IsMapWithSize("k2", IsMap("max-score", "15")));

  // Test invalid json path expression omits that field
  res = Run({"ft.search", "i1", "@name:alex", "return", "1", "::??INVALID??::", "as", "retval"});
  EXPECT_THAT(res, IsMapWithSize("k1", IsMap()));
}
#endif

TEST_F(SearchFamilyTest, Tags) {
  Run({"hset", "d:1", "color", "red, green"});
  Run({"hset", "d:2", "color", "green, blue"});
  Run({"hset", "d:3", "color", "blue, red"});
  Run({"hset", "d:4", "color", "red"});
  Run({"hset", "d:5", "color", "green"});
  Run({"hset", "d:6", "color", "blue"});

  EXPECT_EQ(Run({"ft.create", "i1", "on", "hash", "schema", "color", "tag", "dummy", "numeric"}),
            "OK");
  EXPECT_THAT(Run({"ft.tagvals", "i2", "color"}), ErrArg("Unknown Index name"));
  EXPECT_THAT(Run({"ft.tagvals", "i1", "foo"}), ErrArg("No such field"));
  EXPECT_THAT(Run({"ft.tagvals", "i1", "dummy"}), ErrArg("Not a tag field"));
  auto resp = Run({"ft.tagvals", "i1", "color"});
  ASSERT_THAT(resp, IsUnordArray("red", "blue", "green"));

  // Tags don't participate in full text search
  EXPECT_THAT(Run({"ft.search", "i1", "red"}), kNoResults);

  EXPECT_THAT(Run({"ft.search", "i1", "@color:{ red }"}), AreDocIds("d:1", "d:3", "d:4"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{green}"}), AreDocIds("d:1", "d:2", "d:5"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), AreDocIds("d:2", "d:3", "d:6"));

  EXPECT_THAT(Run({"ft.search", "i1", "@color:{red | green}"}),
              AreDocIds("d:1", "d:2", "d:3", "d:4", "d:5"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue | green}"}),
              AreDocIds("d:1", "d:2", "d:3", "d:5", "d:6"));

  EXPECT_EQ(Run({"ft.create", "i2", "on", "hash", "schema", "c1", "as", "c2", "tag"}), "OK");

  // TODO: there is a discrepancy here between redis stack and Dragonfly,
  // we accept the original field when it has alias, while redis stack does not.
  //
  // EXPECT_THAT(Run({"ft.tagvals", "i2", "c1"}), ErrArg("No such field"));
  EXPECT_THAT(Run({"ft.tagvals", "i2", "c2"}), ArrLen(0));
}

TEST_F(SearchFamilyTest, TagOptions) {
  Run({"hset", "d:1", "color", "    red/   green // bLUe   "});
  Run({"hset", "d:2", "color", "blue   /// GReeN   "});
  Run({"hset", "d:3", "color", "grEEn // yellow   //"});
  Run({"hset", "d:4", "color", "  /blue/green/  "});

  EXPECT_EQ(Run({"ft.create", "i1", "on", "hash", "schema", "color", "tag", "casesensitive",
                 "separator", "/"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "@color:{green}"}), AreDocIds("d:1", "d:4"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{GReeN}"}), AreDocIds("d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), AreDocIds("d:2", "d:4"));
}

TEST_F(SearchFamilyTest, TagNumbers) {
  Run({"hset", "d:1", "number", "1"});
  Run({"hset", "d:2", "number", "2"});
  Run({"hset", "d:3", "number", "3"});

  EXPECT_EQ(Run({"ft.create", "i1", "on", "hash", "schema", "number", "tag"}), "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "@number:{1}"}), AreDocIds("d:1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@number:{1|2}"}), AreDocIds("d:1", "d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@number:{1|2|3}"}), AreDocIds("d:1", "d:2", "d:3"));

  EXPECT_THAT(Run({"ft.search", "i1", "@number:{1.0|2|3.0}"}), AreDocIds("d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@number:{1|2|3.0}"}), AreDocIds("d:1", "d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@number:{1|hello|2}"}), AreDocIds("d:1", "d:2"));
}

TEST_F(SearchFamilyTest, TagEscapeCharacters) {
  EXPECT_EQ(Run({"ft.create", "item_idx", "ON", "JSON", "PREFIX", "1", "p", "SCHEMA", "$.name",
                 "AS", "name", "TAG"}),
            "OK");
  EXPECT_EQ(Run({"json.set", "p:1", "$", "{\"name\":\"escape-error\"}"}), "OK");

  auto resp = Run({"ft.search", "item_idx", "@name:{escape\\-err*}"});
  EXPECT_THAT(resp, AreDocIds("p:1"));
}

TEST_F(SearchFamilyTest, Numbers) {
  for (unsigned i = 0; i <= 10; i++) {
    for (unsigned j = 0; j <= 10; j++) {
      auto key = absl::StrCat("i", i, "j", j);
      Run({"hset", key, "i", absl::StrCat(i), "j", absl::StrCat(j)});
    }
  }

  EXPECT_EQ(Run({"ft.create", "i1", "schema", "i", "numeric", "j", "numeric"}), "OK");

  // Test simple ranges:
  EXPECT_THAT(Run({"ft.search", "i1", "@i:[5 5] @j:[5 5]"}), AreDocIds("i5j5"));

  EXPECT_THAT(Run({"ft.search", "i1", "@i:[0 1] @j:[9 10]"}),
              AreDocIds("i0j9", "i0j10", "i1j9", "i1j10"));

  EXPECT_THAT(Run({"ft.search", "i1", "@i:[7 8] @j:[2 3]"}),
              AreDocIds("i7j2", "i7j3", "i8j2", "i8j3"));

  // Test union of ranges:
  EXPECT_THAT(Run({"ft.search", "i1", "(@i:[1 2] | @i:[6 6]) @j:[7 7]"}),
              AreDocIds("i1j7", "i2j7", "i6j7"));

  EXPECT_THAT(Run({"ft.search", "i1", "(@i:[1 5] | @i:[1 3] | @i:[3 5]) @j:[7 7]"}),
              AreDocIds("i1j7", "i2j7", "i3j7", "i4j7", "i5j7"));

  // Test intersection of ranges:
  EXPECT_THAT(Run({"ft.search", "i1", "(@i:[9 9]) (@j:[5 7] @j:[6 8])"}),
              AreDocIds("i9j6", "i9j7"));

  EXPECT_THAT(Run({"ft.search", "i1", "@i:[9 9] (@j:[4 6] @j:[1 5] @j:[5 10])"}),
              AreDocIds("i9j5"));

  EXPECT_THAT(Run({"ft.search", "i1", "@i:[9 9] (@j:[4 6] @j:[1 5] @j:[5 10])"}),
              AreDocIds("i9j5"));

  // Test negation of ranges:
  EXPECT_THAT(Run({"ft.search", "i1", "@i:[9 9] -@j:[1 10]"}), AreDocIds("i9j0"));
  EXPECT_THAT(Run({"ft.search", "i1", "-@i:[0 9] -@j:[1 10]"}), AreDocIds("i10j0"));

  // Test empty range
  EXPECT_THAT(Run({"ft.search", "i1", "@i:[9 1]"}), AreDocIds());
  EXPECT_THAT(Run({"ft.search", "i1", "@j:[5 0]"}), AreDocIds());
  EXPECT_THAT(Run({"ft.search", "i1", "@i:[7 1] @j:[6 2]"}), AreDocIds());
}

TEST_F(SearchFamilyTest, TestLimit) {
  for (unsigned i = 0; i < 20; i++)
    Run({"hset", to_string(i), "match", "all"});
  Run({"ft.create", "i1", "SCHEMA", "match", "text"});

  // Default limit is 10
  auto resp = Run({"ft.search", "i1", "all"});
  EXPECT_THAT(resp, ArrLen(10 * 2 + 1));

  resp = Run({"ft.search", "i1", "all", "limit", "0", "0"});
  EXPECT_THAT(resp, IntArg(20));

  resp = Run({"ft.search", "i1", "all", "limit", "0", "5"});
  EXPECT_THAT(resp, ArrLen(5 * 2 + 1));

  resp = Run({"ft.search", "i1", "all", "limit", "17", "5"});
  EXPECT_THAT(resp, ArrLen(3 * 2 + 1));
}

string_view FloatSV(const float* f) {
  return {reinterpret_cast<const char*>(f), sizeof(float)};
}

auto MatchEntry = [](string key, auto... fields) { return IsMapWithSize(key, IsMap(fields...)); };

TEST_F(SearchFamilyTest, ReturnOption) {
  for (unsigned i = 0; i < 20; i++) {
    const float score = i;
    Run({"hset", "k"s + to_string(i), "longA", to_string(i), "longB", to_string(i + 1), "longC",
         to_string(i + 2), "secret", to_string(i + 3), "vector", FloatSV(&score)});
  }

  Run({"ft.create", "i1",     "SCHEMA", "longA",   "AS",    "justA", "TEXT",
       "longB",     "AS",     "justB",  "NUMERIC", "longC", "AS",    "justC",
       "NUMERIC",   "vector", "VECTOR", "FLAT",    "2",     "DIM",   "1"});

  // Check all fields are returned
  auto resp = Run({"ft.search", "i1", "@justA:0"});
  EXPECT_THAT(resp, MatchEntry("k0", "longA", "0", "longB", "1", "longC", "2", "secret", "3",
                               "vector", "[0]"));

  // Check no fields are returned
  resp = Run({"ft.search", "i1", "@justA:0", "return", "0"});
  EXPECT_THAT(resp, IsArray(IntArg(1), "k0"));

  resp = Run({"ft.search", "i1", "@justA:0", "nocontent"});
  EXPECT_THAT(resp, IsArray(IntArg(1), "k0"));

  // Check only one field is returned (and with original identifier)
  resp = Run({"ft.search", "i1", "@justA:0", "return", "1", "longA"});
  EXPECT_THAT(resp, MatchEntry("k0", "longA", "0"));

  // Check only one field is returned with right alias
  resp = Run({"ft.search", "i1", "@justA:0", "return", "1", "longB", "as", "madeupname"});
  EXPECT_THAT(resp, MatchEntry("k0", "madeupname", "1"));

  // Check two fields
  resp = Run({"ft.search", "i1", "@justA:0", "return", "2", "longB", "as", "madeupname", "longC"});
  EXPECT_THAT(resp, MatchEntry("k0", "madeupname", "1", "longC", "2"));

  // Check non-existing field
  resp = Run({"ft.search", "i1", "@justA:0", "return", "1", "nothere"});
  EXPECT_THAT(resp, MatchEntry("k0"));

  // Checl implcit __vector_score is provided
  float score = 20;
  resp = Run({"ft.search", "i1", "@justA:0 => [KNN 20 @vector $vector]", "SORTBY", "__vector_score",
              "DESC", "RETURN", "1", "longA", "PARAMS", "2", "vector", FloatSV(&score)});
  EXPECT_THAT(resp, MatchEntry("k0", "longA", "0"));

  // Check sort doesn't shadow knn return alias
  score = 20;
  resp = Run({"ft.search", "i1", "@justA:0 => [KNN 20 @vector $vector AS vec_return]", "SORTBY",
              "vec_return", "DESC", "RETURN", "1", "vec_return", "PARAMS", "2", "vector",
              FloatSV(&score)});
  EXPECT_THAT(resp, MatchEntry("k0", "vec_return", "20"));
}

TEST_F(SearchFamilyTest, ReturnOptionJson) {
  const string_view j =
      R"({"actions":["fly","sleep"],"name":"dragon","not_indexed":true,"size":3})";
  Run({"json.set", "k1", ".", j});
  Run({"ft.create", "i1", "on", "json", "schema", "$.name", "as", "name", "text", "$.actions[0]",
       "as", "primary_action", "tag", "$.size", "as", "size", "numeric"});

  // Return whole document as a single field by default
  EXPECT_THAT(Run({"ft.search", "i1", "*"}), MatchEntry("k1", "$", j));

  // RETURN 0
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "0"}), IsArray(IntArg(1), "k1"));

  // RETURN by full path
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "$.name"}),
              MatchEntry("k1", "$.name", "dragon"));
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "$.actions"}),
              MatchEntry("k1", "$.actions", "[\"fly\",\"sleep\"]"));

  // RETURN by full path with alias
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "$.name", "as", "n"}),
              MatchEntry("k1", "n", "dragon"));

  // RETURN by schema alias
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "name"}),
              MatchEntry("k1", "name", "dragon"));
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "primary_action"}),
              MatchEntry("k1", "primary_action", "fly"));

  // RETURN by schema alias with new alias
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "name", "as", "n"}),
              MatchEntry("k1", "n", "dragon"));
  EXPECT_THAT(Run({"ft.search", "i1", "*", "return", "1", "primary_action", "as", "pa"}),
              MatchEntry("k1", "pa", "fly"));

  // Whole document with SORTBY includes sortable field as return field
  EXPECT_THAT(Run({"ft.search", "i1", "*", "sortby", "size"}),
              MatchEntry("k1", "$", j, "size", "3"));

  // RETURN with SORTBY doesn't include sortable field
  EXPECT_THAT(Run({"ft.search", "i1", "*", "sortby", "size", "return", "1", "name"}),
              MatchEntry("k1", "name", "dragon"));
}

TEST_F(SearchFamilyTest, TestStopWords) {
  Run({"ft.create", "i1", "STOPWORDS", "3", "red", "green", "blue", "SCHEMA", "title", "TEXT"});

  Run({"hset", "d:1", "title", "ReD? parrot flies away"});
  Run({"hset", "d:2", "title", "GrEEn crocodile eats you"});
  Run({"hset", "d:3", "title", "BLUe. Whale surfes the sea"});

  EXPECT_THAT(Run({"ft.search", "i1", "red"}), kNoResults);
  EXPECT_THAT(Run({"ft.search", "i1", "green"}), kNoResults);
  EXPECT_THAT(Run({"ft.search", "i1", "blue"}), kNoResults);

  EXPECT_THAT(Run({"ft.search", "i1", "parrot"}), AreDocIds("d:1"));
  EXPECT_THAT(Run({"ft.search", "i1", "crocodile"}), AreDocIds("d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "whale"}), AreDocIds("d:3"));
}

TEST_F(SearchFamilyTest, SimpleUpdates) {
  EXPECT_EQ(Run({"ft.create", "i1", "schema", "title", "text", "visits", "numeric"}), "OK");

  Run({"hset", "d:1", "title", "Dragonfly article", "visits", "100"});
  Run({"hset", "d:2", "title", "Butterfly observations", "visits", "50"});
  Run({"hset", "d:3", "title", "Bumblebee studies", "visits", "30"});

  // Check values above were added to the index
  EXPECT_THAT(Run({"ft.search", "i1", "article | observations | studies"}),
              AreDocIds("d:1", "d:2", "d:3"));

  // Update title - text value
  {
    Run({"hset", "d:2", "title", "Butterfly studies"});
    EXPECT_THAT(Run({"ft.search", "i1", "observations"}), kNoResults);
    EXPECT_THAT(Run({"ft.search", "i1", "studies"}), AreDocIds("d:2", "d:3"));

    Run({"hset", "d:1", "title", "Upcoming Dragonfly presentation"});
    EXPECT_THAT(Run({"ft.search", "i1", "article"}), kNoResults);
    EXPECT_THAT(Run({"ft.search", "i1", "upcoming presentation"}), AreDocIds("d:1"));

    Run({"hset", "d:3", "title", "Secret bumblebee research"});
    EXPECT_THAT(Run({"ft.search", "i1", "studies"}), AreDocIds("d:2"));
    EXPECT_THAT(Run({"ft.search", "i1", "secret research"}), AreDocIds("d:3"));
  }

  // Update visits - numeric value
  {
    EXPECT_THAT(Run({"ft.search", "i1", "@visits:[50 1000]"}), AreDocIds("d:1", "d:2"));

    Run({"hset", "d:3", "visits", "75"});
    EXPECT_THAT(Run({"ft.search", "i1", "@visits:[0 49]"}), kNoResults);
    EXPECT_THAT(Run({"ft.search", "i1", "@visits:[50 1000]"}), AreDocIds("d:1", "d:2", "d:3"));

    Run({"hset", "d:1", "visits", "125"});
    Run({"hset", "d:2", "visits", "150"});
    EXPECT_THAT(Run({"ft.search", "i1", "@visits:[100 1000]"}), AreDocIds("d:1", "d:2"));

    Run({"hset", "d:3", "visits", "175"});
    EXPECT_THAT(Run({"ft.search", "i1", "@visits:[0 100]"}), kNoResults);
    EXPECT_THAT(Run({"ft.search", "i1", "@visits:[150 1000]"}), AreDocIds("d:2", "d:3"));
  }

  // Delete documents
  {
    Run({"del", "d:2", "d:3"});
    EXPECT_THAT(Run({"ft.search", "i1", "dragonfly"}), AreDocIds("d:1"));
    EXPECT_THAT(Run({"ft.search", "i1", "butterfly | bumblebee"}), kNoResults);
  }
}

TEST_F(SearchFamilyTest, Unicode) {
  EXPECT_EQ(Run({"ft.create", "i1", "schema", "title", "text", "visits", "numeric"}), "OK");

  // Explicitly using screaming uppercase to check utf-8 to lowercase functionality
  Run({"hset", "d:1", "title", "Веселая СТРЕКОЗА Иван", "visits", "400"});
  Run({"hset", "d:2", "title", "Die fröhliche Libelle Günther", "visits", "300"});
  Run({"hset", "d:3", "title", "השפירית המהירה יעקב", "visits", "200"});
  Run({"hset", "d:4", "title", "πανίσχυρη ΛΙΒΕΛΛΟΎΛΗ Δίας", "visits", "100"});

  // Check we find our dragonfly in all languages
  EXPECT_THAT(Run({"ft.search", "i1", "стРекоЗа|liBellE|השפירית|λΙβελλοΎλη"}),
              AreDocIds("d:1", "d:2", "d:3", "d:4"));

  // Check the result is valid
  auto resp = Run({"ft.search", "i1", "λιβελλούλη"});
  EXPECT_THAT(resp,
              IsMapWithSize("d:4", IsMap("visits", "100", "title", "πανίσχυρη ΛΙΒΕΛΛΟΎΛΗ Δίας")));
}

TEST_F(SearchFamilyTest, UnicodeWords) {
  EXPECT_EQ(Run({"ft.create", "i1", "schema", "title", "text"}), "OK");

  Run({"hset", "d:1", "title",
       "WORD!!! Одно слово? Zwei Wörter. Comma before ,sentence, "
       "Τρεις λέξεις: χελώνα-σκύλου-γάτας. !זה עובד",
       "visits", "400"});

  // Make sure it includes ALL those words
  EXPECT_THAT(Run({"ft.search", "i1", "word слово wörter sentence λέξεις γάτας עובד"}),
              AreDocIds("d:1"));
}

TEST_F(SearchFamilyTest, PrefixSuffixInfixTrie) {
  Run({"ft.create", "i1", "schema", "title", "text", "withsuffixtrie"});

  Run({"hset", "d:1", "title", "CaspIAn SeA"});
  Run({"hset", "d:2", "title", "GreAt LakEs"});
  Run({"hset", "d:3", "title", "Lake VictorIA"});
  Run({"hset", "d:4", "title", "LaKE Como"});

  EXPECT_THAT(Run({"ft.search", "i1", "*ea*"}), AreDocIds("d:1", "d:2"));
  EXPECT_THAT(Run({"ft.search", "i1", "*ia*"}), AreDocIds("d:1", "d:3"));
  EXPECT_THAT(Run({"ft.search", "i1", "lake*"}), AreDocIds("d:2", "d:3", "d:4"));
  EXPECT_THAT(Run({"ft.search", "i1", "*lake"}), AreDocIds("d:3", "d:4"));
}

struct SortTest : SearchFamilyTest, public testing::WithParamInterface<bool /* sortable */> {};

TEST_P(SortTest, BasicSort) {
  auto AreRange = [](size_t total, size_t l, size_t r, string_view prefix) {
    vector<string> out;
    for (size_t i = min(l, r); i < max(l, r); i++)
      out.push_back(absl::StrCat(prefix, i));
    if (l > r)
      reverse(out.begin(), out.end());
    return DocIds(total, out);
  };

  vector<string_view> params{"ft.create", "i1", "prefix", "1", "d:", "schema", "ord", "numeric"};
  if (GetParam())
    params.emplace_back("sortable");
  Run(params);

  size_t num_docs = 100;
  for (size_t i = 0; i < num_docs; i++)
    Run({"hset", absl::StrCat("d:", i), "ord", absl::StrCat(i)});

  // Check SORTBY in ASC and DESC mode with different LIMIT parameters
  for (int take = 17; take < 35; take += 7) {
    for (size_t i = 0; i < num_docs - take; i++)
      EXPECT_THAT(
          Run({"ft.search", "i1", "*", "SORTBY", "ord", "LIMIT", to_string(i), to_string(take)}),
          AreRange(num_docs, i, i + take, "d:"));

    for (size_t i = 0; i < num_docs - take; i++)
      EXPECT_THAT(Run({"ft.search", "i1", "*", "SORTBY", "ord", "DESC", "LIMIT", to_string(i),
                       to_string(take)}),
                  AreRange(num_docs, num_docs - i, num_docs - i - take, "d:"));
  }

  params = {"ft.create", "i2", "prefix", "1", "d2:", "schema", "name", "text"};
  if (GetParam())
    params.emplace_back("sortable");
  Run(params);

  absl::InsecureBitGen gen;
  vector<string> random_strs;
  for (size_t i = 0; i < 10; i++)
    random_strs.emplace_back(dfly::GetRandomHex(gen, 7));
  sort(random_strs.begin(), random_strs.end());

  for (size_t i = 0; i < 10; i++)
    Run({"hset", absl::StrCat("d2:", i), "name", random_strs[i]});

  for (size_t i = 0; i < 7; i++)
    EXPECT_THAT(Run({"ft.search", "i2", "*", "SORTBY", "name", "DESC", "LIMIT", to_string(i), "3"}),
                AreRange(10, 10 - i, 10 - i - 3, "d2:"));
}

INSTANTIATE_TEST_SUITE_P(Sortable, SortTest, testing::Values(true));
INSTANTIATE_TEST_SUITE_P(NotSortable, SortTest, testing::Values(false));

TEST_F(SearchFamilyTest, FtProfile) {
  Run({"ft.create", "i1", "schema", "name", "text"});

  auto resp = Run({"ft.profile", "i1", "search", "query", "(a | b) c d"});
  ASSERT_ARRAY_OF_TWO_ARRAYS(resp);

  const auto& top_level = resp.GetVec();
  EXPECT_THAT(top_level[0], IsMapWithSize());

  const auto& profile_result = top_level[1].GetVec();
  EXPECT_EQ(profile_result.size(), shard_set->size() + 1);

  EXPECT_THAT(profile_result[0].GetVec(), ElementsAre("took", _, "hits", _, "serialized", _));

  for (size_t sid = 0; sid < shard_set->size(); sid++) {
    const auto& shard_resp = profile_result[sid + 1].GetVec();
    EXPECT_THAT(shard_resp, ElementsAre("took", _, "tree", _));

    const auto& tree = shard_resp[3].GetVec();
    EXPECT_EQ(tree[3].GetString() /* operation */, "Logical{n=3,o=and}"s);
    EXPECT_GT(tree[1].GetInt() /* total time*/, tree[5].GetInt() /* self time */);
    EXPECT_EQ(tree[7].GetInt() /* processed */, 0);
  }

  // Test LIMITED throws no errors
  resp = Run({"ft.profile", "i1", "search", "limited", "query", "(a | b) c d"});
  ASSERT_ARRAY_OF_TWO_ARRAYS(resp);
}

#ifndef SANITIZERS
TEST_F(SearchFamilyTest, FtProfileInvalidQuery) {
  Run({"json.set", "j1", ".", R"({"id":"1"})"});
  Run({"ft.create", "i1", "on", "json", "schema", "$.id", "as", "id", "tag"});

  auto resp = Run({"ft.profile", "i1", "search", "query", "@id:[1 1]"});
  ASSERT_ARRAY_OF_TWO_ARRAYS(resp);

  EXPECT_THAT(resp.GetVec()[0], IsMapWithSize());

  resp = Run({"ft.profile", "i1", "search", "query", "@{invalid13289}"});
  EXPECT_THAT(resp, ErrArg("query syntax error"));
}
#endif

TEST_F(SearchFamilyTest, FtProfileErrorReply) {
  Run({"ft.create", "i1", "schema", "name", "text"});

  auto resp = Run({"ft.profile", "i1", "not_search", "query", "(a | b) c d"});
  EXPECT_THAT(resp, ErrArg("no `SEARCH` or `AGGREGATE` provided"));

  resp = Run({"ft.profile", "i1", "search", "not_query", "(a | b) c d"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  resp = Run({"ft.profile", "non_existent_key", "search", "query", "(a | b) c d"});
  EXPECT_THAT(resp, ErrArg("non_existent_key: no such index"));
}

TEST_F(SearchFamilyTest, SimpleExpiry) {
  EXPECT_EQ(Run({"ft.create", "i1", "schema", "title", "text", "expires-in", "numeric"}), "OK");

  Run({"hset", "d:1", "title", "never to expire", "expires-in", "100500"});

  Run({"hset", "d:2", "title", "first to expire", "expires-in", "50"});
  Run({"pexpire", "d:2", "50"});

  Run({"hset", "d:3", "title", "second to expire", "expires-in", "100"});
  Run({"pexpire", "d:3", "100"});

  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1", "d:2", "d:3"));

  AdvanceTime(60);
  ThisFiber::SleepFor(5ms);  // Give heartbeat time to delete expired doc
  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1", "d:3"));

  AdvanceTime(60);
  Run({"HGETALL", "d:3"});  // Trigger expiry by access
  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1"));

  Run({"flushall"});
}

TEST_F(SearchFamilyTest, DocsEditing) {
  auto resp = Run({"JSON.SET", "k1", ".", R"({"a":"1"})"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "index", "ON", "JSON", "SCHEMA", "$.a", "AS", "a", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, IsMapWithSize("k1", IsMap("$", R"({"a":"1"})")));

  // Test dump and restore
  resp = Run({"DUMP", "k1"});
  auto dump = resp.GetBuf();

  resp = Run({"DEL", "k1"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"RESTORE", "k1", "0", ToSV(dump)});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, IsMapWithSize("k1", IsMap("$", R"({"a":"1"})")));

  // Test renaming a key
  EXPECT_EQ(Run({"RENAME", "k1", "new_k1"}), "OK");

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, IsMapWithSize("new_k1", IsMap("$", R"({"a":"1"})")));

  EXPECT_EQ(Run({"RENAME", "new_k1", "k1"}), "OK");

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, IsMapWithSize("k1", IsMap("$", R"({"a":"1"})")));
}

TEST_F(SearchFamilyTest, AggregateGroupBy) {
  Run({"hset", "key:1", "word", "item1", "foo", "10", "text", "\"first key\"", "non_indexed_value",
       "1"});
  Run({"hset", "key:2", "word", "item2", "foo", "20", "text", "\"second key\"", "non_indexed_value",
       "2"});
  Run({"hset", "key:3", "word", "item1", "foo", "40", "text", "\"third key\"", "non_indexed_value",
       "3"});

  auto resp = Run(
      {"ft.create", "i1", "ON", "HASH", "SCHEMA", "word", "TAG", "foo", "NUMERIC", "text", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp = Run(
      {"ft.aggregate", "i1", "*", "GROUPBY", "1", "@word", "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("count", "2", "word", "item1"),
                                         IsMap("word", "item2", "count", "1")));

  resp = Run({"ft.aggregate", "i1", "*", "GROUPBY", "1", "@word", "REDUCE", "SUM", "1", "@foo",
              "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("foo_total", "50", "word", "item1"),
                                         IsMap("foo_total", "20", "word", "item2")));

  resp = Run({"ft.aggregate", "i1", "*", "GROUPBY", "1", "@word", "REDUCE", "AVG", "1", "@foo",
              "AS", "foo_average"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("foo_average", "20", "word", "item2"),
                                         IsMap("foo_average", "25", "word", "item1")));

  resp = Run({"ft.aggregate", "i1", "*", "GROUPBY", "2", "@word", "@text", "REDUCE", "SUM", "1",
              "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("foo_total", "10", "word", "item1", "text", "\"first key\""),
                        IsMap("foo_total", "40", "word", "item1", "text", "\"third key\""),
                        IsMap("foo_total", "20", "word", "item2", "text", "\"second key\"")));

  resp = Run({"ft.aggregate", "i1", "*", "LOAD", "2", "foo", "word", "GROUPBY", "1", "@word",
              "REDUCE", "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("foo_total", "20", "word", "item2"),
                                         IsMap("foo_total", "50", "word", "item1")));

  resp = Run({"ft.aggregate", "i1", "*", "LOAD", "2", "foo", "text", "GROUPBY", "2", "@word",
              "@text", "REDUCE", "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("foo_total", "40", "word", "item1", "text", "\"third key\""),
                        IsMap("foo_total", "20", "word", "item2", "text", "\"second key\""),
                        IsMap("foo_total", "10", "word", "item1", "text", "\"first key\"")));
}

TEST_F(SearchFamilyTest, JsonAggregateGroupBy) {
  Run({"JSON.SET", "product:1", "$", R"({"name": "Product A", "price": 10, "quantity": 2})"});
  Run({"JSON.SET", "product:2", "$", R"({"name": "Product B", "price": 20, "quantity": 3})"});
  Run({"JSON.SET", "product:3", "$", R"({"name": "Product C", "price": 30, "quantity": 5})"});

  auto resp =
      Run({"FT.CREATE", "json_index", "ON", "JSON", "SCHEMA", "$.name", "AS", "name", "TEXT",
           "$.price", "AS", "price", "NUMERIC", "$.quantity", "AS", "quantity", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.AGGREGATE", "json_index", "*", "GROUPBY", "0", "REDUCE", "SUM", "1", "price",
              "AS", "total_price"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("total_price", "60")));

  resp = Run({"FT.AGGREGATE", "json_index", "*", "GROUPBY", "0", "REDUCE", "AVG", "1", "price",
              "AS", "avg_price"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("avg_price", "20")));
}

TEST_F(SearchFamilyTest, JsonAggregateGroupByWithoutAtSign) {
  absl::FlagSaver fs;
  Run({"HSET", "h1", "group", "first", "value", "1"});
  Run({"HSET", "h2", "group", "second", "value", "2"});
  Run({"HSET", "h3", "group", "first", "value", "3"});

  auto resp =
      Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "group", "TAG", "value", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  absl::SetFlag(&FLAGS_search_reject_legacy_field, false);
  resp = Run({"FT.AGGREGATE", "index", "*", "GROUPBY", "1", "group", "REDUCE", "COUNT", "0", "AS",
              "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("count", "2", "group", "first"),
                                         IsMap("group", "second", "count", "1")));
  absl::SetFlag(&FLAGS_search_reject_legacy_field, true);
  resp = Run({"FT.AGGREGATE", "index", "*", "GROUPBY", "1", "group", "REDUCE", "COUNT", "0", "AS",
              "count"});
  EXPECT_THAT(resp, ErrArg("bad arguments: Field name should start with '@'"));
}

TEST_F(SearchFamilyTest, AggregateGroupByReduceSort) {
  for (size_t i = 0; i < 101; i++) {  // 51 even, 50 odd
    Run({"hset", absl::StrCat("k", i), "even", (i % 2 == 0) ? "true" : "false", "value",
         absl::StrCat(i)});
  }
  Run({"ft.create", "i1", "schema", "even", "tag", "sortable", "value", "numeric", "sortable"});

  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_search_reject_legacy_field, false);
  // clang-format off
  auto resp = Run({"ft.aggregate", "i1", "*",
                  "GROUPBY", "1", "@even",
                      "REDUCE", "count", "0", "as", "count",
                      "REDUCE", "count_distinct", "1", "even", "as", "distinct_tags",
                      "REDUCE", "count_distinct", "1", "value", "as", "distinct_vals",
                      "REDUCE", "max", "1", "value", "as", "max_val",
                      "REDUCE", "min", "1", "value", "as", "min_val",
                  "SORTBY", "1", "count"});
  // clang-format on

  EXPECT_THAT(resp,
              IsUnordArrayWithSize(IsMap("even", "false", "count", "50", "distinct_tags", "1",
                                         "distinct_vals", "50", "max_val", "99", "min_val", "1"),
                                   IsMap("even", "true", "count", "51", "distinct_tags", "1",
                                         "distinct_vals", "51", "max_val", "100", "min_val", "0")));
  absl::SetFlag(&FLAGS_search_reject_legacy_field, true);
  // clang-format off
  resp = Run({"ft.aggregate", "i1", "*",
                  "GROUPBY", "1", "@even",
                      "REDUCE", "count", "0", "as", "count",
                      "REDUCE", "count_distinct", "1", "even", "as", "distinct_tags",
                      "REDUCE", "count_distinct", "1", "value", "as", "distinct_vals",
                      "REDUCE", "max", "1", "value", "as", "max_val",
                      "REDUCE", "min", "1", "value", "as", "min_val",
                  "SORTBY", "1", "count"});
  // clang-format on

  EXPECT_THAT(resp, ErrArg("SORTBY field name 'count' must start with '@'"));
}

TEST_F(SearchFamilyTest, AggregateLoadGroupBy) {
  for (size_t i = 0; i < 101; i++) {  // 51 even, 50 odd
    Run({"hset", absl::StrCat("k", i), "even", (i % 2 == 0) ? "true" : "false", "value",
         absl::StrCat(i)});
  }
  Run({"ft.create", "i1", "schema", "value", "numeric", "sortable"});

  // clang-format off
  auto resp = Run({"ft.aggregate", "i1", "*",
                  "LOAD", "1", "even",
                  "GROUPBY", "1", "@even"});
  // clang-format on

  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("even", "false"), IsMap("even", "true")));
}

TEST_F(SearchFamilyTest, AggregateLoad) {
  Run({"hset", "key:1", "word", "item1", "foo", "10"});
  Run({"hset", "key:2", "word", "item2", "foo", "20"});
  Run({"hset", "key:3", "word", "item1", "foo", "30"});

  auto resp = Run({"ft.create", "index", "ON", "HASH", "SCHEMA", "word", "TAG", "foo", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  // ft.aggregate index "*" LOAD 1 @word LOAD 1 @foo
  resp = Run({"ft.aggregate", "index", "*", "LOAD", "1", "@word", "LOAD", "1", "@foo"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("word", "item1", "foo", "30"),
                                         IsMap("word", "item2", "foo", "20"),
                                         IsMap("word", "item1", "foo", "10")));

  // ft.aggregate index "*" GROUPBY 1 @word REDUCE SUM 1 @foo AS foo_total LOAD 1 foo_total
  resp = Run({"ft.aggregate", "index", "*", "GROUPBY", "1", "@word", "REDUCE", "SUM", "1", "@foo",
              "AS", "foo_total", "LOAD", "1", "foo_total"});
  EXPECT_THAT(resp, ErrArg("LOAD cannot be applied after projectors or reducers"));
}

TEST_F(SearchFamilyTest, Vector) {
  auto resp = Run({"ft.create", "ann", "ON", "HASH", "SCHEMA", "vector", "VECTOR", "HNSW", "8",
                   "TYPE", "FLOAT32", "DIM", "100", "distance_metric", "cosine", "M", "64"});
  EXPECT_EQ(resp, "OK");
}

TEST_F(SearchFamilyTest, EscapedSymbols) {
  Run({"ft.create", "i1", "ON", "HASH", "SCHEMA", "color", "tag"});

  // TODO ',' is separator, we need to check should next request work or not
  // In redis it works for JSON but not for HASH
  // Run({"hset", "i1", "color", R"(blue,1\$+)"});
  // EXPECT_THAT(Run({"ft.search", "i1", R"(@color:{blue\,1\\\$\+})"}), AreDocIds("i1"));
  // EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue.1\"%="});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\.1\\\"\\%\\=}"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue<1'^~"});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\<1\\'\\^\\~}"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue>1:&/"});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\>1\\:\\&\\/}"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue{1;* "});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\{1\\;\\*\\ }"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue}1!("});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\}1\\!\\(}"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue[1@)"});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\[1\\@\\)}"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);

  Run({"hset", "i1", "color", "blue]1#-"});
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue\\]1\\#\\-}"}), AreDocIds("i1"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), kNoResults);
}

TEST_F(SearchFamilyTest, FlushSearchIndices) {
  auto resp =
      Run({"FT.CREATE", "json", "ON", "JSON", "SCHEMA", "$.nested.value", "AS", "value", "TEXT"});
  EXPECT_EQ(resp, "OK");

  EXPECT_EQ(Run({"FLUSHALL"}), "OK");

  // Test that the index was removed
  resp = Run({"FT.CREATE", "json", "ON", "JSON", "SCHEMA", "$.another.nested.value", "AS", "value",
              "TEXT"});
  EXPECT_EQ(resp, "OK");

  EXPECT_EQ(Run({"FLUSHDB"}), "OK");

  // Test that the index was removed
  resp = Run({"FT.CREATE", "json", "ON", "JSON", "SCHEMA", "$.another.nested.value", "AS", "value",
              "TEXT"});
  EXPECT_EQ(resp, "OK");

  EXPECT_EQ(Run({"select", "1"}), "OK");
  EXPECT_EQ(Run({"FLUSHDB"}), "OK");
  EXPECT_EQ(Run({"select", "0"}), "OK");

  // Test that index was not removed
  resp = Run({"FT.CREATE", "json", "ON", "JSON", "SCHEMA", "$.another.nested.value", "AS", "value",
              "TEXT"});
  EXPECT_THAT(resp, ErrArg("ERR Index already exists"));
}

// todo: ASAN fails heres on arm
#ifndef SANITIZERS
TEST_F(SearchFamilyTest, AggregateWithLoadOptionHard) {
  // Test HASH
  Run({"HSET", "h1", "word", "item1", "foo", "10", "text", "first key"});
  Run({"HSET", "h2", "word", "item2", "foo", "20", "text", "second key"});

  auto resp = Run(
      {"FT.CREATE", "i1", "ON", "HASH", "SCHEMA", "word", "TAG", "foo", "NUMERIC", "text", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "2", "foo", "text", "GROUPBY", "2", "@word",
              "@text", "REDUCE", "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(IsMap("foo_total", "20", "word", "item2", "text", "second key"),
                                   IsMap("foo_total", "10", "word", "item1", "text", "first key")));

  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "1", "@word", "GROUPBY", "1", "@word", "REDUCE",
              "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("foo_total", "20", "word", "item2"),
                                         IsMap("foo_total", "10", "word", "item1")));

  // Test JSON
  Run({"JSON.SET", "j1", ".", R"({"word":"item1","foo":10,"text":"first key"})"});
  Run({"JSON.SET", "j2", ".", R"({"word":"item2","foo":20,"text":"second key"})"});

  resp = Run({"FT.CREATE", "i2", "ON", "JSON", "SCHEMA", "$.word", "AS", "word", "TAG", "$.foo",
              "AS", "foo", "NUMERIC", "$.text", "AS", "text", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "2", "foo", "text", "GROUPBY", "2", "@word",
              "@text", "REDUCE", "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(IsMap("foo_total", "20", "word", "item2", "text", "second key"),
                                   IsMap("foo_total", "10", "word", "item1", "text", "first key")));

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "1", "@word", "GROUPBY", "1", "@word", "REDUCE",
              "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("foo_total", "20", "word", "item2"),
                                         IsMap("foo_total", "10", "word", "item1")));
}
#endif

TEST_F(SearchFamilyTest, WrongFieldTypeJson) {
  // Test simple
  Run({"JSON.SET", "j1", ".", R"({"value":"one"})"});
  Run({"JSON.SET", "j2", ".", R"({"value":1})"});

  EXPECT_EQ(Run({"FT.CREATE", "i1", "ON", "JSON", "SCHEMA", "$.value", "AS", "value", "NUMERIC",
                 "SORTABLE"}),
            "OK");

  auto resp = Run({"FT.SEARCH", "i1", "*"});
  EXPECT_THAT(resp, AreDocIds("j2"));

  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "1", "$.value"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("$.value", "1")));

  // Test with two fields. One is loading
  Run({"JSON.SET", "j3", ".", R"({"value":"two","another_value":1})"});
  Run({"JSON.SET", "j4", ".", R"({"value":2,"another_value":2})"});

  EXPECT_EQ(Run({"FT.CREATE", "i2", "ON", "JSON", "SCHEMA", "$.value", "AS", "value", "NUMERIC"}),
            "OK");

  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_search_reject_legacy_field, false);
  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "2", "$.value", "$.another_value", "GROUPBY", "2",
              "$.value", "$.another_value", "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(
                  IsMap("$.value", "1", "$.another_value", ArgType(RespExpr::NIL), "count", "1"),
                  IsMap("$.value", "2", "$.another_value", "2", "count", "1")));
  absl::SetFlag(&FLAGS_search_reject_legacy_field, true);

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "2", "$.value", "$.another_value", "GROUPBY", "2",
              "$.value", "$.another_value", "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, ErrArg("bad arguments: Field name should start with '@'"));

  // Test multiple field values
  Run({"JSON.SET", "j5", ".", R"({"arr":[{"id":1},{"id":"two"}]})"});
  Run({"JSON.SET", "j6", ".", R"({"arr":[{"id":1},{"id":2}]})"});
  Run({"JSON.SET", "j7", ".", R"({"arr":[]})"});

  resp = Run({"FT.CREATE", "i3", "ON", "JSON", "SCHEMA", "$.arr[*].id", "AS", "id", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "i3", "*"});
  EXPECT_THAT(resp, AreDocIds("j1", "j2", "j3", "j4", "j6", "j7"));  // Only j5 fails

  resp = Run({"FT.CREATE", "i4", "ON", "JSON", "SCHEMA", "$.arr[*].id", "AS", "id", "NUMERIC",
              "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "i4", "*"});
  EXPECT_THAT(resp, AreDocIds("j1", "j2", "j3", "j4", "j6", "j7"));  // Only j5 fails
}

TEST_F(SearchFamilyTest, WrongFieldTypeHash) {
  // Test simple
  Run({"HSET", "h1", "value", "one"});
  Run({"HSET", "h2", "value", "1"});

  EXPECT_EQ(Run({"FT.CREATE", "i1", "ON", "HASH", "SCHEMA", "value", "NUMERIC", "SORTABLE"}), "OK");

  auto resp = Run({"FT.SEARCH", "i1", "*"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("value", "1")));

  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "1", "@value"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("value", "1")));

  // Test with two fields. One is loading
  Run({"HSET", "h3", "value", "two", "another_value", "1"});
  Run({"HSET", "h4", "value", "2", "another_value", "2"});

  EXPECT_EQ(Run({"FT.CREATE", "i2", "ON", "HASH", "SCHEMA", "value", "NUMERIC"}), "OK");

  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "@another_value"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("value", "1"), "h4",
                                  IsMap("value", "2", "another_value", "2")));

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "2", "@value", "@another_value", "GROUPBY", "2",
              "@value", "@another_value", "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("value", "1", "another_value", ArgType(RespExpr::NIL), "count", "1"),
                        IsMap("value", "2", "another_value", "2", "count", "1")));
}

TEST_F(SearchFamilyTest, WrongFieldTypeHardJson) {
  Run({"JSON.SET", "j1", ".", R"({"data":1,"name":"doc_with_int"})"});
  Run({"JSON.SET", "j2", ".", R"({"data":"1","name":"doc_with_int_as_string"})"});
  Run({"JSON.SET", "j3", ".", R"({"data":"string","name":"doc_with_string"})"});
  Run({"JSON.SET", "j4", ".",
       R"({"data":["first", "second", "third"],"name":"doc_with_strings"})"});
  Run({"JSON.SET", "j5", ".", R"({"name":"no_data"})"});
  Run({"JSON.SET", "j6", ".", R"({"data":[5,4,3],"name":"doc_with_vector"})"});
  Run({"JSON.SET", "j7", ".", R"({"data":"[5,4,3]","name":"doc_with_vector_as_string"})"});
  Run({"JSON.SET", "j8", ".", R"({"data":null,"name":"doc_with_null"})"});
  Run({"JSON.SET", "j9", ".", R"({"data":[null, null, null],"name":"doc_with_nulls"})"});
  Run({"JSON.SET", "j10", ".", R"({"data":true,"name":"doc_with_boolean"})"});
  Run({"JSON.SET", "j11", ".", R"({"data":[true, false, true],"name":"doc_with_booleans"})"});

  auto resp = Run({"FT.CREATE", "i1", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  resp = Run(
      {"FT.CREATE", "i2", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "NUMERIC", "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i3", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "TAG"});
  EXPECT_EQ(resp, "OK");

  resp =
      Run({"FT.CREATE", "i4", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "TAG", "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i5", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp =
      Run({"FT.CREATE", "i6", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "TEXT", "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i7", "ON", "JSON", "SCHEMA", "$.data", "AS", "data", "VECTOR", "FLAT",
              "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "i1", "*"});
  EXPECT_THAT(resp, AreDocIds("j1", "j5", "j6", "j8", "j9"));

  resp = Run({"FT.SEARCH", "i2", "*"});
  EXPECT_THAT(resp, AreDocIds("j1", "j5", "j6", "j8", "j9"));

  resp = Run({"FT.SEARCH", "i3", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j3", "j4", "j5", "j7", "j8", "j9", "j10", "j11"));

  resp = Run({"FT.SEARCH", "i4", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j3", "j4", "j5", "j7", "j8", "j9", "j10", "j11"));

  resp = Run({"FT.SEARCH", "i5", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j3", "j4", "j5", "j7", "j8", "j9"));

  resp = Run({"FT.SEARCH", "i6", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j3", "j4", "j5", "j7", "j8", "j9"));

  resp = Run({"FT.SEARCH", "i7", "*"});
  EXPECT_THAT(resp, AreDocIds("j5", "j6", "j8"));
}

TEST_F(SearchFamilyTest, WrongFieldTypeHardHash) {
  Run({"HSET", "j1", "data", "1", "name", "doc_with_int"});
  Run({"HSET", "j2", "data", "1", "name", "doc_with_int_as_string"});
  Run({"HSET", "j3", "data", "string", "name", "doc_with_string"});
  Run({"HSET", "j4", "name", "no_data"});
  Run({"HSET", "j5", "data", "5,4,3", "name", "doc_with_fake_vector"});
  Run({"HSET", "j6", "data", "[5,4,3]", "name", "doc_with_fake_vector_as_string"});

  // Vector [1, 2, 3]
  std::string vector = std::string("\x3f\x80\x00\x00\x40\x00\x00\x00\x40\x40\x00\x00", 12);
  Run({"HSET", "j7", "data", vector, "name", "doc_with_vector [1, 2, 3]"});

  auto resp = Run({"FT.CREATE", "i1", "ON", "HASH", "SCHEMA", "data", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i2", "ON", "HASH", "SCHEMA", "data", "NUMERIC", "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i3", "ON", "HASH", "SCHEMA", "data", "TAG"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i4", "ON", "HASH", "SCHEMA", "data", "TAG", "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i5", "ON", "HASH", "SCHEMA", "data", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i6", "ON", "HASH", "SCHEMA", "data", "TEXT", "SORTABLE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.CREATE", "i7", "ON", "HASH", "SCHEMA", "data", "VECTOR", "FLAT", "6", "TYPE",
              "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "i1", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j1", "j4"));

  resp = Run({"FT.SEARCH", "i2", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j1", "j4"));

  resp = Run({"FT.SEARCH", "i3", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j7", "j3", "j6", "j1", "j4", "j5"));

  resp = Run({"FT.SEARCH", "i4", "*"});
  EXPECT_THAT(resp, AreDocIds("j2", "j7", "j3", "j6", "j1", "j4", "j5"));

  resp = Run({"FT.SEARCH", "i5", "*"});
  EXPECT_THAT(resp, AreDocIds("j4", "j2", "j7", "j3", "j6", "j1", "j5"));

  resp = Run({"FT.SEARCH", "i6", "*"});
  EXPECT_THAT(resp, AreDocIds("j4", "j2", "j7", "j3", "j6", "j1", "j5"));

  resp = Run({"FT.SEARCH", "i7", "*"});
  EXPECT_THAT(resp, AreDocIds("j4", "j7"));
}

TEST_F(SearchFamilyTest, WrongVectorFieldType) {
  Run({"JSON.SET", "j1", ".",
       R"({"vector_field": [0.1, 0.2, 0.3], "name": "doc_with_correct_dim"})"});
  Run({"JSON.SET", "j2", ".", R"({"vector_field": [0.1, 0.2], "name": "doc_with_small_dim"})"});
  Run({"JSON.SET", "j3", ".",
       R"({"vector_field": [0.1, 0.2, 0.3, 0.4], "name": "doc_with_large_dim"})"});
  Run({"JSON.SET", "j4", ".", R"({"vector_field": [1, 2, 3], "name": "doc_with_int_values"})"});
  Run({"JSON.SET", "j5", ".",
       R"({"vector_field":"not_vector", "name":"doc_with_incorrect_field_type"})"});
  Run({"JSON.SET", "j6", ".", R"({"name":"doc_with_no_field"})"});
  Run({"JSON.SET", "j7", ".",
       R"({"vector_field": [999999999999999999999999999999999999999, -999999999999999999999999999999999999999, 500000000000000000000000000000000000000], "name": "doc_with_out_of_range_values"})"});
  Run({"JSON.SET", "j8", ".", R"({"vector_field":null, "name": "doc_with_null"})"});
  Run({"JSON.SET", "j9", ".", R"({"vector_field":[null, null, null], "name": "doc_with_nulls"})"});
  Run({"JSON.SET", "j10", ".", R"({"vector_field":true, "name": "doc_with_boolean"})"});
  Run({"JSON.SET", "j11", ".",
       R"({"vector_field":[true, false, true], "name": "doc_with_booleans"})"});
  Run({"JSON.SET", "j12", ".", R"({"vector_field":1, "name": "doc_with_int"})"});

  auto resp =
      Run({"FT.CREATE", "index", "ON", "JSON", "SCHEMA", "$.vector_field", "AS", "vector_field",
           "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, AreDocIds("j6", "j7", "j1", "j4", "j8"));
}

// Test that FT.AGGREGATE prints only needed fields
TEST_F(SearchFamilyTest, AggregateResultFields) {
  Run({"JSON.SET", "j1", ".", R"({"a":"1","b":"2","c":"3"})"});
  Run({"JSON.SET", "j2", ".", R"({"a":"4","b":"5","c":"6"})"});
  Run({"JSON.SET", "j3", ".", R"({"a":"7","b":"8","c":"9"})"});

  auto resp = Run({"FT.CREATE", "i1", "ON", "JSON", "SCHEMA", "$.a", "AS", "a", "TEXT", "SORTABLE",
                   "$.b", "AS", "b", "TEXT", "$.c", "AS", "c", "TEXT"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.AGGREGATE", "i1", "*"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap(), IsMap(), IsMap()));

  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_search_reject_legacy_field, false);
  resp = Run({"FT.AGGREGATE", "i1", "*", "SORTBY", "1", "a"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("a", "1"), IsMap("a", "4"), IsMap("a", "7")));
  absl::SetFlag(&FLAGS_search_reject_legacy_field, true);
  resp = Run({"FT.AGGREGATE", "i1", "*", "SORTBY", "1", "a"});
  EXPECT_THAT(resp, ErrArg("SORTBY field name 'a' must start with '@'"));

  absl::SetFlag(&FLAGS_search_reject_legacy_field, false);
  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "1", "@b", "SORTBY", "1", "a"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("b", "2", "a", "1"), IsMap("b", "5", "a", "4"),
                                         IsMap("b", "8", "a", "7")));
  absl::SetFlag(&FLAGS_search_reject_legacy_field, true);
  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "1", "@b", "SORTBY", "1", "a"});
  EXPECT_THAT(resp, ErrArg("SORTBY field name 'a' must start with '@'"));

  absl::SetFlag(&FLAGS_search_reject_legacy_field, false);
  resp = Run({"FT.AGGREGATE", "i1", "*", "SORTBY", "1", "a", "GROUPBY", "2", "@b", "@a", "REDUCE",
              "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("b", "8", "a", "7", "count", "1"),
                                         IsMap("b", "2", "a", "1", "count", "1"),
                                         IsMap("b", "5", "a", "4", "count", "1")));
  absl::SetFlag(&FLAGS_search_reject_legacy_field, true);
  resp = Run({"FT.AGGREGATE", "i1", "*", "SORTBY", "1", "a", "GROUPBY", "2", "@b", "@a", "REDUCE",
              "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, ErrArg("SORTBY field name 'a' must start with '@'"));

  Run({"JSON.SET", "j4", ".", R"({"id":1, "number":4})"});
  Run({"JSON.SET", "j5", ".", R"({"id":2})"});

  resp = Run({"FT.CREATE", "i2", "ON", "JSON", "SCHEMA", "$.id", "AS", "id", "NUMERIC", "$.number",
              "AS", "number", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "2", "@id", "@number"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("id", "1", "number", "4"), IsMap("id", "2"), IsMap(),
                                         IsMap(), IsMap()));
}

TEST_F(SearchFamilyTest, AggregateSortByJson) {
  Run({"JSON.SET", "j1", "$", R"({"name": "first", "number": 1200, "group": "first"})"});
  Run({"JSON.SET", "j2", "$", R"({"name": "second", "number": 800, "group": "first"})"});
  Run({"JSON.SET", "j3", "$", R"({"name": "third", "number": 300, "group": "first"})"});
  Run({"JSON.SET", "j4", "$", R"({"name": "fourth", "number": 400, "group": "second"})"});
  Run({"JSON.SET", "j5", "$", R"({"name": "fifth", "number": 900, "group": "second"})"});
  Run({"JSON.SET", "j6", "$", R"({"name": "sixth", "number": 300, "group": "first"})"});
  Run({"JSON.SET", "j7", "$", R"({"name": "seventh", "number": 400, "group": "second"})"});
  Run({"JSON.SET", "j8", "$", R"({"name": "eighth", "group": "first"})"});
  Run({"JSON.SET", "j9", "$", R"({"name": "ninth", "group": "second"})"});

  Run({"FT.CREATE", "index", "ON", "JSON", "SCHEMA", "$.name", "AS", "name", "TEXT", "$.number",
       "AS", "number", "NUMERIC", "$.group", "AS", "group", "TAG"});

  // Test sorting by name (DESC) and number (ASC)
  auto resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "4", "@name", "DESC", "@number", "ASC"});
  EXPECT_THAT(
      resp, IsUnordArrayWithSize(
                IsMap("name", "third", "number", "300"), IsMap("name", "sixth", "number", "300"),
                IsMap("name", "seventh", "number", "400"), IsMap("name", "second", "number", "800"),
                IsMap("name", "ninth"), IsMap("name", "fourth", "number", "400"),
                IsMap("name", "first", "number", "1200"), IsMap("name", "fifth", "number", "900"),
                IsMap("name", "eighth")));

  // Test sorting by name (ASC) and number (DESC)
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "4", "@name", "ASC", "@number", "DESC"});
  EXPECT_THAT(
      resp, IsUnordArrayWithSize(
                IsMap("name", "eighth"), IsMap("name", "fifth", "number", "900"),
                IsMap("name", "first", "number", "1200"), IsMap("name", "fourth", "number", "400"),
                IsMap("name", "ninth"), IsMap("name", "second", "number", "800"),
                IsMap("name", "seventh", "number", "400"), IsMap("name", "sixth", "number", "300"),
                IsMap("name", "third", "number", "300")));

  // Test sorting by group (ASC), number (DESC), and name
  resp = Run(
      {"FT.AGGREGATE", "index", "*", "SORTBY", "5", "@group", "ASC", "@number", "DESC", "@name"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(IsMap("group", "first", "number", "1200", "name", "first"),
                                   IsMap("group", "first", "number", "800", "name", "second"),
                                   IsMap("group", "first", "number", "300", "name", "sixth"),
                                   IsMap("group", "first", "number", "300", "name", "third"),
                                   IsMap("group", "first", "name", "eighth"),
                                   IsMap("group", "second", "number", "900", "name", "fifth"),
                                   IsMap("group", "second", "number", "400", "name", "fourth"),
                                   IsMap("group", "second", "number", "400", "name", "seventh"),
                                   IsMap("group", "second", "name", "ninth")));

  // Test sorting by number (ASC), group (DESC), and name
  resp = Run(
      {"FT.AGGREGATE", "index", "*", "SORTBY", "5", "@number", "ASC", "@group", "DESC", "@name"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(IsMap("number", "300", "group", "first", "name", "sixth"),
                                   IsMap("number", "300", "group", "first", "name", "third"),
                                   IsMap("number", "400", "group", "second", "name", "fourth"),
                                   IsMap("number", "400", "group", "second", "name", "seventh"),
                                   IsMap("number", "800", "group", "first", "name", "second"),
                                   IsMap("number", "900", "group", "second", "name", "fifth"),
                                   IsMap("number", "1200", "group", "first", "name", "first"),
                                   IsMap("group", "second", "name", "ninth"),
                                   IsMap("group", "first", "name", "eighth")));

  // Test sorting with MAX 3
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@number", "MAX", "3"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("number", "300"), IsMap("number", "300"),
                                         IsMap("number", "400")));

  // Test sorting with MAX 3
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "2", "@number", "DESC", "MAX", "3"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("number", "1200"), IsMap("number", "900"),
                                         IsMap("number", "800")));

  // Test sorting by number (ASC) with MAX 999
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@number", "MAX", "999"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("number", "300"), IsMap("number", "300"),
                                         IsMap("number", "400"), IsMap("number", "400"),
                                         IsMap("number", "800"), IsMap("number", "900"),
                                         IsMap("number", "1200"), IsMap(), IsMap()));

  // Test sorting by name and number (DESC)
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "3", "@name", "@number", "DESC"});
  EXPECT_THAT(
      resp, IsUnordArrayWithSize(
                IsMap("name", "eighth"), IsMap("name", "fifth", "number", "900"),
                IsMap("name", "first", "number", "1200"), IsMap("name", "fourth", "number", "400"),
                IsMap("name", "ninth"), IsMap("name", "second", "number", "800"),
                IsMap("name", "seventh", "number", "400"), IsMap("name", "sixth", "number", "300"),
                IsMap("name", "third", "number", "300")));

  // Test SORTBY with MAX, GROUPBY, and REDUCE COUNT
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@name", "MAX", "3", "GROUPBY", "1",
              "@number", "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("number", "900", "count", "1"),
                                         IsMap("number", ArgType(RespExpr::NIL), "count", "1"),
                                         IsMap("number", "1200", "count", "1")));

  // Test SORTBY with MAX, GROUPBY (0 fields), and REDUCE COUNT
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@name", "MAX", "3", "GROUPBY", "0",
              "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("count", "3")));
}

TEST_F(SearchFamilyTest, AggregateSortByParsingErrors) {
  Run({"JSON.SET", "j1", "$", R"({"name": "first", "number": 1200, "group": "first"})"});
  Run({"JSON.SET", "j2", "$", R"({"name": "second", "number": 800, "group": "first"})"});
  Run({"JSON.SET", "j3", "$", R"({"name": "third", "number": 300, "group": "first"})"});
  Run({"JSON.SET", "j4", "$", R"({"name": "fourth", "number": 400, "group": "second"})"});
  Run({"JSON.SET", "j5", "$", R"({"name": "fifth", "number": 900, "group": "second"})"});
  Run({"JSON.SET", "j6", "$", R"({"name": "sixth", "number": 300, "group": "first"})"});
  Run({"JSON.SET", "j7", "$", R"({"name": "seventh", "number": 400, "group": "second"})"});
  Run({"JSON.SET", "j8", "$", R"({"name": "eighth", "group": "first"})"});
  Run({"JSON.SET", "j9", "$", R"({"name": "ninth", "group": "second"})"});

  Run({"FT.CREATE", "index", "ON", "JSON", "SCHEMA", "$.name", "AS", "name", "TEXT", "$.number",
       "AS", "number", "NUMERIC", "$.group", "AS", "group", "TAG"});

  // Test SORTBY with invalid argument count
  auto resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "999", "@name", "@number", "DESC"});
  EXPECT_THAT(resp, ErrArg("bad arguments for SORTBY: specified invalid number of strings"));

  // Test SORTBY with negative argument count
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "-3", "@name", "@number", "DESC"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test MAX with invalid value
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@name", "MAX", "-10"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test MAX without a value
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@name", "MAX"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  // Test SORTBY with a non-existing field
  /* Temporary unsupported
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@nonexistingfield"});
  EXPECT_THAT(resp, ErrArg("Property `nonexistingfield` not loaded nor in schema")); */

  // Test SORTBY with an invalid value
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "notvalue", "@name"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));
}

TEST_F(SearchFamilyTest, AggregateSortByParsingErrorsWithoutAt) {
  Run({"JSON.SET", "j1", "$", R"({"name": "first", "number": 1200, "group": "first"})"});

  Run({"FT.CREATE", "index", "ON", "JSON", "SCHEMA", "$.name", "AS", "name", "TEXT", "$.number",
       "AS", "number", "NUMERIC", "$.group", "AS", "group", "TAG"});

  // Test SORTBY with field name without '@'
  auto resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "name"});
  EXPECT_THAT(resp, ErrArg("SORTBY field name 'name' must start with '@'"));

  // Test SORTBY with field name without '@' and multiple sort fields
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "3", "name", "@number", "DESC"});
  EXPECT_THAT(resp, ErrArg("SORTBY field name 'name' must start with '@'"));

  // Test SORTBY with field name without '@' and MAX option
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "name", "MAX", "1"});
  EXPECT_THAT(resp, ErrArg("SORTBY field name 'name' must start with '@'"));

  // Check that the old error still works for wrong number of args
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "2", "@name"});
  EXPECT_THAT(resp, ErrArg("bad arguments for SORTBY: specified invalid number of strings"));
}

TEST_F(SearchFamilyTest, InvalidSearchOptions) {
  Run({"JSON.SET", "j1", ".", R"({"field1":"first","field2":"second"})"});
  Run({"FT.CREATE", "idx", "ON", "JSON", "SCHEMA", "$.field1", "AS", "field1", "TEXT", "$.field2",
       "AS", "field2", "TEXT"});

  /* Test with an empty query and LOAD. TODO: Add separate test for query syntax
  auto resp = Run({"FT.SEARCH", "idx", "", "LOAD", "1", "@field1"});
  EXPECT_THAT(resp, IsMapWithSize()); */

  // Test with LIMIT missing arguments
  auto resp = Run({"FT.SEARCH", "idx", "*", "LIMIT", "0"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  // Test with LIMIT exceeding the maximum allowed value
  resp = Run({"FT.SEARCH", "idx", "*", "LIMIT", "0", "100000000000000000000"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test with LIMIT and negative arguments
  resp = Run({"FT.SEARCH", "idx", "*", "LIMIT", "-1", "10"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test with LIMIT and invalid argument types
  resp = Run({"FT.SEARCH", "idx", "*", "LIMIT", "start", "count"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test with invalid RETURN syntax (missing count)
  resp = Run({"FT.SEARCH", "idx", "*", "RETURN", "@field1", "@field2"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test with RETURN having duplicate fields
  resp = Run({"FT.SEARCH", "idx", "*", "RETURN", "4", "field1", "field1", "field2", "field2"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("field1", "first", "field2", "second")));

  // Test with RETURN exceeding maximum allowed count
  resp = Run({"FT.SEARCH", "idx", "*", "RETURN", "100000000000000000000", "@field1", "@field2"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test with NOCONTENT and RETURN
  resp = Run({"FT.SEARCH", "idx", "*", "NOCONTENT", "RETURN", "2", "@field1", "@field2"});
  EXPECT_THAT(resp, IsArray(IntArg(1), "j1"));
}

TEST_F(SearchFamilyTest, KnnSearchOptions) {
  Run({"JSON.SET", "doc:1", ".", R"({"vector": [0.1, 0.2, 0.3, 0.4]})"});
  Run({"JSON.SET", "doc:2", ".", R"({"vector": [0.5, 0.6, 0.7, 0.8]})"});
  Run({"JSON.SET", "doc:3", ".", R"({"vector": [0.9, 0.1, 0.4, 0.3]})"});

  auto resp = Run({"FT.CREATE", "my_index", "ON",  "JSON",   "PREFIX",          "1",     "doc:",
                   "SCHEMA",    "$.vector", "AS",  "vector", "VECTOR",          "FLAT",  "6",
                   "TYPE",      "FLOAT32",  "DIM", "4",      "DISTANCE_METRIC", "COSINE"});
  EXPECT_EQ(resp, "OK");

  std::string query_vector("\x00\x00\x00\x3f\x00\x00\x00\x40\x00\x00\x00\x41\x00\x00\x80\x42", 16);

  // KNN 2
  resp = Run({"FT.SEARCH", "my_index", "*=>[KNN 2 @vector $query_vector]", "PARAMS", "2",
              "query_vector", query_vector});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2"));

  // KNN 11929939
  resp = Run({"FT.SEARCH", "my_index", "*=>[KNN 11929939 @vector $query_vector]", "PARAMS", "2",
              "query_vector", query_vector});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:3"));

  // KNN 11929939, LIMIT 4 2
  resp = Run({"FT.SEARCH", "my_index", "*=>[KNN 11929939 @vector $query_vector]", "PARAMS", "2",
              "query_vector", query_vector, "LIMIT", "4", "2"});
  EXPECT_THAT(resp, IntArg(3));

  // KNN 11929939, LIMIT 0 10
  resp = Run({"FT.SEARCH", "my_index", "*=>[KNN 11929939 @vector $query_vector]", "PARAMS", "2",
              "query_vector", query_vector, "LIMIT", "0", "10"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:3"));

  // KNN 1, LIMIT 0 2
  resp = Run({"FT.SEARCH", "my_index", "*=>[KNN 1 @vector $query_vector]", "PARAMS", "2",
              "query_vector", query_vector, "LIMIT", "0", "2"});
  EXPECT_THAT(resp, AreDocIds("doc:1"));
}

TEST_F(SearchFamilyTest, KnnWithSortBy) {
  vector<string> doc_ids(100);
  for (size_t i = 0; i < doc_ids.size(); i++) {
    doc_ids[i] = absl::StrCat("d:", i);
    auto v = absl::StrFormat(R"({"v": [%d.0], "d": %d})", i, i);
    Run({"JSON.SET", doc_ids[i], ".", v});
  }

  Run({"FT.CREATE", "i1",      "ON",     "JSON", "PREFIX",          "1",    "d:",
       "SCHEMA",    "$.v",     "AS",     "v",    "VECTOR",          "FLAT", "6",
       "TYPE",      "FLOAT32", "DIM",    "1",    "DISTANCE_METRIC", "L2",   "$.d",
       "AS",        "d",       "NUMERIC"});

  // We first select knn_limit closest values and then sort in REVERSE by distance
  // on a non-sortable field. The result should be first cut off by knn_limit and then sorted
  for (size_t knn_limit = 8; knn_limit < 47; knn_limit += 3) {
    vector<string> expect_ids(doc_ids.begin() + knn_limit - min<size_t>(knn_limit, 10u),
                              doc_ids.begin() + knn_limit);
    reverse(expect_ids.begin(), expect_ids.end());

    const float qpoint = 0.0f;
    std::string q = absl::StrFormat("*=>[KNN %d @v $query_vector]", knn_limit);
    auto resp = Run({"ft.search", "i1", q, "SORTBY", "d", "DESC", "PARAMS", "2", "query_vector",
                     FloatSV(&qpoint), "LIMIT", "0", "10", "RETURN", "1", "d"});
    EXPECT_THAT(resp, DocIds(knn_limit, expect_ids)) << knn_limit;
  }
}

TEST_F(SearchFamilyTest, InvalidAggregateOptions) {
  Run({"JSON.SET", "j1", ".", R"({"field1":"first","field2":"second"})"});
  Run({"FT.CREATE", "idx", "ON", "JSON", "SCHEMA", "$.field1", "AS", "field1", "TEXT", "$.field2",
       "AS", "field2", "TEXT"});

  // Test GROUPBY with no arguments
  auto resp = Run({"FT.AGGREGATE", "idx", "*", "GROUPBY"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  // Test GROUPBY with invalid count
  resp = Run({"FT.AGGREGATE", "idx", "*", "GROUPBY", "-1", "@field1"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp =
      Run({"FT.AGGREGATE", "idx", "*", "GROUPBY", "100000000000000000000", "@field1", "@field2"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test REDUCE with no REDUCE function
  resp = Run({"FT.AGGREGATE", "idx", "*", "GROUPBY", "1", "@field1", "REDUCE"});
  EXPECT_THAT(resp, ErrArg("reducer function  not found"));

  /* // Test REDUCE with COUNT function
  resp = Run({"FT.AGGREGATE", "idx", "*", "GROUPBY", "1", "@field1", "REDUCE", "COUNT", "0"});
  EXPECT_THAT(resp, IsMapWithSize("__generated_aliascount", "1", "field1", "first")); */

  // Test REDUCE with invalid function
  resp = Run({"FT.AGGREGATE", "idx", "*", "GROUPBY", "1", "@field1", "REDUCE", "INVALIDFUNC", "0",
              "AS", "result"});
  EXPECT_THAT(resp, ErrArg("reducer function INVALIDFUNC not found"));

  // Test SORTBY with no arguments
  resp = Run({"FT.AGGREGATE", "idx", "*", "SORTBY"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  // Test SORTBY with invalid count
  resp = Run({"FT.AGGREGATE", "idx", "*", "SORTBY", "-1", "@field1"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"FT.AGGREGATE", "idx", "*", "SORTBY", "100000000000000000000", "@field1"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test LIMIT with invalid arguments
  resp = Run({"FT.AGGREGATE", "idx", "*", "LIMIT", "0"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  resp = Run({"FT.AGGREGATE", "idx", "*", "LIMIT", "-1", "10"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"FT.AGGREGATE", "idx", "*", "LIMIT", "0", "100000000000000000000"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  // Test LOAD with invalid arguments
  resp = Run({"FT.AGGREGATE", "idx", "*", "LOAD", "@field1", "@field2"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"FT.AGGREGATE", "idx", "*", "LOAD", "-1", "@field1"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"FT.AGGREGATE", "idx", "*", "LOAD", "100000000000000000000", "@field1", "@field2"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));
}

TEST_F(SearchFamilyTest, InvalidCreateOptions) {
  // Test with a duplicate field in the schema
  auto resp = Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "title", "TEXT", "title", "TEXT"});
  EXPECT_THAT(resp, ErrArg("Duplicate field in schema - title"));

  // Test with no fields in the schema
  resp = Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA"});
  EXPECT_THAT(resp, ErrArg("Fields arguments are missing"));

  // Test with an invalid field type
  resp = Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "title", "UNKNOWN_TYPE"});
  EXPECT_THAT(resp, ErrArg("Field type UNKNOWN_TYPE is not supported"));

  // Test with an invalid STOPWORDS argument
  resp = Run({"FT.CREATE", "index", "ON", "HASH", "STOPWORDS", "10", "the", "and", "of", "SCHEMA",
              "title", "TEXT"});
  EXPECT_THAT(resp, ErrArg(kSyntaxErr));

  resp = Run({"FT.CREATE", "index", "ON", "HASH", "STOPWORDS", "99999999999999999999", "the", "and",
              "of", "SCHEMA", "title", "TEXT"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"FT.CREATE", "index", "ON", "HASH", "STOPWORDS", "-1", "the", "and", "of", "SCHEMA",
              "title", "TEXT"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));

  resp = Run({"FT.CREATE", "index", "ON", "HASH", "STOPWORDS", "not_a_number", "the", "and", "of",
              "SCHEMA", "title", "TEXT"});
  EXPECT_THAT(resp, ErrArg(kInvalidIntErr));
}

TEST_F(SearchFamilyTest, SynonymManagement) {
  // Create index with prefix
  EXPECT_EQ(
      Run({"FT.CREATE", "my_idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT"}),
      "OK");

  // Add first group of synonyms
  EXPECT_EQ(Run({"FT.SYNUPDATE", "my_idx", "1", "cat", "feline", "kitty"}), "OK");

  // Add second group of synonyms
  EXPECT_EQ(Run({"FT.SYNUPDATE", "my_idx", "2", "kitty", "cute", "adorable"}), "OK");

  // Add third group of synonyms
  EXPECT_EQ(Run({"FT.SYNUPDATE", "my_idx", "3", "kitty", "tiger", "cub"}), "OK");

  // Check the dump output
  auto resp = Run({"FT.SYNDUMP", "my_idx"});
  EXPECT_THAT(resp, IsUnordArray("cub", IsArray("3"), "cute", IsArray("2"), "adorable",
                                 IsArray("2"), "kitty", IsArray("1", "2", "3"), "feline",
                                 IsArray("1"), "tiger", IsArray("3"), "cat", IsArray("1")));
}

TEST_F(SearchFamilyTest, SynonymsSearch) {
  // Create search index
  auto resp =
      Run({"FT.CREATE", "myIndex", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT"});
  EXPECT_EQ(resp, "OK");

  // Add documents
  EXPECT_THAT(Run({"HSET", "doc:1", "title", "car"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:2", "title", "automobile"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:3", "title", "vehicle"}), IntArg(1));

  // Add synonyms "car" and "automobile" to group 1
  resp = Run({"FT.SYNUPDATE", "myIndex", "1", "car", "automobile"});
  EXPECT_EQ(resp, "OK");

  // Check synonyms list
  resp = Run({"FT.SYNDUMP", "myIndex"});
  ASSERT_THAT(resp, ArrLen(4));

  // Search for "car" (should find both "car" and "automobile")
  resp = Run({"FT.SEARCH", "myIndex", "car"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2"));

  // Search for "automobile" (should find both "car" and "automobile")
  resp = Run({"FT.SEARCH", "myIndex", "automobile"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2"));

  // Add "vehicle" to the synonym group
  resp = Run({"FT.SYNUPDATE", "myIndex", "1", "vehicle"});
  EXPECT_EQ(resp, "OK");

  // Search for "vehicle" (should find all three documents)
  resp = Run({"FT.SEARCH", "myIndex", "vehicle"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:3"));
}

// Test for case-insensitive synonyms
TEST_F(SearchFamilyTest, CaseInsensitiveSynonyms) {
  // Create an index
  EXPECT_EQ(Run({"FT.CREATE", "case_idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title",
                 "TEXT"}),
            "OK");

  // Add documents with different case words
  EXPECT_THAT(Run({"HSET", "doc:1", "title", "The cat is sleeping"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:2", "title", "A feline hunter"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:3", "title", "The dog is barking"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:4", "title", "A Canine friend"}), IntArg(1));

  // Add synonym groups with text IDs
  EXPECT_EQ(Run({"FT.SYNUPDATE", "case_idx", "my_synonyms_group0", "cat", "feline"}), "OK");
  EXPECT_EQ(Run({"FT.SYNUPDATE", "case_idx", "my_synonyms_group1", "dog", "canine"}), "OK");

  // Check synonym output
  auto resp = Run({"FT.SYNDUMP", "case_idx"});
  EXPECT_THAT(resp, ArrLen(8));  // 4 terms, each with a list of groups

  // Synonym search is case-insensitive
  // Search for "cat" should find "cat" and "feline"
  resp = Run({"FT.SEARCH", "case_idx", "cat"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2"));

  // Search for "feline" should find "feline" and "cat"
  resp = Run({"FT.SEARCH", "case_idx", "feline"});
  EXPECT_THAT(resp, AreDocIds("doc:2", "doc:1"));

  // Search for "dog" should find "dog" and "canine"
  resp = Run({"FT.SEARCH", "case_idx", "dog"});
  EXPECT_THAT(resp, AreDocIds("doc:3", "doc:4"));

  // Search for "canine" should find "canine" and "dog"
  resp = Run({"FT.SEARCH", "case_idx", "canine"});
  EXPECT_THAT(resp, AreDocIds("doc:4", "doc:3"));

  // Search with different case
  // Search for "Cat" (uppercase) should find "cat" and "feline"
  resp = Run({"FT.SEARCH", "case_idx", "Cat"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2"));

  // Search for "FELINE" (uppercase) should find "feline" and "cat"
  resp = Run({"FT.SEARCH", "case_idx", "FELINE"});
  EXPECT_THAT(resp, AreDocIds("doc:2", "doc:1"));

  // Search for "DoG" (mixed case) should find "dog" and "canine"
  resp = Run({"FT.SEARCH", "case_idx", "DoG"});
  EXPECT_THAT(resp, AreDocIds("doc:3", "doc:4"));

  // Search for "cAnInE" (mixed case) should find "canine" and "dog"
  resp = Run({"FT.SEARCH", "case_idx", "cAnInE"});
  EXPECT_THAT(resp, AreDocIds("doc:4", "doc:3"));
}

TEST_F(SearchFamilyTest, SynonymsWithSpaces) {
  EXPECT_EQ(Run({"FT.CREATE", "my_index", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "field",
                 "TEXT"}),
            "OK");

  EXPECT_EQ(Run({"FT.SYNUPDATE", "my_index", "syn_group", "word1", "word2"}), "OK");

  EXPECT_THAT(Run({"HSET", "doc:1", "field", " syn_group"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:2", "field", "syn_group"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:3", "field", "word1"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:4", "field", "word2"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:5", "field", R"(\ syn_group)"}), IntArg(1));

  auto resp = Run({"FT.SEARCH", "my_index", "word1"});
  EXPECT_THAT(resp, AreDocIds("doc:3", "doc:4"));

  resp = Run({"FT.SEARCH", "my_index", "word2"});
  EXPECT_THAT(resp, AreDocIds("doc:4", "doc:3"));

  resp = Run({"FT.SEARCH", "my_index", "syn_group"});
  EXPECT_THAT(resp, AreDocIds("doc:2", "doc:1", "doc:5"));

  // FT.SEARCH my_index "\ syn_group"
  // FT.SEARCH my_index " syn_group"
  // The both transform to " syn_group" after syntax analysis
  // " syn_group" passes to query_str in FtSearch
  resp = Run({"FT.SEARCH", "my_index", " syn_group"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:5"));
}

TEST_F(SearchFamilyTest, SynonymsWithLeadingSpaces) {
  EXPECT_EQ(Run({"FT.CREATE", "my_index", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title",
                 "TEXT"}),
            "OK");

  EXPECT_EQ(Run({"FT.SYNUPDATE", "my_index", "group1", "word", "    several_spaces_synonym"}),
            "OK");

  auto resp = Run({"FT.SYNDUMP", "my_index"});
  EXPECT_THAT(resp, IsUnordArray("    several_spaces_synonym", IsArray("group1"), "word",
                                 IsArray("group1")));

  EXPECT_THAT(Run({"HSET", "doc:1", "title", "word"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:2", "title", "several_spaces_synonym"}), IntArg(1));

  resp = Run({"FT.SEARCH", "my_index", "word"});
  EXPECT_THAT(resp, AreDocIds("doc:1"));

  resp = Run({"FT.SEARCH", "my_index", "several_spaces_synonym"});
  EXPECT_THAT(resp, AreDocIds("doc:2"));

  EXPECT_THAT(Run({"HSET", "doc:3", "title", "    several_spaces_synonym"}), IntArg(1));

  resp = Run({"FT.SEARCH", "my_index", "word"});
  EXPECT_THAT(resp, AreDocIds("doc:1"));
}

// Test to verify prefix search works correctly with synonyms
TEST_F(SearchFamilyTest, PrefixSearchWithSynonyms) {
  // Create search index
  EXPECT_EQ(Run({"FT.CREATE", "prefix_index", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
                 "title", "TEXT"}),
            "OK");

  // Add documents with words that start with the same prefix
  EXPECT_THAT(Run({"HSET", "doc:1", "title", "apple"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:2", "title", "application"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:3", "title", "banana"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:4", "title", "appetizer"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:5", "title", "pineapple"}), IntArg(1));
  EXPECT_THAT(Run({"HSET", "doc:6", "title", "macintosh"}), IntArg(1));

  // Check prefix search before adding synonyms
  auto resp = Run({"FT.SEARCH", "prefix_index", "app*"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:4"));

  // Add synonym: apple <-> macintosh
  EXPECT_EQ(Run({"FT.SYNUPDATE", "prefix_index", "1", "apple", "macintosh"}), "OK");

  // Verify prefix search still works after adding synonyms
  resp = Run({"FT.SEARCH", "prefix_index", "app*"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:4"));

  // Check exact term search for terms that are now synonyms
  resp = Run({"FT.SEARCH", "prefix_index", "apple"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:6"));  // Should find both apple and macintosh

  resp = Run({"FT.SEARCH", "prefix_index", "macintosh"});
  EXPECT_THAT(resp, AreDocIds("doc:6", "doc:1"));  // Should find both macintosh and apple

  // Check that prefix search for mac* only finds macintosh, not apple
  resp = Run({"FT.SEARCH", "prefix_index", "mac*"});
  EXPECT_THAT(resp, AreDocIds("doc:6"));  // Should only find macintosh
}

TEST_F(SearchFamilyTest, SearchSortByOptionNonSortableFieldJson) {
  Run({"JSON.SET", "json1", "$", R"({"text":"2"})"});
  Run({"JSON.SET", "json2", "$", R"({"text":"1"})"});

  auto resp = Run({"FT.CREATE", "index", "ON", "JSON", "SCHEMA", "$.text", "AS", "text", "TEXT"});
  EXPECT_EQ(resp, "OK");

  auto expect_expr = [](std::string_view text_field) {
    return IsArray(2, "json2", IsMap(text_field, "1", "$", R"({"text":"1"})"), "json1",
                   IsMap(text_field, "2", "$", R"({"text":"2"})"));
  };

  resp = Run({"FT.SEARCH", "index", "*", "SORTBY", "text"});
  EXPECT_THAT(resp, expect_expr("text"sv));
}

TEST_F(SearchFamilyTest, SearchNonNullFields) {
  // Basic schema with text, tag, and numeric fields
  EXPECT_EQ(Run({"ft.create", "i1", "schema", "title", "text", "tags", "tag", "score", "numeric",
                 "sortable"}),
            "OK");

  Run({"hset", "d:1", "title", "Document with title and tags", "tags", "tag1,tag2"});
  Run({"hset", "d:2", "title", "Document with title and score", "score", "75"});
  Run({"hset", "d:3", "title", "Document with all fields", "tags", "tag2,tag3", "score", "100"});
  Run({"hset", "d:4", "tags", "Document with only tags", "score", "50"});

  // Testing non-null field searches with @field:* syntax
  EXPECT_THAT(Run({"ft.search", "i1", "@title:*"}), AreDocIds("d:1", "d:2", "d:3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@tags:*"}), AreDocIds("d:1", "d:3", "d:4"));
  EXPECT_THAT(Run({"ft.search", "i1", "@score:*"}), AreDocIds("d:2", "d:3", "d:4"));

  // Testing combinations of non-null field searches
  EXPECT_THAT(Run({"ft.search", "i1", "@title:* @tags:*"}), AreDocIds("d:1", "d:3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@title:* @score:*"}), AreDocIds("d:2", "d:3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@tags:* @score:*"}), AreDocIds("d:3", "d:4"));
  EXPECT_THAT(Run({"ft.search", "i1", "@title:* @tags:* @score:*"}), AreDocIds("d:3"));

  // Testing non-null field searches with sorting
  auto result = Run({"ft.search", "i1", "@score:*", "SORTBY", "score", "DESC"});
  ASSERT_EQ(result.GetVec().size(), 7);
  EXPECT_EQ(result.GetVec()[1].GetString(), "d:3");  // Highest score (100) first
  EXPECT_EQ(result.GetVec()[3].GetString(), "d:2");  // Middle score (75)
  EXPECT_EQ(result.GetVec()[5].GetString(), "d:4");  // Lowest score (50) last

  // Testing non-null field searches with JSON
  Run({"json.set", "j:1", ".",
       R"({"title": "JSON document", "meta": {"tags": ["tag1", "tag2"]}})"});
  Run({"json.set", "j:2", ".", R"({"meta": {"score": 100}})"});
  Run({"json.set", "j:3", ".",
       R"({"title": "Full JSON", "meta": {"tags": ["tag3"], "score": 80}})"});

  EXPECT_EQ(Run({"ft.create", "i2", "on", "json", "schema", "$.title", "as", "title", "text",
                 "$.meta.tags", "as", "tags", "tag", "$.meta.score", "as", "score", "numeric"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "i2", "@title:*"}), AreDocIds("j:1", "j:3"));
  EXPECT_THAT(Run({"ft.search", "i2", "@tags:*"}), AreDocIds("j:1", "j:3"));
  EXPECT_THAT(Run({"ft.search", "i2", "@score:*"}), AreDocIds("j:2", "j:3"));
  EXPECT_THAT(Run({"ft.search", "i2", "@title:* @tags:* @score:*"}), AreDocIds("j:3"));

  // Testing text indices with star query
  Run({"hset", "text:1", "content", "apple banana"});
  Run({"hset", "text:2", "content", "cherry date"});
  Run({"hset", "text:3", "content", "elephant fig"});

  EXPECT_EQ(Run({"ft.create", "text_idx", "ON", "HASH", "PREFIX", "1", "text:", "SCHEMA", "content",
                 "TEXT"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "text_idx", "*"}), AreDocIds("text:1", "text:2", "text:3"));

  // Testing tag indices with star query
  Run({"hset", "tag:1", "categories", "fruit,food"});
  Run({"hset", "tag:2", "categories", "drink,beverage"});
  Run({"hset", "tag:3", "categories", "tech,gadget"});

  EXPECT_EQ(Run({"ft.create", "tag_idx", "ON", "HASH", "PREFIX", "1", "tag:", "SCHEMA",
                 "categories", "TAG", "SEPARATOR", ","}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "tag_idx", "*"}), AreDocIds("tag:1", "tag:2", "tag:3"));

  // Testing numeric indices with star query
  Run({"hset", "num:1", "price", "10.5"});
  Run({"hset", "num:2", "price", "20.75"});
  Run({"hset", "num:3", "price", "30.99"});

  EXPECT_EQ(Run({"ft.create", "num_idx", "ON", "HASH", "PREFIX", "1", "num:", "SCHEMA", "price",
                 "NUMERIC", "SORTABLE"}),
            "OK");

  EXPECT_THAT(Run({"ft.search", "num_idx", "*"}), AreDocIds("num:1", "num:2", "num:3"));

  // Testing vector indices with star query
  string vector1 = R"(\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00)";  // [1,0,0]
  string vector2 = R"(\x00\x00\x00\x00\x00\x00\x80\x3f\x00\x00\x00\x00)";  // [0,1,0]
  string vector3 = R"(\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x3f)";  // [0,0,1]

  Run({"hset", "vec:1", "embedding", vector1});
  Run({"hset", "vec:2", "embedding", vector2});
  Run({"hset", "vec:3", "embedding", vector3});

  // Testing star query with result limit
  auto limit_result = Run({"ft.search", "text_idx", "*", "LIMIT", "0", "2"});

  // No sorting, so results returned are in random order (implementation-dependent).
  EXPECT_THAT(limit_result, RespElementsAre(IntArg(3), _, _, _, _));

  // Testing star query with sorting
  auto price_desc_result = Run({"ft.search", "num_idx", "*", "SORTBY", "price", "DESC"});
  ASSERT_EQ(price_desc_result.GetVec().size(), 7);
  EXPECT_EQ(price_desc_result.GetVec()[1].GetString(), "num:3");  // Most expensive item first
  EXPECT_EQ(price_desc_result.GetVec()[3].GetString(), "num:2");
  EXPECT_EQ(price_desc_result.GetVec()[5].GetString(), "num:1");  // Cheapest item last

  auto price_asc_result = Run({"ft.search", "num_idx", "*", "SORTBY", "price", "ASC"});
  ASSERT_EQ(price_asc_result.GetVec().size(), 7);
  EXPECT_EQ(price_asc_result.GetVec()[1].GetString(), "num:1");  // Cheapest item first
  EXPECT_EQ(price_asc_result.GetVec()[3].GetString(), "num:2");
  EXPECT_EQ(price_asc_result.GetVec()[5].GetString(), "num:3");  // Most expensive item last
}

TEST_F(SearchFamilyTest, SortIndexBasicOperations) {
  // Create an index with a numeric field and a text field, both SORTABLE
  EXPECT_EQ(Run({"ft.create", "sort_idx", "SCHEMA", "num_field", "NUMERIC", "SORTABLE", "str_field",
                 "TEXT", "SORTABLE"}),
            "OK");

  // Add documents with different field values - only with both fields for test simplification
  Run({"hset", "doc:1", "num_field", "10", "str_field", "apple"});
  Run({"hset", "doc:2", "num_field", "20", "str_field", "banana"});
  Run({"hset", "doc:3", "num_field", "5", "str_field", "cherry"});
  Run({"hset", "doc:4", "num_field", "15", "str_field", "date"});

  // Test search with star (* - all documents)
  EXPECT_THAT(Run({"ft.search", "sort_idx", "*"}), AreDocIds("doc:1", "doc:2", "doc:3", "doc:4"));

  // Test search by field presence
  EXPECT_THAT(Run({"ft.search", "sort_idx", "@num_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:3", "doc:4"));
  EXPECT_THAT(Run({"ft.search", "sort_idx", "@str_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:3", "doc:4"));

  // Test sorting by numeric field (ascending)
  auto num_asc_result = Run({"ft.search", "sort_idx", "*", "SORTBY", "num_field", "ASC"});

  // Check the overall order, not specific indices
  ASSERT_GE(num_asc_result.GetVec().size(), 9);  // 4 documents * 2 + 1

  // Collect document IDs in the order they appear in the result
  std::vector<std::string> sorted_ids;
  for (size_t i = 1; i < num_asc_result.GetVec().size(); i += 2) {
    sorted_ids.push_back(num_asc_result.GetVec()[i].GetString());
  }

  // Verify that the numeric field sorting order is correct
  ASSERT_EQ(sorted_ids.size(), 4);
  EXPECT_EQ(sorted_ids[0], "doc:3");  // 5
  EXPECT_EQ(sorted_ids[1], "doc:1");  // 10
  EXPECT_EQ(sorted_ids[2], "doc:4");  // 15
  EXPECT_EQ(sorted_ids[3], "doc:2");  // 20

  // Sorting by text field (descending)
  auto str_desc_result = Run({"ft.search", "sort_idx", "*", "SORTBY", "str_field", "DESC"});

  // Check the overall order of text sorting
  sorted_ids.clear();
  for (size_t i = 1; i < str_desc_result.GetVec().size(); i += 2) {
    sorted_ids.push_back(str_desc_result.GetVec()[i].GetString());
  }

  ASSERT_EQ(sorted_ids.size(), 4);
  EXPECT_EQ(sorted_ids[0], "doc:4");  // date
  EXPECT_EQ(sorted_ids[1], "doc:3");  // cherry
  EXPECT_EQ(sorted_ids[2], "doc:2");  // banana
  EXPECT_EQ(sorted_ids[3], "doc:1");  // apple

  // Update a document
  Run({"hset", "doc:3", "num_field", "30"});  // 5 -> 30

  // Check the updated sorting
  auto updated_result = Run({"ft.search", "sort_idx", "*", "SORTBY", "num_field", "ASC"});
  sorted_ids.clear();
  for (size_t i = 1; i < updated_result.GetVec().size(); i += 2) {
    sorted_ids.push_back(updated_result.GetVec()[i].GetString());
  }

  ASSERT_EQ(sorted_ids.size(), 4);
  EXPECT_EQ(sorted_ids[0], "doc:1");  // 10
  EXPECT_EQ(sorted_ids[1], "doc:4");  // 15
  EXPECT_EQ(sorted_ids[2], "doc:2");  // 20
  EXPECT_EQ(sorted_ids[3], "doc:3");  // 30

  // Test document deletion
  Run({"del", "doc:2"});
  auto after_delete_result = Run({"ft.search", "sort_idx", "*"});
  EXPECT_THAT(after_delete_result, AreDocIds("doc:1", "doc:3", "doc:4"));
}

// Separate test for documents with missing fields during sorting
TEST_F(SearchFamilyTest, SortIndexWithNullFields) {
  EXPECT_EQ(Run({"ft.create", "null_sort_idx", "SCHEMA", "num_field", "NUMERIC", "SORTABLE"}),
            "OK");

  // Documents with and without numeric field
  Run({"hset", "doc:1", "num_field", "10"});
  Run({"hset", "doc:2", "num_field", "20"});
  Run({"hset", "doc:3", "other_field", "value"});  // no numeric field

  // Verify that all documents are indexed
  EXPECT_THAT(Run({"ft.search", "null_sort_idx", "*"}), AreDocIds("doc:1", "doc:2", "doc:3"));

  // Verify that only documents with numeric field are found by @num_field:* query
  EXPECT_THAT(Run({"ft.search", "null_sort_idx", "@num_field:*"}), AreDocIds("doc:1", "doc:2"));

  // When sorting, documents without the field should be at the end (but exact order may vary)
  auto sort_result = Run({"ft.search", "null_sort_idx", "*", "SORTBY", "num_field", "ASC"});

  // Collect results
  std::vector<std::string> sorted_ids;
  for (size_t i = 1; i < sort_result.GetVec().size(); i += 2) {
    sorted_ids.push_back(sort_result.GetVec()[i].GetString());
  }

  // Verify that documents with numeric fields are in the correct order,
  // and the document without a numeric field is either at the end or not included (depends on
  // implementation)
  ASSERT_GE(sorted_ids.size(), 2);

  // Check only documents with known field values
  auto doc1_pos = std::find(sorted_ids.begin(), sorted_ids.end(), "doc:1");
  auto doc2_pos = std::find(sorted_ids.begin(), sorted_ids.end(), "doc:2");

  ASSERT_NE(doc1_pos, sorted_ids.end());
  ASSERT_NE(doc2_pos, sorted_ids.end());

  // doc:1 (10) should be before doc:2 (20) in ascending sort
  EXPECT_LT(std::distance(sorted_ids.begin(), doc1_pos),
            std::distance(sorted_ids.begin(), doc2_pos));
}

TEST_F(SearchFamilyTest, VectorIndexOperations) {
  // Create an index with a vector field
  EXPECT_EQ(Run({"ft.create", "vector_idx", "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE",
                 "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2", "name", "TEXT"}),
            "OK");

  // Function to convert float vectors to binary representation
  auto FloatsToBytes = [](const std::vector<float>& floats) -> std::string {
    return std::string(reinterpret_cast<const char*>(floats.data()), floats.size() * sizeof(float));
  };

  // Prepare vector data in binary format
  std::string vec1 = FloatsToBytes({1.0f, 0.0f, 0.0f});
  std::string vec2 = FloatsToBytes({0.0f, 1.0f, 0.0f});
  std::string vec3 = FloatsToBytes({0.0f, 0.0f, 1.0f});
  std::string vec4 = FloatsToBytes({0.5f, 0.5f, 0.0f});
  std::string vec5 = FloatsToBytes({0.3f, 0.3f, 0.3f});

  // Add documents with vector data in binary format
  Run({"hset", "vec:1", "vec", vec1, "name", "vector1"});
  Run({"hset", "vec:2", "vec", vec2, "name", "vector2"});
  Run({"hset", "vec:3", "vec", vec3, "name", "vector3"});
  Run({"hset", "vec:4", "vec", vec4, "name", "vector4"});
  Run({"hset", "vec:5", "vec", vec5, "name", "vector5"});

  // Basic star search
  auto star_search = Run({"ft.search", "vector_idx", "*"});
  EXPECT_THAT(star_search, AreDocIds("vec:1", "vec:2", "vec:3", "vec:4", "vec:5"));

  // Search by vector field presence
  auto vec_field_search = Run({"ft.search", "vector_idx", "@vec:*"});
  EXPECT_THAT(vec_field_search, AreDocIds("vec:1", "vec:2", "vec:3", "vec:4", "vec:5"));
}

// Test to verify that @field:* syntax works with sortable fields
TEST_F(SearchFamilyTest, SortIndexGetAllResults) {
  // Create an index with a numeric field that is SORTABLE but not indexed as a regular field
  EXPECT_EQ(Run({"ft.create", "sort_only_idx", "SCHEMA", "sort_field", "NUMERIC", "SORTABLE"}),
            "OK");

  // Add documents with and without the sortable field
  Run({"hset", "doc:1", "sort_field", "10", "other_field", "value1"});
  Run({"hset", "doc:2", "sort_field", "20", "other_field", "value2"});
  Run({"hset", "doc:3", "sort_field", "30", "other_field", "value3"});
  Run({"hset", "doc:4", "other_field", "value4"});  // no sort_field
  Run({"hset", "doc:5", "other_field", "value5"});  // no sort_field

  // Test that all documents are indexed
  EXPECT_THAT(Run({"ft.search", "sort_only_idx", "*"}),
              AreDocIds("doc:1", "doc:2", "doc:3", "doc:4", "doc:5"));

  // Test that @field:* search works for sortable field
  // This should only return documents that have the sort_field
  EXPECT_THAT(Run({"ft.search", "sort_only_idx", "@sort_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:3"));

  // Test sorting with @field:* query
  auto sort_result =
      Run({"ft.search", "sort_only_idx", "@sort_field:*", "SORTBY", "sort_field", "DESC"});

  // Collect document IDs in order
  std::vector<std::string> sorted_ids;
  for (size_t i = 1; i < sort_result.GetVec().size(); i += 2) {
    sorted_ids.push_back(sort_result.GetVec()[i].GetString());
  }

  // Verify correct order
  ASSERT_EQ(sorted_ids.size(), 3);
  EXPECT_EQ(sorted_ids[0], "doc:3");  // 30
  EXPECT_EQ(sorted_ids[1], "doc:2");  // 20
  EXPECT_EQ(sorted_ids[2], "doc:1");  // 10
}

TEST_F(SearchFamilyTest, JsonWithNullFields) {
  // Create JSON documents with null values in different field types
  Run({"JSON.SET", "doc:1", ".",
       R"({"text_field": "sample text", "tag_field": "tag1,tag2", "num_field": 100})"});
  Run({"JSON.SET", "doc:2", ".", R"({"text_field": null, "tag_field": "tag3", "num_field": 200})"});
  Run({"JSON.SET", "doc:3", ".",
       R"({"text_field": "another text", "tag_field": null, "num_field": 300})"});
  Run({"JSON.SET", "doc:4", ".",
       R"({"text_field": "more text", "tag_field": "tag4,tag5", "num_field": null})"});
  Run({"JSON.SET", "doc:5", ".", R"({"text_field": null, "tag_field": null, "num_field": null})"});
  Run({"JSON.SET", "doc:6", ".", R"({"other_field": "not indexed field"})"});

  // Create indices for text, tag, and numeric fields (non-sortable)
  EXPECT_EQ(Run({"FT.CREATE", "idx:regular", "ON", "JSON", "SCHEMA", "$.text_field", "AS",
                 "text_field", "TEXT", "$.tag_field", "AS", "tag_field", "TAG", "$.num_field", "AS",
                 "num_field", "NUMERIC"}),
            "OK");

  // Create indices for text, tag, and numeric fields (sortable)
  EXPECT_EQ(Run({"FT.CREATE",    "idx:sortable", "ON",         "JSON",    "SCHEMA",
                 "$.text_field", "AS",           "text_field", "TEXT",    "SORTABLE",
                 "$.tag_field",  "AS",           "tag_field",  "TAG",     "SORTABLE",
                 "$.num_field",  "AS",           "num_field",  "NUMERIC", "SORTABLE"}),
            "OK");

  // Test @field:* searches on non-sortable index
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@text_field:*"}),
              AreDocIds("doc:1", "doc:3", "doc:4"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@tag_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:4"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@num_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:3"));

  // Test @field:* searches on sortable index
  EXPECT_THAT(Run({"FT.SEARCH", "idx:sortable", "@text_field:*"}),
              AreDocIds("doc:1", "doc:3", "doc:4"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:sortable", "@tag_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:4"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:sortable", "@num_field:*"}),
              AreDocIds("doc:1", "doc:2", "doc:3"));

  // Test search for documents with non-null values for all fields
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@text_field:* @tag_field:* @num_field:*"}),
              AreDocIds("doc:1"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:sortable", "@text_field:* @tag_field:* @num_field:*"}),
              AreDocIds("doc:1"));

  // Test combined queries
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@text_field:* @tag_field:*"}),
              AreDocIds("doc:1", "doc:4"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@text_field:* @num_field:*"}),
              AreDocIds("doc:1", "doc:3"));
  EXPECT_THAT(Run({"FT.SEARCH", "idx:regular", "@tag_field:* @num_field:*"}),
              AreDocIds("doc:1", "doc:2"));
}

TEST_F(SearchFamilyTest, TestHsetDeleteDocumentHnswSchemaCrash) {
  EXPECT_EQ(Run({"FT.CREATE", "idx", "SCHEMA", "n", "NUMERIC", "v", "VECTOR", "HNSW", "8", "TYPE",
                 "FLOAT16", "DIM", "4", "DISTANCE_METRIC", "L2", "M", "65536"}),
            "OK");

  auto res = Run({"HSET", "doc", "n", "0"});
  EXPECT_EQ(res, 1);

  res = Run({"DEL", "doc"});
  EXPECT_EQ(res, 1);
}

TEST_F(SearchFamilyTest, RenameDocumentBetweenIndices) {
  absl::FlagSaver fs;

  SetTestFlag("cluster_mode", "emulated");
  ResetService();

  EXPECT_EQ(Run({"ft.create", "idx1", "prefix", "1", "idx1", "filter", "@index==\"yes\"", "schema",
                 "t", "text"}),
            "OK");
  EXPECT_EQ(Run({"ft.create", "idx2", "prefix", "1", "idx2", "filter", "@index==\"yes\"", "schema",
                 "t", "text"}),
            "OK");

  Run({"hset", "idx1:{doc}1", "t", "foo1", "index", "yes"});

  EXPECT_EQ(Run({"rename", "idx1:{doc}1", "idx2:{doc}1"}), "OK");
  EXPECT_EQ(Run({"rename", "idx2:{doc}1", "idx1:{doc}1"}), "OK");
}

TEST_F(SearchFamilyTest, JsonSetIndexesBug) {
  auto resp = Run({"JSON.SET", "j1", "$", R"({"text":"some text"})"});
  EXPECT_THAT(resp, "OK");

  resp = Run(
      {"FT.CREATE", "index", "ON", "json", "SCHEMA", "$.text", "AS", "text", "TEXT", "SORTABLE"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.SET", "j1", "$", R"({"asd}"})"});
  EXPECT_THAT(resp, ErrArg("ERR failed to parse JSON"));

  resp = Run({"FT.AGGREGATE", "index", "*", "GROUPBY", "1", "@text"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("text", "some text")));
}

TEST_F(SearchFamilyTest, SearchReindexWriteSearchRace) {
  const std::string kIndexName = "myRaceIdx";
  const int kWriterOps = 200;
  const int kSearcherOps = 200;
  const int kReindexerOps = 200;

  auto writer_fiber = pp_->at(0)->LaunchFiber([&] {
    for (int i = 1; i <= kWriterOps; ++i) {
      std::string doc_key = absl::StrCat("doc:", i);
      std::string content = absl::StrCat("text data item ", i, " for race condition test");
      std::string tags_val = absl::StrCat("tagA,tagB,", (i % 10));
      std::string numeric_field_val = std::to_string(i);
      Run({"hset", doc_key, "content", content, "tags", tags_val, "numeric_field",
           numeric_field_val});
    }
  });

  auto searcher_fiber = pp_->at(1)->LaunchFiber([&] {
    for (int i = 1; i <= kSearcherOps; ++i) {
      int random_val_content = 1 + (i % kWriterOps);
      std::string query_content = absl::StrCat("@content:item", random_val_content);
      Run({"ft.search", kIndexName, query_content});
    }
  });

  auto reindexer_fiber = pp_->at(2)->LaunchFiber([&] {
    for (int i = 1; i <= kReindexerOps; ++i) {
      Run({"ft.create", kIndexName, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "content",
           "TEXT", "SORTABLE", "tags", "TAG", "SORTABLE", "numeric_field", "NUMERIC", "SORTABLE"});
      Run({"ft.dropindex", kIndexName});
    }
  });

  // Join fibers
  writer_fiber.Join();
  searcher_fiber.Join();
  reindexer_fiber.Join();

  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(SearchFamilyTest, IgnoredOptionsInFtCreate) {
  GTEST_SKIP() << "The usage of ignored options is now wrong - it skips supported ones!";

  Run({"HSET", "doc:1", "title", "Test Document"});

  // Create an index with various options, some of which should be ignored
  // INDEXMISSING and INDEXEMPTY are supported by default
  auto resp = Run({"FT.CREATE",
                   "idx",
                   "ON",
                   "HASH",
                   "SCHEMA",
                   "title",
                   "TEXT",
                   "UNF",
                   "NOSTEM",
                   "CASESENSITIVE",
                   "WITHSUFFIXTRIE",
                   "INDEXMISSING",
                   "INDEXEMPTY",
                   "WEIGHT",
                   "1",
                   "SEPARATOR",
                   "|",
                   "PHONETIC",
                   "dm:en",
                   "SORTABLE"});

  // Check that the response is OK, indicating the index was created successfully
  EXPECT_THAT(resp, "OK");

  // Verify that the index was created correctly
  resp = Run({"FT.SEARCH", "idx", "*"});
  EXPECT_THAT(resp, AreDocIds("doc:1"));
}

TEST_F(SearchFamilyTest, JsonDelIndexesBug) {
  auto resp = Run({"JSON.SET", "j1", "$", R"({"text":"some text"})"});
  EXPECT_THAT(resp, "OK");

  resp = Run(
      {"FT.CREATE", "index", "ON", "json", "SCHEMA", "$.text", "AS", "text", "TEXT", "SORTABLE"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"JSON.DEL", "j1", "$.text"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"FT.AGGREGATE", "index", "*", "GROUPBY", "1", "@text"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("text", ArgType(RespExpr::NIL))));
}

TEST_F(SearchFamilyTest, SearchStatsInfoRace) {
  auto index_ops_fiber = pp_->at(0)->LaunchFiber([&] {
    for (int i = 1; i <= 5; ++i) {
      std::string idx_name = absl::StrCat("idx", i);
      std::string prefix = absl::StrCat("prefix", i, ":");
      Run({"FT.CREATE", idx_name, "ON", "HASH", "PREFIX", "1", prefix});
      Run({"FT.DROPINDEX", idx_name});
    }
  });

  auto info_ops_fiber = pp_->at(1)->LaunchFiber([&] {
    for (int i = 1; i <= 10; ++i) {
      Run({"INFO"});
    }
  });

  index_ops_fiber.Join();
  info_ops_fiber.Join();

  ASSERT_FALSE(service_->IsShardSetLocked());
}

TEST_F(SearchFamilyTest, EmptyKeyBug) {
  auto resp = Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "field", "TEXT"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"HSET", "", "field", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, AreDocIds(""));
}

TEST_F(SearchFamilyTest, SetDoesNotUpdateIndexesBug) {
  auto resp = Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "field", "TEXT"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"HSET", "k1", "field", "value"});
  EXPECT_THAT(resp, IntArg(1));

  // Here we are changing the type of k1 from HASH to STRING.
  // This should affect the index, the hset value should not be indexed anymore.
  resp = Run({"SET", "k1", "anothervalue"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"RENAME", "k1", "anotherkey"});
  EXPECT_EQ(resp, "OK");

  /* Here we should see that the value is indexed again.
     We have checks in indexes that prove that the key was not present in the index.
     The bug was, that this check was failing for this operation because it was not removed from the
     index during the SET operation */
  resp = Run({"HSET", "k1", "field", "value"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"FT.SEARCH", "index", "*"});
  EXPECT_THAT(resp, AreDocIds("k1"));
}

TEST_F(SearchFamilyTest, BlockSizeOptionFtCreate) {
  // Create an index with a block size option
  auto resp = Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "number1", "NUMERIC", "BLOCKSIZE",
                   "2", "number2", "NUMERIC", "BLOCKSIZE", "1024"});
  EXPECT_THAT(resp, "OK");

  // Verify that the index was created successfully
  resp = Run({"FT.INFO", "index"});
  EXPECT_THAT(resp, IsArray(_, _, _, _, "attributes",
                            IsUnordArray(IsArray("identifier", "number1", "attribute", "number1",
                                                 "type", "NUMERIC", "blocksize", "2"),
                                         IsArray("identifier", "number2", "attribute", "number2",
                                                 "type", "NUMERIC", "blocksize", "1024")),
                            "num_docs", IntArg(0)));

  // Add a document to the index
  for (int i = 1; i <= 5; ++i) {
    Run({"HSET", absl::StrCat("doc:", i), "number1", std::to_string(i), "number2",
         std::to_string(i * 10)});
  }

  // Search the index
  resp = Run({"FT.SEARCH", "index", "@number1:[1 3] @number2:[10 30]", "SORTBY", "number1", "ASC"});
  EXPECT_THAT(resp, AreDocIds("doc:1", "doc:2", "doc:3"));
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromJoinSimple) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "num2", "NUMERIC"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num3", "NUMERIC", "num4", "NUMERIC"});

  Run({"hset", "k1", "num1", "0", "num2", "1"});
  Run({"hset", "k2", "num1", "1", "num2", "2"});

  Run({"hset", "k3", "num3", "0", "num4", "3"});
  Run({"hset", "k4", "num3", "1", "num4", "4"});

  auto resp = Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.num2", "idx2.num3",
                   "idx2.num4", "LOAD_FROM", "idx2", "1", "idx2.num3=idx1.num1"});

  EXPECT_THAT(resp,
              IsUnordArrayWithSize(
                  IsMap("idx1.num1", "1", "idx1.num2", "2", "idx2.num3", "1", "idx2.num4", "4"),
                  IsMap("idx1.num1", "0", "idx1.num2", "1", "idx2.num3", "0", "idx2.num4", "3")));
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromJoinMultipleJoins) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "str1", "TEXT"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num2", "NUMERIC", "str2", "TAG"});
  Run({"ft.create", "idx3", "ON", "HASH", "SCHEMA", "num3", "NUMERIC", "str3", "TAG"});
  Run({"ft.create", "idx4", "ON", "HASH", "SCHEMA", "num4", "NUMERIC", "str4", "TEXT"});

  Run({"hset", "k1", "num1", "0", "str1", "value1"});
  Run({"hset", "k2", "num1", "1", "str1", "value2"});

  Run({"hset", "k3", "num2", "0", "str2", "value3"});
  Run({"hset", "k4", "num2", "1", "str2", "value4"});

  Run({"hset", "k5", "num3", "2", "str3", "value1"});
  Run({"hset", "k6", "num3", "3", "str3", "value2"});

  Run({"hset", "k7", "num4", "2", "str4", "value3"});
  Run({"hset", "k8", "num4", "3", "str4", "value4"});

  auto resp = Run({"ft.aggregate",
                   "idx1",
                   "*",
                   "LOAD",
                   "8",
                   "idx1.num1",
                   "idx1.str1",
                   "idx2.num2",
                   "idx2.str2",
                   "idx3.num3",
                   "idx3.str3",
                   "idx4.num4",
                   "idx4.str4",
                   "LOAD_FROM",
                   "idx2",
                   "1",
                   "idx2.num2=idx1.num1",
                   "LOAD_FROM",
                   "idx3",
                   "1",
                   "idx3.str3=idx1.str1",
                   "LOAD_FROM",
                   "idx4",
                   "1",
                   "idx4.str4=idx2.str2"});

  EXPECT_THAT(
      resp,
      IsUnordArrayWithSize(
          IsMap("idx1.num1", "1", "idx1.str1", "value2", "idx2.num2", "1", "idx2.str2", "value4",
                "idx3.num3", "3", "idx3.str3", "value2", "idx4.num4", "3", "idx4.str4", "value4"),
          IsMap("idx1.num1", "0", "idx1.str1", "value1", "idx2.num2", "0", "idx2.str2", "value3",
                "idx3.num3", "2", "idx3.str3", "value1", "idx4.num4", "2", "idx4.str4", "value3")));

  // Simple requests
  resp = Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.str1", "idx2.num2",
              "idx2.str2", "LOAD_FROM", "idx2", "1", "idx2.num2=idx1.num1"});
  EXPECT_THAT(
      resp,
      IsUnordArrayWithSize(
          IsMap("idx1.num1", "1", "idx1.str1", "value2", "idx2.num2", "1", "idx2.str2", "value4"),
          IsMap("idx1.num1", "0", "idx1.str1", "value1", "idx2.num2", "0", "idx2.str2", "value3")));

  resp = Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.str1", "idx3.num3",
              "idx3.str3", "LOAD_FROM", "idx3", "1", "idx3.str3=idx1.str1"});
  EXPECT_THAT(
      resp,
      IsUnordArrayWithSize(
          IsMap("idx1.num1", "1", "idx1.str1", "value2", "idx3.num3", "3", "idx3.str3", "value2"),
          IsMap("idx1.num1", "0", "idx1.str1", "value1", "idx3.num3", "2", "idx3.str3", "value1")));

  resp = Run({"ft.aggregate", "idx2", "*", "LOAD", "4", "idx2.num2", "idx2.str2", "idx4.num4",
              "idx4.str4", "LOAD_FROM", "idx4", "1", "idx4.str4=idx2.str2"});
  EXPECT_THAT(
      resp,
      IsUnordArrayWithSize(
          IsMap("idx2.num2", "1", "idx2.str2", "value4", "idx4.num4", "3", "idx4.str4", "value4"),
          IsMap("idx2.num2", "0", "idx2.str2", "value3", "idx4.num4", "2", "idx4.str4", "value3")));

  resp = Run({"ft.aggregate", "idx3", "*", "LOAD", "4", "idx3.num3", "idx3.str3", "idx4.num4",
              "idx4.str4", "LOAD_FROM", "idx4", "1", "idx3.num3=idx4.num4"});
  EXPECT_THAT(
      resp,
      IsUnordArrayWithSize(
          IsMap("idx3.num3", "3", "idx3.str3", "value2", "idx4.num4", "3", "idx4.str4", "value4"),
          IsMap("idx3.num3", "2", "idx3.str3", "value1", "idx4.num4", "2", "idx4.str4", "value3")));
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromMultipleFields) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "str1", "TEXT", "num2",
       "NUMERIC"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num2", "NUMERIC", "str2", "TAG", "num3",
       "NUMERIC"});
  Run({"ft.create", "idx3", "ON", "HASH", "SCHEMA", "num3", "NUMERIC", "str3", "TEXT", "num4",
       "NUMERIC"});

  Run({"hset", "k1", "num1", "0", "str1", "value1", "num2", "5"});
  Run({"hset", "k2", "num1", "1", "str1", "value2", "num2", "10"});

  Run({"hset", "k3", "num2", "1", "str2", "value3", "num3", "10"});
  Run({"hset", "k4", "num2", "0", "str2", "value4", "num3", "5"});

  Run({"hset", "k5", "num3", "2", "str3", "value4", "num4", "5"});
  Run({"hset", "k6", "num3", "3", "str3", "value3", "num4", "10"});

  auto resp = Run({"ft.aggregate",
                   "idx1",
                   "*",
                   "LOAD",
                   "9",
                   "idx1.num1",
                   "idx1.str1",
                   "idx1.num2",
                   "idx2.num2",
                   "idx2.str2",
                   "idx2.num3",
                   "idx3.num3",
                   "idx3.str3",
                   "idx3.num4",
                   "LOAD_FROM",
                   "idx2",
                   "2",
                   "idx1.num1=idx2.num2",
                   "idx1.num2=idx2.num3",
                   "LOAD_FROM",
                   "idx3",
                   "3",
                   "idx1.num2=idx3.num4",
                   "idx2.num3=idx3.num4",
                   "idx2.str2=idx3.str3"});

  EXPECT_THAT(
      resp, IsUnordArrayWithSize(IsMap("idx1.num1", "1", "idx1.str1", "value2", "idx1.num2", "10",
                                       "idx2.num2", "1", "idx2.str2", "value3", "idx2.num3", "10",
                                       "idx3.num3", "3", "idx3.str3", "value3", "idx3.num4", "10"),
                                 IsMap("idx1.num1", "0", "idx1.str1", "value1", "idx1.num2", "5",
                                       "idx2.num2", "0", "idx2.str2", "value4", "idx2.num3", "5",
                                       "idx3.num3", "2", "idx3.str3", "value4", "idx3.num4", "5")));
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromSeveralCopiesOfSameKey) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "str1", "TEXT", "num2",
       "NUMERIC"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num2", "NUMERIC", "str2", "TAG", "num3",
       "NUMERIC"});
  Run({"ft.create", "idx3", "ON", "HASH", "SCHEMA", "num3", "NUMERIC", "str3", "TEXT", "num4",
       "NUMERIC"});

  Run({"hset", "k1", "num1", "0", "str1", "value1", "num2", "5"});
  Run({"hset", "k2", "num1", "1", "str1", "value2", "num2", "10"});

  Run({"hset", "k3", "num2", "1", "str2", "value3", "num3", "10"});
  Run({"hset", "k4", "num2", "0", "str2", "value4", "num3", "5"});

  Run({"hset", "k5", "num3", "2", "str3", "value1", "num4", "15"});
  Run({"hset", "k6", "num3", "3", "str3", "value1", "num4", "20"});
  Run({"hset", "k7", "num3", "4", "str3", "value2", "num4", "25"});
  Run({"hset", "k8", "num3", "5", "str3", "value2", "num4", "30"});

  auto resp = Run({"ft.aggregate",
                   "idx1",
                   "*",
                   "LOAD",
                   "9",
                   "idx1.num1",
                   "idx1.str1",
                   "idx1.num2",
                   "idx2.num2",
                   "idx2.str2",
                   "idx2.num3",
                   "idx3.num3",
                   "idx3.str3",
                   "idx3.num4",
                   "LOAD_FROM",
                   "idx2",
                   "2",
                   "idx1.num1=idx2.num2",
                   "idx1.num2=idx2.num3",
                   "LOAD_FROM",
                   "idx3",
                   "1",  // Multiple copies of the same key
                   "idx1.str1=idx3.str3"});

  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("idx1.num1", "0", "idx1.str1", "value1", "idx1.num2", "5",
                              "idx2.num2", "0", "idx2.str2", "value4", "idx2.num3", "5",
                              "idx3.num3", "2", "idx3.str3", "value1", "idx3.num4", "15"),
                        IsMap("idx1.num1", "0", "idx1.str1", "value1", "idx1.num2", "5",
                              "idx2.num2", "0", "idx2.str2", "value4", "idx2.num3", "5",
                              "idx3.num3", "3", "idx3.str3", "value1", "idx3.num4", "20"),
                        IsMap("idx1.num1", "1", "idx1.str1", "value2", "idx1.num2", "10",
                              "idx2.num2", "1", "idx2.str2", "value3", "idx2.num3", "10",
                              "idx3.num3", "4", "idx3.str3", "value2", "idx3.num4", "25"),
                        IsMap("idx1.num1", "1", "idx1.str1", "value2", "idx1.num2", "10",
                              "idx2.num2", "1", "idx2.str2", "value3", "idx2.num3", "10",
                              "idx3.num3", "5", "idx3.str3", "value2", "idx3.num4", "30")));
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromNoMatches) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "str1", "TEXT"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num2", "NUMERIC", "str2", "TEXT"});

  Run({"hset", "k1", "num1", "0", "str1", "value1"});
  Run({"hset", "k2", "num1", "1", "str1", "value2"});

  Run({"hset", "k3", "num2", "0", "str2", "value3"});
  Run({"hset", "k4", "num2", "1", "str2", "value4"});

  auto resp =
      Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.str1", "idx2.num2",
           "idx2.str2", "LOAD_FROM", "idx2", "2", "idx2.num2=idx1.num1", "idx2.str2=idx1.str1"});

  EXPECT_THAT(resp, IntArg(0));  // No matches, so result should be empty
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromQueries) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "str1", "TAG"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num2", "NUMERIC", "str2", "TEXT"});

  std::vector<::testing::Matcher<RespExpr>> matchers;
  for (int i = 0; i < 100; ++i) {
    // For even i str1 and str2 should match, for odd i they should not
    std::string str1 = absl::StrCat("tag", i);
    std::string str2 = i % 2 == 0 ? str1 : absl::StrCat("text", i);
    Run({"hset", absl::StrCat("k1:", i), "num1", std::to_string(i), "str1", str1});
    Run({"hset", absl::StrCat("k2:", i), "num2", std::to_string(i), "str2", str2});

    if (i % 2 == 0 && i >= 35 && i <= 57) {
      matchers.emplace_back(IsMap("idx1.num1", std::to_string(i), "idx1.str1", str1, "idx2.num2",
                                  std::to_string(i), "idx2.str2", str2));
    }
  }
  matchers.insert(matchers.begin(), IntArg(matchers.size()));

  auto resp = Run({"ft.aggregate", "idx1", "@num1:[35 57]", "LOAD", "4", "idx1.num1", "idx1.str1",
                   "idx2.num2", "idx2.str2", "LOAD_FROM", "idx2", "1", "idx2.str2=idx1.str1",
                   "QUERY", "@num2:[35 57]"});

  EXPECT_THAT(resp.GetVec(), UnorderedElementsAreArray(matchers));

  // Another case
  Run({"ft.create", "idx3", "ON", "HASH", "SCHEMA", "num3", "NUMERIC", "str3", "TAG"});
  Run({"ft.create", "idx4", "ON", "HASH", "SCHEMA", "num4", "NUMERIC", "str4", "TAG"});

  size_t num3 = 1;
  size_t num4 = 5;

  std::vector<std::string> tag_values = {"tag1", "tag2", "tag3", "tag4"};
  matchers.clear();
  for (size_t i = 0; i < 100; ++i) {
    std::string str = tag_values[i % tag_values.size()];
    const size_t num3_actual = i * 100 + num3;
    const size_t num4_actual = i * 100 + num4;

    Run({"hset", absl::StrCat("k3:", i), "num3", std::to_string(num3_actual), "str3", str});
    Run({"hset", absl::StrCat("k4:", i), "num4", std::to_string(num4_actual), "str4", str});

    if ((str == "tag1" || str == "tag4") && num3 == num4) {
      matchers.emplace_back(IsMap("idx3.num3", std::to_string(num3_actual), "idx3.str3", str,
                                  "idx4.num4", std::to_string(num4_actual), "idx4.str4", str));
    }

    num3 = (num3 + 3) % 12;
    num4 = (num4 + 7) % 12;
  }
  DCHECK(!matchers.empty());
  matchers.insert(matchers.begin(), IntArg(matchers.size()));

  resp = Run({"ft.aggregate", "idx3", "@str3:{tag1|tag4}", "LOAD", "4", "idx3.num3", "idx3.str3",
              "idx4.num4", "idx4.str4", "LOAD_FROM", "idx4", "1", "idx4.num4=idx3.num3", "QUERY",
              "@str4:{tag1|tag4}"});
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAreArray(matchers));
}

TEST_F(SearchFamilyTest, AggregateWithLoadFromSyntaxErrors) {
  Run({"ft.create", "idx1", "ON", "HASH", "SCHEMA", "num1", "NUMERIC", "str1", "TEXT"});
  Run({"ft.create", "idx2", "ON", "HASH", "SCHEMA", "num2", "NUMERIC", "str2", "TEXT"});
  Run({"ft.create", "idx3", "ON", "HASH", "SCHEMA", "num3", "NUMERIC", "str3", "TEXT"});

  Run({"hset", "k1", "num1", "0", "str1", "str"});
  Run({"hset", "k2", "num2", "0", "str2", "str"});
  Run({"hset", "k3", "num3", "0", "str3", "str"});

  // Test when index does not exist
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM",
                   "idx4", "1", "idx4.num2=idx1.num1"}),
              IntArg(0));

  // Test when index exists but no LOAD_FROM is specified
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM",
                   "idx3", "1", "idx3.num3=idx2.num2"}),
              ErrArg("bad arguments for LOAD_FROM: unknown index 'idx2'"));

  // Test when index exists but was specified after it was used
  EXPECT_THAT(
      Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM", "idx2",
           "1", "idx2.num2=idx3.num3", "LOAD_FROM", "idx3", "1", "idx3.str3=idx1.str1"}),
      ErrArg("bad arguments for LOAD_FROM: unknown index 'idx3'"));

  // Test when LOAD_FROM is not using fields of current index
  EXPECT_THAT(
      Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM", "idx2",
           "1", "idx2.str2=idx1.str1", "LOAD_FROM", "idx3", "1", "idx2.str2=idx1.str1"}),
      ErrArg("bad arguments for LOAD_FROM: one of the field must be from the current index 'idx3'. "
             "Got 'idx2.str2' and 'idx1.str1'"));

  // Test when field of index does not exist
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM",
                   "idx2", "1", "idx2.num2=idx1.nonexistent_field"}),
              IntArg(0));
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM",
                   "idx2", "1", "idx2.nonexistent_field=idx1.num1"}),
              IntArg(0));

  // Test when field in QUERY does not exist in index
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.str1", "LOAD_FROM",
                   "idx2", "1", "idx2.num2=idx1.num1", "QUERY", "@nonexistent_tag:{tag1|tag2}"}),
              IntArg(0));

  // Test when field in LOAD does not exist in index
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "2", "idx1.num1", "idx1.non_existent_field",
                   "LOAD_FROM", "idx2", "1", "idx2.num2=idx1.num1"}),
              IsUnordArrayWithSize(
                  IsMap("idx1.num1", "0", "idx1.non_existent_field", ArgType(RespExpr::NIL))));

  // Test index aliases
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.str1", "alias.num2",
                   "alias.str2", "LOAD_FROM", "idx2", "AS", "alias", "1", "alias.num2=idx1.num1"}),
              IsUnordArrayWithSize(IsMap("idx1.num1", "0", "idx1.str1", "str", "alias.num2", "0",
                                         "alias.str2", "str")));
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.str1", "idx2.num2",
                   "idx2.str2", "LOAD_FROM", "idx2", "AS", "alias", "1", "alias.num2=idx1.num1"}),
              ErrArg("Unknown index alias 'idx2' in the LOAD option. Field: 'num2'"));

  // Test same index used multiple times
  EXPECT_THAT(Run({"ft.aggregate", "idx1", "*", "LOAD", "4", "idx1.num1", "idx1.str1", "idx2.num2",
                   "idx2.str2", "LOAD_FROM", "idx2", "1", "idx2.num2=idx1.num1", "LOAD_FROM",
                   "idx2", "1", "idx2.str2=idx1.str1"}),
              ErrArg("Duplicate index alias in LOAD_FROM: 'idx2'"));
}

}  // namespace dfly
