// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

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

TEST_F(SearchFamilyTest, TestReturn) {
  auto floatsv = [](const float* f) -> string_view {
    return {reinterpret_cast<const char*>(f), sizeof(float)};
  };

  for (unsigned i = 0; i < 20; i++) {
    const float score = i;
    Run({"hset", "k"s + to_string(i), "longA", to_string(i), "longB", to_string(i + 1), "longC",
         to_string(i + 2), "secret", to_string(i + 3), "vector", floatsv(&score)});
  }

  Run({"ft.create", "i1",     "SCHEMA", "longA",   "AS",    "justA", "TEXT",
       "longB",     "AS",     "justB",  "NUMERIC", "longC", "AS",    "justC",
       "NUMERIC",   "vector", "VECTOR", "FLAT",    "2",     "DIM",   "1"});

  auto MatchEntry = [](string key, auto... fields) { return IsMapWithSize(key, IsMap(fields...)); };

  // Check all fields are returned
  auto resp = Run({"ft.search", "i1", "@justA:0"});
  EXPECT_THAT(resp, MatchEntry("k0", "longA", "0", "longB", "1", "longC", "2", "secret", "3",
                               "vector", "[0]"));

  // Check no fields are returned
  resp = Run({"ft.search", "i1", "@justA:0", "return", "0"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), "k0")));

  resp = Run({"ft.search", "i1", "@justA:0", "nocontent"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), "k0")));

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
              "DESC", "RETURN", "1", "longA", "PARAMS", "2", "vector", floatsv(&score)});
  EXPECT_THAT(resp, MatchEntry("k0", "longA", "0"));

  // Check sort doesn't shadow knn return alias
  score = 20;
  resp = Run({"ft.search", "i1", "@justA:0 => [KNN 20 @vector $vector AS vec_return]", "SORTBY",
              "vec_return", "DESC", "RETURN", "1", "vec_return", "PARAMS", "2", "vector",
              floatsv(&score)});
  EXPECT_THAT(resp, MatchEntry("k0", "vec_return", "20"));
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

TEST_F(SearchFamilyTest, BasicSort) {
  auto AreRange = [](size_t total, size_t l, size_t r, string_view prefix) {
    vector<string> out;
    for (size_t i = min(l, r); i < max(l, r); i++)
      out.push_back(absl::StrCat(prefix, i));
    if (l > r)
      reverse(out.begin(), out.end());
    return DocIds(total, out);
  };

  // max_memory_limit = INT_MAX;

  Run({"ft.create", "i1", "prefix", "1", "d:", "schema", "ord", "numeric", "sortable"});

  for (size_t i = 0; i < 100; i++)
    Run({"hset", absl::StrCat("d:", i), "ord", absl::StrCat(i)});

  // Sort ranges of 23 elements
  for (size_t i = 0; i < 77; i++)
    EXPECT_THAT(Run({"ft.search", "i1", "*", "SORTBY", "ord", "LIMIT", to_string(i), "23"}),
                AreRange(100, i, i + 23, "d:"));

  // Sort ranges of 27 elements in reverse
  for (size_t i = 0; i < 73; i++)
    EXPECT_THAT(Run({"ft.search", "i1", "*", "SORTBY", "ord", "DESC", "LIMIT", to_string(i), "27"}),
                AreRange(100, 100 - i, 100 - i - 27, "d:"));

  Run({"ft.create", "i2", "prefix", "1", "d2:", "schema", "name", "text", "sortable"});

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
    EXPECT_THAT(tree[0].GetString(), HasSubstr("Logical{n=3,o=and}"sv));
    EXPECT_EQ(tree[1].GetVec().size(), 3);
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
  ;

  auto resp = Run({"ft.profile", "i1", "not_search", "query", "(a | b) c d"});
  EXPECT_THAT(resp, ErrArg("no `SEARCH` or `AGGREGATE` provided"));

  resp = Run({"ft.profile", "i1", "search", "not_query", "(a | b) c d"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

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
  Run({"HSET", "h1", "group", "first", "value", "1"});
  Run({"HSET", "h2", "group", "second", "value", "2"});
  Run({"HSET", "h3", "group", "first", "value", "3"});

  auto resp =
      Run({"FT.CREATE", "index", "ON", "HASH", "SCHEMA", "group", "TAG", "value", "NUMERIC"});
  EXPECT_EQ(resp, "OK");

  // TODO: Throw an error when no '@' is provided in the GROUPBY option
  resp = Run({"FT.AGGREGATE", "index", "*", "GROUPBY", "1", "group", "REDUCE", "COUNT", "0", "AS",
              "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("count", "2", "group", "first"),
                                         IsMap("group", "second", "count", "1")));
}

TEST_F(SearchFamilyTest, AggregateGroupByReduceSort) {
  for (size_t i = 0; i < 101; i++) {  // 51 even, 50 odd
    Run({"hset", absl::StrCat("k", i), "even", (i % 2 == 0) ? "true" : "false", "value",
         absl::StrCat(i)});
  }
  Run({"ft.create", "i1", "schema", "even", "tag", "sortable", "value", "numeric", "sortable"});

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

TEST_F(SearchFamilyTest, SearchWithLoadOptionHard) {
  // Test HASH
  Run({"HSET", "h1", "a", "1", "b", "one", "first", "h1"});
  Run({"HSET", "h2", "a", "2", "b", "two", "second", "h2"});
  Run({"HSET", "h3", "a", "1", "b", "one", "third", "h3"});

  auto resp = Run({"FT.CREATE", "i1", "ON", "HASH", "SCHEMA", "@a", "TAG"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "@b"});
  EXPECT_THAT(resp, IsMapWithSize("h1", IsMap("a", "1", "b", "one", "first", "h1"), "h2",
                                  IsMap("a", "2", "b", "two", "second", "h2"), "h3",
                                  IsMap("a", "1", "b", "one", "third", "h3")));

  // Test JSON
  Run({"JSON.SET", "j1", ".", R"({"a":"1","b":"one","first":"j1"})"});
  Run({"JSON.SET", "j2", ".", R"({"a":"2","b":"two","second":"j2"})"});
  Run({"JSON.SET", "j3", ".", R"({"a":"1","b":"one","third":"j3"})"});

  resp = Run({"FT.CREATE", "i2", "ON", "JSON", "SCHEMA", "$.a", "AS", "a", "TAG"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "$.b", "AS", "b"});
  EXPECT_THAT(
      resp, IsMapWithSize("j1", IsMap("b", "\"one\"", "$", R"({"a":"1","b":"one","first":"j1"})"),
                          "j2", IsMap("b", "\"two\"", "$", R"({"a":"2","b":"two","second":"j2"})"),
                          "j3", IsMap("b", "\"one\"", "$", R"({"a":"1","b":"one","third":"j3"})")));
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
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("foo_total", "20", "word", "\"item2\"", "text", "\"second key\""),
                        IsMap("foo_total", "10", "word", "\"item1\"", "text", "\"first key\"")));

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "1", "@word", "GROUPBY", "1", "@word", "REDUCE",
              "SUM", "1", "@foo", "AS", "foo_total"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("foo_total", "20", "word", "\"item2\""),
                                         IsMap("foo_total", "10", "word", "\"item1\"")));
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

  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "$.another_value"});
  EXPECT_THAT(
      resp, IsMapWithSize("j2", IsMap("$", R"({"value":1})"), "j4",
                          IsMap("$", R"({"another_value":2,"value":2})", "$.another_value", "2")));

  resp = Run({"FT.AGGREGATE", "i2", "*", "LOAD", "2", "$.value", "$.another_value", "GROUPBY", "2",
              "$.value", "$.another_value", "REDUCE", "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(
                  IsMap("$.value", "1", "$.another_value", ArgType(RespExpr::NIL), "count", "1"),
                  IsMap("$.value", "2", "$.another_value", "2", "count", "1")));

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

#ifndef SANITIZERS
TEST_F(SearchFamilyTest, SearchLoadReturnJson) {
  Run({"JSON.SET", "j1", ".", R"({"a":"one"})"});
  Run({"JSON.SET", "j2", ".", R"({"a":"two"})"});

  auto resp = Run({"FT.CREATE", "i1", "ON", "JSON", "SCHEMA", "$.a", "AS", "a", "TEXT"});
  EXPECT_EQ(resp, "OK");

  // Search with RETURN $.a
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "$.a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("$.a", "\"one\""), "j2", IsMap("$.a", "\"two\"")));

  // Search with RETURN a
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("a", "\"one\""), "j2", IsMap("a", "\"two\"")));

  // Search with RETURN @a
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap(), "j2", IsMap()));

  // Search with RETURN $.a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "$.a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("vvv", "\"one\""), "j2", IsMap("vvv", "\"two\"")));

  // Search with RETURN a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("vvv", "\"one\""), "j2", IsMap("vvv", "\"two\"")));

  // Search with RETURN @a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "@a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap(), "j2", IsMap()));

  // Search with LOAD $.a
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "$.a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("$.a", "\"one\"", "$", R"({"a":"one"})"), "j2",
                                  IsMap("$.a", "\"two\"", "$", R"({"a":"two"})")));

  // Search with LOAD a
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("a", "\"one\"", "$", R"({"a":"one"})"), "j2",
                                  IsMap("a", "\"two\"", "$", R"({"a":"two"})")));

  // Search with LOAD @a
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("a", "\"one\"", "$", R"({"a":"one"})"), "j2",
                                  IsMap("a", "\"two\"", "$", R"({"a":"two"})")));

  // Search with LOAD $.a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "$.a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("$", R"({"a":"one"})", "vvv", "\"one\""), "j2",
                                  IsMap("$", R"({"a":"two"})", "vvv", "\"two\"")));

  // Search with LOAD a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("$", R"({"a":"one"})", "vvv", "\"one\""), "j2",
                                  IsMap("$", R"({"a":"two"})", "vvv", "\"two\"")));

  // Search with LOAD @a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "@a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("$", R"({"a":"one"})", "vvv", "\"one\""), "j2",
                                  IsMap("$", R"({"a":"two"})", "vvv", "\"two\"")));

  /* Test another name */

  resp = Run({"FT.CREATE", "i2", "ON", "JSON", "SCHEMA", "$.a", "AS", "nnn", "TEXT"});
  EXPECT_EQ(resp, "OK");

  // Search with RETURN nnn
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "nnn"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("nnn", "\"one\""), "j2", IsMap("nnn", "\"two\"")));

  // Search with RETURN @nnn
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "@nnn"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap(), "j2", IsMap()));

  // Search with RETURN a
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap(), "j2", IsMap()));

  // Search with RETURN @a
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap(), "j2", IsMap()));

  // Search with LOAD nnn
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "nnn"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("nnn", "\"one\"", "$", R"({"a":"one"})"), "j2",
                                  IsMap("nnn", "\"two\"", "$", R"({"a":"two"})")));

  // Search with LOAD @nnn
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "@nnn"});
  EXPECT_THAT(resp, IsMapWithSize("j1", IsMap("nnn", "\"one\"", "$", R"({"a":"one"})"), "j2",
                                  IsMap("nnn", "\"two\"", "$", R"({"a":"two"})")));

  // Search with LOAD a
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "a"});
  EXPECT_THAT(
      resp, IsMapWithSize("j1", IsMap("$", R"({"a":"one"})"), "j2", IsMap("$", R"({"a":"two"})")));

  // Search with LOAD @a
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "@a"});
  EXPECT_THAT(
      resp, IsMapWithSize("j1", IsMap("$", R"({"a":"one"})"), "j2", IsMap("$", R"({"a":"two"})")));
}
#endif

TEST_F(SearchFamilyTest, SearchLoadReturnHash) {
  Run({"HSET", "h1", "a", "one"});
  Run({"HSET", "h2", "a", "two"});

  auto resp = Run({"FT.CREATE", "i1", "ON", "HASH", "SCHEMA", "a", "TEXT"});
  EXPECT_EQ(resp, "OK");

  // Search with RETURN $.a
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "$.a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap(), "h1", IsMap()));

  // Search with RETURN a
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with RETURN @a
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap(), "h1", IsMap()));

  // Search with RETURN $.a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "$.a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap(), "h1", IsMap()));

  // Search with RETURN a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("vvv", "two"), "h1", IsMap("vvv", "one")));

  // Search with RETURN @a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "RETURN", "1", "@a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap(), "h1", IsMap()));

  // Search with LOAD $.a
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "$.a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with LOAD a
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with LOAD @a
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with LOAD $.a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "$.a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with LOAD a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("vvv", "two", "a", "two"), "h1",
                                  IsMap("vvv", "one", "a", "one")));

  // Search with LOAD @a AS vvv
  resp = Run({"FT.SEARCH", "i1", "*", "LOAD", "1", "@a", "AS", "vvv"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("vvv", "two", "a", "two"), "h1",
                                  IsMap("vvv", "one", "a", "one")));

  /* Test another name */

  resp = Run({"FT.CREATE", "i2", "ON", "HASH", "SCHEMA", "a", "AS", "nnn", "TEXT"});
  EXPECT_EQ(resp, "OK");

  // Search with RETURN nnn
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "nnn"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("nnn", "two"), "h1", IsMap("nnn", "one")));

  // Search with RETURN @nnn
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "@nnn"});
  EXPECT_THAT(resp, IsMapWithSize("h1", IsMap(), "h2", IsMap()));

  // Search with RETURN a
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with RETURN @a
  resp = Run({"FT.SEARCH", "i2", "*", "RETURN", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("h1", IsMap(), "h2", IsMap()));

  // Search with LOAD nnn
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "nnn"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("nnn", "two", "a", "two"), "h1",
                                  IsMap("nnn", "one", "a", "one")));

  // Search with LOAD @nnn
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "@nnn"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("nnn", "two", "a", "two"), "h1",
                                  IsMap("nnn", "one", "a", "one")));

  // Search with LOAD a
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));

  // Search with LOAD @a
  resp = Run({"FT.SEARCH", "i2", "*", "LOAD", "1", "@a"});
  EXPECT_THAT(resp, IsMapWithSize("h2", IsMap("a", "two"), "h1", IsMap("a", "one")));
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

  resp = Run({"FT.AGGREGATE", "i1", "*", "SORTBY", "1", "a"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("a", "1"), IsMap("a", "4"), IsMap("a", "7")));

  resp = Run({"FT.AGGREGATE", "i1", "*", "LOAD", "1", "@b", "SORTBY", "1", "a"});
  EXPECT_THAT(resp,
              IsUnordArrayWithSize(IsMap("b", "\"2\"", "a", "1"), IsMap("b", "\"5\"", "a", "4"),
                                   IsMap("b", "\"8\"", "a", "7")));

  resp = Run({"FT.AGGREGATE", "i1", "*", "SORTBY", "1", "a", "GROUPBY", "2", "@b", "@a", "REDUCE",
              "COUNT", "0", "AS", "count"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(IsMap("b", "\"8\"", "a", "7", "count", "1"),
                                         IsMap("b", "\"2\"", "a", "1", "count", "1"),
                                         IsMap("b", "\"5\"", "a", "4", "count", "1")));

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
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("name", "\"third\"", "number", "300"),
                        IsMap("name", "\"sixth\"", "number", "300"),
                        IsMap("name", "\"seventh\"", "number", "400"),
                        IsMap("name", "\"second\"", "number", "800"), IsMap("name", "\"ninth\""),
                        IsMap("name", "\"fourth\"", "number", "400"),
                        IsMap("name", "\"first\"", "number", "1200"),
                        IsMap("name", "\"fifth\"", "number", "900"), IsMap("name", "\"eighth\"")));

  // Test sorting by name (ASC) and number (DESC)
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "4", "@name", "ASC", "@number", "DESC"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("name", "\"eighth\""), IsMap("name", "\"fifth\"", "number", "900"),
                        IsMap("name", "\"first\"", "number", "1200"),
                        IsMap("name", "\"fourth\"", "number", "400"), IsMap("name", "\"ninth\""),
                        IsMap("name", "\"second\"", "number", "800"),
                        IsMap("name", "\"seventh\"", "number", "400"),
                        IsMap("name", "\"sixth\"", "number", "300"),
                        IsMap("name", "\"third\"", "number", "300")));

  // Test sorting by group (ASC), number (DESC), and name
  resp = Run(
      {"FT.AGGREGATE", "index", "*", "SORTBY", "5", "@group", "ASC", "@number", "DESC", "@name"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("group", "\"first\"", "number", "1200", "name", "\"first\""),
                        IsMap("group", "\"first\"", "number", "800", "name", "\"second\""),
                        IsMap("group", "\"first\"", "number", "300", "name", "\"sixth\""),
                        IsMap("group", "\"first\"", "number", "300", "name", "\"third\""),
                        IsMap("group", "\"first\"", "name", "\"eighth\""),
                        IsMap("group", "\"second\"", "number", "900", "name", "\"fifth\""),
                        IsMap("group", "\"second\"", "number", "400", "name", "\"fourth\""),
                        IsMap("group", "\"second\"", "number", "400", "name", "\"seventh\""),
                        IsMap("group", "\"second\"", "name", "\"ninth\"")));

  // Test sorting by number (ASC), group (DESC), and name
  resp = Run(
      {"FT.AGGREGATE", "index", "*", "SORTBY", "5", "@number", "ASC", "@group", "DESC", "@name"});
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("number", "300", "group", "\"first\"", "name", "\"sixth\""),
                        IsMap("number", "300", "group", "\"first\"", "name", "\"third\""),
                        IsMap("number", "400", "group", "\"second\"", "name", "\"fourth\""),
                        IsMap("number", "400", "group", "\"second\"", "name", "\"seventh\""),
                        IsMap("number", "800", "group", "\"first\"", "name", "\"second\""),
                        IsMap("number", "900", "group", "\"second\"", "name", "\"fifth\""),
                        IsMap("number", "1200", "group", "\"first\"", "name", "\"first\""),
                        IsMap("group", "\"second\"", "name", "\"ninth\""),
                        IsMap("group", "\"first\"", "name", "\"eighth\"")));

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
  EXPECT_THAT(resp, IsUnordArrayWithSize(
                        IsMap("name", "\"eighth\""), IsMap("name", "\"fifth\"", "number", "900"),
                        IsMap("name", "\"first\"", "number", "1200"),
                        IsMap("name", "\"fourth\"", "number", "400"), IsMap("name", "\"ninth\""),
                        IsMap("name", "\"second\"", "number", "800"),
                        IsMap("name", "\"seventh\"", "number", "400"),
                        IsMap("name", "\"sixth\"", "number", "300"),
                        IsMap("name", "\"third\"", "number", "300")));

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
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  // Test MAX with invalid value
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@name", "MAX", "-10"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  // Test MAX without a value
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@name", "MAX"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Test SORTBY with a non-existing field
  /* Temporary unsupported
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "1", "@nonexistingfield"});
  EXPECT_THAT(resp, ErrArg("Property `nonexistingfield` not loaded nor in schema")); */

  // Test SORTBY with an invalid value
  resp = Run({"FT.AGGREGATE", "index", "*", "SORTBY", "notvalue", "@name"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));
}

}  // namespace dfly
