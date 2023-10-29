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
  EXPECT_THAT(res.GetVec()[2], RespArray(ElementsAre("max-score", "15")));

  // Test invalid json path expression omits that field
  res = Run({"ft.search", "i1", "@name:alex", "return", "1", "::??INVALID??::", "as", "retval"});
  EXPECT_EQ(res.GetVec()[1], "k1");
  EXPECT_THAT(res.GetVec()[2], RespArray(ElementsAre()));
}
#endif

TEST_F(SearchFamilyTest, Tags) {
  Run({"hset", "d:1", "color", "red, green"});
  Run({"hset", "d:2", "color", "green, blue"});
  Run({"hset", "d:3", "color", "blue, red"});
  Run({"hset", "d:4", "color", "red"});
  Run({"hset", "d:5", "color", "green"});
  Run({"hset", "d:6", "color", "blue"});

  EXPECT_EQ(Run({"ft.create", "i1", "on", "hash", "schema", "color", "tag"}), "OK");

  // Tags don't participate in full text search
  EXPECT_THAT(Run({"ft.search", "i1", "red"}), kNoResults);

  EXPECT_THAT(Run({"ft.search", "i1", "@color:{ red }"}), AreDocIds("d:1", "d:3", "d:4"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{green}"}), AreDocIds("d:1", "d:2", "d:5"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue}"}), AreDocIds("d:2", "d:3", "d:6"));

  EXPECT_THAT(Run({"ft.search", "i1", "@color:{red | green}"}),
              AreDocIds("d:1", "d:2", "d:3", "d:4", "d:5"));
  EXPECT_THAT(Run({"ft.search", "i1", "@color:{blue | green}"}),
              AreDocIds("d:1", "d:2", "d:3", "d:5", "d:6"));
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

  // TODO: Check on new algo
  // EXPECT_THAT(Run({"ft.search", "i1", "-@i:[0 9] -@j:[1 10]"}), AreDocIds("i10j0"));

  /*
  TODO: Breaks the parser
  EXPECT_THAT(Run({"ft.search", "i1", "(@i:[1 3] ! @i:[2 2]) @j:[7 7]"}),
              DocIds(vector<string>{"i1j7", "i3j7"}));
  */
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

  auto MatchEntry = [](string key, auto... fields) {
    return RespArray(ElementsAre(IntArg(1), key, IsUnordArray(fields...)));
  };

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
  EXPECT_THAT(resp, MatchEntry("k0", "nothere", ""));

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
  EXPECT_THAT(resp.GetVec()[2].GetVec(),
              UnorderedElementsAre("visits", "100", "title", "πανίσχυρη ΛΙΒΕΛΛΟΎΛΗ Δίας"));
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

  const auto& top_level = resp.GetVec();
  EXPECT_EQ(top_level.size(), shard_set->size() + 1);

  EXPECT_THAT(top_level[0].GetVec(),
              ElementsAre("took", _, "hits", _, "serialized", _, "cutoff", _, "hops", _));

  for (size_t sid = 0; sid < shard_set->size(); sid++) {
    const auto& shard_resp = top_level[sid + 1].GetVec();
    EXPECT_THAT(shard_resp, ElementsAre("took", _, "tree", _));

    const auto& tree = shard_resp[3].GetVec();
    EXPECT_THAT(tree[0].GetString(), HasSubstr("Logical{n=3,o=and}"sv));
    EXPECT_EQ(tree[1].GetVec().size(), 3);
  }
}

vector<vector<string>> FillShard(ShardId sid, string_view prefix, size_t num, size_t idx = 0) {
  vector<vector<string>> out;
  size_t entries = 0;
  while (entries < num) {
    auto key = absl::StrCat(prefix, idx++);
    if (Shard(key, shard_set->size()) == sid) {
      out.emplace_back(vector<string>{"hset", key, "idx", to_string(idx)});
      entries++;
    }
  }
  return out;
}

// Check basic multi shard search cuts off a big portion of the results
TEST_F(SearchFamilyTest, MultiShardBalanced) {
  Run({"ft.create", "i1", "schema", "idx", "numeric"});

  // Fill two shards with 100 values each
  for (auto cmd : FillShard(0, "doc0-", 100))
    Run(absl::MakeSpan(cmd));
  for (auto cmd : FillShard(1, "doc1-", 100))
    Run(absl::MakeSpan(cmd));

  auto resp = Run({"ft.profile", "i1", "SEARCH", "QUERY", "*", "LIMIT", "0", "50"});

  auto stats = resp.GetVec()[0].GetVec();

  // Make sure no refill was needed
  EXPECT_EQ(stats[8], "hops");
  EXPECT_THAT(stats[9], IntArg(1));

  // Make sure at least around half of serialization was cut off
  EXPECT_EQ(stats[4], "serialized");
  EXPECT_LE(stats[5].GetInt(), 55);
  EXPECT_EQ(stats[6], "cutoff");
  EXPECT_GE(stats[7].GetInt(), 45);
}

// Simulate an uneven distribution which forces multi shard search to perform a refill
TEST_F(SearchFamilyTest, MultiShardRefill) {
  Run({"ft.create", "i1", "schema", "idx", "numeric"});

  // Place 100 keys ONLY on shard 0
  for (auto cmd : FillShard(0, "doc", 100))
    Run(absl::MakeSpan(cmd));

  // This will fail the probabilistc bound as well as the refill phase,
  // but should still succeed to select enough entries
  for (size_t limit : {10, 20, 50}) {
    auto resp = Run({"ft.search", "i1", "*", "LIMIT", "0", to_string(limit)});
    EXPECT_THAT(resp.GetVec().size(), 2 * limit + 1);
    EXPECT_THAT(resp.GetVec()[0], IntArg(100));

    resp = Run({"ft.profile", "i1", "SEARCH", "QUERY", "*", "LIMIT", "0", to_string(limit)});
    auto stats = resp.GetVec()[0].GetVec();
    // Make sure only one additional hop was needed for refill
    EXPECT_EQ(stats[8], "hops");
    EXPECT_THAT(stats[9], IntArg(2)) << "On limit " << limit;
    EXPECT_EQ(stats[6], "cutoff");
    EXPECT_THAT(stats[7], IntArg(0));
  }
}

// Simulate an uneven distribution which forces multi shard search to perform a refill,
// but the refill is interrupted by constant updates, which should lead to a full repeated query
// on a single shard (the interrupted one). After the repeated query, a successful order is built.
TEST_F(SearchFamilyTest, MultiShardRefillRefresh) {
  Run({"ft.create", "i1", "schema", "idx", "numeric"});

  // Place 100 keys ONLY on shard 0
  for (auto cmd : FillShard(0, "doc", 100))
    Run(absl::MakeSpan(cmd));

  atomic_bool keep_running = true;
  string_view key = "doc1";
  EXPECT_EQ(Shard(key, shard_set->size()), 0);
  auto fb = pp_->at(2)->LaunchFiber([this, &keep_running, key]() {
    while (keep_running.load())
      Run("pressure", {"hset", key, "updates", "more-and-more!"});
  });

  auto resp = Run({"ft.profile", "i1", "SEARCH", "QUERY", "*", "LIMIT", "0", "10"});
  auto stats = resp.GetVec()[0].GetVec();

  // Make sure refill didn't succeed because of constant updates
  EXPECT_EQ(stats[8], "hops");
  EXPECT_THAT(stats[9], IntArg(2));

  keep_running.store(false);
  ThisFiber::SleepFor(10ms);
  fb.Join();
}

// Simulate multi shard worst case. A refill needs to be performed, but fails on one shard due to
// constant writes. After a full repeated query, one element less is returned, which leads to an
// invalid order (because the other shard refilled one less than needed). A full repeated query is
// needed, so a total of 3 hops are performed.
// TODO: Test will be invalidated with fine-grained refills
TEST_F(SearchFamilyTest, MultiShardRefillRepeat) {
  Run({"ft.create", "i1", "schema", "idx", "numeric"});

  // Place 100 keys ONLY on shard 0
  for (auto cmd : FillShard(0, "doc", 100))
    Run(absl::MakeSpan(cmd));

  // Place a single key on shard 1
  auto key = "the-destroyer";
  EXPECT_EQ(Shard(key, shard_set->size()), 1);
  Run({"hset", key, "idx", "1"});

  atomic_bool keep_running = true;
  auto fb = pp_->at(2)->LaunchFiber([this, &keep_running, key]() {
    size_t idx = 0;
    while (keep_running.load()) {
      if (idx++ % 2 == 0)
        Run("pressure", {"del", key});
      else
        Run("pressure", {"hset", key, "idx", "1"});
    }
  });

  bool had_3hops = false;
  for (size_t tries = 0; tries < 100; tries++) {
    auto resp = Run({"ft.profile", "i1", "SEARCH", "QUERY", "*", "LIMIT", "0", "10"});
    auto stats = resp.GetVec()[0].GetVec();

    EXPECT_EQ(stats[8], "hops");
    if (stats[9].GetInt() == 2)
      continue;

    EXPECT_THAT(stats[9], IntArg(3));
    had_3hops = true;
    break;
  }

  EXPECT_TRUE(had_3hops) << "Failed probabilstic test :(";

  keep_running.store(false);
  ThisFiber::SleepFor(10ms);
  fb.Join();
}

TEST_F(SearchFamilyTest, MultiShardAggregation) {
  // Place 50 keys on shards 0 and 1, but values on shard 1 have a larger value
  for (auto cmd : FillShard(0, "doc", 50, 0))
    Run(absl::MakeSpan(cmd));

  for (auto cmd : FillShard(1, "doc", 50, 100))
    Run(absl::MakeSpan(cmd));

  Run({"ft.create", "i1", "schema", "idx", "numeric", "sortable"});

  // The distribution is completely unbalanced, so getting the largest vlaues should require two
  // hops
  auto resp = Run(
      {"ft.profile", "i1", "SEARCH", "QUERY", "*", "LIMIT", "0", "20", "SORTBY", "idx", "DESC"});
  auto stats = resp.GetVec()[0].GetVec();
  EXPECT_EQ(stats[8], "hops");
  EXPECT_THAT(stats[9], IntArg(2));
}

TEST_F(SearchFamilyTest, SimpleExpiry) {
  EXPECT_EQ(Run({"ft.create", "i1", "schema", "title", "text", "expires-in", "numeric"}), "OK");

  Run({"hset", "d:1", "title", "never to expire", "expires-in", "100500"});

  Run({"hset", "d:2", "title", "first to expire", "expires-in", "50"});
  Run({"pexpire", "d:2", "50"});

  Run({"hset", "d:3", "title", "second to expire", "expires-in", "100"});
  Run({"pexpire", "d:3", "100"});

  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1", "d:2", "d:3"));

  // Expired documents are still included in idlist
  AdvanceTime(60);
  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1", "d:2", "d:3"));

  shard_set->TEST_EnableHeartBeat();
  for (size_t i = 0; i < 5; i++) {  // Give heartbeat time to delete expired doc
    ThisFiber::SleepFor(2ms);
    Run({"incr", "run heatbeat run"});
  }
  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1", "d:3"));

  AdvanceTime(60);
  Run({"HGETALL", "d:3"});  // Trigger expiry by access
  EXPECT_THAT(Run({"ft.search", "i1", "*"}), AreDocIds("d:1"));

  Run({"flushall"});
}

TEST_F(SearchFamilyTest, AggregateGroupByReduceSort) {
  for (size_t i = 0; i < 101; i++) {  // 51 even, 50 odd
    Run({"hset", absl::StrCat("k", i), "even", (i % 2 == 0) ? "true" : "false", "value",
         absl::StrCat(i)});
  }
  Run({"ft.create", "i1", "schema", "even", "tag", "sortable", "value", "numeric", "sortable"});

  // clang-format off
  auto resp = Run({"ft.aggregate", "i1", "*",
                  "GROUPBY", "1", "even",
                      "REDUCE", "count", "0", "as", "count",
                      "REDUCE", "count_distinct", "1", "even", "as", "distinct_tags",
                      "REDUCE", "count_distinct", "1", "value", "as", "distinct_vals",
                      "REDUCE", "max", "1", "value", "as", "max_val",
                      "REDUCE", "min", "1", "value", "as", "min_val",
                  "SORTBY", "1", "count"});
  // clang-format on

  EXPECT_THAT(resp,
              IsArray(IsUnordArray(IsArray("even", "false"), IsArray("count", "50"),
                                   IsArray("distinct_tags", "1"), IsArray("distinct_vals", "50"),
                                   IsArray("max_val", "99"), IsArray("min_val", "1")),
                      IsUnordArray(IsArray("even", "true"), IsArray("count", "51"),
                                   IsArray("distinct_tags", "1"), IsArray("distinct_vals", "51"),
                                   IsArray("max_val", "100"), IsArray("min_val", "0"))));
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
                  "GROUPBY", "1", "even"});
  // clang-format on

  EXPECT_THAT(resp, IsUnordArray(IsUnordArray(IsArray("even", "false")),
                                 IsUnordArray(IsArray("even", "true"))));
}

TEST_F(SearchFamilyTest, Vector) {
  auto resp = Run({"ft.create", "ann", "ON", "HASH", "SCHEMA", "vector", "VECTOR", "HNSW", "8",
                   "TYPE", "FLOAT32", "DIM", "100", "distance_metric", "cosine", "M", "64"});
  EXPECT_EQ(resp, "OK");
}

}  // namespace dfly
