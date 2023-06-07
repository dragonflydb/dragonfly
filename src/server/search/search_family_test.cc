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

MATCHER_P(DocIds, arg_ids, "") {
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

  if (auto num_results = results[0].GetInt();
      !num_results || size_t(*num_results) != arg_ids.size()) {
    *result_listener << "Bad document number in reply: " << results.size();
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
  return DocIds(vector<string>{args...});
}

TEST_F(SearchFamilyTest, CreateIndex) {
  EXPECT_EQ(Run({"ft.create", "idx", "ON", "HASH", "PREFIX", "1", "prefix"}), "OK");
}

TEST_F(SearchFamilyTest, Simple) {
  Run({"hset", "d:1", "foo", "baz", "k", "v"});
  Run({"hset", "d:2", "foo", "bar", "k", "v"});
  Run({"hset", "d:3", "foo", "bad", "k", "v"});

  EXPECT_EQ(Run({"ft.create", "i1", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "foo", "TEXT", "k",
                 "TEXT"}),
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

  EXPECT_EQ(Run({"ft.create", "i1", "on", "json", "schema", "a", "text", "b", "text"}), "OK");

  EXPECT_THAT(Run({"ft.search", "i1", "some|more"}), AreDocIds("k1", "k2"));
  EXPECT_THAT(Run({"ft.search", "i1", "some|more|secret"}), AreDocIds("k1", "k2", "k3"));

  EXPECT_THAT(Run({"ft.search", "i1", "@a:last @b:details"}), AreDocIds("k3"));
  EXPECT_THAT(Run({"ft.search", "i1", "@a:(another|small)"}), AreDocIds("k1", "k2"));
  EXPECT_THAT(Run({"ft.search", "i1", "@a:(another|small|secret)"}), AreDocIds("k1", "k2"));

  EXPECT_THAT(Run({"ft.search", "i1", "none"}), kNoResults);
  EXPECT_THAT(Run({"ft.search", "i1", "@a:small @b:secret"}), kNoResults);
}

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

}  // namespace dfly
