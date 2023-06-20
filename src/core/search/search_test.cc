// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/query_driver.h"

namespace dfly {
namespace search {

using namespace std;

struct MockedDocument : public DocumentAccessor {
 public:
  using Map = absl::flat_hash_map<std::string, std::string>;

  MockedDocument() = default;
  MockedDocument(Map map) : fields_{map} {
  }
  MockedDocument(std::string test_field) : fields_{{"field", test_field}} {
  }

  string_view GetString(string_view field) const override {
    auto it = fields_.find(field);
    return it != fields_.end() ? string_view{it->second} : "";
  }

  FtVector GetVector(string_view field) const override {
    string_view str_value = fields_.at(field);
    FtVector out;
    for (string_view coord : absl::StrSplit(str_value, ',')) {
      float v;
      CHECK(absl::SimpleAtof(coord, &v));
      out.push_back(v);
    }
    return out;
  }

  string DebugFormat() {
    string out = "{";
    for (const auto& [field, value] : fields_)
      absl::StrAppend(&out, field, "=", value, ",");
    if (out.size() > 1)
      out.pop_back();
    out += "}";
    return out;
  }

  void Set(Map hset) {
    fields_ = hset;
  }

 private:
  Map fields_{};
};

class SearchParserTest : public ::testing::Test {
 protected:
  SearchParserTest() {
    PrepareSchema();
  }

  ~SearchParserTest() {
    EXPECT_EQ(entries_.size(), 0u) << "Missing check";
  }

  void SetKnnVec(FtVector vec) {
    params_.knn_vec = vec;
  }

  void PrepareSchema(Schema schema = {{{"field", Schema::TEXT}}}) {
    schema_ = schema;
  }

  void PrepareQuery(string_view query) {
    query_ = query;
  }

  template <typename... Args> void ExpectAll(Args... args) {
    (entries_.emplace_back(args, true), ...);
  }

  template <typename... Args> void ExpectNone(Args... args) {
    (entries_.emplace_back(args, false), ...);
  }

  bool Check() {
    absl::Cleanup cl{[this] { entries_.clear(); }};

    FieldIndices index{schema_};

    shuffle(entries_.begin(), entries_.end(), default_random_engine{});
    for (DocId i = 0; i < entries_.size(); i++)
      index.Add(i, &entries_[i].first);

    SearchAlgorithm search_algo{};
    if (!search_algo.Init(query_, params_)) {
      error_ = "Failed to parse query";
      return false;
    }

    auto matched = search_algo.Search(&index);

    if (!is_sorted(matched.ids.begin(), matched.ids.end()))
      LOG(FATAL) << "Search result is not sorted";

    for (DocId i = 0; i < entries_.size(); i++) {
      bool doc_matched = binary_search(matched.ids.begin(), matched.ids.end(), i);
      if (doc_matched != entries_[i].second) {
        error_ = "doc: \"" + entries_[i].first.DebugFormat() + "\"" + " was expected" +
                 (entries_[i].second ? "" : " not") + " to match" + " query: \"" + query_ + "\"";
        return false;
      }
    }

    return true;
  }

  string_view GetError() const {
    return error_;
  }

 private:
  using DocEntry = pair<MockedDocument, bool /*should_match*/>;

  QueryParams params_;
  Schema schema_;
  vector<DocEntry> entries_;
  string query_, error_;
};

TEST_F(SearchParserTest, MatchTerm) {
  PrepareQuery("foo");

  // Check basic cases
  ExpectAll("foo", "foo bar", "more foo bar");
  ExpectNone("wrong", "nomatch");

  // Check part of sentence + case.
  ExpectAll("Foo is cool.", "Where is foo?", "One. FOO!. More", "Foo is foo.");

  // Check part of word is not matched
  ExpectNone("foocool", "veryfoos", "ufoo", "morefoomore", "thefoo");

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, MatchNotTerm) {
  PrepareQuery("-foo");

  ExpectAll("faa", "definitielyright");
  ExpectNone("foo", "foo bar", "more foo bar");

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, MatchLogicalNode) {
  {
    PrepareQuery("foo bar");

    ExpectAll("foo bar", "bar foo", "more bar and foo");
    ExpectNone("wrong", "foo", "bar", "foob", "far");

    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("foo | bar");

    ExpectAll("foo bar", "foo", "bar", "foo and more", "or only bar");
    ExpectNone("wrong", "only far");

    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("foo bar baz");

    ExpectAll("baz bar foo", "bar and foo and baz");
    ExpectNone("wrong", "foo baz", "bar baz", "and foo");

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchParserTest, MatchParenthesis) {
  PrepareQuery("( foo | oof ) ( bar | rab )");

  ExpectAll("foo bar", "oof rab", "foo rab", "oof bar", "foo oof bar rab");
  ExpectNone("wrong", "bar rab", "foo oof");

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, CheckNotPriority) {
  for (auto expr : {"-bar foo baz", "foo -bar baz", "foo baz -bar"}) {
    PrepareQuery(expr);

    ExpectAll("foo baz", "foo rab baz", "baz rab foo");
    ExpectNone("wrong", "bar", "foo bar baz", "foo baz bar");

    EXPECT_TRUE(Check()) << GetError();
  }

  for (auto expr : {"-bar | foo", "foo | -bar"}) {
    PrepareQuery(expr);

    ExpectAll("foo", "right", "foo bar");
    ExpectNone("bar", "bar baz");

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchParserTest, CheckParenthesisPriority) {
  {
    PrepareQuery("foo | -(bar baz)");

    ExpectAll("foo", "not b/r and b/z", "foo bar baz", "single bar", "only baz");
    ExpectNone("bar baz", "some more bar and baz");

    EXPECT_TRUE(Check()) << GetError();
  }
  {
    PrepareQuery("( foo (bar | baz) (rab | zab) ) | true");

    ExpectAll("true", "foo bar rab", "foo baz zab", "foo bar zab");
    ExpectNone("wrong", "foo bar baz", "foo rab zab", "foo bar what", "foo rab foo");

    EXPECT_TRUE(Check()) << GetError();
  }
}

using Map = MockedDocument::Map;

TEST_F(SearchParserTest, MatchField) {
  PrepareSchema({{{"f1", Schema::TEXT}, {"f2", Schema::TEXT}, {"f3", Schema::TEXT}}});
  PrepareQuery("@f1:foo @f2:bar @f3:baz");

  ExpectAll(Map{{"f1", "foo"}, {"f2", "bar"}, {"f3", "baz"}});
  ExpectNone(Map{{"f1", "foo"}, {"f2", "bar"}, {"f3", "last is wrong"}},
             Map{{"f1", "its"}, {"f2", "totally"}, {"f3", "wrong"}},
             Map{{"f1", "im foo but its only me and"}, {"f2", "bar"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, MatchRange) {
  PrepareSchema({{{"f1", Schema::NUMERIC}, {"f2", Schema::NUMERIC}}});
  PrepareQuery("@f1:[1 10] @f2:[50 100]");

  ExpectAll(Map{{"f1", "5"}, {"f2", "50"}}, Map{{"f1", "1"}, {"f2", "100"}},
            Map{{"f1", "10"}, {"f2", "50"}});
  ExpectNone(Map{{"f1", "11"}, {"f2", "49"}}, Map{{"f1", "0"}, {"f2", "101"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, MatchStar) {
  PrepareQuery("*");
  ExpectAll("one", "two", "three", "and", "all", "documents");
  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, CheckExprInField) {
  PrepareSchema({{{"f1", Schema::TEXT}, {"f2", Schema::TEXT}, {"f3", Schema::TEXT}}});
  {
    PrepareQuery("@f1:(a|b) @f2:(c d) @f3:-e");

    ExpectAll(Map{{"f1", "a"}, {"f2", "c and d"}, {"f3", "right"}},
              Map{{"f1", "b"}, {"f2", "d and c"}, {"f3", "ok"}});
    ExpectNone(Map{{"f1", "none"}, {"f2", "only d"}, {"f3", "ok"}},
               Map{{"f1", "b"}, {"f2", "d and c"}, {"f3", "it has an e"}});

    EXPECT_TRUE(Check()) << GetError();
  }
  {
    PrepareQuery({"@f1:(a (b | c) -(d | e)) @f2:-(a|b)"});

    ExpectAll(Map{{"f1", "a b w"}, {"f2", "c"}});
    ExpectNone(Map{{"f1", "a b d"}, {"f2", "c"}}, Map{{"f1", "a b w"}, {"f2", "a"}},
               Map{{"f1", "a w"}, {"f2", "c"}});

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchParserTest, CheckTag) {
  PrepareSchema({{{"f1", Schema::TAG}, {"f2", Schema::TAG}}});

  PrepareQuery("@f1:{red | blue} @f2:{circle | square}");

  ExpectAll(Map{{"f1", "red"}, {"f2", "square"}}, Map{{"f1", "blue"}, {"f2", "square"}},
            Map{{"f1", "red"}, {"f2", "circle"}}, Map{{"f1", "red"}, {"f2", "circle, square"}},
            Map{{"f1", "red"}, {"f2", "triangle, circle"}},
            Map{{"f1", "red, green"}, {"f2", "square"}},
            Map{{"f1", "green, blue"}, {"f2", "circle"}});
  ExpectNone(Map{{"f1", "green"}, {"f2", "square"}}, Map{{"f1", "green"}, {"f2", "circle"}},
             Map{{"f1", "red"}, {"f2", "triangle"}}, Map{{"f1", "blue"}, {"f2", "line, triangle"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchParserTest, SimpleKnn) {
  Schema schema{{{"even", Schema::TAG}, {"pos", Schema::VECTOR}}};
  FieldIndices indices{schema};

  // Place points on a straight line
  for (size_t i = 0; i < 100; i++) {
    Map values{{{"even", i % 2 == 0 ? "YES" : "NO"}, {"pos", to_string(float(i))}}};
    MockedDocument doc{values};
    indices.Add(i, &doc);
  }

  SearchAlgorithm algo{};

  // Five closest to 50
  {
    algo.Init("*=>[KNN 5 @pos VEC]", QueryParams{FtVector{50.0}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(48, 49, 50, 51, 52));
  }

  // Five closest to 0
  {
    algo.Init("*=>[KNN 5 @pos VEC]", QueryParams{FtVector{0.0}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3, 4));
  }

  // Five closest to 20, all even
  {
    algo.Init("@even:{yes} =>[KNN 5 @pos VEC]", QueryParams{FtVector{20.0}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(16, 18, 20, 22, 24));
  }

  // Three closest to 31, all odd
  {
    algo.Init("@even:{no} =>[KNN 3 @pos VEC]", QueryParams{FtVector{31.0}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(29, 31, 33));
  }

  // Two closest to 70.5
  {
    algo.Init("* =>[KNN 2 @pos VEC]", QueryParams{FtVector{70.5}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(70, 71));
  }
}

TEST_F(SearchParserTest, Simple2dKnn) {
  // Square:
  // 3      2
  //    4
  // 0      1
  const pair<float, float> kTestCoords[] = {{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0.5, 0.5}};

  Schema schema{{{"pos", Schema::VECTOR}}};
  FieldIndices indices{schema};

  for (size_t i = 0; i < ABSL_ARRAYSIZE(kTestCoords); i++) {
    auto [x, y] = kTestCoords[i];
    string coords = absl::StrCat(x, ",", y);
    MockedDocument doc{Map{{"pos", coords}}};
    indices.Add(i, &doc);
  }

  SearchAlgorithm algo{};

  // Single center
  {
    algo.Init("* =>[KNN 1 @pos VEC]", QueryParams{FtVector{0.5, 0.5}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(4));
  }

  // Lower left
  {
    algo.Init("* =>[KNN 4 @pos VEC]", QueryParams{FtVector{0, 0}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 3, 4));
  }

  // Upper right
  {
    algo.Init("* =>[KNN 4 @pos VEC]", QueryParams{FtVector{1, 1}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1, 2, 3, 4));
  }

  // Request more than there is
  {
    algo.Init("* => [KNN 10 @pos VEC]", QueryParams{FtVector{0, 0}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3, 4));
  }

  // Test correct order: (0.7, 0.15)
  {
    algo.Init("* => [KNN 10 @pos VEC]", QueryParams{FtVector{0.7, 0.15}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(1, 4, 0, 2, 3));
  }

  // Test correct order: (0.8, 0.9)
  {
    algo.Init("* => [KNN 10 @pos VEC]", QueryParams{FtVector{0.8, 0.9}});
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(2, 4, 3, 1, 0));
  }
}

}  // namespace search

}  // namespace dfly
