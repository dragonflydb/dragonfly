// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/container/flat_hash_map.h>

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

  string_view Get(string_view field) const override {
    auto it = fields_.find(field);
    return it != fields_.end() ? string_view{it->second} : "";
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
    FieldIndices index{schema_};

    shuffle(entries_.begin(), entries_.end(), default_random_engine{});
    for (DocId i = 0; i < entries_.size(); i++)
      index.Add(i, &entries_[i].first);

    SearchAlgorithm search_algo{};
    search_algo.Init(query_);
    auto matched = search_algo.Search(&index);

    if (!is_sorted(matched.begin(), matched.end()))
      LOG(FATAL) << "Search result is not sorted";

    for (DocId i = 0; i < entries_.size(); i++) {
      bool doc_matched = binary_search(matched.begin(), matched.end(), i);
      if (doc_matched != entries_[i].second) {
        error_ = "doc: \"" + entries_[i].first.DebugFormat() + "\"" + " was expected" +
                 (entries_[i].second ? "" : " not") + " to match" + " query: \"" + query_ + "\"";
        entries_.clear();
        return false;
      }
    }

    entries_.clear();
    return true;
  }

  string_view GetError() const {
    return error_;
  }

 private:
  using DocEntry = pair<MockedDocument, bool /*should_match*/>;

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

}  // namespace search

}  // namespace dfly
