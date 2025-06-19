// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <memory_resource>
#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "core/search/query_driver.h"
#include "core/search/vector_utils.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {
namespace search {

using namespace std;

using ::testing::HasSubstr;

struct MockedDocument : public DocumentAccessor {
 public:
  using Map = absl::flat_hash_map<std::string, std::string>;

  MockedDocument() = default;
  MockedDocument(Map map) : fields_{map} {
  }
  MockedDocument(std::string test_field) : fields_{{"field", test_field}} {
  }

  std::optional<StringList> GetStrings(string_view field) const override {
    auto it = fields_.find(field);
    if (it == fields_.end()) {
      return EmptyAccessResult<StringList>();
    }
    return StringList{string_view{it->second}};
  }

  std::optional<StringList> GetTags(string_view field) const override {
    return GetStrings(field);
  }

  std::optional<VectorInfo> GetVector(string_view field) const override {
    auto strings_list = GetStrings(field);
    if (!strings_list)
      return std::nullopt;
    return !strings_list->empty() ? BytesToFtVectorSafe(strings_list->front()) : VectorInfo{};
  }

  std::optional<NumsList> GetNumbers(std::string_view field) const override {
    auto strings_list = GetStrings(field);
    if (!strings_list)
      return std::nullopt;

    NumsList nums_list;
    nums_list.reserve(strings_list->size());
    for (auto str : strings_list.value()) {
      auto num = ParseNumericField(str);
      if (!num) {
        return std::nullopt;
      }
      nums_list.push_back(num.value());
    }
    return nums_list;
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

IndicesOptions kEmptyOptions{{}};

Schema MakeSimpleSchema(initializer_list<pair<string_view, SchemaField::FieldType>> ilist) {
  Schema schema;
  for (auto [name, type] : ilist) {
    schema.fields[name] = {type, 0, string{name}};
    if (type == SchemaField::TAG)
      schema.fields[name].special_params = SchemaField::TagParams{};
  }
  return schema;
}

class SearchTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  SearchTest() {
    PrepareSchema({{"field", SchemaField::TEXT}});
  }

  ~SearchTest() {
    EXPECT_EQ(entries_.size(), 0u) << "Missing check";
  }

  void PrepareSchema(initializer_list<pair<string_view, SchemaField::FieldType>> ilist) {
    schema_ = MakeSimpleSchema(ilist);
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

    FieldIndices index{schema_, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

    shuffle(entries_.begin(), entries_.end(), default_random_engine{});
    for (DocId i = 0; i < entries_.size(); i++)
      index.Add(i, entries_[i].first);

    SearchAlgorithm search_algo{};
    if (!search_algo.Init(query_, &params_)) {
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

TEST_F(SearchTest, MatchTerm) {
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

TEST_F(SearchTest, MatchNotTerm) {
  PrepareQuery("-foo");

  ExpectAll("faa", "definitielyright");
  ExpectNone("foo", "foo bar", "more foo bar");

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, MatchLogicalNode) {
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

TEST_F(SearchTest, MatchParenthesis) {
  PrepareQuery("( foo | oof ) ( bar | rab )");

  ExpectAll("foo bar", "oof rab", "foo rab", "oof bar", "foo oof bar rab");
  ExpectNone("wrong", "bar rab", "foo oof");

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, CheckNotPriority) {
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

  for (auto expr : {"-bar far|-foo tam"}) {
    PrepareQuery(expr);

    ExpectAll("far baz", "far foo", "bar tam");
    ExpectNone("bar far", "foo tam", "bar foo", "far bar foo");

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchTest, CheckParenthesisPriority) {
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

TEST_F(SearchTest, CheckPrefix) {
  {
    PrepareQuery("pre*");

    ExpectAll("pre", "prepre", "preachers", "prepared", "pRetty", "PRedators", "prEcisely!");
    ExpectNone("pristine", "represent", "repair", "depreciation");

    EXPECT_TRUE(Check()) << GetError();
  }
  {
    PrepareQuery("new*");

    ExpectAll("new", "New York", "Newham", "newbie", "news", "Welcome to Newark!");
    ExpectNone("ne", "renew", "nev", "ne-w", "notnew", "casino in neVada");

    EXPECT_TRUE(Check()) << GetError();
  }
}

using Map = MockedDocument::Map;

TEST_F(SearchTest, MatchField) {
  PrepareSchema({{"f1", SchemaField::TEXT}, {"f2", SchemaField::TEXT}, {"f3", SchemaField::TEXT}});
  PrepareQuery("@f1:foo @f2:bar @f3:baz");

  ExpectAll(Map{{"f1", "foo"}, {"f2", "bar"}, {"f3", "baz"}});
  ExpectNone(Map{{"f1", "foo"}, {"f2", "bar"}, {"f3", "last is wrong"}},
             Map{{"f1", "its"}, {"f2", "totally"}, {"f3", "wrong"}},
             Map{{"f1", "im foo but its only me and"}, {"f2", "bar"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, MatchRange) {
  PrepareSchema({{"f1", SchemaField::NUMERIC}, {"f2", SchemaField::NUMERIC}});
  PrepareQuery("@f1:[1 10] @f2:[50 100]");

  ExpectAll(Map{{"f1", "5"}, {"f2", "50"}}, Map{{"f1", "1"}, {"f2", "100"}},
            Map{{"f1", "10"}, {"f2", "50"}});
  ExpectNone(Map{{"f1", "11"}, {"f2", "49"}}, Map{{"f1", "0"}, {"f2", "101"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, MatchDoubleRange) {
  PrepareSchema({{"f1", SchemaField::NUMERIC}});

  {
    PrepareQuery("@f1: [100.03 199.97]");

    ExpectAll(Map{{"f1", "130"}}, Map{{"f1", "170"}}, Map{{"f1", "100.03"}}, Map{{"f1", "199.97"}});

    ExpectNone(Map{{"f1", "0"}}, Map{{"f1", "200"}}, Map{{"f1", "100.02999"}},
               Map{{"f1", "199.9700001"}});

    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@f1: [(100 (199.9]");

    ExpectAll(Map{{"f1", "150"}}, Map{{"f1", "100.00001"}}, Map{{"f1", "199.8999999"}});

    ExpectNone(Map{{"f1", "50"}}, Map{{"f1", "100"}}, Map{{"f1", "199.9"}}, Map{{"f1", "200"}});

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchTest, MatchStar) {
  PrepareQuery("*");
  ExpectAll("one", "two", "three", "and", "all", "documents");
  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, CheckExprInField) {
  PrepareSchema({{"f1", SchemaField::TEXT}, {"f2", SchemaField::TEXT}, {"f3", SchemaField::TEXT}});
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
  {
    PrepareQuery("@f1:(-a c|-b d)");

    ExpectAll(Map{{"f1", "c"}}, Map{{"f1", "d"}});
    ExpectNone(Map{{"f1", "a"}}, Map{{"f1", "b"}});

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchTest, CheckTag) {
  PrepareSchema({{"f1", SchemaField::TAG}, {"f2", SchemaField::TAG}});

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

TEST_F(SearchTest, CheckTagPrefix) {
  PrepareSchema({{"color", SchemaField::TAG}});
  PrepareQuery("@color:{green* | orange | yellow*}");

  ExpectAll(Map{{"color", "green"}}, Map{{"color", "yellow"}}, Map{{"color", "greenish"}},
            Map{{"color", "yellowish"}}, Map{{"color", "green-forestish"}},
            Map{{"color", "yellowsunish"}}, Map{{"color", "orange"}});
  ExpectNone(Map{{"color", "red"}}, Map{{"color", "blue"}}, Map{{"color", "orangeish"}},
             Map{{"color", "darkgreen"}}, Map{{"color", "light-yellow"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, IntegerTerms) {
  PrepareSchema({{"status", SchemaField::TAG}, {"title", SchemaField::TEXT}});

  PrepareQuery("@status:{1} @title:33");

  ExpectAll(Map{{"status", "1"}, {"title", "33 cars on the road"}});
  ExpectNone(Map{{"status", "0"}, {"title", "22 trains on the tracks"}});

  EXPECT_TRUE(Check()) << GetError();
}

TEST_F(SearchTest, StopWords) {
  auto schema = MakeSimpleSchema({{"title", SchemaField::TEXT}});
  IndicesOptions options{{"some", "words", "are", "left", "out"}};

  FieldIndices indices{schema, options, PMR_NS::get_default_resource(), nullptr};
  SearchAlgorithm algo{};
  QueryParams params;

  vector<string> documents = {"some words left out",      //
                              "some can be found",        //
                              "words are never matched",  //
                              "explicitly found!"};
  for (size_t i = 0; i < documents.size(); i++) {
    MockedDocument doc{{{"title", documents[i]}}};
    indices.Add(i, doc);
  }

  // words is a stopword
  algo.Init("words", &params);
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre());

  // some is a stopword
  algo.Init("some", &params);
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre());

  // found is not a stopword
  algo.Init("found", &params);
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1, 3));
}

std::string ToBytes(absl::Span<const float> vec) {
  return string{reinterpret_cast<const char*>(vec.data()), sizeof(float) * vec.size()};
}

TEST_F(SearchTest, Errors) {
  auto schema = MakeSimpleSchema(
      {{"score", SchemaField::NUMERIC}, {"even", SchemaField::TAG}, {"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params = SchemaField::VectorParams{false, 1};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo{};
  QueryParams params;

  // Non-existent field
  algo.Init("@cantfindme:[1 10]", &params);
  EXPECT_THAT(algo.Search(&indices).error, HasSubstr("Invalid field"));

  // Invalid type
  algo.Init("@even:[1 10]", &params);
  EXPECT_THAT(algo.Search(&indices).error, HasSubstr("Wrong access type"));

  // Wrong vector index dimensions
  params["vec"] = ToBytes({1, 2, 3, 4});
  algo.Init("* => [KNN 5 @pos $vec]", &params);
  EXPECT_THAT(algo.Search(&indices).error, HasSubstr("Wrong vector index dimensions"));
}

TEST_F(SearchTest, MatchNumericRangeWithCommas) {
  PrepareSchema({{"f1", SchemaField::NUMERIC}, {"draw_end", SchemaField::NUMERIC}});

  // Main tests for point range with identical values and different delimiters
  {
    PrepareQuery("@draw_end:[1742916180 1742916180]");
    ExpectAll(Map{{"draw_end", "1742916180"}});
    ExpectNone(Map{{"draw_end", "1742916181"}}, Map{{"draw_end", "1742916179"}});
    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@draw_end:[1742916180, 1742916180]");
    ExpectAll(Map{{"draw_end", "1742916180"}});
    ExpectNone(Map{{"draw_end", "1742916181"}}, Map{{"draw_end", "1742916179"}});
    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@draw_end:[1742916180 ,1742916180]");
    ExpectAll(Map{{"draw_end", "1742916180"}});
    ExpectNone(Map{{"draw_end", "1742916181"}}, Map{{"draw_end", "1742916179"}});
    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@draw_end:[1742916180   1742916180]");
    ExpectAll(Map{{"draw_end", "1742916180"}});
    ExpectNone(Map{{"draw_end", "1742916181"}}, Map{{"draw_end", "1742916179"}});
    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@f1:[100   ,     200]");
    ExpectAll(Map{{"f1", "100"}}, Map{{"f1", "150"}}, Map{{"f1", "200"}});
    ExpectNone(Map{{"f1", "99"}}, Map{{"f1", "201"}});
    EXPECT_TRUE(Check()) << GetError();
  }
}

class KnnTest : public SearchTest, public testing::WithParamInterface<bool /* hnsw */> {};

TEST_P(KnnTest, Simple1D) {
  auto schema = MakeSimpleSchema({{"even", SchemaField::TAG}, {"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params = SchemaField::VectorParams{GetParam(), 1};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  // Place points on a straight line
  for (size_t i = 0; i < 100; i++) {
    Map values{{{"even", i % 2 == 0 ? "YES" : "NO"}, {"pos", ToBytes({float(i)})}}};
    MockedDocument doc{values};
    indices.Add(i, doc);
  }

  SearchAlgorithm algo{};
  QueryParams params;

  // Five closest to 50
  {
    params["vec"] = ToBytes({50.0});
    algo.Init("*=>[KNN 5 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(48, 49, 50, 51, 52));
  }

  // Five closest to 0
  {
    params["vec"] = ToBytes({0.0});
    algo.Init("*=>[KNN 5 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3, 4));
  }

  // Five closest to 20, all even
  {
    params["vec"] = ToBytes({20.0});
    algo.Init("@even:{yes} =>[KNN 5 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(16, 18, 20, 22, 24));
  }

  // Three closest to 31, all odd
  {
    params["vec"] = ToBytes({31.0});
    algo.Init("@even:{no} =>[KNN 3 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(29, 31, 33));
  }

  // Two closest to 70.5
  {
    params["vec"] = ToBytes({70.5});
    algo.Init("* =>[KNN 2 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(70, 71));
  }

  // Two closest to 70.5
  {
    params["vec"] = ToBytes({70.5});
    algo.Init("* =>[KNN 2 @pos $vec as vector_distance]", &params);
    EXPECT_EQ("vector_distance", algo.GetKnnScoreSortOption()->score_field_alias);
    SearchResult result = algo.Search(&indices);
    EXPECT_THAT(result.ids, testing::UnorderedElementsAre(70, 71));
  }
}

TEST_P(KnnTest, Simple2D) {
  // Square:
  // 3      2
  //    4
  // 0      1
  const pair<float, float> kTestCoords[] = {{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0.5, 0.5}};

  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params = SchemaField::VectorParams{GetParam(), 2};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  for (size_t i = 0; i < ABSL_ARRAYSIZE(kTestCoords); i++) {
    string coords = ToBytes({kTestCoords[i].first, kTestCoords[i].second});
    MockedDocument doc{Map{{"pos", coords}}};
    indices.Add(i, doc);
  }

  SearchAlgorithm algo{};
  QueryParams params;

  // Single center
  {
    params["vec"] = ToBytes({0.5, 0.5});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(4));
  }

  // Lower left
  {
    params["vec"] = ToBytes({0, 0});
    algo.Init("* =>[KNN 4 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 3, 4));
  }

  // Upper right
  {
    params["vec"] = ToBytes({1, 1});
    algo.Init("* =>[KNN 4 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1, 2, 3, 4));
  }

  // Request more than there is
  {
    params["vec"] = ToBytes({0, 0});
    algo.Init("* => [KNN 10 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3, 4));
  }

  // Test correct order: (0.7, 0.15)
  {
    params["vec"] = ToBytes({0.7, 0.15});
    algo.Init("* => [KNN 10 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(1, 4, 0, 2, 3));
  }

  // Test correct order: (0.8, 0.9)
  {
    params["vec"] = ToBytes({0.8, 0.9});
    algo.Init("* => [KNN 10 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(2, 4, 3, 1, 0));
  }
}

TEST_P(KnnTest, Cosine) {
  // Four arrows, closest cosing distance will be closes by angle
  // 0 🡢 1 🡣 2 🡠 3 🡡
  const pair<float, float> kTestCoords[] = {{1, 0}, {0, -1}, {-1, 0}, {0, 1}};

  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params =
      SchemaField::VectorParams{GetParam(), 2, VectorSimilarity::COSINE};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  for (size_t i = 0; i < ABSL_ARRAYSIZE(kTestCoords); i++) {
    string coords = ToBytes({kTestCoords[i].first, kTestCoords[i].second});
    MockedDocument doc{Map{{"pos", coords}}};
    indices.Add(i, doc);
  }

  SearchAlgorithm algo{};
  QueryParams params;

  // Point down
  {
    params["vec"] = ToBytes({-0.1, -10});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1));
  }

  // Point left
  {
    params["vec"] = ToBytes({-0.1, -0.01});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(2));
  }

  // Point up
  {
    params["vec"] = ToBytes({0, 5});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(3));
  }

  // Point right
  {
    params["vec"] = ToBytes({0.2, 0.05});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0));
  }
}

TEST_P(KnnTest, AddRemove) {
  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params =
      SchemaField::VectorParams{GetParam(), 1, VectorSimilarity::L2};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  vector<MockedDocument> documents(10);
  for (size_t i = 0; i < 10; i++) {
    documents[i] = Map{{"pos", ToBytes({float(i)})}};
    indices.Add(i, documents[i]);
  }

  SearchAlgorithm algo{};
  QueryParams params;

  // search leftmost 5
  {
    params["vec"] = ToBytes({-1.0});
    algo.Init("* =>[KNN 5 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(0, 1, 2, 3, 4));
  }

  // delete leftmost 5
  for (size_t i = 0; i < 5; i++)
    indices.Remove(i, documents[i]);

  // search leftmost 5 again
  {
    params["vec"] = ToBytes({-1.0});
    algo.Init("* =>[KNN 5 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(5, 6, 7, 8, 9));
  }

  // add removed elements
  for (size_t i = 0; i < 5; i++)
    indices.Add(i, documents[i]);

  // repeat first search
  {
    params["vec"] = ToBytes({-1.0});
    algo.Init("* =>[KNN 5 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::ElementsAre(0, 1, 2, 3, 4));
  }
}

TEST_P(KnnTest, AutoResize) {
  // Make sure index resizes automatically even with a small initial capacity
  const size_t kInitialCapacity = 5;

  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params =
      SchemaField::VectorParams{GetParam(), 1, VectorSimilarity::L2, kInitialCapacity};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  for (size_t i = 0; i < 100; i++) {
    MockedDocument doc{Map{{"pos", ToBytes({float(i)})}}};
    indices.Add(i, doc);
  }

  EXPECT_EQ(indices.GetAllDocs().size(), 100);
}

INSTANTIATE_TEST_SUITE_P(KnnFlat, KnnTest, testing::Values(false));
INSTANTIATE_TEST_SUITE_P(KnnHnsw, KnnTest, testing::Values(true));

static void BM_VectorSearch(benchmark::State& state) {
  unsigned ndims = state.range(0);
  unsigned nvecs = state.range(1);

  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params = SchemaField::VectorParams{false, ndims};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  auto random_vec = [ndims]() {
    vector<float> coords;
    for (size_t j = 0; j < ndims; j++)
      coords.push_back(static_cast<float>(rand()) / static_cast<float>(RAND_MAX));
    return coords;
  };

  for (size_t i = 0; i < nvecs; i++) {
    auto rv = random_vec();
    MockedDocument doc{Map{{"pos", ToBytes(rv)}}};
    indices.Add(i, doc);
  }

  SearchAlgorithm algo{};
  QueryParams params;

  auto rv = random_vec();
  params["vec"] = ToBytes(rv);
  algo.Init("* =>[KNN 1 @pos $vec]", &params);

  while (state.KeepRunningBatch(10)) {
    for (size_t i = 0; i < 10; i++)
      benchmark::DoNotOptimize(algo.Search(&indices));
  }
}

BENCHMARK(BM_VectorSearch)->Args({120, 10'000});

TEST_F(SearchTest, MatchNonNullField) {
  PrepareSchema({{"text_field", SchemaField::TEXT},
                 {"tag_field", SchemaField::TAG},
                 {"num_field", SchemaField::NUMERIC}});

  {
    PrepareQuery("@text_field:*");

    ExpectAll(Map{{"text_field", "any value"}}, Map{{"text_field", "another value"}},
              Map{{"text_field", "third"}, {"tag_field", "tag1"}});

    ExpectNone(Map{{"tag_field", "wrong field"}}, Map{{"num_field", "123"}}, Map{});

    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@tag_field:*");

    ExpectAll(Map{{"tag_field", "tag1"}}, Map{{"tag_field", "tag2"}},
              Map{{"text_field", "value"}, {"tag_field", "tag3"}});

    ExpectNone(Map{{"text_field", "wrong field"}}, Map{{"num_field", "456"}}, Map{});

    EXPECT_TRUE(Check()) << GetError();
  }

  {
    PrepareQuery("@num_field:*");

    ExpectAll(Map{{"num_field", "123"}}, Map{{"num_field", "456"}},
              Map{{"text_field", "value"}, {"num_field", "789"}});

    ExpectNone(Map{{"text_field", "wrong field"}}, Map{{"tag_field", "tag1"}}, Map{});

    EXPECT_TRUE(Check()) << GetError();
  }
}

TEST_F(SearchTest, InvalidVectorParameter) {
  search::Schema schema;
  schema.fields["v"] = search::SchemaField{
      search::SchemaField::VECTOR,
      0,   // flags
      "v"  // short_name
  };

  search::SchemaField::VectorParams params;
  params.use_hnsw = true;
  params.dim = 2;
  params.sim = search::VectorSimilarity::L2;
  params.capacity = 10;
  params.hnsw_m = 16;
  params.hnsw_ef_construction = 200;
  schema.fields["v"].special_params = params;

  search::IndicesOptions options;
  search::FieldIndices indices{schema, options, PMR_NS::get_default_resource(), nullptr};

  search::SearchAlgorithm algo;
  search::QueryParams query_params;

  query_params["b"] = "abcdefg";

  ASSERT_FALSE(algo.Init("*=>[KNN 2 @v $b]", &query_params));
}

TEST_F(SearchTest, NotImplementedSearchTypes) {
  auto schema = MakeSimpleSchema({{"title", SchemaField::TEXT}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo{};
  QueryParams params;

  // Add a document for testing
  MockedDocument doc{Map{{"title", "text for search"}}};
  indices.Add(0, doc);

  // Test suffix search (words ending with "search")
  algo.Init("*search", &params);
  auto suffix_result = algo.Search(&indices);
  EXPECT_TRUE(suffix_result.ids.empty()) << "Suffix search should return empty result";
  EXPECT_THAT(suffix_result.error, testing::HasSubstr("Not implemented"))
      << "Suffix search should return a not implemented error";

  // Test infix search (words containing "for")
  algo.Init("*for*", &params);
  auto infix_result = algo.Search(&indices);
  EXPECT_TRUE(infix_result.ids.empty()) << "Infix search should return empty result";
  EXPECT_THAT(infix_result.error, testing::HasSubstr("Not implemented"))
      << "Infix search should return a not implemented error";
}

// Enumeration for different search types
enum class SearchType { PREFIX = 0, SUFFIX = 1, INFIX = 2 };

// Helper function to generate content with ASCII characters
static std::string GenerateWordSequence(size_t word_count, size_t doc_offset = 0) {
  std::string content;
  for (size_t i = 0; i < word_count; ++i) {
    std::string word;
    char start_char = 'a' + ((doc_offset + i) % 26);
    size_t word_len = 3 + (i % 5);  // Word length 3-7 chars

    for (size_t j = 0; j < word_len; ++j) {
      char c = start_char + (j % 26);
      if (c > 'z')
        c = 'a' + (c - 'z' - 1);
      word += c;
    }

    if (i > 0)
      content += " ";
    content += word;
  }
  return content;
}

// Helper function to generate pattern with variety
static std::string GeneratePattern(SearchType search_type, size_t pattern_len, bool use_uniform) {
  if (use_uniform) {
    // Original uniform pattern for comparison
    switch (search_type) {
      case SearchType::PREFIX:
        return std::string(pattern_len, 'p');
      case SearchType::SUFFIX:
        return std::string(pattern_len, 's');
      case SearchType::INFIX:
        return std::string(pattern_len, 'i');
    }
  } else {
    // Diverse ASCII pattern
    std::string pattern;
    char base_char = (search_type == SearchType::PREFIX)   ? 'p'
                     : (search_type == SearchType::SUFFIX) ? 's'
                                                           : 'i';

    for (size_t i = 0; i < pattern_len; ++i) {
      char c = base_char + (i % 10);  // Use variety of chars
      if (c > 'z')
        c = 'a' + (c - 'z' - 1);
      pattern += c;
    }
    return pattern;
  }
  return "";
}

static void BM_SearchByTypeImpl(benchmark::State& state, bool use_diverse_pattern) {
  size_t num_docs = state.range(0);
  size_t pattern_len = state.range(1);
  SearchType search_type = static_cast<SearchType>(state.range(2));

  auto schema = MakeSimpleSchema({{"title", SchemaField::TEXT}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  // Generate pattern
  std::string pattern = GeneratePattern(search_type, pattern_len, !use_diverse_pattern);
  std::string search_type_name = (search_type == SearchType::PREFIX)   ? "prefix"
                                 : (search_type == SearchType::SUFFIX) ? "suffix"
                                                                       : "infix";

  // Generate test data with more realistic content
  for (size_t i = 0; i < num_docs; i++) {
    std::string content;
    if (i < num_docs / 2) {
      // Half documents have the pattern in appropriate position
      std::string base_content = GenerateWordSequence(5 + (i % 5), i);

      switch (search_type) {
        case SearchType::PREFIX:
          content = pattern + base_content;
          break;
        case SearchType::SUFFIX:
          content = base_content + pattern;
          break;
        case SearchType::INFIX:
          // Fix: embed pattern inside a word, not as separate word
          size_t split_pos = base_content.length() / 2;
          content = base_content.substr(0, split_pos) + pattern + base_content.substr(split_pos);
          break;
      }
    } else {
      // Half don't have the pattern - generate different content
      content = GenerateWordSequence(8 + (i % 3), i + 1000);
    }
    MockedDocument doc{Map{{"title", content}}};
    indices.Add(i, doc);
  }

  SearchAlgorithm algo{};
  QueryParams params;
  std::string query;

  // Generate query based on search type
  switch (search_type) {
    case SearchType::PREFIX:
      query = pattern + "*";
      break;
    case SearchType::SUFFIX:
      query = "*" + pattern;
      break;
    case SearchType::INFIX:
      query = "*" + pattern + "*";
      break;
  }

  if (!algo.Init(query, &params)) {
    state.SkipWithError("Failed to initialize " + search_type_name + " search");
    return;
  }

  while (state.KeepRunning()) {
    auto result = algo.Search(&indices);
    benchmark::DoNotOptimize(result);

    // If result has error, skip the benchmark
    if (!result.error.empty()) {
      state.SkipWithError(search_type_name + " search returned error: " + result.error);
      return;
    }
  }

  // Set counters for analysis
  state.counters["docs_total"] = num_docs;
  state.counters["pattern_length"] = pattern_len;
  state.counters["diverse_pattern"] = use_diverse_pattern ? 1 : 0;
  state.SetLabel(search_type_name + (use_diverse_pattern ? "_diverse" : "_uniform"));
}

// Instantiate template functions
static void BM_SearchByType_Uniform(benchmark::State& state) {
  BM_SearchByTypeImpl(state, false);
}

static void BM_SearchByType_Diverse(benchmark::State& state) {
  BM_SearchByTypeImpl(state, true);
}

// Benchmark to compare all search types - removed 100K docs per romange's suggestion
BENCHMARK(BM_SearchByType_Uniform)
    // Uniform patterns (original test)
    ->Args({1000, 3, static_cast<int>(SearchType::PREFIX)})
    ->Args({1000, 5, static_cast<int>(SearchType::PREFIX)})
    ->Args({10000, 3, static_cast<int>(SearchType::PREFIX)})
    ->Args({10000, 5, static_cast<int>(SearchType::PREFIX)})
    ->Args({1000, 3, static_cast<int>(SearchType::SUFFIX)})
    ->Args({1000, 5, static_cast<int>(SearchType::SUFFIX)})
    ->Args({10000, 3, static_cast<int>(SearchType::SUFFIX)})
    ->Args({10000, 5, static_cast<int>(SearchType::SUFFIX)})
    ->Args({1000, 3, static_cast<int>(SearchType::INFIX)})
    ->Args({1000, 5, static_cast<int>(SearchType::INFIX)})
    ->Args({10000, 3, static_cast<int>(SearchType::INFIX)})
    ->Args({10000, 5, static_cast<int>(SearchType::INFIX)})
    ->ArgNames({"docs", "pattern_len", "search_type"})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_SearchByType_Diverse)
    // Diverse patterns (new test with ASCII variety)
    ->Args({1000, 3, static_cast<int>(SearchType::PREFIX)})
    ->Args({1000, 5, static_cast<int>(SearchType::PREFIX)})
    ->Args({10000, 3, static_cast<int>(SearchType::PREFIX)})
    ->Args({10000, 5, static_cast<int>(SearchType::PREFIX)})
    ->Args({1000, 3, static_cast<int>(SearchType::SUFFIX)})
    ->Args({1000, 5, static_cast<int>(SearchType::SUFFIX)})
    ->Args({10000, 3, static_cast<int>(SearchType::SUFFIX)})
    ->Args({10000, 5, static_cast<int>(SearchType::SUFFIX)})
    ->Args({1000, 3, static_cast<int>(SearchType::INFIX)})
    ->Args({1000, 5, static_cast<int>(SearchType::INFIX)})
    ->Args({10000, 3, static_cast<int>(SearchType::INFIX)})
    ->Args({10000, 5, static_cast<int>(SearchType::INFIX)})
    ->ArgNames({"docs", "pattern_len", "search_type"})
    ->Unit(benchmark::kMicrosecond);

}  // namespace search

}  // namespace dfly
