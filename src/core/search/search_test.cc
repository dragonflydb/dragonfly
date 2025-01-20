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

    FieldIndices index{schema_, kEmptyOptions, PMR_NS::get_default_resource()};

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

  FieldIndices indices{schema, options, PMR_NS::get_default_resource()};
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
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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

class KnnTest : public SearchTest, public testing::WithParamInterface<bool /* hnsw */> {};

TEST_P(KnnTest, Simple1D) {
  auto schema = MakeSimpleSchema({{"even", SchemaField::TAG}, {"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params = SchemaField::VectorParams{GetParam(), 1};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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
}

TEST_P(KnnTest, Simple2D) {
  // Square:
  // 3      2
  //    4
  // 0      1
  const pair<float, float> kTestCoords[] = {{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0.5, 0.5}};

  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params = SchemaField::VectorParams{GetParam(), 2};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource()};

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

}  // namespace search

}  // namespace dfly
