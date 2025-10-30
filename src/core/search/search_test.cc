// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <benchmark/benchmark.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <memory_resource>
#include <random>

#include "absl/base/macros.h"
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

// Used for NumericIndex benchmarks.
// The value is used to determine the maximum size of a range block in the range tree.
constexpr size_t kMaxRangeBlockSize = 500000;

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

struct SchemaFieldInitializer {
  SchemaFieldInitializer(std::string_view name, SchemaField::FieldType type)
      : name{name}, type{type} {
    switch (type) {
      case SchemaField::TAG:
        special_params = SchemaField::TagParams{};
        break;
      case SchemaField::TEXT:
        special_params = SchemaField::TextParams{};
        break;
      case SchemaField::NUMERIC:
        special_params = SchemaField::NumericParams{};
        break;
      case SchemaField::VECTOR:
        special_params = SchemaField::VectorParams{};
        break;
      case SchemaField::GEO:
        break;
    }
  }

  SchemaFieldInitializer(std::string_view name, SchemaField::FieldType type,
                         SchemaField::ParamsVariant special_params)
      : name{name}, type{type}, special_params{special_params} {
  }

  std::string_view name;
  SchemaField::FieldType type;
  SchemaField::ParamsVariant special_params{std::monostate{}};
};

Schema MakeSimpleSchema(initializer_list<SchemaFieldInitializer> ilist) {
  Schema schema;
  for (auto ifield : ilist) {
    auto& field = schema.fields[ifield.name];
    field = {ifield.type, 0, string{ifield.name}, ifield.special_params};
  }
  return schema;
}

class SearchTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
    // Initialize SimSIMD runtime for tests that may exercise vector kernels
    InitSimSIMD();
  }

  SearchTest() {
    PrepareSchema({{"field", SchemaField::TEXT}});
  }

  ~SearchTest() {
    EXPECT_EQ(entries_.size(), 0u) << "Missing check";
  }

  void PrepareSchema(initializer_list<SchemaFieldInitializer> ilist) {
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

class SearchRaxTest
    : public SearchTest,
      public testing::WithParamInterface<pair<bool /* build suffix trie */, bool /* tag index */>> {
};

TEST_P(SearchRaxTest, SuffixInfix) {
  auto [with_trie, use_tag] = GetParam();
  Schema schema = MakeSimpleSchema({{"title", use_tag ? SchemaField::TAG : SchemaField::TEXT}});
  if (use_tag) {
    schema.fields["title"].special_params = SchemaField::TagParams{.with_suffixtrie = with_trie};
  } else {
    schema.fields["title"].special_params = SchemaField::TextParams{.with_suffixtrie = with_trie};
  }

  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};
  SearchAlgorithm algo{};
  QueryParams params;

  vector<string> documents = {"Berries",     "BlueBeRRies", "Blackberries", "APPLES",
                              "CranbeRRies", "Wolfberry",   "StraWberry"};
  for (size_t i = 0; i < documents.size(); i++) {
    MockedDocument doc{{{"title", documents[i]}}};
    indices.Add(i, doc);
  }

  auto prepare = [&, use_tag = use_tag](string q) {
    if (use_tag)
      q = "@title:{"s + q + "}"s;
    algo.Init(q, &params);
  };

  // suffix queries

  prepare("*Es");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3, 4));

  prepare("*beRRies");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 4));

  prepare("*les");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(3));

  prepare("*lueBERRies");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1));

  prepare("*berrY");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(5, 6));

  // infix queries

  prepare("*berr*");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 4, 5, 6));

  prepare("*ANB*");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(4));

  prepare("*berries*");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 4));

  prepare("*bL*");
  EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1, 2));
}

INSTANTIATE_TEST_SUITE_P(NoTrieText, SearchRaxTest, testing::Values(pair{false, false}));
INSTANTIATE_TEST_SUITE_P(WithTrieText, SearchRaxTest, testing::Values(pair{true, false}));
INSTANTIATE_TEST_SUITE_P(NoTrieTag, SearchRaxTest, testing::Values(pair{false, true}));
INSTANTIATE_TEST_SUITE_P(WithTrieTag, SearchRaxTest, testing::Values(pair{true, true}));

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

TEST_P(KnnTest, IP) {
  // Test with normalized unit vectors for IP distance
  // Using unit vectors pointing in different directions
  const pair<float, float> kTestCoords[] = {
      {1.0f, 0.0f}, {0.0f, 1.0f}, {-1.0f, 0.0f}, {0.0f, -1.0f}};

  auto schema = MakeSimpleSchema({{"pos", SchemaField::VECTOR}});
  schema.fields["pos"].special_params =
      SchemaField::VectorParams{GetParam(), 2, VectorSimilarity::IP};
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  for (size_t i = 0; i < ABSL_ARRAYSIZE(kTestCoords); i++) {
    string coords = ToBytes({kTestCoords[i].first, kTestCoords[i].second});
    MockedDocument doc{Map{{"pos", coords}}};
    indices.Add(i, doc);
  }

  SearchAlgorithm algo{};
  QueryParams params;

  // Query with vector pointing right - should find exact match (highest dot product)
  {
    params["vec"] = ToBytes({1.0f, 0.0f});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0));
  }

  // Query with vector pointing up - should find exact match (highest dot product)
  {
    params["vec"] = ToBytes({0.0f, 1.0f});
    algo.Init("* =>[KNN 1 @pos $vec]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(1));
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

TEST_F(SearchTest, GeoSearch) {
  auto schema = MakeSimpleSchema({{"name", SchemaField::TEXT}, {"location", SchemaField::GEO}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  indices.Add(0, MockedDocument(Map{{"name", "Mountain View"}, {"location", "-122.08, 37.386"}}));
  indices.Add(1, MockedDocument(Map{{"name", "Palo Alto"}, {"location", "-122.143, 37.444"}}));
  indices.Add(2, MockedDocument(Map{{"name", "San Jose"}, {"location", "-121.886, 37.338"}}));
  indices.Add(3, MockedDocument(Map{{"name", "San Francisco"}, {"location", "-122.419, 37.774"}}));

  SearchAlgorithm algo{};
  QueryParams params;

  // Search around Mount View 30 miles - San Francisco not included
  {
    algo.Init("@location:[-122.083 37.386 30 mi]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2));
  }

  // Search around Mount View 50 miles - all points included
  {
    algo.Init("@location:[-122.083 37.386 50 mi]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3));
  }

  // Return all indexes
  {
    algo.Init("@location:*", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(0, 1, 2, 3));
  }

  // Search around Mount View 50 miles - all points included and filter on prefix
  {
    algo.Init("San* @location:[-122.083 37.386 50 mi]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(2, 3));
  }

  // Add duplicate point of San Francisco and search again to include this point also
  {
    indices.Add(4,
                MockedDocument(Map{{"name", "San Francisco"}, {"location", "-122.419, 37.774"}}));
    algo.Init("San* @location:[-122.083 37.386 50 mi]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(2, 3, 4));
  }

  // Remove first index of San Francisco (id = 3) and search
  {
    indices.Remove(
        3, MockedDocument(Map{{"name", "San Francisco"}, {"location", "-122.419, 37.774"}}));
    algo.Init("San* @location:[-122.083 37.386 50 mi]", &params);
    EXPECT_THAT(algo.Search(&indices).ids, testing::UnorderedElementsAre(2, 4));
  }
}

INSTANTIATE_TEST_SUITE_P(KnnFlat, KnnTest, testing::Values(false));
// INSTANTIATE_TEST_SUITE_P(KnnHnsw, KnnTest, testing::Values(true));

TEST_F(SearchTest, VectorDistanceBasic) {
  // Test basic vector distance calculations
  std::vector<float> vec1 = {1.0f, 2.0f, 3.0f};
  std::vector<float> vec2 = {4.0f, 5.0f, 6.0f};

  // Test L2 distance
  float l2_dist = VectorDistance(vec1.data(), vec2.data(), 3, VectorSimilarity::L2);
  EXPECT_GT(l2_dist, 0.0f);
  EXPECT_LT(l2_dist, 10.0f);  // Should be reasonable value

  // Test Cosine distance
  float cos_dist = VectorDistance(vec1.data(), vec2.data(), 3, VectorSimilarity::COSINE);
  EXPECT_GE(cos_dist, 0.0f);
  EXPECT_LE(cos_dist, 2.0f);  // Cosine distance range

  // Test IP distance
  float ip_dist = VectorDistance(vec1.data(), vec2.data(), 3, VectorSimilarity::IP);
  // IP distance can be negative for non-normalized vectors
  EXPECT_NE(ip_dist, 0.0f);  // Should be non-zero for different vectors

  // Test identical vectors
  float l2_same = VectorDistance(vec1.data(), vec1.data(), 3, VectorSimilarity::L2);
  EXPECT_NEAR(l2_same, 0.0f, 1e-6);

  float cos_same = VectorDistance(vec1.data(), vec1.data(), 3, VectorSimilarity::COSINE);
  EXPECT_NEAR(cos_same, 0.0f, 1e-6);

  float ip_same = VectorDistance(vec1.data(), vec1.data(), 3, VectorSimilarity::IP);
  // For identical vectors: IP = 1 - dot_product(v, v) = 1 - ||v||^2
  // For vec1 = {1, 2, 3}: ||v||^2 = 1 + 4 + 9 = 14, so IP = 1 - 14 = -13
  EXPECT_LT(ip_same, 0.0f);  // Should be negative for non-normalized vectors
}

TEST_F(SearchTest, VectorDistanceConsistency) {
  // Test that results are consistent across multiple calls
  std::vector<float> vec1 = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
  std::vector<float> vec2 = {0.6f, 0.7f, 0.8f, 0.9f, 1.0f};

  float l2_dist1 = VectorDistance(vec1.data(), vec2.data(), 5, VectorSimilarity::L2);
  float l2_dist2 = VectorDistance(vec1.data(), vec2.data(), 5, VectorSimilarity::L2);
  EXPECT_EQ(l2_dist1, l2_dist2);

  float cos_dist1 = VectorDistance(vec1.data(), vec2.data(), 5, VectorSimilarity::COSINE);
  float cos_dist2 = VectorDistance(vec1.data(), vec2.data(), 5, VectorSimilarity::COSINE);
  EXPECT_EQ(cos_dist1, cos_dist2);

  float ip_dist1 = VectorDistance(vec1.data(), vec2.data(), 5, VectorSimilarity::IP);
  float ip_dist2 = VectorDistance(vec1.data(), vec2.data(), 5, VectorSimilarity::IP);
  EXPECT_EQ(ip_dist1, ip_dist2);
}

static void BM_VectorSearch(benchmark::State& state) {
  // Ensure SimSIMD dynamic dispatch is initialized for the benchmark
  InitSimSIMD();
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

  // Parser accepts any string as placeholder
  // Invalid vectors result in empty vector (dimension 0) which returns empty results
  ASSERT_TRUE(algo.Init("*=>[KNN 2 @v $b]", &query_params));

  // Search should return empty results for invalid vector
  auto result = algo.Search(&indices);
  EXPECT_TRUE(result.ids.empty());
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

// Helper function to generate random vector
static std::vector<float> GenerateRandomVector(size_t dims, unsigned seed = 42) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

  std::vector<float> vec(dims);
  for (size_t i = 0; i < dims; ++i) {
    vec[i] = dis(gen);
  }
  return vec;
}

static void BM_SearchDocIds(benchmark::State& state) {
  auto schema = MakeSimpleSchema({{"score", SchemaField::NUMERIC}, {"tag", SchemaField::TAG}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  default_random_engine rnd;
  const char* tag_vals[] = {"test", "example", "sample", "demo", "demo2"};
  uniform_int_distribution<size_t> tag_dist(0, ABSL_ARRAYSIZE(tag_vals) - 1);
  uniform_int_distribution<size_t> score_dist(0, 100);

  for (size_t i = 0; i < 1000; i++) {
    MockedDocument doc{
        Map{{"score", std::to_string(score_dist(rnd))}, {"tag", tag_vals[tag_dist(rnd)]}}};
    indices.Add(i, doc);
  }

  std::string queries[] = {"@tag:{test} @score:[10 50]", "@tag: *", "@score:*"};
  size_t query_type = state.range(0);
  CHECK_LT(query_type, ABSL_ARRAYSIZE(queries));
  CHECK(algo.Init(queries[query_type], &params));
  while (state.KeepRunning()) {
    auto result = algo.Search(&indices);
    CHECK(result.error.empty());
  }
}
BENCHMARK(BM_SearchDocIds)->Range(0, 2);

static void BM_SearchNumericIndexes(benchmark::State& state) {
  auto schema = MakeSimpleSchema({{"numeric", SchemaField::NUMERIC,
                                   SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  default_random_engine rnd;

  using NumericType = long long;
  uniform_int_distribution<NumericType> dist(std::numeric_limits<NumericType>::min(),
                                             std::numeric_limits<NumericType>::max());

  const size_t num_docs = state.range(0);
  for (size_t i = 0; i < num_docs; i++) {
    MockedDocument doc{Map{{"numeric", std::to_string(dist(rnd))}}};
    indices.Add(i, doc);
  }

  std::string queries[] = {"@numeric:[15 +inf]", "@numeric:[-inf 20]", "@numeric:[-inf +inf]",
                           "@numeric:[0 100000]"};

  std::unordered_map<size_t, std::vector<size_t>> expected_results_per_num_docs = {
      {10000, {4982, 5018, 10000, 0}},
      {100000, {49885, 50115, 100000, 0}},
      {1000000, {500853, 499147, 1000000, 0}},
  };

  while (state.KeepRunning()) {
    for (size_t i = 0; i < ABSL_ARRAYSIZE(queries); ++i) {
      const auto& query = queries[i];

      CHECK(algo.Init(query, &params));
      auto result = algo.Search(&indices);
      CHECK(result.error.empty());

      const size_t expected_result = expected_results_per_num_docs[num_docs][i];
      CHECK_EQ(result.total, expected_result);
      CHECK_EQ(result.ids.size(), expected_result);
    }
  }
}

BENCHMARK(BM_SearchNumericIndexes)->Arg(10000)->Arg(100000)->Arg(1000000)->ArgNames({"num_docs"});

static void BM_SearchNumericIndexesSmallRanges(benchmark::State& state) {
  auto schema = MakeSimpleSchema({{"numeric", SchemaField::NUMERIC,
                                   SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  default_random_engine rnd;

  using NumericType = uint16_t;
  uniform_int_distribution<NumericType> dist(0, std::numeric_limits<NumericType>::max());

  const size_t num_docs = state.range(0);
  // Insert zero values
  for (size_t i = 0; i < num_docs / 50; i++) {
    MockedDocument doc{Map{{"numeric", "0"}}};
    indices.Add(i, doc);
  }
  for (size_t i = num_docs / 50; i < num_docs; i++) {
    MockedDocument doc{Map{{"numeric", std::to_string(dist(rnd))}}};
    indices.Add(i, doc);
  }

  std::string queries[] = {"@numeric:[0 40000]", "@numeric:[-inf +inf]"};

  std::unordered_map<size_t, std::vector<size_t>> expected_results_per_num_docs = {
      {100000, {61939, 100000}},
      {1000000, {618365, 1000000}},
  };

  while (state.KeepRunning()) {
    for (size_t i = 0; i < ABSL_ARRAYSIZE(queries); ++i) {
      const auto& query = queries[i];

      CHECK(algo.Init(query, &params));
      auto result = algo.Search(&indices);
      CHECK(result.error.empty());

      const size_t expected_result = expected_results_per_num_docs[num_docs][i];
      CHECK_EQ(result.total, expected_result);
      CHECK_EQ(result.ids.size(), expected_result);
    }
  }
}

BENCHMARK(BM_SearchNumericIndexesSmallRanges)
    ->Arg(100000)   // One block
    ->Arg(1000000)  // Two blocks
    ->ArgNames({"num_docs"});

static void BM_SearchTwoNumericIndexes(benchmark::State& state) {
  auto schema = MakeSimpleSchema({
      {"numeric1", SchemaField::NUMERIC,
       SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}},
      {"numeric2", SchemaField::NUMERIC,
       SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}},
  });

  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  std::default_random_engine rnd;

  using NumericType = long long;
  uniform_int_distribution<NumericType> dist1(std::numeric_limits<NumericType>::min(),
                                              std::numeric_limits<NumericType>::max());
  uniform_int_distribution<NumericType> dist2(std::numeric_limits<NumericType>::min(),
                                              std::numeric_limits<NumericType>::max());

  const size_t num_docs = state.range(0);
  for (size_t i = 0; i < num_docs; ++i) {
    MockedDocument doc{Map{
        {"numeric1", std::to_string(dist1(rnd))},
        {"numeric2", std::to_string(dist2(rnd))},
    }};
    indices.Add(i, doc);
  }

  std::string queries[] = {absl::StrCat("@numeric1:[15 +inf] @numeric2:[-inf 20]"),
                           absl::StrCat("@numeric1:[-inf 20] @numeric2:[15 +inf]"),
                           absl::StrCat("@numeric1:[0 100000] @numeric2:[-100000 0]"),
                           absl::StrCat("@numeric1:[-100000 0] @numeric2:[0 100000]")};

  std::unordered_map<size_t, std::vector<size_t>> expected_results_per_num_docs = {
      {10000, {2508, 2507, 0, 0}},
      {100000, {25119, 25232, 0, 0}},
      {1000000, {250623, 250643, 0, 0}},
  };

  while (state.KeepRunning()) {
    for (size_t i = 0; i < ABSL_ARRAYSIZE(queries); ++i) {
      const auto& query = queries[i];

      CHECK(algo.Init(query, &params));
      auto result = algo.Search(&indices);
      CHECK(result.error.empty());

      const size_t expected_result = expected_results_per_num_docs[num_docs][i];
      CHECK_EQ(result.total, expected_result);
      CHECK_EQ(result.ids.size(), expected_result);
    }
  }
}

BENCHMARK(BM_SearchTwoNumericIndexes)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->ArgNames({"num_docs"});

static void BM_SearchNumericAndTagIndexes(benchmark::State& state) {
  auto schema = MakeSimpleSchema({{"tag", SchemaField::TAG},
                                  {"numeric", SchemaField::NUMERIC,
                                   SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  default_random_engine rnd;

  using NumericType = long long;
  uniform_int_distribution<NumericType> dist(std::numeric_limits<NumericType>::min(),
                                             std::numeric_limits<NumericType>::max());

  size_t tag_number = 0;
  const size_t max_tag_number = 1000;

  const size_t num_docs = state.range(0);
  for (size_t i = 0; i < num_docs; i++) {
    MockedDocument doc{
        Map{{"tag", absl::StrCat("tag", tag_number)}, {"numeric", std::to_string(dist(rnd))}}};
    indices.Add(i, doc);

    tag_number = (tag_number + 1) % max_tag_number;
  }

  std::string queries[] = {absl::StrCat("@tag:{tag230|tag3|tag942} @numeric:[15 +inf]"),
                           absl::StrCat("@tag:{tag1|tag829|tag236} @numeric:[-inf 20]"),
                           absl::StrCat("@tag:{tag0|tag999} @numeric:[-1000000 +inf]")};

  std::unordered_map<size_t, std::vector<size_t>> expected_results_per_num_docs = {
      {10000, {19, 16, 8}},
      {100000, {164, 157, 97}},
      {1000000, {1528, 1518, 1017}},
  };

  while (state.KeepRunning()) {
    for (size_t i = 0; i < ABSL_ARRAYSIZE(queries); ++i) {
      const auto& query = queries[i];

      CHECK(algo.Init(query, &params));
      auto result = algo.Search(&indices);
      CHECK(result.error.empty());

      const size_t expected_result = expected_results_per_num_docs[num_docs][i];
      CHECK_EQ(result.total, expected_result);
      CHECK_EQ(result.ids.size(), expected_result);
    }
  }
}

BENCHMARK(BM_SearchNumericAndTagIndexes)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->ArgNames({"num_docs"});

static void BM_SearchSeveralNumericAndTagIndexes(benchmark::State& state) {
  auto schema = MakeSimpleSchema({{"tag", SchemaField::TAG},
                                  {"numeric1", SchemaField::NUMERIC,
                                   SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}},
                                  {"numeric2", SchemaField::NUMERIC,
                                   SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}},
                                  {"numeric3", SchemaField::NUMERIC,
                                   SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}}});
  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  default_random_engine rnd;

  using NumericType = uint16_t;
  uniform_int_distribution<NumericType> dist(std::numeric_limits<NumericType>::min(),
                                             std::numeric_limits<NumericType>::max());

  const size_t num_docs = state.range(0);

  size_t tag_number = 0;
  const size_t max_tag_number = num_docs / 30;

  for (size_t i = 0; i < num_docs; i++) {
    MockedDocument doc{Map{{"tag", absl::StrCat("tag", tag_number)},
                           {"numeric1", std::to_string(dist(rnd))},
                           {"numeric2", std::to_string(dist(rnd))},
                           {"numeric3", std::to_string(dist(rnd))}}};
    indices.Add(i, doc);

    tag_number = (tag_number + 1) % max_tag_number;
  }

  std::string queries[] = {
      absl::StrCat(
          "@tag:{tag230|tag3} @numeric1:[0 10000] @numeric2:[20000 30000] @numeric3:[-1000 +inf]"),
      absl::StrCat("@tag:{tag829|tag236} @numeric1:[-inf 10000] @numeric2:[40000 +inf] "
                   "@numeric3:[10000 30000]"),
      absl::StrCat(
          "@tag:{tag0|tag999} @numeric1:[-inf +inf] @numeric2:[20 +inf] @numeric3:[1000 10000]")};

  std::unordered_map<size_t, std::vector<size_t>> expected_results_per_num_docs = {
      {10000, {1, 0, 4}},
      {100000, {1, 1, 10}},
      {1000000, {0, 1, 9}},
  };

  while (state.KeepRunning()) {
    for (size_t i = 0; i < ABSL_ARRAYSIZE(queries); ++i) {
      const auto& query = queries[i];

      CHECK(algo.Init(query, &params));
      auto result = algo.Search(&indices);
      CHECK(result.error.empty());

      const size_t expected_result = expected_results_per_num_docs[num_docs][i];
      CHECK_EQ(result.total, expected_result);
      CHECK_EQ(result.ids.size(), expected_result);
    }
  }
}

BENCHMARK(BM_SearchSeveralNumericAndTagIndexes)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->ArgNames({"num_docs"});

static void BM_SearchMergeEqualSets(benchmark::State& state) {
  auto schema = MakeSimpleSchema({
      {"numeric1", SchemaField::NUMERIC,
       SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}},
      {"numeric2", SchemaField::NUMERIC,
       SchemaField::NumericParams{.block_size = kMaxRangeBlockSize}},
  });

  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  SearchAlgorithm algo;
  QueryParams params;
  std::default_random_engine rnd;

  using NumericType = long long;
  uniform_int_distribution<NumericType> dist1(std::numeric_limits<NumericType>::min(),
                                              std::numeric_limits<NumericType>::max());
  uniform_int_distribution<NumericType> dist2(std::numeric_limits<NumericType>::min(),
                                              std::numeric_limits<NumericType>::max());

  const size_t num_docs = state.range(0);
  for (size_t i = 0; i < num_docs; ++i) {
    MockedDocument doc{Map{
        {"numeric1", std::to_string(dist1(rnd))},
        {"numeric2", std::to_string(dist2(rnd))},
    }};
    indices.Add(i, doc);
  }

  std::string query = absl::StrCat("@numeric1:[-inf +inf] @numeric2:[-inf +inf]");

  while (state.KeepRunning()) {
    CHECK(algo.Init(query, &params));
    auto result = algo.Search(&indices);
    CHECK(result.error.empty());

    // All documents should match both conditions, so total should equal num_docs
    CHECK_EQ(result.total, num_docs);
    CHECK_EQ(result.ids.size(), num_docs);
  }
}

BENCHMARK(BM_SearchMergeEqualSets)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->ArgNames({"num_docs"});

static void BM_SearchRangeTreeSplits(benchmark::State& state) {
  auto schema = MakeSimpleSchema({
      {"num", SchemaField::NUMERIC, SchemaField::NumericParams{}},
  });

  FieldIndices indices{schema, kEmptyOptions, PMR_NS::get_default_resource(), nullptr};

  const size_t batch_size = state.range(0);
  std::default_random_engine rnd;

  using NumericType = long long;
  uniform_int_distribution<NumericType> dist(0, batch_size + 1);

  size_t doc_index = 0;
  while (state.KeepRunning()) {
    for (size_t i = 0; i < batch_size; i++) {
      MockedDocument doc{Map{{"num", std::to_string(dist(rnd))}}};
      indices.Add(doc_index++, doc);
    }
  }
}

BENCHMARK(BM_SearchRangeTreeSplits)
    ->Arg(100000)
    ->Arg(1000000)
    ->Arg(3000000)
    ->ArgNames({"batch_size"});

// Semantics test for cosine on zero vectors (independent of SimSIMD)
TEST(CosineDistanceTest, ZeroVectors) {
  const size_t dims = 128;
  std::vector<float> zero(dims, 0.0f);
  float d = VectorDistance(zero.data(), zero.data(), dims, VectorSimilarity::COSINE);
  EXPECT_EQ(d, 0.0f);
}

// Unified vector distance benchmarks using VectorDistance function
static void BM_VectorDistance(benchmark::State& state) {
  // Ensure SimSIMD dynamic dispatch is initialized for the benchmark
  InitSimSIMD();
  size_t dims = state.range(0);
  size_t num_pairs = state.range(1);
  VectorSimilarity sim = static_cast<VectorSimilarity>(state.range(2));

  std::vector<std::vector<float>> vectors_a, vectors_b;
  vectors_a.reserve(num_pairs);
  vectors_b.reserve(num_pairs);

  for (size_t i = 0; i < num_pairs; ++i) {
    vectors_a.push_back(GenerateRandomVector(dims, i));
    vectors_b.push_back(GenerateRandomVector(dims, i + 1000));
  }

  size_t pair_idx = 0;
  for (auto _ : state) {
    float distance =
        VectorDistance(vectors_a[pair_idx].data(), vectors_b[pair_idx].data(), dims, sim);
    benchmark::DoNotOptimize(distance);
    pair_idx = (pair_idx + 1) % num_pairs;
  }

  state.counters["dims"] = dims;
  state.counters["pairs"] = num_pairs;

  std::string sim_name = (sim == VectorSimilarity::L2)       ? "L2"
                         : (sim == VectorSimilarity::COSINE) ? "Cosine"
                                                             : "IP";
  state.SetLabel(sim_name);
}

// Intensive benchmark with batch processing
static void BM_VectorDistance_Intensive(benchmark::State& state) {
  // Ensure SimSIMD dynamic dispatch is initialized for the benchmark
  InitSimSIMD();
  size_t dims = 512;  // Fixed medium size
  size_t batch_size = 1000;
  VectorSimilarity sim = static_cast<VectorSimilarity>(state.range(0));

  std::vector<std::vector<float>> vectors_a, vectors_b;
  vectors_a.reserve(batch_size);
  vectors_b.reserve(batch_size);

  for (size_t i = 0; i < batch_size; ++i) {
    vectors_a.push_back(GenerateRandomVector(dims, i));
    vectors_b.push_back(GenerateRandomVector(dims, i + 4000));
  }

  size_t total_ops = 0;
  while (state.KeepRunning()) {
    for (size_t i = 0; i < batch_size; ++i) {
      float distance = VectorDistance(vectors_a[i].data(), vectors_b[i].data(), dims, sim);
      benchmark::DoNotOptimize(distance);
      ++total_ops;
    }
  }

  state.counters["ops"] = total_ops;
  state.counters["ops_per_sec"] = benchmark::Counter(total_ops, benchmark::Counter::kIsRate);

  std::string sim_name = (sim == VectorSimilarity::L2)       ? "L2"
                         : (sim == VectorSimilarity::COSINE) ? "Cosine"
                                                             : "IP";
  state.SetLabel(sim_name + "_Intensive");
}

// Benchmark declarations
BENCHMARK(BM_VectorDistance)
    // Small vectors - L2 Distance
    ->Args({32, 100, static_cast<int>(VectorSimilarity::L2)})
    ->Args({32, 1000, static_cast<int>(VectorSimilarity::L2)})
    ->Args({32, 10000, static_cast<int>(VectorSimilarity::L2)})
    // Medium vectors - L2 Distance
    ->Args({128, 100, static_cast<int>(VectorSimilarity::L2)})
    ->Args({128, 1000, static_cast<int>(VectorSimilarity::L2)})
    ->Args({128, 10000, static_cast<int>(VectorSimilarity::L2)})
    // Large vectors - L2 Distance
    ->Args({512, 100, static_cast<int>(VectorSimilarity::L2)})
    ->Args({512, 1000, static_cast<int>(VectorSimilarity::L2)})
    ->Args({512, 5000, static_cast<int>(VectorSimilarity::L2)})
    // Very large vectors - L2 Distance
    ->Args({1536, 100, static_cast<int>(VectorSimilarity::L2)})
    ->Args({1536, 1000, static_cast<int>(VectorSimilarity::L2)})

    // Small vectors - Cosine Distance
    ->Args({32, 100, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({32, 1000, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({32, 10000, static_cast<int>(VectorSimilarity::COSINE)})
    // Medium vectors - Cosine Distance
    ->Args({128, 100, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({128, 1000, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({128, 10000, static_cast<int>(VectorSimilarity::COSINE)})
    // Large vectors - Cosine Distance
    ->Args({512, 100, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({512, 1000, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({512, 5000, static_cast<int>(VectorSimilarity::COSINE)})
    // Very large vectors - Cosine Distance
    ->Args({1536, 100, static_cast<int>(VectorSimilarity::COSINE)})
    ->Args({1536, 1000, static_cast<int>(VectorSimilarity::COSINE)})

    // Small vectors - IP Distance
    ->Args({32, 100, static_cast<int>(VectorSimilarity::IP)})
    ->Args({32, 1000, static_cast<int>(VectorSimilarity::IP)})
    ->Args({32, 10000, static_cast<int>(VectorSimilarity::IP)})
    // Medium vectors - IP Distance
    ->Args({128, 100, static_cast<int>(VectorSimilarity::IP)})
    ->Args({128, 1000, static_cast<int>(VectorSimilarity::IP)})
    ->Args({128, 10000, static_cast<int>(VectorSimilarity::IP)})
    // Large vectors - IP Distance
    ->Args({512, 100, static_cast<int>(VectorSimilarity::IP)})
    ->Args({512, 1000, static_cast<int>(VectorSimilarity::IP)})
    ->Args({512, 5000, static_cast<int>(VectorSimilarity::IP)})
    // Very large vectors - IP Distance
    ->Args({1536, 100, static_cast<int>(VectorSimilarity::IP)})
    ->Args({1536, 1000, static_cast<int>(VectorSimilarity::IP)})
    ->ArgNames({"dims", "pairs", "similarity"})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_VectorDistance_Intensive)
    ->Arg(static_cast<int>(VectorSimilarity::L2))
    ->Arg(static_cast<int>(VectorSimilarity::COSINE))
    ->Arg(static_cast<int>(VectorSimilarity::IP))
    ->ArgNames({"similarity_type"})
    ->Unit(benchmark::kMicrosecond);

}  // namespace search
}  // namespace dfly
