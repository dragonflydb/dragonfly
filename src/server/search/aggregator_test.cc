// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/aggregator.h"

#include "base/gtest.h"

namespace dfly::aggregate {

using namespace std::string_literals;

TEST(AggregatorTest, Sort) {
  std::vector<DocValues> values = {
      DocValues{{"a", 1.0}},
      DocValues{{"a", 0.5}},
      DocValues{{"a", 1.5}},
  };
  PipelineStep steps[] = {MakeSortStep("a", false)};

  auto result = Process(values, steps);

  EXPECT_TRUE(result);
  EXPECT_EQ(result->at(0)["a"], Value(0.5));
  EXPECT_EQ(result->at(1)["a"], Value(1.0));
  EXPECT_EQ(result->at(2)["a"], Value(1.5));
}

TEST(AggregatorTest, Limit) {
  std::vector<DocValues> values = {
      DocValues{{"i", 1.0}},
      DocValues{{"i", 2.0}},
      DocValues{{"i", 3.0}},
      DocValues{{"i", 4.0}},
  };
  PipelineStep steps[] = {MakeLimitStep(1, 2)};

  auto result = Process(values, steps);

  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);
  EXPECT_EQ(result->at(0)["i"], Value(2.0));
  EXPECT_EQ(result->at(1)["i"], Value(3.0));
}

TEST(AggregatorTest, SimpleGroup) {
  std::vector<DocValues> values = {
      DocValues{{"i", 1.0}, {"tag", "odd"}},
      DocValues{{"i", 2.0}, {"tag", "even"}},
      DocValues{{"i", 3.0}, {"tag", "odd"}},
      DocValues{{"i", 4.0}, {"tag", "even"}},
  };

  std::string_view fields[] = {"tag"};
  PipelineStep steps[] = {MakeGroupStep(fields, {})};

  auto result = Process(values, steps);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);

  EXPECT_EQ(result->at(0).size(), 1);
  std::set<Value> groups{result->at(0)["tag"], result->at(1)["tag"]};
  std::set<Value> expected{"even", "odd"};
  EXPECT_EQ(groups, expected);
}

TEST(AggregatorTest, GroupWithReduce) {
  std::vector<DocValues> values;
  // range from 0 to 9 inclusive
  for (size_t i = 0; i < 10; i++) {
    values.push_back(DocValues{
        {"i", double(i)},
        {"half-i", double(i / 4)},
        {"tag", i % 2 == 0 ? "even" : "odd"},
    });
  }

  std::string_view fields[] = {"tag"};
  std::vector<Reducer> reducers = {
      Reducer{"", "count", FindReducerFunc(ReducerFunc::COUNT)},
      Reducer{"i", "sum-i", FindReducerFunc(ReducerFunc::SUM)},
      Reducer{"half-i", "distinct-hi", FindReducerFunc(ReducerFunc::COUNT_DISTINCT)},
      Reducer{"null-field", "distinct-null", FindReducerFunc(ReducerFunc::COUNT_DISTINCT)}};
  PipelineStep steps[] = {MakeGroupStep(fields, std::move(reducers))};

  auto result = Process(values, steps);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);

  // Reorder even first
  if (result->at(0).at("tag") == Value("odd"))
    std::swap(result->at(0), result->at(1));

  // Even
  EXPECT_EQ(result->at(0).at("count"), Value{(double)5});
  EXPECT_EQ(result->at(0).at("sum-i"), Value{(double)2 + 4 + 6 + 8});
  EXPECT_EQ(result->at(0).at("distinct-hi"), Value{(double)3});
  EXPECT_EQ(result->at(0).at("distinct-null"), Value{(double)1});

  // Odd
  EXPECT_EQ(result->at(1).at("count"), Value{(double)5});
  EXPECT_EQ(result->at(1).at("sum-i"), Value{(double)1 + 3 + 5 + 7 + 9});
  EXPECT_EQ(result->at(1).at("distinct-hi"), Value{(double)3});
  EXPECT_EQ(result->at(1).at("distinct-null"), Value{(double)1});
}

}  // namespace dfly::aggregate
