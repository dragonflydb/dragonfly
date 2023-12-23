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

  auto result = Execute(values, steps);

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

  auto result = Execute(values, steps);

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
  PipelineStep steps[] = {MakeGroupStep(fields)};

  auto result = Execute(values, steps);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);

  EXPECT_EQ(result->at(0).size(), 1);
  std::set<Value> groups{result->at(0)["tag"], result->at(1)["tag"]};
  std::set<Value> expected{"even", "odd"};
  EXPECT_EQ(groups, expected);
}

}  // namespace dfly::aggregate
