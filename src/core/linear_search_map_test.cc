// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/linear_search_map.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <utility>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

class LinearSearchMapTest : public testing::Test {
 protected:
};

TEST_F(LinearSearchMapTest, Insert) {
  LinearSearchMap<int, double> map;

  for (int i = 0; i < 100; ++i) {
    map.Insert(i, i * 1.1);
  }

  for (int i = 199; i >= 100; --i) {
    map.Insert(i, i * 12.1);
  }

  for (int i = 0; i < 200; ++i) {
    auto it = map.Find(i);
    EXPECT_NE(it, map.end());
    EXPECT_TRUE(map.Contains(i));

    EXPECT_EQ(it->second, (i < 100) ? i * 1.1 : i * 12.1);
  }
}

TEST_F(LinearSearchMapTest, Emplace) {
  struct Value {
    Value(double value_, std::string str_) : value(value_), str(std::move(str_)) {
    }

    double value;
    std::string str;
  };

  LinearSearchMap<int, Value> map;

  for (int i = 0; i < 100; ++i) {
    map.Emplace(i, i * 1.1, "value_" + std::to_string(i));
  }

  for (int i = 199; i >= 100; --i) {
    map.Emplace(i, i * 12.1, "value_" + std::to_string(i));
  }

  for (int i = 0; i < 200; ++i) {
    auto it = map.Find(i);
    EXPECT_NE(it, map.end());
    EXPECT_TRUE(map.Contains(i));

    EXPECT_EQ(it->second.value, (i < 100) ? i * 1.1 : i * 12.1);
    EXPECT_EQ(it->second.str, "value_" + std::to_string(i));
  }
}

TEST_F(LinearSearchMapTest, EraseSimple) {
  LinearSearchMap<int, double> map;

  for (int i = 0; i < 200; ++i) {
    map.Insert(i, i * 1.1);
  }

  // Erase by iterator
  for (int i = 0; i < 100; ++i) {
    auto it = map.Find(i);
    EXPECT_NE(it, map.end());
    EXPECT_TRUE(map.Contains(i));

    map.Erase(it);
    EXPECT_FALSE(map.Contains(i));
  }

  // Erase by key
  for (int i = 100; i < 200; ++i) {
    EXPECT_TRUE(map.Contains(i));
    map.Erase(i);
    EXPECT_FALSE(map.Contains(i));
  }

  EXPECT_TRUE(map.Empty());
}

TEST_F(LinearSearchMapTest, Erase) {
  std::unordered_map<int, double> expected_map;
  LinearSearchMap<int, double> map;

  // First wave insert / erase
  for (int i = 0; i < 300; i++) {
    double value = i * 1.1;
    map.Insert(i, value);
    expected_map[i] = value;
  }

  for (int i = 0; i < 300; i += 3) {
    EXPECT_TRUE(map.Contains(i));
    map.Erase(i);
    expected_map.erase(i);
    EXPECT_FALSE(map.Contains(i));
  }

  // Second wave insert / erase
  for (int i = 300; i < 600; i++) {
    double value = i * 2.2;
    map.Insert(i, value);
    expected_map[i] = value;
  }

  for (int i = 300; i < 600; i += 5) {
    EXPECT_TRUE(map.Contains(i));
    map.Erase(i);
    expected_map.erase(i);
    EXPECT_FALSE(map.Contains(i));
  }

  // Erase all remaining elements
  while (!expected_map.empty()) {
    size_t index = 0;
    const size_t step = 7;

    for (auto it = expected_map.begin(); it != expected_map.end(); ++index) {
      auto [i, value] = *it;
      EXPECT_TRUE(map.Contains(i));
      EXPECT_EQ(map.Find(i)->second, value);

      if (index % step == 0) {
        map.Erase(i);
        it = expected_map.erase(it);
      } else {
        ++it;
      }
    }
  }

  EXPECT_TRUE(map.Empty());
}

}  // namespace dfly
