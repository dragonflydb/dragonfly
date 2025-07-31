// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/maps_list.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <utility>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

class MapsListTest : public testing::Test {
 protected:
};

TEST_F(MapsListTest, BasicFunctionality) {
  MapsList<std::string, int> maps({"key1", "key2", "key3"});

  auto map1 = maps.CreateNewMap();
  map1["key1"] = 10;
  map1["key2"] = 20;

  auto map2 = maps.CreateNewMap();
  map2["key1"] = 30;
  map2["key3"] = 40;

  EXPECT_EQ(maps.Size(), 2);
  EXPECT_EQ(maps[0]["key1"], 10);
  EXPECT_EQ(maps[0]["key2"], 20);
  EXPECT_EQ(maps[1]["key1"], 30);
  EXPECT_EQ(maps[1]["key3"], 40);

  std::vector<std::string> keys = {"key1", "key2", "key3", "key4"};
  MapsList<std::string, int> maps2(keys.begin(), keys.end(), 10);
  EXPECT_EQ(maps2.Size(), 10);

  for (size_t i = 0; i < maps2.Size(); ++i) {
    auto map = maps2[i];
    // Use operator[] by key
    for (const auto& key : keys) {
      map[key] = i * 10;
    }

    // Use operator[] by index
    for (size_t j = 0; j < keys.size(); ++j) {
      EXPECT_EQ(map[keys[j]], i * 10);
    }

    // Use Contains and Find
    for (const auto& key : keys) {
      EXPECT_TRUE(map.Contains(key));
      EXPECT_NE(map.Find(key), map.end());
    }

    EXPECT_FALSE(map.Contains("nonexistent_key"));
  }
}

TEST_F(MapsListTest, InsertNewMaps) {
  std::vector<std::string> keys;
  keys.reserve(500);
  for (int i = 0; i < 500; ++i) {
    keys.push_back("key" + std::to_string(i));
  }

  MapsList<std::string, int> maps(keys.begin(), keys.end());
  for (size_t i = 0; i < 500; ++i) {
    auto map = maps.CreateNewMap();
    size_t j = 0;
    for (const auto& key : keys) {
      map[key] = i * 10 + j;
      j++;
    }
  }

  EXPECT_EQ(maps.Size(), 500);
  for (size_t i = 0; i < maps.Size(); ++i) {
    auto map = maps[i];
    size_t j = 0;
    for (const auto& values : map) {
      EXPECT_LE(j, keys.size());
      EXPECT_EQ(values, i * 10 + j);
      j++;
    }
  }

  // Second wave
  for (size_t i = 500; i < 1000; ++i) {
    auto map = maps.CreateNewMap();
    size_t j = 0;
    for (const auto& key : keys) {
      map[key] = i * 20 + j;
      j++;
    }
  }

  EXPECT_EQ(maps.Size(), 1000);
  for (size_t i = 500; i < maps.Size(); ++i) {
    auto map = maps[i];
    size_t j = 0;
    for (const auto& values : map) {
      EXPECT_LE(j, keys.size());
      EXPECT_EQ(values, i * 20 + j);
      j++;
    }
  }
}

}  // namespace dfly
