// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/top_keys.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"

using ::testing::Pair;
using ::testing::UnorderedElementsAre;

namespace dfly {

TEST(TopKeysTest, Basic) {
  TopKeys top_keys({.min_key_count_to_record = 1});
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 1)));
}

TEST(TopKeysTest, MultiTouch) {
  TopKeys top_keys({.min_key_count_to_record = 1});
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 1)));
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 2)));
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 3)));
}

TEST(TopKeysTest, MinKeyCountToRecord) {
  TopKeys top_keys({.min_key_count_to_record = 3});
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre());
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre());
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 3)));
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 4)));
  top_keys.Touch("key1");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 5)));
}

TEST(TopKeysTest, MultiKeys) {
  TopKeys top_keys({.min_key_count_to_record = 1});
  top_keys.Touch("key1");
  top_keys.Touch("key2");
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 1), Pair("key2", 1)));
}

TEST(TopKeysTest, BucketCollision) {
  TopKeys top_keys({.buckets = 1, .min_key_count_to_record = 1});
  for (int i = 0; i < 5; ++i) {
    top_keys.Touch("key1");
  }
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 5)));

  for (int i = 0; i < 100; ++i) {
    top_keys.Touch("key2");
  }

  auto top_keys_table = top_keys.GetTopKeys();
  EXPECT_EQ(top_keys_table.size(), 1);
  EXPECT_LE(top_keys_table["key2"], 100);
  EXPECT_GE(top_keys_table["key2"], 50);

  // Touching "key1" should *not* replace "key2".
  top_keys.Touch("key1");
  EXPECT_FALSE(top_keys.GetTopKeys().contains("key1"));
}

TEST(TopKeysTest, BucketCollisionAggressiveDecay) {
  TopKeys top_keys({.buckets = 1, .decay_base = 1.0, .min_key_count_to_record = 1});
  for (int i = 0; i < 5; ++i) {
    top_keys.Touch("key1");
  }
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 5)));

  for (int i = 0; i < 100; ++i) {
    top_keys.Touch("key2");
  }
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key2", 96)));
}

TEST(TopKeysTest, BucketCollisionHesitantDecay) {
  TopKeys top_keys({.buckets = 1, .decay_base = 1000.0, .min_key_count_to_record = 1});
  for (int i = 0; i < 5; ++i) {
    top_keys.Touch("key1");
  }
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 5)));

  for (int i = 0; i < 100; ++i) {
    top_keys.Touch("key2");
  }
  // "key2" will never replace "key1", as the decay practically never happens (1000^-5)
  EXPECT_THAT(top_keys.GetTopKeys(), UnorderedElementsAre(Pair("key1", 5)));
}

TEST(TopKeysTest, SavedByMultipleArrays) {
  // This test is not trivial. It tests that having multiple arrays inside TopKeys saves keys in
  // case of collision. The way it does it is by inserting an arbitrary key (= "key"), and then (at
  // runtime) finding another key which *does* collide with that key.
  //
  // Once we've found such a key, we create another TopKeys instance, but this time with 10 arrays
  // which should mean that for some hash value, the keys won't be present in the same bucket.

  std::string collision_key;

  TopKeys::Options options(
      {.buckets = 2, .arrays = 1, .decay_base = 1, .min_key_count_to_record = 1});
  {
    TopKeys top_keys(options);

    // Insert some key
    top_keys.Touch("key");
    top_keys.Touch("key");

    // Find a key with a collision
    int i = 0;
    while (true) {
      collision_key = absl::StrCat("key", i);
      top_keys.Touch(collision_key);
      if (!top_keys.GetTopKeys().contains(collision_key)) {
        break;
      }
      ++i;
    }
  }

  options.arrays = 10;
  {
    TopKeys top_keys(options);

    // Insert some key
    top_keys.Touch("key");
    top_keys.Touch("key");

    // Insert collision key, expect result to be present
    top_keys.Touch(collision_key);
    EXPECT_THAT(top_keys.GetTopKeys(),
                UnorderedElementsAre(Pair("key", 2), Pair(collision_key, 1)));
  }
}

}  // end of namespace dfly
