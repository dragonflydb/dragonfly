// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_data.h"

#include "base/gtest.h"

namespace dfly {

class ClusterDataTest : public ::testing::Test {
 protected:
  ClusterDataTest() {
  }
};

TEST_F(ClusterDataTest, KeyTagTest) {
  std::string key = "{user1000}.following";
  ASSERT_EQ("user1000", KeyTag(key));

  key = " foo{}{bar}";
  ASSERT_EQ(key, KeyTag(key));

  key = "foo{{bar}}zap";
  ASSERT_EQ("{bar", KeyTag(key));

  key = "foo{bar}{zap}";
  ASSERT_EQ("bar", KeyTag(key));

  key = "{}foo{bar}{zap}";
  ASSERT_EQ(key, KeyTag(key));
}

}  // namespace dfly
