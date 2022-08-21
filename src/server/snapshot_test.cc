// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;
using absl::StrCat;

namespace dfly {

class SnapshotTest : public Test {
 protected:
};

bool IsValidSaveSchedule(string_view time);
bool DoesTimeMatchSpecifier(string_view time, unsigned int hour, unsigned int min);

TEST_F(SnapshotTest, InvalidTimes) {
  EXPECT_FALSE(IsValidSaveSchedule("24:00"));
  EXPECT_FALSE(IsValidSaveSchedule("00:60"));
  EXPECT_FALSE(IsValidSaveSchedule("100:00"));
  EXPECT_FALSE(IsValidSaveSchedule("00:100"));

  // invalid times with regex
  EXPECT_FALSE(IsValidSaveSchedule("23:6*"));

  // Minutes must be zero padded
  EXPECT_FALSE(IsValidSaveSchedule("00:9"));

  // No separators or start with separator
  EXPECT_FALSE(IsValidSaveSchedule(":12"));
  EXPECT_FALSE(IsValidSaveSchedule("1234"));
  EXPECT_FALSE(IsValidSaveSchedule("1"));

  // Negative numbers / non numeric characters
  EXPECT_FALSE(IsValidSaveSchedule("-1:-2"));
  EXPECT_FALSE(IsValidSaveSchedule("12:34b"));

  // Wildcards for full times
  EXPECT_FALSE(IsValidSaveSchedule("12*:09"));
  EXPECT_FALSE(IsValidSaveSchedule("23:45*"));
}

TEST_F(SnapshotTest, ValidTimes) {
  // Test endpoints
  EXPECT_TRUE(IsValidSaveSchedule("23:59"));
  EXPECT_TRUE(IsValidSaveSchedule("00:00"));
  // hours don't need to be zero padded
  EXPECT_TRUE(IsValidSaveSchedule("0:00"));

  // wildcard checks
  EXPECT_TRUE(IsValidSaveSchedule("1*:09"));
  EXPECT_TRUE(IsValidSaveSchedule("*9:23"));
  EXPECT_TRUE(IsValidSaveSchedule("23:*1"));
  EXPECT_TRUE(IsValidSaveSchedule("18:1*"));

  // Greedy wildcards
  EXPECT_TRUE(IsValidSaveSchedule("*:12"));
  EXPECT_TRUE(IsValidSaveSchedule("9:*"));
  EXPECT_TRUE(IsValidSaveSchedule("09:*"));
  EXPECT_TRUE(IsValidSaveSchedule("*:*"));
}

TEST_F(SnapshotTest, TimeMatches) {
  EXPECT_TRUE(DoesTimeMatchSpecifier("12:34", 12, 34));
  EXPECT_TRUE(DoesTimeMatchSpecifier("2:34", 2, 34));
  EXPECT_TRUE(DoesTimeMatchSpecifier("2:04", 2, 4));

  EXPECT_FALSE(DoesTimeMatchSpecifier("12:34", 2, 4));
  EXPECT_FALSE(DoesTimeMatchSpecifier("2:34", 3, 34));
  EXPECT_FALSE(DoesTimeMatchSpecifier("2:04", 3, 5));

  // Check wildcard for one slot
  for (int i = 0; i < 9; ++i)
    EXPECT_TRUE(DoesTimeMatchSpecifier("1*:34", 10 + i, 34));

  EXPECT_TRUE(DoesTimeMatchSpecifier("*3:04", 13, 4));
  EXPECT_TRUE(DoesTimeMatchSpecifier("*3:04", 23, 4));

  // do the same checks for the minutes
  for (int i = 0; i < 9; ++i)
    EXPECT_TRUE(DoesTimeMatchSpecifier("10:3*", 10, 30 + i));

  for (int i = 0; i < 6; ++i)
    EXPECT_TRUE(DoesTimeMatchSpecifier("13:*4", 13, (10 * i) + 4));

  // check greedy wildcards
  for (int i = 0; i < 24; ++i)
    EXPECT_TRUE(DoesTimeMatchSpecifier("*:12", i, 12));

  for (int i = 0; i < 60; ++i)
    EXPECT_TRUE(DoesTimeMatchSpecifier("3:*", 3, i));

  for (int i = 0; i < 24; ++i)
    for (int j = 0; j < 60; ++j)
      EXPECT_TRUE(DoesTimeMatchSpecifier("*:*", i, j));
}

}  // namespace dfly
