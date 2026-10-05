// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/shutdown_watchdog.h"

#include <gtest/gtest.h>

#include <chrono>

namespace dfly {
namespace {

using namespace std::chrono_literals;

class ShutdownStallDetectorTest : public ::testing::Test {
 protected:
  const char* CheckAt(std::chrono::seconds offset) {
    return detector_.Check(progress_, start_ + offset);
  }

  const DetectStall::Clock::time_point start_ = DetectStall::Clock::now();
  ShutdownProgress progress_;
  DetectStall detector_;
};

TEST_F(ShutdownStallDetectorTest, IgnoresIdle) {
  EXPECT_EQ(CheckAt(0s), nullptr);
  EXPECT_EQ(CheckAt(100s), nullptr);
}

TEST_F(ShutdownStallDetectorTest, ReportsStallOncePerEpisode) {
  constexpr auto report_line = __LINE__ + 1;
  progress_.Update("slow cleanup");
  EXPECT_STREQ(progress_.file_name(), __FILE__);
  EXPECT_EQ(progress_.line(), report_line);
  EXPECT_EQ(CheckAt(0s), nullptr);
  EXPECT_EQ(CheckAt(19s), nullptr);
  EXPECT_STREQ(CheckAt(20s), "slow cleanup");
  EXPECT_EQ(CheckAt(100s), nullptr);

  progress_.Update("slow cleanup");
  EXPECT_EQ(CheckAt(100s), nullptr);
  EXPECT_EQ(CheckAt(119s), nullptr);
  EXPECT_STREQ(CheckAt(120s), "slow cleanup");
}

TEST_F(ShutdownStallDetectorTest, ProgressPreventsStall) {
  for (int i = 0; i < 10; ++i) {
    progress_.Update("batch cleanup");
    EXPECT_EQ(CheckAt(i * 19s), nullptr);
  }
}

TEST_F(ShutdownStallDetectorTest, ClearEndsEpisode) {
  progress_.Update("cleanup");
  EXPECT_EQ(CheckAt(0s), nullptr);
  progress_.Update(nullptr);
  EXPECT_EQ(CheckAt(100s), nullptr);

  progress_.Update("cleanup");
  EXPECT_EQ(CheckAt(100s), nullptr);
  EXPECT_STREQ(CheckAt(120s), "cleanup");
}

}  // namespace
}  // namespace dfly
