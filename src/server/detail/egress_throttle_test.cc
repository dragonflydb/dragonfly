// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <gtest/gtest.h>

#include "base/gtest.h"

namespace dfly::detail {

using namespace std;

// Use a window whose bucket size is exactly 100 cycles so `now` maps to slices intuitively:
// slot = now / 100, bucket index = slot % kBuckets.
constexpr uint64_t kBucketTicks = 100;
constexpr uint64_t kWindow = kBucketTicks * SlidingTracker::kBuckets;  // 2000 ticks.

class SlidingTrackerTest : public ::testing::Test {
 protected:
  SlidingTracker tracker_{kWindow};
};

TEST_F(SlidingTrackerTest, AccumulatesWithinWindow) {
  tracker_.Add(10, 0);
  tracker_.Add(20, 50);   // same bucket as t=0
  tracker_.Add(30, 150);  // next bucket
  EXPECT_EQ(tracker_.Estimate(150), 60u);
}

TEST_F(SlidingTrackerTest, ClearsStaleBuckets) {
  tracker_.Add(100, 0);
  EXPECT_EQ(tracker_.Estimate(0), 100u);

  // Advance beyond one full window: the t=0 bucket must roll off.
  uint64_t later = kWindow + kBucketTicks;  // 2100 ticks.
  EXPECT_EQ(tracker_.Estimate(later), 0u);

  tracker_.Add(42, later);
  EXPECT_EQ(tracker_.Estimate(later), 42u);
}

TEST_F(SlidingTrackerTest, PartialRollKeepsRecentBuckets) {
  // Fill each of the 20 buckets across one full window, 5 bytes each.
  for (unsigned i = 0; i < SlidingTracker::kBuckets; ++i)
    tracker_.Add(5, i * kBucketTicks);

  uint64_t now = (SlidingTracker::kBuckets - 1) * kBucketTicks;
  EXPECT_EQ(tracker_.Estimate(now), 5u * SlidingTracker::kBuckets);

  // Advance by 2 buckets: exactly 2 oldest buckets (the ones at slots 0 and 1) roll off.
  uint64_t adv = now + 2 * kBucketTicks;
  EXPECT_EQ(tracker_.Estimate(adv), 5u * (SlidingTracker::kBuckets - 2));
}

TEST_F(SlidingTrackerTest, HugeJumpResetsEverything) {
  for (unsigned i = 0; i < SlidingTracker::kBuckets; ++i)
    tracker_.Add(7, i * kBucketTicks);

  // Jump far beyond the window (more than kBuckets slices ahead).
  uint64_t far = 1'000 * kWindow;
  EXPECT_EQ(tracker_.Estimate(far), 0u);
}

TEST_F(SlidingTrackerTest, TicksToNextBucket) {
  EXPECT_EQ(tracker_.TicksToNextBucket(0), kBucketTicks);
  EXPECT_EQ(tracker_.TicksToNextBucket(30), kBucketTicks - 30);
  EXPECT_EQ(tracker_.TicksToNextBucket(kBucketTicks - 1), 1u);
}

TEST(EgressThrottlerTest, DisabledWhenNoLimit) {
  EgressThrottler t{0, kWindow};
  EXPECT_FALSE(t.enabled());
  // Recording on a disabled throttler is a no-op and must not crash.
  t.RecordEgress(1'000'000, true);
}

TEST(EgressThrottlerTest, EnabledWithLimit) {
  EgressThrottler t{1'000, kWindow};
  EXPECT_TRUE(t.enabled());
}

}  // namespace dfly::detail
