// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/egress_throttle.h"

#include <gtest/gtest.h>

#include "base/gtest.h"
#include "util/sliding_counter.h"

namespace dfly::detail {

using namespace std;

// A controllable clock policy so the ring-buffer behavior can be tested deterministically.
// operator() returns the current slot index directly (a new slot begins when `slot` increments).
struct ManualClock {
  uint64_t* slot;
  uint64_t operator()() const {
    return *slot;
  }
};

class SlidingCounterTest : public ::testing::Test {
 protected:
  static constexpr unsigned kBuckets = 20;
  using Counter = util::SlidingCounter<kBuckets, uint64_t, ManualClock>;

  uint64_t slot_ = 0;
  Counter counter_{ManualClock{&slot_}};
};

TEST_F(SlidingCounterTest, AccumulatesWithinWindow) {
  counter_.IncBy(10);  // slot 0
  counter_.IncBy(20);  // slot 0
  slot_ = 1;
  counter_.IncBy(30);  // slot 1
  EXPECT_EQ(counter_.Sum(), 60u);
}

TEST_F(SlidingCounterTest, ClearsStaleBuckets) {
  counter_.IncBy(100);  // slot 0
  EXPECT_EQ(counter_.Sum(), 100u);

  // Advance beyond one full window: the slot-0 bucket must roll off.
  slot_ = kBuckets + 1;
  EXPECT_EQ(counter_.Sum(), 0u);

  counter_.IncBy(42);
  EXPECT_EQ(counter_.Sum(), 42u);
}

TEST_F(SlidingCounterTest, PartialRollKeepsRecentBuckets) {
  // Fill each of the buckets across one full window, 5 bytes each.
  for (unsigned i = 0; i < kBuckets; ++i) {
    slot_ = i;
    counter_.IncBy(5);
  }

  slot_ = kBuckets - 1;
  EXPECT_EQ(counter_.Sum(), 5u * kBuckets);

  // Advance by 2 slots: exactly the 2 oldest buckets (slots 0 and 1) roll off.
  slot_ = kBuckets - 1 + 2;
  EXPECT_EQ(counter_.Sum(), 5u * (kBuckets - 2));
}

TEST_F(SlidingCounterTest, HugeJumpResetsEverything) {
  for (unsigned i = 0; i < kBuckets; ++i) {
    slot_ = i;
    counter_.IncBy(7);
  }

  slot_ = 1'000 * kBuckets;  // far beyond the window
  EXPECT_EQ(counter_.Sum(), 0u);
}

TEST_F(SlidingCounterTest, SumTailExcludesCurrentBucket) {
  counter_.IncBy(10);  // slot 0
  slot_ = 1;
  counter_.IncBy(20);  // slot 1 (currently filling)
  EXPECT_EQ(counter_.Sum(), 30u);
  EXPECT_EQ(counter_.SumTail(), 10u);  // excludes the slot-1 bucket
}

TEST_F(SlidingCounterTest, BackwardClockKeepsWindow) {
  slot_ = 5;
  counter_.IncBy(50);
  slot_ = 3;  // clock steps backward (e.g. NTP)
  counter_.IncBy(10);
  EXPECT_EQ(counter_.Sum(), 60u);  // no spurious clearing, attributed to current head
}

TEST(EgressThrottlerTest, DisabledWhenNoLimit) {
  EgressThrottler t{0};
  EXPECT_FALSE(t.enabled());
  // Recording on a disabled throttler is a no-op and must not crash.
  t.Record(1'000'000, true);
}

TEST(EgressThrottlerTest, EnabledWithLimit) {
  EgressThrottler t{1'000};
  EXPECT_TRUE(t.enabled());
}

}  // namespace dfly::detail
