// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "core/glob_matcher.h"
#include "core/intent_lock.h"
#include "core/tx_queue.h"

namespace dfly {

using namespace std;

class TxQueueTest : public ::testing::Test {
 protected:
  TxQueueTest() {
  }

  uint64_t Pop() {
    if (pq_.Empty())
      return uint64_t(-1);
    TxQueue::ValueType val = pq_.Front();
    pq_.PopFront();

    return std::get<uint64_t>(val);
  }

  TxQueue pq_;
};

TEST_F(TxQueueTest, Basic) {
  pq_.Insert(4);
  pq_.Insert(3);
  pq_.Insert(2);

  unsigned cnt = 0;
  auto head = pq_.Head();
  auto it = head;
  do {
    ++cnt;
    it = pq_.Next(it);
  } while (it != head);
  EXPECT_EQ(3, cnt);

  ASSERT_EQ(2, Pop());
  ASSERT_EQ(3, Pop());
  ASSERT_EQ(4, Pop());
  ASSERT_TRUE(pq_.Empty());

  EXPECT_EQ(TxQueue::kEnd, pq_.Head());

  pq_.Insert(10);
  ASSERT_EQ(10, Pop());
}

class IntentLockTest : public ::testing::Test {
 protected:
  IntentLock lk_;
};

TEST_F(IntentLockTest, Basic) {
  ASSERT_TRUE(lk_.Acquire(IntentLock::SHARED));
  ASSERT_FALSE(lk_.Acquire(IntentLock::EXCLUSIVE));
  lk_.Release(IntentLock::EXCLUSIVE);

  ASSERT_FALSE(lk_.Check(IntentLock::EXCLUSIVE));
  lk_.Release(IntentLock::SHARED);
  ASSERT_TRUE(lk_.Check(IntentLock::EXCLUSIVE));
}

class StringMatchTest : public ::testing::Test {
 protected:
  // wrapper around stringmatchlen with stringview arguments
  int MatchLen(string_view pattern, string_view str, bool nocase) {
    GlobMatcher matcher(pattern, !nocase);
    return matcher.Matches(str);
  }
};

TEST_F(StringMatchTest, Basic) {
  // ExactMatch
  EXPECT_EQ(MatchLen("hello", "hello", 0), 1);
  EXPECT_EQ(MatchLen("hello", "world", 0), 0);

  // Wildcards
  EXPECT_EQ(MatchLen("*", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h*", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h*o", "hello", 0), 1);
  EXPECT_EQ(MatchLen("hel*o*", "hello*", 0), 1);

  // Single character wildcard
  EXPECT_EQ(MatchLen("h[aeiou]llo", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h[aeiou]llo", "hallo", 0), 1);
  EXPECT_EQ(MatchLen("h[^aeiou]llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h[a-z]llo", "hello", 0), 1);

  EXPECT_EQ(MatchLen("h[A-Z]llo", "Hello", 1), 1);

  EXPECT_EQ(MatchLen("h\\*llo", "h*llo", 0), 1);
  EXPECT_EQ(MatchLen("h\\\\llo", "h\\llo", 0), 1);

  // ?
  EXPECT_EQ(MatchLen("h?llo", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h??llo", "ha llo", 0), 1);
  EXPECT_EQ(MatchLen("h??llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h\\?llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h\\?llo", "h?llo", 0), 1);
}

}  // namespace dfly
