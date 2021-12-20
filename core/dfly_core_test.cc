// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/tx_queue.h"

#include "base/gtest.h"
#include "core/intent_lock.h"

namespace dfly {

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

  ASSERT_EQ(2, Pop());
  ASSERT_EQ(3, Pop());
  ASSERT_EQ(4, Pop());
  ASSERT_TRUE(pq_.Empty());

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

}  // namespace dfly
