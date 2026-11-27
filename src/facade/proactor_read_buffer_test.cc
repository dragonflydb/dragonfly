// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/proactor_read_buffer.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "util/fibers/fibers.h"

namespace facade {
namespace {

// Only one connection may use the shared buffer at a time.
// Borrow it twice, then confirm the second borrow works only after the first is released.
TEST(ProactorReadBufferTest, RejectsSecondBorrowUntilFirstIsReleased) {
  ProactorReadBuffer read_buffer;
  read_buffer.Init(128);

  auto first_borrow = read_buffer.TryBorrow(1);
  ASSERT_TRUE(first_borrow);
  EXPECT_TRUE(read_buffer.in_use());
  EXPECT_EQ(read_buffer.OwnerConnId(), 1u);
  EXPECT_FALSE(read_buffer.TryBorrow(2));

  first_borrow.reset();
  EXPECT_FALSE(read_buffer.in_use());
  EXPECT_TRUE(read_buffer.TryBorrow(2));
}

#ifndef NDEBUG
// Releasing a buffer with unread data would lose that data for the connection.
// Put one byte in the buffer and check that the release fails.
TEST(ProactorReadBufferDeathTest, RejectsNonEmptyBufferOnRelease) {
  EXPECT_DEATH(
      {
        ProactorReadBuffer read_buffer;
        read_buffer.Init(128);
        auto borrow = read_buffer.TryBorrow(1);
        borrow->buf().WriteAndCommit("x", 1);
      },
      "");
}

// A buffer borrow must stay in the same fiber that received it.
// Switch to another fiber and check that the borrow detects the mistake.
TEST(ProactorReadBufferDeathTest, RejectsFiberSwitchDuringBorrow) {
  EXPECT_DEATH(
      {
        ProactorReadBuffer read_buffer;
        read_buffer.Init(128);
        auto borrow = read_buffer.TryBorrow(1);
        util::fb2::Fiber other("switch_epoch", [] {});
        other.Join();
      },
      "");
}
#endif

}  // namespace
}  // namespace facade
