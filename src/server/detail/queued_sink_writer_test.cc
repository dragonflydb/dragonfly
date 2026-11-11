// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/queued_sink_writer.h"

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "base/gtest.h"
#include "server/execution_state.h"

namespace dfly::detail {

using namespace std;
using namespace util;

namespace {

// A sink whose writes block until the test allows them.
class GatedSink {
 public:
  void Write(string data) {
    fb2::NoOpLock lk;
    cv_.wait(lk, [&] { return permits_ > 0; });
    --permits_;
    written_.push_back(std::move(data));
  }

  // Lets n more writes through.
  void Allow(size_t n) {
    permits_ += n;
    cv_.notify_all();
  }

  void Open() {
    Allow(1 << 30);
  }

  const vector<string>& written() const {
    return written_;
  }

 private:
  size_t permits_ = 0;
  fb2::CondVarAny cv_;
  vector<string> written_;
};

string Chunk(char c, size_t size) {
  return string(size, c);
}

string Chunk(char c) {
  return Chunk(c, 1000);
}

const size_t kChunkBytes = Chunk('a').capacity();

}  // namespace

class QueuedSinkWriterTest : public testing::Test {
 protected:
  void StartWriter(size_t limit) {
    writer_.Start("test-writer", limit);
  }

  ExecutionState cntx_;
  GatedSink sink_;
  QueuedSinkWriter writer_{&cntx_, [this](string data) { sink_.Write(std::move(data)); }};
};

// Chunks pushed from several fibers reach the sink in the order they were pushed.
TEST_F(QueuedSinkWriterTest, PreservesPushOrder) {
  StartWriter(1 << 20);
  vector<string> pushed;
  vector<fb2::Fiber> producers;
  for (char c : {'a', 'b', 'c'}) {
    producers.emplace_back([&, c] {
      for (unsigned i = 0; i < 20; ++i) {
        string data = absl::StrCat(string(1, c), i);
        pushed.push_back(data);
        writer_.Push(std::move(data));
        ThisFiber::Yield();
      }
    });
  }
  for (auto& fb : producers)
    fb.Join();

  sink_.Open();
  writer_.Stop();
  EXPECT_EQ(sink_.written(), pushed);
}

// With a limit of 0, Push returns only once its chunk was written.
TEST_F(QueuedSinkWriterTest, ZeroLimitWaitsForWrite) {
  StartWriter(0);
  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(Chunk('a'));
    pushed = true;
  });

  ThisFiber::SleepFor(1ms);
  EXPECT_FALSE(pushed);

  sink_.Open();
  producer.Join();
  EXPECT_EQ(sink_.written().size(), 1u);
  writer_.Stop();
}

// With a limit of 0, a producer returns once its own chunk is written, while a chunk pushed after
// it is still unwritten.
TEST_F(QueuedSinkWriterTest, ZeroLimitDoesNotWaitForLaterChunks) {
  StartWriter(0);
  bool a_pushed = false, b_pushed = false;
  fb2::Fiber producer_a([&] {
    writer_.Push(Chunk('a'));
    a_pushed = true;
  });
  fb2::Fiber producer_b([&] {
    writer_.Push(Chunk('b'));
    b_pushed = true;
  });
  ThisFiber::SleepFor(1ms);

  sink_.Allow(1);
  producer_a.Join();
  EXPECT_TRUE(a_pushed);
  EXPECT_FALSE(b_pushed);

  sink_.Open();
  producer_b.Join();
  writer_.Stop();
}

// Raising the limit releases a producer that waits above the old one, while the sink is blocked.
TEST_F(QueuedSinkWriterTest, RaisingLimitReleasesProducers) {
  StartWriter(0);
  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(Chunk('a'));
    pushed = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_FALSE(pushed);

  writer_.SetLimit(1 << 20);
  producer.Join();
  EXPECT_TRUE(sink_.written().empty());

  sink_.Open();
  writer_.Stop();
}

// LowerLimit makes a producer wait only for part of the backlog ahead of it, and returns once the
// limit got down to the target.
TEST_F(QueuedSinkWriterTest, LowerLimitDoesNotWaitForBacklog) {
  StartWriter(1 << 20);
  for (char c : {'a', 'b', 'c', 'd'})
    writer_.Push(Chunk(c));

  fb2::Fiber lowerer([&] { writer_.LowerLimit(0, 0); });
  ThisFiber::SleepFor(1ms);

  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(Chunk('e'));
    pushed = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_FALSE(pushed);

  // SetLimit(0) would release the producer only once all 5 chunks are written.
  while (!pushed) {
    sink_.Allow(1);
    ThisFiber::SleepFor(1ms);
  }
  EXPECT_LT(sink_.written().size(), 4u);

  sink_.Open();
  lowerer.Join();
  producer.Join();
  EXPECT_EQ(writer_.limit(), 0u);
  writer_.Stop();
}

// Each write while LowerLimit runs lowers the limit by half of its capacity, so a small push right
// after a write fits in the other half and does not wait for the next write, however slow.
TEST_F(QueuedSinkWriterTest, LowerLimitKeepsHeadroom) {
  StartWriter(1 << 20);
  for (char c : {'a', 'b', 'c', 'd'})
    writer_.Push(Chunk(c));

  fb2::Fiber lowerer([&] { writer_.LowerLimit(0, 0); });
  ThisFiber::SleepFor(1ms);
  EXPECT_EQ(writer_.limit(), 4 * kChunkBytes);

  sink_.Allow(1);
  ThisFiber::SleepFor(1ms);
  EXPECT_EQ(writer_.pending_bytes(), 3 * kChunkBytes);
  EXPECT_EQ(writer_.limit(), 4 * kChunkBytes - kChunkBytes / 2);

  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(Chunk('e', 80));
    pushed = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_TRUE(pushed);
  EXPECT_EQ(sink_.written().size(), 1u);

  sink_.Open();
  lowerer.Join();
  producer.Join();
  EXPECT_EQ(writer_.limit(), 0u);
  writer_.Stop();
}

// LowerLimit starts `headroom` above the backlog, so a small push before the first write
// completes does not wait for it.
TEST_F(QueuedSinkWriterTest, LowerLimitStartsWithHeadroom) {
  constexpr size_t kHeadroom = 500;
  StartWriter(1 << 20);
  for (char c : {'a', 'b', 'c', 'd'})
    writer_.Push(Chunk(c));

  fb2::Fiber lowerer([&] { writer_.LowerLimit(0, kHeadroom); });
  ThisFiber::SleepFor(1ms);
  EXPECT_EQ(writer_.limit(), 4 * kChunkBytes + kHeadroom);

  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(Chunk('e', 80));
    pushed = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_TRUE(pushed);
  EXPECT_TRUE(sink_.written().empty());

  sink_.Open();
  lowerer.Join();
  producer.Join();
  EXPECT_EQ(writer_.limit(), 0u);
  writer_.Stop();
}

// LowerLimit never raises a limit that is already below its target, headroom included.
TEST_F(QueuedSinkWriterTest, LowerLimitDoesNotRaiseLimit) {
  StartWriter(0);
  writer_.LowerLimit(1 << 20, 1 << 20);
  EXPECT_EQ(writer_.limit(), 0u);
  writer_.Stop();
}

// LowerLimit returns although producers keep pushing as fast as the sink writes.
TEST_F(QueuedSinkWriterTest, LowerLimitConvergesUnderLoad) {
  StartWriter(1 << 20);
  sink_.Open();
  bool done = false;
  fb2::Fiber producer([&] {
    while (!done)
      writer_.Push(Chunk('a'));
  });
  for (unsigned i = 0; i < 100; ++i)
    writer_.Push(Chunk('b'));

  writer_.LowerLimit(0, 0);
  EXPECT_EQ(writer_.limit(), 0u);
  done = true;
  producer.Join();
  writer_.Stop();
}

// Push returns at once while the queue holds at most the limit, and blocks above it.
TEST_F(QueuedSinkWriterTest, LimitBoundsPush) {
  string a = Chunk('a');
  string b = Chunk('b');
  string c = Chunk('c');

  // Include unused capacity in the queued byte count.
  a.reserve(2 * a.size());
  const size_t limit = a.capacity() + b.capacity();
  const size_t total_bytes = limit + c.capacity();
  StartWriter(limit);
  writer_.Push(std::move(a));
  writer_.Push(std::move(b));
  EXPECT_EQ(writer_.pending_bytes(), limit);

  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(std::move(c));
    pushed = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_FALSE(pushed);
  EXPECT_EQ(writer_.pending_bytes(), total_bytes);

  sink_.Open();
  producer.Join();
  writer_.Stop();
  EXPECT_EQ(sink_.written().size(), 3u);
  EXPECT_EQ(writer_.pending_bytes(), 0u);
}

// WaitBelow blocks until the queue drains below the given size.
TEST_F(QueuedSinkWriterTest, WaitBelow) {
  StartWriter(1 << 20);
  writer_.Push(Chunk('a'));
  writer_.Push(Chunk('b'));

  bool waited = false;
  fb2::Fiber waiter([&] {
    writer_.WaitBelow(0);
    waited = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_FALSE(waited);

  sink_.Open();
  waiter.Join();
  EXPECT_EQ(writer_.pending_bytes(), 0u);
  writer_.Stop();
}

// Chunks pushed while WaitBelow waits do not extend its wait.
TEST_F(QueuedSinkWriterTest, WaitBelowIgnoresLaterPushes) {
  StartWriter(1 << 20);
  writer_.Push(Chunk('a'));
  writer_.Push(Chunk('b'));

  bool waited = false;
  fb2::Fiber waiter([&] {
    writer_.WaitBelow(0);
    waited = true;
  });
  ThisFiber::SleepFor(1ms);
  writer_.Push(Chunk('c'));

  sink_.Allow(2);
  ThisFiber::SleepFor(1ms);
  EXPECT_TRUE(waited);
  EXPECT_EQ(writer_.pending_bytes(), kChunkBytes);

  sink_.Open();
  waiter.Join();
  writer_.Stop();
}

// A cancelled context releases a producer blocked above the limit once the write in flight
// returns, even though the queue is still above the limit.
TEST_F(QueuedSinkWriterTest, CancelReleasesProducers) {
  StartWriter(1500);
  writer_.Push(Chunk('a', 100));
  writer_.Push(Chunk('b'));

  bool pushed = false;
  fb2::Fiber producer([&] {
    writer_.Push(Chunk('c'));
    pushed = true;
  });
  ThisFiber::SleepFor(1ms);
  EXPECT_FALSE(pushed);

  cntx_.Cancel();
  sink_.Allow(1);
  producer.Join();
  EXPECT_TRUE(pushed);
  EXPECT_GT(writer_.pending_bytes(), 1500u);

  sink_.Open();
  writer_.Stop();
}

// Stop hands everything queued to the sink before it returns, and can be called again.
TEST_F(QueuedSinkWriterTest, StopDrains) {
  StartWriter(1 << 20);
  for (char c : {'a', 'b', 'c'})
    writer_.Push(Chunk(c));

  fb2::Fiber stopper([&] { writer_.Stop(); });
  ThisFiber::SleepFor(1ms);
  sink_.Open();
  stopper.Join();

  EXPECT_EQ(sink_.written().size(), 3u);
  EXPECT_EQ(writer_.pending_bytes(), 0u);
  writer_.Stop();
}

}  // namespace dfly::detail
