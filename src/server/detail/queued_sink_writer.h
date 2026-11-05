// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <string_view>

#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {
class ExecutionState;
}  // namespace dfly

namespace dfly::detail {

// Decouples producers from a slow synchronous sink: Push() queues a chunk that a dedicated fiber
// writes in FIFO order, and blocks only while its chunk is unwritten and more than limit() bytes
// are queued. A limit of 0 makes Push() wait until its own chunk is written, not for the chunks
// queued after it. Must be used from the thread that created it.
class QueuedSinkWriter {
 public:
  // Writes a chunk to the sink. Can block. Called only from the writer fiber.
  using WriteFn = std::function<void(std::string)>;

  // Once cntx stops running, waiting producers are released.
  QueuedSinkWriter(ExecutionState* cntx, WriteFn write_fn);
  ~QueuedSinkWriter();

  QueuedSinkWriter(const QueuedSinkWriter&) = delete;
  QueuedSinkWriter& operator=(const QueuedSinkWriter&) = delete;

  // Starts the writer fiber. Must precede Push().
  void Start(std::string_view name, size_t limit);

  // Queues data and blocks until it is written or at most limit() bytes are queued.
  void Push(std::string data);

  // Blocks while more than `bytes` are queued.
  void WaitBelow(size_t bytes);

  // Hands what is queued to write_fn and joins the writer fiber. No Push() may follow.
  // Idempotent.
  void Stop();

  void SetLimit(size_t limit) {
    limit_ = limit;
    cv_.notify_all();  // a raised limit may release waiting producers
  }

  // Lowers the limit to `limit` without making a producer wait for the whole backlog, as
  // SetLimit(limit) would. The limit follows the queue down: each write lowers it by half of the
  // bytes written, so producers keep half of the sink's throughput while the queue shrinks.
  // Blocks until the limit or the queue is down to `limit`. Does not raise a lower limit.
  void LowerLimit(size_t limit);

  size_t limit() const {
    return limit_;
  }

  // Sum of the capacities of the chunks pushed but not yet written.
  size_t pending_bytes() const {
    return pushed_bytes_ - written_bytes_;
  }

 private:
  void WriterFb();

  std::deque<std::string> pending_;  // chunks not yet handed to write_fn_, in push order

  // Running totals of the capacities of the pushed and of the written chunks. A chunk is written
  // once written_bytes_ reaches the value pushed_bytes_ had right after its push.
  uint64_t pushed_bytes_ = 0, written_bytes_ = 0;

  size_t limit_ = 0;      // bytes producers may leave queued without waiting for their chunk
  bool stop_ = false;     // set by Stop(): fb_ exits once pending_ is empty
  ExecutionState* cntx_;  // once it stops running, waiting producers are released
  WriteFn write_fn_;

  util::fb2::Fiber fb_;  // the writer fiber, runs WriterFb()
  // Notified on every change to the queue: Push(), Stop(), each write and SetLimit(). The writer
  // fiber and the producers wait on it for their own conditions.
  util::fb2::CondVarAny cv_;
};

}  // namespace dfly::detail
