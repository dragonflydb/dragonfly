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
    drain_cv_.notify_all();  // a raised limit may release waiting producers
  }

  size_t limit() const {
    return limit_;
  }

  // Sum of the capacities of the queued strings.
  size_t pending_bytes() const {
    return pending_bytes_;
  }

 private:
  void WriterFb();

  std::deque<std::string> pending_;
  size_t pending_bytes_ = 0;
  uint64_t pushed_ = 0, written_ = 0;  // chunk counts, to tell when a given chunk is written
  size_t limit_ = 0;
  bool stop_ = false;
  ExecutionState* cntx_;
  WriteFn write_fn_;

  util::fb2::Fiber fb_;
  util::fb2::CondVarAny writer_cv_;  // wakes fb_
  util::fb2::CondVarAny drain_cv_;   // wakes producers after each write
};

}  // namespace dfly::detail
