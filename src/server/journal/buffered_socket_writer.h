// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <string>

#include "server/execution_state.h"
#include "server/journal/pending_buf.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/synchronization.h"

namespace dfly {

// Buffers data and writes it asynchronously to a socket, keeping at most one write in flight.
// Producers call Throttle() to block while too much data is pending.
// Must be used from a single thread.
class BufferedSocketWriter {
 public:
  struct Options {
    // Minimal number of pending bytes to aggregate before issuing a non-forced write.
    // 0 writes eagerly.
    size_t dispatch_threshold = 0;
  };

  BufferedSocketWriter(ExecutionState* cntx, Options opts);
  ~BufferedSocketWriter();

  BufferedSocketWriter(const BufferedSocketWriter&) = delete;
  BufferedSocketWriter& operator=(const BufferedSocketWriter&) = delete;

  void Start(util::FiberSocketBase* dest);

  // TODO: we copy the string on each write because JournalItem may be passed to multiple
  // consumers so we can not move it. However, if we would either wrap JournalItem in shared_ptr
  // or wrap JournalItem::data in shared_ptr, we can avoid the cost of copying strings.
  // Also, for small strings it's more peformant to copy to the intermediate buffer than
  // to issue an io operation.
  void Write(std::string str);

  // Writes pending data regardless of dispatch_threshold.
  void Flush();

  // Flushes pending data if nothing was written during the last `period`.
  void FlushIfIdle(std::chrono::milliseconds period);

  // Blocks the caller if the socket is not keeping up.
  void Throttle();

  void WaitForInflightToComplete(bool with_timeout);

  // Wakes up fibers blocked in Throttle() or WaitForInflightToComplete().
  void WakeWaiters() {
    waker_.notifyAll();
  }

  util::FiberSocketBase* dest() const {
    return dest_;
  }

  size_t pending_bytes() const {
    return pending_buf_.Size();
  }

  size_t output_limit() const {
    return output_limit_;
  }

  uint32_t throttle_waiters() const {
    return throttle_waiters_;
  }

  uint64_t throttle_count() const {
    return throttle_count_;
  }

  // For debugging purposes. Return string with formatted internal state.
  std::string FormatInternalState() const;

 private:
  void AsyncWrite(bool force_send);
  void OnCompletion(std::error_code ec, size_t len);

  bool IsStalled() const;

  ExecutionState* cntx_;
  const Options opts_;
  const size_t output_limit_;
  util::FiberSocketBase* dest_ = nullptr;

  PendingBuf pending_buf_;
  size_t in_flight_bytes_ = 0, total_sent_ = 0;

  uint64_t throttle_count_ = 0;
  uint64_t total_throttle_wait_usec_ = 0;
  uint32_t throttle_waiters_ = 0;

  // Last time we sent async data, as base::CycleClock::Now() cycles.
  uint64_t last_async_write_time_ = 0;
  util::fb2::EventCount waker_;
};

}  // namespace dfly
