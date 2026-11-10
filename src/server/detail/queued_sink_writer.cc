// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/queued_sink_writer.h"

#include "base/logging.h"
#include "server/execution_state.h"

namespace dfly::detail {

using namespace std;

QueuedSinkWriter::QueuedSinkWriter(ExecutionState* cntx, WriteFn write_fn)
    : cntx_(cntx), write_fn_(std::move(write_fn)) {
}

QueuedSinkWriter::~QueuedSinkWriter() {
  DCHECK(!fb_.IsJoinable());
}

void QueuedSinkWriter::Start(string_view name, size_t limit) {
  DCHECK(!fb_.IsJoinable());
  limit_ = limit;
  fb_ = util::fb2::Fiber(name, [this] { WriterFb(); });
}

void QueuedSinkWriter::Push(string data) {
  DCHECK(fb_.IsJoinable());
  pushed_bytes_ += data.capacity();
  const uint64_t end = pushed_bytes_;
  pending_.push_back(std::move(data));
  cv_.notify_all();

  util::fb2::NoOpLock lk;
  cv_.wait(lk, [&] {
    return written_bytes_ >= end || pending_bytes() <= limit_ || !cntx_->IsRunning();
  });
}

void QueuedSinkWriter::WaitBelow(size_t bytes) {
  // `fence` is the cumulative bytes pushed at entry. The predicate is true once all but at most
  // `bytes` of that prefix have been written.
  // Pushed data from other fibers won't affect this wait which prevents starvation for the waiter.
  const uint64_t fence = pushed_bytes_;
  util::fb2::NoOpLock lk;
  cv_.wait(lk, [&] { return fence <= written_bytes_ + bytes || !cntx_->IsRunning(); });
}

void QueuedSinkWriter::LowerLimit(size_t target, size_t headroom) {
  target = min(target, limit_);  // never raises the limit

  // Start `headroom` above the backlog, so a producer that pushes before the write in flight
  // completes does not wait for it.
  util::fb2::NoOpLock lk;

  DCHECK_GE(limit_, target);
  const size_t start = max(target, pending_bytes() + headroom);
  if (start < limit_)
    limit_ = start;  // Reduce the limit to the current backlog plus the headroom.

  // The limit may stay above a queue that drains faster than it falls, so stop once either is
  // down to `target`.
  while (limit_ > target && pending_bytes() > target && cntx_->IsRunning()) {
    // More than `target` is queued, so the writer is busy and the wait below ends.
    const uint64_t written_bytes = written_bytes_;
    cv_.wait(lk, [&] { return written_bytes_ > written_bytes || !cntx_->IsRunning(); });

    // Not clamped to what is still queued: the other half of the write is headroom for producers.
    size_t step = (written_bytes_ - written_bytes) / 2;
    if (step < limit_ - target)
      limit_ -= step;
    else
      limit_ = target;
  }
  SetLimit(target);
}

void QueuedSinkWriter::Stop() {
  if (!fb_.IsJoinable())
    return;
  stop_ = true;
  cv_.notify_all();
  fb_.Join();
}

void QueuedSinkWriter::WriterFb() {
  util::fb2::NoOpLock lk;
  while (true) {
    cv_.wait(lk, [&] { return !pending_.empty() || stop_; });
    if (pending_.empty())
      return;

    string record = std::move(pending_.front());
    pending_.pop_front();
    size_t bytes = record.capacity();
    write_fn_(std::move(record));
    written_bytes_ += bytes;
    cv_.notify_all();
  }
}

}  // namespace dfly::detail
