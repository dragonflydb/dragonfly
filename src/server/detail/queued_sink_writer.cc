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
  writer_cv_.notify_one();

  util::fb2::NoOpLock lk;
  drain_cv_.wait(lk, [&] {
    return written_bytes_ >= end || pending_bytes() <= limit_ || !cntx_->IsRunning();
  });
}

void QueuedSinkWriter::WaitBelow(size_t bytes) {
  util::fb2::NoOpLock lk;
  drain_cv_.wait(lk, [&] { return pending_bytes() <= bytes || !cntx_->IsRunning(); });
}

void QueuedSinkWriter::Stop() {
  if (!fb_.IsJoinable())
    return;
  stop_ = true;
  writer_cv_.notify_one();
  fb_.Join();
}

void QueuedSinkWriter::WriterFb() {
  util::fb2::NoOpLock lk;
  while (true) {
    writer_cv_.wait(lk, [&] { return !pending_.empty() || stop_; });
    if (pending_.empty())
      return;

    string record = std::move(pending_.front());
    pending_.pop_front();
    size_t bytes = record.capacity();
    write_fn_(std::move(record));
    written_bytes_ += bytes;
    drain_cv_.notify_all();
  }
}

}  // namespace dfly::detail
