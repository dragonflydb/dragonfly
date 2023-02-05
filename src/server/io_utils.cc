// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/io_utils.h"

#include "base/flags.h"
#include "base/logging.h"
#include "server/error.h"

using namespace std;

namespace dfly {

io::Result<size_t> BufferedStreamerBase::WriteSome(const iovec* vec, uint32_t len) {
  return io::BufSink{&producer_buf_}.WriteSome(vec, len);
}

void BufferedStreamerBase::NotifyWritten() {
  if (IsStopped())
    return;
  buffered_++;
  // Wake up the consumer.
  waker_.notify();
  // Block if we're stalled because the consumer is not keeping up.
  waker_.await([this]() { return !IsStalled() || IsStopped(); });
}

error_code BufferedStreamerBase::ConsumeIntoSink(io::Sink* dest) {
  while (!IsStopped()) {
    // Wait for more data or stop signal.
    waker_.await([this]() { return buffered_ > 0 || IsStopped(); });
    // Break immediately on cancellation.
    if (cll_->IsCancelled()) {
      waker_.notifyAll();  // Wake consumer if it missed it.
      break;
    }

    // Swap producer and consumer buffers
    std::swap(producer_buf_, consumer_buf_);
    buffered_ = 0;

    // If producer stalled, notify we consumed data and it can unblock.
    waker_.notifyAll();

    // Write data and check for errors.
    if (auto ec = dest->Write(consumer_buf_.InputBuffer()); ec) {
      Finalize();  // Finalize on error to unblock prodcer immediately.
      return ec;
    }

    // TODO: shrink big stash.
    consumer_buf_.Clear();
  }

  return std::error_code{};
}

void BufferedStreamerBase::Finalize() {
  producer_done_ = true;
  waker_.notifyAll();
}

bool BufferedStreamerBase::IsStopped() {
  return cll_->IsCancelled() || producer_done_;
}

bool BufferedStreamerBase::IsStalled() {
  return buffered_ > max_buffered_cnt_ || producer_buf_.InputLen() > max_buffered_mem_;
}
}  // namespace dfly
