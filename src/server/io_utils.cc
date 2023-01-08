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
  // Write data to producer buffer.
  buffered_++;
  return io::BufSink{&producer_buf_}.WriteSome(vec, len);
}

void BufferedStreamerBase::NotifyWritten() {
  // Wake up the consumer.
  waker_.notify();
  // Block if we're stalled because the consumer is not keeping up.
  waker_.await([this]() { return !IsStalled() || IsStopped(); });
}

error_code BufferedStreamerBase::ConsumeIntoSink(io::Sink* dest) {
  while (!IsStopped()) {
    // Wait for more data or stop signal.
    waker_.await([this]() { return buffered_ > 0 || IsStopped(); });

    // Swap producer and consumer buffers
    std::swap(producer_buf_, consumer_buf_);
    buffered_ = 0;

    // If producer stalled, notify we consumed data and it can unblock.
    waker_.notify();
    RETURN_ON_ERR(dest->Write(consumer_buf_.InputBuffer()));

    if (IsStopped())
      break;

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
