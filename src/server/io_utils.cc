// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/io_utils.h"

#include "base/flags.h"
#include "base/logging.h"
#include "server/error.h"

namespace dfly {

void BufferedStreamerBase::ReportWritten() {
  buffered_++;
  // Check and possibly wait for consumer to keep up.
  waker_.await([this]() { return !IsStalled() || IsStopped(); });
  // Notify consumer we have new data.
  waker_.notify();
}

std::error_code BufferedStreamerBase::WriteAll(io::Sink* dest) {
  CHECK_NE(buf_ptr_, nullptr);

  while (!IsStopped()) {
    // Wait for more data or stop signal.
    waker_.await([this]() { return buffered_ > 0 || IsStopped(); });
    if (IsStopped())
      break;

    // Steal-swap producer buffer.
    std::swap(buf_stash_, *buf_ptr_);
    buffered_ = 0;

    // If producer stalled, notify we consumed data and it can unblock.
    waker_.notify();
    RETURN_ON_ERR(dest->Write(buf_stash_.InputBuffer()));

    // TODO: shrink big stash.
    buf_stash_.Clear();
  }

  return std::error_code{};
}

void BufferedStreamerBase::ReportDone() {
  producer_done_ = true;
  waker_.notifyAll();
}

bool BufferedStreamerBase::IsStopped() {
  return cll_->IsCancelled() || producer_done_;
}

bool BufferedStreamerBase::IsStalled() {
  return buffered_ > max_buffered_cnt_ || buf_ptr_->InputLen() > max_buffered_mem_;
}
}  // namespace dfly
