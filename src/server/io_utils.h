// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/io_buf.h"
#include "io/io.h"
#include "server/common.h"
#include "util/fibers/event_count.h"

namespace dfly {

// Base class for buffering byte streams by swapping producer and consumer buffers in turns.
// Both consumer and producer should work on the same thread but on different fibers.
class BufferedStreamerBase {
 protected:
  // Initialize with global cancellation and optional stall conditions.
  // Pointer to external buffer is set separatly because of sub-intialization order.
  BufferedStreamerBase(const Cancellation* cll, unsigned max_buffered_cnt = 5,
                       unsigned max_buffered_mem = 512)
      : cll_{cll}, max_buffered_cnt_{max_buffered_cnt}, max_buffered_mem_{max_buffered_mem} {
  }

  // Report that producer wrote some data. Block if writer is not keeping up.
  void ReportWritten();

  // Report producer finished.
  void ReportDone();

  // Write stream in buffered parts until producer finishes or error occurs.
  std::error_code WriteAll(io::Sink* dest);

  // Whether the consumer is not keeping up.
  bool IsStalled();

  // Whether the producer stopped or the context was cancelled.
  bool IsStopped();

 protected:
  bool producer_done_ = false;              // whether producer is done
  unsigned buffered_ = 0;                   // how many entries are buffered
  ::util::fibers_ext::EventCount waker_{};  // two sided waker

  const Cancellation* cll_;  // global cancellation

  unsigned max_buffered_cnt_;  // Max buffered entries before stall
  unsigned max_buffered_mem_;  // Max buffered mem before stall

  base::IoBuf* buf_ptr_ = nullptr;  // reference to producer buffer
  base::IoBuf buf_stash_{};         // local stash buffer to swap
};

}  // namespace dfly
