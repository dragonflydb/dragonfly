// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/io_buf.h"
#include "io/io.h"
#include "server/common.h"
#include "util/fibers/event_count.h"

namespace dfly {

// Base for constructing buffered byte streams with backpressure
// for single producer and consumer on the same thread.
//
// Use it as a io::Sink to write data from a producer fiber,
// and ConsumeIntoSink to extract this data in a consumer fiber.
// Use NotifyWritten to request the consumer to be woken up.
//
// Uses two base::IoBuf internally that are swapped in turns.
class BufferedStreamerBase : public io::Sink {
 protected:
  // Initialize with global cancellation and optional stall conditions.
  // Pointer to external buffer is set separatly because of sub-intialization order.
  BufferedStreamerBase(const Cancellation* cll, unsigned max_buffered_cnt = 5,
                       unsigned max_buffered_mem = 512)
      : cll_{cll}, max_buffered_cnt_{max_buffered_cnt}, max_buffered_mem_{max_buffered_mem} {
  }

  // Write some data into the internal buffer.
  // Consumer needs to be woken up manually with NotifyWritten to avoid waking it up for small
  // writes.
  io::Result<size_t> WriteSome(const iovec* vec, uint32_t len) override;

  // Report that a batch of data has been written and the consumer can be woken up.
  // Blocks if the consumer if not keeping up.
  void NotifyWritten();

  // Report producer finished.
  void Finalize();

  // Consume whole stream to sink from the consumer fiber. Unblocks when cancelled or finalized.
  std::error_code ConsumeIntoSink(io::Sink* dest);

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

  base::IoBuf producer_buf_, consumer_buf_;  // Two buffers that are swapped in turns.
};

}  // namespace dfly
