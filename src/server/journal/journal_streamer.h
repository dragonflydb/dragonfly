// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/io_utils.h"
#include "server/journal/journal.h"
#include "server/journal/serializer.h"
#include "util/fibers/fiber.h"

namespace dfly {

// Buffered single-shard journal streamer that listens for journal changes with a
// journal listener and writes them to a destination sink in a separate fiber.
class JournalStreamer : protected BufferedStreamerBase {
 public:
  JournalStreamer(journal::Journal* journal, Context* cntx)
      : BufferedStreamerBase{cntx->GetCancellation()}, cntx_{cntx},
        journal_cb_id_{0}, journal_{journal}, write_fb_{}, writer_{this} {
  }

  // Self referential.
  JournalStreamer(const JournalStreamer& other) = delete;
  JournalStreamer(JournalStreamer&& other) = delete;

  // Register journal listener and start writer in fiber.
  void Start(io::Sink* dest);

  // Must be called on context cancellation for unblocking
  // and manual cleanup.
  void Cancel();
  uint32 GetRecordCount();

 private:
  // Writer fiber that steals buffer contents and writes them to dest.
  void WriterFb(io::Sink* dest);

 private:
  Context* cntx_;

  uint32_t journal_cb_id_;
  journal::Journal* journal_;

  util::fibers_ext::Fiber write_fb_;
  JournalWriter writer_;
  std::atomic_uint32_t record_cnt_;
};

}  // namespace dfly
