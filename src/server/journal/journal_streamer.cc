// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_streamer.h"

namespace dfly {

void JournalStreamer::Start(io::Sink* dest) {
  write_fb_ = util::fibers_ext::Fiber(&JournalStreamer::WriterFb, this, dest);
  journal_cb_id_ = journal_->RegisterOnChange([this](const journal::Entry& entry) {
    writer_.Write(entry);
    record_cnt_.fetch_add(1, std::memory_order_relaxed);
    NotifyWritten();
  });
}

uint32_t JournalStreamer::GetRecordCount() {
  return record_cnt_.load(std::memory_order_relaxed);
}

void JournalStreamer::Cancel() {
  journal_->UnregisterOnChange(journal_cb_id_);
  Finalize();

  if (write_fb_.IsJoinable())
    write_fb_.Join();
}

void JournalStreamer::WriterFb(io::Sink* dest) {
  if (auto ec = ConsumeIntoSink(dest); ec) {
    cntx_->ReportError(ec);
  }
}

}  // namespace dfly
