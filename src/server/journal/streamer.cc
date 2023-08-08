// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

namespace dfly {
using namespace util;

void JournalStreamer::Start(io::Sink* dest) {
  using namespace journal;
  write_fb_ = fb2::Fiber("journal_stream", &JournalStreamer::WriterFb, this, dest);
  journal_cb_id_ = journal_->RegisterOnChange([this](const Entry& entry, bool allow_await) {
    if (entry.opcode == Op::NOOP) {
      // No recode to write, just await if data was written so consumer will read the data.
      return AwaitIfWritten();
    }
    writer_.Write(entry);
    NotifyWritten(allow_await);
  });
}

void JournalStreamer::Cancel() {
  Finalize();  // Finalize must be called before UnregisterOnChange because we first need to stop
               // writing to buffer and notify the all the producers.
               // Writing to journal holds mutex protecting change_cb_arr_, than the fiber can
               // preemt when calling NotifyWritten and it will not run again till notified.
               // UnregisterOnChange will try to lock the mutex therefor calling UnregisterOnChange
               // before Finalize may cause deadlock.
  journal_->UnregisterOnChange(journal_cb_id_);

  if (write_fb_.IsJoinable()) {
    write_fb_.Join();
  }
}

void JournalStreamer::WriterFb(io::Sink* dest) {
  if (auto ec = ConsumeIntoSink(dest); ec) {
    cntx_->ReportError(ec);
  }
}

}  // namespace dfly
