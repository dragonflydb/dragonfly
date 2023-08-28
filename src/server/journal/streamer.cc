// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

namespace dfly {
using namespace util;

void JournalStreamer::Start(io::Sink* dest) {
  using namespace journal;
  write_fb_ = fb2::Fiber("journal_stream", &JournalStreamer::WriterFb, this, dest);
  journal_cb_id_ = journal_->RegisterOnChange([this](const JournalItem& item, bool allow_await) {
    if (item.opcode == Op::NOOP) {
      // No record to write, just await if data was written so consumer will read the data.
      return AwaitIfWritten();
    }
    Write(io::Buffer(item.data));
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
