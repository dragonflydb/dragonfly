// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_streamer.h"

#include <absl/flags/flag.h>
#include <absl/time/clock.h>

#include "base/logging.h"
#include "facade/facade_types.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "strings/human_readable.h"

using facade::operator""_MB;

// TODO: figure out a good default value.
ABSL_FLAG(strings::MemoryBytesFlag, aof_max_buffered_bytes, 64_MB,
          "Per shard: unwritten AOF bytes above which the shard blocks until writes complete");

namespace dfly {

using namespace std;

AofStreamer::AofStreamer(string dir, uint32_t shard_id, uint32_t shard_count)
    : writer_(std::move(dir), shard_id, shard_count),
      max_buffered_bytes_(absl::GetFlag(FLAGS_aof_max_buffered_bytes)) {
}

error_code AofStreamer::Start(uint64_t seq, uint64_t next_lsn, uint64_t log_size) {
  RETURN_ON_ERR(writer_.Open(seq, next_lsn - 1));
  prev_log_size_ = log_size;
  // A standalone node has no journal otherwise. Registering also holds a journal user.
  journal::StartInThreadAtLsn(next_lsn);
  consumer_id_ = journal::RegisterConsumer(this);
  return {};
}

error_code AofStreamer::Shutdown() {
  if (consumer_id_) {
    journal::UnregisterConsumer(*consumer_id_);
    consumer_id_.reset();
  }
  return writer_.Shutdown();
}

void AofStreamer::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  writer_.AddRecord(item.journal_item.data, item.journal_item.lsn);
}

void AofStreamer::ThrottleIfNeeded() {
  // TODO: decide whether backpressure should ever give up instead of stalling the shard.
  if (writer_.UnwrittenBytes() <= max_buffered_bytes_)
    return;
  uint64_t start = absl::GetCurrentTimeNanos();
  writer_.WaitUnwritten(max_buffered_bytes_);
  throttle_usec_ += (absl::GetCurrentTimeNanos() - start) / 1000;
}

void AofStreamer::Seal() {
  writer_.Seal();
}

AofStreamer::Stats AofStreamer::GetStats() const {
  Stats st;
  st.log_size = prev_log_size_ + writer_.WrittenSize();
  st.buffered_bytes = writer_.UnwrittenBytes();
  st.open_block_bytes = writer_.OpenBlockBytes();
  st.unsynced_bytes = writer_.UnsyncedBytes();
  st.throttle_usec = throttle_usec_;
  st.fsync_latency_usec = writer_.LastSyncUsec();
  st.appended_lsn = writer_.AppendedLsn();
  st.written_lsn = writer_.WrittenLsn();
  st.durable_lsn = writer_.DurableLsn();
  return st;
}

}  // namespace dfly
