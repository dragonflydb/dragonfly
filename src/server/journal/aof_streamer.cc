// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_streamer.h"

#include <absl/flags/flag.h>

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

error_code AofStreamer::Start() {
  // No replay yet, so the log always starts with segment 0.
  RETURN_ON_ERR(writer_.Open(0));
  // A standalone node has no journal otherwise. Registering also holds a journal user.
  journal::StartInThread();
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
  writer_.WaitPending(max_buffered_bytes_);
}

void AofStreamer::Seal() {
  writer_.Seal();
}

}  // namespace dfly
