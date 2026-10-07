// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string>
#include <system_error>

#include "server/journal/aof_segment_writer.h"
#include "server/journal/types.h"

namespace dfly {

// Writes a shard's journal records to its AOF segment. Used only from the shard's thread.
class AofStreamer : public journal::JournalConsumerInterface {
 public:
  struct Stats {
    size_t log_size = 0;
    size_t buffered_bytes = 0;
    size_t unsynced_bytes = 0;
    uint64_t throttle_usec = 0;
    uint64_t fsync_latency_usec = 0;
    uint64_t appended_lsn = 0;
    uint64_t written_lsn = 0;
    uint64_t durable_lsn = 0;
  };

  AofStreamer(std::string dir, uint32_t shard_id, uint32_t shard_count);

  // Opens segment seq, starts the journal at next_lsn and registers as its consumer.
  // log_size is the size of the segments before seq.
  std::error_code Start(uint64_t seq, uint64_t next_lsn, uint64_t log_size);

  // Unregisters first, so no record arrives after the segment is closed.
  std::error_code Shutdown();

  // Never yields
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override;

  // Backpressure control. For now we just stall the shard.
  void ThrottleIfNeeded() override;

  // Called by the heartbeat, so small records reach the disk.
  void Seal();

  Stats GetStats() const;

 private:
  AofSegmentWriter writer_;
  size_t max_buffered_bytes_;
  std::optional<uint32_t> consumer_id_;
  uint64_t prev_log_size_ = 0;
  // Total time the shard was blocked by backpressure.
  uint64_t throttle_usec_ = 0;
};

}  // namespace dfly
