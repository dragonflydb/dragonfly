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
  AofStreamer(std::string dir, uint32_t shard_id, uint32_t shard_count);

  // Opens the segment, starts the journal and registers as its consumer.
  std::error_code Start();

  // Unregisters first, so no record arrives after the segment is closed.
  std::error_code Shutdown();

  // Never yields
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override;

  // Backpressure control. For now we just stall the shard.
  void ThrottleIfNeeded() override;

  // Called by the heartbeat, so small records reach the disk.
  void Seal();

 private:
  AofSegmentWriter writer_;
  size_t max_buffered_bytes_;
  std::optional<uint32_t> consumer_id_;
};

}  // namespace dfly
