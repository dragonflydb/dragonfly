// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <system_error>

#include "server/journal/types.h"

#ifdef __linux__
#include "server/journal/aof_segment_writer.h"
#endif

namespace dfly {

#ifdef __linux__

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
  uint32_t consumer_id_ = 0;
};

#else

// No-op: the AOF segment writer needs io_uring files, so --aof is rejected at startup.
class AofStreamer : public journal::JournalConsumerInterface {
 public:
  AofStreamer(std::string dir, uint32_t shard_id, uint32_t shard_count) {
  }

  std::error_code Start() {
    return std::make_error_code(std::errc::not_supported);
  }

  std::error_code Shutdown() {
    return {};
  }

  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
  }

  void ThrottleIfNeeded() override {
  }

  void Seal() {
  }
};

#endif  // __linux__

}  // namespace dfly
