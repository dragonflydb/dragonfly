// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string>
#include <system_error>

#include "server/journal/types.h"

#ifdef __linux__
#include <memory>

#include "server/journal/aof_segment_writer.h"
#include "util/fibers/fibers.h"
#endif

namespace dfly {

#ifdef __linux__

// Writes a shard's journal records to its AOF segments. Used only from the shard's thread.
// One writer receives the records; a second one is the spare, an empty next segment prepared in
// the background, so a rotation only switches between the two and never creates a file.
class AofStreamer : public journal::JournalConsumerInterface {
 public:
  AofStreamer(std::string dir, uint32_t shard_id, uint32_t shard_count);

  // Opens segment seq, starts the journal at next_lsn and registers as its consumer. Then prepares
  // segment seq + 1 as the spare.
  std::error_code Start(uint64_t seq, uint64_t next_lsn);

  // Unregisters first, so no record arrives after the segment is closed. Deletes the unused
  // spare. Returns the first error, including one from a writer retired by a rotation.
  std::error_code Shutdown();

  // Waits until the spare is prepared; true if it is ready. Called before a checkpoint's cut, so
  // Rotate() finds the spare and never waits.
  bool WaitSpareReady();

  // Seals the active segment and switches the records to the spare, then retires the old writer
  // and prepares the next spare in the background. Never yields: called in a checkpoint's cut,
  // after WaitSpareReady(). Returns the new segment's seq, or nullopt if no spare is ready.
  std::optional<uint64_t> Rotate();

  // Last record covered by a successful fdatasync. Advances in log order: into the active segment
  // only once the segment a rotation left had its final fdatasync succeed.
  uint64_t DurableLsn() const;

  // Never yields
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override;

  // Backpressure control. For now we just stall the shard.
  void ThrottleIfNeeded() override;

  // Called by the heartbeat, so small records reach the disk.
  void Seal();

 private:
  // Starts spare_fb_: it shuts down the writer standby_ holds, if any (the one a rotation left),
  // then opens a new one on segment seq as the spare.
  void PrepareSpare(uint64_t seq);

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  size_t max_buffered_bytes_;
  uint32_t consumer_id_ = 0;

  // Receives the records; its seq.
  std::shared_ptr<AofSegmentWriter> active_;
  uint64_t seq_ = 0;

  // The writer a rotation left until spare_fb_ shut it down, then the next spare.
  std::shared_ptr<AofSegmentWriter> standby_;
  bool spare_ready_ = false;
  util::fb2::Fiber spare_fb_;

  // Durable lsn of the segments before the active one. While a writer a rotation left is being
  // shut down, or after one failed, durability goes no further.
  uint64_t prev_durable_lsn_ = 0;
  bool retiring_ = false;

  // First error of a writer retired by a rotation, returned by Shutdown(). Freezes DurableLsn()
  // across later rotations.
  // TODO: harmless once the checkpoint that rotated commits, since the old segment is then garbage:
  // clear it there, with prev_durable_lsn_ at the cut. If it does not commit, the chain has a hole
  // at the old segment's tail: stop the active writer.
  std::error_code retired_ec_;
};

#else

// No-op: the AOF segment writer needs io_uring files, so --aof is rejected at startup.
class AofStreamer : public journal::JournalConsumerInterface {
 public:
  AofStreamer(std::string dir, uint32_t shard_id, uint32_t shard_count) {
  }

  std::error_code Start(uint64_t seq, uint64_t next_lsn) {
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

  bool WaitSpareReady() {
    return false;
  }

  std::optional<uint64_t> Rotate() {
    return std::nullopt;
  }

  uint64_t DurableLsn() const {
    return 0;
  }
};

#endif  // __linux__

}  // namespace dfly
