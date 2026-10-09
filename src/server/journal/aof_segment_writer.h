// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

#include "server/journal/aof.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"
#include "util/fibers/uring_file.h"

namespace dfly {

// Fsyncs a directory, so the names created or removed in it are durable.
std::error_code AofSyncDir(const std::string& dir);

class AofSegmentWriter {
 public:
  enum class Durability : uint8_t {
    kNormal,
    // A write failed; ends once it is written and a later fdatasync covers it.
    kWriteFailed,
    // An fdatasync failed; ends only with a checkpoint. Writes go on as best effort, syncs stop.
    kSyncFailed,
  };

  AofSegmentWriter(std::string dir, uint32_t shard_id, uint32_t shard_count);
  ~AofSegmentWriter();

  // Creates segment `seq` with a durable header: tmp -> header -> fdatasync -> link -> fsync dir.
  // Synchronous. last_lsn is the last record already in the log, written and durable.
  // Then prepares segment seq + 1 as the spare, in the background.
  std::error_code Open(uint64_t seq, uint64_t last_lsn = 0);

  // non blocking
  void AddRecord(std::string_view record, uint64_t lsn);
  void Seal();

  // Seals the open block and switches to the spare, then starts preparing the next one; never
  // yields. Returns the new segment's seq, or nullopt if the spare is not ready: callers wait with
  // WaitSpareReady() first, so nullopt means its preparation failed.
  std::optional<uint64_t> Rotate();

  // Blocks until no spare is being prepared; true if a spare is ready.
  bool WaitSpareReady();

  // A checkpoint whose cut starts at segment cut_seq committed. Ends a failed-sync state that hit
  // an earlier segment: the base holds those records.
  void OnCheckpoint(uint64_t cut_seq);

  // Retries failed writes once, waits for in-flight I/O and fdatasyncs the segments. Deletes the
  // unused spare. Fails if any write or fdatasync failed, even an earlier periodic one.
  std::error_code Shutdown();

  // Blocks the calling fiber until UnwrittenBytes() <= limit.
  void WaitUnwritten(size_t limit);

  // Open block payload + sealed blocks not yet written.
  size_t UnwrittenBytes() const;

  // Payload of the open block, not sealed yet.
  size_t OpenBlockBytes() const {
    return builder_->PayloadSize();
  }

  // Contiguous written bytes of this writer's segments, headers included.
  size_t WrittenSize() const {
    return written_bytes_;
  }

  // Written but not yet covered by a successful fdatasync.
  size_t UnsyncedBytes() const;

  // Duration of the last completed periodic fdatasync, 0 if none.
  uint64_t LastSyncUsec() const {
    return last_sync_usec_;
  }

  // Last record received, last_lsn of Open() if none.
  uint64_t AppendedLsn() const {
    return appended_lsn_;
  }

  // Last record fully written (not necessarily durable).
  uint64_t WrittenLsn() const {
    return written_lsn_;
  }

  // Last record covered by a successful fdatasync before any failed one.
  uint64_t DurableLsn() const {
    return durable_lsn_;
  }

  Durability GetDurability() const;

  static std::string SegmentName(uint32_t shard_id, uint64_t seq);

 private:
  struct Segment {
    uint64_t seq;
    uint64_t uid;
    std::unique_ptr<util::fb2::LinuxFile> file;
    // Its header is durable and blocks can be written.
    bool ready = false;
    size_t next_offset = kAofSegmentHeaderSize;
    // End of the contiguous written prefix, and of the part covered by a successful fdatasync.
    size_t written_offset = kAofSegmentHeaderSize;
    size_t durable_offset = kAofSegmentHeaderSize;
    // End of the furthest block whose write ever failed.
    size_t repair_offset = 0;
    // Last record of the written prefix, 0 if none.
    uint64_t written_lsn = 0;
  };

  enum class BlockState : uint8_t { kInFlight, kFailed, kDone };

  struct PendingBlock {
    // Bound when sealed.
    // TODO: old_'s blocks are a prefix of pending_, so a count could replace this pointer.
    Segment* seg;
    std::string bytes;
    size_t offset;
    // Last record ending in this block, 0 if none.
    uint64_t last_lsn;
    size_t written = 0;
    BlockState state = BlockState::kInFlight;
  };

  std::error_code CreateSegment(Segment* seg);
  // Starts spare_fb_: it waits until the old segment is done, closes it, then prepares spare seq.
  // So at most one old segment exists, and only one fiber is in flight.
  void PrepareSpare(uint64_t seq);
  // Drained, and durable unless a sync failed: nothing more is written or synced to it.
  bool OldDone() const;

  void OnSealed(AofSealedBlock block);
  void Submit(PendingBlock* pb);
  void Write(PendingBlock* pb);
  void OnWriteDone(PendingBlock* pb, int res);

  // Runs every kAofSyncMs: retries failed writes and syncs completed ones.
  void OnTick();
  void RetryFailed();
  // Syncs the old segment before the current one, so durability advances in log order. A drained
  // old segment is synced right away; otherwise syncs happen on a tick.
  void MaybeSync(bool tick);
  void OnSyncDone(Segment* seg, size_t sync_offset, uint64_t sync_lsn, int res);

  std::string dir_;
  uint32_t shard_id_;
  uint32_t shard_count_;
  // Receives new blocks.
  std::unique_ptr<Segment> current_;
  // The segment before the last rotation, until it is done and closed.
  std::unique_ptr<Segment> old_;
  std::unique_ptr<Segment> spare_;
  bool preparing_ = false;
  bool stopping_ = false;
  util::fb2::Fiber spare_fb_;
  std::optional<AofBlockBuilder> builder_;

  // Log order across segments; deque keeps references stable for completions.
  std::deque<PendingBlock> pending_;
  size_t pending_bytes_ = 0;
  uint64_t in_flight_ = 0;
  size_t written_bytes_ = 0;

  uint64_t appended_lsn_ = 0;
  uint64_t written_lsn_ = 0;
  uint64_t durable_lsn_ = 0;

  bool sync_in_flight_ = false;
  uint64_t sync_start_ns_ = 0;
  uint64_t last_sync_usec_ = 0;
  uint32_t tick_id_ = 0;
  // Last write error.
  std::error_code write_ec_;
  // First failed fdatasync; sticky until a checkpoint whose cut is past failed_seq_.
  std::error_code sync_ec_;
  uint64_t failed_seq_ = 0;
  util::fb2::EventCount ev_;
};

}  // namespace dfly
