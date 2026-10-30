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
    : dir_(std::move(dir)),
      shard_id_(shard_id),
      shard_count_(shard_count),
      max_buffered_bytes_(absl::GetFlag(FLAGS_aof_max_buffered_bytes)) {
}

error_code AofStreamer::Start(uint64_t seq, uint64_t next_lsn) {
  active_ = make_shared<AofSegmentWriter>(dir_, shard_id_, shard_count_);
  RETURN_ON_ERR(active_->Open(seq));
  active_->StartPeriodicSync();
  seq_ = seq;

  // Replay repaired and fdatasynced the log before it.
  prev_durable_lsn_ = next_lsn - 1;

  // A standalone node has no journal otherwise. Registering also holds a journal user.
  journal::StartInThreadAtLsn(next_lsn);
  consumer_id_ = journal::RegisterConsumer(this);
  PrepareSpare(seq + 1);
  return {};
}

error_code AofStreamer::Shutdown() {
  journal::UnregisterConsumer(consumer_id_);

  // Also shuts down the writer a rotation left, if the fiber did not finish it yet.
  spare_fb_.JoinIfNeeded();
  error_code ec = active_->Shutdown();

  // The unused spare would only add an empty segment to the log.
  if (spare_ready_) {
    error_code discard_ec = standby_->Discard();
    LOG_IF(WARNING, discard_ec) << "Failed to remove the AOF spare: " << discard_ec.message();
  }
  return retired_ec_ ? retired_ec_ : ec;
}

bool AofStreamer::WaitSpareReady() {
  // May yield, so Rotate() does not have to.
  spare_fb_.JoinIfNeeded();
  return spare_ready_;
}

optional<uint64_t> AofStreamer::Rotate() {
  // Runs in a checkpoint's cut: no record may land between its GetLsn() and the switch.
  util::FiberAtomicGuard guard;

  // WaitSpareReady() joined the fiber before the cut, so nothing here yields.
  DCHECK(!spare_fb_.IsJoinable());
  if (!spare_ready_)
    return nullopt;

  // Records from here on go to the spare; the old writer finishes in the background. Until then,
  // durability stays at what the old segment reached so far.
  active_->Seal();
  if (!retired_ec_)
    prev_durable_lsn_ = max(prev_durable_lsn_, active_->DurableLsn());
  retiring_ = true;
  swap(active_, standby_);
  active_->StartPeriodicSync();
  spare_ready_ = false;
  ++seq_;
  PrepareSpare(seq_ + 1);
  return seq_;
}

void AofStreamer::PrepareSpare(uint64_t seq) {
  // Launched without switching fibers, so Rotate() does not yield.
  spare_fb_ = util::fb2::Fiber("aof_spare", [this, seq] {
    if (standby_) {
      // Drains, fdatasyncs and closes the segment the rotation left.
      if (error_code ec = standby_->Shutdown(); ec) {
        // Durability stays before the old segment's tail, which may be lost.
        if (!retired_ec_)
          retired_ec_ = ec;
      } else if (!retired_ec_) {
        prev_durable_lsn_ = max(prev_durable_lsn_, standby_->DurableLsn());
      }
      retiring_ = false;
      standby_.reset();
    }

    auto spare = make_shared<AofSegmentWriter>(dir_, shard_id_, shard_count_);
    // TODO: no retries for now; without a spare, the next checkpoint fails before its cut.
    if (error_code ec = spare->Open(seq); ec) {
      LOG(ERROR) << "Failed to prepare AOF segment " << seq << ": " << ec.message();
      return;
    }
    standby_ = std::move(spare);
    spare_ready_ = true;
  });
}

uint64_t AofStreamer::DurableLsn() const {
  // A retirement in progress, or one that failed: nothing past the old segment counts. A failure
  // holds until a committed checkpoint supersedes the segment.
  if (retiring_ || retired_ec_)
    return prev_durable_lsn_;
  return max(prev_durable_lsn_, active_->DurableLsn());
}

void AofStreamer::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  active_->AddRecord(item.journal_item.data, item.journal_item.lsn);
}

void AofStreamer::ThrottleIfNeeded() {
  // TODO: decide whether backpressure should ever give up instead of stalling the shard.
  // The open block leaves PendingBytes() only once sealed: when it fills up, or by the heartbeat.
  // With a limit below kAofBlockBytes, the heartbeat can wait here on the unsealed block holding
  // its own expiry records, and nothing seals it.
  // TODO: refuse to start with --aof_max_buffered_bytes below kAofBlockBytes (8KB), or seal the
  // open block before waiting.

  // A rotation may retire and release the writer while ThrottleIfNeeded() waits in it; this
  // reference keeps it alive until the wait returns.
  shared_ptr<AofSegmentWriter> writer = active_;
  writer->WaitPending(max_buffered_bytes_);
}

void AofStreamer::Seal() {
  active_->Seal();
}

}  // namespace dfly
