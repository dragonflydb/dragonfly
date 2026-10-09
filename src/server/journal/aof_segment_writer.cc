// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_segment_writer.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <fcntl.h>
// For IORING_FSYNC_DATASYNC.
#include <linux/io_uring.h>
#include <unistd.h>

#include <algorithm>

#include "base/logging.h"
#include "server/error.h"
#include "util/fibers/proactor_base.h"

namespace dfly {

using namespace std;
using util::fb2::OpenLinux;

namespace {

constexpr uint32_t kAofSyncMs = 1000;

error_code IoError(int res) {
  return res < 0 ? error_code{-res, system_category()} : make_error_code(errc::io_error);
}

error_code LastErrno() {
  return {errno, system_category()};
}

}  // namespace

error_code AofSyncDir(const string& dir) {
  auto res = OpenLinux(dir, O_RDONLY | O_DIRECTORY, 0);
  if (!res)
    return res.error();
  RETURN_ON_ERR((*res)->FSync(0));
  return (*res)->Close();
}

AofSegmentWriter::AofSegmentWriter(string dir, uint32_t shard_id, uint32_t shard_count)
    : dir_(std::move(dir)), shard_id_(shard_id), shard_count_(shard_count) {
}

AofSegmentWriter::~AofSegmentWriter() {
  DCHECK_EQ(in_flight_, 0u);
  DCHECK(!sync_in_flight_);
  DCHECK(!preparing_);
  DCHECK(!spare_fb_.IsJoinable());
  DCHECK_EQ(tick_id_, 0u) << "Shutdown() was not called";
}

string AofSegmentWriter::SegmentName(uint32_t shard_id, uint64_t seq) {
  return absl::StrCat("appendonly-", shard_id, "-", seq, ".aof");
}

error_code AofSegmentWriter::CreateSegment(Segment* seg) {
  string path = absl::StrCat(dir_, "/", SegmentName(shard_id_, seg->seq));
  // tmp because we switch after the header becomes durable
  string tmp_path = absl::StrCat(path, ".tmp");

  // On failure, leave neither the tmp file nor a segment we created, so it can be retried.
  bool linked = false;
  absl::Cleanup cleanup = [&] {
    seg->file.reset();
    unlink(tmp_path.c_str());
    if (linked)
      unlink(path.c_str());
  };

  auto res = OpenLinux(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644 /* rw-r--r-- */);
  if (!res)
    return res.error();
  seg->file = std::move(*res);

  string hdr = EncodeAofSegmentHeader({shard_id_, shard_count_, seg->seq, seg->uid});
  RETURN_ON_ERR(seg->file->Write(io::Buffer(hdr), 0, 0));
  RETURN_ON_ERR(seg->file->FSync(IORING_FSYNC_DATASYNC));
  // Header is durable now

  // link() fails with EEXIST instead of replacing an existing segment.
  if (link(tmp_path.c_str(), path.c_str()) != 0)
    return LastErrno();
  linked = true;
  // Fails only if the disk dies; the segment is valid, so a stray tmp is just clutter.
  if (unlink(tmp_path.c_str()) != 0)
    LOG(WARNING) << "Failed to remove " << tmp_path << ": " << LastErrno().message();
  // Only the final name remains.
  RETURN_ON_ERR(AofSyncDir(dir_));
  std::move(cleanup).Cancel();
  return {};
}

error_code AofSegmentWriter::Open(uint64_t seq, uint64_t last_lsn) {
  DCHECK(!current_);
  auto seg = make_unique<Segment>(seq, absl::Uniform<uint64_t>(absl::BitGen{}));
  RETURN_ON_ERR(CreateSegment(seg.get()));
  seg->ready = true;
  current_ = std::move(seg);
  written_bytes_ = kAofSegmentHeaderSize;

  appended_lsn_ = written_lsn_ = durable_lsn_ = last_lsn;
  builder_.emplace(current_->uid, [this](AofSealedBlock block) { OnSealed(std::move(block)); });
  tick_id_ = util::fb2::ProactorBase::me()->AddPeriodic(kAofSyncMs, [this] { OnTick(); });
  PrepareSpare(seq + 1);
  return {};
}

void AofSegmentWriter::PrepareSpare(uint64_t seq) {
  DCHECK(!preparing_);
  // The previous spare fiber has finished, so this does not yield.
  spare_fb_.JoinIfNeeded();
  spare_ = make_unique<Segment>(seq, absl::Uniform<uint64_t>(absl::BitGen{}));
  preparing_ = true;
  spare_fb_ = util::fb2::Fiber("aof_spare", [this] {
    ev_.await([this] { return !old_ || OldDone() || stopping_; });
    if (!stopping_) {
      if (old_) {
        error_code ec = old_->file->Close();
        LOG_IF(WARNING, ec) << "Failed to close AOF segment " << old_->seq << ": " << ec.message();
        old_.reset();
      }
      // TODO: no retries for now; without a spare, the next rotation fails.
      if (error_code ec = CreateSegment(spare_.get()); ec)
        LOG(ERROR) << "Failed to prepare AOF segment " << spare_->seq << ": " << ec.message();
      else
        spare_->ready = true;
    }
    preparing_ = false;
    ev_.notifyAll();
  });
}

bool AofSegmentWriter::OldDone() const {
  return old_->written_offset == old_->next_offset &&
         (old_->durable_offset == old_->written_offset || sync_ec_);
}

void AofSegmentWriter::AddRecord(string_view record, uint64_t lsn) {
  appended_lsn_ = lsn;
  builder_->Append(record, lsn);
}

void AofSegmentWriter::Seal() {
  builder_->Seal();
}

bool AofSegmentWriter::WaitSpareReady() {
  ev_.await([this] { return !preparing_; });
  return spare_ && spare_->ready;
}

void AofSegmentWriter::OnCheckpoint(uint64_t cut_seq) {
  if (sync_ec_ && failed_seq_ < cut_seq) {
    LOG(INFO) << "AOF checkpoint at segment " << cut_seq << " ends the failed fdatasync state";
    sync_ec_.clear();
  }
}

optional<uint64_t> AofSegmentWriter::Rotate() {
  if (!spare_ || !spare_->ready)
    return nullopt;
  // A ready spare means the previous old segment is closed.
  DCHECK(!old_);
  Seal();
  old_ = std::move(current_);
  current_ = std::move(spare_);
  written_bytes_ += kAofSegmentHeaderSize;
  // Each segment seeds its blocks' CRCs with its own uid.
  builder_.emplace(current_->uid, [this](AofSealedBlock block) { OnSealed(std::move(block)); });
  PrepareSpare(current_->seq + 1);
  // The old segment may have drained already.
  MaybeSync(false);
  return current_->seq;
}

error_code AofSegmentWriter::Shutdown() {
  util::fb2::ProactorBase::me()->CancelPeriodic(tick_id_);
  tick_id_ = 0;
  // A spare fiber waiting for the old segment stops; the old one is closed below.
  stopping_ = true;
  ev_.notifyAll();
  spare_fb_.JoinIfNeeded();

  Seal();
  // Let current writes finish so that every failed block, old or new, gets one last retry.
  ev_.await([this] { return in_flight_ == 0; });
  // With the tick cancelled, a failure now is final.
  RetryFailed();
  ev_.await([this] { return in_flight_ == 0 && !sync_in_flight_; });

  // Sync whatever prefix is written, even if a later block failed.
  error_code close_ec;
  for (Segment* seg : {old_.get(), current_.get()}) {
    if (!seg)
      continue;
    error_code ec = seg->file->FSync(IORING_FSYNC_DATASYNC);
    if (ec && !sync_ec_)
      sync_ec_ = ec;
    if (error_code ec = seg->file->Close(); ec && !close_ec)
      close_ec = ec;
  }
  // A failed sync may have dropped dirty pages, so a later success proves nothing.
  if (!sync_ec_)
    durable_lsn_ = written_lsn_;

  // The unused spare would only add an empty segment to the log.
  if (spare_ && spare_->ready) {
    string path = absl::StrCat(dir_, "/", SegmentName(shard_id_, spare_->seq));
    error_code ec = spare_->file->Close();
    if (!ec && unlink(path.c_str()) != 0)
      ec = LastErrno();
    if (!ec)
      ec = AofSyncDir(dir_);
    LOG_IF(WARNING, ec) << "Failed to remove the AOF spare " << path << ": " << ec.message();
  }

  if (!pending_.empty()) {
    DCHECK(write_ec_);
    return write_ec_;
  }
  return sync_ec_ ? sync_ec_ : close_ec;
}

void AofSegmentWriter::WaitUnwritten(size_t limit) {
  ev_.await([&] { return UnwrittenBytes() <= limit; });
}

size_t AofSegmentWriter::UnwrittenBytes() const {
  return pending_bytes_ + builder_->PayloadSize();
}

size_t AofSegmentWriter::UnsyncedBytes() const {
  size_t res = current_->written_offset - current_->durable_offset;
  if (old_)
    res += old_->written_offset - old_->durable_offset;
  return res;
}

AofSegmentWriter::Durability AofSegmentWriter::GetDurability() const {
  if (sync_ec_)
    return Durability::kSyncFailed;
  // A failed block is unwritten, or written but not yet covered by a sync.
  for (const Segment* seg : {old_.get(), current_.get()}) {
    if (seg && seg->durable_offset < seg->repair_offset)
      return Durability::kWriteFailed;
  }
  return Durability::kNormal;
}

void AofSegmentWriter::OnSealed(AofSealedBlock block) {
  Segment* seg = current_.get();
  size_t len = block.bytes.size();
  // A block ending with a partial record commits nothing: recovery drops it unless the record
  // completes, so the block that completes it covers all of its records.
  bool commits = block.n_records > 0 && !(block.flags & kEndsWithPartial);
  uint64_t last_lsn = commits ? block.first_lsn + block.n_records - 1 : 0;
  pending_.push_back({seg, std::move(block.bytes), seg->next_offset, last_lsn});
  seg->next_offset += len;
  pending_bytes_ += len;
  Submit(&pending_.back());
}

void AofSegmentWriter::Submit(PendingBlock* pb) {
  pb->state = BlockState::kInFlight;
  ++in_flight_;
  Write(pb);
}

void AofSegmentWriter::Write(PendingBlock* pb) {
  io::Bytes src = io::Buffer(pb->bytes).subspan(pb->written);
  pb->seg->file->WriteAsync(src, pb->offset + pb->written,
                            [this, pb](int res) { OnWriteDone(pb, res); });
}

void AofSegmentWriter::OnWriteDone(PendingBlock* pb, int res) {
  if (res <= 0) {
    // Keep the buffer and retry at the same offset on the next tick. Blocks after it can
    // complete but are not popped, so WrittenLsn stops before the failed block.
    // TODO: decide when a persistent error (EIO, ENOSPC) stops retrying, e.g. fail-stop after a
    // deadline; until then backpressure stalls the shard.
    write_ec_ = IoError(res);
    LOG_EVERY_T(ERROR, 1) << "AOF write failed at offset " << pb->offset << ": "
                          << write_ec_.message();
    --in_flight_;
    pb->state = BlockState::kFailed;
    pb->seg->repair_offset = max(pb->seg->repair_offset, pb->offset + pb->bytes.size());
    ev_.notifyAll();
    return;
  }

  pb->written += res;
  // Short writes are retried
  // TODO This should be a helio utility. Short writes should be resubmitted and driven fully
  // by WriteAsync. It's easy to implement IMO and we get rid of written field.
  if (pb->written < pb->bytes.size())
    return Write(pb);

  pb->state = BlockState::kDone;
  --in_flight_;
  // Popped in log order, so the written prefix never skips a block of an older segment.
  while (!pending_.empty() && pending_.front().state == BlockState::kDone) {
    PendingBlock& front = pending_.front();
    front.seg->written_offset = front.offset + front.bytes.size();
    // can be zero for a block that is fully partial. E.g. blk 1(part1) - blk 2(part2) - blk
    // 3(part3, other) blk 2 is fully partial, it's a record whose front/tail belongs to adjacent
    // blocks.
    if (front.last_lsn)
      front.seg->written_lsn = written_lsn_ = front.last_lsn;
    written_bytes_ += front.bytes.size();
    pending_bytes_ -= front.bytes.size();
    pending_.pop_front();
  }
  MaybeSync(false);
  ev_.notifyAll();
}

void AofSegmentWriter::OnTick() {
  RetryFailed();
  MaybeSync(true);
}

void AofSegmentWriter::RetryFailed() {
  for (PendingBlock& pb : pending_) {
    if (pb.state == BlockState::kFailed)
      Submit(&pb);
  }
}

void AofSegmentWriter::MaybeSync(bool tick) {
  if (sync_in_flight_ || sync_ec_)
    return;
  Segment* seg = current_.get();
  if (old_ && !OldDone()) {
    // The current segment waits, so durability advances in log order.
    seg = old_.get();
    tick |= seg->written_offset == seg->next_offset;
  }
  if (!tick || seg->written_offset <= seg->durable_offset)
    return;
  // fdatasync covers only writes completed before it is issued, hence the captured targets.
  size_t sync_offset = seg->written_offset;
  uint64_t sync_lsn = seg->written_lsn;
  sync_in_flight_ = true;
  sync_start_ns_ = absl::GetCurrentTimeNanos();
  seg->file->FSyncAsync(IORING_FSYNC_DATASYNC, [this, seg, sync_offset, sync_lsn](int res) {
    OnSyncDone(seg, sync_offset, sync_lsn, res);
  });
}

void AofSegmentWriter::OnSyncDone(Segment* seg, size_t sync_offset, uint64_t sync_lsn, int res) {
  sync_in_flight_ = false;
  last_sync_usec_ = (absl::GetCurrentTimeNanos() - sync_start_ns_) / 1000;
  if (res < 0) {
    // Never retried: the kernel may have dropped the dirty pages ("fsyncgate").
    // DurableLsn stops until a checkpoint.
    // TODO: decide how to trigger an immediate checkpoint (#8410) to clear this state.
    sync_ec_ = IoError(res);
    failed_seq_ = seg->seq;
    LOG(ERROR) << "AOF fdatasync failed: " << sync_ec_.message();
  } else {
    seg->durable_offset = sync_offset;
    if (sync_lsn)
      durable_lsn_ = sync_lsn;
  }
  ev_.notifyAll();
}

}  // namespace dfly
