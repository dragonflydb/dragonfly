// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_segment_writer.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
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

error_code SyncDir(const string& dir) {
  auto res = OpenLinux(dir, O_RDONLY | O_DIRECTORY, 0);
  if (!res)
    return res.error();
  RETURN_ON_ERR((*res)->FSync(0));
  return (*res)->Close();
}

error_code LastErrno() {
  return {errno, system_category()};
}

}  // namespace

AofSegmentWriter::AofSegmentWriter(string dir, uint32_t shard_id, uint32_t shard_count)
    : dir_(std::move(dir)), shard_id_(shard_id), shard_count_(shard_count) {
}

AofSegmentWriter::~AofSegmentWriter() {
  DCHECK_EQ(in_flight_, 0u);
  DCHECK(!sync_in_flight_);
  DCHECK_EQ(tick_id_, 0u) << "Shutdown() was not called";
}

string AofSegmentWriter::SegmentName(uint32_t shard_id, uint64_t seq) {
  return absl::StrCat("appendonly-", shard_id, "-", seq, ".aof");
}

error_code AofSegmentWriter::Open(uint64_t seq) {
  DCHECK(!file_);
  string path = absl::StrCat(dir_, "/", SegmentName(shard_id_, seq));
  // tmp because we switch after the header becomes durable
  string tmp_path = absl::StrCat(path, ".tmp");

  uint64_t uid = absl::Uniform<uint64_t>(absl::BitGen{});

  // On failure, leave neither the tmp file nor a segment we created, so Open can be retried.
  bool linked = false;
  absl::Cleanup cleanup = [&] {
    file_.reset();
    unlink(tmp_path.c_str());
    if (linked)
      unlink(path.c_str());
  };

  auto res = OpenLinux(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644 /* rw-r--r-- */);
  if (!res)
    return res.error();
  file_ = std::move(*res);

  string hdr = EncodeAofSegmentHeader({shard_id_, shard_count_, seq, uid});
  RETURN_ON_ERR(file_->Write(io::Buffer(hdr), 0, 0));
  RETURN_ON_ERR(file_->FSync(IORING_FSYNC_DATASYNC));
  // Header is durable now

  // link() fails with EEXIST instead of replacing an existing segment.
  if (link(tmp_path.c_str(), path.c_str()) != 0)
    return LastErrno();
  linked = true;
  // Fails only if the disk dies; the segment is valid, so a stray tmp is just clutter.
  if (unlink(tmp_path.c_str()) != 0)
    LOG(WARNING) << "Failed to remove " << tmp_path << ": " << LastErrno().message();
  // Only the final name remains.
  RETURN_ON_ERR(SyncDir(dir_));
  std::move(cleanup).Cancel();

  builder_.emplace(uid, [this](AofSealedBlock block) { OnSealed(std::move(block)); });
  tick_id_ = util::fb2::ProactorBase::me()->AddPeriodic(kAofSyncMs, [this] { OnTick(); });
  return {};
}

void AofSegmentWriter::AddRecord(string_view record, uint64_t lsn) {
  builder_->Append(record, lsn);
}

void AofSegmentWriter::Seal() {
  builder_->Seal();
}

error_code AofSegmentWriter::Shutdown() {
  util::fb2::ProactorBase::me()->CancelPeriodic(tick_id_);
  tick_id_ = 0;

  Seal();
  // Let current writes finish so that every failed block, old or new, gets one last retry.
  ev_.await([this] { return in_flight_ == 0; });
  // With the tick cancelled, a failure now is final.
  RetryFailed();
  ev_.await([this] { return in_flight_ == 0 && !sync_in_flight_; });

  // Sync whatever prefix is written, even if a later block failed.
  error_code ec = file_->FSync(IORING_FSYNC_DATASYNC);
  if (ec && !sync_ec_)
    sync_ec_ = ec;
  // A failed sync may have dropped dirty pages, so a later success proves nothing.
  if (!sync_ec_) {
    durable_offset_ = written_offset_;
    durable_lsn_ = written_lsn_;
  }
  error_code close_ec = file_->Close();

  if (written_offset_ < next_offset_) {
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

AofSegmentWriter::Durability AofSegmentWriter::GetDurability() const {
  if (sync_ec_)
    return Durability::kSyncFailed;
  // A failed block is unwritten, or written but not yet covered by a sync.
  if (durable_offset_ < repair_offset_)
    return Durability::kWriteFailed;
  return Durability::kNormal;
}

void AofSegmentWriter::OnSealed(AofSealedBlock block) {
  size_t len = block.bytes.size();
  uint64_t last_lsn = block.n_records ? block.first_lsn + block.n_records - 1 : 0;
  pending_.push_back({std::move(block.bytes), next_offset_, last_lsn});
  next_offset_ += len;
  pending_bytes_ += len;
  ++in_flight_;
  Submit(&pending_.back());
}

void AofSegmentWriter::Submit(PendingBlock* pb) {
  io::Bytes src = io::Buffer(pb->bytes).subspan(pb->written);
  file_->WriteAsync(src, pb->offset + pb->written, [this, pb](int res) { OnWriteDone(pb, res); });
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
    repair_offset_ = max(repair_offset_, pb->offset + pb->bytes.size());
    ev_.notifyAll();
    return;
  }

  pb->written += res;
  // Short writes are retried
  // TODO This should be a helio utility. Short writes should be resubmitted and driven fully
  // by WriteAsync. It's easy to implement IMO and we get rid of written field.
  if (pb->written < pb->bytes.size())
    return Submit(pb);

  pb->state = BlockState::kDone;
  --in_flight_;
  while (!pending_.empty() && pending_.front().state == BlockState::kDone) {
    PendingBlock& front = pending_.front();
    written_offset_ = front.offset + front.bytes.size();
    // can be zero for a block that is fully partial. E.g. blk 1(part1) - blk 2(part2) - blk
    // 3(part3, other) blk 2 is fully partial, it's a record whose front/tail belongs to adjacent
    // blocks.
    if (front.last_lsn)
      written_lsn_ = front.last_lsn;
    pending_bytes_ -= front.bytes.size();
    pending_.pop_front();
  }
  ev_.notifyAll();
}

void AofSegmentWriter::OnTick() {
  RetryFailed();

  // fdatasync covers only writes completed before it is issued, hence the captured targets.
  if (sync_in_flight_ || sync_ec_ || written_offset_ <= durable_offset_)
    return;
  size_t sync_offset = written_offset_;
  uint64_t sync_lsn = written_lsn_;
  sync_in_flight_ = true;
  file_->FSyncAsync(IORING_FSYNC_DATASYNC, [this, sync_offset, sync_lsn](int res) {
    OnSyncDone(sync_offset, sync_lsn, res);
  });
}

void AofSegmentWriter::RetryFailed() {
  // Failed blocks are not written yet, so they all lie before repair_offset_.
  if (written_offset_ >= repair_offset_)
    return;
  for (PendingBlock& pb : pending_) {
    if (pb.state != BlockState::kFailed)
      continue;
    pb.state = BlockState::kInFlight;
    ++in_flight_;
    Submit(&pb);
  }
}

void AofSegmentWriter::OnSyncDone(size_t sync_offset, uint64_t sync_lsn, int res) {
  sync_in_flight_ = false;
  if (res < 0) {
    // Never retried: the kernel may have dropped the dirty pages ("fsyncgate").
    // DurableLsn stops until a checkpoint.
    // TODO: decide how to trigger an immediate checkpoint (#8410) to clear this state.
    sync_ec_ = IoError(res);
    LOG(ERROR) << "AOF fdatasync failed: " << sync_ec_.message();
  } else {
    durable_offset_ = sync_offset;
    durable_lsn_ = sync_lsn;
  }
  ev_.notifyAll();
}

}  // namespace dfly
