// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

#include <absl/functional/bind_front.h>

#include "base/flags.h"
#include "base/logging.h"
#include "server/engine_shard.h"
#include "server/journal/cmd_serializer.h"
#include "server/server_state.h"
#include "util/fibers/synchronization.h"

using namespace facade;

ABSL_FLAG(uint32_t, replication_timeout, 30000,
          "Time in milliseconds to wait for the replication writes being stuck.");

ABSL_FLAG(uint32_t, replication_stream_output_limit, 64_KB,
          "Time to wait for the replication output buffer go below the throttle limit");

ABSL_FLAG(uint32_t, migration_buckets_serialization_threshold, 100,
          "The Number of buckets to serialize on each iteration before yielding");

namespace dfly {
using namespace util;
using namespace journal;

namespace {

iovec IoVec(io::Bytes src) {
  return iovec{const_cast<uint8_t*>(src.data()), src.size()};
}

uint32_t replication_stream_output_limit_cached = 64_KB;
uint32_t migration_buckets_serialization_threshold_cached = 100;
uint32_t stalled_writer_base_period_ms = 10;

}  // namespace

JournalStreamer::JournalStreamer(journal::Journal* journal, ExecutionState* cntx, SendLsn send_lsn,
                                 bool is_stable_sync)
    : cntx_(cntx), journal_(journal), is_stable_sync_(is_stable_sync), send_lsn_(send_lsn) {
  // cache the flag to avoid accessing it later.
  replication_stream_output_limit_cached = absl::GetFlag(FLAGS_replication_stream_output_limit);
  last_async_write_time_ = fb2::ProactorBase::GetMonotonicTimeNs() / 1000000;
}

JournalStreamer::~JournalStreamer() {
  if (!cntx_->IsError()) {
    DCHECK_EQ(in_flight_bytes_, 0u);
  }
  VLOG(1) << "~JournalStreamer";
}

void JournalStreamer::ConsumeJournalChange(const JournalItem& item) {
  if (!ShouldWrite(item)) {
    return;
  }

  DCHECK_GT(item.lsn, last_lsn_writen_);
  Write(item.data);
  time_t now = time(nullptr);
  last_lsn_writen_ = item.lsn;
  // TODO: to chain it to the previous Write call.
  if (send_lsn_ == SendLsn::YES && now - last_lsn_time_ > 3) {
    last_lsn_time_ = now;
    io::StringSink sink;
    JournalWriter writer(&sink);
    writer.Write(Entry{journal::Op::LSN, item.lsn});
    Write(std::move(sink).str());
  }
}

void JournalStreamer::Start(util::FiberSocketBase* dest) {
  CHECK(dest_ == nullptr && dest != nullptr);
  dest_ = dest;
  journal_cb_id_ = journal_->RegisterOnChange(this);
  StartStalledDataWriterFiber();
}

void JournalStreamer::Cancel() {
  VLOG(1) << "JournalStreamer::Cancel";
  waker_.notifyAll();
  journal_->UnregisterOnChange(journal_cb_id_);
  StopStalledDataWriterFiber();
  WaitForInflightToComplete();
}

size_t JournalStreamer::UsedBytes() const {
  return pending_buf_.Size();
}

void JournalStreamer::Write(std::string str) {
  DCHECK(!str.empty());
  DVLOG(3) << "Writing " << str.size() << " bytes";

  pending_buf_.Push(std::move(str));
  AsyncWrite(false);
}

void JournalStreamer::StartStalledDataWriterFiber() {
  if (is_stable_sync_ && !stalled_writer_.IsJoinable()) {
    auto pb = fb2::ProactorBase::me();
    std::chrono::milliseconds period_us(stalled_writer_base_period_ms);
    stalled_writer_ = MakeFiber([this, index = pb->GetPoolIndex(), period_us]() mutable {
      ThisFiber::SetName(absl::StrCat("fiber_periodic_journal_writer_", index));
      this->StalledWriterFiber(period_us, &stalled_writer_done_);
    });
  }
}

void JournalStreamer::StalledWriterFiber(std::chrono::milliseconds period_ms,
                                         util::fb2::Done* waiter) {
  while (cntx_->IsRunning()) {
    if (waiter->WaitFor(period_ms)) {
      if (!cntx_->IsRunning()) {
        return;
      }
    }

    // We don't want to force async write to replicate if last data
    // was written recent. Data needs to be stalled for period_ms duration.
    if (!pending_buf_.Size() || in_flight_bytes_ > 0 ||
        ((last_async_write_time_ + period_ms.count()) >
         (fb2::ProactorBase::GetMonotonicTimeNs() / 1000000))) {
      continue;
    }

    AsyncWrite(true);
  }
}

void JournalStreamer::AsyncWrite(bool force_send) {
  // Stable sync or RestoreStreamer replication can't write data until
  // previous AsyncWriter finished.
  if (in_flight_bytes_ > 0) {
    return;
  }

  // Writing in stable sync and outside of fiber needs to check
  // threshold before writing data.
  if (is_stable_sync_ && !force_send &&
      pending_buf_.FrontBufSize() < PendingBuf::Buf::kMaxBufSize) {
    return;
  }

  const auto& cur_buf = pending_buf_.PrepareSendingBuf();

  in_flight_bytes_ = cur_buf.mem_size;
  total_sent_ += in_flight_bytes_;
  last_async_write_time_ = fb2::ProactorBase::GetMonotonicTimeNs() / 1000000;

  const auto v_size = cur_buf.buf.size();
  absl::InlinedVector<iovec, 8> v(v_size);

  for (size_t i = 0; i < v_size; ++i) {
    const auto* uptr = reinterpret_cast<const uint8_t*>(cur_buf.buf[i].data());
    v[i] = IoVec(io::Bytes(uptr, cur_buf.buf[i].size()));
  }

  dest_->AsyncWrite(v.data(), v.size(),
                    [this, len = in_flight_bytes_](std::error_code ec) { OnCompletion(ec, len); });
}

void JournalStreamer::OnCompletion(std::error_code ec, size_t len) {
  DCHECK_EQ(in_flight_bytes_, len);

  DVLOG(3) << "Completing " << in_flight_bytes_;
  in_flight_bytes_ = 0;
  pending_buf_.Pop();
  if (cntx_->IsRunning()) {
    if (ec) {
      cntx_->ReportError(ec);
    } else if (!pending_buf_.Empty()) {
      AsyncWrite(false);
    }
  }

  // notify ThrottleIfNeeded or WaitForInflightToComplete that waits
  // for all the completions to finish.
  // ThrottleIfNeeded can run from multiple fibers in the journal thread.
  // For example, from Heartbeat calling TriggerJournalWriteToSink to flush potential
  // expiration deletions and there are other cases as well.
  waker_.notifyAll();
}

void JournalStreamer::ThrottleIfNeeded() {
  if (!cntx_->IsRunning() || !IsStalled())
    return;

  auto next =
      chrono::steady_clock::now() + chrono::milliseconds(absl::GetFlag(FLAGS_replication_timeout));
  size_t inflight_start = in_flight_bytes_;
  size_t sent_start = total_sent_;

  std::cv_status status =
      waker_.await_until([this]() { return !IsStalled() || !cntx_->IsRunning(); }, next);
  if (status == std::cv_status::timeout) {
    LOG(WARNING) << "Stream timed out, inflight bytes/sent start: " << inflight_start << "/"
                 << sent_start << ", end: " << in_flight_bytes_ << "/" << total_sent_;
    cntx_->ReportError("JournalStreamer write operation timeout");
  }
}

void JournalStreamer::WaitForInflightToComplete() {
  while (in_flight_bytes_) {
    auto next = chrono::steady_clock::now() + 1s;
    std::cv_status status =
        waker_.await_until([this] { return this->in_flight_bytes_ == 0; }, next);
    LOG_IF(WARNING, status == std::cv_status::timeout)
        << "Waiting for inflight bytes " << in_flight_bytes_;
  }
}

void JournalStreamer::StopStalledDataWriterFiber() {
  if (is_stable_sync_ && stalled_writer_.IsJoinable()) {
    stalled_writer_done_.Notify();
    if (stalled_writer_.IsJoinable()) {
      stalled_writer_.Join();
    }
  }
}

bool JournalStreamer::IsStalled() const {
  return pending_buf_.Size() >= replication_stream_output_limit_cached;
}

RestoreStreamer::RestoreStreamer(DbSlice* slice, cluster::SlotSet slots, journal::Journal* journal,
                                 ExecutionState* cntx)
    : JournalStreamer(journal, cntx, JournalStreamer::SendLsn::NO, false),
      db_slice_(slice),
      my_slots_(std::move(slots)) {
  DCHECK(slice != nullptr);
  migration_buckets_serialization_threshold_cached =
      absl::GetFlag(FLAGS_migration_buckets_serialization_threshold);
  db_array_ = slice->databases();  // Inc ref to make sure DB isn't deleted while we use it
}

void RestoreStreamer::Start(util::FiberSocketBase* dest) {
  if (!cntx_->IsRunning())
    return;

  VLOG(1) << "RestoreStreamer start";
  auto db_cb = absl::bind_front(&RestoreStreamer::OnDbChange, this);
  snapshot_version_ = db_slice_->RegisterOnChange(std::move(db_cb));

  JournalStreamer::Start(dest);
}

void RestoreStreamer::Run() {
  VLOG(1) << "RestoreStreamer run";

  PrimeTable::Cursor cursor;
  uint64_t last_yield = 0;
  PrimeTable* pt = &db_array_[0]->prime;

  do {
    if (!cntx_->IsRunning())
      return;
    cursor = pt->TraverseBuckets(cursor, [&](PrimeTable::bucket_iterator it) {
      if (!cntx_->IsRunning())  // Could be cancelled any time as Traverse may preempt
        return;

      db_slice_->FlushChangeToEarlierCallbacks(0 /*db_id always 0 for cluster*/,
                                               DbSlice::Iterator::FromPrime(it), snapshot_version_);

      if (!cntx_->IsRunning())  // Could have been cancelled in above call too
        return;

      std::lock_guard guard(big_value_mu_);

      // Locking this never preempts. See snapshot.cc for why we need it.
      auto* blocking_counter = db_slice_->GetLatch();
      lock_guard blocking_counter_guard(*blocking_counter);

      stats_.buckets_loop += WriteBucket(it);
    });

    if (++last_yield >= migration_buckets_serialization_threshold_cached) {
      ThisFiber::Yield();
      last_yield = 0;
    }
  } while (cursor);

  VLOG(1) << "RestoreStreamer finished loop of " << my_slots_.ToSlotRanges().ToString()
          << ", shard " << db_slice_->shard_id() << ". Buckets looped " << stats_.buckets_loop;
}

void RestoreStreamer::SendFinalize(long attempt) {
  VLOG(1) << "RestoreStreamer LSN of " << my_slots_.ToSlotRanges().ToString() << ", shard "
          << db_slice_->shard_id() << " attempt " << attempt << " with " << stats_.commands
          << " commands. Buckets looped " << stats_.buckets_loop << ", buckets on_db_update "
          << stats_.buckets_on_db_update << ", buckets skipped " << stats_.buckets_skipped
          << ", buckets written " << stats_.buckets_written << ". Keys skipped "
          << stats_.keys_skipped << ", keys written " << stats_.keys_written;

  journal::Entry entry(journal::Op::LSN, attempt);

  io::StringSink sink;
  JournalWriter writer{&sink};
  writer.Write(entry);
  Write(std::move(sink).str());

  // DFLYMIGRATE ACK command has a timeout so we want to send it only when LSN is ready to be sent
  ThrottleIfNeeded();
}

RestoreStreamer::~RestoreStreamer() {
}

void RestoreStreamer::Cancel() {
  auto sver = snapshot_version_;
  snapshot_version_ = 0;  // to prevent double cancel in another fiber
  cntx_->Cancel();
  if (sver != 0) {
    db_slice_->UnregisterOnChange(sver);
    JournalStreamer::Cancel();
  }
}

bool RestoreStreamer::ShouldWrite(const journal::JournalItem& item) const {
  if (item.cmd == "FLUSHALL" || item.cmd == "FLUSHDB") {
    // On FLUSH* we restart the migration
    CHECK(dest_ != nullptr);
    cntx_->ReportError("FLUSH command during migration");
    std::ignore = dest_->Shutdown(SHUT_RDWR);
    return false;
  }

  if (!item.slot.has_value()) {
    return false;
  }

  return ShouldWrite(*item.slot);
}

bool RestoreStreamer::ShouldWrite(std::string_view key) const {
  return ShouldWrite(KeySlot(key));
}

bool RestoreStreamer::ShouldWrite(SlotId slot_id) const {
  return my_slots_.Contains(slot_id);
}

bool RestoreStreamer::WriteBucket(PrimeTable::bucket_iterator it) {
  auto& shard_stats = EngineShard::tlocal()->stats();
  bool written = false;

  if (!it.is_done() && it.GetVersion() < snapshot_version_) {
    stats_.buckets_written++;

    it.SetVersion(snapshot_version_);
    string key_buffer;  // we can reuse it
    for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
      const auto& pv = it->second;
      string_view key = it->first.GetSlice(&key_buffer);
      if (ShouldWrite(key)) {
        ++stats_.keys_written;
        ++shard_stats.total_migrated_keys;
        uint64_t expire = 0;
        if (pv.HasExpire()) {
          auto eit = db_slice_->databases()[0]->expire.Find(it->first);
          expire = db_slice_->ExpireTime(eit);
        }

        WriteEntry(key, it->first, pv, expire);
        written = true;
      } else {
        stats_.keys_skipped++;
      }
    }
  } else {
    stats_.buckets_skipped++;
  }
  ThrottleIfNeeded();

  return written;
}

void RestoreStreamer::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  std::lock_guard guard(big_value_mu_);
  DCHECK_EQ(db_index, 0) << "Restore migration only allowed in cluster mode in db0";

  PrimeTable* table = db_slice_->GetTables(0).first;

  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    stats_.buckets_on_db_update += WriteBucket(*bit);
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [this](PrimeTable::bucket_iterator it) {
      DCHECK_LT(it.GetVersion(), snapshot_version_);
      stats_.buckets_on_db_update += WriteBucket(it);
    });
  }
}

void RestoreStreamer::WriteEntry(string_view key, const PrimeValue& pk, const PrimeValue& pv,
                                 uint64_t expire_ms) {
  CmdSerializer serializer(
      [&](std::string s) {
        Write(std::move(s));
        ThrottleIfNeeded();
      },
      ServerState::tlocal()->serialization_max_chunk_size);
  stats_.commands += serializer.SerializeEntry(key, pk, pv, expire_ms);
}

}  // namespace dfly
