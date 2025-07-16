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
ABSL_FLAG(uint32_t, migration_buckets_sleep_usec, 100,
          "Sleep time in microseconds after each time we reach "
          "migration_buckets_serialization_threshold");

namespace dfly {
using namespace util;
using namespace journal;

namespace {

iovec IoVec(io::Bytes src) {
  return iovec{const_cast<uint8_t*>(src.data()), src.size()};
}

uint32_t replication_stream_output_limit_cached = 64_KB;
uint32_t migration_buckets_serialization_threshold_cached = 100;
uint32_t migration_buckets_sleep_usec_cached = 100;

}  // namespace

JournalStreamer::JournalStreamer(journal::Journal* journal, ExecutionState* cntx, SendLsn send_lsn)
    : cntx_(cntx), journal_(journal), send_lsn_(send_lsn) {
  // cache the flag to avoid accessing it later.
  replication_stream_output_limit_cached = absl::GetFlag(FLAGS_replication_stream_output_limit);
  migration_buckets_sleep_usec_cached = absl::GetFlag(FLAGS_migration_buckets_sleep_usec);
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
}

void JournalStreamer::Cancel() {
  VLOG(1) << "JournalStreamer::Cancel " << cntx_->IsCancelled();
  waker_.notifyAll();
  journal_->UnregisterOnChange(journal_cb_id_);
  if (!cntx_->IsError()) {
    WaitForInflightToComplete();
  }
}

size_t JournalStreamer::UsedBytes() const {
  return pending_buf_.Size();
}

void JournalStreamer::AsyncWrite() {
  DCHECK(!pending_buf_.Empty());

  if (in_flight_bytes_ > 0) {
    // We can not flush data while there are in flight requests because AsyncWrite
    // is not atomic. Therefore, we just aggregate.
    return;
  }

  const auto& cur_buf = pending_buf_.PrepareSendingBuf();

  in_flight_bytes_ = cur_buf.mem_size;
  total_sent_ += in_flight_bytes_;

  const auto v_size = cur_buf.buf.size();
  absl::InlinedVector<iovec, 8> v(v_size);

  for (size_t i = 0; i < v_size; ++i) {
    const auto* uptr = reinterpret_cast<const uint8_t*>(cur_buf.buf[i].data());
    v[i] = IoVec(io::Bytes(uptr, cur_buf.buf[i].size()));
  }

  dest_->AsyncWrite(v.data(), v.size(), [this, len = in_flight_bytes_](std::error_code ec) {
    OnCompletion(std::move(ec), len);
  });
}

void JournalStreamer::Write(std::string str) {
  DCHECK(!str.empty());
  DVLOG(3) << "Writing " << str.size() << " bytes";

  pending_buf_.Push(std::move(str));

  AsyncWrite();
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
      AsyncWrite();
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

  ++throttle_count_;
  ++throttle_waiters_;

  const auto start = chrono::steady_clock::now();
  const auto next = start + chrono::milliseconds(absl::GetFlag(FLAGS_replication_timeout));
  auto log_start = start;
  size_t inflight_start = in_flight_bytes_;
  size_t sent_start = total_sent_;

  // Please note that ThrottleIfNeeded is unfair. Specifically with several producers pushing data
  // to this JournalStreamer, one of them may be stalled and the other will be able to
  // progress indefinitely. The stalled producer will be woken up only to verify again that the
  // other one succeeded to push data before it.
  // We currently do not solve this problem, but at least we will be more verbose about it.
  std::cv_status status = waker_.await_until(
      [&] {
        bool finished = !IsStalled() || !cntx_->IsRunning();
        if (finished)
          return finished;

        // Log every second that we are stalled and for how long.
        auto current = chrono::steady_clock::now();
        if (current - log_start > 1000ms) {
          log_start = current;
          LOG(WARNING) << "Waiting for "
                       << chrono::duration_cast<chrono::milliseconds>(current - start).count()
                       << "ms " << ThisFiber::GetName();
        }

        return false;
      },
      next);

  --throttle_waiters_;
  total_throttle_wait_usec_ +=
      chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
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

bool JournalStreamer::IsStalled() const {
  return pending_buf_.Size() >= replication_stream_output_limit_cached;
}

RestoreStreamer::RestoreStreamer(DbSlice* slice, cluster::SlotSet slots, journal::Journal* journal,
                                 ExecutionState* cntx)
    : JournalStreamer(journal, cntx, JournalStreamer::SendLsn::NO),
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

    // If someone else is waiting for the inflight bytes to complete, give it priority.
    // Apparently, continue goes through the loop by checking the condition below, so we check
    // cursor here as well.
    if (cursor &&
        (throttle_waiters_ > 0 || inflight_bytes() > replication_stream_output_limit_cached / 3)) {
      ThisFiber::SleepFor(300us);
      stats_.iter_skips++;
      continue;
    }

    cursor = pt->TraverseBuckets(cursor, [this](PrimeTable::bucket_iterator it) {
      if (!cntx_->IsRunning())  // Could be cancelled any time as Traverse may preempt
        return;

      db_slice_->FlushChangeToEarlierCallbacks(0 /*db_id always 0 for cluster*/,
                                               DbSlice::Iterator::FromPrime(it), snapshot_version_);

      if (!cntx_->IsRunning())  // Could have been cancelled in above call too
        return;

      // Do not progress if we are stalled.
      ThrottleIfNeeded();

      std::lock_guard guard(big_value_mu_);

      // Locking this never preempts. See snapshot.cc for why we need it.
      auto* blocking_counter = db_slice_->GetLatch();
      lock_guard blocking_counter_guard(*blocking_counter);

      stats_.buckets_loop += WriteBucket(it);
    });

    if (++last_yield >= migration_buckets_serialization_threshold_cached) {
      // TODO: to align this with how we sleep in SliceSnapshot::FlushSerialized.
      ThisFiber::SleepFor(chrono::microseconds(migration_buckets_sleep_usec_cached));
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
          << stats_.keys_skipped << ", keys written " << stats_.keys_written
          << " throttle count: " << throttle_count_
          << ", throttle on db update: " << stats_.throttle_on_db_update
          << ", throttle usec on db update: " << stats_.throttle_usec_on_db_update
          << ", iter_skips: " << stats_.iter_skips;

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

  uint64_t throttle_start = throttle_count_;
  uint64_t throttle_usec_start = total_throttle_wait_usec_;
  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    stats_.buckets_on_db_update += WriteBucket(*bit);
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [this](PrimeTable::bucket_iterator it) {
      DCHECK_LT(it.GetVersion(), snapshot_version_);
      stats_.buckets_on_db_update += WriteBucket(it);
    });
  }
  stats_.throttle_on_db_update += throttle_count_ - throttle_start;
  stats_.throttle_usec_on_db_update += total_throttle_wait_usec_ - throttle_usec_start;
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
