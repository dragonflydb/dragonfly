// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

#include <absl/functional/bind_front.h>
#include <sys/socket.h>

#include <chrono>

#ifdef __linux__
#include <netinet/tcp.h>
#endif

#include "base/flags.h"
#include "base/logging.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/journal/cmd_serializer.h"
#include "server/journal/serializer.h"
#include "server/rdb_save.h"
#include "server/server_state.h"
#include "util/fibers/synchronization.h"

using namespace facade;

ABSL_FLAG(uint32_t, replication_timeout, 30000,
          "Time in milliseconds to wait for the replication writes being stuck.");

ABSL_FLAG(uint32_t, replication_stream_output_limit, 1_MB,
          "Time to wait for the replication output buffer go below the throttle limit");

ABSL_FLAG(uint32_t, migration_buckets_serialization_threshold, 10,
          "The Number of buckets to serialize on each iteration before yielding");
ABSL_FLAG(uint32_t, migration_buckets_sleep_usec, 500,
          "Sleep time in microseconds after each time we reach "
          "migration_buckets_serialization_threshold");

ABSL_FLAG(float, migration_buckets_cpu_budget, 0.2,
          "How much CPU budget to use for migration buckets serialization");

ABSL_FLAG(uint32_t, replication_dispatch_threshold, 1500,
          "Number of bytes to aggregate before replication");

namespace dfly {
using namespace util;
using namespace journal;
using namespace std;
namespace {

iovec IoVec(io::Bytes src) {
  return iovec{const_cast<uint8_t*>(src.data()), src.size()};
}

uint32_t replication_stream_output_limit_cached = 64_KB;
uint32_t migration_buckets_serialization_threshold_cached = 100;
uint32_t migration_buckets_sleep_usec_cached = 100;
uint32_t replication_dispatch_threshold = 1500;
uint32_t stalled_writer_base_period_ms = 10;

void LogTcpSocketDiagnostics(util::FiberSocketBase* dest) {
  if (!dest) {
    return;
  }

#ifdef __linux__
  // On Linux, we can get TCP diagnostics using getsockopt.
  int sockfd = dest->native_handle();
  if (sockfd < 0) {
    return;
  }

  struct tcp_info info;
  socklen_t info_len = sizeof(info);
  if (getsockopt(sockfd, IPPROTO_TCP, TCP_INFO, &info, &info_len) == 0) {
    LOG_EVERY_T(INFO, 1) << "TCP socket diagnostics - "
                         << "state: " << static_cast<int>(info.tcpi_state)
                         << ", ca_state: " << static_cast<int>(info.tcpi_ca_state)
                         << ", retransmits: " << static_cast<int>(info.tcpi_retransmits)
                         << ", probes: " << static_cast<int>(info.tcpi_probes)
                         << ", backoff: " << static_cast<int>(info.tcpi_backoff)
                         << ", options: " << static_cast<int>(info.tcpi_options)
                         << ", snd_wscale: " << static_cast<int>(info.tcpi_snd_wscale)
                         << ", rcv_wscale: " << static_cast<int>(info.tcpi_rcv_wscale)
                         << ", rto: " << info.tcpi_rto << ", ato: " << info.tcpi_ato
                         << ", snd_mss: " << info.tcpi_snd_mss << ", rcv_mss: " << info.tcpi_rcv_mss
                         << ", unacked: " << info.tcpi_unacked << ", sacked: " << info.tcpi_sacked
                         << ", lost: " << info.tcpi_lost << ", retrans: " << info.tcpi_retrans
                         << ", fackets: " << info.tcpi_fackets
                         << ", last_data_sent: " << info.tcpi_last_data_sent
                         << ", last_ack_sent: " << info.tcpi_last_ack_sent
                         << ", last_data_recv: " << info.tcpi_last_data_recv
                         << ", last_ack_recv: " << info.tcpi_last_ack_recv
                         << ", pmtu: " << info.tcpi_pmtu
                         << ", rcv_ssthresh: " << info.tcpi_rcv_ssthresh
                         << ", rtt: " << info.tcpi_rtt << ", rttvar: " << info.tcpi_rttvar
                         << ", snd_ssthresh: " << info.tcpi_snd_ssthresh
                         << ", snd_cwnd: " << info.tcpi_snd_cwnd << ", advmss: " << info.tcpi_advmss
                         << ", reordering: " << info.tcpi_reordering
                         << ", rcv_rtt: " << info.tcpi_rcv_rtt
                         << ", rcv_space: " << info.tcpi_rcv_space
                         << ", total_retrans: " << info.tcpi_total_retrans;
  } else {
    LOG_EVERY_T(INFO, 1) << "Failed to get TCP socket info: " << strerror(errno);
  }
#endif
}

}  // namespace

JournalStreamer::JournalStreamer(journal::Journal* journal, ExecutionState* cntx,
                                 JournalStreamer::Config config)
    : cntx_(cntx), journal_(journal), config_(config) {
  // cache the flag to avoid accessing it later.
  replication_stream_output_limit_cached = absl::GetFlag(FLAGS_replication_stream_output_limit);
  migration_buckets_sleep_usec_cached = absl::GetFlag(FLAGS_migration_buckets_sleep_usec);
  replication_dispatch_threshold = absl::GetFlag(FLAGS_replication_dispatch_threshold);
  last_async_write_time_ = fb2::ProactorBase::GetMonotonicTimeNs() / 1000000;
}

JournalStreamer::~JournalStreamer() {
  if (!cntx_->IsError()) {
    DCHECK_EQ(in_flight_bytes_, 0u);
  }
  VLOG(1) << "~JournalStreamer";
}

void JournalStreamer::ConsumeJournalChange(const JournalChangeItem& item) {
  if (!ShouldWrite(item)) {
    return;
  }

  DCHECK_GT(item.journal_item.lsn, last_lsn_writen_);
  Write(item.journal_item.data);
  time_t now = time(nullptr);
  last_lsn_writen_ = item.journal_item.lsn;
  // TODO: to chain it to the previous Write call.
  if (config_.should_sent_lsn && now - last_lsn_time_ > 3) {
    last_lsn_time_ = now;
    io::StringSink sink;
    JournalWriter writer(&sink);
    writer.Write(Entry{journal::Op::LSN, last_lsn_writen_});
    Write(std::move(sink).str());
  }
}

void JournalStreamer::Start(util::FiberSocketBase* dest) {
  CHECK(dest_ == nullptr && dest != nullptr);
  dest_ = dest;
  // For partial sync we first catch up from journal replication buffer and only then register.
  if (config_.start_partial_sync_at == 0) {
    journal_cb_id_ = journal_->RegisterOnChange(this);
  }
  StartStalledDataWriterFiber();
}

void JournalStreamer::Cancel() {
  VLOG(1) << "JournalStreamer::Cancel " << cntx_->IsCancelled();
  waker_.notifyAll();
  if (journal_cb_id_) {
    journal_->UnregisterOnChange(journal_cb_id_);
  }
  StopStalledDataWriterFiber();
  WaitForInflightToComplete();
}

size_t JournalStreamer::UsedBytes() const {
  return pending_buf_.Size();
}

std::string JournalStreamer::FormatInternalState() const {
  return absl::StrCat(
      "pending_buf_size:", pending_buf_.Size(), " in_flight_bytes:", in_flight_bytes_,
      " total_sent:", total_sent_, " throttle_count:", throttle_count_,
      " total_throttle_wait_usec:", total_throttle_wait_usec_,
      " throttle_waiters:", throttle_waiters_, " last_async_write_time_ms:", last_async_write_time_,
      " last_lsn_time_s:", last_lsn_time_, " last_lsn_writen_:", last_lsn_writen_);
}

void JournalStreamer::Write(std::string str) {
  DCHECK(!str.empty());
  DVLOG(3) << "Writing " << str.size() << " bytes";

  pending_buf_.Push(std::move(str));
  AsyncWrite(false);
}

void JournalStreamer::StartStalledDataWriterFiber() {
  if (config_.init_from_stable_sync && !stalled_data_writer_.IsJoinable()) {
    auto pb = fb2::ProactorBase::me();
    std::chrono::milliseconds period_us(stalled_writer_base_period_ms);
    stalled_data_writer_ = MakeFiber([this, index = pb->GetPoolIndex(), period_us]() mutable {
      ThisFiber::SetName(absl::StrCat("fiber_periodic_journal_writer_", index));
      this->StalledDataWriterFiber(period_us, &stalled_data_writer_done_);
    });
  }
}

bool JournalStreamer::MaybePartialStreamLSNs() {
  // Same algorithm as SwitchIncrementalFb. The only difference is that we don't sent
  // the old LSN"s via a snapshot but rather as journal changes.
  if (config_.start_partial_sync_at > 0) {
    LSN lsn = config_.start_partial_sync_at;
    DCHECK_LE(lsn, journal_->GetLsn()) << "The replica tried to sync from the future.";

    LOG(INFO) << "Starting partial sync from lsn: " << lsn;
    // The replica sends the LSN of the next entry is wants to receive.
    while (cntx_->IsRunning() && journal_->IsLSNInBuffer(lsn)) {
      JournalChangeItem item;
      item.journal_item.data = journal_->GetEntry(lsn);
      item.journal_item.lsn = lsn;
      ConsumeJournalChange(item);
      lsn++;
    }

    if (!cntx_->IsRunning()) {
      return false;
    }

    if (journal_->GetLsn() != lsn) {
      // We stopped but we didn't manage to send the whole stream.
      cntx_->ReportError(
          std::make_error_code(errc::state_not_recoverable),
          absl::StrCat("Partial sync was unsuccessful because entry #", lsn,
                       " was dropped from the buffer. Current lsn=", journal_->GetLsn()));
      return false;
    }

    // We are done, register back to the journal so we don't miss any changes
    journal_cb_id_ = journal_->RegisterOnChange(this);

    LOG(INFO) << "Last LSN sent in partial sync was " << (lsn - 1);
    // flush pending
    if (pending_buf_.Size() != 0) {
      AsyncWrite(true);
    }
  }
  return true;
}

void JournalStreamer::StalledDataWriterFiber(std::chrono::milliseconds period_ms,
                                             util::fb2::Done* waiter) {
  if (!MaybePartialStreamLSNs()) {
    // Either context got cancelled, or partial sync failed because the lsn's stalled.
    return;
  }

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
  if (config_.init_from_stable_sync && !force_send &&
      pending_buf_.FrontBufSize() < replication_dispatch_threshold) {
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

  DVLOG(3) << "calling AsyncWrite with buff size:" << v.size();
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
      // Enhanced error logging with socket diagnostics for master disconnects
      LOG_EVERY_T(INFO, 1) << "JournalStreamer write error: " << ec.message()
                           << " (code: " << ec.value() << ", category: " << ec.category().name()
                           << ")";

      LogTcpSocketDiagnostics(dest_);

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

void JournalStreamer::StopStalledDataWriterFiber() {
  if (config_.init_from_stable_sync && stalled_data_writer_.IsJoinable()) {
    stalled_data_writer_done_.Notify();
    if (stalled_data_writer_.IsJoinable()) {
      stalled_data_writer_.Join();
    }
  }
}

bool JournalStreamer::IsStalled() const {
  return pending_buf_.Size() >= replication_stream_output_limit_cached;
}

RestoreStreamer::RestoreStreamer(DbSlice* slice, cluster::SlotSet slots, journal::Journal* journal,
                                 ExecutionState* cntx)
    : JournalStreamer(journal, cntx, {}), db_slice_(slice), my_slots_(std::move(slots)) {
  DCHECK(slice != nullptr);
  migration_buckets_serialization_threshold_cached =
      absl::GetFlag(FLAGS_migration_buckets_serialization_threshold);
  db_array_ = slice->databases();  // Inc ref to make sure DB isn't deleted while we use it

  cmd_serializer_ = std::make_unique<CmdSerializer>(
      [&](std::string s) {
        Write(std::move(s));
        ThrottleIfNeeded();
      },
      ServerState::tlocal()->serialization_max_chunk_size);
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

  // Explicitly copy table smart pointer to keep reference count up (flushall drops it)
  boost::intrusive_ptr<DbTable> table = db_array_.front();
  PrimeTable* pt = &table->prime;
  ExpireTable& expire_table = table->expire;
  do {
    if (!cntx_->IsRunning())
      return;

    // If someone else throtles due to huge pending_buf_, give it priority.
    // Apparently, continue goes through the loop by checking the condition below, so we check
    // cursor here as well.
    // In addition if bucket writing was too intensive on CPU and we are overloaded.
    // Note that we account for CPU time from OnDbChange and here as well (inside WriteBucket).
    // But we only throttle here, so if we migrated lots of slots during mutations, we
    // won't progress here but if we have not, then this fiber will progress withing the
    // CPU budget we defined for it.
    bool should_stall =
        throttle_waiters_ > 0 ||
        (pending_buf_.Size() >= replication_stream_output_limit_cached / 3) ||
        cpu_aggregator_.IsOverloaded(absl::GetFlag(FLAGS_migration_buckets_cpu_budget));
    if (cursor && should_stall) {
      ThisFiber::SleepFor(300us);

      // We have a design bug in RealTimeAggregator that resets it measurements only when
      // the next sample is taken. So we add this sample to ensure cpu_aggregator_
      // refreshes its state.
      base::CpuTimeGuard guard(&cpu_aggregator_);
      stats_.iter_skips++;
      continue;
    }

    cursor = pt->TraverseBuckets(cursor, [&](PrimeTable::bucket_iterator it) {
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

      stats_.buckets_loop += WriteBucket(it, expire_table);
    });

    // TODO: FLAGS_migration_buckets_cpu_budget should eventually be a single configurable
    // setting that controls how agressive we are with migration pace.
    // Once we gain confidence with FLAGS_migration_buckets_cpu_budget we should retire
    // migration_buckets_serialization_threshold and migration_buckets_sleep_usec.
    if (++last_yield >= migration_buckets_serialization_threshold_cached) {
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

bool RestoreStreamer::ShouldWrite(const journal::JournalChangeItem& item) const {
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

bool RestoreStreamer::WriteBucket(PrimeTable::bucket_iterator it, const ExpireTable& expire_table) {
  auto& shard_stats = EngineShard::tlocal()->stats();
  bool written = false;

  if (!it.is_done() && it.GetVersion() < snapshot_version_) {
    base::CpuTimeGuard guard(&cpu_aggregator_);

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
          auto eit = expire_table.Find(it->first);
          CHECK(IsValid(eit)) << " " << expire_table.size();
          expire = db_slice_->ExpireTime(eit->second);
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
  // we don't need throttle here, because we throttle after every entry written

  return written;
}

void RestoreStreamer::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  std::lock_guard guard(big_value_mu_);
  DCHECK_EQ(db_index, 0) << "Restore migration only allowed in cluster mode in db0";

  PrimeTable* table = db_slice_->GetTables(0).first;
  ExpireTable* expire_table = db_slice_->GetTables(0).second;
  uint64_t throttle_start = throttle_count_;
  uint64_t throttle_usec_start = total_throttle_wait_usec_;
  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    if (snapshot_version_ == 0) {
      // If snapshot_version_ is 0, it means that Cancel() was called and we shouldn't proceed.
      return;
    }
    stats_.buckets_on_db_update += WriteBucket(*bit, *expire_table);
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [&](PrimeTable::bucket_iterator it) {
      if (snapshot_version_ != 0) {  // we need this check because lambda can be called several
                                     // times and we can preempt in WriteBucket
        DCHECK_LT(it.GetVersion(), snapshot_version_);
        stats_.buckets_on_db_update += WriteBucket(it, *expire_table);
      }
    });
  }
  stats_.throttle_on_db_update += throttle_count_ - throttle_start;
  stats_.throttle_usec_on_db_update += total_throttle_wait_usec_ - throttle_usec_start;
}

void RestoreStreamer::WriteEntry(string_view key, const PrimeKey& pk, const PrimeValue& pv,
                                 uint64_t expire_ms) {
  stats_.commands += cmd_serializer_->SerializeEntry(key, pk, pv, expire_ms);
}

}  // namespace dfly
