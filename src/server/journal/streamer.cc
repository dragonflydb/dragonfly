// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

#include <absl/cleanup/cleanup.h>
#include <sys/socket.h>

#include <chrono>

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

uint32_t migration_buckets_serialization_threshold_cached = 100;
uint32_t migration_buckets_sleep_usec_cached = 100;
constexpr chrono::milliseconds kFlushPeriod{10};

}  // namespace

ReplicaStreamer::ReplicaStreamer(ExecutionState* cntx, LSN start_partial_sync_at)
    : cntx_(cntx),
      start_partial_sync_at_(start_partial_sync_at),
      writer_(cntx, {.dispatch_threshold = absl::GetFlag(FLAGS_replication_dispatch_threshold)}) {
}

ReplicaStreamer::~ReplicaStreamer() {
  VLOG(1) << "~ReplicaStreamer";
}

void ReplicaStreamer::ConsumeJournalChange(const JournalChangeItem& item) {
  if (!cntx_->IsRunning()) {
    return;
  }

  DCHECK_GT(item.journal_item.lsn, last_lsn_writen_);
  writer_.Write(item.journal_item.data);
  time_t now = time(nullptr);
  last_lsn_writen_ = item.journal_item.lsn;
  // TODO: to chain it to the previous Write call.
  if (now - last_lsn_time_ > 3) {
    last_lsn_time_ = now;
    io::StringSink sink;
    JournalWriter writer(&sink);
    writer.Write(Entry{journal::Op::LSN, last_lsn_writen_});
    writer_.Write(std::move(sink).str());
  }
}

void ReplicaStreamer::ThrottleIfNeeded() {
  writer_.Throttle();
}

void ReplicaStreamer::Start(util::FiberSocketBase* dest) {
  writer_.Start(dest);
  // For partial sync we first catch up from journal replication buffer and only then register.
  if (start_partial_sync_at_ == 0) {
    journal_cb_id_ = journal::RegisterConsumer(this);
  }

  auto pb = fb2::ProactorBase::me();
  flush_fiber_ = MakeFiber([this, index = pb->GetPoolIndex()]() {
    ThisFiber::SetName(absl::StrCat("fiber_periodic_journal_writer_", index));
    this->PeriodicFlushFiber(kFlushPeriod);
  });
}

bool ReplicaStreamer::Cancel() {
  VLOG(1) << "ReplicaStreamer::Cancel " << cntx_->IsCancelled();
  writer_.WakeWaiters();
  bool res = false;
  if (journal_cb_id_) {
    auto cb_id = journal_cb_id_;
    journal_cb_id_ = 0;  // Reset to prevent double unregistration in another fiber
    journal::UnregisterConsumer(cb_id);
    res = true;
  }

  if (flush_fiber_.IsJoinable()) {
    flush_fiber_done_.Notify();
    flush_fiber_.Join();
  }

  writer_.WaitForInflightToComplete(false);
  return res;
}

std::string ReplicaStreamer::FormatInternalState() const {
  return absl::StrCat(writer_.FormatInternalState(), " last_lsn_time_s:", last_lsn_time_,
                      " last_lsn_writen_:", last_lsn_writen_);
}

bool ReplicaStreamer::MaybePartialStreamLSNs() {
  // Same algorithm as SwitchIncrementalFb. The only difference is that we don't sent
  // the old LSN"s via a snapshot but rather as journal changes.
  if (start_partial_sync_at_ > 0) {
    LSN lsn = start_partial_sync_at_;
    DCHECK_LE(lsn, journal::GetLsn()) << "The replica tried to sync from the future.";

    LOG(INFO) << "Starting partial sync from lsn: " << lsn;
    // The replica sends the LSN of the next entry is wants to receive.
    while (cntx_->IsRunning() && journal::IsLSNInBuffer(lsn)) {
      JournalChangeItem item;
      item.journal_item.data = journal::GetEntry(lsn);
      item.journal_item.lsn = lsn;
      ConsumeJournalChange(item);
      lsn++;
    }

    if (!cntx_->IsRunning()) {
      return false;
    }

    if (journal::GetLsn() != lsn) {
      // We stopped but we didn't manage to send the whole stream.
      cntx_->ReportError(
          std::make_error_code(errc::state_not_recoverable),
          absl::StrCat("Partial sync was unsuccessful because entry #", lsn,
                       " was dropped from the buffer. Current lsn=", journal::GetLsn()));
      return false;
    }

    // We are done, register back to the journal so we don't miss any changes
    journal_cb_id_ = journal::RegisterConsumer(this);

    LOG(INFO) << "Last LSN sent in partial sync was " << (lsn - 1);
    writer_.Flush();
  }
  return true;
}

void ReplicaStreamer::PeriodicFlushFiber(chrono::milliseconds period) {
  if (!MaybePartialStreamLSNs()) {
    // Either context got cancelled, or partial sync failed because the lsn's stalled.
    return;
  }

  while (cntx_->IsRunning()) {
    if (flush_fiber_done_.WaitFor(period)) {
      if (!cntx_->IsRunning()) {
        return;
      }
    }

    writer_.FlushIfIdle(period);
  }
}

SlotMigrationStreamer::SlotMigrationStreamer(DbSlice* slice, cluster::SlotSet slots,
                                             ExecutionState* cntx)
    : SerializerBase(slice, cntx), cntx_(cntx), writer_(cntx, {}), my_slots_(std::move(slots)) {
  DCHECK(slice != nullptr);
  migration_buckets_serialization_threshold_cached =
      absl::GetFlag(FLAGS_migration_buckets_serialization_threshold);
  migration_buckets_sleep_usec_cached = absl::GetFlag(FLAGS_migration_buckets_sleep_usec);

  cmd_serializer_ = std::make_unique<CmdSerializer>(
      [&](std::string s) {
        writer_.Write(std::move(s));
        writer_.Throttle();
      },
      ServerState::tlocal()->serialization_max_chunk_size);
}

void SlotMigrationStreamer::Start(util::FiberSocketBase* dest) {
  if (!cntx_->IsRunning())
    return;

  VLOG(1) << "SlotMigrationStreamer start";
  SerializerBase::RegisterChangeListener(true);
  writer_.Start(dest);
  journal_cb_id_ = journal::RegisterConsumer(this);
}

void SlotMigrationStreamer::Run() {
  VLOG(1) << "SlotMigrationStreamer run";

  // If the context was cancelled before Start() ran, RegisterChangeListener was skipped and
  // db_array_ is empty (see SlotMigrationStreamer::Start). Guard the db_array_.front() access
  // below; mid-traversal cancellation is handled by the cntx_->IsRunning() check inside the loop.
  if (db_array_.empty())
    return;

  // Covers cancellation exits; on the normal path the entries were already drained.
  absl::Cleanup discard_delayed = [this] { DiscardDelayedEntries(); };

  PrimeTable::Cursor cursor;
  uint64_t last_yield = 0;

  // Explicitly copy table smart pointer to keep reference count up (flushall drops it)
  boost::intrusive_ptr<DbTable> table = db_array_.front();
  PrimeTable* pt = &table->prime;

  do {
    if (!cntx_->IsRunning())
      return;

    // If someone else throtles due to huge pending_buf_, give it priority.
    // Apparently, continue goes through the loop by checking the condition below, so we check
    // cursor here as well.
    // In addition if bucket writing was too intensive on CPU and we are overloaded.
    // Note that we account for CPU time from OnChangeBlocking and here as well
    // (inside WriteBucket).
    // But we only throttle here, so if we migrated lots of slots during mutations, we
    // won't progress here but if we have not, then this fiber will progress withing the
    // CPU budget we defined for it.
    bool should_stall =
        writer_.throttle_waiters() > 0 || (writer_.pending_bytes() >= writer_.output_limit() / 3) ||
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

    // Throttle main loop if we are over the egress limit
    ServerState::tlocal()->GetEgressThrottler().Throttle();

    cursor = pt->TraverseBuckets(cursor, [&](PrimeTable::bucket_iterator it) {
      if (!cntx_->IsRunning())  // Could be cancelled any time as Traverse may preempt
        return;

      // Do not progress if we are stalled.
      ThrottleIfNeeded();

      stats_.buckets_loop += ProcessBucket(0, it, false);
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

  // Force serialize of all delayed entries.
  ProcessDelayedEntries(true, 0, cntx_);

  VLOG(1) << "SlotMigrationStreamer finished loop of " << my_slots_.ToSlotRanges().ToString()
          << ", shard " << db_slice_->shard_id() << ". Buckets looped " << stats_.buckets_loop;
}

void SlotMigrationStreamer::SendFinalize(long attempt) {
  auto base_stats = SerializerBase::GetStats();
  VLOG(1) << "SlotMigrationStreamer LSN of " << my_slots_.ToSlotRanges().ToString() << ", shard "
          << db_slice_->shard_id() << " attempt " << attempt << " with " << stats_.commands
          << " commands. Buckets looped " << stats_.buckets_loop << ", buckets on_db_update "
          << base_stats.buckets_on_change << ", buckets skipped " << base_stats.buckets_skipped
          << ", buckets written " << base_stats.buckets_serialized << ". Keys skipped "
          << stats_.keys_skipped << ", keys written " << base_stats.keys_serialized
          << " throttle count: " << writer_.throttle_count()
          << ", throttle on db update: " << stats_.throttle_on_db_update
          << ", throttle usec on db update: " << stats_.throttle_usec_on_db_update
          << ", iter_skips: " << stats_.iter_skips;

  // Drain all pending journal data before sending the finalize marker.
  // At this point client pause is active, so no new entries can arrive.
  writer_.WaitForInflightToComplete(true);

  journal::Entry entry(journal::Op::LSN, attempt);

  io::StringSink sink;
  JournalWriter writer{&sink};
  writer.Write(entry);
  writer_.Write(std::move(sink).str());

  // DFLYMIGRATE ACK command has a timeout so we want to send it only when LSN is ready to be sent
  ThrottleIfNeeded();
}

SlotMigrationStreamer::~SlotMigrationStreamer() {
}

bool SlotMigrationStreamer::Cancel() {
  // Cancel the execution context unconditionally, even if the streamer was not started yet. A
  // concurrent Start() may run after Cancel() on the same shard (e.g. OutgoingMigration::Finish()
  // cancels the flow while it is between ChangeState(C_SYNC) and PrepareSync()). Start() bails out
  // early when the context is cancelled, which prevents the streamer from being registered (and
  // leaked) after cancellation.
  cntx_->Cancel();

  // UnregisterOnChange is idempotent and returns true only for the caller that actually removed the
  // listener, so racing Cancel() calls (e.g. Finish() vs ~SliceSlotMigration) can't double-erase.
  if (!db_slice_->UnregisterOnChange(this))
    return false;

  writer_.WakeWaiters();
  if (journal_cb_id_) {
    auto cb_id = journal_cb_id_;
    journal_cb_id_ = 0;  // Reset to prevent double unregistration in another fiber
    journal::UnregisterConsumer(cb_id);
  }
  writer_.WaitForInflightToComplete(false);
  return true;
}

void SlotMigrationStreamer::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  if (!ShouldWrite(item)) {
    return;
  }

  DCHECK_GT(item.journal_item.lsn, last_lsn_writen_);
  writer_.Write(item.journal_item.data);
  last_lsn_writen_ = item.journal_item.lsn;
}

void SlotMigrationStreamer::ThrottleIfNeeded() {
  writer_.Throttle();
}

bool SlotMigrationStreamer::ShouldWrite(const journal::JournalChangeItem& item) const {
  if (item.cmd == "FLUSHALL" || item.cmd == "FLUSHDB") {
    // On FLUSH* we restart the migration
    CHECK(writer_.dest() != nullptr);
    cntx_->ReportError("FLUSH command during migration");
    std::ignore = writer_.dest()->Shutdown(SHUT_RDWR);
    return false;
  }

  if (!item.slot.has_value()) {
    return false;
  }

  return ShouldWrite(*item.slot);
}

bool SlotMigrationStreamer::ShouldWrite(std::string_view key) const {
  return ShouldWrite(KeySlot(key));
}

bool SlotMigrationStreamer::ShouldWrite(SlotId slot_id) const {
  return my_slots_.Contains(slot_id);
}

unsigned SlotMigrationStreamer::SerializeBucketLocked(DbIndex db_index,
                                                      PrimeTable::bucket_iterator it,
                                                      bool on_update) {
  auto& shard_stats = EngineShard::tlocal()->stats();

  unsigned written = 0;
  std::string key_buffer;

  base::CpuTimeGuard guard(&cpu_aggregator_);

  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    const auto& pv = it->second;
    string_view key = it->first.GetSlice(&key_buffer);
    if (ShouldWrite(key)) {
      ++shard_stats.total_migrated_keys;
      SerializerBase::SerializeEntry(it.bucket_address(), db_index, it->first, pv);
      ++written;
    } else {
      stats_.keys_skipped++;
    }
  }

  // we don't need throttle here, because we throttle after every entry written
  return written;
}

// TODO: Update those
// stats_.throttle_on_db_update += throttle_count_ - throttle_start;
// stats_.throttle_usec_on_db_update += total_throttle_wait_usec_ - throttle_usec_start;

void SlotMigrationStreamer::SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk,
                                                 const PrimeValue& pv, time_t expire,
                                                 uint32_t mc_flags) {
  stats_.commands += cmd_serializer_->SerializeEntry(pk.ToString(), pk, pv, expire);
}

}  // namespace dfly
