// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

#include <absl/strings/str_cat.h>

#include <mutex>
#include <utility>

#include "base/cycle_clock.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/execution_state.h"
#include "server/journal/journal.h"
#include "server/rdb_extensions.h"
#include "server/rdb_save.h"
#include "server/search/global_hnsw_index.h"
#include "server/search/serialization_utils.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "util/fibers/fibers.h"
#include "util/fibers/stacktrace.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(bool, background_snapshotting, false, "Whether to run snapshot as a background fiber");
ABSL_FLAG(bool, serialize_hnsw_index, false, "Serialize HNSW vector index graph structure");
ABSL_FLAG(bool, serialization_tagged_chunks, true,
          "Allow serializer output to be split into tagged chunks and reassembled by receiver");

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;

using facade::operator""_KB;

namespace {
thread_local absl::flat_hash_set<SliceSnapshot*> tl_slice_snapshots;

// Controls the chunks size for pushing serialized data. The larger the chunk the more CPU
// it may require (especially with compression), and less responsive the server may be.
constexpr size_t kMinBlobSize = 8_KB;

}  // namespace

SliceSnapshot::SliceSnapshot(CompressionMode compression_mode, DbSlice* slice,
                             SnapshotDataConsumerInterface* consumer, ExecutionState* cntx,
                             DflyVersion replica_dfly_version)
    : SerializerBase(slice, cntx),
      compression_mode_(compression_mode),
      replica_dfly_version_(replica_dfly_version),
      consumer_(consumer) {
  tl_slice_snapshots.insert(this);
}

SliceSnapshot::~SliceSnapshot() {
  DCHECK(db_slice_->shard_owner()->IsMyThread());
  tl_slice_snapshots.erase(this);
}

size_t SliceSnapshot::GetThreadLocalMemoryUsage() {
  size_t mem = 0;
  for (SliceSnapshot* snapshot : tl_slice_snapshots) {
    mem += snapshot->GetBufferCapacity();
  }
  return mem;
}

bool SliceSnapshot::IsSnaphotInProgress() {
  return !tl_slice_snapshots.empty();
}

void SliceSnapshot::Start(bool stream_journal, SnapshotFlush allow_flush) {
  DCHECK(!snapshot_fb_.IsJoinable());

  use_background_mode_ = absl::GetFlag(FLAGS_background_snapshotting);
  SerializerBase::RegisterChangeListener(stream_journal);

  if (stream_journal) {
    journal_cb_id_ = journal::RegisterConsumer(this);
  }

  size_t flush_threshold = 0;
  RdbSerializer::ConsumeFun consume_fun;
  if (allow_flush == SnapshotFlush::kAllow) {
    flush_threshold = ServerState::tlocal()->serialization_max_chunk_size;
    // The callback receives data directly from the serializer, no need to call back into it.
    if (flush_threshold != 0)
      consume_fun = std::bind_front(&SliceSnapshot::ConsumeBigValueChunk, this);
  }

  bool serialize_index = SaveMode() != dfly::SaveMode::RDB &&
                         absl::GetFlag(FLAGS_serialize_hnsw_index) &&
                         replica_dfly_version_ >= DflyVersion::VER6;

  serializer_ = std::make_unique<RdbSerializer>(compression_mode_, consume_fun, flush_threshold);

  if (allow_flush == SnapshotFlush::kAllow) {
    serializer_->SetTagEntries(absl::GetFlag(FLAGS_serialization_tagged_chunks));
  }

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  fb2::Fiber::Opts opts{.priority = use_background_mode_ ? fb2::FiberPriority::BACKGROUND
                                                         : fb2::FiberPriority::NORMAL,
                        .name = absl::StrCat("SliceSnapshot-", ProactorBase::me()->GetPoolIndex())};
  snapshot_fb_ = fb2::Fiber(opts, [this, stream_journal, serialize_index] {
    if (serialize_index) {
      // TODO add error processing for index serialization
      SearchSerializer::Serialize(serializer_.get(), db_slice_,
                                  std::bind(&SliceSnapshot::PushSerialized, this, false));
    }
    this->IterateBucketsFb(stream_journal);
    UnregisterChangeListener();
    consumer_->Finalize();
    VLOG(1) << "Serialization peak bytes: " << serializer_->GetSerializationPeakBytes();
  });
}

// Called only for replication use-case.
void SliceSnapshot::FinalizeJournalStream(bool cancel) {
  VLOG(1) << "FinalizeJournalStream";
  DCHECK(db_slice_->shard_owner()->IsMyThread());
  if (!journal_cb_id_) {  // Finalize only once.
    // In case of incremental snapshotting in StartIncremental, if an error is encountered,
    // journal_cb_id_ may not be set, but the snapshot fiber is still running.
    snapshot_fb_.JoinIfNeeded();
    return;
  }
  uint32_t cb_id = journal_cb_id_;
  journal_cb_id_ = 0;

  // Wait for serialization to finish in any case.
  snapshot_fb_.JoinIfNeeded();

  journal::UnregisterConsumer(cb_id);
  if (!cancel) {
    // always succeeds because serializer_ flushes to string.
    VLOG(1) << "FinalizeJournalStream lsn: " << journal::GetLsn();
    std::ignore = serializer_->SendJournalOffset(journal::GetLsn());
    PushSerialized(true);
  }
}

// The algorithm is to go over all the buckets and serialize those with
// version < snapshot_version_. In order to serialize each physical bucket exactly once we update
// bucket version to snapshot_version_ once it has been serialized.
// We handle serialization at physical bucket granularity.
// To further complicate things, Table::Traverse covers a logical bucket that may comprise of
// several physical buckets in dash table. For example, items belonging to logical bucket 0
// can reside in buckets 0,1 and stash buckets 56-59.
// PrimeTable::Traverse guarantees an atomic traversal of a single logical bucket,
// it also guarantees 100% coverage of all items that exists when the traversal started
// and survived until it finished.

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::IterateBucketsFb(bool send_full_sync_cut) {
  const uint64_t kCyclesPerJiffy = base::CycleClock::Frequency() >> 16;  // ~15usec.

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    stats_.keys_total += db_slice_->DbSize(db_indx);
  }

  for (DbIndex snapshot_db_indx = 0; snapshot_db_indx < db_array_.size(); ++snapshot_db_indx) {
    if (!base_cntx_->IsRunning())
      return;

    if (!db_array_[snapshot_db_indx])
      continue;

    PrimeTable* pt = &db_array_[snapshot_db_indx]->prime;
    VLOG(1) << "Start traversing " << pt->size() << " items for index " << snapshot_db_indx;

    do {
      if (!base_cntx_->IsRunning())
        return;

      snapshot_cursor_ = pt->TraverseBuckets(
          snapshot_cursor_,
          [this, snapshot_db_indx](auto it) { ProcessBucket(snapshot_db_indx, it, false); },
          true /* include empty buckets */);

      if (use_background_mode_) {
        // Yielding for background fibers has low overhead if the time slice isn't used up.
        // Do it after every bucket for maximum responsiveness.
        DCHECK(ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND);
        ThisFiber::Yield();
        PushSerialized(false);
      } else {
        if (!PushSerialized(false)) {
          if (ThisFiber::GetRunningTimeCycles() > kCyclesPerJiffy) {
            ThisFiber::Yield();
          }
        }

        // Pay down CPU-time debt accrued by HandleFlushData() (including any big-value
        // chunk flushes) since the last batch. This is the only place the snapshot fiber's
        // backpressure sleep runs - always between TraverseBuckets() batches, so it never
        // happens while a bucket's BucketDependencies latch is held.
        //
        // Clear the counter *before* sleeping, not after: SleepFor() yields, so a write
        // command's inline catch-up serialization (HandleFlushData on its own fiber) can
        // run concurrently and add to accrued_run_cycles_ while we're asleep. Clearing
        // after the sleep would silently drop that newly-accrued debt; clearing first means
        // it accrues against an already-zeroed counter and is correctly picked up next batch.
        if (accrued_run_cycles_ > 0) {
          uint64_t debt = std::exchange(accrued_run_cycles_, 0);
          uint64_t sleep_usec = (debt * 1000'000 / base::CycleClock::Frequency()) / 2;
          ThisFiber::SleepFor(chrono::microseconds(std::min<uint64_t>(sleep_usec, 2000ul)));
        }
      }
    } while (snapshot_cursor_);

    // Wait for all the outstanding delayed entries and serialize them as well.
    ProcessDelayedEntries(true, 0, base_cntx_);

    PushSerialized(true);
  }  // for (dbindex)

  CHECK(!serialize_bucket_running_);
  if (send_full_sync_cut) {
    CHECK(!serializer_->SendFullSyncCut());
    PushSerialized(true);
  }

  if (VLOG_IS_ON(1)) {
    auto stats = SerializerBase::GetStats();

    // serialized + side_saved must be equal to the total saved.
    VLOG(1) << "Exit SnapshotSerializer total_serialized: " << stats.keys_serialized
            << ", buckets side saved " << stats.buckets_on_change << ", total bucket saved "
            << stats.buckets_serialized << ", journal_saved " << stats_.jounal_changes;
  }
}

unsigned SliceSnapshot::SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                              bool on_update) {
  // traverse physical bucket and write it into string file.
  serialize_bucket_running_ = true;

  unsigned serialized = 0;

  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    // Version is already stamped by SerializerBase::ProcessBucket.
    DCHECK_EQ(it.GetVersion(), snapshot_version_);

    ++serialized;

    // might preempt due to big value serialization.
    SerializerBase::SerializeEntry(it.bucket_address(), db_index, it->first, it->second);
  }

  serialize_bucket_running_ = false;
  return serialized;
}

void SliceSnapshot::SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                                         time_t expire, uint32_t mc_flags) {
  io::Result<uint8_t> res = serializer_->SaveEntry(pk, pv, expire, mc_flags, db_index);
  LOG_IF(ERROR, !res.has_value()) << "Serialization error: " << res.error();
  if (res)
    ++type_freq_map_[*res];
}

void SliceSnapshot::HandleFlushData(std::string data) {
  if (data.empty())
    return;

  if (stream_mu_.is_locked()) {
    ++stats_.flushed_under_lock;
  }
  size_t serialized = data.size();
  uint64_t id = rec_id_++;

  if (use_background_mode_) {
    // Yield after possibly long cpu slice due to compression and serialization
    // before possbile suspension of ConsumeData resets the cpu time of the last slice
    if (ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND)
      ThisFiber::Yield();
    // else: This function is invoked from the journal with regular priority as well.
    // TODO: Mavbe Sleep() to provide write backpressure in advance?
  }

  uint64_t running_cycles = ThisFiber::GetRunningTimeCycles();

  fb2::NoOpLock lk;
  // We create a critical section here that ensures that records are pushed in sequential order.
  // As a result, it is not possible for two fiber producers to push concurrently.
  // If A.id = 5, and then B.id = 6, and both are blocked here, it means that last_pushed_id_ < 4.
  // Once last_pushed_id_ = 4, A will be unblocked, while B will wait until A finishes pushing and
  // update last_pushed_id_ to 5.
  seq_cond_.wait(lk, [&] { return id == this->last_pushed_id_ + 1; });

  // Blocking point.
  consumer_->ConsumeData(std::move(data), base_cntx_);

  DCHECK_EQ(last_pushed_id_ + 1, id);
  last_pushed_id_ = id;
  seq_cond_.notify_all();

  // Accrue CPU-time debt instead of sleeping here - see accrued_run_cycles_ comment in the
  // header. Accrued regardless of which fiber runs this call (snapshot_fb_'s own traversal,
  // or a write command's inline catch-up serialization): it's a shared serialization budget,
  // and only IterateBucketsFb ever pays it down, so a write's own execution is never delayed
  // by it either way.
  if (!use_background_mode_) {
    accrued_run_cycles_ += running_cycles;
  }

  VLOG(2) << "Pushed with Serialize() " << serialized;
}

std::error_code SliceSnapshot::ConsumeBigValueChunk(std::string data) {
  if (base_cntx_->IsError())
    return base_cntx_->GetError();

  if (base_cntx_->IsCancelled())
    return std::make_error_code(std::errc::operation_canceled);

  HandleFlushData(std::move(data));
  ++ServerState::tlocal()->stats.big_value_preemptions;
  return {};
}

size_t SliceSnapshot::FlushSerialized() {
  std::string blob = serializer_->Flush(RdbSerializer::FlushState::kFlushEndEntry);

  size_t serialized = blob.size();
  HandleFlushData(std::move(blob));
  return serialized;
}

bool SliceSnapshot::PushSerialized(bool force) {
  if (!force && serializer_->SerializedLen() < kMinBlobSize)
    return false;
  return FlushSerialized();
}

// stream_mu_ prevents expiry/eviction DEL journal entries from interleaving with an
// in-progress SaveEntry for a large value. SaveEntry may yield mid-entry (emitting chunks
// across multiple scheduler turns); expiry paths emit DEL via RecordDelete directly,
// bypassing OnChange. Without the lock, such a DEL could be written between two chunks
// of the same entry, producing an invalid wire format for the downstream consumer.
//
// Note: even if the protocol were extended to support interleaved chunks, the lock would
// still be required semantically: a DEL journal entry must not be applied on the replica
// while the entry's baseline is still being loaded. The delayed deletion queue proposal
// in the design doc addresses this without a shard-wide lock.
//
// Note: for transaction-driven mutations, baseline-before-journal ordering is already
// guaranteed by call order on the mutation fiber (OnChange precedes ConsumeJournalChange);
// stream_mu_ is not needed for that ordering.
void SliceSnapshot::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  std::lock_guard lk{stream_mu_};
  std::ignore = serializer_->WriteJournalEntry(item.journal_item.data);
  ++stats_.jounal_changes;
}

void SliceSnapshot::ThrottleIfNeeded() {
  PushSerialized(false);
}

size_t SliceSnapshot::GetBufferCapacity() const {
  return serializer_ ? serializer_->GetBufferCapacity() : 0;
}

size_t SliceSnapshot::GetTempBuffersSize() const {
  return serializer_ ? serializer_->GetTempBufferSize() : 0;
}

RdbSaver::SnapshotStats SliceSnapshot::GetCurrentSnapshotProgress() const {
  return {SerializerBase::GetStats().keys_serialized, stats_.keys_total};
}

}  // namespace dfly
