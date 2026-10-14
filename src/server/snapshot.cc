// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

#include <absl/strings/str_cat.h>

#include <mutex>

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
#include "strings/human_readable.h"
#include "util/fibers/fibers.h"
#include "util/fibers/stacktrace.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(bool, background_snapshotting, false, "Whether to run snapshot as a background fiber");

ABSL_FLAG(bool, serialize_hnsw_index, false, "Serialize HNSW vector index graph structure");
ABSL_FLAG(bool, serialization_tagged_chunks, true,
          "Allow serializer output to be split into tagged chunks and reassembled by receiver");
ABSL_FLAG(strings::MemoryBytesFlag, snapshot_output_buffer_limit, 0,
          "Bytes of serialized snapshot data that mutation fibers may leave queued for the "
          "snapshot's flush fiber instead of waiting for the sink write, per shard and per running "
          "snapshot (each full sync and each save holds its own queue). Soft bound: each blocked "
          "producer may add one more chunk, a whole value when serialization_max_chunk_size is 0. "
          "Bounds the write path latency during a full sync or save at the cost of memory, which "
          "counts toward RSS and snapshot_serialization_bytes but not used_memory. 0 makes every "
          "producer wait for its data to be written. Accepts human-readable sizes (e.g. 512mb).");

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
  DCHECK(!flush_fb_.IsJoinable());
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

  serializer_ = std::make_unique<RdbSerializer>(compression_mode_, consume_fun, flush_threshold,
                                                replica_dfly_version_);

  if (allow_flush == SnapshotFlush::kAllow) {
    serializer_->SetTagEntries(absl::GetFlag(FLAGS_serialization_tagged_chunks));
  }

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  pending_limit_ = absl::GetFlag(FLAGS_snapshot_output_buffer_limit);
  stop_flush_ = false;

  // The flush fiber only hands bytes to the sink and mostly waits on it, so it keeps NORMAL
  // priority even in background mode: the chunks of mutation fibers drain through it too.
  flush_fb_ = fb2::Fiber(absl::StrCat("SnapshotFlush-", ProactorBase::me()->GetPoolIndex()),
                         [this] { FlushPendingFb(); });

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
    // Journal streaming keeps producing after the traversal; FinalizeJournalStream stops the
    // flush fiber then. Otherwise every chunk must reach the consumer before Finalize.
    if (!stream_journal)
      StopFlushFb();
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
    StopFlushFb();
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
  StopFlushFb();
}

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::IterateBucketsFb(bool send_full_sync_cut) {
  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    stats_.keys_total += db_slice_->DbSize(db_indx);
  }

  if (!TraverseAllBuckets())
    return;

  PushSerialized(true);

  // A successful traversal leaves no bucket dependency pending.
  DCHECK(!IsAnyBucketBlocked());
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

void SliceSnapshot::PaceTraversal(bool done) {
  if (use_background_mode_) {
    // Yielding for background fibers has low overhead if the time slice isn't used up.
    // Do it after every bucket for maximum responsiveness.
    DCHECK(ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND);
    ThisFiber::Yield();
    PushSerialized(false);
    return;
  }

  const uint64_t kCyclesPerJiffy = base::CycleClock::Frequency() >> 16;  // ~15usec.
  if (!PushSerialized(false) && ThisFiber::GetRunningTimeCycles() > kCyclesPerJiffy) {
    ThisFiber::Yield();
  }
}

void SliceSnapshot::SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                                         time_t expire, uint32_t mc_flags) {
  io::Result<uint8_t> res = serializer_->SaveEntry(pk, pv, expire, mc_flags, db_index);
  if (!res) {
    if (res.error() == errc::operation_canceled) {
      LOG_EVERY_T(WARNING, 1) << "Snapshot cancelled while serializing entry in dbid=" << db_index;
    } else {
      string tmp;
      LOG_EVERY_T(ERROR, 1) << "Snapshot failed to serialize key " << pk.GetSlice(&tmp)
                            << " in dbid=" << db_index << " with err: " << res.error().message();
    }
    return;
  }
  ++type_freq_map_[*res];
}

void SliceSnapshot::HandleFlushData(std::string data, bool safe_point) {
  if (data.empty())
    return;

  if (stream_mu_.is_locked()) {
    ++stats_.flushed_under_lock;
  }
  size_t serialized = data.size();

  if (use_background_mode_) {
    // Yield after possibly long cpu slice due to compression and serialization
    // before possbile suspension of ConsumeData resets the cpu time of the last slice
    if (ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND)
      ThisFiber::Yield();
    // else: This function is invoked from the journal with regular priority as well.
    // TODO: Mavbe Sleep() to provide write backpressure in advance?
  }

  uint64_t running_cycles = ThisFiber::GetRunningTimeCycles();
  bool traversal = snapshot_fb_.IsActive();

  // Every producer is unregistered before StopFlushFb joins the flush fiber: the snapshot fiber's
  // UnregisterChangeListener and FinalizeJournalStream's UnregisterConsumer both wait for the
  // callbacks in flight. So no chunk can be produced once the fiber is gone.
  if (!flush_fb_.IsJoinable()) {
    LOG(DFATAL) << "Snapshot chunk of " << serialized << " bytes produced after the flush fiber "
                << "was stopped";
    consumer_->ConsumeData(std::move(data), base_cntx_);
    return;
  }

  // Appending does not preempt, so FIFO order equals production order across fibers. The queue is
  // accounted by string capacity, which is what it holds.
  size_t queued = data.capacity();
  pending_.push_back({std::move(data), !traversal});
  pending_bytes_ += queued;
  uint64_t seq = ++produced_seq_;
  flush_cv_.notify_one();

  // The traversal paces itself at safe points by waiting for its chunk to be written. Every
  // other call (mutation fibers, and any fiber mid-value while it holds a bucket dependency)
  // returns once its chunk is written or while the queue holds at most pending_limit_ bytes,
  // so a limit of 0 keeps the original wait-for-own-write semantics. The check runs after the
  // append, so the bound is soft by one chunk per blocked producer. Blocking point.
  fb2::NoOpLock lk;
  if (traversal && safe_point) {
    drain_cv_.wait(lk, [&] { return written_seq_ >= seq || !base_cntx_->IsRunning(); });
  } else {
    drain_cv_.wait(lk, [&] {
      return written_seq_ >= seq || pending_bytes_ <= pending_limit_ || !base_cntx_->IsRunning();
    });
  }

  if (traversal && !use_background_mode_) {
    // serializer_->Flush can be quite slow for large values or due to compression, therefore
    // we counter-balance CPU over-usage by sleeping. Only the traversal paces itself this way:
    // a mutation fiber holds its shard's transaction queue, so each sleep lands on every queued
    // command, 240 times for a 15 MB value chunked at 64 KB.
    // We measure running_cycles before the preemption points, because they reset the counter.
    uint64_t sleep_usec = (running_cycles * 1000'000 / base::CycleClock::Frequency()) / 2;
    ThisFiber::SleepFor(chrono::microseconds(std::min<uint64_t>(sleep_usec, 2000ul)));
  }

  VLOG(2) << "Pushed with Serialize() " << serialized;
}

void SliceSnapshot::FlushPendingFb() {
  fb2::NoOpLock lk;
  while (true) {
    flush_cv_.wait(lk, [&] { return !pending_.empty() || stop_flush_; });
    if (pending_.empty())
      return;

    PendingChunk chunk = std::move(pending_.front());
    pending_.pop_front();
    size_t queued = chunk.data.capacity();

    // Track egress just before the socket write, as the producers did when they wrote directly.
    // A chunk from outside the traversal counts as a high priority (out of order) write.
    ServerState::tlocal()->GetEgressThrottler().Record(chunk.data.size(), chunk.high_prio);

    // Blocking point. ConsumeData returns at once when the context is no longer running.
    consumer_->ConsumeData(std::move(chunk.data), base_cntx_);

    pending_bytes_ -= queued;
    ++written_seq_;
    drain_cv_.notify_all();
  }
}

void SliceSnapshot::StopFlushFb() {
  if (!flush_fb_.IsJoinable())
    return;
  stop_flush_ = true;
  flush_cv_.notify_one();
  flush_fb_.Join();
  DCHECK(pending_.empty());
}

std::error_code SliceSnapshot::ConsumeBigValueChunk(std::string data) {
  if (base_cntx_->IsError())
    return base_cntx_->GetError();

  if (base_cntx_->IsCancelled())
    return std::make_error_code(std::errc::operation_canceled);

  HandleFlushData(std::move(data), false);
  ++ServerState::tlocal()->stats.big_value_preemptions;
  return {};
}

size_t SliceSnapshot::FlushSerialized() {
  std::string blob = serializer_->Flush(RdbSerializer::FlushState::kFlushEndEntry);

  size_t serialized = blob.size();
  HandleFlushData(std::move(blob), true);
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
  return (serializer_ ? serializer_->GetBufferCapacity() : 0) + pending_bytes_;
}

size_t SliceSnapshot::GetTempBuffersSize() const {
  return serializer_ ? serializer_->GetTempBufferSize() : 0;
}

RdbSaver::SnapshotStats SliceSnapshot::GetCurrentSnapshotProgress() const {
  return {SerializerBase::GetStats().keys_serialized, stats_.keys_total};
}

}  // namespace dfly
