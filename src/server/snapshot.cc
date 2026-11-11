// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

#include <absl/strings/str_cat.h>

#include <limits>
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

using facade::operator""_MB;

ABSL_FLAG(bool, background_snapshotting, false, "Whether to run snapshot as a background fiber");

ABSL_FLAG(bool, serialize_hnsw_index, false, "Serialize HNSW vector index graph structure");
ABSL_FLAG(bool, serialization_tagged_chunks, true,
          "Allow serializer output to be split into tagged chunks and reassembled by receiver");
ABSL_FLAG(strings::MemoryBytesFlag, snapshot_output_buffer_limit, 64_MB,
          "Bytes of serialized data a shard's snapshot may queue for the sink before producers "
          "wait for it. Lets a write that serializes a bucket inline during a save or a full sync "
          "proceed without waiting on a slow replica or storage. 0 makes every producer wait for "
          "its data to be written.");

ABSL_DECLARE_FLAG(uint32_t, replication_stream_output_limit);

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

// Between traversal steps, wait for at most this multiple of traversal output to drain.
constexpr size_t kTraverseMultiplier = 4;

// True once the current fiber has run for more than a jiffy (~15usec) since it was last resumed,
// i.e. it should yield to let the other fibers of the thread run.
bool CpuSliceUsedUp() {
  return ThisFiber::GetRunningTimeCycles() > (base::CycleClock::Frequency() >> 16);
}

}  // namespace

SliceSnapshot::SliceSnapshot(CompressionMode compression_mode, DbSlice* slice,
                             SnapshotDataConsumerInterface* consumer, ExecutionState* cntx,
                             DflyVersion replica_dfly_version)
    : SerializerBase(slice, cntx),
      writer_(cntx, [this](string data) { consumer_->ConsumeData(std::move(data), base_cntx_); }),
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

  writer_.Start(absl::StrCat("SnapshotWriter-", ProactorBase::me()->GetPoolIndex()),
                absl::GetFlag(FLAGS_snapshot_output_buffer_limit));

  use_background_mode_ = absl::GetFlag(FLAGS_background_snapshotting);
  SerializerBase::RegisterChangeListener(stream_journal);

  if (stream_journal) {
    journal_cb_id_ = journal::RegisterConsumer(this);
  }

  size_t flush_threshold = 0;
  RdbSerializer::ConsumeFun consume_fun;
  if (allow_flush == SnapshotFlush::kAllow) {
    // 0 disables splitting by size. The callback is still needed for forced consume points.
    flush_threshold = ServerState::tlocal()->serialization_max_chunk_size;
    if (flush_threshold == 0)
      flush_threshold = std::numeric_limits<size_t>::max();

    // The callback receives data directly from the serializer, no need to call back into it.
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

    // Journal streaming keeps producing until FinalizeJournalStream, which stops the writer then.
    if (!stream_journal)
      writer_.Stop();
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
    writer_.Stop();
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

  // Every producer is unregistered, so the writer can drain and stop.
  writer_.Stop();
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
    // DFLY STARTSTABLE, which the replica sends after the cut, stops the writer under a global
    // transaction and the replica's mutex. A replica that stops reading would block that drain,
    // and every shard with it, where replication_timeout can not break it, so by then nothing may
    // be queued beyond the chunks producers still wait on, as without the buffer. Lower the limit
    // to 0 gradually: dropping it at once would make the next journal flush wait for the whole
    // backlog while it holds the shard's transaction queue. Starting the stable sync bound above
    // the backlog keeps a flush right at the cut from waiting for the write in flight.
    writer_.LowerLimit(0, absl::GetFlag(FLAGS_replication_stream_output_limit));
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

void SliceSnapshot::PaceTraversal(bool /*done*/) {
  if (use_background_mode_) {
    // Yielding for background fibers has low overhead if the time slice isn't used up.
    // Do it after every bucket for maximum responsiveness.
    DCHECK(ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND);
    ThisFiber::Yield();
    ThrottleIfNeeded();
  } else {
    // Skip the flush while another fiber is mid-entry, as in ThrottleIfNeeded.
    bool pushed = !stream_mu_.is_locked() && PushSerialized(false);
    if (!pushed && CpuSliceUsedUp())
      ThisFiber::Yield();
  }

  // Between buckets no dependency is held, so this is where the traversal waits for the sink.
  // Cap the drain wait at four times the traversal output since the previous step. The low
  // watermark still applies, and later mutation chunks do not extend this pacing wait.
  size_t pending = writer_.pending_bytes();
  size_t budget = traversal_bytes_since_pace_ * kTraverseMultiplier;
  size_t remaining_after_budget = pending > budget ? pending - budget : 0;
  size_t chunk_size = ServerState::tlocal()->serialization_max_chunk_size;
  size_t min_to_wait = min(writer_.limit() / 4, chunk_size);
  size_t allowed_remaining = max(min_to_wait, remaining_after_budget);
  writer_.WaitBelow(allowed_remaining);
  traversal_bytes_since_pace_ = 0;
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

void SliceSnapshot::HandleFlushData(std::string data) {
  if (data.empty())
    return;

  if (stream_mu_.is_locked()) {
    ++stats_.flushed_under_lock;
  }
  size_t serialized = data.size();

  // We measure running_cycles before the preemption points, because they reset the counter.
  uint64_t running_cycles = ThisFiber::GetRunningTimeCycles();

  bool on_mutation_path = OnMutationPath();
  if (!on_mutation_path)
    traversal_bytes_since_pace_ += data.capacity();

  // Track egress as the chunk is queued. Attribute it to a high priority (out of order) write when
  // it comes from the mutation path.
  ServerState::tlocal()->GetEgressThrottler().Record(serialized, on_mutation_path);

  // Nothing preempts between taking the chunk from the serializer and queueing it, so the queue
  // order is the production order across fibers. Blocking point.
  writer_.Push(std::move(data));

  if (use_background_mode_) {
    // Yield after possibly long cpu slice due to compression and serialization. Only after the
    // push: a fiber running during the yield must queue behind this chunk.
    if (ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND)
      ThisFiber::Yield();
    // else: This function is invoked from the journal with regular priority as well.
    // TODO: Mavbe Sleep() to provide write backpressure in advance?
  }

  // A mutation fiber holds its shard's transaction queue, so only the traversal paces itself.
  if (!on_mutation_path && !use_background_mode_) {
    // serializer_->Flush can be quite slow for large values or due to compression, therefore
    // we counter-balance CPU over-usage by sleeping.
    uint64_t sleep_usec = (running_cycles * 1000'000 / base::CycleClock::Frequency()) / 2;
    ThisFiber::SleepFor(chrono::microseconds(std::min<uint64_t>(sleep_usec, 2000ul)));
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
  // Without tagged chunks, an entry holds stream_mu_ while it may suspend mid-entry (e.g. reading a
  // tiered list node). Flushing then would cut it, so leave the flush to a later call.
  if (stream_mu_.is_locked())
    return;
  PushSerialized(false);
}

size_t SliceSnapshot::GetBufferCapacity() const {
  return (serializer_ ? serializer_->GetBufferCapacity() : 0) + writer_.pending_bytes();
}

size_t SliceSnapshot::GetTempBuffersSize() const {
  return serializer_ ? serializer_->GetTempBufferSize() : 0;
}

RdbSaver::SnapshotStats SliceSnapshot::GetCurrentSnapshotProgress() const {
  return {SerializerBase::GetStats().keys_serialized, stats_.keys_total};
}

}  // namespace dfly
