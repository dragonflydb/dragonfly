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
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "util/fibers/stacktrace.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(bool, point_in_time_snapshot, true, "If true replication uses point in time snapshoting");
ABSL_FLAG(bool, background_snapshotting, false, "Whether to run snapshot as a background fiber");
ABSL_FLAG(bool, serialize_hnsw_index, false, "Serialize HNSW vector index graph structure");

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
    : SerializerBase(slice),
      compression_mode_(compression_mode),
      replica_dfly_version_(replica_dfly_version),
      consumer_(consumer),
      cntx_(cntx) {
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
  RegisterChangeListener();

  if (stream_journal) {
    journal_cb_id_ = journal::RegisterConsumer(this);
  }

  size_t flush_threshold = 0;
  RdbSerializer::ConsumeFun consume_fun;
  if (allow_flush == SnapshotFlush::kAllow) {
    flush_threshold = ServerState::tlocal()->serialization_max_chunk_size;
    if (flush_threshold != 0) {
      // The callback receives data directly from the serializer, no need to call back into it.
      consume_fun = [this](std::string data) {
        HandleFlushData(std::move(data));
        VLOG(2) << "HandleFlushData via callback";
        ++ServerState::tlocal()->stats.big_value_preemptions;
      };
    }
  }
  serializer_ = std::make_unique<RdbSerializer>(compression_mode_, consume_fun, flush_threshold);

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  fb2::Fiber::Opts opts{.priority = use_background_mode_ ? fb2::FiberPriority::BACKGROUND
                                                         : fb2::FiberPriority::NORMAL,
                        .name = absl::StrCat("SliceSnapshot-", ProactorBase::me()->GetPoolIndex())};
  snapshot_fb_ = fb2::Fiber(opts, [this, stream_journal] {
    // TODO add error processing for index serialization
    SerializeIndexMappings();
    SerializeGlobalHnswIndices();
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

void SliceSnapshot::SerializeIndexMapping(
    uint32_t shard_id, std::string_view index_name,
    const std::vector<std::pair<std::string, search::DocId>>& mappings) {
  // Format: [RDB_OPCODE_SHARD_DOC_INDEX, shard_id, index_name, mapping_count,
  //          then for each mapping: key_string, doc_id]
  if (auto ec = serializer_->WriteOpcode(RDB_OPCODE_SHARD_DOC_INDEX); ec)
    return;
  if (auto ec = serializer_->SaveLen(shard_id); ec)
    return;
  if (auto ec = serializer_->SaveString(index_name); ec)
    return;
  if (auto ec = serializer_->SaveLen(mappings.size()); ec)
    return;

  for (const auto& [key, doc_id] : mappings) {
    if (auto ec = serializer_->SaveString(key); ec)
      return;
    if (auto ec = serializer_->SaveLen(doc_id); ec)
      return;
  }
  PushSerialized(false);
}

void SliceSnapshot::SerializeIndexMappings() {
#ifdef WITH_SEARCH
  if (SaveMode() == dfly::SaveMode::RDB || !absl::GetFlag(FLAGS_serialize_hnsw_index) ||
      replica_dfly_version_ < DflyVersion::VER6) {
    return;
  }

  // Get all HNSW index names from the global registry
  absl::flat_hash_set<std::string> hnsw_index_names =
      GlobalHnswIndexRegistry::Instance().GetIndexNames();

  auto* indices = db_slice_->shard_owner()->search_indices();
  uint32_t shard_id = db_slice_->shard_owner()->shard_id();

  for (const auto& index_name : hnsw_index_names) {
    auto* index = indices->GetIndex(index_name);
    if (!index) {
      continue;
    }

    auto mappings = index->SerializeKeyIndex();
    if (mappings.empty()) {
      continue;
    }

    SerializeIndexMapping(shard_id, index_name, mappings);
  }
#endif
}

void SliceSnapshot::SerializeGlobalHnswIndices() {
#ifdef WITH_SEARCH
  // Serialize HNSW global indices for shard 0 only
  if (db_slice_->shard_owner()->shard_id() != 0 || SaveMode() == dfly::SaveMode::RDB ||
      !absl::GetFlag(FLAGS_serialize_hnsw_index) || replica_dfly_version_ < DflyVersion::VER6) {
    return;
  }

  auto all_indices = GlobalHnswIndexRegistry::Instance().GetAll();

  // Preallocate buffer for HNSW entry serialization.
  std::vector<uint8_t> tmp_buf;

  for (const auto& [index_key, index] : all_indices) {
    {
      // Acquire a read lock to ensure a consistent snapshot of the graph.
      // While held, Add/Remove calls will defer into the adapter's internal list
      // and will be replayed automatically on the next write operation.
      auto read_lock = index->GetReadLock();

      // Format: [RDB_OPCODE_VECTOR_INDEX, index_name, elements_number,
      //          then for each node: binary encoded entry via SaveHNSWEntry]
      if (auto ec = serializer_->WriteOpcode(RDB_OPCODE_VECTOR_INDEX); ec) {
        continue;
      }
      if (auto ec = serializer_->SaveString(index_key); ec) {
        continue;
      }

      size_t node_count = index->GetNodeCount();
      if (auto ec = serializer_->SaveLen(node_count); ec) {
        continue;
      }

      constexpr size_t kBatchSize = 1000;
      for (size_t i = 0; i < node_count; i += kBatchSize) {
        size_t batch_end = std::min(i + kBatchSize, node_count);
        auto nodes = index->GetNodesRange(i, batch_end);
        for (const auto& node : nodes) {
          tmp_buf.resize(node.TotalSize());
          if (auto ec = serializer_->SaveHNSWEntry(node, absl::MakeSpan(tmp_buf)); ec)
            break;
        }
      }
    }  // read_lock released here

    // Flush after completing entire index to avoid splitting HNSW data across compressed blobs.
    // The HNSW loader expects all nodes for an index to be readable in one pass.
    PushSerialized(false);
  }
#endif
}

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::IterateBucketsFb(bool send_full_sync_cut) {
  const uint64_t kCyclesPerJiffy = base::CycleClock::Frequency() >> 16;  // ~15usec.

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    stats_.keys_total += db_slice_->DbSize(db_indx);
  }

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    if (!cntx_->IsRunning())
      return;

    if (!db_array_[db_indx])
      continue;

    PrimeTable* pt = &db_array_[db_indx]->prime;
    VLOG(1) << "Start traversing " << pt->size() << " items for index " << db_indx;

    do {
      if (!cntx_->IsRunning()) {
        return;
      }

      snapshot_cursor_ = pt->TraverseBuckets(
          snapshot_cursor_, [this, db_indx](auto it) { return BucketSaveCb(db_indx, it); });

      if (use_background_mode_) {
        // Yielding for background fibers has low overhead if the time slice isn't used up.
        // Do it after every bucket for maximum responsiveness.
        DCHECK(ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND);
        ThisFiber::Yield();
        PushSerialized(false);
      } else {
        if (!PushSerialized(false)) {
          if (!use_background_mode_ && ThisFiber::GetRunningTimeCycles() > kCyclesPerJiffy) {
            ThisFiber::Yield();
          }
        }
      }
    } while (snapshot_cursor_);

    DVLOG(2) << "after loop " << ThisFiber::GetName();
    // Wait for all the outstanding delayed entries and serialize them as well.
    PushDelayedEntries(true, nullptr);
    PushSerialized(true);
  }  // for (dbindex)

  CHECK(!serialize_bucket_running_);
  if (send_full_sync_cut) {
    CHECK(!serializer_->SendFullSyncCut());
    PushSerialized(true);
  }

  // serialized + side_saved must be equal to the total saved.
  VLOG(1) << "Exit SnapshotSerializer loop_serialized: " << stats_.loop_serialized
          << ", side_saved " << SerializerBase::GetStats().keys_serialized << ", cbcalls "
          << stats_.savecb_calls << ", journal_saved " << stats_.jounal_changes;
}

bool SliceSnapshot::BucketSaveCb(DbIndex db_index, PrimeTable::bucket_iterator it) {
  std::lock_guard guard(big_value_mu_);

  ++stats_.savecb_calls;

  if (it.GetVersion() >= snapshot_version_) {
    // Either already serialized by OnChange or inserted after snapshotting started.
    DVLOG(3) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << " at " << it.GetVersion();
    ++stats_.skipped;
    return false;
  }

  db_slice_->FlushChangeToEarlierCallbacks(db_index, DbSlice::Iterator::FromPrime(it),
                                           snapshot_version_);

  auto* latch = db_slice_->GetLatch();

  // Locking this never preempts. We merely just increment the underline counter such that
  // if DoSerializeBucket preempts, Heartbeat() won't run because the blocking counter is not zero.
  std::lock_guard latch_guard(*latch);

  BucketIdentity bid = it.bucket_address();
  it.SetVersion(snapshot_version_);
  MarkBucketSerializing(bid);
  stats_.loop_serialized += DoSerializeBucket(db_index, it);
  FinishBucketIteration(bid, {});

  return false;
}

unsigned SliceSnapshot::DoSerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it) {
  // Version is already stamped by the caller (BucketSaveCb or SerializerBase::OnChange).
  DCHECK_EQ(it.GetVersion(), snapshot_version_);

  // Always called under big_value_mu_, so clearing here is safe.
  // OnChange will read this set after the base call returns to force-flush tiered keys.
  tiered_keys_on_change_.clear();
  serialize_bucket_running_ = true;
  unsigned result = 0;

  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    ++result;
    // might preempt due to big value serialization.
    SerializeEntry(db_index, it->first, it->second);
  }

  serialize_bucket_running_ = false;
  return result;
}

void SliceSnapshot::SerializeEntry(DbIndex db_indx, const PrimeKey& pk, const PrimeValue& pv) {
  if (pv.IsExternal() && pv.IsCool())
    return SerializeEntry(db_indx, pk, pv.GetCool().record->value);

  time_t expire_time = 0;
  if (pk.HasExpire()) {
    auto eit = db_array_[db_indx]->expire.Find(pk);
    if (!IsValid(eit)) {
      LOG(DFATAL) << "Internal error, entry " << pk.ToString()
                  << " not found in expire table, db_index: " << db_indx
                  << ", expire table size: " << db_array_[db_indx]->expire.size()
                  << ", prime table size: " << db_array_[db_indx]->prime.size()
                  << util::fb2::GetStacktrace();
    } else {
      expire_time = db_slice_->ExpireTime(eit->second);
    }
  }
  uint32_t mc_flags = pv.HasFlag() ? db_slice_->GetMCFlag(db_indx, pk) : 0;

  if (pv.IsExternal()) {
    SerializeExternal(db_indx, pk.ToString(), pv, expire_time, mc_flags);
  } else {
    io::Result<uint8_t> res = serializer_->SaveEntry(pk, pv, expire_time, mc_flags, db_indx);
    CHECK(res);
    ++type_freq_map_[*res];
  }
}

void SliceSnapshot::HandleFlushData(std::string data) {
  if (data.empty())
    return;

  if (big_value_mu_.is_locked()) {
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
  consumer_->ConsumeData(std::move(data), cntx_);

  DCHECK_EQ(last_pushed_id_ + 1, id);
  last_pushed_id_ = id;
  seq_cond_.notify_all();

  if (!use_background_mode_) {
    // serializer_->Flush can be quite slow for large values or due to compression, therefore
    // we counter-balance CPU over-usage by sleeping.
    // We measure running_cycles before the preemption points, because they reset the counter.
    uint64_t sleep_usec = (running_cycles * 1000'000 / base::CycleClock::Frequency()) / 2;
    ThisFiber::SleepFor(chrono::microseconds(std::min<uint64_t>(sleep_usec, 2000ul)));
  }

  VLOG(2) << "Pushed with Serialize() " << serialized;
}

size_t SliceSnapshot::FlushSerialized() {
  std::string blob = serializer_->Flush(RdbSerializerBase::FlushState::kFlushEndEntry);

  size_t serialized = blob.size();
  HandleFlushData(std::move(blob));
  return serialized;
}

bool SliceSnapshot::PushSerialized(bool force) {
  if (!force && serializer_->SerializedLen() < kMinBlobSize)
    return false;
  return FlushSerialized();
}

void SliceSnapshot::PushDelayedEntries(bool force,
                                       absl::flat_hash_set<TieredDelayEntryKey>* tiered_keys) {
  using DelayedEntryNode = decltype(delayed_entries_)::node_type;
  std::vector<DelayedEntryNode> ready_entries;

  // First, directly extract entries matching the provided key filter (O(k)).
  if (tiered_keys) {
    for (const auto& key : *tiered_keys) {
      if (auto it = delayed_entries_.find(key); it != delayed_entries_.end())
        ready_entries.push_back(delayed_entries_.extract(it));
    }
  }

  // Then drain remaining resolved entries (or all if force=true).
  for (auto it = delayed_entries_.begin(); it != delayed_entries_.end();) {
    if (!force && !it->second->value.IsResolved()) {
      ++it;
      continue;
    }
    auto current = it++;
    ready_entries.push_back(delayed_entries_.extract(current));
  }

  for (auto& node : ready_entries) {
    auto& entry = node.mapped();
    auto res = entry->value.Get();  // blocks if not resolved and force=true
    if (!res.has_value()) {
      cntx_->ReportError(make_error_code(errc::io_error),
                         absl::StrCat("Failed to read tiered key: ", entry->key.ToString()));
      return;
    }

    PrimeValue pv{*res};
    auto save_res =
        serializer_->SaveEntry(entry->key, pv, entry->expire, entry->mc_flags, entry->dbid);
    CHECK(save_res);

    // Flush accumulated data to avoid holding too much in the buffer.
    PushSerialized(force);
  }
}

void SliceSnapshot::OnChange(DbIndex db_index, PrimeTable::bucket_iterator it) {
  SerializerBase::OnChange(db_index, it);

  // DoSerializeBucket (called by the base under big_value_mu_) cleared and re-populated
  // tiered_keys_on_change_ with the tiered keys of this bucket. Force-flush them under
  // big_value_mu_ so ConsumeJournalChange cannot interleave before the baseline is sent.
  if (!tiered_keys_on_change_.empty()) {
    std::lock_guard guard(big_value_mu_);
    PushDelayedEntries(true, &tiered_keys_on_change_);
  }
}

void SliceSnapshot::SerializeExternal(DbIndex db_index, std::string_view key, const PrimeValue& pv,
                                      time_t expire_time, uint32_t mc_flags) {
  // We prefer avoid blocking, so we just schedule a tiered read and append
  // it to the delayed entries.
  auto future = ReadTieredString(db_index, key, pv, EngineShard::tlocal()->tiered_storage());
  auto entry = std::make_unique<TieredDelayedEntry>(db_index, PrimeKey{key}, std::move(future),
                                                    expire_time, mc_flags);
  delayed_entries_.emplace(TieredDelayEntryKey{db_index, std::string(key)}, std::move(entry));
  tiered_keys_on_change_.emplace(db_index, key);
  ++type_freq_map_[RDB_TYPE_STRING];
}

// big_value_mu_ prevents expiry/eviction DEL journal entries from interleaving with an
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
// big_value_mu_ is not needed for that ordering.
void SliceSnapshot::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  std::lock_guard barrier(big_value_mu_);

  // remove when we support interleaving chunks.
  LOG_IF(DFATAL, serialize_bucket_running_)
      << "Internal error: can not run interleave journal and bucket serialization";
  std::ignore = serializer_->WriteJournalEntry(item.journal_item.data);
  ++stats_.jounal_changes;
}

void SliceSnapshot::ThrottleIfNeeded() {
  PushSerialized(false);
}

size_t SliceSnapshot::GetBufferCapacity() const {
  if (serializer_ == nullptr) {
    return 0;
  }

  return serializer_->GetBufferCapacity();
}

size_t SliceSnapshot::GetTempBuffersSize() const {
  if (serializer_ == nullptr) {
    return 0;
  }

  return serializer_->GetTempBufferSize();
}

RdbSaver::SnapshotStats SliceSnapshot::GetCurrentSnapshotProgress() const {
  return {stats_.loop_serialized + SerializerBase::GetStats().keys_serialized, stats_.keys_total};
}

}  // namespace dfly
