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
ABSL_FLAG(bool, serialization_tagged_chunks, false,
          "Tag each chunk for replication with type of stream");

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
    : db_slice_(slice),
      db_array_(slice->databases()),
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

  auto db_cb = [this](DbIndex db_index, const DbSlice::ChangeReq& req) {
    OnDbChange(db_index, req);
  };

  use_background_mode_ = absl::GetFlag(FLAGS_background_snapshotting);
  snapshot_version_ = db_slice_->RegisterOnChange(std::move(db_cb));

  if (stream_journal) {
    use_snapshot_version_ = absl::GetFlag(FLAGS_point_in_time_snapshot);
    journal_cb_id_ = journal::RegisterConsumer(this);
    if (!use_snapshot_version_) {
      auto moved_cb = [this](DbIndex db_index, const DbSlice::MovedItemsVec& items) {
        OnMoved(db_index, items);
      };
      moved_cb_id_ = db_slice_->RegisterOnMove(std::move(moved_cb));
    }
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

  if (allow_flush == SnapshotFlush::kAllow) {
    serializer_->SetTagEntries(absl::GetFlag(FLAGS_serialization_tagged_chunks));
  }

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  fb2::Fiber::Opts opts{.priority = use_background_mode_ ? fb2::FiberPriority::BACKGROUND
                                                         : fb2::FiberPriority::NORMAL,
                        .name = absl::StrCat("SliceSnapshot-", ProactorBase::me()->GetPoolIndex())};
  snapshot_fb_ = fb2::Fiber(opts, [this, stream_journal] {
    // TODO add error processing for index serialization
    SerializeIndexMappings();
    SerializeGlobalHnswIndices();
    this->IterateBucketsFb(stream_journal);
    db_slice_->UnregisterOnChange(snapshot_version_);
    if (!use_snapshot_version_) {
      db_slice_->UnregisterOnMoved(moved_cb_id_);
    }
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

  for (DbIndex snapshot_db_index_ = 0; snapshot_db_index_ < db_array_.size();
       ++snapshot_db_index_) {
    if (!cntx_->IsRunning())
      return;

    if (!db_array_[snapshot_db_index_])
      continue;

    PrimeTable* pt = &db_array_[snapshot_db_index_]->prime;
    VLOG(1) << "Start traversing " << pt->size() << " items for index " << snapshot_db_index_;

    do {
      if (!cntx_->IsRunning()) {
        return;
      }

      snapshot_cursor_ = pt->TraverseBuckets(
          snapshot_cursor_,
          [this, &snapshot_db_index_](auto it) { return BucketSaveCb(snapshot_db_index_, it); });

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
          << ", side_saved " << stats_.side_saved << ", cbcalls " << stats_.savecb_calls
          << ", journal_saved " << stats_.jounal_changes << ", moved_saved " << stats_.moved_saved
          << ", flushed_under_lock " << stats_.flushed_under_lock;
}

bool SliceSnapshot::BucketSaveCb(DbIndex db_index, PrimeTable::bucket_iterator it) {
  std::lock_guard guard(big_value_mu_);

  ++stats_.savecb_calls;

  if (use_snapshot_version_) {
    if (it.GetVersion() >= snapshot_version_) {
      // either has been already serialized or added after snapshotting started.
      DVLOG(3) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << " at "
               << it.GetVersion();
      ++stats_.skipped;
      return false;
    }

    db_slice_->FlushChangeToEarlierCallbacks(db_index, DbSlice::Iterator::FromPrime(it),
                                             snapshot_version_);
  }

  auto* latch = db_slice_->GetLatch();

  // Locking this never preempts. We merely just increment the underline counter such that
  // if SerializeBucket preempts, Heartbeat() won't run because the blocking counter is not
  // zero.
  std::lock_guard latch_guard(*latch);

  stats_.loop_serialized += SerializeBucket(db_index, it, false);

  return false;
}

unsigned SliceSnapshot::SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it,
                                        bool push_tiered) {
  if (use_snapshot_version_) {
    DCHECK_LT(it.GetVersion(), snapshot_version_);
    it.SetVersion(snapshot_version_);
  }

  // traverse physical bucket and write it into string file.
  serialize_bucket_running_ = true;

  unsigned result = 0;

  std::vector<TieredDelayEntryKey> bucket_tiered_keys;
  const bool tiering_enabled = EngineShard::tlocal()->tiered_storage() != nullptr;
  const bool track_tiered_keys = push_tiered && tiering_enabled;

  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    ++result;
    // might preempt due to big value serialization.
    SerializeEntry(db_index, it->first, it->second);
    // Track tiered keys to push them with priority after the loop, but only for callbacks.
    if (track_tiered_keys && it->second.IsExternal()) {
      bucket_tiered_keys.emplace_back(db_index, it->first.ToString());
    }
  }

  if (tiering_enabled) {
    // Push tracked tiered keys forcefully. If there are too many delayed entries
    // accumulated we should also push them forcefully.
    const size_t kMaxDelayedEntries = 512;
    PushDelayedEntries(delayed_entries_.size() > kMaxDelayedEntries,
                       track_tiered_keys ? &bucket_tiered_keys : nullptr);
  }

  serialize_bucket_running_ = false;
  return result;
}

void SliceSnapshot::SerializeEntry(DbIndex db_indx, const PrimeKey& pk, const PrimeValue& pv) {
  if (pv.IsExternal() && pv.IsCool())
    return SerializeEntry(db_indx, pk, pv.GetCool().record->value);

  time_t expire_time = pk.GetExpireTime();
  uint32_t mc_flags = pv.HasFlag() ? db_slice_->GetMCFlag(db_indx, pk) : 0;

  if (pv.IsExternal()) {
    // TODO: we loose the stickiness attribute by cloning like this PrimeKey.
    SerializeExternal(db_indx, PrimeKey{pk.ToString()}, pv, expire_time, mc_flags);
  } else {
    serializer_->SetCurrentStreamId(next_stream_id_++);
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
                                       std::vector<TieredDelayEntryKey>* bucket_tiered_keys) {
  using DelayedEntryIt = decltype(delayed_entries_)::iterator;

  // Serializes a single delayed entry. Resolves the tiered read future, write the
  // key/value and removes the entry from the map.
  auto serialize_entry = [this](DelayedEntryIt it) {
    auto& entry = it->second;
    auto value = entry->value.Get();

    if (!value.has_value()) {
      cntx_->ReportError(make_error_code(errc::io_error),
                         absl::StrCat("Failed to read tiered key: ", entry->key.ToString()));
      return;
    }

    PrimeValue pv{*value};
    serializer_->SetCurrentStreamId(next_stream_id_++);
    auto res = serializer_->SaveEntry(entry->key, pv, entry->expire, entry->mc_flags, entry->dbid);
    CHECK(res);

    delayed_entries_.erase(it);

    // If we have serialized enough data we should push it to avoid building
    // up a large blob in memory.
    PushSerialized(false);
  };

  // When tiered_keys are provided, we should serialize the entries matching the keys.
  if (bucket_tiered_keys) {
    for (const auto& key : *bucket_tiered_keys) {
      if (auto it = delayed_entries_.find(key); it != delayed_entries_.end())
        serialize_entry(it);
    }
  }

  // Serialize the delayed entries that are resolved, or all if force it true.
  for (auto it = delayed_entries_.begin(); it != delayed_entries_.end();) {
    if (!force && !it->second->value.IsResolved()) {
      ++it;
      continue;
    }
    serialize_entry(it++);
  }

  // If we need to serialize all entries (force=true), we should push
  // leftover serialized data after the loop.
  PushSerialized(force);
}

void SliceSnapshot::SerializeExternal(DbIndex db_index, PrimeKey pk, const PrimeValue& pv,
                                      time_t expire_time, uint32_t mc_flags) {
  // We prefer avoid blocking, so we just schedule a tiered read and append
  // it to the delayed entries.
  auto key = pk.ToString();
  auto future = ReadTieredString(db_index, key, pv, EngineShard::tlocal()->tiered_storage());
  auto entry = std::make_unique<TieredDelayedEntry>(db_index, std::move(pk), std::move(future),
                                                    expire_time, mc_flags);
  delayed_entries_.emplace(std::make_pair(db_index, key), std::move(entry));
  ++type_freq_map_[RDB_TYPE_STRING];
}

// Ordering invariant (both modes):
//   For any key K, the replica must receive K's baseline value strictly before any journal entry
//   that mutates K. This is required for baseline-dependent journal entries (e.g., HSET, LPUSH)
//   which cannot be replayed without the prior value.
//
// PIT mode: enforced by serialize-before-mutate. OnDbChange serializes the bucket before the
//   mutation commits; ConsumeJournalChange runs after the mutation on the same fiber, so the
//   baseline is always first. big_value_mu_ prevents interleaving with the traversal fiber's
//   SerializeBucket (which can preempt via consume_fun_).
//
// Non-PIT mode: OnDbChange only acquires big_value_mu_ as a barrier — no serialization. The
//   mutex prevents journaling mutations from slipping in the middle of bucket serialization
//   on the traversal fiber — see ConsumeJournalChange for details. OnMoved handles items
//   displaced across the traversal cursor.
void SliceSnapshot::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  std::lock_guard guard(big_value_mu_);
  if (use_snapshot_version_) {
    PrimeTable* table = db_slice_->GetTables(db_index);
    const PrimeTable::bucket_iterator* bit = req.update();

    if (bit) {
      if (!bit->is_done() && bit->GetVersion() < snapshot_version_) {
        stats_.side_saved += SerializeBucket(db_index, *bit, true);
      }
    } else {
      string_view key = get<string_view>(req.change);
      table->CVCUponInsert(snapshot_version_, key,
                           [this, db_index](PrimeTable::bucket_iterator it) {
                             DCHECK_LT(it.GetVersion(), snapshot_version_);
                             stats_.side_saved += SerializeBucket(db_index, it, true);
                           });
    }
  }
}

bool SliceSnapshot::IsPositionSerialized(DbIndex id, PrimeTable::Cursor cursor) {
  uint8_t depth = db_slice_->GetTables(id)->depth();

  return id < snapshot_db_index_ ||
         (id == snapshot_db_index_ &&
          (cursor.bucket_id() < snapshot_cursor_.bucket_id() ||
           (cursor.bucket_id() == snapshot_cursor_.bucket_id() &&
            cursor.segment_id(depth) < snapshot_cursor_.segment_id(depth))));
}

void SliceSnapshot::OnMoved(DbIndex id, const DbSlice::MovedItemsVec& items) {
  std::lock_guard barrier(big_value_mu_);
  DCHECK(!use_snapshot_version_);
  for (const auto& item_cursors : items) {
    // If item was moved from a bucket that was serialized to a bucket that was not serialized
    // serialize the moved item.
    const PrimeTable::Cursor& dest = item_cursors.second;
    const PrimeTable::Cursor& source = item_cursors.first;
    if (IsPositionSerialized(id, dest) && !IsPositionSerialized(id, source)) {
      PrimeTable::bucket_iterator bit = db_slice_->GetTables(id)->CursorToBucketIt(dest);
      ++stats_.moved_saved;
      SerializeBucket(id, bit, true);
    }
  }
}

// big_value_mu_ prevents expiry/eviction DEL journal entries from interleaving with an
// in-progress SaveEntry for a large value. SaveEntry may yield mid-entry (emitting chunks
// across multiple scheduler turns); expiry paths emit DEL via RecordDelete directly,
// bypassing OnDbChange. Without the lock, such a DEL could be written between two chunks
// of the same entry, producing an invalid wire format for the downstream consumer.
//
// Note: even if the protocol were extended to support interleaved chunks, the lock would
// still be required semantically: a DEL journal entry must not be applied on the replica
// while the entry's baseline is still being loaded. The delayed deletion queue proposal
// in the design doc addresses this without a shard-wide lock.
//
// Note: for transaction-driven mutations, baseline-before-journal ordering is already
// guaranteed by call order on the mutation fiber (OnDbChange precedes ConsumeJournalChange);
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
  return {stats_.loop_serialized + stats_.side_saved, stats_.keys_total};
}

}  // namespace dfly
