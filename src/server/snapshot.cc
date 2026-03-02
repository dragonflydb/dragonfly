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
          << ", journal_saved " << stats_.jounal_changes << ", moved_saved " << stats_.moved_saved;
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

  stats_.loop_serialized += SerializeBucket(db_index, it);

  return false;
}

unsigned SliceSnapshot::SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it) {
  if (use_snapshot_version_) {
    DCHECK_LT(it.GetVersion(), snapshot_version_);
    it.SetVersion(snapshot_version_);
  }

  // traverse physical bucket and write it into string file.
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
    // TODO: we loose the stickiness attribute by cloning like this PrimeKey.
    SerializeExternal(db_indx, PrimeKey{pk.ToString()}, pv, expire_time, mc_flags);
  } else {
    io::Result<uint8_t> res = serializer_->SaveEntry(pk, pv, expire_time, mc_flags, db_indx);
    CHECK(res);
    ++type_freq_map_[*res];
  }
}

void SliceSnapshot::HandleFlushData(std::string data) {
  if (data.empty())
    return;

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

size_t SliceSnapshot::FlushSerialized(RdbSerializer* serializer) {
  if (serializer == nullptr) {
    CHECK(delayed_entries_.empty());
    serializer = serializer_.get();
  }
  std::string blob = serializer->Flush(SerializerBase::FlushState::kFlushEndEntry);

  size_t serialized = blob.size();
  HandleFlushData(std::move(blob));
  return serialized;
}

bool SliceSnapshot::PushSerialized(bool force) {
  if (!force && serializer_->SerializedLen() < kMinBlobSize && delayed_entries_.size() < 32)
    return false;

  size_t serialized = 0;

  // Atomic bucket serialization might have accumulated some delayed values.
  // Because we can finally block in this function, we'll await and serialize them
  thread_local LocalLatch delayed_flush_latch_;
  while (!delayed_entries_.empty()) {
    // After pop_front there is no indication of the operation ongoing, so we need a latch
    std::unique_lock lk{delayed_flush_latch_};

    RdbSerializer delayed_serializer{compression_mode_};
    do {
      // This code can run concurrently, so pop the entries one by one.
      // Because the keys never repeat (bucket visited once) order is not important.
      TieredDelayedEntry entry = std::move(delayed_entries_.front());
      delayed_entries_.pop_front();

      // TODO: https://github.com/dragonflydb/dragonfly/issues/4654
      // there are a few problems with how we serialize external values.
      // 1. We may block here too frequently, slowing down the process.
      // 2. For small bin values, we issue multiple reads for the same page, creating
      //    read factor amplification that can reach factor of ~60.
      auto res = entry.value.Get();  // Blocking point
      if (!res.has_value()) {
        cntx_->ReportError(make_error_code(errc::io_error),
                           absl::StrCat("Failed to read ", entry.key.ToString()));
        return false;
      }

      // TODO: to introduce RdbSerializer::SaveString that can accept a string value directly.
      PrimeValue pv{*res};
      delayed_serializer.SaveEntry(entry.key, pv, entry.expire, entry.mc_flags, entry.dbid);
    } while (!delayed_entries_.empty());

    lk.unlock();
    serialized += FlushSerialized(&delayed_serializer);
  }

  // Flush any of the leftovers to avoid interleavings
  delayed_flush_latch_.Wait();
  serialized += FlushSerialized();

  return serialized > 0;
}

void SliceSnapshot::SerializeExternal(DbIndex db_index, PrimeKey key, const PrimeValue& pv,
                                      time_t expire_time, uint32_t mc_flags) {
  // We prefer avoid blocking, so we just schedule a tiered read and append
  // it to the delayed entries.
  auto future =
      ReadTieredString(db_index, key.ToString(), pv, EngineShard::tlocal()->tiered_storage());
  delayed_entries_.push_back({db_index, std::move(key), std::move(future), expire_time, mc_flags});
  ++type_freq_map_[RDB_TYPE_STRING];
}

void SliceSnapshot::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  std::lock_guard guard(big_value_mu_);
  // Only when creating point in time snapshot we need to serialize the bucket before we change the
  // db entry. When creating no point in time snapshot we need to call OnDbChange which will take
  // the big_value_mu_ to make sure we do not mutate the bucket while serializing it.
  if (use_snapshot_version_) {
    PrimeTable* table = db_slice_->GetTables(db_index).first;
    const PrimeTable::bucket_iterator* bit = req.update();

    if (bit) {
      if (!bit->is_done() && bit->GetVersion() < snapshot_version_) {
        stats_.side_saved += SerializeBucket(db_index, *bit);
      }
    } else {
      string_view key = get<string_view>(req.change);
      table->CVCUponInsert(snapshot_version_, key,
                           [this, db_index](PrimeTable::bucket_iterator it) {
                             DCHECK_LT(it.GetVersion(), snapshot_version_);
                             stats_.side_saved += SerializeBucket(db_index, it);
                           });
    }
  }
}

bool SliceSnapshot::IsPositionSerialized(DbIndex id, PrimeTable::Cursor cursor) {
  uint8_t depth = db_slice_->GetTables(id).first->depth();

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
      PrimeTable::bucket_iterator bit = db_slice_->GetTables(id).first->CursorToBucketIt(dest);
      ++stats_.moved_saved;
      SerializeBucket(id, bit);
    }
  }
}

// For any key any journal entry must arrive at the replica strictly after its first original rdb
// value. This is guaranteed because journal change callbacks run after OnDbChange, and no
// database switch can be performed between those two calls, as they are part of one transaction.
void SliceSnapshot::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  // We grab the lock in case we are in the middle of serializing a bucket, so it serves as a
  // barrier here for atomic serialization.
  std::lock_guard barrier(big_value_mu_);
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
