// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <mutex>

#include "base/flags.h"
#include "base/logging.h"
#include "core/heap_size.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/rdb_extensions.h"
#include "server/rdb_save.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "util/fibers/synchronization.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;

using facade::operator""_MB;
using facade::operator""_KB;
namespace {
thread_local absl::flat_hash_set<SliceSnapshot*> tl_slice_snapshots;

constexpr size_t kMinBlobSize = 32_KB;

}  // namespace

SliceSnapshot::SliceSnapshot(CompressionMode compression_mode, DbSlice* slice,
                             SnapshotDataConsumerInterface* consumer, Context* cntx)
    : db_slice_(slice),
      db_array_(slice->databases()),
      compression_mode_(compression_mode),
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
  return tl_slice_snapshots.size() > 0;
}

void SliceSnapshot::Start(bool stream_journal, SnapshotFlush allow_flush) {
  DCHECK(!snapshot_fb_.IsJoinable());

  auto db_cb = [this](DbIndex db_index, const DbSlice::ChangeReq& req) {
    OnDbChange(db_index, req);
  };

  snapshot_version_ = db_slice_->RegisterOnChange(std::move(db_cb));

  if (stream_journal) {
    auto* journal = db_slice_->shard_owner()->journal();
    DCHECK(journal);
    auto journal_cb = [this](const journal::JournalItem& item, bool await) {
      OnJournalEntry(item, await);
    };
    journal_cb_id_ = journal->RegisterOnChange(std::move(journal_cb));
  }

  const auto flush_threshold = ServerState::tlocal()->serialization_max_chunk_size;
  std::function<void(size_t, RdbSerializer::FlushState)> flush_fun;
  if (flush_threshold != 0 && allow_flush == SnapshotFlush::kAllow) {
    flush_fun = [this, flush_threshold](size_t bytes_serialized,
                                        RdbSerializer::FlushState flush_state) {
      if (bytes_serialized > flush_threshold) {
        size_t serialized = FlushSerialized(flush_state);
        VLOG(2) << "FlushSerialized " << serialized << " bytes";
        auto& stats = ServerState::tlocal()->stats;
        ++stats.big_value_preemptions;
      }
    };
  }
  serializer_ = std::make_unique<RdbSerializer>(compression_mode_, flush_fun);

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  snapshot_fb_ = fb2::Fiber("snapshot", [this, stream_journal] {
    this->IterateBucketsFb(stream_journal);
    db_slice_->UnregisterOnChange(snapshot_version_);
    consumer_->Finalize();
  });
}

void SliceSnapshot::StartIncremental(LSN start_lsn) {
  serializer_ = std::make_unique<RdbSerializer>(compression_mode_);

  snapshot_fb_ = fb2::Fiber("incremental_snapshot",
                            [start_lsn, this] { this->SwitchIncrementalFb(start_lsn); });
}

// Called only for replication use-case.
void SliceSnapshot::FinalizeJournalStream(bool cancel) {
  DVLOG(1) << "Finalize Snapshot";
  DCHECK(db_slice_->shard_owner()->IsMyThread());
  if (!journal_cb_id_) {  // Finalize only once.
    return;
  }
  uint32_t cb_id = journal_cb_id_;
  journal_cb_id_ = 0;

  // Wait for serialization to finish in any case.
  snapshot_fb_.JoinIfNeeded();

  auto* journal = db_slice_->shard_owner()->journal();

  journal->UnregisterOnChange(cb_id);
  if (!cancel) {
    serializer_->SendJournalOffset(journal->GetLsn());
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
  {
    auto fiber_name = absl::StrCat("SliceSnapshot-", ProactorBase::me()->GetPoolIndex());
    ThisFiber::SetName(std::move(fiber_name));
  }

  PrimeTable::Cursor cursor;
  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    stats_.keys_total += db_slice_->DbSize(db_indx);
  }

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    if (cntx_->IsCancelled())
      return;

    if (!db_array_[db_indx])
      continue;

    uint64_t last_yield = 0;
    PrimeTable* pt = &db_array_[db_indx]->prime;

    VLOG(1) << "Start traversing " << pt->size() << " items for index " << db_indx;
    do {
      if (cntx_->IsCancelled()) {
        return;
      }

      PrimeTable::Cursor next = pt->TraverseBuckets(
          cursor, [this, &db_indx](auto it) { return BucketSaveCb(db_indx, it); });
      cursor = next;
      PushSerialized(false);

      if (stats_.loop_serialized >= last_yield + 100) {
        DVLOG(2) << "Before sleep " << ThisFiber::GetName();
        ThisFiber::Yield();
        DVLOG(2) << "After sleep";

        last_yield = stats_.loop_serialized;
        // Push in case other fibers (writes commands that pushed previous values)
        // filled the buffer.
        PushSerialized(false);
      }
    } while (cursor);

    DVLOG(2) << "after loop " << ThisFiber::GetName();
    PushSerialized(true);
  }  // for (dbindex)

  CHECK(!serialize_bucket_running_);
  if (send_full_sync_cut) {
    CHECK(!serializer_->SendFullSyncCut());
    PushSerialized(true);
  }

  // serialized + side_saved must be equal to the total saved.
  VLOG(1) << "Exit SnapshotSerializer (loop_serialized/side_saved/cbcalls): "
          << stats_.loop_serialized << "/" << stats_.side_saved << "/" << stats_.savecb_calls;
}

void SliceSnapshot::SwitchIncrementalFb(LSN lsn) {
  auto* journal = db_slice_->shard_owner()->journal();
  DCHECK(journal);
  DCHECK_LE(lsn, journal->GetLsn()) << "The replica tried to sync from the future.";

  VLOG(1) << "Starting incremental snapshot from lsn=" << lsn;

  // The replica sends the LSN of the next entry is wants to receive.
  while (!cntx_->IsCancelled() && journal->IsLSNInBuffer(lsn)) {
    serializer_->WriteJournalEntry(journal->GetEntry(lsn));
    PushSerialized(false);
    lsn++;
  }

  VLOG(1) << "Last LSN sent in incremental snapshot was " << (lsn - 1);

  // This check is safe, but it is not trivially safe.
  // We rely here on the fact that JournalSlice::AddLogRecord can
  // only preempt while holding the callback lock.
  // That guarantees that if we have processed the last LSN the callback
  // will only be added after JournalSlice::AddLogRecord has finished
  // iterating its callbacks and we won't process the record twice.
  // We have to make sure we don't preempt ourselves before registering the callback!

  // GetLsn() is always the next lsn that we expect to create.
  if (journal->GetLsn() == lsn) {
    {
      FiberAtomicGuard fg;
      serializer_->SendFullSyncCut();
    }
    auto journal_cb = [this](const journal::JournalItem& item, bool await) {
      OnJournalEntry(item, await);
    };
    journal_cb_id_ = journal->RegisterOnChange(std::move(journal_cb));
    PushSerialized(true);
  } else {
    // We stopped but we didn't manage to send the whole stream.
    cntx_->ReportError(
        std::make_error_code(errc::state_not_recoverable),
        absl::StrCat("Partial sync was unsuccessful because entry #", lsn,
                     " was dropped from the buffer. Current lsn=", journal->GetLsn()));
    FinalizeJournalStream(true);
  }
}

bool SliceSnapshot::BucketSaveCb(DbIndex db_index, PrimeTable::bucket_iterator it) {
  std::lock_guard guard(big_value_mu_);

  ++stats_.savecb_calls;

  auto check = [&](auto v) {
    if (v >= snapshot_version_) {
      // either has been already serialized or added after snapshotting started.
      DVLOG(3) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << " at " << v;
      ++stats_.skipped;
      return false;
    }
    return true;
  };

  if (!check(it.GetVersion())) {
    return false;
  }

  db_slice_->FlushChangeToEarlierCallbacks(db_index, DbSlice::Iterator::FromPrime(it),
                                           snapshot_version_);

  auto* blocking_counter = db_slice_->BlockingCounter();
  // Locking this never preempts. We merely just increment the underline counter such that
  // if SerializeBucket preempts, Heartbeat() won't run because the blocking counter is not
  // zero.
  std::lock_guard blocking_counter_guard(*blocking_counter);

  stats_.loop_serialized += SerializeBucket(db_index, it);

  return false;
}

unsigned SliceSnapshot::SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it) {
  DCHECK_LT(it.GetVersion(), snapshot_version_);

  // traverse physical bucket and write it into string file.
  serialize_bucket_running_ = true;
  it.SetVersion(snapshot_version_);
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
  if (pv.HasExpire()) {
    auto eit = db_array_[db_indx]->expire.Find(pk);
    expire_time = db_slice_->ExpireTime(eit);
  }

  uint32_t mc_flags = pv.HasFlag() ? db_slice_->GetMCFlag(db_indx, pk) : 0;

  if (pv.IsExternal()) {
    // We can't block, so we just schedule a tiered read and append it to the delayed entries
    util::fb2::Future<PrimeValue> future;
    EngineShard::tlocal()->tiered_storage()->Read(
        db_indx, pk.ToString(), pv,
        [future](const std::string& v) mutable { future.Resolve(PrimeValue(v)); });
    delayed_entries_.push_back(
        {db_indx, PrimeKey(pk.ToString()), std::move(future), expire_time, mc_flags});
    ++type_freq_map_[RDB_TYPE_STRING];
  } else {
    io::Result<uint8_t> res = serializer_->SaveEntry(pk, pv, expire_time, mc_flags, db_indx);
    CHECK(res);
    ++type_freq_map_[*res];
  }
}

size_t SliceSnapshot::FlushSerialized(SerializerBase::FlushState flush_state) {
  io::StringFile sfile;
  serializer_->FlushToSink(&sfile, flush_state);

  size_t serialized = sfile.val.size();
  if (serialized == 0)
    return 0;

  uint64_t id = rec_id_++;
  DVLOG(2) << "Pushing " << id;

  fb2::NoOpLock lk;

  // We create a critical section here that ensures that records are pushed in sequential order.
  // As a result, it is not possible for two fiber producers to push concurrently.
  // If A.id = 5, and then B.id = 6, and both are blocked here, it means that last_pushed_id_ < 4.
  // Once last_pushed_id_ = 4, A will be unblocked, while B will wait until A finishes pushing and
  // update last_pushed_id_ to 5.
  seq_cond_.wait(lk, [&] { return id == this->last_pushed_id_ + 1; });

  // Blocking point.
  consumer_->ConsumeData(std::move(sfile.val), cntx_);

  DCHECK_EQ(last_pushed_id_ + 1, id);
  last_pushed_id_ = id;
  seq_cond_.notify_all();

  VLOG(2) << "Pushed with Serialize() " << serialized;

  return serialized;
}

bool SliceSnapshot::PushSerialized(bool force) {
  if (!force && serializer_->SerializedLen() < kMinBlobSize)
    return false;

  // Flush any of the leftovers to avoid interleavings
  size_t serialized = FlushSerialized(FlushState::kFlushEndEntry);

  if (!delayed_entries_.empty()) {
    // Async bucket serialization might have accumulated some delayed values.
    // Because we can finally block in this function, we'll await and serialize them
    do {
      auto& entry = delayed_entries_.back();
      serializer_->SaveEntry(entry.key, entry.value.Get(), entry.expire, entry.dbid,
                             entry.mc_flags);
      delayed_entries_.pop_back();
    } while (!delayed_entries_.empty());

    // blocking point.
    serialized += FlushSerialized(FlushState::kFlushEndEntry);
  }
  return serialized > 0;
}

void SliceSnapshot::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  std::lock_guard guard(big_value_mu_);

  PrimeTable* table = db_slice_->GetTables(db_index).first;
  const PrimeTable::bucket_iterator* bit = req.update();

  if (bit) {
    if (!bit->is_done() && bit->GetVersion() < snapshot_version_) {
      stats_.side_saved += SerializeBucket(db_index, *bit);
    }
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [this, db_index](PrimeTable::bucket_iterator it) {
      DCHECK_LT(it.GetVersion(), snapshot_version_);
      stats_.side_saved += SerializeBucket(db_index, it);
    });
  }
}

// For any key any journal entry must arrive at the replica strictly after its first original rdb
// value. This is guaranteed by the fact that OnJournalEntry runs always after OnDbChange, and
// no database switch can be performed between those two calls, because they are part of one
// transaction.
void SliceSnapshot::OnJournalEntry(const journal::JournalItem& item, bool await) {
  // To enable journal flushing to sync after non auto journal command is executed we call
  // TriggerJournalWriteToSink. This call uses the NOOP opcode with await=true. Since there is no
  // additional journal change to serialize, it simply invokes PushSerialized.
  std::lock_guard guard(big_value_mu_);
  if (item.opcode != journal::Op::NOOP) {
    serializer_->WriteJournalEntry(item.data);
  }

  if (await) {
    // This is the only place that flushes in streaming mode
    // once the iterate buckets fiber finished.
    PushSerialized(false);
  }
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
