// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

extern "C" {
#include "redis/object.h"
}

#include <absl/functional/bind_front.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/rdb_extensions.h"
#include "server/rdb_save.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;

namespace {
thread_local absl::flat_hash_set<SliceSnapshot*> tl_slice_snapshots;
}  // namespace

SliceSnapshot::SliceSnapshot(DbSlice* slice, RecordChannel* dest, CompressionMode compression_mode)
    : db_slice_(slice), dest_(dest), compression_mode_(compression_mode) {
  db_array_ = slice->databases();
  tl_slice_snapshots.insert(this);
}

SliceSnapshot::~SliceSnapshot() {
  tl_slice_snapshots.erase(this);
}

size_t SliceSnapshot::GetThreadLocalMemoryUsage() {
  size_t mem = 0;
  for (SliceSnapshot* snapshot : tl_slice_snapshots) {
    mem += snapshot->GetTotalBufferCapacity() + snapshot->GetTotalChannelCapacity();
  }
  return mem;
}

void SliceSnapshot::Start(bool stream_journal, const Cancellation* cll) {
  DCHECK(!snapshot_fb_.IsJoinable());

  auto db_cb = absl::bind_front(&SliceSnapshot::OnDbChange, this);
  snapshot_version_ = db_slice_->RegisterOnChange(std::move(db_cb));

  if (stream_journal) {
    auto* journal = db_slice_->shard_owner()->journal();
    DCHECK(journal);
    auto journal_cb = absl::bind_front(&SliceSnapshot::OnJournalEntry, this);
    journal_cb_id_ = journal->RegisterOnChange(std::move(journal_cb));
  }

  serializer_ = std::make_unique<RdbSerializer>(compression_mode_);

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  snapshot_fb_ = fb2::Fiber("snapshot", [this, stream_journal, cll] {
    IterateBucketsFb(cll);
    db_slice_->UnregisterOnChange(snapshot_version_);
    if (cll->IsCancelled()) {
      Cancel();
    } else if (!stream_journal) {
      CloseRecordChannel();
    }
  });
}

void SliceSnapshot::StartIncremental(Context* cntx, LSN start_lsn) {
  auto* journal = db_slice_->shard_owner()->journal();
  DCHECK(journal);

  serializer_ = std::make_unique<RdbSerializer>(compression_mode_);

  snapshot_fb_ =
      fb2::Fiber("incremental_snapshot", [this, journal, cntx, lsn = start_lsn]() mutable {
        DCHECK(lsn <= journal->GetLsn()) << "The replica tried to sync from the future.";

        VLOG(1) << "Starting incremental snapshot from lsn=" << lsn;

        // The replica sends the LSN of the next entry is wants to receive.
        while (!cntx->IsCancelled() && journal->IsLSNInBuffer(lsn)) {
          serializer_->WriteJournalEntry(journal->GetEntry(lsn));
          PushSerializedToChannel(false);
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
          auto journal_cb = absl::bind_front(&SliceSnapshot::OnJournalEntry, this);
          journal_cb_id_ = journal->RegisterOnChange(std::move(journal_cb));
          PushSerializedToChannel(true);
        } else {
          // We stopped but we didn't manage to send the whole stream.
          cntx->ReportError(
              std::make_error_code(errc::state_not_recoverable),
              absl::StrCat("Partial sync was unsuccessful because entry #", lsn,
                           " was dropped from the buffer. Current lsn=", journal->GetLsn()));
          Cancel();
        }
      });
}

void SliceSnapshot::Stop() {
  // Wait for serialization to finish in any case.
  Join();

  if (journal_cb_id_) {
    auto* journal = db_slice_->shard_owner()->journal();
    serializer_->SendJournalOffset(journal->GetLsn());
    journal->UnregisterOnChange(journal_cb_id_);
  }

  PushSerializedToChannel(true);
  CloseRecordChannel();
}

void SliceSnapshot::Cancel() {
  VLOG(1) << "SliceSnapshot::Cancel";

  // Cancel() might be called multiple times from different fibers of the same thread, but we
  // should unregister the callback only once.
  uint32_t cb_id = journal_cb_id_;
  if (cb_id) {
    journal_cb_id_ = 0;
    db_slice_->shard_owner()->journal()->UnregisterOnChange(cb_id);
  }

  CloseRecordChannel();
}

void SliceSnapshot::Join() {
  // Fiber could have already been joined by Stop.
  snapshot_fb_.JoinIfNeeded();
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
void SliceSnapshot::IterateBucketsFb(const Cancellation* cll) {
  {
    auto fiber_name = absl::StrCat("SliceSnapshot-", ProactorBase::GetIndex());
    ThisFiber::SetName(std::move(fiber_name));
  }

  PrimeTable::Cursor cursor;
  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    if (cll->IsCancelled())
      return;

    if (!db_array_[db_indx])
      continue;

    uint64_t last_yield = 0;
    PrimeTable* pt = &db_array_[db_indx]->prime;
    current_db_ = db_indx;

    VLOG(1) << "Start traversing " << pt->size() << " items for index " << db_indx;
    do {
      if (cll->IsCancelled())
        return;

      PrimeTable::Cursor next =
          pt->Traverse(cursor, absl::bind_front(&SliceSnapshot::BucketSaveCb, this));
      cursor = next;
      PushSerializedToChannel(false);

      if (stats_.loop_serialized >= last_yield + 100) {
        DVLOG(2) << "Before sleep " << ThisFiber::GetName();
        ThisFiber::Yield();
        DVLOG(2) << "After sleep";

        last_yield = stats_.loop_serialized;
        // Push in case other fibers (writes commands that pushed previous values)
        // filled the buffer.
        PushSerializedToChannel(false);
      }
    } while (cursor);

    DVLOG(2) << "after loop " << ThisFiber::GetName();
    PushSerializedToChannel(true);
  }  // for (dbindex)

  CHECK(!serialize_bucket_running_);
  CHECK(!serializer_->SendFullSyncCut());
  PushSerializedToChannel(true);

  // serialized + side_saved must be equal to the total saved.
  VLOG(1) << "Exit SnapshotSerializer (loop_serialized/side_saved/cbcalls): "
          << stats_.loop_serialized << "/" << stats_.side_saved << "/" << stats_.savecb_calls;
}

bool SliceSnapshot::BucketSaveCb(PrimeIterator it) {
  ++stats_.savecb_calls;

  uint64_t v = it.GetVersion();
  if (v >= snapshot_version_) {
    // either has been already serialized or added after snapshotting started.
    DVLOG(3) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << ":" << it.slot_id()
             << " at " << v;
    ++stats_.skipped;
    return false;
  }
  db_slice_->FlushChangeToEarlierCallbacks(current_db_, it, snapshot_version_);

  stats_.loop_serialized += SerializeBucket(current_db_, it);
  return false;
}

unsigned SliceSnapshot::SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it) {
  // Must be atomic because after after we call it.snapshot_version_ we're starting
  // to send incremental updates instead of serializing the whole bucket: We must not
  // send the update until the initial SerializeBucket is called.
  // Relying on the atomicity of SerializeBucket is Ok here because only one thread may handle this
  // bucket.
  FiberAtomicGuard fg;
  DCHECK_LT(it.GetVersion(), snapshot_version_);

  // traverse physical bucket and write it into string file.
  serialize_bucket_running_ = true;
  it.SetVersion(snapshot_version_);
  unsigned result = 0;

  while (!it.is_done()) {
    ++result;
    SerializeEntry(db_index, it->first, it->second, nullopt, serializer_.get());
    ++it;
  }
  serialize_bucket_running_ = false;
  return result;
}

// This function should not block and should not preempt because it's called
// from SerializeBucket which should execute atomically.
void SliceSnapshot::SerializeEntry(DbIndex db_indx, const PrimeKey& pk, const PrimeValue& pv,
                                   optional<uint64_t> expire, RdbSerializer* serializer) {
  time_t expire_time = expire.value_or(0);
  if (!expire && pv.HasExpire()) {
    auto eit = db_array_[db_indx]->expire.Find(pk);
    expire_time = db_slice_->ExpireTime(eit);
  }

  io::Result<uint8_t> res = serializer->SaveEntry(pk, pv, expire_time, db_indx);
  CHECK(res);
  ++type_freq_map_[*res];
}

bool SliceSnapshot::PushSerializedToChannel(bool force) {
  if (!force && serializer_->SerializedLen() < 4096)
    return false;

  io::StringFile sfile;
  serializer_->FlushToSink(&sfile);

  size_t serialized = sfile.val.size();
  if (serialized == 0)
    return 0;

  auto id = rec_id_++;
  DVLOG(2) << "Pushed " << id;
  DbRecord db_rec{.id = id, .value = std::move(sfile.val)};

  dest_->Push(std::move(db_rec));

  VLOG(2) << "PushSerializedToChannel " << serialized << " bytes";
  return true;
}

void SliceSnapshot::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  FiberAtomicGuard fg;
  PrimeTable* table = db_slice_->GetTables(db_index).first;

  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    if (bit->GetVersion() < snapshot_version_) {
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
// OnJournalEntry registers for changes in journal, the journal change function signature is
// (const journal::Entry& entry, bool await) In snapshot flow we dont use the await argument.
void SliceSnapshot::OnJournalEntry(const journal::JournalItem& item, bool unused_await_arg) {
  // We ignore EXEC and NOOP entries because we they have no meaning during
  // the LOAD phase on replica.
  if (item.opcode == journal::Op::NOOP || item.opcode == journal::Op::EXEC)
    return;

  serializer_->WriteJournalEntry(item.data);

  // This is the only place that flushes in streaming mode
  // once the iterate buckets fiber finished.
  PushSerializedToChannel(false);
}

void SliceSnapshot::CloseRecordChannel() {
  CHECK(!serialize_bucket_running_);
  // Make sure we close the channel only once with a CAS check.
  bool expected = false;
  if (closed_chan_.compare_exchange_strong(expected, true)) {
    dest_->StartClosing();
  }
}

size_t SliceSnapshot::GetTotalBufferCapacity() const {
  return serializer_->GetTotalBufferCapacity();
}

size_t SliceSnapshot::GetTotalChannelCapacity() const {
  return dest_->GetSize();
}

}  // namespace dfly
